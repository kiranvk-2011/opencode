import { BusEvent } from "@/bus/bus-event"
import { Bus } from "@/bus"
import * as Session from "./session"
import { SessionID, MessageID, PartID } from "./schema"
import { Provider } from "../provider"
import { MessageV2 } from "./message-v2"
import z from "zod"
import { Token, Log } from "../util"
import { SessionProcessor } from "./processor"
import { Agent } from "@/agent/agent"
import { Plugin } from "@/plugin"
import { Config } from "@/config"
import { NotFoundError } from "@/storage"
import { ModelID, ProviderID } from "@/provider/schema"
import { Effect, Layer, Context, Duration, Scope } from "effect"
import { InstanceState } from "@/effect"
import { isOverflow as overflow } from "./overflow"

const log = Log.create({ service: "session.compaction" })

// OOM Safeguard thresholds
export const HARD_CAP_RATIO = 0.95       // At 95%, reject new messages
export const EMERGENCY_PRUNE_RATIO = 1.0 // At 100%, hard-delete oldest messages
export const SYNC_FALLBACK_THRESHOLD = 2 // After 2 background failures, go sync
export const LOG_RATE_LIMIT_MS = 1000    // Max 1 compaction check log per second

// Per-session failure tracking for sync fallback
const compactionFailures = new Map<string, number>()
// Log rate limiting: track last log time per session
const lastLogTime = new Map<string, number>()

/** Hard prune: delete oldest N messages without LLM (emergency only) */
export const EMERGENCY_PRUNE_COUNT = Math.floor(0.5 * 20) // ~50% of recent 20 turns

export const Event = {
  Compacted: BusEvent.define(
    "session.compacted",
    z.object({
      sessionID: SessionID.zod,
    }),
  ),
}

export const PRUNE_MINIMUM = 20_000
export const PRUNE_PROTECT = 40_000
const PRUNE_PROTECTED_TOOLS = ["skill"]

export interface Interface {
  readonly isOverflow: (input: {
    tokens: MessageV2.Assistant["tokens"]
    model: Provider.Model
  }) => Effect.Effect<boolean>
  readonly prune: (input: { sessionID: SessionID }) => Effect.Effect<void>
  readonly process: (input: {
    parentID: MessageID
    messages: MessageV2.WithParts[]
    sessionID: SessionID
    auto: boolean
    overflow?: boolean
  }) => Effect.Effect<"continue" | "stop">
  readonly create: (input: {
    sessionID: SessionID
    agent: string
    model: { providerID: ProviderID; modelID: ModelID }
    auto: boolean
    overflow?: boolean
  }) => Effect.Effect<void>
  /** Background compaction: checks threshold, cooldown, and forks compaction
   * as a daemon if conditions are met. Returns true if triggered. */
  readonly background: (input: {
    sessionID: SessionID
    agent: string
    model: { providerID: ProviderID; modelID: ModelID }
    currentTokens: number
    contextWindow: number
  }) => Effect.Effect<boolean>
  /** Sync fallback: runs compaction synchronously if background failed N times */
  readonly syncFallback: (input: {
    sessionID: SessionID
    agent: string
    model: { providerID: ProviderID; modelID: ModelID }
    currentTokens: number
    contextWindow: number
  }) => Effect.Effect<boolean>
  /** Record a compaction failure (for sync fallback tracking) */
  readonly recordCompactionFailure: (sessionID: SessionID) => Effect.Effect<void>
  /** Emergency prune: hard-delete oldest messages when context >100% */
  readonly emergencyPrune: (input: { sessionID: SessionID }) => Effect.Effect<void>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/SessionCompaction") {}

export const layer: Layer.Layer<
  Service,
  never,
  | Bus.Service
  | Config.Service
  | Session.Service
  | Agent.Service
  | Plugin.Service
  | SessionProcessor.Service
  | Provider.Service
> = Layer.effect(
  Service,
  Effect.gen(function* () {
    const bus = yield* Bus.Service
    const config = yield* Config.Service
    const session = yield* Session.Service
    const agents = yield* Agent.Service
    const plugin = yield* Plugin.Service
    const processors = yield* SessionProcessor.Service
    const provider = yield* Provider.Service

    const isOverflow = Effect.fn("SessionCompaction.isOverflow")(function* (input: {
      tokens: MessageV2.Assistant["tokens"]
      model: Provider.Model
    }) {
      return overflow({ cfg: yield* config.get(), tokens: input.tokens, model: input.model })
    })

    // goes backwards through parts until there are PRUNE_PROTECT tokens worth of tool
    // calls, then erases output of older tool calls to free context space
    const prune = Effect.fn("SessionCompaction.prune")(function* (input: { sessionID: SessionID }) {
      const cfg = yield* config.get()
      if (cfg.compaction?.prune === false) return
      log.info("pruning")

      const msgs = yield* session
        .messages({ sessionID: input.sessionID })
        .pipe(Effect.catchIf(NotFoundError.isInstance, () => Effect.succeed(undefined)))
      if (!msgs) return

      let total = 0
      let pruned = 0
      const toPrune: MessageV2.ToolPart[] = []
      let turns = 0

      loop: for (let msgIndex = msgs.length - 1; msgIndex >= 0; msgIndex--) {
        const msg = msgs[msgIndex]
        if (msg.info.role === "user") turns++
        if (turns < 2) continue
        if (msg.info.role === "assistant" && msg.info.summary) break loop
        for (let partIndex = msg.parts.length - 1; partIndex >= 0; partIndex--) {
          const part = msg.parts[partIndex]
          if (part.type === "tool")
            if (part.state.status === "completed") {
              if (PRUNE_PROTECTED_TOOLS.includes(part.tool)) continue
              if (part.state.time.compacted) break loop
              const estimate = Token.estimate(part.state.output)
              total += estimate
              if (total > PRUNE_PROTECT) {
                pruned += estimate
                toPrune.push(part)
              }
            }
        }
      }

      log.info("found", { pruned, total })
      if (pruned > PRUNE_MINIMUM) {
        for (const part of toPrune) {
          if (part.state.status === "completed") {
            part.state.time.compacted = Date.now()
            yield* session.updatePart(part)
          }
        }
        log.info("pruned", { count: toPrune.length })
      }
    })

    const processCompaction = Effect.fn("SessionCompaction.process")(function* (input: {
      parentID: MessageID
      messages: MessageV2.WithParts[]
      sessionID: SessionID
      auto: boolean
      overflow?: boolean
    }) {
      const parent = input.messages.findLast((m) => m.info.id === input.parentID)
      if (!parent || parent.info.role !== "user") {
        throw new Error(`Compaction parent must be a user message: ${input.parentID}`)
      }
      const userMessage = parent.info

      let messages = input.messages
      let replay:
        | {
            info: MessageV2.User
            parts: MessageV2.Part[]
          }
        | undefined
      if (input.overflow) {
        const idx = input.messages.findIndex((m) => m.info.id === input.parentID)
        for (let i = idx - 1; i >= 0; i--) {
          const msg = input.messages[i]
          if (msg.info.role === "user" && !msg.parts.some((p) => p.type === "compaction")) {
            replay = { info: msg.info, parts: msg.parts }
            messages = input.messages.slice(0, i)
            break
          }
        }
        const hasContent =
          replay && messages.some((m) => m.info.role === "user" && !m.parts.some((p) => p.type === "compaction"))
        if (!hasContent) {
          replay = undefined
          messages = input.messages
        }
      }

      const agent = yield* agents.get("compaction")
      const model = agent.model
        ? yield* provider.getModel(agent.model.providerID, agent.model.modelID)
        : yield* provider.getModel(userMessage.model.providerID, userMessage.model.modelID)
      // Allow plugins to inject context or replace compaction prompt.
      const compacting = yield* plugin.trigger(
        "experimental.session.compacting",
        { sessionID: input.sessionID },
        { context: [], prompt: undefined },
      )
      const defaultPrompt = `Provide a detailed prompt for continuing our conversation above.
Focus on information that would be helpful for continuing the conversation, including what we did, what we're doing, which files we're working on, and what we're going to do next.
The summary that you construct will be used so that another agent can read it and continue the work.
Do not call any tools. Respond only with the summary text.
Respond in the same language as the user's messages in the conversation.

When constructing the summary, try to stick to this template:
---
## Goal

[What goal(s) is the user trying to accomplish?]

## Instructions

- [What important instructions did the user give you that are relevant]
- [If there is a plan or spec, include information about it so next agent can continue using it]

## Discoveries

[What notable things were learned during this conversation that would be useful for the next agent to know when continuing the work]

## Accomplished

[What work has been completed, what work is still in progress, and what work is left?]

## Relevant files / directories

[Construct a structured list of relevant files that have been read, edited, or created that pertain to the task at hand. If all the files in a directory are relevant, include the path to the directory.]
---`

      const prompt = compacting.prompt ?? [defaultPrompt, ...compacting.context].join("\n\n")
      const msgs = structuredClone(messages)
      yield* plugin.trigger("experimental.chat.messages.transform", {}, { messages: msgs })
      const modelMessages = yield* MessageV2.toModelMessagesEffect(msgs, model, { stripMedia: true })
      const ctx = yield* InstanceState.context
      const msg: MessageV2.Assistant = {
        id: MessageID.ascending(),
        role: "assistant",
        parentID: input.parentID,
        sessionID: input.sessionID,
        mode: "compaction",
        agent: "compaction",
        variant: userMessage.model.variant,
        summary: true,
        path: {
          cwd: ctx.directory,
          root: ctx.worktree,
        },
        cost: 0,
        tokens: {
          output: 0,
          input: 0,
          reasoning: 0,
          cache: { read: 0, write: 0 },
        },
        modelID: model.id,
        providerID: model.providerID,
        time: {
          created: Date.now(),
        },
      }
      yield* session.updateMessage(msg)
      const processor = yield* processors.create({
        assistantMessage: msg,
        sessionID: input.sessionID,
        model,
      })
      const result = yield* processor.process({
        user: userMessage,
        agent,
        sessionID: input.sessionID,
        tools: {},
        system: [],
        messages: [
          ...modelMessages,
          {
            role: "user",
            content: [{ type: "text", text: prompt }],
          },
        ],
        model,
      })

      if (result === "compact") {
        processor.message.error = new MessageV2.ContextOverflowError({
          message: replay
            ? "Conversation history too large to compact - exceeds model context limit"
            : "Session too large to compact - context exceeds model limit even after stripping media",
        }).toObject()
        processor.message.finish = "error"
        yield* session.updateMessage(processor.message)
        return "stop"
      }

      if (result === "continue" && input.auto) {
        if (replay) {
          const original = replay.info
          const replayMsg = yield* session.updateMessage({
            id: MessageID.ascending(),
            role: "user",
            sessionID: input.sessionID,
            time: { created: Date.now() },
            agent: original.agent,
            model: original.model,
            format: original.format,
            tools: original.tools,
            system: original.system,
          })
          for (const part of replay.parts) {
            if (part.type === "compaction") continue
            const replayPart =
              part.type === "file" && MessageV2.isMedia(part.mime)
                ? { type: "text" as const, text: `[Attached ${part.mime}: ${part.filename ?? "file"}]` }
                : part
            yield* session.updatePart({
              ...replayPart,
              id: PartID.ascending(),
              messageID: replayMsg.id,
              sessionID: input.sessionID,
            })
          }
        }

        if (!replay) {
          const info = yield* provider.getProvider(userMessage.model.providerID)
          if (
            (yield* plugin.trigger(
              "experimental.compaction.autocontinue",
              {
                sessionID: input.sessionID,
                agent: userMessage.agent,
                model: yield* provider.getModel(userMessage.model.providerID, userMessage.model.modelID),
                provider: {
                  source: info.source,
                  info,
                  options: info.options,
                },
                message: userMessage,
                overflow: input.overflow === true,
              },
              { enabled: true },
            )).enabled
          ) {
            const continueMsg = yield* session.updateMessage({
              id: MessageID.ascending(),
              role: "user",
              sessionID: input.sessionID,
              time: { created: Date.now() },
              agent: userMessage.agent,
              model: userMessage.model,
            })
            const text =
              (input.overflow
                ? "The previous request exceeded the provider's size limit due to large media attachments. The conversation was compacted and media files were removed from context. If the user was asking about attached images or files, explain that the attachments were too large to process and suggest they try again with smaller or fewer files.\n\n"
                : "") +
              "Continue if you have next steps, or stop and ask for clarification if you are unsure how to proceed."
            yield* session.updatePart({
              id: PartID.ascending(),
              messageID: continueMsg.id,
              sessionID: input.sessionID,
              type: "text",
              // Internal marker for auto-compaction followups so provider plugins
              // can distinguish them from manual post-compaction user prompts.
              // This is not a stable plugin contract and may change or disappear.
              metadata: { compaction_continue: true },
              synthetic: true,
              text,
              time: {
                start: Date.now(),
                end: Date.now(),
              },
            })
          }
        }
      }

      if (processor.message.error) return "stop"
      if (result === "continue") yield* bus.publish(Event.Compacted, { sessionID: input.sessionID })
      return result
    })

    const create = Effect.fn("SessionCompaction.create")(function* (input: {
      sessionID: SessionID
      agent: string
      model: { providerID: ProviderID; modelID: ModelID }
      auto: boolean
      overflow?: boolean
    }) {
      const msg = yield* session.updateMessage({
        id: MessageID.ascending(),
        role: "user",
        model: input.model,
        sessionID: input.sessionID,
        agent: input.agent,
        time: { created: Date.now() },
      })
      yield* session.updatePart({
        id: PartID.ascending(),
        messageID: msg.id,
        sessionID: msg.sessionID,
        type: "compaction",
        auto: input.auto,
        overflow: input.overflow,
      })
    })

    // Per-session cooldown tracking for background compaction
    const cooldowns = new Map<string, number>()
    const DEFAULT_COOLDOWN_MS = 5 * 60 * 1000 // 5 minutes

    function parseCooldownMs(raw: string | undefined): number {
      if (!raw) return DEFAULT_COOLDOWN_MS
      const match = raw.match(/^(\d+)(ms|s|m|h)?$/)
      if (!match) return DEFAULT_COOLDOWN_MS
      const value = Number(match[1])
      const unit = match[2] || "ms"
      switch (unit) {
        case "s": return value * 1000
        case "m": return value * 60 * 1000
        case "h": return value * 60 * 60 * 1000
        default: return value
      }
    }

    const rateLimitedLog = (sessionId: string, msg: string, data?: Record<string, unknown>) => {
      const now = Date.now()
      const last = lastLogTime.get(sessionId) ?? 0
      if (now - last < LOG_RATE_LIMIT_MS) return // Skip if within rate limit
      lastLogTime.set(sessionId, now)
      log.info(msg, data)
    }

    const background = Effect.fn("SessionCompaction.background")(function* (input: {
      sessionID: SessionID
      agent: string
      model: { providerID: ProviderID; modelID: ModelID }
      currentTokens: number
      contextWindow: number
    }) {
      const cfg = yield* Config.Service.use((svc) => svc.get())
      const mode = cfg.compaction?.mode
      const auto = cfg.compaction?.auto
      const threshold = cfg.compaction?.threshold ?? 0.70
      const ratio = input.currentTokens / input.contextWindow

      // EMERGENCY PRUNE: At 100%+, hard-delete oldest messages without LLM
      if (ratio >= EMERGENCY_PRUNE_RATIO) {
        rateLimitedLog(input.sessionID, "EMERGENCY: context >100%, hard pruning oldest messages", {
          ratio: Math.round(ratio * 100) + "%",
          tokens: input.currentTokens,
          contextWindow: input.contextWindow,
        })
        yield* emergencyPrune({ sessionID: input.sessionID })
        return true
      }

      // HARD CAP: At 95%, reject new messages (caller should check this)
      // We log a warning but don't block — the prompt.ts layer handles rejection
      if (ratio >= HARD_CAP_RATIO) {
        rateLimitedLog(input.sessionID, "WARNING: context at hard cap threshold (95%)", {
          ratio: Math.round(ratio * 100) + "%",
          tokens: input.currentTokens,
        })
      }

      rateLimitedLog(input.sessionID, "background compaction check", {
        mode,
        auto,
        threshold,
        ratio: Math.round(ratio * 100) + "%",
        currentTokens: input.currentTokens,
        contextWindow: input.contextWindow,
      })

      if (mode !== "background") {
        rateLimitedLog(input.sessionID, "background compaction skipped: mode mismatch", { mode })
        return false
      }
      if (auto === false) {
        rateLimitedLog(input.sessionID, "background compaction skipped: auto=false")
        return false
      }
      if (ratio < threshold) {
        rateLimitedLog(input.sessionID, "background compaction skipped: below threshold", { ratio, threshold })
        return false
      }

      const cooldownMs = parseCooldownMs(cfg.compaction?.cooldown)
      const lastRun = cooldowns.get(input.sessionID) ?? 0
      if (Date.now() - lastRun < cooldownMs) {
        rateLimitedLog(input.sessionID, "background compaction skipped: cooldown active")
        return false
      }

      cooldowns.set(input.sessionID, Date.now())

      rateLimitedLog(input.sessionID, "background compaction triggered", {
        ratio: Math.round(ratio * 100) + "%",
        threshold: Math.round(threshold * 100) + "%",
        tokens: input.currentTokens,
        contextWindow: input.contextWindow,
      })

      yield* Effect.gen(function* () {
        yield* create({
          sessionID: input.sessionID,
          agent: input.agent,
          model: input.model,
          auto: true,
        })
      }).pipe(Effect.forkIn(yield* Scope.Scope))

      return true
    })

    // Sync fallback: if background compaction fails N times, run it synchronously
    const syncFallback = Effect.fn("SessionCompaction.syncFallback")(function* (input: {
      sessionID: SessionID
      agent: string
      model: { providerID: ProviderID; modelID: ModelID }
      currentTokens: number
      contextWindow: number
    }) {
      const failures = compactionFailures.get(input.sessionID) ?? 0
      if (failures < SYNC_FALLBACK_THRESHOLD) return false

      rateLimitedLog(input.sessionID, "SYNC FALLBACK: background failed " + failures + "x, running synchronously", {
        tokens: input.currentTokens,
        contextWindow: input.contextWindow,
      })

      yield* create({
        sessionID: input.sessionID,
        agent: input.agent,
        model: input.model,
        auto: true,
      })

      compactionFailures.set(input.sessionID, 0) // Reset on success
      return true
    })

    // Increment failure counter when compaction doesn't reduce tokens
    const recordCompactionFailure = Effect.fn("SessionCompaction.recordFailure")(function* (sessionID: SessionID) {
      const failures = compactionFailures.get(sessionID) ?? 0
      compactionFailures.set(sessionID, failures + 1)
      rateLimitedLog(sessionID, "Compaction failure recorded", { failures: failures + 1 })
    })

    // Hard-delete oldest messages without LLM (emergency memory reclaim)
    const emergencyPrune = Effect.fn("SessionCompaction.emergencyPrune")(function* (input: { sessionID: SessionID }) {
      const msgs = yield* session
        .messages({ sessionID: input.sessionID })
        .pipe(Effect.catchIf(NotFoundError.isInstance, () => Effect.succeed(undefined)))
      if (!msgs || msgs.length < 4) {
        log.info("emergency prune: too few messages to prune")
        return
      }

      // Keep the last 2 user messages + their responses, delete everything before
      let keepFrom = msgs.length - 1
      let userMessagesSeen = 0
      for (let i = msgs.length - 1; i >= 0; i--) {
        if (msgs[i].info.role === "user") {
          userMessagesSeen++
          if (userMessagesSeen >= 2) {
            keepFrom = i
            break
          }
        }
        keepFrom = i
      }

      const toDelete = msgs.slice(0, keepFrom)
      log.info("emergency prune: deleting " + toDelete.length + " oldest messages (kept " + (msgs.length - keepFrom) + " recent)")

      for (const msg of toDelete) {
        yield* session.removeMessage({ sessionID: input.sessionID, messageID: msg.info.id })
      }
    })

    return Service.of({
      isOverflow,
      prune,
      process: processCompaction,
      create,
      background,
      emergencyPrune,
      syncFallback,
      recordCompactionFailure,
    })
  }),
)

export const defaultLayer = Layer.suspend(() =>
  layer.pipe(
    Layer.provide(Provider.defaultLayer),
    Layer.provide(Session.defaultLayer),
    Layer.provide(SessionProcessor.defaultLayer),
    Layer.provide(Agent.defaultLayer),
    Layer.provide(Plugin.defaultLayer),
    Layer.provide(Bus.layer),
    Layer.provide(Config.defaultLayer),
  ),
)

export * as SessionCompaction from "./compaction"
