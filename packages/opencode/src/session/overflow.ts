import type { Config } from "@/config/config"
import type { Provider } from "@/provider/provider"
import { ProviderTransform } from "@/provider/transform"
import type { MessageV2 } from "./message-v2"
import { Log } from "../util"

const COMPACTION_BUFFER = 20_000
const log = Log.create({ service: "session.overflow" })

export function isOverflow(input: { cfg: Config.Info; tokens: MessageV2.Assistant["tokens"]; model: Provider.Model }) {
  if (input.cfg.compaction?.auto === false) return false
  const context = input.model.limit.context
  if (context === 0) return false

  const count =
    input.tokens.total || input.tokens.input + input.tokens.output + input.tokens.cache.read + input.tokens.cache.write

  const reserved =
    input.cfg.compaction?.reserved ?? Math.min(COMPACTION_BUFFER, ProviderTransform.maxOutputTokens(input.model))
  const usable = input.model.limit.input
    ? input.model.limit.input - reserved
    : context - ProviderTransform.maxOutputTokens(input.model)

  const wouldOverflow = count >= usable
  log.info("overflow check", {
    count,
    usable,
    reserved,
    limitInput: input.model.limit.input,
    limitContext: input.model.limit.context,
    maxOutput: ProviderTransform.maxOutputTokens(input.model),
    wouldOverflow,
    ratio: Math.round((count / usable) * 100) + "%",
  })
  return wouldOverflow
}
