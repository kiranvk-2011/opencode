import type { Config } from "@/config"
import type { Provider } from "@/provider"
import { ProviderTransform } from "@/provider"
import type { MessageV2 } from "./message-v2"
import { Log } from "../util"

const COMPACTION_BUFFER = 20_000
const log = Log.create({ service: "session.overflow" })

export function usable(input: { cfg: Config.Info; model: Provider.Model }) {
  const context = input.model.limit.context
  if (context === 0) return 0

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
