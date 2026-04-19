# OpenCode Config Processing Path — Complete Trace

## Entry Point

`opencode` command → `/usr/lib/node_modules/opencode-ai/bin/opencode` (4.5KB Node wrapper)
→ Spawns `/usr/lib/node_modules/opencode-ai/bin/.opencode` (173MB Bun-compiled binary)

## Version: 1.14.18-copilot-fix
## Branch: fix/background-compaction (commit 74c6ac17)

---

## Step 1: Binary Startup

Binary is a single-file Bun bundle. All TS source compiled into one JS blob.
The entry point is the main CLI handler in `packages/opencode/src/cli/`.

When invoked, the binary:
1. Initializes Bun runtime
2. Loads the main module (bundled index)
3. CLI argument parsing happens
4. Creates a server-proxy instance

## Step 2: Server-Proxy Boot (log line: `service=server-proxy version=1.14.18-copilot-fix`)

From the `--print-logs` output:
```
INFO  service=server-proxy version=1.14.18-copilot-fix args=["run","test","--print-logs"]
INFO  service=server-proxy directory=/root/openclaw_master creating instance
```

This creates an Instance for the project directory `/root/openclaw_master`.

## Step 3: Project Resolution (log: `service=project directory=/root/openclaw_master fromDirectory`)

Resolves the project directory and initializes services.

## Step 4: Database Init (log: `service=db opening database`)

Opens SQLite at `/root/.local/share/opencode/opencode.db`, applies migrations.

## Step 5: Config Layer Construction

**File:** `packages/opencode/src/config/config.ts`

### 5a: Schema Definition (Lines 94-239)

The `InfoSchema` is defined as `Schema.Struct({...})` with all config fields.
For compaction (lines 199-220):
```ts
compaction: Schema.optional(
  Schema.Struct({
    auto: Schema.optional(Schema.Boolean),
    prune: Schema.optional(Schema.Boolean),
    reserved: Schema.optional(NonNegativeInt),
    mode: Schema.optional(Schema.Literal("sync", "background")),  // <-- THIS
    threshold: Schema.optional(Schema.Number.check(...)),
    cooldown: Schema.optional(Schema.String),
  }),
),
```

### 5b: Zod Derivation (Lines 265-267)

```ts
export const Info = (zod(InfoSchema) as unknown as z.ZodObject<any>)
  .strict()
  .meta({ ref: "Config" })
```

The `zod()` function (from `effect-zod.ts`) walks the Effect Schema AST:
- `Schema.Literal("sync", "background")` → AST node with `_tag: "Union"` containing two `Literal` children
- `union()` in effect-zod.ts (line 303-319) detects all string literals → calls `z.enum(["sync", "background"])`
- BUT: This requires `ast.types.every((t) => t._tag === "Literal" && typeof t.literal === "string")`

### 5c: `.strict()` Application

After `zod(InfoSchema)` produces a Zod object, `.strict()` is applied.
**IMPORTANT:** `.strict()` means `additionalProperties: false`. Extra fields are rejected.

## Step 6: Global Config Loading (Lines 385-420)

```ts
const loadGlobal = Effect.fnUntraced(function* () {
  let result: Info = pipe(
    {},
    mergeDeep(yield* loadFile(path.join(Global.Path.config, "config.json"))),
    mergeDeep(yield* loadFile(path.join(Global.Path.config, "opencode.json"))),
    mergeDeep(yield* loadFile(path.join(Global.Path.config, "opencode.jsonc"))),
  )
  ...
})
```

Load order:
1. `config.json` (doesn't exist → `{}`)
2. `opencode.json` (EXISTS at `/root/.config/opencode/opencode.json`)
3. `opencode.jsonc` (doesn't exist → `{}`)

Each file goes through `loadFile()` → `loadConfig()`:

```ts
const loadConfig = Effect.fnUntraced(function* (text, options) {
  const expanded = yield* ConfigVariable.substitute(...)  // ${env} substitution
  const parsed = ConfigParse.jsonc(expanded, source)      // JSONC parse
  const data = ConfigParse.schema(Info, normalizeLoadedConfig(parsed, source), source)  // <-- VALIDATION
  ...
})
```

## Step 7: parse.ts — Schema Validation (Lines 36-44)

```ts
export function schema<T>(schema: Schema<T>, data: unknown, source: string): T {
  const parsed = schema.safeParse(data)  // Zod validation
  if (parsed.success) return parsed.data
  throw new InvalidError({
    path: source,
    issues: parsed.error.issues,
  })
}
```

The `Info` zod schema validates the parsed JSON against the derived structure.

## Step 8: The Bug Location

The error: `expected "sync" compaction.mode` with `values: ["sync"]`

This means the Zod schema for `compaction.mode` is `z.enum(["sync"])` — NOT `z.enum(["sync", "background"])`.

**WHY?** The Effect Schema AST for `Schema.Literal("sync", "background")` might NOT be producing a Union of two Literal nodes. It might be producing a single Literal or a different structure.

Looking at Effect Schema source:
- `Schema.Literal("sync", "background")` calls `Literal<L>` with `L` = `"sync" | "background"` 
- But `Literal` function takes ONE literal: `export function Literal<L extends AST.LiteralValue>(literal: L)`
- `Schema.Literal("sync", "background")` is calling `Literal` with variadic args

Wait — `Schema.Literal` in Effect Schema v4 takes variadic args. Let me check:
- The Effect source shows: `export function Literal<L extends AST.LiteralValue>(literal: L)` — takes ONE arg
- But in config.ts line 210: `Schema.Literal("sync", "background")` — passes TWO args

This means the second argument `"background"` is being IGNORED! The AST only contains `"sync"`.

**ROOT CAUSE FOUND:** `Schema.Literal("sync", "background")` only uses the first argument. The correct API is `Schema.Literals(["sync", "background"])` (with an 's') or `Schema.Union([Schema.Literal("sync"), Schema.Literal("background")])`.

The Effect Schema `Literal` function signature is:
```ts
export function Literal<L extends AST.LiteralValue>(literal: L): Literal<L>
```
It accepts a SINGLE literal value, not multiple.

So `Schema.Literal("sync", "background")` → AST contains only `Literal("sync")`.
The second arg `"background"` is silently ignored by TypeScript/JS.

## Step 9: Instance State Loading (Lines 444-696)

After global config loads successfully, the instance state merges:
1. Well-known remote configs
2. Global config (from Step 6)
3. OPENCODE_CONFIG env var
4. Project config files
5. .opencode directory configs
6. Account/org configs
7. Managed preferences

All go through the same `loadConfig()` → `ConfigParse.schema(Info, ...)` pipeline.

## Step 10: Final Config Assembly

After all sources merge:
- `result.compaction` contains merged compaction settings
- `result.agent` / `result.mode` handling (line 648-655)
- Permission normalization (lines 657-672)
- Username defaulting (line 674)
- Share migration (lines 676-678)
- Compaction flag overrides (lines 680-685)

## Step 11: State Creation and Service Return

```ts
const state = yield* InstanceState.make<State>(...)
return Service.of({ get, getGlobal, getConsoleState, update, updateGlobal, invalidate, directories, waitForDependencies })
```

## Step 12: Error Propagation

When `loadConfig` throws `InvalidError`, it bubbles up through:
1. `cachedGlobal` → `Effect.cachedInvalidateWithTTL`
2. `Effect.tapError` → logs the error
3. `Effect.orElseSucceed(() => ({}))` → swallows error, returns empty config
4. But for instance-level loading, the error is NOT swallowed → crashes

Wait — the global config loading has `Effect.orElseSucceed` which swallows errors. But the instance-level loading at line 703 uses `Effect.orDie` which propagates as a fatal error.

The error path:
1. `loadFile` → `loadConfig` → `ConfigParse.schema(Info, ...)` → Zod `.safeParse()` fails
2. `ConfigParse.schema` throws `InvalidError`
3. In instance loading, `Effect.orDie` makes it fatal
4. Error handler in `cli/error.ts` formats it:
   ```
   Configuration is invalid at /root/.config/opencode/opencode.json
   ↳ Invalid input: expected "sync" compaction.mode
   ```
