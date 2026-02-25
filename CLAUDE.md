# CLAUDE.md — AI Assistant Guide for node-mqtt-tail

## Project Overview

`mqtt-tail` is a Node.js CLI tool that monitors MQTT topics in real-time, similar to how `tail -f` monitors log files. Published to npm as `mqtt-tail`, it supports flexible configuration, rich terminal output, and multiple output formats for scripting/piping.

```
mqtt-tail "sensors/#" "home/+/temperature"
```

---

## Repository Structure

```
node-mqtt-tail/
├── src/
│   ├── index.js        # CLI entry point — Commander.js argument parsing & startup
│   ├── config.js       # Multi-source config loading (files, env vars, .env)
│   ├── setup.js        # Interactive first-run setup wizard
│   ├── formatter.js    # Output formatting, colorization, and JSON syntax highlighting
│   └── subscriber.js   # MQTT connection, subscription, message filtering
├── package.json        # Project metadata, dependencies, bin registration
├── package-lock.json   # Locked dependency versions
├── README.md           # End-user documentation
└── .gitignore          # Ignores: node_modules/, .env, *.log
```

### Module Responsibilities

| File | Responsibility |
|------|----------------|
| `src/index.js` | Parse CLI args (Commander.js), merge config sources, call `runSetupIfNeeded()`, then `connect()` |
| `src/config.js` | Load and merge config from `~/.mqtttailrc.json`, `.env`, and environment variables |
| `src/setup.js` | Interactive wizard that writes initial config if none exists |
| `src/formatter.js` | Format MQTT messages: timestamps, JSON syntax highlighting, topic coloring, output modes |
| `src/subscriber.js` | Build MQTT connect options, manage broker connection lifecycle, filter and display messages |

---

## Key Technical Details

### Runtime Requirements
- **Node.js >= 18.0.0** (required for built-in `fetch`, modern ESM, `crypto.hash`, etc.)
- **ES Modules only** — the package sets `"type": "module"`. All files use `import`/`export`. Do not use `require()`.
- No build step — source files are run directly with `node`.

### Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| `mqtt` | ^5.10.0 | MQTT client (mqtt.js) |
| `commander` | ^12.1.0 | CLI argument parsing |
| `chalk` | ^5.3.0 | Terminal colors/styles |
| `@inquirer/prompts` | ^8.3.0 | Interactive setup prompts |

All are ESM-compatible. Chalk v5+ and mqtt v5+ are ESM-only — do not downgrade.

---

## Configuration System

Configuration is resolved with this priority (highest wins):

1. **CLI flags** (e.g., `--host`, `--port`, `--tls`)
2. **Shell environment variables** (`MQTT_HOST`, `MQTT_PORT`, `MQTT_USERNAME`, `MQTT_PASSWORD`, `MQTT_TLS`)
3. **Project `.env` file** (parsed manually — not via dotenv package)
4. **Global user config** (`~/.mqtttailrc.json` or `~/.mqtttailrc`)

### Config Key → Env Var Mapping (`src/config.js`)

```
host     → MQTT_HOST
port     → MQTT_PORT
username → MQTT_USERNAME
password → MQTT_PASSWORD
tls      → MQTT_TLS
```

### CLI Flag Handling in `src/index.js`

Commander.js assigns default values to options. To avoid defaults overwriting higher-priority config (env/file), the code uses `program.getOptionValueSource(key)` to detect whether a flag was explicitly passed:

```js
if (program.getOptionValueSource('port') === 'cli') {
  opts.port = program.opts().port;
}
```

Never use `program.opts()` directly to populate config — always check the source.

---

## Code Conventions

### Style
- **ES module imports** at the top of each file — no dynamic `require()`
- **async/await** for all async operations (no callback patterns)
- **Guard clauses** for early returns rather than deep nesting
- **Object destructuring** for function options and config objects
- Section dividers: `// --- SECTION NAME ---` for visual separation in longer files

### Error Handling
- Config loading always returns an empty object (`{}`) on failure — never throws to the caller
- Regex compilation in `subscriber.js` catches invalid patterns and exits with a user-friendly message
- MQTT connection errors are printed to **stderr** (not stdout) so they don't corrupt piped output
- Fatal errors use `process.exit(1)`

### Output Separation
- **stdout**: MQTT message output only
- **stderr**: Connection status, errors, debug/verbose logs

This is critical — piping `mqtt-tail` to `jq` or other tools must only receive message output on stdout.

---

## Output Formats

Controlled by `--format` flag or the shorthand flags `--compact`, `--jsonl`, `--raw`:

| Format | Flag | Description |
|--------|------|-------------|
| `pretty` | (default) | Colorized, formatted multi-line output with timestamp and topic header |
| `compact` | `--compact` | Single line per message |
| `jsonl` | `--jsonl` | Newline-delimited JSON (for piping to `jq`) |
| `raw` | `--raw` | Unformatted payload bytes only |

---

## Topic Coloring

`formatter.js` assigns a deterministic color to each topic using a `Math.imul()`-based hash:

```js
function hashString(str) { ... }  // Returns a stable integer for the same input
const color = TOPIC_COLORS[Math.abs(hash) % TOPIC_COLORS.length];
```

This means the same topic always gets the same color across runs. Do not replace this with `Math.random()`.

---

## JSON Syntax Highlighting

`formatter.js` includes a custom character-by-character JSON tokenizer (in `colorizeJson()`) rather than regex-based coloring. This correctly handles:
- Escaped characters inside strings
- Distinguishing object keys from string values (uses lookahead for `:`)
- Nested structures

Do not replace this with a regex approach — it will break on edge cases.

---

## MQTT Connection Lifecycle (`src/subscriber.js`)

The `connect()` function manages:

1. Build broker URL (`mqtt://` or `mqtts://`) and connection options
2. Load TLS certificates from disk if `--ca`, `--cert`, `--key` are provided
3. Call `mqtt.connect()` and register event listeners:
   - `connect` — log connection, subscribe to topics
   - `message` — apply regex filters, format, print to stdout
   - `error` — print to stderr
   - `offline` / `reconnect` — track reconnection state
   - `close` / `disconnect` — log disconnect
4. Handle `SIGINT`/`SIGTERM` for graceful shutdown

Reconnect interval is hardcoded at **2000ms** (`reconnectPeriod: 2000`).

### State Variables (module-level in `subscriber.js`)

```js
let messageCount = 0;      // Count of received messages
let everConnected = false;  // Whether we've ever connected (for error display)
let offlineShown = false;   // Avoid duplicate "offline" log lines
let reconnectCount = 0;     // Number of reconnect attempts
```

These are intentionally module-level (not passed as arguments) — this is a CLI tool with a single connection lifecycle.

---

## Development Workflow

### Installation
```bash
git clone <repo>
cd node-mqtt-tail
npm install
```

### Running Locally
```bash
node src/index.js --help
node src/index.js -h localhost "test/#"
# or
npm start -- -h localhost "test/#"
```

### No Build Step
There is no compilation, bundling, or transpilation. Edit source files and run directly.

### No Test Suite
There are currently no automated tests. Manual testing is done by connecting to a live or local MQTT broker (e.g., Mosquitto).

### Publishing
```bash
npm publish
```
The `prepublishOnly` script runs `node src/index.js --version` as a basic sanity check.

---

## Common Tasks for AI Assistants

### Adding a New CLI Option
1. Add the option in `src/index.js` using `program.option(...)`.
2. Add the option to the config merge block, guarding with `getOptionValueSource()` if it has a default.
3. Pass it through to `connect()` in `src/subscriber.js` via the `opts` object.
4. Use it in `subscriber.js` or `formatter.js` as needed.

### Adding a New Output Format
1. Add a new case to the `switch (format)` block in `formatMessage()` in `src/formatter.js`.
2. Add the format name to `--format` option's allowed values in `src/index.js`.
3. Optionally add a shorthand flag (like `--compact`, `--jsonl`, `--raw`).

### Adding a New Config Source
1. Add a new reader function in `src/config.js` following the pattern of `readDotEnv()`.
2. Integrate it into `loadConfig()` with the correct priority position.

### Adding TLS Options
New cert/key options follow the pattern in `subscriber.js` — read the file path from opts, load with `readFile()`, and add to `connectOpts.options`.

---

## What to Avoid

- **Do not use `require()`** — this is a pure ESM project.
- **Do not write to stdout from `subscriber.js` for non-message output** — use `process.stderr.write()` for status/debug.
- **Do not use `console.log()` in `src/subscriber.js` or `src/formatter.js`** for connection status — it goes to stdout and corrupts piped output. Use `process.stderr.write()`.
- **Do not use `Math.random()` for topic coloring** — coloring must be deterministic per topic name.
- **Do not break the config priority chain** — CLI > env > .env > file is a documented guarantee.
- **Do not add `devDependencies` build tools** unless introducing a test framework — this project intentionally has no build pipeline.
- **Do not downgrade chalk below v5** or mqtt below v5 — both v5+ are ESM-only and the project depends on this.
