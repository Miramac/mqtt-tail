# mqtt-tail

[![npm version](https://img.shields.io/npm/v/mqtt-tail)](https://www.npmjs.com/package/mqtt-tail)
[![Node.js >= 18](https://img.shields.io/badge/node-%3E%3D18-brightgreen)](https://nodejs.org)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

`tail -f` for MQTT. Watch topics stream live in your terminal.

```
16:42:03.112 │ sensors/temperature
{
  "value": 23.5,
  "unit": "C",
  "sensor": "living-room"
}

16:42:04.891 │ sensors/humidity
{
  "value": 61,
  "unit": "%"
}
```

## Install

```bash
# Run without installing
npx mqtt-tail

# Or install globally
npm install -g mqtt-tail
```

## Usage

```
mqtt-tail [options] [topics...]
```

Topics default to `#` (all) when omitted. MQTT wildcards are fully supported:
- `+` matches a single level: `sensors/+/temp`
- `#` matches multiple levels: `sensors/#`

## Examples

```bash
# Watch everything on localhost
mqtt-tail

# Single topic
mqtt-tail sensors/temperature

# Multiple topics with wildcards
mqtt-tail "sensors/+" "control/#"

# Remote broker with TLS and auth
mqtt-tail -H mqtt.example.com -p 8883 --tls -u alice -P secret "sensors/#"

# Filter by topic regex
mqtt-tail -f "temperature|humidity" "#"

# Filter by payload content
mqtt-tail --payload-filter "error" "logs/#"

# Exit after 20 messages
mqtt-tail -n 20 "#"

# One line per message (good for grepping)
mqtt-tail --compact "#"

# Pipe to jq
mqtt-tail --output-json "#" | jq '.payload'
mqtt-tail --output-json "sensors/#" | jq 'select(.topic | test("temp")) | .payload.value'

# Skip retained messages, show metadata
mqtt-tail --no-retained --verbose "#"

# CI / scripting (no colors, no timestamps)
mqtt-tail --no-color --no-timestamp --raw "#"
```

## Options

### Connection

| Flag | Description | Default |
|------|-------------|---------|
| `-H, --host <host>` | Broker host | `localhost` |
| `-p, --port <port>` | Broker port | `1883` (`8883` with `--tls`) |
| `-u, --username <user>` | Username | |
| `-P, --password <pass>` | Password | |
| `--tls` | Use TLS/SSL (`mqtts://`) | |
| `--ca <file>` | CA certificate file | |
| `--cert <file>` | Client certificate file | |
| `--key <file>` | Client key file | |
| `--client-id <id>` | MQTT client ID | random |
| `-q, --qos <level>` | Subscription QoS (0\|1\|2) | `0` |

### Filtering

| Flag | Description |
|------|-------------|
| `-n, --count <n>` | Exit after n messages |
| `-f, --filter <regex>` | Only show topics matching regex |
| `--payload-filter <regex>` | Only show messages whose payload matches regex |
| `--no-retained` | Ignore retained messages |

### Output

| Flag | Description |
|------|-------------|
| `--compact` | One line per message |
| `--output-json` | Newline-delimited JSON (for piping) |
| `--raw` | Raw payload, no formatting |
| `--no-timestamp` | Hide timestamps |
| `--timestamp-format <fmt>` | `local` (default) \| `iso` \| `unix` \| `unixms` |
| `--no-color` | Disable colors |
| `-v, --verbose` | Show connection info and per-message QoS/size/retain |

### Misc

| Flag | Description |
|------|-------------|
| `--config <file>` | Path to config file (default: `~/.mqtttailrc.json`) |

## Output formats

**Default** — pretty-printed, syntax-highlighted JSON, colored topics:
```
16:42:03.112 │ sensors/temperature
{
  "value": 23.5,
  "unit": "C"
}
```

**`--compact`** — one line per message:
```
16:42:03.112 │ sensors/temperature  {"value":23.5,"unit":"C"}
```

**`--output-json`** — newline-delimited JSON for scripting:
```json
{"timestamp":"2024-01-15T16:42:03.112Z","topic":"sensors/temperature","payload":{"value":23.5},"qos":0,"retain":false,"size":28}
```

**`--raw`** — no formatting:
```
sensors/temperature {"value":23.5,"unit":"C"}
```

## Configuration

Options are resolved in this order (highest priority first):

1. **CLI flags**
2. **`process.env`** — shell environment variables
3. **`.env` file** — in the current working directory
4. **`~/.mqtttailrc.json`** — global user config file

This means you can configure a broker once and forget about it, while still being able to override any value inline.

### First-run setup wizard

On first run (no broker config found), `mqtt-tail` starts an interactive setup wizard that asks for your broker host, port, credentials, and TLS settings, then saves them to `~/.mqtttailrc.json`. Skip it by passing connection flags directly or by pre-creating the config file.

### With `npx`

Config is read from your home directory and environment, not from the npx temp cache, so it works transparently with `npx`:

```bash
# One-time setup
echo '{"host":"mqtt.example.com","username":"alice","password":"secret"}' > ~/.mqtttailrc.json

# From now on, just run
npx mqtt-tail sensors/#
```

### `.env` file

Place a `.env` in your current directory for project-specific settings:

```dotenv
MQTT_HOST=mqtt.example.com
MQTT_PORT=8883
MQTT_TLS=true
MQTT_USERNAME=alice
MQTT_PASSWORD=secret
```

Then run from that directory:

```bash
npx mqtt-tail sensors/#
```

Add `.env` to your `.gitignore` to keep credentials out of version control.

### Environment variables

Useful for CI/CD, Docker, or one-off overrides:

```bash
MQTT_HOST=broker.example.com MQTT_USERNAME=alice MQTT_PASSWORD=secret npx mqtt-tail "#"
```

| Variable | Option |
|----------|--------|
| `MQTT_HOST` | `--host` |
| `MQTT_PORT` | `--port` |
| `MQTT_USERNAME` | `--username` |
| `MQTT_PASSWORD` | `--password` |
| `MQTT_TLS` | `--tls` |
| `MQTT_CLIENT_ID` | `--client-id` |
| `MQTT_CA` | `--ca` |
| `MQTT_CERT` | `--cert` |
| `MQTT_KEY` | `--key` |

### Global config file

`mqtt-tail` searches the following locations in order:

1. `~/.mqtttailrc.json`
2. `~/.mqtttailrc`
3. `.mqtttailrc.json` in the current directory

Override the path with `--config <file>`.

```json
{
  "host": "mqtt.example.com",
  "port": 8883,
  "tls": true,
  "username": "alice",
  "password": "secret"
}
```

## Requirements

- Node.js >= 18

## License

MIT

---

> Built with [Claude Code](https://claude.ai/claude-code).
