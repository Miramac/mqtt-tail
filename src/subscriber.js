import { readFile } from 'fs/promises'
import { connect as mqttConnect } from 'mqtt'
import chalk from 'chalk'
import { formatMessage, colorTopic } from './formatter.js'

// --- Debug logger ------------------------------------------------------------

function dbg(opts, msg) {
  if (opts.verbose) {
    process.stderr.write(chalk.dim(`[debug] ${msg}\n`))
  }
}

// --- Builders ----------------------------------------------------------------

export async function buildConnectOptions(opts) {
  const connectOpts = {
    clientId: opts.clientId || `mqtt-tail-${Math.random().toString(16).slice(2, 8)}`,
    clean: true,
    reconnectPeriod: 2000,
    connectTimeout: 10_000,
  }

  if (opts.username) connectOpts.username = opts.username
  if (opts.password) connectOpts.password = opts.password

  if (opts.tls || opts.ca || opts.cert || opts.key) {
    connectOpts.rejectUnauthorized = true
    if (opts.ca)   connectOpts.ca   = await readFile(opts.ca)
    if (opts.cert) connectOpts.cert = await readFile(opts.cert)
    if (opts.key)  connectOpts.key  = await readFile(opts.key)
  }

  return connectOpts
}

export function buildBrokerUrl(opts) {
  const protocol = opts.tls || opts.ca || opts.cert || opts.key ? 'mqtts' : 'mqtt'
  const host = opts.host || 'localhost'
  const port = opts.port || (protocol === 'mqtts' ? 8883 : 1883)
  return `${protocol}://${host}:${port}`
}

export function compileFilter(pattern, label) {
  if (!pattern) return null
  try {
    return new RegExp(pattern)
  } catch (err) {
    process.stderr.write(chalk.red(`Invalid ${label} regex "${pattern}": ${err.message}\n`))
    process.exit(1)
  }
}

// --- Main --------------------------------------------------------------------

export async function connect(topics, opts) {
  dbg(opts, 'building broker URL')
  const brokerUrl = buildBrokerUrl(opts)

  dbg(opts, 'building connect options')
  const connectOpts = await buildConnectOptions(opts)

  const qos = parseInt(opts.qos ?? 0, 10)
  const topicFilter   = compileFilter(opts.filter, '--filter')
  const payloadFilter = compileFilter(opts.payloadFilter, '--payload-filter')

  let messageCount   = 0
  let reconnectCount = 0
  let everConnected  = false   // true after first successful connect
  let offlineShown   = false   // suppress repeated offline messages
  const maxMessages  = opts.count ? parseInt(opts.count, 10) : Infinity

  const authLabel = connectOpts.username ? `user="${connectOpts.username}"` : 'none'

  dbg(opts, `broker URL : ${brokerUrl}`)
  dbg(opts, `client ID  : ${connectOpts.clientId}`)
  dbg(opts, `auth       : ${authLabel}`)
  dbg(opts, `tls        : ${connectOpts.rejectUnauthorized ? 'yes' : 'no'}`)
  dbg(opts, `topics     : ${topics.join(', ')} (QoS ${qos})`)
  dbg(opts, `filters    : topic=${opts.filter || 'none'}  payload=${opts.payloadFilter || 'none'}`)
  dbg(opts, `resolved opts (no password): ${JSON.stringify({ ...opts, password: opts.password ? '***' : undefined }, null, 2)}`)
  dbg(opts, 'calling mqttConnect()...')

  const asUser = connectOpts.username ? chalk.dim(` as ${connectOpts.username}`) : ''
  process.stderr.write(chalk.dim(`Connecting to ${brokerUrl}${asUser}...\n`))

  const client = mqttConnect(brokerUrl, connectOpts)

  // --- connect ---------------------------------------------------------------

  client.on('connect', (connack) => {
    dbg(opts, `connect event  sessionPresent=${connack.sessionPresent}  returnCode=${connack.returnCode}`)

    if (everConnected) {
      // Came back after a drop
      process.stderr.write(chalk.green(`Reconnected to ${brokerUrl}\n`))
    } else {
      process.stderr.write(chalk.green(`Connected to ${brokerUrl}\n`))
    }

    everConnected = true
    offlineShown  = false

    const subscribeTopics = Object.fromEntries(topics.map((t) => [t, { qos }]))
    dbg(opts, `subscribing to: ${Object.keys(subscribeTopics).join(', ')}`)

    client.subscribe(subscribeTopics, (err, granted) => {
      if (err) {
        process.stderr.write(chalk.red(`Subscribe error: ${err.message}\n`))
        process.exit(1)
      }
      for (const { topic, qos: grantedQos } of granted) {
        dbg(opts, `subscribed     ${colorTopic(topic)} (QoS ${grantedQos})`)
        process.stderr.write(chalk.dim(`  watching ${colorTopic(topic)} (QoS ${grantedQos})\n`))
      }
      process.stderr.write('\n')
    })
  })

  // --- reconnect -------------------------------------------------------------

  client.on('reconnect', () => {
    reconnectCount++
    dbg(opts, `reconnect event  attempt #${reconnectCount}`)
  })

  // --- error -----------------------------------------------------------------

  client.on('error', (err) => {
    const code  = err.code  ? chalk.bold(` [${err.code}]`)  : ''
    const errno = err.errno ? chalk.dim(` errno=${err.errno}`) : ''
    process.stderr.write(chalk.red(`Error: ${err.message}${code}${errno}\n`))
    if (opts.verbose && err.stack) {
      process.stderr.write(chalk.dim(err.stack.split('\n').slice(1).join('\n') + '\n'))
    }
  })

  // --- close / disconnect / offline ------------------------------------------

  client.on('close', () => {
    dbg(opts, 'close event  (connection closed)')
  })

  client.on('disconnect', (packet) => {
    dbg(opts, `disconnect packet received  reasonCode=${packet.reasonCode ?? 'n/a'}`)
  })

  client.on('offline', () => {
    dbg(opts, 'offline event  (network unreachable or broker gone)')
    if (!offlineShown) {
      offlineShown = true
      const action = chalk.dim('retrying every 2s  (Ctrl+C to quit)')
      if (everConnected) {
        process.stderr.write(chalk.yellow(`Lost connection to ${brokerUrl}  ${action}\n`))
      } else {
        process.stderr.write(chalk.yellow(`Cannot reach ${brokerUrl}  ${action}\n`))
      }
    }
  })

  // --- message ---------------------------------------------------------------

  client.on('message', (topic, payload, packet) => {
    dbg(opts, `message  topic="${topic}"  size=${payload.length}B  qos=${packet.qos}  retain=${packet.retain}`)

    if (opts.retained === false && packet.retain) {
      dbg(opts, '  -> dropped (retained)')
      return
    }
    if (topicFilter && !topicFilter.test(topic)) {
      dbg(opts, '  -> dropped (topic filter)')
      return
    }
    if (payloadFilter && !payloadFilter.test(payload.toString())) {
      dbg(opts, '  -> dropped (payload filter)')
      return
    }

    const output = formatMessage(topic, payload, packet, opts)
    process.stdout.write(output + '\n')

    messageCount++
    dbg(opts, `  -> printed  (total: ${messageCount})`)

    if (messageCount >= maxMessages) {
      dbg(opts, `message limit reached (${maxMessages}), disconnecting`)
      client.end(false, {}, () => process.exit(0))
    }
  })

  // --- graceful shutdown -----------------------------------------------------

  const shutdown = () => {
    dbg(opts, 'shutdown signal received')
    process.stderr.write(chalk.dim('\nDisconnecting...\n'))
    client.end(false, {}, () => process.exit(0))
  }

  process.on('SIGINT', shutdown)
  process.on('SIGTERM', shutdown)
}
