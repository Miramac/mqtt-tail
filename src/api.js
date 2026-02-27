import { connect as mqttConnect } from 'mqtt'
import { loadConfig } from './config.js'
import { buildBrokerUrl, buildConnectOptions } from './subscriber.js'

function compileFilter(pattern) {
  if (!pattern) return null
  try { return new RegExp(pattern) }
  catch (err) { throw new Error(`Invalid filter regex "${pattern}": ${err.message}`) }
}

/**
 * Subscribes to MQTT topics and yields messages as an async iterator.
 *
 * @param {string|string[]} topics  - MQTT topic(s), supports + and # wildcards
 * @param {object}          opts    - host, port, username, password, tls, ca, cert, key,
 *                                    filter, payloadFilter, qos, count, retained, config
 * @yields {{ topic: string, payload: Buffer, packet: object }}
 *
 * @example
 * for await (const { topic, payload } of subscribe('sensors/#', { host: 'localhost' })) {
 *   console.log(topic, payload.toString())
 * }
 */
export async function* subscribe(topics, opts = {}) {
  const topicList = Array.isArray(topics) ? topics : [topics]

  const fileConfig = await loadConfig(opts.config)
  const merged = { ...fileConfig, ...opts }

  const brokerUrl   = buildBrokerUrl(merged)
  const connectOpts = await buildConnectOptions(merged)
  connectOpts.reconnectPeriod = 0   // no silent reconnection in library mode — throw instead

  const qos           = parseInt(merged.qos ?? 0, 10)
  const topicFilter   = compileFilter(merged.filter)
  const payloadFilter = compileFilter(merged.payloadFilter)
  const maxMessages   = merged.count ? parseInt(merged.count, 10) : Infinity
  let count = 0

  // --- push/pull bridge (queue + waiting promise) ---
  const queue = []
  let waiting      = null   // { resolve, reject }
  let pendingError = null
  let done         = false

  const enqueue = (msg) => {
    if (waiting) { const w = waiting; waiting = null; w.resolve({ msg }) }
    else queue.push(msg)
  }

  const finish = (err) => {
    done = true
    if (waiting) {
      const w = waiting; waiting = null
      err ? w.reject(err) : w.resolve({ done: true })
    } else if (err) {
      pendingError = err
    }
  }

  const client = mqttConnect(brokerUrl, connectOpts)

  await new Promise((resolve, reject) => {
    client.once('connect', resolve)
    client.once('error', reject)
  })

  const subscribeMap = Object.fromEntries(topicList.map(t => [t, { qos }]))
  await new Promise((resolve, reject) =>
    client.subscribe(subscribeMap, (err) => err ? reject(err) : resolve())
  )

  client.on('message', (topic, payload, packet) => {
    if (merged.retained === false && packet.retain) return
    if (topicFilter   && !topicFilter.test(topic))              return
    if (payloadFilter && !payloadFilter.test(payload.toString())) return

    enqueue({ topic, payload, packet })
    if (++count >= maxMessages) { client.end(); finish() }
  })

  client.on('error', (err) => finish(err))
  client.on('close', () => { if (!done) finish() })

  try {
    while (true) {
      if (pendingError) throw pendingError
      if (queue.length > 0) { yield queue.shift(); continue }
      if (done) break
      const result = await new Promise((resolve, reject) => { waiting = { resolve, reject } })
      if (result.done) break
      yield result.msg
    }
  } finally {
    if (!client.disconnected && !client.disconnecting) client.end()
  }
}
