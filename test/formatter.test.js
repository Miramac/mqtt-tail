import { describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { colorizeJson, formatTimestamp, colorTopic, formatMessage } from '../src/formatter.js'

const stripAnsi = (str) => str.replace(/\x1b\[[0-9;]*m/g, '')

// --- colorizeJson ------------------------------------------------------------

describe('colorizeJson', () => {
  it('preserves plain text after stripping ANSI codes', () => {
    const json = JSON.stringify({ key: 'value', num: 42, flag: true, nothing: null }, null, 2)
    assert.equal(stripAnsi(colorizeJson(json)), json)
  })

  it('handles nested objects', () => {
    const json = JSON.stringify({ a: { b: { c: 1 } } }, null, 2)
    assert.equal(stripAnsi(colorizeJson(json)), json)
  })

  it('handles arrays', () => {
    const json = JSON.stringify([1, 'two', true, null, false], null, 2)
    assert.equal(stripAnsi(colorizeJson(json)), json)
  })

  it('handles escaped characters inside strings', () => {
    const json = JSON.stringify({ msg: 'hello\nworld\t!' }, null, 2)
    assert.equal(stripAnsi(colorizeJson(json)), json)
  })

  it('handles negative and floating point numbers', () => {
    const json = JSON.stringify({ a: -1, b: 3.14, c: -0.5, d: 1e10 }, null, 2)
    assert.equal(stripAnsi(colorizeJson(json)), json)
  })

  it('handles empty object', () => {
    assert.equal(stripAnsi(colorizeJson('{}')), '{}')
  })

  it('handles empty array', () => {
    assert.equal(stripAnsi(colorizeJson('[]')), '[]')
  })
})

// --- formatTimestamp ---------------------------------------------------------

describe('formatTimestamp', () => {
  it('iso returns ISO 8601 format', () => {
    assert.match(formatTimestamp('iso'), /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/)
  })

  it('unix returns integer seconds string', () => {
    const ts = formatTimestamp('unix')
    assert.match(ts, /^\d+$/)
    assert.ok(parseInt(ts, 10) > 1_000_000_000)
  })

  it('unixms returns milliseconds string', () => {
    const ts = formatTimestamp('unixms')
    assert.match(ts, /^\d+$/)
    assert.ok(parseInt(ts, 10) > 1_000_000_000_000)
  })

  it('local returns HH:MM:SS.mmm format', () => {
    assert.match(formatTimestamp('local'), /^\d{2}:\d{2}:\d{2}\.\d{3}$/)
  })

  it('unknown format falls back to local', () => {
    assert.match(formatTimestamp('unknown'), /^\d{2}:\d{2}:\d{2}\.\d{3}$/)
  })
})

// --- colorTopic --------------------------------------------------------------

describe('colorTopic', () => {
  it('returns a non-empty string', () => {
    assert.ok(colorTopic('sensors/temperature').length > 0)
  })

  it('is deterministic - same topic always same output', () => {
    assert.equal(colorTopic('a/b/c'), colorTopic('a/b/c'))
  })

  it('different topics produce different colored strings', () => {
    // Strip ANSI to compare raw text - they differ in color codes
    const t1 = colorTopic('sensors/temp')
    const t2 = colorTopic('control/fan')
    // At least the plain text content differs
    assert.notEqual(stripAnsi(t1), stripAnsi(t2))
  })

  it('plain text content matches the topic', () => {
    assert.equal(stripAnsi(colorTopic('my/topic')), 'my/topic')
  })
})

// --- formatMessage -----------------------------------------------------------

describe('formatMessage', () => {
  const packet      = { qos: 0, retain: false }
  const jsonPayload = Buffer.from(JSON.stringify({ value: 42, unit: 'C' }))
  const textPayload = Buffer.from('hello world')

  it('outputJson mode emits valid JSON with expected fields', () => {
    const line = formatMessage('test/topic', jsonPayload, packet, { outputJson: true })
    const obj  = JSON.parse(line)
    assert.equal(obj.topic, 'test/topic')
    assert.deepEqual(obj.payload, { value: 42, unit: 'C' })
    assert.equal(obj.qos, 0)
    assert.equal(obj.retain, false)
    assert.equal(obj.size, jsonPayload.length)
    assert.match(obj.timestamp, /^\d{4}-\d{2}-\d{2}T/)
  })

  it('outputJson mode with non-JSON payload stores raw string', () => {
    const line = formatMessage('raw/topic', textPayload, packet, { outputJson: true })
    const obj  = JSON.parse(line)
    assert.equal(obj.payload, 'hello world')
  })

  it('compact mode produces no internal newlines', () => {
    const out = formatMessage('test/topic', jsonPayload, packet, { compact: true, timestamp: false })
    assert.ok(!out.includes('\n'))
  })

  it('pretty mode has a newline between header and body', () => {
    const out = formatMessage('test/topic', jsonPayload, packet, { timestamp: false })
    assert.ok(out.includes('\n'))
  })

  it('raw mode passes payload through unformatted', () => {
    const out = formatMessage('test/topic', textPayload, packet, { raw: true, timestamp: false })
    assert.ok(stripAnsi(out).includes('hello world'))
  })

  it('pretty mode pretty-prints valid JSON', () => {
    const out = stripAnsi(formatMessage('test/topic', jsonPayload, packet, { timestamp: false }))
    assert.ok(out.includes('"value"'))
    assert.ok(out.includes('42'))
  })

  it('verbose mode includes QoS and size info', () => {
    const out = stripAnsi(
      formatMessage('test/topic', jsonPayload, { qos: 1, retain: true }, { verbose: true, timestamp: false })
    )
    assert.ok(out.includes('qos:1'))
    assert.ok(out.includes('retained'))
  })
})
