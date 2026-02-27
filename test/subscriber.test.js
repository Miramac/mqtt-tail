import { describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { buildBrokerUrl, compileFilter } from '../src/subscriber.js'

// --- buildBrokerUrl ----------------------------------------------------------

describe('buildBrokerUrl', () => {
  it('defaults to mqtt://localhost:1883', () => {
    assert.equal(buildBrokerUrl({}), 'mqtt://localhost:1883')
  })

  it('uses custom host and port', () => {
    assert.equal(buildBrokerUrl({ host: 'broker.example.com', port: 9999 }), 'mqtt://broker.example.com:9999')
  })

  it('switches to mqtts when tls is true', () => {
    assert.equal(buildBrokerUrl({ tls: true }), 'mqtts://localhost:8883')
  })

  it('switches to mqtts when ca is provided', () => {
    assert.equal(buildBrokerUrl({ ca: '/path/to/ca.pem' }), 'mqtts://localhost:8883')
  })

  it('switches to mqtts when cert is provided', () => {
    assert.equal(buildBrokerUrl({ cert: '/path/to/cert.pem' }), 'mqtts://localhost:8883')
  })

  it('switches to mqtts when key is provided', () => {
    assert.equal(buildBrokerUrl({ key: '/path/to/key.pem' }), 'mqtts://localhost:8883')
  })

  it('respects custom port with tls', () => {
    assert.equal(buildBrokerUrl({ tls: true, host: 'secure.broker.com', port: 1234 }), 'mqtts://secure.broker.com:1234')
  })

  it('explicit port overrides tls default port', () => {
    assert.equal(buildBrokerUrl({ tls: true, port: 443 }), 'mqtts://localhost:443')
  })
})

// --- compileFilter -----------------------------------------------------------

describe('compileFilter', () => {
  it('returns null for falsy input', () => {
    assert.equal(compileFilter(null, '--filter'), null)
    assert.equal(compileFilter(undefined, '--filter'), null)
    assert.equal(compileFilter('', '--filter'), null)
  })

  it('returns a RegExp for a valid pattern', () => {
    const re = compileFilter('sensors/.*', '--filter')
    assert.ok(re instanceof RegExp)
  })

  it('matches expected topics', () => {
    const re = compileFilter('temperature|humidity', '--filter')
    assert.ok(re.test('sensors/temperature'))
    assert.ok(re.test('sensors/humidity'))
    assert.ok(!re.test('sensors/pressure'))
  })

  it('supports anchored patterns', () => {
    const re = compileFilter('^sensors/', '--filter')
    assert.ok(re.test('sensors/temp'))
    assert.ok(!re.test('home/sensors/temp'))
  })

  it('is case-sensitive by default', () => {
    const re = compileFilter('TEMP', '--filter')
    assert.ok(!re.test('sensors/temp'))
    assert.ok(re.test('sensors/TEMP'))
  })
})
