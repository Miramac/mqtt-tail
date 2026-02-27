import { describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { parseDotEnvContent } from '../src/config.js'

describe('parseDotEnvContent', () => {
  it('parses simple key=value pairs', () => {
    const result = parseDotEnvContent('MQTT_HOST=broker.example.com\nMQTT_PORT=1883')
    assert.equal(result.MQTT_HOST, 'broker.example.com')
    assert.equal(result.MQTT_PORT, '1883')
  })

  it('strips double-quoted values', () => {
    const result = parseDotEnvContent('MQTT_PASSWORD="my secret"')
    assert.equal(result.MQTT_PASSWORD, 'my secret')
  })

  it('strips single-quoted values', () => {
    const result = parseDotEnvContent("MQTT_PASSWORD='my secret'")
    assert.equal(result.MQTT_PASSWORD, 'my secret')
  })

  it('keeps unquoted values with special characters', () => {
    const result = parseDotEnvContent('MQTT_PASSWORD=mqtt_user!')
    assert.equal(result.MQTT_PASSWORD, 'mqtt_user!')
  })

  it('ignores lines starting with #', () => {
    const result = parseDotEnvContent('# comment\nMQTT_HOST=broker.local')
    assert.equal(result.MQTT_HOST, 'broker.local')
    assert.ok(!Object.hasOwn(result, '# comment'))
  })

  it('ignores empty lines', () => {
    const result = parseDotEnvContent('\n\nMQTT_HOST=broker.local\n\n')
    assert.equal(result.MQTT_HOST, 'broker.local')
    assert.equal(Object.keys(result).length, 1)
  })

  it('handles Windows CRLF line endings', () => {
    const result = parseDotEnvContent('MQTT_HOST=broker.local\r\nMQTT_PORT=1883\r\n')
    assert.equal(result.MQTT_HOST, 'broker.local')
    assert.equal(result.MQTT_PORT, '1883')
  })

  it('ignores lines without an equals sign', () => {
    const result = parseDotEnvContent('INVALID_LINE\nMQTT_HOST=broker.local')
    assert.equal(result.MQTT_HOST, 'broker.local')
    assert.ok(!Object.hasOwn(result, 'INVALID_LINE'))
  })

  it('allows = in value', () => {
    const result = parseDotEnvContent('MQTT_PASSWORD=a=b=c')
    assert.equal(result.MQTT_PASSWORD, 'a=b=c')
  })

  it('returns empty object for empty input', () => {
    const result = parseDotEnvContent('')
    assert.deepEqual(result, {})
  })

  it('trims whitespace around key and value', () => {
    const result = parseDotEnvContent('  MQTT_HOST  =  broker.local  ')
    assert.equal(result.MQTT_HOST, 'broker.local')
  })
})
