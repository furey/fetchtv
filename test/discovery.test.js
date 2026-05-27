import { test, beforeEach, after } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import path from 'node:path'
import nock from 'nock'

import { discoverFetch } from '../fetchtv.js'

const fixturesDir = path.join(path.dirname(fileURLToPath(import.meta.url)), 'fixtures')
const readFixture = (name) => readFileSync(path.join(fixturesDir, name), 'utf-8')

beforeEach(() => {
  nock.cleanAll()
})

after(() => {
  nock.restore()
})

test('discoverFetch: with explicit --ip returns the Fetch server descriptor', async () => {
  nock('http://192.168.1.20:49152')
    .get('/MediaServer.xml')
    .reply(200, readFixture('media-server.xml'), { 'Content-Type': 'text/xml' })

  const server = await discoverFetch({ ip: '192.168.1.20', port: 49152 })
  assert.ok(server, 'expected discovery result')
  assert.equal(server.manufacturerURL, 'http://www.fetch.com/')
  assert.equal(server.friendlyName, 'Fetch TV Test Box')
  assert.equal(server.modelName, 'Mighty')
  assert.equal(server.modelNumber, 'FTV-1000')
  assert.equal(server.url, 'http://192.168.1.20:49152/MediaServer.xml')
})

test('discoverFetch: returns null when MediaServer.xml is not a Fetch device', async () => {
  const nonFetchXml = readFixture('media-server.xml')
    .replace('http://www.fetch.com/', 'http://example.com/other-vendor/')

  nock('http://192.168.1.30:49152')
    .get('/MediaServer.xml')
    .reply(200, nonFetchXml, { 'Content-Type': 'text/xml' })

  const server = await discoverFetch({ ip: '192.168.1.30', port: 49152 })
  assert.equal(server, null)
})

test('discoverFetch: returns null when MediaServer.xml is unreachable', async () => {
  nock('http://192.168.1.40:49152')
    .get('/MediaServer.xml')
    .reply(500, 'Internal Server Error')

  const server = await discoverFetch({ ip: '192.168.1.40', port: 49152 })
  assert.equal(server, null)
})

test('discoverFetch: exposes the raw device XML for downstream service lookup', async () => {
  nock('http://192.168.1.50:49152')
    .get('/MediaServer.xml')
    .reply(200, readFixture('media-server.xml'), { 'Content-Type': 'text/xml' })

  const server = await discoverFetch({ ip: '192.168.1.50', port: 49152 })
  assert.ok(server._rawDeviceXml.serviceList, 'serviceList must survive parse')
  assert.equal(
    server._rawDeviceXml.serviceList.service.serviceType,
    'urn:schemas-upnp-org:service:ContentDirectory:1',
  )
})
