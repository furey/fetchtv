import { test, beforeEach, after, mock } from 'node:test'
import assert from 'node:assert/strict'
import dgram from 'node:dgram'
import os from 'node:os'
import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import path from 'node:path'
import nock from 'nock'

import { discoverFetch, discoverFetchServers, testInternals } from '../fetchtv.js'

const { searchSsdp, parseSsdpResponse } = testInternals

const trackCreatedSockets = () => {
  const createSocket = dgram.createSocket
  const sockets = []
  const spy = mock.method(dgram, 'createSocket', (...args) => {
    const socket = createSocket(...args)
    const tracked = { socket, isClosed: false }
    socket.once('close', () => { tracked.isClosed = true })
    sockets.push(tracked)
    return socket
  })
  return { sockets, restore: () => spy.mock.restore() }
}

const startSsdpResponder = async ({ reply }) => {
  const responder = dgram.createSocket('udp4')
  const requests = []
  responder.on('message', (message, remote) => {
    requests.push(message.toString())
    responder.send(reply, remote.port, remote.address)
  })
  await new Promise(resolve => responder.bind(0, '127.0.0.1', resolve))
  return { responder, requests, port: responder.address().port }
}

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

test('parseSsdpResponse: reads M-SEARCH response headers with upper-case names', () => {
  const message = Buffer.from([
    'HTTP/1.1 200 OK',
    'Cache-Control: max-age=1800',
    'Location: http://192.168.1.20:49152/MediaServer.xml',
    'Server: Linux/4.1.20-1.9, UPnP/1.0, DLNADOC/1.50',
    'EXT:',
    'ST: urn:schemas-upnp-org:device:MediaServer:1',
    '',
    ''
  ].join('\r\n'))

  assert.deepEqual(parseSsdpResponse(message), {
    'CACHE-CONTROL': 'max-age=1800',
    LOCATION: 'http://192.168.1.20:49152/MediaServer.xml',
    SERVER: 'Linux/4.1.20-1.9, UPnP/1.0, DLNADOC/1.50',
    EXT: '',
    ST: 'urn:schemas-upnp-org:device:MediaServer:1'
  })
})

test('parseSsdpResponse: ignores NOTIFY and M-SEARCH requests', () => {
  const notify = Buffer.from('NOTIFY * HTTP/1.1\r\nLOCATION: http://192.168.1.20:49152/MediaServer.xml\r\n\r\n')
  const search = Buffer.from('M-SEARCH * HTTP/1.1\r\nST: ssdp:all\r\n\r\n')
  assert.equal(parseSsdpResponse(notify), null)
  assert.equal(parseSsdpResponse(search), null)
})

test('searchSsdp: sends the M-SEARCH, reports each reply, and closes its sockets', async () => {
  const reply = [
    'HTTP/1.1 200 OK',
    'LOCATION: http://127.0.0.1:49152/MediaServer.xml',
    'ST: upnp:rootdevice',
    '',
    ''
  ].join('\r\n')
  const { responder, requests, port } = await startSsdpResponder({ reply })
  const created = trackCreatedSockets()
  const responses = []

  try {
    await searchSsdp({
      timeoutMs: 300,
      onResponse: (headers) => responses.push(headers),
      addresses: ['127.0.0.1'],
      target: { address: '127.0.0.1', port }
    })
  } finally {
    created.restore()
    await new Promise(resolve => responder.close(resolve))
  }

  assert.deepEqual(requests, [
    'M-SEARCH * HTTP/1.1\r\nHOST: 239.255.255.250:1900\r\nST: ssdp:all\r\nMAN: "ssdp:discover"\r\nMX: 3\r\n\r\n'
  ])
  assert.deepEqual(responses, [{ LOCATION: 'http://127.0.0.1:49152/MediaServer.xml', ST: 'upnp:rootdevice' }])
  assert.equal(created.sockets.length, 1)
  assert.ok(created.sockets.every(({ isClosed }) => isClosed), 'every search socket must be closed')
})

test('searchSsdp: a throwing response handler does not stop the search', async () => {
  const { responder, port } = await startSsdpResponder({ reply: 'HTTP/1.1 200 OK\r\nLOCATION: x\r\n\r\n' })
  let calls = 0
  try {
    await searchSsdp({
      timeoutMs: 300,
      onResponse: () => {
        calls++
        throw new Error('handler failed')
      },
      addresses: ['127.0.0.1'],
      target: { address: '127.0.0.1', port }
    })
  } finally {
    responder.close()
  }
  assert.equal(calls, 1)
})

test('searchSsdp: rejects when there is no IPv4 address to search from', async () => {
  await assert.rejects(
    searchSsdp({ timeoutMs: 10, onResponse: () => {}, addresses: [] }),
    /No sockets available/
  )
})

test('discoverFetchServers: returns an empty array when the machine has no IPv4 interface', async () => {
  const networkInterfaces = mock.method(os, 'networkInterfaces', () => ({
    lo0: [{ address: '127.0.0.1', family: 'IPv4', internal: true }],
    en0: [{ address: 'fe80::1', family: 'IPv6', internal: false }]
  }))
  try {
    assert.deepEqual(await discoverFetchServers({ timeoutMs: 10 }), [])
  } finally {
    networkInterfaces.mock.restore()
  }
})
