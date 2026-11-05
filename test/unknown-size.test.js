import { test, before, after, beforeEach } from 'node:test'
import assert from 'node:assert/strict'
import { createServer } from 'node:http'
import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import fs from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'

import {
  getFetchRecordings,
  isCurrentlyRecording,
  saveRecordings,
  downloadFile,
  formatItem,
  handleSaveAction,
  testInternals,
} from '../fetchtv.js'

const fixturesDir = path.join(path.dirname(fileURLToPath(import.meta.url)), 'fixtures')
const readFixture = (name) => readFileSync(path.join(fixturesDir, name), 'utf-8')

const CONTROL_PATH = '/upnp/control/ContentDirectory'
const MAX_OCTET_RECORDING = 4398046510080
const BODY = Buffer.alloc(256 * 1024, 'uvwxyz0123')
const FETCH_BOX_OVERSTATEMENT = 84

let server = null
let baseUrl = null
let requestLog = []
let growingLength = 1000
let tmpDir = null

const escapeXml = (text) => text
  .replaceAll('&', '&amp;')
  .replaceAll('<', '&lt;')
  .replaceAll('>', '&gt;')
  .replaceAll('"', '&quot;')

const didlItem = ({ id, title, route, size = -1 }) =>
  `<item id="${id}" parentID="10" restricted="1"><dc:title>${title}</dc:title>` +
  '<upnp:class>object.item.videoItem</upnp:class>' +
  `<res protocolInfo="http-get:*:video/mpeg-tts:*" size="${size}" duration="0:00:00">${baseUrl}${route}</res></item>`

const ITEMS = [
  { id: 201, title: 'S1 E1 - Finished', route: '/item/finished' },
  { id: 202, title: 'S1 E2 - Growing', route: '/item/growing' },
  { id: 203, title: 'S1 E3 - Deleted', route: '/item/deleted' },
  { id: 204, title: 'S1 E4 - No Head', route: '/item/no-head' },
  { id: 205, title: 'S1 E5 - Live Marker', route: '/item/live-marker' },
  { id: 206, title: 'S1 E6 - Silent', route: '/item/silent' },
  { id: 207, title: 'S1 E7 - Reset', route: '/item/reset' },
  { id: 208, title: 'S1 E8 - Listed Marker', route: '/item/listed-marker', size: MAX_OCTET_RECORDING },
  { id: 209, title: 'S1 E9 - Gone', route: '/item/gone' },
]

const SHORT_PROBE_TIMEOUT = 200

const MANY_SHOW_IDS = Array.from({ length: 12 }, (_, index) => 300 + index)
let isManyShows = false

const browseResponse = (entries) => {
  const didl = '<DIDL-Lite xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:upnp="urn:schemas-upnp-org:metadata-1-0/upnp/" xmlns="urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/">' +
    entries.join('') + '</DIDL-Lite>'
  return '<?xml version="1.0" encoding="utf-8"?><s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"><s:Body>' +
    '<u:BrowseResponse xmlns:u="urn:schemas-upnp-org:service:ContentDirectory:1">' +
    `<Result>${escapeXml(didl)}</Result><NumberReturned>${entries.length}</NumberReturned>` +
    `<TotalMatches>${entries.length}</TotalMatches><UpdateID>1</UpdateID></u:BrowseResponse></s:Body></s:Envelope>`
}

const showContainer = (id) =>
  `<container id="${id}" parentID="1" restricted="1"><dc:title>Show ${id}</dc:title><upnp:class>object.container</upnp:class></container>`

const browseReplyFor = (objectId) => {
  if (objectId === '0') return readFixture('browse-root.xml')
  if (objectId === '1' && isManyShows) return browseResponse(MANY_SHOW_IDS.map(showContainer))
  if (objectId === '1') return readFixture('browse-recordings.xml')
  if (objectId === '10') return browseResponse(ITEMS.map(didlItem))
  if (isManyShows && MANY_SHOW_IDS.includes(Number(objectId)))
    return browseResponse([didlItem({ id: Number(objectId) * 10, title: 'S1 E1 - Finished', route: `/item/many/${objectId}` })])
  return readFixture('browse-empty.xml')
}

const collectBody = (req) => new Promise((resolve) => {
  const chunks = []
  req.on('data', (chunk) => chunks.push(chunk))
  req.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')))
})

const rangeStartOf = (req) => {
  const match = String(req.headers.range ?? '').match(/bytes=(\d+)-/)
  return match ? parseInt(match[1], 10) : null
}

const serveFinished = (req, res) => {
  const statedLength = BODY.length + FETCH_BOX_OVERSTATEMENT
  if (req.method === 'HEAD') {
    res.writeHead(200, { 'Content-Length': String(statedLength) })
    return res.end()
  }
  if (rangeStartOf(req) !== null) {
    res.writeHead(416, { 'Content-Range': `bytes */${statedLength}` })
    return res.end()
  }
  res.writeHead(200, { 'Content-Length': String(statedLength) })
  res.write(BODY, () => setTimeout(() => res.socket.destroy(), 20))
}

const serveGrowing = (req, res) => {
  growingLength += 188 * 1000
  res.writeHead(200, { 'Content-Length': String(growingLength) })
  res.end()
}

const serveNoHead = (req, res) => {
  if (req.method === 'HEAD') {
    res.writeHead(405)
    return res.end()
  }
  if (req.headers.range === 'bytes=0-0') {
    res.writeHead(206, { 'Content-Length': '1', 'Content-Range': `bytes 0-0/${BODY.length}` })
    return res.end(BODY.subarray(0, 1))
  }
  res.writeHead(200, { 'Content-Length': String(BODY.length) })
  res.end(BODY)
}

const serveLiveMarker = (req, res) => {
  res.writeHead(200, { 'Content-Length': String(MAX_OCTET_RECORDING) })
  res.end()
}

const serveSilent = () => {}

const serveGone = (req, res) => {
  res.writeHead(410)
  res.end()
}

const serveReset = (req) => req.socket.destroy()

const itemRoutes = {
  '/item/finished': serveFinished,
  '/item/growing': serveGrowing,
  '/item/no-head': serveNoHead,
  '/item/live-marker': serveLiveMarker,
  '/item/listed-marker': serveLiveMarker,
  '/item/silent': serveSilent,
  '/item/reset': serveReset,
  '/item/gone': serveGone,
}

const location = () => ({
  url: `${baseUrl}/MediaServer.xml`,
  _rawDeviceXml: {
    serviceList: {
      service: {
        serviceType: 'urn:schemas-upnp-org:service:ContentDirectory:1',
        controlURL: CONTROL_PATH,
      },
    },
  },
})

const silenced = async (fn) => {
  const original = console.log
  console.log = () => {}
  try {
    return await fn()
  } finally {
    console.log = original
  }
}

const listItems = async () => {
  const shows = await silenced(() => getFetchRecordings({ location: location(), onProgress: () => {} }))
  return Object.fromEntries(shows.flatMap(show => show.items).map(item => [item.id, item]))
}

const probesOf = (route) => requestLog.filter(entry => entry.route === route && (entry.method === 'HEAD' || entry.range === 'bytes=0-0'))

before(async () => {
  server = createServer(async (req, res) => {
    requestLog.push({ route: req.url, method: req.method, range: req.headers.range })
    if (req.method === 'POST' && req.url === CONTROL_PATH) {
      const objectId = (await collectBody(req)).match(/<ObjectID>([^<]+)<\/ObjectID>/)?.[1]
      res.writeHead(200, { 'Content-Type': 'text/xml' })
      return res.end(browseReplyFor(objectId))
    }
    const handler = req.url.startsWith('/item/many/') ? serveFinished : itemRoutes[req.url]
    if (!handler) {
      res.writeHead(404)
      return res.end()
    }
    handler(req, res)
  })
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
  baseUrl = `http://127.0.0.1:${server.address().port}`
  tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'fetchtv-unknown-size-'))
  testInternals.setLengthProbeTimeout(SHORT_PROBE_TIMEOUT)
})

after(async () => {
  testInternals.setLengthProbeTimeout()
  server.closeAllConnections()
  await new Promise(resolve => server.close(resolve))
  await fs.rm(tmpDir, { recursive: true, force: true })
})

beforeEach(() => {
  requestLog = []
  isManyShows = false
  testInternals.setLengthSettleDelay()
  testInternals.forgetKnownLengths()
})

test('unknown size: a stable Content-Length makes the item finished, with that size', async () => {
  const items = await listItems()
  assert.equal(items['201'].size, BODY.length + FETCH_BOX_OVERSTATEMENT)
  assert.equal(items['201'].unavailable, undefined)
  assert.equal(await isCurrentlyRecording(items['201']), false)
  assert.equal(probesOf('/item/finished').length, 2)
})

test('unknown size: a growing Content-Length stays recording', async () => {
  const items = await listItems()
  assert.equal(items['202'].size, -1)
  assert.equal(items['202'].unavailable, undefined)
  assert.equal(items['202'].growing, true)
  assert.equal(await isCurrentlyRecording(items['202']), true)
})

test('unknown size: the live-recording marker stays recording after one probe', async () => {
  const items = await listItems()
  assert.equal(items['205'].size, -1)
  assert.equal(items['205'].growing, true)
  assert.equal(items['205'].unavailable, undefined)
  assert.equal(await isCurrentlyRecording(items['205']), true)
  assert.equal(probesOf('/item/live-marker').length, 1)
})

test('unknown size: a listed marker size is growing without a probe', async () => {
  const items = await listItems()
  assert.equal(items['208'].size, MAX_OCTET_RECORDING)
  assert.equal(items['208'].growing, true)
  assert.equal(await isCurrentlyRecording(items['208']), true)
  assert.equal(probesOf('/item/listed-marker').length, 0)
  const result = await silenced(() => downloadFile({ item: items['208'], filePath: path.join(tmpDir, 'listed-marker.ts') }))
  assert.equal(result.stillRecording, true)
  assert.equal(result.unavailable, undefined)
})

test('unknown size: a timeout or reset gets neither flag, stays recording, and is cached for a minute', async () => {
  testInternals.setLengthSettleDelay(20)
  const firstStartedAt = Date.now()
  const first = await listItems()
  const firstElapsed = Date.now() - firstStartedAt
  for (const id of ['206', '207']) {
    assert.equal(first[id].size, -1, id)
    assert.equal(first[id].unavailable, undefined, id)
    assert.equal(first[id].growing, undefined, id)
    assert.equal(await isCurrentlyRecording(first[id]), true, id)
  }
  const result = await silenced(() => downloadFile({ item: first['206'], filePath: path.join(tmpDir, 'silent.ts') }))
  assert.equal(result.stillRecording, true)
  assert.equal(result.unavailable, undefined)
  requestLog = []
  const secondStartedAt = Date.now()
  const second = await listItems()
  const secondElapsed = Date.now() - secondStartedAt
  testInternals.setLengthSettleDelay()
  assert.equal(probesOf('/item/silent').length, 0)
  assert.equal(probesOf('/item/reset').length, 0)
  assert.ok(secondElapsed < firstElapsed - SHORT_PROBE_TIMEOUT, `re-listing took ${secondElapsed} ms, first listing ${firstElapsed} ms`)
  assert.equal(second['206'].size, -1)
  assert.equal(second['206'].unavailable, undefined)
  assert.equal(second['206'].growing, undefined)
})

test('unknown size: a 404 marks the item unavailable, not recording', async () => {
  const items = await listItems()
  assert.equal(items['203'].size, -1)
  assert.equal(items['203'].unavailable, true)
  assert.equal(items['203'].growing, undefined)
  assert.equal(items['209'].unavailable, true)
  assert.equal(await isCurrentlyRecording(items['209']), false)
  assert.equal(await isCurrentlyRecording(items['203']), false)
  const result = await silenced(() => downloadFile({ item: items['203'], filePath: path.join(tmpDir, 'deleted.ts') }))
  assert.equal(result.unavailable, true)
  assert.match(result.error, /could not be opened/)
})

test('unknown size: a box that refuses HEAD is probed with a one-byte range', async () => {
  const items = await listItems()
  assert.equal(items['204'].size, BODY.length)
  assert.equal(await isCurrentlyRecording(items['204']), false)
})

test('unknown size: finished and missing results are cached, growing ones are probed again', async () => {
  await listItems()
  requestLog = []
  const items = await listItems()
  assert.equal(items['201'].size, BODY.length + FETCH_BOX_OVERSTATEMENT)
  assert.equal(items['203'].unavailable, true)
  assert.equal(probesOf('/item/finished').length, 0)
  assert.equal(probesOf('/item/deleted').length, 0)
  assert.ok(probesOf('/item/growing').length > 0)
})

test('unknown size: a finished item saves byte-exact, an unavailable one gets status unavailable', async () => {
  const items = await listItems()
  const recordings = [{ title: 'Bluey', items: [items['201'], items['203']] }]
  const results = await silenced(() => saveRecordings({ recordings, savePath: tmpDir, retryDelays: [], onProgress: () => {} }))
  const saved = results.find(result => result.status === 'saved')
  assert.ok(saved, JSON.stringify(results))
  const savedFiles = await fs.readdir(tmpDir, { recursive: true })
  const savedFile = savedFiles.find(name => name.endsWith('Finished.ts'))
  assert.ok(savedFile, savedFiles.join(', '))
  assert.deepEqual(await fs.readFile(path.join(tmpDir, savedFile)), BODY)
  const unavailable = results.find(result => result.status !== 'saved')
  assert.equal(unavailable.status, 'unavailable', JSON.stringify(results))
  assert.match(unavailable.error, /may have been deleted on the box/)
})

test('unknown size: a CLI save with one unavailable item exits 0 and names it', async () => {
  const items = await listItems()
  const recordings = [{ title: 'Bluey', items: [items['203']] }]
  const lines = []
  const original = console.log
  console.log = (...args) => lines.push(args.join(' '))
  try {
    const summary = await handleSaveAction({ recordings, savePath: tmpDir, jsonOutput: true, retryDelays: [] })
    assert.equal(summary.unavailable, 1)
    assert.equal(summary.failed, 0)
    assert.equal(summary.remaining, 0)
    assert.equal(process.exitCode, undefined)
  } finally {
    console.log = original
    process.exitCode = undefined
  }
  const output = lines.join('\n')
  assert.match(output, /S1 E3 - Deleted is listed on the box but its file could not be opened; it may have been deleted on the box\./)
  const json = JSON.parse(output.match(/=== Start JSON Output ===\s*([\s\S]*?)\s*=== End JSON Output ===/)[1])
  assert.equal(json.results[0].status, 'unavailable')
  assert.equal(json.results[0].item.unavailable, true)
})

test('unknown size: JSON items carry growing and unavailable booleans', async () => {
  const items = await listItems()
  assert.equal(formatItem(items['202']).growing, true)
  assert.equal(formatItem(items['202']).unavailable, false)
  assert.equal(formatItem(items['203']).unavailable, true)
  assert.equal(formatItem(items['203']).growing, false)
  assert.equal(formatItem(items['201']).growing, false)
  assert.equal(formatItem(items['201']).unavailable, false)
})

test('unknown size: downloadFile still refuses an unprobed non-positive size as still recording', async () => {
  const result = await silenced(() => downloadFile({
    item: { title: 'Unprobed', size: -1, url: `${baseUrl}/item/finished` },
    filePath: path.join(tmpDir, 'unprobed.ts'),
  }))
  assert.equal(result.stillRecording, true)
})

test('unknown size: unknown-size items across show batches share one probe cycle', async () => {
  const settleDelay = 600
  const timedListing = async () => {
    testInternals.forgetKnownLengths()
    const startedAt = Date.now()
    const items = await listItems()
    return { items, elapsed: Date.now() - startedAt }
  }
  isManyShows = true
  try {
    testInternals.setLengthSettleDelay(0)
    const baseline = await timedListing()
    testInternals.setLengthSettleDelay(settleDelay)
    const { items, elapsed } = await timedListing()
    assert.equal(Object.keys(items).length, MANY_SHOW_IDS.length)
    for (const item of Object.values(items)) assert.equal(item.size, BODY.length + FETCH_BOX_OVERSTATEMENT, item.id)
    const probeWait = elapsed - baseline.elapsed
    assert.ok(probeWait < settleDelay * 2, `probes added ${probeWait} ms, more than one probe cycle`)
  } finally {
    testInternals.setLengthSettleDelay()
  }
})
