import { test, before, after, beforeEach } from 'node:test'
import assert from 'node:assert/strict'
import { createServer } from 'node:http'

import { getFetchRecordings, browseRequest, LISTING_OUTCOME } from '../fetchtv.js'

const CONTROL_PATH = '/upnp/control/ContentDirectory'
const SERVICE_TYPE = 'urn:schemas-upnp-org:service:ContentDirectory:1'
const RECORDINGS_ID = '1'

let server = null
let baseUrl = null
let box = null
let browses = []

const showsOf = (counts) => counts.map((count, index) => ({ id: String(100 + index), title: `Show ${100 + index}`, count }))

const capByLevel = ({ root = 20, shows = 3, items = 24 } = {}) => (objectId) => {
  if (objectId === '0') return root
  if (objectId === RECORDINGS_ID) return shows
  return items
}

const defaultBox = () => ({
  shows: showsOf([45, 2, 1, 7, 30]),
  capFor: capByLevel(),
  totalMatches: (entries) => entries.length,
  ignoresStartingIndex: false,
  failsFromIndex: null,
})

const escapeXml = (text) => text
  .replaceAll('&', '&amp;')
  .replaceAll('<', '&lt;')
  .replaceAll('>', '&gt;')
  .replaceAll('"', '&quot;')

const containerXml = ({ id, parentId, title, childCount }) =>
  `<container id="${id}" parentID="${parentId}" childCount="${childCount}" restricted="1"><dc:title>${title}</dc:title>` +
  '<upnp:class>object.container</upnp:class></container>'

const itemXml = ({ showId, index }) =>
  `<item id="${showId}-${index}" parentID="${showId}" restricted="1"><dc:title>S1 E${index + 1} - Episode</dc:title>` +
  '<upnp:class>object.item.videoItem</upnp:class>' +
  `<res protocolInfo="http-get:*:video/mpeg-tts:*" size="1000000" duration="0:30:00">${baseUrl}/item/${showId}-${index}</res></item>`

const entriesFor = (objectId) => {
  if (objectId === '0') return [
    containerXml({ id: RECORDINGS_ID, parentId: '0', title: 'Recordings', childCount: box.shows.length }),
    containerXml({ id: '2', parentId: '0', title: 'Live TV', childCount: 0 }),
  ]
  if (objectId === RECORDINGS_ID)
    return box.shows.map(show => containerXml({ id: show.id, parentId: RECORDINGS_ID, title: show.title, childCount: show.count }))
  const show = box.shows.find(candidate => candidate.id === objectId)
  if (!show) return []
  return Array.from({ length: show.count }, (_, index) => itemXml({ showId: show.id, index }))
}

const browseEnvelope = ({ entries, totalMatches }) => {
  const didl = '<DIDL-Lite xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:upnp="urn:schemas-upnp-org:metadata-1-0/upnp/" xmlns="urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/">' +
    entries.join('') + '</DIDL-Lite>'
  const total = totalMatches === null ? '' : `<TotalMatches>${totalMatches}</TotalMatches>`
  return '<?xml version="1.0" encoding="utf-8"?><s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"><s:Body>' +
    `<u:BrowseResponse xmlns:u="${SERVICE_TYPE}">` +
    `<Result>${escapeXml(didl)}</Result><NumberReturned>${entries.length}</NumberReturned>` +
    `${total}<UpdateID>1</UpdateID></u:BrowseResponse></s:Body></s:Envelope>`
}

const fieldOf = (body, name) => body.match(new RegExp(`<${name}>([^<]*)</${name}>`))?.[1]

const answerBrowse = (body) => {
  const objectId = fieldOf(body, 'ObjectID')
  const startingIndex = Number(fieldOf(body, 'StartingIndex') ?? 0)
  const requestedCount = Number(fieldOf(body, 'RequestedCount') ?? 0)
  browses.push({ objectId, startingIndex, requestedCount })
  if (box.failsFromIndex !== null && startingIndex >= box.failsFromIndex) return { status: 500, xml: '' }
  const entries = entriesFor(objectId)
  const start = box.ignoresStartingIndex ? 0 : startingIndex
  const cap = box.capFor(objectId)
  const wanted = requestedCount > 0 ? Math.min(requestedCount, cap) : cap
  const page = entries.slice(start, start + wanted)
  return { status: 200, xml: browseEnvelope({ entries: page, totalMatches: box.totalMatches(entries) }) }
}

const collectBody = (req) => new Promise((resolve) => {
  const chunks = []
  req.on('data', (chunk) => chunks.push(chunk))
  req.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')))
})

const location = () => ({
  url: `${baseUrl}/MediaServer.xml`,
  _rawDeviceXml: { serviceList: { service: { serviceType: SERVICE_TYPE, controlURL: CONTROL_PATH } } },
})

const apiService = () => ({ cd_ctr: `${baseUrl}${CONTROL_PATH}`, cd_service: SERVICE_TYPE })

const silenced = async (fn) => {
  const original = { log: console.log, warn: console.warn, error: console.error }
  console.log = console.warn = console.error = () => {}
  try {
    return await fn()
  } finally {
    Object.assign(console, original)
  }
}

const listShows = async () => {
  const outcomes = []
  const shows = await silenced(() => getFetchRecordings({
    location: location(),
    onProgress: () => {},
    onOutcome: (outcome) => outcomes.push(outcome.outcome),
  }))
  return { shows, outcomes, byTitle: Object.fromEntries(shows.map(show => [show.title, show])) }
}

const browseIds = async (objectId) => {
  const didl = await silenced(() => browseRequest({ apiService: apiService(), objectId }))
  return [didl.container, didl.item].flat().filter(Boolean).map(node => String(node['@_id']))
}

before(async () => {
  server = createServer(async (req, res) => {
    if (req.method !== 'POST' || req.url !== CONTROL_PATH) {
      res.writeHead(404)
      return res.end()
    }
    const { status, xml } = answerBrowse(await collectBody(req))
    res.writeHead(status, { 'Content-Type': 'text/xml' })
    res.end(xml)
  })
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
  baseUrl = `http://127.0.0.1:${server.address().port}`
})

after(async () => {
  server.closeAllConnections()
  await new Promise(resolve => server.close(resolve))
})

beforeEach(() => {
  box = defaultBox()
  browses = []
})

test('browse paging: a show with 45 recordings lists all 45 when the box sends 24 per reply', async () => {
  const { byTitle, outcomes } = await listShows()
  assert.equal(byTitle['Show 100'].items.length, 45)
  assert.deepEqual(outcomes, [LISTING_OUTCOME.OK])
  const showPages = browses.filter(browse => browse.objectId === '100').map(browse => browse.startingIndex)
  assert.deepEqual(showPages, [0, 24])
})

test('browse paging: a library with more shows than one reply lists every show', async () => {
  const { shows } = await listShows()
  assert.deepEqual(shows.map(show => show.title).sort(), box.shows.map(show => show.title).sort())
  const recordingsPages = browses.filter(browse => browse.objectId === RECORDINGS_ID).map(browse => browse.startingIndex)
  assert.deepEqual(recordingsPages, [0, 3])
})

test('browse paging: every listed count matches the box count for every show', async () => {
  const { byTitle } = await listShows()
  box.shows.forEach(show => assert.equal(byTitle[show.title].items.length, show.count, show.title))
  const listed = Object.values(byTitle).reduce((total, show) => total + show.items.length, 0)
  assert.equal(listed, 85)
})

test('browse paging: the first request per folder stays StartingIndex 0 with RequestedCount 0', async () => {
  await listShows()
  const firstRequests = browses.filter(browse => browse.startingIndex === 0)
  assert.ok(firstRequests.length >= 7)
  firstRequests.forEach(browse => assert.equal(browse.requestedCount, 0))
})

test('browse paging: a box with no cap answers in one request per folder', async () => {
  box.capFor = () => 1000
  await listShows()
  assert.equal(browses.length, 2 + box.shows.length)
})

test('browse paging: an empty reply before TotalMatches stops paging with what was sent', async () => {
  box.totalMatches = (entries) => entries.length + 10
  const ids = await browseIds('100')
  assert.equal(ids.length, 45)
  assert.deepEqual(browses.map(browse => browse.startingIndex), [0, 24, 45])
})

test('browse paging: a box that ignores StartingIndex stops after one repeated page', async () => {
  box.ignoresStartingIndex = true
  const ids = await browseIds('100')
  assert.equal(ids.length, 24)
  assert.equal(new Set(ids).size, 24)
  assert.equal(browses.length, 2)
})

test('browse paging: a reply with no TotalMatches is taken as complete', async () => {
  box.totalMatches = () => null
  const ids = await browseIds('100')
  assert.equal(ids.length, 24)
  assert.equal(browses.length, 1)
})

test('browse paging: a lone container or item on a later page still merges into the list', async () => {
  box.shows = showsOf([4])
  box.capFor = () => 1
  assert.deepEqual(await browseIds(RECORDINGS_ID), ['100'])
  assert.deepEqual(await browseIds('100'), ['100-0', '100-1', '100-2', '100-3'])
})

test('browse paging: a later page that keeps failing fails the whole Browse instead of a short list', async () => {
  box.failsFromIndex = 24
  const failures = []
  const didl = await silenced(() => browseRequest({ apiService: apiService(), objectId: '100', onFailure: (error) => failures.push(error) }))
  assert.equal(didl, null)
  assert.equal(failures.length, 1)
})
