import { test, before, after, beforeEach } from 'node:test'
import assert from 'node:assert/strict'
import { createServer } from 'node:http'

import { getFetchRecordings, browseRequest, createRequestManager, LISTING_OUTCOME, testInternals } from '../fetchtv.js'

const CONTROL_PATH = '/upnp/control/ContentDirectory'
const SERVICE_TYPE = 'urn:schemas-upnp-org:service:ContentDirectory:1'
const RECORDINGS_ID = '1'
const NO_SUCH_OBJECT = '<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"><s:Body><s:Fault>' +
  '<faultcode>s:Client</faultcode><faultstring>UPnPError</faultstring><detail>' +
  '<UPnPError xmlns="urn:schemas-upnp-org:control-1-0"><errorCode>701</errorCode></UPnPError>' +
  '</detail></s:Fault></s:Body></s:Envelope>'

let server = null
let baseUrl = null
let box = null
let browses = []

const showsOf = (counts) => counts.map((count, index) => ({
  id: String(100 + index),
  title: `Show ${100 + index}`,
  items: Array.from({ length: count }, (_, item) => `${100 + index}-${item}`),
}))

const capByLevel = ({ root = 20, shows = 3, items = 24 } = {}) => (objectId) => {
  if (objectId === '0') return root
  if (objectId === RECORDINGS_ID) return shows
  return items
}

const defaultBox = () => ({
  shows: showsOf([45, 2, 1, 7, 30]),
  capFor: capByLevel(),
  totalMatches: (entries) => entries.length,
  numberReturned: (page) => page.length,
  updateId: 1,
  ignoresStartingIndex: false,
  failsFromIndex: null,
  goneFromIndex: null,
  resetsFromIndex: null,
  hangsPastEndOf: () => false,
  updateIdWhenEmpty: null,
  unknownSizes: false,
  onItem: () => {},
  idlessItems: false,
  onBrowse: () => {},
})

const escapeXml = (text) => text
  .replaceAll('&', '&amp;')
  .replaceAll('<', '&lt;')
  .replaceAll('>', '&gt;')
  .replaceAll('"', '&quot;')

const containerXml = ({ id, parentId, title, childCount }) =>
  `<container id="${id}" parentID="${parentId}" childCount="${childCount}" restricted="1"><dc:title>${title}</dc:title>` +
  '<upnp:class>object.container</upnp:class></container>'

const itemXml = ({ showId, itemId }) => {
  const idAttribute = box.idlessItems ? '' : ` id="${itemId}"`
  return `<item${idAttribute} parentID="${showId}" restricted="1"><dc:title>Episode ${itemId}</dc:title>` +
    '<upnp:class>object.item.videoItem</upnp:class>' +
    `<res protocolInfo="http-get:*:video/mpeg-tts:*" size="${box.unknownSizes ? -1 : 1000000}" duration="0:30:00">${baseUrl}/item/${itemId}</res></item>`
}

const entriesFor = (objectId) => {
  if (objectId === '0') return [
    containerXml({ id: RECORDINGS_ID, parentId: '0', title: 'Recordings', childCount: box.shows.length }),
    containerXml({ id: '2', parentId: '0', title: 'Live TV', childCount: 0 }),
  ]
  if (objectId === RECORDINGS_ID)
    return box.shows.map(show => containerXml({ id: show.id, parentId: RECORDINGS_ID, title: show.title, childCount: show.items.length }))
  const show = box.shows.find(candidate => candidate.id === objectId)
  if (!show) return []
  return show.items.map(itemId => itemXml({ showId: show.id, itemId }))
}

const browseEnvelope = ({ page, totalMatches, numberReturned }) => {
  const didl = '<DIDL-Lite xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:upnp="urn:schemas-upnp-org:metadata-1-0/upnp/" xmlns="urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/">' +
    page.join('') + '</DIDL-Lite>'
  const total = totalMatches === null ? '' : `<TotalMatches>${totalMatches}</TotalMatches>`
  return '<?xml version="1.0" encoding="utf-8"?><s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"><s:Body>' +
    `<u:BrowseResponse xmlns:u="${SERVICE_TYPE}">` +
    `<Result>${escapeXml(didl)}</Result><NumberReturned>${numberReturned}</NumberReturned>` +
    `${total}<UpdateID>${page.length === 0 && box.updateIdWhenEmpty !== null ? box.updateIdWhenEmpty : box.updateId}</UpdateID></u:BrowseResponse></s:Body></s:Envelope>`
}

const fieldOf = (body, name) => body.match(new RegExp(`<${name}>([^<]*)</${name}>`))?.[1]

const answerBrowse = (body) => {
  const request = {
    objectId: fieldOf(body, 'ObjectID'),
    startingIndex: Number(fieldOf(body, 'StartingIndex') ?? 0),
    requestedCount: Number(fieldOf(body, 'RequestedCount') ?? 0),
  }
  browses.push(request)
  box.onBrowse(request)
  const { objectId, startingIndex, requestedCount } = request
  if (box.resetsFromIndex !== null && startingIndex >= box.resetsFromIndex) return { status: 'reset' }
  if (box.goneFromIndex !== null && startingIndex >= box.goneFromIndex) return { status: 500, xml: NO_SUCH_OBJECT }
  if (box.failsFromIndex !== null && startingIndex >= box.failsFromIndex) return { status: 500, xml: '' }
  const entries = entriesFor(objectId)
  if (startingIndex >= entries.length && startingIndex > 0 && box.hangsPastEndOf(objectId)) return { status: 'hang' }
  const start = box.ignoresStartingIndex ? 0 : startingIndex
  const cap = box.capFor(objectId)
  const wanted = requestedCount > 0 ? Math.min(requestedCount, cap) : cap
  const page = entries.slice(start, start + wanted)
  const xml = browseEnvelope({ page, totalMatches: box.totalMatches(entries, page), numberReturned: box.numberReturned(page) })
  return { status: 200, xml }
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

const listShows = async ({ signal, onProgress = () => {} } = {}) => {
  const outcomes = []
  const folders = []
  const shows = await silenced(() => getFetchRecordings({
    location: location(),
    onProgress,
    signal,
    onOutcome: (outcome) => outcomes.push(outcome),
    onFolderRead: (folder) => folders.push(folder),
  }))
  return { shows, outcomes, folders, byTitle: Object.fromEntries(shows.map(show => [show.title, show])) }
}

const browseFolder = async (objectId) => {
  const failures = []
  const incompletes = []
  const didl = await silenced(() => browseRequest({
    apiService: apiService(),
    objectId,
    isGoneWhenMissing: objectId !== '0' && objectId !== RECORDINGS_ID,
    onFailure: (error) => failures.push(error),
    onIncomplete: (incomplete) => incompletes.push(incomplete),
  }))
  const ids = didl ? [didl.container, didl.item].flat().filter(Boolean).map(node => String(node['@_id'] ?? node.res?.['#text'])) : null
  return { didl, ids, failures, incompletes }
}

const startsFor = (objectId) => browses.filter(browse => browse.objectId === objectId).map(browse => browse.startingIndex)

const totalItems = () => box.shows.reduce((total, show) => total + show.items.length, 0)

before(async () => {
  server = createServer(async (req, res) => {
    if (req.url.startsWith('/item/')) {
      box.onItem(req)
      res.writeHead(200, { 'Content-Length': '1000' })
      return res.end()
    }
    if (req.method !== 'POST' || req.url !== CONTROL_PATH) {
      res.writeHead(404)
      return res.end()
    }
    const { status, xml } = answerBrowse(await collectBody(req))
    if (status === 'hang') return
    if (status === 'reset') return req.socket.destroy()
    res.writeHead(status, { 'Content-Type': 'text/xml' })
    res.end(xml)
  })
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
  baseUrl = `http://127.0.0.1:${server.address().port}`
  testInternals.setBrowseRetryDelay(10)
  testInternals.setBrowseTimeout(1000)
  testInternals.setMinBrowseDelay(1)
})

after(async () => {
  testInternals.setBrowseRetryDelay()
  testInternals.setBrowseTimeout()
  testInternals.setMinBrowseDelay()
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
  assert.deepEqual(outcomes, [{ outcome: LISTING_OUTCOME.OK }])
  assert.deepEqual(startsFor('100'), [0, 24, 45])
})

test('browse paging: a library with more shows than one reply lists every show', async () => {
  const { shows } = await listShows()
  assert.deepEqual(shows.map(show => show.title).sort(), box.shows.map(show => show.title).sort())
  assert.deepEqual(startsFor(RECORDINGS_ID), [0, 3, 5])
})

test('browse paging: every listed count matches the box count for every show', async () => {
  const { byTitle } = await listShows()
  box.shows.forEach(show => assert.equal(byTitle[show.title].items.length, show.items.length, show.title))
  assert.equal(Object.values(byTitle).reduce((total, show) => total + show.items.length, 0), 85)
})

test('browse paging: the first request per folder stays StartingIndex 0 with RequestedCount 0', async () => {
  await listShows()
  const firstRequests = browses.filter(browse => browse.startingIndex === 0)
  assert.equal(firstRequests.length, 2 + box.shows.length)
  browses.forEach(browse => assert.equal(browse.requestedCount, 0))
})

test('browse paging: a box with no cap costs one probe read per folder', async () => {
  box.capFor = () => 1000
  const { outcomes } = await listShows()
  const folders = 2 + box.shows.length
  assert.equal(browses.length, folders * 2)
  assert.deepEqual(outcomes, [{ outcome: LISTING_OUTCOME.OK }])
})

test('browse paging: a box whose TotalMatches equals each reply still lists every entry', async () => {
  box.totalMatches = (_entries, page) => page.length
  const { byTitle, outcomes } = await listShows()
  box.shows.forEach(show => assert.equal(byTitle[show.title].items.length, show.items.length, show.title))
  assert.deepEqual(outcomes, [{ outcome: LISTING_OUTCOME.OK }])
  assert.deepEqual(startsFor('100'), [0, 24, 45])
})

for (const [label, totalMatches] of [['0', () => 0], ['missing', () => null]]) {
  test(`browse paging: a box whose TotalMatches is ${label} is paged to the end`, async () => {
    box.totalMatches = totalMatches
    const { byTitle, outcomes } = await listShows()
    box.shows.forEach(show => assert.equal(byTitle[show.title].items.length, show.items.length, show.title))
    assert.deepEqual(outcomes, [{ outcome: LISTING_OUTCOME.OK }])
    assert.deepEqual(startsFor('100'), [0, 24, 45])
  })
}

test('browse paging: a box that ignores StartingIndex gives a partial list, counted by unique ids', async () => {
  box.ignoresStartingIndex = true
  box.shows = showsOf([30])
  const { ids, incompletes } = await browseFolder('100')
  assert.equal(ids.length, 24)
  assert.equal(new Set(ids).size, 24)
  assert.equal(incompletes.length, 1)
  assert.equal(incompletes[0].listed, 24)
  assert.equal(incompletes[0].total, 30)
})

test('browse paging: a box that ignores StartingIndex on a folder that fits one reply is complete', async () => {
  box.ignoresStartingIndex = true
  box.capFor = () => 1000
  const { ids, incompletes } = await browseFolder('100')
  assert.equal(ids.length, 45)
  assert.deepEqual(incompletes, [])
})

test('browse paging: a later page that keeps failing keeps the entries read and marks the show partial', async () => {
  box.failsFromIndex = 24
  const { byTitle, outcomes } = await listShows()
  assert.equal(byTitle['Show 100'].items.length, 24)
  assert.equal(byTitle['Show 104'].items.length, 24)
  assert.equal(byTitle['Show 103'].items.length, 7)
  assert.equal(outcomes.length, 1)
  assert.equal(outcomes[0].outcome, LISTING_OUTCOME.PARTIAL)
  assert.deepEqual([...outcomes[0].folders].sort(), ['Show 100', 'Show 104'])
})

test('browse paging: a first page that keeps failing fails the folder and is still reported as read', async () => {
  box.failsFromIndex = 0
  const { didl, failures } = await browseFolder('100')
  assert.equal(didl, null)
  assert.equal(failures.length, 1)
  box.shows = showsOf([3, 45])
  box.failsFromIndex = null
  box.onBrowse = ({ objectId }) => { box.failsFromIndex = objectId === '101' ? 0 : null }
  const { folders } = await listShows()
  assert.equal(folders.find(folder => folder.title === 'Show 101').isFailed, true)
})

test('browse paging: an overstated TotalMatches ending in an empty page keeps every entry and is partial', async () => {
  box.totalMatches = (entries) => entries.length + 10
  const { ids, incompletes } = await browseFolder('100')
  assert.equal(ids.length, 45)
  assert.equal(incompletes.length, 1)
  assert.deepEqual(startsFor('100'), [0, 24, 45])
})

test('browse paging: a box that says 45, lists 24, then sends an empty page is partial', async () => {
  box.shows = showsOf([24])
  box.totalMatches = () => 45
  const { ids, incompletes } = await browseFolder('100')
  assert.equal(ids.length, 24)
  assert.equal(incompletes.length, 1)
  assert.equal(incompletes[0].total, 45)
})

test('browse paging: a box that never answers past the end everywhere lists every folder as complete', async () => {
  box.hangsPastEndOf = () => true
  const { byTitle, outcomes, folders } = await listShows()
  box.shows.forEach(show => assert.equal(byTitle[show.title].items.length, show.items.length, show.title))
  assert.deepEqual(outcomes, [{ outcome: LISTING_OUTCOME.OK }])
  assert.ok(folders.every(folder => folder.probe === 'no-answer' && !folder.isPartial))
})

test('browse paging: a show that never answers past the end, on a box whose root answered, is partial', async () => {
  box.hangsPastEndOf = (objectId) => objectId === '100'
  const { byTitle, outcomes } = await listShows()
  assert.equal(byTitle['Show 100'].items.length, 45)
  assert.equal(byTitle['Show 100'].incomplete, true)
  assert.equal(outcomes[0].outcome, LISTING_OUTCOME.PARTIAL)
  assert.deepEqual(outcomes[0].folders, ['Show 100'])
})

test('browse paging: UpdateID 0 on an empty reply does not restart the folder', async () => {
  box.updateId = 5
  box.updateIdWhenEmpty = 0
  const { ids } = await browseFolder('100')
  assert.equal(ids.length, 45)
  assert.deepEqual(startsFor('100'), [0, 24, 45])
})

test('browse paging: a no-such-object error on page 2 of a show with no TotalMatches drops the show', async () => {
  box.totalMatches = () => 0
  box.goneFromIndex = 24
  const { didl, failures } = await browseFolder('100')
  assert.deepEqual(didl, {})
  assert.deepEqual(failures, [])
})

test('browse paging: each folder read is reported with its reads, total, unique entries, and probe result', async () => {
  const { folders } = await listShows()
  const show = folders.find(folder => folder.title === 'Show 100')
  assert.deepEqual(
    { reads: show.reads, totalMatches: show.totalMatches, unique: show.unique, probe: show.probe, isPartial: show.isPartial },
    { reads: 3, totalMatches: 45, unique: 45, probe: 'empty', isPartial: false }
  )
  assert.equal(folders.length, 2 + box.shows.length)
})

test('browse paging: each page takes its own queue slot, so a long show does not hold back the others', async () => {
  box.shows = showsOf([60, ...Array(10).fill(1)])
  box.capFor = (objectId) => (objectId === '100' ? 1 : 1000)
  await listShows()
  const showReads = browses.filter(browse => browse.objectId !== '0' && browse.objectId !== RECORDINGS_ID)
  const lastSmallShowRead = showReads.findLastIndex(browse => browse.objectId !== '100')
  const longShowReadsBefore = showReads.slice(0, lastSmallShowRead).filter(browse => browse.objectId === '100').length
  assert.ok(longShowReadsBefore < 10, `the long show had ${longShowReadsBefore} reads before the small shows finished`)
})

test('browse paging: a cancel while sizes are probed reports cancelled', async () => {
  const controller = new AbortController()
  box.shows = showsOf([2])
  box.unknownSizes = true
  box.onItem = () => controller.abort()
  const { shows, outcomes } = await listShows({ signal: controller.signal })
  assert.deepEqual(shows, [])
  assert.deepEqual(outcomes, [{ outcome: LISTING_OUTCOME.CANCELLED }])
})

test('browse paging: an error past the end after the TotalMatches entries is the end of the list', async () => {
  box.failsFromIndex = 45
  const { ids, incompletes, failures } = await browseFolder('100')
  assert.equal(ids.length, 45)
  assert.deepEqual(incompletes, [])
  assert.deepEqual(failures, [])
  assert.deepEqual(startsFor('100'), [0, 24, 45])
})

test('browse paging: an overstated TotalMatches with an error past the end keeps every entry and is marked partial', async () => {
  box.totalMatches = (entries) => entries.length + 10
  box.failsFromIndex = 45
  const { ids, incompletes, failures } = await browseFolder('100')
  assert.equal(ids.length, 45)
  assert.equal(incompletes.length, 1)
  assert.deepEqual(failures, [])
})

test('browse paging: an understated TotalMatches still reads to the end', async () => {
  box.totalMatches = (entries) => entries.length - 5
  const { ids } = await browseFolder('100')
  assert.equal(ids.length, 45)
})

test('browse paging: a recording deleted between pages restarts the folder once when UpdateID changes', async () => {
  box.onBrowse = ({ objectId, startingIndex }) => {
    if (objectId !== '100' || startingIndex !== 24 || box.updateId !== 1) return
    box.shows[0].items.splice(5, 1)
    box.updateId = 2
  }
  const { ids, incompletes } = await browseFolder('100')
  assert.equal(ids.length, 44)
  assert.ok(!ids.includes('100-5'))
  assert.ok(ids.includes('100-24'))
  assert.deepEqual(incompletes, [])
  assert.deepEqual(startsFor('100'), [0, 24, 0, 24, 44])
})

test('browse paging: a recording added between pages gives no duplicates', async () => {
  box.onBrowse = ({ objectId, startingIndex }) => {
    if (objectId !== '100' || startingIndex !== 24 || box.shows[0].items[0] === 'new') return
    box.shows[0].items.unshift('new')
  }
  const { ids } = await browseFolder('100')
  assert.equal(new Set(ids).size, ids.length)
  assert.equal(ids.length, 45)
})

test('browse paging: a show deleted between pages drops out without an error', async () => {
  box.goneFromIndex = 24
  const { didl, failures, incompletes } = await browseFolder('100')
  assert.deepEqual(didl, {})
  assert.deepEqual(failures, [])
  assert.deepEqual(incompletes, [])
})

test('browse paging: a no-such-object error past the end keeps every entry', async () => {
  box.goneFromIndex = 45
  const { ids, failures, incompletes } = await browseFolder('100')
  assert.equal(ids.length, 45)
  assert.deepEqual(failures, [])
  assert.deepEqual(incompletes, [])
})

test('browse paging: a dropped connection on the read past the end marks the folder partial', async () => {
  box.resetsFromIndex = 45
  const { ids, incompletes } = await browseFolder('100')
  assert.equal(ids.length, 45)
  assert.equal(incompletes.length, 1)
  assert.deepEqual(startsFor('100'), [0, 24, 45])
})

test('browse paging: an uncapped folder with one entry returns the same shape as one reply', async () => {
  box.capFor = () => 1000
  box.shows = showsOf([1])
  const { didl } = await browseFolder('100')
  assert.equal(Array.isArray(didl.item), false)
  assert.equal(didl.item['@_id'], '100-0')
})

test('browse paging: once a read past TotalMatches finds more, a later failed page is partial', async () => {
  box.totalMatches = (_entries, page) => page.length
  box.shows = showsOf([72])
  box.failsFromIndex = 48
  const { ids, incompletes, failures } = await browseFolder('100')
  assert.equal(ids.length, 48)
  assert.equal(incompletes.length, 1)
  assert.deepEqual(failures, [])
})

test('browse paging: NumberReturned that disagrees with the reply is ignored', async () => {
  box.numberReturned = (page) => page.length + 5
  const { ids } = await browseFolder('100')
  assert.equal(ids.length, 45)
  assert.deepEqual(startsFor('100'), [0, 24, 45])
})

test('browse paging: items with no id do not defeat the repeated-page check', async () => {
  box.idlessItems = true
  box.ignoresStartingIndex = true
  box.totalMatches = () => 0
  await browseFolder('100')
  assert.equal(browses.length, 2)
})

test('browse paging: the page guard stops a runaway folder and marks it partial', async () => {
  box.shows = showsOf([250])
  box.capFor = () => 1
  const { ids, incompletes } = await browseFolder('100')
  assert.equal(ids.length, 200)
  assert.equal(incompletes.length, 1)
  assert.equal(incompletes[0].total, 250)
})

test('browse paging: a large library lists every show and recording with the progress total from the full list', async () => {
  box.shows = showsOf(Array.from({ length: 250 }, (_, index) => 1 + (index % 60)))
  const progress = []
  const { shows, outcomes } = await listShows({ onProgress: (update) => progress.push(update) })
  assert.equal(shows.length, 250)
  assert.equal(shows.reduce((total, show) => total + show.items.length, 0), totalItems())
  assert.deepEqual(outcomes, [{ outcome: LISTING_OUTCOME.OK }])
  assert.equal(progress[0].total, 250)
  assert.equal(progress.at(-1).done, 250)
})

test('browse paging: a cancelled listing stops reading pages', async () => {
  const controller = new AbortController()
  box.shows = showsOf(Array.from({ length: 40 }, () => 45))
  box.onBrowse = ({ objectId }) => { if (objectId === '105') controller.abort() }
  const { shows, outcomes } = await listShows({ signal: controller.signal })
  await new Promise(resolve => setTimeout(resolve, 100))
  const browsesAtCancel = browses.length
  await new Promise(resolve => setTimeout(resolve, 200))
  assert.deepEqual(shows, [])
  assert.deepEqual(outcomes, [{ outcome: LISTING_OUTCOME.CANCELLED }])
  assert.equal(browses.length, browsesAtCancel)
  assert.ok(browsesAtCancel < 2 + 40 * 3, `${browsesAtCancel} reads`)
})

test('browse paging: a lone container or item on a later page still merges into the list', async () => {
  box.shows = showsOf([4])
  box.capFor = () => 1
  assert.deepEqual((await browseFolder(RECORDINGS_ID)).ids, ['100'])
  assert.deepEqual((await browseFolder('100')).ids, ['100-0', '100-1', '100-2', '100-3'])
})

test('request queue: a probe read skips the spacing between requests, other reads wait for it', async () => {
  const manager = createRequestManager({ initialDelay: 400, minDelay: 400 })
  const started = []
  const read = (name) => async () => { started.push({ name, at: Date.now() }) }
  const startedAt = Date.now()
  await Promise.all([
    manager.enqueue(read('first')),
    manager.enqueue(read('spaced')),
    manager.enqueue(read('probe'), undefined, { isProbe: true }),
  ])
  const at = Object.fromEntries(started.map(entry => [entry.name, entry.at - startedAt]))
  assert.ok(at.probe < 200, `the probe waited ${at.probe} ms`)
  assert.ok(at.spaced >= 350, `the spaced read started after ${at.spaced} ms`)
})

test('browse paging: a cancel with many shows queued settles at once instead of draining the queue', async () => {
  const controller = new AbortController()
  box.shows = showsOf(Array.from({ length: 40 }, () => 2))
  let abortedAt = null
  box.onBrowse = ({ objectId }) => {
    if (objectId !== '101' || abortedAt) return
    abortedAt = Date.now()
    controller.abort()
  }
  testInternals.setMinBrowseDelay(100)
  try {
    const { outcomes } = await listShows({ signal: controller.signal })
    const settledAfter = Date.now() - abortedAt
    assert.deepEqual(outcomes, [{ outcome: LISTING_OUTCOME.CANCELLED }])
    assert.ok(settledAfter < 500, `the listing settled ${settledAfter} ms after the cancel`)
  } finally {
    testInternals.setMinBrowseDelay(1)
  }
})

test('browse paging: probes that hang past the end do not slow the rest of the listing', async () => {
  box.shows = showsOf(Array.from({ length: 50 }, () => 2))
  box.capFor = () => 1000
  box.hangsPastEndOf = (objectId) => objectId !== '0' && objectId !== RECORDINGS_ID
  const startedAt = Date.now()
  const { shows } = await listShows()
  const ms = Date.now() - startedAt
  assert.equal(shows.length, 50)
  assert.ok(ms < 9000, `the listing took ${ms} ms`)
})
