import { test, beforeEach, after } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import path from 'node:path'
import nock from 'nock'

import { getFetchRecordings, parseLocations, LISTING_OUTCOME } from '../fetchtv.js'

const fixturesDir = path.join(path.dirname(fileURLToPath(import.meta.url)), 'fixtures')
const readFixture = (name) => readFileSync(path.join(fixturesDir, name), 'utf-8')

const HOST = 'http://192.168.1.61:49152'
const CONTROL_PATH = '/upnp/control/ContentDirectory'

const location = {
  url: `${HOST}/MediaServer.xml`,
  _rawDeviceXml: {
    serviceList: {
      service: {
        serviceType: 'urn:schemas-upnp-org:service:ContentDirectory:1',
        controlURL: CONTROL_PATH,
      },
    },
  },
}

const BOX_HEADERS = { SERVER: 'Linux/4.1.20-1.9, UPnP/1.0, DLNADOC/1.50', CONNECTION: 'close' }

const objectIdOf = (body) => String(body).match(/<ObjectID>([^<]+)<\/ObjectID>/)?.[1]

const serveBrowse = (replies) => {
  nock(HOST)
    .post(CONTROL_PATH)
    .times(Infinity)
    .reply((_uri, body) => {
      const reply = replies[objectIdOf(body)] ?? 'browse-empty.xml'
      if (reply === 500) return [500, 'Internal Server Error']
      if (reply === 501) return [500, readFixture('upnp-error-501.xml'), BOX_HEADERS]
      return [200, readFixture(reply)]
    })
}

const listWithOutcome = async (listLocation = location) => {
  const outcomes = []
  const original = console.log
  console.log = () => {}
  try {
    const shows = await getFetchRecordings({ location: listLocation, onOutcome: (outcome) => outcomes.push(outcome) })
    return { shows, outcomes }
  } finally {
    console.log = original
  }
}

beforeEach(() => nock.cleanAll())

after(() => nock.restore())

test('listing outcome: no ContentDirectory service', async () => {
  const { shows, outcomes } = await listWithOutcome({ url: `${HOST}/MediaServer.xml` })
  assert.deepEqual(shows, [])
  assert.deepEqual(outcomes, [{ outcome: LISTING_OUTCOME.NO_CONTENT_DIRECTORY }])
  assert.equal(LISTING_OUTCOME.NO_CONTENT_DIRECTORY, 'no-content-directory')
})

test('listing outcome: the root browse fails after retries', async () => {
  serveBrowse({ 0: 500 })
  const { shows, outcomes } = await listWithOutcome()
  assert.deepEqual(shows, [])
  assert.equal(outcomes.length, 1)
  assert.equal(outcomes[0].outcome, 'browse-failed')
  assert.match(outcomes[0].error, /500/)
})

test('listing outcome: no Recordings folder, with the folders the box listed', async () => {
  serveBrowse({ 0: 'browse-empty.xml' })
  const { shows, outcomes } = await listWithOutcome()
  assert.deepEqual(shows, [])
  assert.deepEqual(outcomes, [{ outcome: 'no-recordings-folder', folders: [] }])
})

test('listing outcome: the Recordings browse fails after retries', async () => {
  serveBrowse({ 0: 'browse-root.xml', 1: 500 })
  const { shows, outcomes } = await listWithOutcome()
  assert.deepEqual(shows, [])
  assert.equal(outcomes[0].outcome, 'recordings-browse-failed')
  assert.match(outcomes[0].error, /500/)
})

test('listing outcome: an empty Recordings folder', async () => {
  serveBrowse({ 0: 'browse-root.xml', 1: 'browse-empty.xml' })
  const { shows, outcomes } = await listWithOutcome()
  assert.deepEqual(shows, [])
  assert.deepEqual(outcomes, [{ outcome: 'empty' }])
})

test('listing outcome: a box with recordings', async () => {
  serveBrowse({ 0: 'browse-root.xml', 1: 'browse-recordings.xml', 10: 'browse-show-items.xml', 11: 'browse-show-items.xml', 12: 'browse-show-items.xml' })
  const { shows, outcomes } = await listWithOutcome()
  assert.ok(shows.length > 0)
  assert.deepEqual(outcomes, [{ outcome: 'ok' }])
})

test('listing outcome: every show browse fails after retries', async () => {
  serveBrowse({ 0: 'browse-root.xml', 1: 'browse-recordings.xml', 10: 500, 11: 500, 12: 500 })
  const { shows, outcomes } = await listWithOutcome()
  assert.deepEqual(shows, [])
  assert.equal(outcomes.length, 1)
  assert.equal(outcomes[0].outcome, 'items-browse-failed')
  assert.match(outcomes[0].error, /500/)
})

test('listing outcome: some show browses fail, the rest still list', async () => {
  serveBrowse({ 0: 'browse-root.xml', 1: 'browse-recordings.xml', 10: 'browse-show-items.xml', 11: 500, 12: 'browse-show-items.xml' })
  const { shows, outcomes } = await listWithOutcome()
  assert.deepEqual(shows.map(show => show.id), [10, 12])
  assert.deepEqual(outcomes, [{ outcome: 'ok' }])
})

test('listing outcome: a show the box no longer has is skipped at once, and the rest still list', async () => {
  let goneBrowses = 0
  nock(HOST)
    .post(CONTROL_PATH)
    .times(Infinity)
    .reply((_uri, body) => {
      const objectId = objectIdOf(body)
      if (objectId === '11') goneBrowses++
      if (objectId === '11') return [500, readFixture('upnp-error-501.xml'), BOX_HEADERS]
      return [200, readFixture({ 0: 'browse-root.xml', 1: 'browse-recordings.xml' }[objectId] ?? 'browse-show-items.xml')]
    })
  const startedAt = Date.now()
  const { shows, outcomes } = await listWithOutcome()
  assert.deepEqual(shows.map(show => show.id), [10, 12])
  assert.deepEqual(outcomes, [{ outcome: 'ok' }])
  assert.equal(goneBrowses, 1)
  assert.ok(Date.now() - startedAt < 1000)
})

test('listing outcome: every show gone lists no shows, not a failed browse', async () => {
  serveBrowse({ 0: 'browse-root.xml', 1: 'browse-recordings.xml', 10: 501, 11: 501, 12: 501 })
  const { shows, outcomes } = await listWithOutcome()
  assert.deepEqual(shows, [])
  assert.deepEqual(outcomes, [{ outcome: 'ok' }])
})

test('listing outcome: a cancelled box, from the real captures, lists an empty Recordings folder', async () => {
  const BOX = 'http://192.168.86.224:49152'
  nock(BOX).get('/MediaServer.xml').reply(200, readFixture('cancelled-media-server.xml'), { 'CONTENT-TYPE': 'text/xml; charset="utf-8"', ...BOX_HEADERS })
  nock(BOX)
    .post('/web/cds_control')
    .times(Infinity)
    .reply((_uri, body) => {
      const objectId = objectIdOf(body)
      if (objectId === '0') return [200, readFixture('cancelled-browse-root.xml'), BOX_HEADERS]
      if (objectId === '1' || objectId === '2') return [200, readFixture('cancelled-browse-empty.xml'), BOX_HEADERS]
      return [500, readFixture('upnp-error-501.xml'), BOX_HEADERS]
    })
  const [boxLocation] = await parseLocations([`${BOX}/MediaServer.xml`])
  assert.equal(boxLocation.manufacturerURL, 'http://www.fetch.com/')
  const { shows, outcomes } = await listWithOutcome(boxLocation)
  assert.deepEqual(shows, [])
  assert.deepEqual(outcomes, [{ outcome: LISTING_OUTCOME.EMPTY }])
})
