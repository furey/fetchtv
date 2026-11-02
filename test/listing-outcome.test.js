import { test, beforeEach, after } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import path from 'node:path'
import nock from 'nock'

import { getFetchRecordings, LISTING_OUTCOME } from '../fetchtv.js'

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

const objectIdOf = (body) => String(body).match(/<ObjectID>([^<]+)<\/ObjectID>/)?.[1]

const serveBrowse = (replies) => {
  nock(HOST)
    .post(CONTROL_PATH)
    .times(Infinity)
    .reply((_uri, body) => {
      const reply = replies[objectIdOf(body)] ?? 'browse-empty.xml'
      return reply === 500 ? [500, 'Internal Server Error'] : [200, readFixture(reply)]
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
