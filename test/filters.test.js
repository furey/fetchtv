import { test, beforeEach, after } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import path from 'node:path'
import nock from 'nock'

import { getFetchRecordings } from '../fetchtv.js'

const fixturesDir = path.join(path.dirname(fileURLToPath(import.meta.url)), 'fixtures')
const readFixture = (name) => readFileSync(path.join(fixturesDir, name), 'utf-8')

const HOST = 'http://192.168.1.60:49152'
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

const extractObjectId = (body) => {
  const match = String(body).match(/<ObjectID>([^<]+)<\/ObjectID>/)
  return match ? match[1] : null
}

const interceptBrowse = () => {
  nock(HOST)
    .post(CONTROL_PATH)
    .times(Infinity)
    .reply(200, (_uri, body) => {
      const objectId = extractObjectId(body)
      switch (objectId) {
        case '0': return readFixture('browse-root.xml')
        case '1': return readFixture('browse-recordings.xml')
        case '10':
        case '11':
        case '12':
          return readFixture('browse-show-items.xml')
        default:
          return readFixture('browse-empty.xml')
      }
    })
}

beforeEach(() => {
  nock.cleanAll()
  interceptBrowse()
})

after(() => {
  nock.restore()
})

test('getFetchRecordings: showsOnly returns folders without enumerating items', async () => {
  const results = await getFetchRecordings({
    location,
    filters: { folderFilter: [], excludeFilter: [], titleFilter: [], showsOnly: true },
  })
  const titles = results.map(r => r.title)
  assert.deepEqual(titles.sort(), ['Bluey', 'MasterChef Australia', 'The Amazing Race'])
  for (const r of results) assert.deepEqual(r.items, [])
})

test('getFetchRecordings: --show filters folders by case-insensitive substring', async () => {
  const results = await getFetchRecordings({
    location,
    filters: { folderFilter: ['bluey'], excludeFilter: [], titleFilter: [], showsOnly: true },
  })
  assert.equal(results.length, 1)
  assert.equal(results[0].title, 'Bluey')
})

test('getFetchRecordings: --exclude removes matching folders', async () => {
  const results = await getFetchRecordings({
    location,
    filters: { folderFilter: [], excludeFilter: ['masterchef'], titleFilter: [], showsOnly: true },
  })
  const titles = results.map(r => r.title)
  assert.ok(!titles.includes('MasterChef Australia'))
  assert.ok(titles.includes('Bluey'))
  assert.ok(titles.includes('The Amazing Race'))
})

test('getFetchRecordings: --show + --exclude apply together (include then exclude)', async () => {
  const results = await getFetchRecordings({
    location,
    filters: {
      folderFilter: ['the', 'bluey'],
      excludeFilter: ['amazing'],
      titleFilter: [],
      showsOnly: true,
    },
  })
  const titles = results.map(r => r.title)
  assert.deepEqual(titles, ['Bluey'])
})

test('getFetchRecordings: --title filters episode items by case-insensitive substring', async () => {
  const results = await getFetchRecordings({
    location,
    filters: {
      folderFilter: ['bluey'],
      excludeFilter: [],
      titleFilter: ['hospital'],
      showsOnly: false,
    },
  })
  assert.equal(results.length, 1)
  assert.equal(results[0].items.length, 1)
  assert.match(results[0].items[0].title, /Hospital/i)
})

test('getFetchRecordings: empty title filter returns all episodes', async () => {
  const results = await getFetchRecordings({
    location,
    filters: {
      folderFilter: ['bluey'],
      excludeFilter: [],
      titleFilter: [],
      showsOnly: false,
    },
  })
  assert.equal(results[0].items.length, 3)
})

test('getFetchRecordings: drops shows with zero matching items after title filter', async () => {
  const results = await getFetchRecordings({
    location,
    filters: {
      folderFilter: [],
      excludeFilter: [],
      titleFilter: ['does-not-match-any-title'],
      showsOnly: false,
    },
  })
  assert.deepEqual(results, [])
})
