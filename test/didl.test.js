import { test, beforeEach, after } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import path from 'node:path'
import nock from 'nock'

import { findItems, findDirectories, requestCache } from '../fetchtv.js'

const fixturesDir = path.join(path.dirname(fileURLToPath(import.meta.url)), 'fixtures')
const readFixture = (name) => readFileSync(path.join(fixturesDir, name), 'utf-8')

const HOST = 'http://192.168.1.10:49152'
const CONTROL_PATH = '/upnp/control/ContentDirectory'
const SERVICE_TYPE = 'urn:schemas-upnp-org:service:ContentDirectory:1'

const apiService = { cd_ctr: `${HOST}${CONTROL_PATH}`, cd_service: SERVICE_TYPE }

beforeEach(() => {
  requestCache.clear()
  nock.cleanAll()
})

after(() => {
  nock.restore()
})

test('findDirectories: parses root containers from a SOAP Browse response', async () => {
  nock(HOST).post(CONTROL_PATH).reply(200, readFixture('browse-root.xml'))

  const dirs = await findDirectories({ apiService, objectId: 'root-a' })
  assert.equal(dirs.length, 2)
  assert.equal(dirs[0].title, 'Recordings')
  assert.equal(String(dirs[0].id), '1')
  assert.equal(String(dirs[0].parent_id), '0')
  assert.equal(dirs[1].title, 'Live TV')
})

test('findDirectories: parses the Recordings folder children', async () => {
  nock(HOST).post(CONTROL_PATH).reply(200, readFixture('browse-recordings.xml'))

  const dirs = await findDirectories({ apiService, objectId: 'rec-b' })
  const titles = dirs.map(d => d.title)
  assert.deepEqual(titles, ['Bluey', 'The Amazing Race', 'MasterChef Australia'])
})

test('findItems: extracts season/episode numbers from "S1 E2" titles', async () => {
  nock(HOST).post(CONTROL_PATH).reply(200, readFixture('browse-show-items.xml'))

  const items = await findItems({ apiService, objectId: 'items-d', showTitle: 'Bluey' })
  assert.equal(items[0].season_number, '1')
  assert.equal(items[0].season_number_padded, '01')
  assert.equal(items[0].episode_number, '1')
  assert.equal(items[0].episode_number_padded, '01')
  assert.equal(items[0].show_title, 'Bluey')
})

test('findItems: extension is "ts" for mpeg-tts protocolInfo', async () => {
  nock(HOST).post(CONTROL_PATH).reply(200, readFixture('browse-show-items.xml'))
  const items = await findItems({ apiService, objectId: 'items-e', showTitle: 'Bluey' })
  assert.equal(items[0].ext, 'ts')
})

test('findItems: extension is "mp4" for video/mp4 protocolInfo', async () => {
  nock(HOST).post(CONTROL_PATH).reply(200, readFixture('browse-show-items.xml'))
  const items = await findItems({ apiService, objectId: 'items-f', showTitle: 'Bluey' })
  assert.equal(items[1].ext, 'mp4')
})

test('findItems: marks size=-1 items as item_type movie/episode based on S/E presence', async () => {
  nock(HOST).post(CONTROL_PATH).reply(200, readFixture('browse-show-items.xml'))
  const items = await findItems({ apiService, objectId: 'items-g', showTitle: 'Bluey' })
  assert.equal(items[2].size, -1)
  assert.equal(items[2].item_type, 'movie')
})

test('findItems: parses duration "1:25:30" to seconds', async () => {
  nock(HOST).post(CONTROL_PATH).reply(200, readFixture('browse-show-items.xml'))
  const items = await findItems({ apiService, objectId: 'items-h', showTitle: 'Bluey' })
  assert.equal(items[0].duration, 1 * 3600 + 25 * 60 + 30)
})

test('findItems: returns size as integer from res @size attribute', async () => {
  nock(HOST).post(CONTROL_PATH).reply(200, readFixture('browse-show-items.xml'))
  const items = await findItems({ apiService, objectId: 'items-i', showTitle: 'Bluey' })
  assert.equal(items[0].size, 1073741824)
  assert.equal(typeof items[0].size, 'number')
})
