import { test, beforeEach, afterEach, after } from 'node:test'
import assert from 'node:assert/strict'
import fs from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import nock from 'nock'

import {
  loadSavedFiles,
  addSavedFile,
  isLockFileStale,
  saveRecordings,
  downloadFile,
} from '../fetchtv.js'

let tmpDir = null

beforeEach(async () => {
  tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'fetchtv-save-'))
  nock.cleanAll()
})

afterEach(async () => {
  if (tmpDir) await fs.rm(tmpDir, { recursive: true, force: true })
  tmpDir = null
})

after(() => {
  nock.restore()
})

test('loadSavedFiles: returns {} when fetchtv.json does not exist', async () => {
  const result = await loadSavedFiles(tmpDir)
  assert.deepEqual(result, {})
})

test('loadSavedFiles: returns parsed contents when fetchtv.json exists', async () => {
  await fs.writeFile(path.join(tmpDir, 'fetchtv.json'), JSON.stringify({ '100': 'S1 E1' }))
  const result = await loadSavedFiles(tmpDir)
  assert.deepEqual(result, { '100': 'S1 E1' })
})

test('loadSavedFiles: returns {} when fetchtv.json is malformed', async () => {
  await fs.writeFile(path.join(tmpDir, 'fetchtv.json'), '{not valid json')
  const result = await loadSavedFiles(tmpDir)
  assert.deepEqual(result, {})
})

test('addSavedFile: persists an entry keyed by item id', async () => {
  const db = {}
  await addSavedFile({ savePath: tmpDir, savedFilesDb: db, item: { id: '200', title: 'S2 E4' } })

  const raw = await fs.readFile(path.join(tmpDir, 'fetchtv.json'), 'utf-8')
  assert.deepEqual(JSON.parse(raw), { '200': 'S2 E4' })
  assert.equal(db['200'], 'S2 E4')
})

test('addSavedFile: creates the save directory if it does not exist', async () => {
  const nested = path.join(tmpDir, 'does-not-exist-yet')
  const db = {}
  await addSavedFile({ savePath: nested, savedFilesDb: db, item: { id: '300', title: 'X' } })

  const raw = await fs.readFile(path.join(nested, 'fetchtv.json'), 'utf-8')
  assert.deepEqual(JSON.parse(raw), { '300': 'X' })
})

test('isLockFileStale: returns true when lock file is missing', async () => {
  const lockPath = path.join(tmpDir, 'ghost.lock')
  assert.equal(await isLockFileStale(lockPath), true)
})

test('isLockFileStale: returns false for a freshly-created lock file', async () => {
  const lockPath = path.join(tmpDir, 'fresh.lock')
  await fs.writeFile(lockPath, '')
  assert.equal(await isLockFileStale(lockPath), false)
})

test('isLockFileStale: returns true once the lock file is older than 10 minutes', async () => {
  const lockPath = path.join(tmpDir, 'old.lock')
  await fs.writeFile(lockPath, '')
  const ancient = (Date.now() - 11 * 60 * 1000) / 1000
  await fs.utimes(lockPath, ancient, ancient)
  assert.equal(await isLockFileStale(lockPath), true)
})

test('saveRecordings: writes downloaded files to disk and updates fetchtv.json', async () => {
  const host = 'http://192.168.1.80:49152'
  const itemPath = '/item/saved'
  const payload = Buffer.from('this-is-mock-mpeg-data')

  nock(host)
    .get(itemPath)
    .reply(200, payload, {
      'content-length': String(payload.length),
      'content-type': 'video/mpeg',
      'accept-ranges': 'bytes',
    })

  const recordings = [{
    title: 'Bluey',
    items: [{
      id: '900',
      title: 'S1 E5 - Pool',
      url: `${host}${itemPath}`,
      size: payload.length,
      ext: 'ts',
      season_number: '1',
      season_number_padded: '01',
      episode_number: '5',
      episode_number_padded: '05',
      item_type: 'episode',
    }],
  }]

  const results = await saveRecordings({
    recordings,
    savePath: tmpDir,
    template: null,
    overwrite: false,
  })

  const filePath = path.join(tmpDir, 'Bluey', 'S1 E5 - Pool.ts')
  const written = await fs.readFile(filePath)
  assert.equal(written.length, payload.length)

  const dbRaw = await fs.readFile(path.join(tmpDir, 'fetchtv.json'), 'utf-8')
  assert.deepEqual(JSON.parse(dbRaw), { '900': 'S1 E5 - Pool' })

  assert.ok(results.find(r => r.recorded === true), 'expected at least one recorded result')
})

test('saveRecordings: skips items already present in fetchtv.json', async () => {
  await fs.writeFile(
    path.join(tmpDir, 'fetchtv.json'),
    JSON.stringify({ '901': 'previously-saved' }),
  )
  const existingFilePath = path.join(tmpDir, 'Bluey', 'S1 E6 - Pool.ts')
  await fs.mkdir(path.dirname(existingFilePath), { recursive: true })
  await fs.writeFile(existingFilePath, 'pretend this is a complete file')

  const recordings = [{
    title: 'Bluey',
    items: [{
      id: '901',
      title: 'S1 E6 - Pool',
      url: 'http://nowhere.invalid/item/901',
      size: 31,
      ext: 'ts',
      season_number: '1',
      episode_number: '6',
      item_type: 'episode',
    }],
  }]

  const results = await saveRecordings({
    recordings,
    savePath: tmpDir,
    template: null,
    overwrite: false,
  })

  assert.equal(results.length, 0, 'no download tasks should be created for already-saved items')
})

test('downloadFile: refuses to download items with non-positive size', async () => {
  const result = await downloadFile({
    item: { title: 'still-recording', size: -1, url: 'http://nowhere/x' },
    filePath: path.join(tmpDir, 'still-recording.ts'),
    overwrite: false,
  })
  assert.equal(result.recorded, false)
  assert.match(result.warning, /currently recording/i)
})
