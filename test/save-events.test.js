import { test, before, after, beforeEach, afterEach } from 'node:test'
import assert from 'node:assert/strict'
import { createServer } from 'node:http'
import fs from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'

import { saveRecordings, getFetchRecordings } from '../fetchtv.js'

const BODY = Buffer.alloc(256 * 1024, 'klmnopqrst')
const CHUNK = 16 * 1024
const SHOW = 'Harbour Lights'

let server = null
let baseUrl = null
let tmpDir = null

const sendSlowly = (res) => {
  res.writeHead(200, { 'Content-Length': String(BODY.length) })
  let offset = 0
  const timer = setInterval(() => {
    if (res.destroyed || offset >= BODY.length) {
      clearInterval(timer)
      if (!res.destroyed) res.end()
      return
    }
    res.write(BODY.subarray(offset, offset + CHUNK))
    offset += CHUNK
  }, 20)
  res.on('close', () => clearInterval(timer))
}

let rangeStarts = []

const rangeStartOf = (req) => {
  const match = String(req.headers.range || '').match(/bytes=(\d+)-/)
  return match ? parseInt(match[1], 10) : null
}

const routes = {
  '/full': (res, start) => {
    if (start === null) {
      res.writeHead(200, { 'Content-Length': String(BODY.length) })
      return res.end(BODY)
    }
    res.writeHead(206, {
      'Content-Length': String(BODY.length - start),
      'Content-Range': `bytes ${start}-${BODY.length - 1}/${BODY.length}`,
    })
    res.end(BODY.subarray(start))
  },
  '/slow': (res) => sendSlowly(res),
}

const makeItem = ({ id, route }) => ({
  id,
  title: `S1 E${id} - Test`,
  url: `${baseUrl}${route}`,
  size: BODY.length,
  ext: 'ts',
  season_number: '1',
  season_number_padded: '01',
  episode_number: String(id),
  episode_number_padded: String(id).padStart(2, '0'),
  item_type: 'episode',
})

const filePathFor = (item) => path.join(tmpDir, SHOW, `${item.title}.ts`)

const exists = (filePath) => fs.access(filePath).then(() => true, () => false)

const quietly = async (fn) => {
  const original = console.log
  console.log = () => {}
  try {
    return await fn()
  } finally {
    console.log = original
  }
}

before(async () => {
  server = createServer((req, res) => {
    const handler = routes[req.url]
    if (!handler) {
      res.writeHead(404)
      return res.end()
    }
    const start = rangeStartOf(req)
    rangeStarts.push(start)
    handler(res, start)
  })
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
  baseUrl = `http://127.0.0.1:${server.address().port}`
})

after(async () => {
  server.closeAllConnections()
  await new Promise(resolve => server.close(resolve))
})

beforeEach(async () => {
  tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'fetchtv-events-'))
  rangeStarts = []
})

afterEach(async () => {
  if (tmpDir) await fs.rm(tmpDir, { recursive: true, force: true })
  tmpDir = null
})

test('events: onProgress reports bytes and onResult reports each result as it finishes', async () => {
  const item = makeItem({ id: 1, route: '/full' })
  const progress = []
  const results = []
  const returned = await quietly(() => saveRecordings({
    recordings: [{ title: SHOW, items: [item] }],
    savePath: tmpDir,
    retryDelays: [],
    onProgress: (event) => progress.push(event),
    onResult: (result) => results.push(result),
  }))

  assert.deepEqual(results, returned)
  assert.equal(results[0].status, 'saved')
  assert.equal(results[0].item.id, 1)
  assert.ok(progress.every(event => event.id === 1 && event.filePath === filePathFor(item)))
  assert.equal(progress.at(-1).bytes, BODY.length)
  assert.equal(progress.at(-1).total, BODY.length)
})

test('events: onResult reports a skip for a recording that is already saved', async () => {
  const item = makeItem({ id: 2, route: '/full' })
  const recordings = [{ title: SHOW, items: [item] }]
  await quietly(() => saveRecordings({ recordings, savePath: tmpDir, retryDelays: [], onProgress: () => {} }))

  const results = []
  await quietly(() => saveRecordings({ recordings, savePath: tmpDir, retryDelays: [], onProgress: () => {}, onResult: (result) => results.push(result) }))
  assert.deepEqual(results.map(result => result.status), ['already_saved'])
})

test('cancel: aborting stops the running copy, keeps its partial file, removes its lock, and cancels the rest', async () => {
  const running = makeItem({ id: 3, route: '/slow' })
  const waiting = makeItem({ id: 4, route: '/slow' })
  const controller = new AbortController()
  const onProgress = ({ bytes }) => {
    if (bytes >= CHUNK * 3) controller.abort()
  }

  const results = await quietly(() => saveRecordings({
    recordings: [{ title: SHOW, items: [running, waiting] }],
    savePath: tmpDir,
    concurrency: 1,
    retryDelays: [10, 10],
    onProgress,
    signal: controller.signal,
  }))

  const statusById = Object.fromEntries(results.map(result => [result.item.id, result.status]))
  assert.deepEqual(statusById, { 3: 'cancelled', 4: 'cancelled' })
  const partSize = (await fs.stat(`${filePathFor(running)}.part`)).size
  assert.ok(partSize > 0 && partSize < BODY.length)
  assert.equal(await exists(`${filePathFor(running)}.lock`), false)
  assert.equal(await exists(filePathFor(waiting)), false)
})

test('cancel: a save with a signal leaves the process signal handlers alone', async () => {
  const before = process.listenerCount('SIGINT')
  await quietly(() => saveRecordings({
    recordings: [{ title: SHOW, items: [makeItem({ id: 5, route: '/full' })] }],
    savePath: tmpDir,
    retryDelays: [],
    onProgress: () => {},
    signal: new AbortController().signal,
  }))
  assert.equal(process.listenerCount('SIGINT'), before)
})

test('cancel: a re-run after a stop resumes the partial file and saves it', async () => {
  const item = makeItem({ id: 6, route: '/slow' })
  const recordings = [{ title: SHOW, items: [item] }]
  const controller = new AbortController()
  await quietly(() => saveRecordings({
    recordings,
    savePath: tmpDir,
    retryDelays: [],
    onProgress: ({ bytes }) => { if (bytes >= CHUNK * 2) controller.abort() },
    signal: controller.signal,
  }))

  const partSize = (await fs.stat(`${filePathFor(item)}.part`)).size
  item.url = `${baseUrl}/full`
  rangeStarts = []
  const results = await quietly(() => saveRecordings({ recordings, savePath: tmpDir, retryDelays: [], onProgress: () => {} }))
  assert.equal(results[0].status, 'saved')
  assert.deepEqual(rangeStarts, [partSize])
  assert.equal((await fs.stat(filePathFor(item))).size, BODY.length)
})

test('listing: getFetchRecordings accepts empty filters and reports nothing to read without a ContentDirectory', async () => {
  const progress = []
  const shows = await quietly(() => getFetchRecordings({ location: { url: baseUrl }, onProgress: (event) => progress.push(event) }))
  assert.deepEqual(shows, [])
  assert.deepEqual(progress, [])
})
