import { test, before, after, beforeEach, afterEach } from 'node:test'
import assert from 'node:assert/strict'
import { createServer } from 'node:http'
import { spawn } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import fs from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'

import {
  saveRecordings,
  handleSaveAction,
  summariseSaveResults,
  formatSaveSummary,
} from '../fetchtv.js'

const projectRoot = path.dirname(path.dirname(fileURLToPath(import.meta.url)))
const cliPath = path.join(projectRoot, 'fetchtv.js')

const BODY = Buffer.alloc(64 * 1024, 'abcdefghij')
const HALF = BODY.length / 2
const NO_RETRIES = []
const FAST_RETRIES = [10, 10]

let server = null
let baseUrl = null
let tmpDir = null
let requestLog = []

const parseRangeStart = (rangeHeader) => {
  const match = String(rangeHeader || '').match(/bytes=(\d+)-/)
  return match ? parseInt(match[1], 10) : null
}

const sendRemainder = ({ res, start }) => {
  res.writeHead(206, {
    'Content-Length': String(BODY.length - start),
    'Content-Range': `bytes ${start}-${BODY.length - 1}/${BODY.length}`,
  })
  res.end(BODY.subarray(start))
}

const sendHalfThenReset = (res) => {
  res.writeHead(200, { 'Content-Length': String(BODY.length) })
  res.write(BODY.subarray(0, HALF), () => setTimeout(() => res.socket.destroy(), 20))
}

const routes = {
  '/full': ({ res, start }) => {
    if (start !== null) return sendRemainder({ res, start })
    res.writeHead(200, { 'Content-Length': String(BODY.length) })
    res.end(BODY)
  },
  '/short': ({ res, start }) => {
    if (start !== null) return sendRemainder({ res, start })
    res.writeHead(200, { 'Content-Length': String(HALF) })
    res.end(BODY.subarray(0, HALF))
  },
  '/reset': ({ res, start }) => {
    if (start !== null) return sendRemainder({ res, start })
    sendHalfThenReset(res)
  },
  '/always-reset': ({ res }) => sendHalfThenReset(res),
}

const makeRecordings = (items) => [{ title: 'Bluey', items }]

const makeItem = ({ id, route, title = `S1 E${id} - Test`, size = BODY.length }) => ({
  id,
  title,
  url: `${baseUrl}${route}`,
  size,
  ext: 'ts',
  season_number: '1',
  season_number_padded: '01',
  episode_number: String(id),
  episode_number_padded: String(id).padStart(2, '0'),
  item_type: 'episode',
})

const filePathFor = (item) => path.join(tmpDir, 'Bluey', `${item.title}.ts`)

const readDb = async () => JSON.parse(await fs.readFile(path.join(tmpDir, 'fetchtv.json'), 'utf-8'))

const writeDb = (db) => fs.writeFile(path.join(tmpDir, 'fetchtv.json'), JSON.stringify(db))

const writePartial = async (item, bytes) => {
  await fs.mkdir(path.dirname(filePathFor(item)), { recursive: true })
  await fs.writeFile(filePathFor(item), BODY.subarray(0, bytes))
}

const fileSize = async (item) => (await fs.stat(filePathFor(item))).size

const requestsFor = (route) => requestLog.filter(entry => entry.route === route)

const silenceConsole = async (fn) => {
  const lines = []
  const original = console.log
  console.log = (...args) => lines.push(args.join(' '))
  try {
    return { result: await fn(), lines }
  } finally {
    console.log = original
  }
}

const runCli = (args) =>
  new Promise((resolve) => {
    const child = spawn(process.execPath, [cliPath, ...args], {
      env: { ...process.env, FORCE_COLOR: '0', NO_COLOR: '1' },
    })
    let stdout = ''
    let stderr = ''
    child.stdout.on('data', (chunk) => { stdout += chunk })
    child.stderr.on('data', (chunk) => { stderr += chunk })
    child.on('close', (code) => resolve({ code, stdout, stderr }))
  })

before(async () => {
  server = createServer((req, res) => {
    const route = req.url
    const start = parseRangeStart(req.headers.range)
    requestLog.push({ route, start })
    const handler = routes[route]
    if (!handler) {
      res.writeHead(404)
      res.end()
      return
    }
    handler({ res, start })
  })
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
  baseUrl = `http://127.0.0.1:${server.address().port}`
})

after(async () => {
  await new Promise(resolve => server.close(resolve))
})

beforeEach(async () => {
  tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'fetchtv-reliability-'))
  requestLog = []
})

afterEach(async () => {
  if (tmpDir) await fs.rm(tmpDir, { recursive: true, force: true })
  tmpDir = null
  process.exitCode = undefined
})

test('download: a full body is saved and fetchtv.json stores its size', async () => {
  const item = makeItem({ id: '1', route: '/full' })
  const { result } = await silenceConsole(() =>
    saveRecordings({ recordings: makeRecordings([item]), savePath: tmpDir, retryDelays: NO_RETRIES }))

  assert.equal(result[0].status, 'saved')
  assert.equal(await fileSize(item), BODY.length)
  assert.deepEqual((await readDb())['1'], { title: item.title, size: BODY.length })
})

test('download: a short body is not marked saved and keeps the partial file', async () => {
  const item = makeItem({ id: '2', route: '/short' })
  const { result } = await silenceConsole(() =>
    saveRecordings({ recordings: makeRecordings([item]), savePath: tmpDir, retryDelays: NO_RETRIES }))

  assert.equal(result[0].recorded, false)
  assert.equal(result[0].status, 'failed')
  assert.equal(await fileSize(item), HALF)
  assert.equal((await readDb().catch(() => ({})))['2'], undefined)
})

test('download: a mid-stream reset is not marked saved and keeps the partial file', async () => {
  const item = makeItem({ id: '3', route: '/always-reset' })
  const { result } = await silenceConsole(() =>
    saveRecordings({ recordings: makeRecordings([item]), savePath: tmpDir, retryDelays: NO_RETRIES }))

  assert.equal(result[0].recorded, false)
  assert.match(result[0].warning, /Download interrupted .* at 50\.0%/)
  assert.equal(result[0].status, 'failed')
  assert.equal(await fileSize(item), HALF)
  assert.equal((await readDb().catch(() => ({})))['3'], undefined)
})

test('retry: a mid-stream reset resumes with a Range request in the same run', async () => {
  const item = makeItem({ id: '4', route: '/reset' })
  const { result } = await silenceConsole(() =>
    saveRecordings({ recordings: makeRecordings([item]), savePath: tmpDir, retryDelays: FAST_RETRIES }))

  assert.equal(result[0].status, 'saved')
  assert.equal(result[0].attempts, 2)
  assert.deepEqual(requestsFor('/reset').map(entry => entry.start), [null, HALF])
  assert.ok((await fs.readFile(filePathFor(item))).equals(BODY))
})

test('retry: a short body resumes with a Range request in the same run', async () => {
  const item = makeItem({ id: '5', route: '/short' })
  const { result } = await silenceConsole(() =>
    saveRecordings({ recordings: makeRecordings([item]), savePath: tmpDir, retryDelays: FAST_RETRIES }))

  assert.equal(result[0].status, 'saved')
  assert.deepEqual(requestsFor('/short').map(entry => entry.start), [null, HALF])
  assert.ok((await fs.readFile(filePathFor(item))).equals(BODY))
})

test('retry: gives up after the configured retries', async () => {
  const item = makeItem({ id: '6', route: '/always-reset' })
  const { result } = await silenceConsole(() =>
    saveRecordings({ recordings: makeRecordings([item]), savePath: tmpDir, retryDelays: FAST_RETRIES }))

  assert.equal(result[0].status, 'failed')
  assert.equal(result[0].attempts, 3)
  assert.equal(requestsFor('/always-reset').length, 3)
})

test('re-run: a failed reset from an earlier run resumes from the partial file', async () => {
  const item = makeItem({ id: '7', route: '/reset' })
  await silenceConsole(() =>
    saveRecordings({ recordings: makeRecordings([item]), savePath: tmpDir, retryDelays: NO_RETRIES }))
  const { result } = await silenceConsole(() =>
    saveRecordings({ recordings: makeRecordings([item]), savePath: tmpDir, retryDelays: NO_RETRIES }))

  assert.equal(result[0].status, 'saved')
  assert.equal(result[0].resumed, true)
  assert.deepEqual(requestsFor('/reset').map(entry => entry.start), [null, HALF])
})

test('skip: a saved entry whose file is shorter than the stored size is resumed', async () => {
  const item = makeItem({ id: '8', route: '/full' })
  await writeDb({ '8': { title: item.title, size: BODY.length } })
  await writePartial(item, HALF)
  const { result } = await silenceConsole(() =>
    saveRecordings({ recordings: makeRecordings([{ ...item, size: 0 }]), savePath: tmpDir, retryDelays: NO_RETRIES }))

  assert.equal(requestsFor('/full').length, 0)
  assert.equal(result[0].status, 'still_recording')

  const { result: rerun } = await silenceConsole(() =>
    saveRecordings({ recordings: makeRecordings([item]), savePath: tmpDir, retryDelays: NO_RETRIES }))
  assert.equal(rerun[0].status, 'saved')
  assert.deepEqual(requestsFor('/full').map(entry => entry.start), [HALF])
  assert.equal(await fileSize(item), BODY.length)
})

test('skip: an old-format entry with a short file and a known item size is resumed', async () => {
  const item = makeItem({ id: '9', route: '/full' })
  await writeDb({ '9': item.title })
  await writePartial(item, HALF)
  const { result } = await silenceConsole(() =>
    saveRecordings({ recordings: makeRecordings([item]), savePath: tmpDir, retryDelays: NO_RETRIES }))

  assert.equal(result[0].status, 'saved')
  assert.deepEqual(requestsFor('/full').map(entry => entry.start), [HALF])
  assert.deepEqual((await readDb())['9'], { title: item.title, size: BODY.length })
})

test('skip: a saved entry whose file is missing is saved again', async () => {
  const item = makeItem({ id: '10', route: '/full' })
  await writeDb({ '10': { title: item.title, size: BODY.length } })
  const { result } = await silenceConsole(() =>
    saveRecordings({ recordings: makeRecordings([item]), savePath: tmpDir, retryDelays: NO_RETRIES }))

  assert.equal(result[0].status, 'saved')
  assert.deepEqual(requestsFor('/full').map(entry => entry.start), [null])
})

test('skip: complete files in old and new formats are skipped without a request', async () => {
  const oldItem = makeItem({ id: '11', route: '/full' })
  const newItem = makeItem({ id: '12', route: '/full' })
  await writeDb({ '11': oldItem.title, '12': { title: newItem.title, size: BODY.length } })
  await writePartial(oldItem, BODY.length)
  await writePartial(newItem, BODY.length)
  const { result } = await silenceConsole(() =>
    saveRecordings({ recordings: makeRecordings([oldItem, newItem]), savePath: tmpDir, retryDelays: NO_RETRIES }))

  assert.deepEqual(result.map(entry => entry.status), ['already_saved', 'already_saved'])
  assert.equal(requestLog.length, 0)
})

test('summary: counts each status and formats the final line', () => {
  const results = [
    ...Array(3).fill({ status: 'saved' }),
    ...Array(4).fill({ status: 'already_saved' }),
    ...Array(2).fill({ status: 'failed' }),
    { status: 'still_recording' },
  ]
  const summary = summariseSaveResults(results)

  assert.deepEqual(summary, {
    total: 10, saved: 3, alreadySaved: 4, failed: 2, stillRecording: 1, locked: 0, remaining: 3,
  })
  assert.equal(
    formatSaveSummary(summary),
    'Saved 3 of 10, 4 already saved. 2 failed, 1 still recording. Run the same command again to finish the remaining 3.',
  )
  assert.equal(
    formatSaveSummary(summariseSaveResults([{ status: 'saved' }, { status: 'saved' }])),
    'Saved 2 of 2.',
  )
})

test('summary: handleSaveAction prints the line, adds counts to JSON, and sets exit code 1 on failure', async () => {
  const items = [makeItem({ id: '13', route: '/full' }), makeItem({ id: '14', route: '/always-reset' })]
  const { result: summary, lines } = await silenceConsole(() =>
    handleSaveAction({
      recordings: makeRecordings(items),
      savePath: tmpDir,
      jsonOutput: true,
      concurrency: 2,
      retryDelays: NO_RETRIES,
    }))

  assert.equal(process.exitCode, 1)
  assert.equal(summary.saved, 1)
  assert.equal(summary.failed, 1)
  const output = lines.join('\n')
  assert.match(output, /Saved 1 of 2\. 1 failed\. Run the same command again to finish the remaining 1\./)
  const json = JSON.parse(output.match(/=== Start JSON Output ===\s*([\s\S]*?)\s*=== End JSON Output ===/)[1])
  assert.equal(json.summary.failed, 1)
  assert.equal(json.results.length, 2)
})

test('summary: handleSaveAction leaves the exit code alone when nothing failed', async () => {
  const items = [makeItem({ id: '15', route: '/full' })]
  await silenceConsole(() =>
    handleSaveAction({ recordings: makeRecordings(items), savePath: tmpDir, retryDelays: NO_RETRIES }))

  assert.equal(process.exitCode, undefined)
})

test('CLI: --concurrency outside 1 to 10 is rejected before discovery', async () => {
  const { code, stderr } = await runCli(['recordings', '--ip', '127.0.0.1', '--concurrency', '11'])
  assert.notEqual(code, 0)
  assert.match(stderr, /--concurrency must be a whole number from 1 to 10/)
})

test('CLI: --help documents --concurrency', async () => {
  const { stdout } = await runCli(['--help'])
  assert.match(stdout, /--concurrency/)
})

const PLEX_TEMPLATE = '${show_title}/Season ${season_number}/${show_title} - S${season_number}E${episode_number_padded}.${ext}'

const makeBareItem = ({ id, title, route = '/full', ...rest }) => ({
  id,
  title,
  url: `${baseUrl}${route}`,
  size: BODY.length,
  ext: 'ts',
  item_type: 'movie',
  ...rest,
})

test('naming: an item with no season or episode falls back to the default name under --for-plex', async () => {
  const film = makeBareItem({ id: '30', title: 'Some Film' })
  const episode = makeItem({ id: '31', route: '/full' })
  const { result, lines } = await silenceConsole(() =>
    saveRecordings({
      recordings: makeRecordings([film, episode]),
      savePath: tmpDir,
      template: PLEX_TEMPLATE,
      retryDelays: NO_RETRIES,
    }))

  assert.deepEqual(result.map(entry => entry.status), ['saved', 'saved'])
  assert.ok((await fs.readFile(path.join(tmpDir, 'Bluey', 'Some Film.ts'))).equals(BODY))
  assert.ok((await fs.readFile(path.join(tmpDir, 'Bluey', 'Season 1', 'Bluey - S1E31.ts'))).equals(BODY))
  const warnings = lines.filter(line => line.includes('without a matching value'))
  assert.equal(warnings.length, 1)
  assert.match(warnings[0], /Using the default name for Some Film\./)
})

test('naming: season and episode from parentTaskName prefix the default filename', async () => {
  const item = makeBareItem({
    id: '32',
    title: 'Episode 8 - Tue 18 Feb',
    item_type: 'episode',
    season_number: '20',
    season_number_padded: '20',
    episode_number: '8',
    episode_number_padded: '08',
  })
  await silenceConsole(() =>
    saveRecordings({ recordings: makeRecordings([item]), savePath: tmpDir, retryDelays: NO_RETRIES }))

  const saved = path.join(tmpDir, 'Bluey', 'S20E08 - Episode 8 - Tue 18 Feb.ts')
  assert.ok((await fs.readFile(saved)).equals(BODY))
})

test('naming: a title that already has an SxxEyy pattern is not prefixed', async () => {
  const item = makeBareItem({
    id: '33',
    title: 'S20 E8 - Episode 8',
    item_type: 'episode',
    season_number: '20',
    season_number_padded: '20',
    episode_number: '8',
    episode_number_padded: '08',
  })
  await silenceConsole(() =>
    saveRecordings({ recordings: makeRecordings([item]), savePath: tmpDir, retryDelays: NO_RETRIES }))

  assert.ok((await fs.readFile(path.join(tmpDir, 'Bluey', 'S20 E8 - Episode 8.ts'))).equals(BODY))
})
