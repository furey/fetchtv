import { test, before, after } from 'node:test'
import assert from 'node:assert/strict'
import { createServer } from 'node:http'
import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { spawn } from 'node:child_process'
import path from 'node:path'

const fixturesDir = path.join(path.dirname(fileURLToPath(import.meta.url)), 'fixtures')
const projectRoot = path.dirname(path.dirname(fileURLToPath(import.meta.url)))
const cliPath = path.join(projectRoot, 'fetchtv.js')

const readFixture = (name) => readFileSync(path.join(fixturesDir, name), 'utf-8')

const extractObjectId = (body) => {
  const match = String(body).match(/<ObjectID>([^<]+)<\/ObjectID>/)
  return match ? match[1] : null
}

const collectBody = (req) =>
  new Promise((resolve) => {
    const chunks = []
    req.on('data', (chunk) => chunks.push(chunk))
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')))
  })

let server = null
let serverPort = null

const startServer = () =>
  new Promise((resolve) => {
    server = createServer(async (req, res) => {
      if (req.method === 'GET' && req.url === '/MediaServer.xml') {
        res.writeHead(200, { 'Content-Type': 'text/xml' })
        res.end(readFixture('media-server.xml'))
        return
      }
      if (req.method === 'POST' && req.url === '/upnp/control/ContentDirectory') {
        const body = await collectBody(req)
        const objectId = extractObjectId(body)
        const fixture = (
          objectId === '0' ? 'browse-root.xml' :
          objectId === '1' ? 'browse-recordings.xml' :
          (objectId === '10' || objectId === '11' || objectId === '12')
            ? 'browse-show-items.xml' :
          'browse-empty.xml'
        )
        res.writeHead(200, { 'Content-Type': 'text/xml' })
        res.end(readFixture(fixture))
        return
      }
      res.writeHead(404)
      res.end()
    })
    server.listen(0, '127.0.0.1', () => {
      serverPort = server.address().port
      resolve()
    })
  })

const stopServer = () =>
  new Promise((resolve) => { server.close(() => resolve()) })

const runCli = (args, { timeoutMs = 15000 } = {}) =>
  new Promise((resolve, reject) => {
    const child = spawn('node', [cliPath, ...args], {
      stdio: ['ignore', 'pipe', 'pipe'],
      env: { ...process.env, NO_COLOR: '1', FORCE_COLOR: '0' },
    })
    let stdout = ''
    let stderr = ''
    const timer = setTimeout(() => {
      child.kill('SIGKILL')
      reject(new Error(`CLI timed out after ${timeoutMs}ms`))
    }, timeoutMs)
    child.stdout.on('data', (chunk) => { stdout += chunk })
    child.stderr.on('data', (chunk) => { stderr += chunk })
    child.on('close', (code) => {
      clearTimeout(timer)
      resolve({ code, stdout, stderr })
    })
    child.on('error', (err) => {
      clearTimeout(timer)
      reject(err)
    })
  })

before(async () => { await startServer() })
after(async () => { await stopServer() })

test('CLI: info command prints Fetch TV device details', async () => {
  const { code, stdout } = await runCli(['info', '--ip', '127.0.0.1', '--port', String(serverPort)])
  assert.equal(code, 0)
  assert.match(stdout, /Fetch TV Test Box/)
  assert.match(stdout, /Mighty/)
  assert.match(stdout, /FTV-1000/)
})

test('CLI: shows command lists folder titles only (no episode lines)', async () => {
  const { code, stdout } = await runCli(['shows', '--ip', '127.0.0.1', '--port', String(serverPort)])
  assert.equal(code, 0)
  assert.match(stdout, /Bluey/)
  assert.match(stdout, /The Amazing Race/)
  assert.match(stdout, /MasterChef Australia/)
  assert.ok(!/S1 E1 - Pilot/.test(stdout), 'shows command must not list episodes')
})

test('CLI: recordings command lists episodes under each folder', async () => {
  const { code, stdout } = await runCli([
    'recordings', '--ip', '127.0.0.1', '--port', String(serverPort),
  ])
  assert.equal(code, 0)
  assert.match(stdout, /S1 E1 - Pilot/)
  assert.match(stdout, /S1 E2 - Hospital/)
})

test('CLI: prefix-matched "rec" resolves to recordings', async () => {
  const { code, stdout } = await runCli(['rec', '--ip', '127.0.0.1', '--port', String(serverPort)])
  assert.equal(code, 0)
  assert.match(stdout, /S1 E1 - Pilot/)
})

test('CLI: --json output produces parseable JSON between markers', async () => {
  const { code, stdout } = await runCli([
    'recordings', '--ip', '127.0.0.1', '--port', String(serverPort),
    '--show', 'bluey',
    '--json',
  ])
  assert.equal(code, 0)
  const m = stdout.match(/=== Start JSON Output ===\s*([\s\S]*?)\s*=== End JSON Output ===/)
  assert.ok(m, 'JSON output markers not found')
  const parsed = JSON.parse(m[1])
  assert.equal(parsed.length, 1)
  assert.equal(parsed[0].title, 'Bluey')
  assert.ok(Array.isArray(parsed[0].items))
  assert.equal(parsed[0].items[0].title, 'S1 E1 - Pilot')
})

test('CLI: --show filter restricts output to matching folders', async () => {
  const { code, stdout } = await runCli([
    'recordings', '--ip', '127.0.0.1', '--port', String(serverPort),
    '--show', 'bluey',
  ])
  assert.equal(code, 0)
  assert.match(stdout, /Bluey/)
  assert.ok(!/MasterChef/.test(stdout), '--show filter should exclude non-matching folders')
})

test('CLI: --exclude filter omits matching folders', async () => {
  const { code, stdout } = await runCli([
    'shows', '--ip', '127.0.0.1', '--port', String(serverPort),
    '--exclude', 'masterchef',
  ])
  assert.equal(code, 0)
  assert.ok(!/MasterChef/.test(stdout), '--exclude filter failed')
  assert.match(stdout, /Bluey/)
})

test('CLI: --title filter restricts episodes within a folder', async () => {
  const { code, stdout } = await runCli([
    'recordings', '--ip', '127.0.0.1', '--port', String(serverPort),
    '--show', 'bluey',
    '--title', 'hospital',
    '--json',
  ])
  assert.equal(code, 0)
  const m = stdout.match(/=== Start JSON Output ===\s*([\s\S]*?)\s*=== End JSON Output ===/)
  const parsed = JSON.parse(m[1])
  assert.equal(parsed[0].items.length, 1)
  assert.match(parsed[0].items[0].title, /Hospital/i)
})

test('CLI: --is-recording surfaces only items with size=-1 sentinel', async () => {
  const { code, stdout } = await runCli([
    '--ip', '127.0.0.1', '--port', String(serverPort),
    '--show', 'bluey',
    '--is-recording',
    '--json',
  ])
  assert.equal(code, 0)
  const m = stdout.match(/=== Start JSON Output ===\s*([\s\S]*?)\s*=== End JSON Output ===/)
  const parsed = JSON.parse(m[1])
  const titles = parsed[0].items.map(i => i.title)
  assert.deepEqual(titles, ['In Progress Recording'])
})

test('CLI: --for-plex without --save logs an ignored warning', async () => {
  const { code, stdout } = await runCli([
    'recordings', '--ip', '127.0.0.1', '--port', String(serverPort),
    '--show', 'bluey',
    '--for-plex',
  ])
  assert.equal(code, 0)
  assert.match(stdout, /--for-plex .* ignored/i)
})
