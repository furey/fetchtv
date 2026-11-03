import { test, beforeEach, afterEach } from 'node:test'
import assert from 'node:assert/strict'
import { createServer } from 'node:http'
import net from 'node:net'
import fs from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'

import { saveRecordings, BOX_UNREACHABLE } from '../fetchtv.js'

const BODY = Buffer.alloc(64 * 1024, 'abcdefghij')
const HALF = BODY.length / 2
const CHUNK = 4 * 1024
const CHUNK_GAP = 20
const FETCH_BOX_OVERSTATEMENT = 84
const createRealConnection = net.createConnection

const FAST = {
  retryDelays: [10, 10],
  idleTimeout: 150,
  connectTimeout: 100,
  probeDelays: [30, 30, 60],
  probeTimeout: 100,
  flowingWindow: 300,
  boxWaitMs: 200,
}

let tmpDir = null
let boxes = []

beforeEach(async () => {
  tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'fetchtv-box-wait-'))
  boxes = []
})

afterEach(async () => {
  await Promise.all(boxes.map(box => box.stop()))
  await fs.rm(tmpDir, { recursive: true, force: true })
})

const parseRangeStart = (rangeHeader) => {
  const match = String(rangeHeader || '').match(/bytes=(\d+)-/)
  return match ? parseInt(match[1], 10) : null
}

const isProbe = (req) => req.headers.range === 'bytes=0-0'

const sendFrom = ({ res, start }) => {
  if (start === null) {
    res.writeHead(200, { 'Content-Length': String(BODY.length) })
    return res.end(BODY)
  }
  res.writeHead(206, {
    'Content-Length': String(BODY.length - start),
    'Content-Range': `bytes ${start}-${BODY.length - 1}/${BODY.length}`,
  })
  res.end(BODY.subarray(start))
}

const sendSlowly = ({ res, start }) => {
  const from = start ?? 0
  res.writeHead(start === null ? 200 : 206, {
    'Content-Length': String(BODY.length - from),
    ...(start === null ? {} : { 'Content-Range': `bytes ${from}-${BODY.length - 1}/${BODY.length}` }),
  })
  let offset = from
  const timer = setInterval(() => {
    if (res.destroyed) return clearInterval(timer)
    const next = Math.min(offset + CHUNK, BODY.length)
    res.write(BODY.subarray(offset, next))
    offset = next
    if (offset < BODY.length) return
    clearInterval(timer)
    res.end()
  }, CHUNK_GAP)
}

const routes = {
  '/full': sendFrom,
  '/slow': sendSlowly,
  '/server-error': ({ res }) => {
    res.writeHead(500)
    res.end()
  },
  '/no-reply': () => {},
  '/reset': ({ res, start }) => {
    if (start !== null) return sendFrom({ res, start })
    res.writeHead(200, { 'Content-Length': String(BODY.length) })
    res.write(BODY.subarray(0, HALF), () => setTimeout(() => res.socket.destroy(), 20))
  },
  '/overstate-then-hold': ({ res, start }) => {
    if (start !== null) return sendFrom({ res, start })
    res.writeHead(200, { 'Content-Length': String(BODY.length + FETCH_BOX_OVERSTATEMENT) })
    res.write(BODY)
  },
  '/short-then-416': ({ res, start }) => {
    if (start !== null) {
      res.writeHead(416, { 'Content-Range': `bytes */${BODY.length}` })
      return res.end()
    }
    if (!routes.shortServed) {
      routes.shortServed = true
      res.writeHead(200, { 'Content-Length': String(BODY.length) })
      return res.write(BODY.subarray(0, BODY.length - 4 * CHUNK))
    }
    sendFrom({ res, start })
  },
}

const createBox = async ({ probeStatus = 206, onSilence } = {}) => {
  const sockets = new Set()
  const requests = []
  let server = null
  const box = {
    port: 0,
    requests,
    downloads: () => requests.filter(request => !request.probe),
    url: (route) => `http://127.0.0.1:${box.port}${route}`,
    start: () => new Promise(resolve => {
      server = createServer((req, res) => {
        const probe = isProbe(req)
        requests.push({ route: req.url, start: parseRangeStart(req.headers.range), probe })
        if (probe) {
          res.writeHead(probeStatus)
          return res.end(probeStatus === 206 ? BODY.subarray(0, 1) : undefined)
        }
        const handler = routes[req.url]
        if (!handler) {
          res.writeHead(404)
          return res.end()
        }
        handler({ req, res, start: parseRangeStart(req.headers.range), box })
      })
      server.on('connection', socket => {
        sockets.add(socket)
        socket.on('close', () => sockets.delete(socket))
      })
      server.listen(box.port, '127.0.0.1', () => {
        box.port = server.address().port
        resolve()
      })
    }),
    stopListening: () => server?.close(),
    stop: async () => {
      for (const socket of sockets) socket.destroy()
      if (!server?.listening) return
      await new Promise(resolve => server.close(resolve))
    },
  }
  await box.start()
  boxes.push(box)
  return box
}

const closedPort = async () => {
  const box = await createBox()
  await box.stop()
  return box.port
}

const makeItem = ({ id, url }) => ({
  id,
  title: `S1 E${id} - Test`,
  url,
  size: BODY.length,
  ext: 'ts',
  season_number: '1',
  season_number_padded: '01',
  episode_number: String(id),
  episode_number_padded: String(id).padStart(2, '0'),
  item_type: 'episode',
})

const filePathFor = (item) => path.join(tmpDir, 'Bluey', `${item.title}.ts`)

const exists = (filePath) => fs.access(filePath).then(() => true, () => false)

const failedConnection = ({ code, after = 0 }) => {
  const socket = new net.Socket()
  socket.connecting = true
  setTimeout(() => socket.destroy(Object.assign(new Error(`connect ${code}`), { code, syscall: 'connect' })), after)
  return socket
}

const unansweredConnection = () => {
  const socket = new net.Socket()
  socket.connecting = true
  return socket
}

const scriptConnections = (t, decide) => {
  let index = 0
  t.mock.method(net, 'createConnection', (options, ...rest) => {
    const outcome = decide({ port: Number(options.port), index: index++ })
    if (outcome === 'hang') return unansweredConnection()
    if (typeof outcome === 'string') return failedConnection({ code: outcome })
    if (outcome) return failedConnection(outcome)
    return createRealConnection.call(net, options, ...rest)
  })
}

const save = async ({ items, concurrency = 2, ...options }) => {
  const states = []
  const retries = []
  const original = console.log
  console.log = () => {}
  const startedAt = Date.now()
  try {
    const results = await saveRecordings({
      recordings: [{ title: 'Bluey', items }],
      savePath: tmpDir,
      concurrency,
      onBoxState: ({ state }) => states.push(state),
      onRetry: (retry) => retries.push(retry),
      onProgress: () => {},
      ...FAST,
      ...options,
    })
    return { results, states, retries, elapsed: Date.now() - startedAt }
  } finally {
    console.log = original
  }
}

const resultFor = (results, item) => results.find(result => result.item.id === item.id)

const assertSaved = async (item) => assert.ok((await fs.readFile(filePathFor(item))).equals(BODY))

test('box wait: a refused box that comes back is waited for, and the save carries on without spending a try', async () => {
  const box = await createBox({ probeStatus: 404 })
  await box.stop()
  const items = [makeItem({ id: '1', url: box.url('/full') }), makeItem({ id: '2', url: box.url('/full') })]
  setTimeout(() => box.start(), 100)
  const { results, states, retries } = await save({ items, concurrency: 1 })

  assert.deepEqual(states, ['lost', 'back'])
  assert.deepEqual(retries, [])
  for (const item of items) {
    assert.equal(resultFor(results, item).status, 'saved')
    assert.equal(resultFor(results, item).attempts, undefined)
    await assertSaved(item)
  }
})

test('box wait: a refused box that never comes back fails every item as box-unreachable with no tries', async () => {
  const port = await closedPort()
  const items = [1, 2, 3].map(id => makeItem({ id: String(id), url: `http://127.0.0.1:${port}/full` }))
  const { results, states, retries } = await save({ items, concurrency: 1 })

  assert.deepEqual(states, ['lost', 'gave-up'])
  assert.deepEqual(retries, [])
  assert.deepEqual(results.map(result => [result.status, result.error]), items.map(() => ['failed', BOX_UNREACHABLE]))
})

test('box wait: a blackholed connect times out after connectTimeout and opens the wait', async (t) => {
  const box = await createBox()
  let isBlackholed = true
  scriptConnections(t, () => (isBlackholed ? 'hang' : null))
  setTimeout(() => { isBlackholed = false }, 250)
  const item = makeItem({ id: '3', url: box.url('/full') })
  const { results, states, elapsed } = await save({ items: [item] })

  assert.deepEqual(states, ['lost', 'back'])
  assert.equal(results[0].status, 'saved')
  assert.ok(elapsed >= FAST.connectTimeout)
  await assertSaved(item)
})

test('box wait: a box that accepts the connection but sends no reply is an item failure', async () => {
  const box = await createBox()
  const item = makeItem({ id: '4', url: box.url('/no-reply') })
  const { results, states, retries } = await save({ items: [item], headerTimeout: 100 })

  assert.deepEqual(states, [])
  assert.equal(retries.length, 2)
  assert.equal(results[0].status, 'failed')
  assert.equal(results[0].attempts, 3)
  assert.match(results[0].error, /timeout/)
})

test('box wait: a stream that goes silent with the socket open reconnects into the wait without spending a try', async () => {
  const box = await createBox()
  routes['/silent-then-gone'] = ({ res, start }) => {
    if (start !== null) return sendFrom({ res, start })
    res.writeHead(200, { 'Content-Length': String(BODY.length) })
    res.write(BODY.subarray(0, HALF))
    box.stopListening()
    setTimeout(() => box.start(), 250)
  }
  const item = makeItem({ id: '5', url: box.url('/silent-then-gone') })
  const { results, states } = await save({ items: [item] })

  assert.deepEqual(states, ['lost', 'back'])
  assert.equal(results[0].status, 'saved')
  assert.equal(results[0].attempts, undefined)
  assert.deepEqual(box.downloads().map(request => request.start), [null, HALF])
  await assertSaved(item)
})

test('box wait: a mid-stream reset with the port open is an item retry', async () => {
  const box = await createBox()
  const item = makeItem({ id: '6', url: box.url('/reset') })
  const { results, states, retries } = await save({ items: [item] })

  assert.deepEqual(states, [])
  assert.deepEqual(retries, [{ id: '6', attempt: 2, of: 3 }])
  assert.equal(results[0].status, 'saved')
  assert.equal(results[0].attempts, 2)
  await assertSaved(item)
})

test('box wait: a 5xx reply is an item failure with no wait and no retry', async () => {
  const box = await createBox()
  const item = makeItem({ id: '7', url: box.url('/server-error') })
  const { results, states } = await save({ items: [item] })

  assert.deepEqual(states, [])
  assert.equal(results[0].status, 'failed')
  assert.match(results[0].error, /status code 500/)
  assert.equal(box.downloads().length, 1)
})

test('box wait: a box that restarts on the same port resumes every item and keeps the 416 and end-of-file rules', async () => {
  const box = await createBox()
  routes.shortServed = false
  const slow = makeItem({ id: '8', url: box.url('/slow') })
  const overstated = makeItem({ id: '9', url: box.url('/overstate-then-hold') })
  const short = makeItem({ id: '10', url: box.url('/short-then-416') })
  setTimeout(async () => {
    await box.stop()
    setTimeout(() => box.start(), 100)
  }, 150)
  const { results, states } = await save({ items: [slow, overstated, short], concurrency: 3 })

  assert.deepEqual(states, ['lost', 'back'])
  assert.equal(resultFor(results, slow).status, 'saved')
  assert.equal(resultFor(results, slow).attempts, undefined)
  assert.ok(box.downloads().some(request => request.route === '/slow' && request.start > 0))
  assert.equal(resultFor(results, overstated).status, 'saved')
  assert.equal(resultFor(results, overstated).attempts, undefined)
  assert.equal(box.downloads().filter(request => request.route === '/overstate-then-hold').length, 1)
  assert.equal(resultFor(results, short).status, 'saved')
  assert.ok(box.downloads().some(request => request.route === '/short-then-416' && request.start > 0))
  for (const item of [slow, overstated, short]) await assertSaved(item)
})

test('box wait: a flapping box opens the wait at most three times per item', async (t) => {
  const box = await createBox()
  scriptConnections(t, ({ index }) => (index % 2 === 0 ? 'ECONNREFUSED' : null))
  const item = makeItem({ id: '11', url: box.url('/full') })
  const { results, states, retries } = await save({ items: [item] })

  assert.deepEqual(states, ['lost', 'back', 'lost', 'back', 'lost', 'back'])
  assert.deepEqual(retries, [{ id: '11', attempt: 2, of: 3 }])
  assert.equal(results[0].status, 'saved')
  assert.equal(results[0].attempts, 2)
})

test('box wait: a slow box that keeps sending never opens the wait', async () => {
  const box = await createBox()
  const items = [makeItem({ id: '12', url: box.url('/slow') }), makeItem({ id: '13', url: box.url('/slow') })]
  const { results, states, retries } = await save({ items })

  assert.deepEqual(states, [])
  assert.deepEqual(retries, [])
  assert.deepEqual(results.map(result => result.status), ['saved', 'saved'])
})

test('box wait: a refused item while another stream flows is an item failure', async (t) => {
  const box = await createBox()
  const refusedPort = await closedPort()
  scriptConnections(t, ({ port }) => (port === refusedPort ? { code: 'ECONNREFUSED', after: 60 } : null))
  const flowing = makeItem({ id: '14', url: box.url('/slow') })
  const refused = makeItem({ id: '15', url: `http://127.0.0.1:${refusedPort}/full` })
  const { results, states } = await save({ items: [flowing, refused], retryDelays: [20, 20] })

  assert.deepEqual(states, [])
  assert.equal(resultFor(results, flowing).status, 'saved')
  assert.equal(resultFor(results, refused).status, 'failed')
  assert.equal(resultFor(results, refused).attempts, 3)
  assert.match(resultFor(results, refused).error, /ECONNREFUSED/)
})

test('box wait: Stop during the wait ends the save at once and keeps the .part file', async () => {
  const port = await closedPort()
  const items = [makeItem({ id: '16', url: `http://127.0.0.1:${port}/full` }), makeItem({ id: '17', url: `http://127.0.0.1:${port}/full` })]
  await fs.mkdir(path.dirname(filePathFor(items[0])), { recursive: true })
  await fs.writeFile(`${filePathFor(items[0])}.part`, BODY.subarray(0, HALF))
  const controller = new AbortController()
  setTimeout(() => controller.abort(), 100)
  const { results, states, elapsed } = await save({ items, concurrency: 1, probeDelays: [5000], boxWaitMs: 60000, signal: controller.signal })

  assert.deepEqual(states, ['lost'])
  assert.ok(elapsed < 1000)
  assert.deepEqual(results.map(result => result.status), ['cancelled', 'cancelled'])
  assert.equal((await fs.stat(`${filePathFor(items[0])}.part`)).size, HALF)
})

test('box wait: Stop during a probe ends the save at once and keeps the .part file', async (t) => {
  const box = await createBox()
  scriptConnections(t, ({ index }) => (index === 0 ? 'ECONNREFUSED' : 'hang'))
  const item = makeItem({ id: '18', url: box.url('/full') })
  await fs.mkdir(path.dirname(filePathFor(item)), { recursive: true })
  await fs.writeFile(`${filePathFor(item)}.part`, BODY.subarray(0, HALF))
  const controller = new AbortController()
  setTimeout(() => controller.abort(), 150)
  const { results, states, elapsed } = await save({ items: [item], probeDelays: [10], probeTimeout: 5000, connectTimeout: 5000, boxWaitMs: 60000, signal: controller.signal })

  assert.deepEqual(states, ['lost'])
  assert.ok(elapsed < 1000)
  assert.equal(results[0].status, 'cancelled')
  assert.equal((await fs.stat(`${filePathFor(item)}.part`)).size, HALF)
  assert.equal(await exists(filePathFor(item)), false)
})

test('box wait: boxWaitMs 0 keeps the old behaviour of retrying and failing', async () => {
  const port = await closedPort()
  const item = makeItem({ id: '19', url: `http://127.0.0.1:${port}/full` })
  const { results, states, retries } = await save({ items: [item], boxWaitMs: 0 })

  assert.deepEqual(states, [])
  assert.equal(retries.length, 2)
  assert.equal(results[0].status, 'failed')
  assert.equal(results[0].attempts, 3)
  assert.match(results[0].error, /^Download initiation failed: connect ECONNREFUSED/)
})

test('box wait: a computer that goes offline (ENETDOWN) waits for the network to come back', async (t) => {
  const box = await createBox()
  let isOffline = true
  scriptConnections(t, () => (isOffline ? 'ENETDOWN' : null))
  setTimeout(() => { isOffline = false }, 100)
  const item = makeItem({ id: '20', url: box.url('/full') })
  const { results, states } = await save({ items: [item] })

  assert.deepEqual(states, ['lost', 'back'])
  assert.equal(results[0].status, 'saved')
  await assertSaved(item)
})

test('box wait: the wait counts probes at 5, 10, and 20, then every 30, up to the window', async (t) => {
  const port = await closedPort()
  let connections = 0
  scriptConnections(t, () => {
    connections++
    return 'ECONNREFUSED'
  })
  const item = makeItem({ id: '21', url: `http://127.0.0.1:${port}/full` })
  const { results, states } = await save({ items: [item], probeDelays: [5, 5, 10, 30], boxWaitMs: 600 })

  assert.deepEqual(states, ['lost', 'gave-up'])
  assert.equal(results[0].error, BOX_UNREACHABLE)
  assert.equal(connections, 1 + 22)
})

test('box wait: a callback that throws does not stop the save', async () => {
  const box = await createBox()
  await box.stop()
  const items = [makeItem({ id: '22', url: box.url('/reset') }), makeItem({ id: '23', url: box.url('/full') })]
  setTimeout(() => box.start(), 100)
  const { results } = await save({
    items,
    concurrency: 1,
    onBoxState: () => { throw new Error('onBoxState broke') },
    onRetry: () => { throw new Error('onRetry broke') },
  })

  assert.deepEqual(results.map(result => result.status), ['saved', 'saved'])
})

test('box wait: off, Stop during a retry delay returns what 2.2.x returned', async () => {
  const box = await createBox()
  const item = makeItem({ id: '24', url: box.url('/reset') })
  const controller = new AbortController()
  const { results } = await save({ items: [item], boxWaitMs: 0, retryDelays: [5000], onRetry: () => setTimeout(() => controller.abort(), 20), signal: controller.signal })

  assert.equal(results[0].status, 'cancelled')
  assert.match(results[0].warning, /^Download interrupted for .* Partial file kept for resume\.$/)
})
