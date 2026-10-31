import { createServer } from 'node:http'
import fs from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import NodeSsdp from 'node-ssdp'

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..')
const mediaServerFixture = path.join(repoRoot, 'test', 'fixtures', 'media-server.xml')

export const FETCH_BOX_OVERSTATEMENT = 84
export const MB = 1024 * 1024

export const demoShows = ({ size = 3 * MB } = {}) => [
  {
    title: 'Harbour Lights',
    items: [
      { title: 'S1 E1 - The Lighthouse Keeper', size, duration: '0:58:12' },
      { title: 'S1 E2 - Low Tide', size: size + 4096, duration: '0:57:40' },
      { title: 'S1 E3 - Storm Warning', size: size + 8192, duration: '0:59:03' },
      { title: 'S1 E4 - Safe Harbour', size: size + 12288, duration: '1:01:27' }
    ]
  },
  {
    title: 'Backyard Builders',
    items: [
      { title: 'S3 E1 - The Shed', size: size * 2, duration: '0:44:10', parentTaskName: 'Backyard Builders S3 E1' },
      { title: 'S3 E2 - The Deck', size: size * 2 + 2048, duration: '0:44:55', parentTaskName: 'Backyard Builders S3 E2' }
    ]
  },
  {
    title: 'The Night Market',
    items: [{ title: 'The Night Market', size: size * 3, duration: '1:52:30' }]
  },
  {
    title: 'Coastal Kitchen',
    items: [
      { title: 'S2 E5 - Oysters', size: size + 1024, duration: '0:28:45' },
      { title: 'S2 E6 - Sourdough', size: -1, duration: '0:00:00', stillRecording: true }
    ]
  },
  {
    title: 'Morning Weather Report',
    items: [{ title: 'Morning Weather Report', size: Math.round(size / 2), duration: '0:15:00' }]
  }
]

export const startFakeBox = async ({
  shows = demoShows(),
  name = 'Test Fetch Box',
  host = '127.0.0.1',
  port = 0,
  ssdp = false,
  quirks = {},
  bytesPerSecond = 0
} = {}) => {
  const catalogue = buildCatalogue(shows)
  const requests = []
  const attempts = new Map()
  const mediaServerXml = (await fs.readFile(mediaServerFixture, 'utf8')).replace('Fetch TV Test Box', escapeXml(name))

  const server = createServer((req, res) => {
    requests.push({ method: req.method, url: req.url, range: req.headers.range ?? null })
    if (req.method === 'GET' && req.url === '/MediaServer.xml') return sendXml(res, mediaServerXml)
    if (req.method === 'POST' && req.url === '/upnp/control/ContentDirectory') return answerBrowse({ req, res, catalogue, baseUrl: baseUrlOf() })
    const itemMatch = req.url.match(/^\/item\/(\d+)$/)
    if (itemMatch && (req.method === 'GET' || req.method === 'HEAD')) {
      const item = catalogue.items.get(itemMatch[1])
      if (!item) return notFound(res)
      const attempt = (attempts.get(item.id) ?? 0) + 1
      attempts.set(item.id, attempt)
      return sendRecording({ req, res, item, quirks: { ...DEFAULT_QUIRKS, ...quirks, ...item.quirks }, attempt, bytesPerSecond })
    }
    notFound(res)
  })

  await new Promise(resolve => server.listen(port, host, resolve))
  const actualPort = server.address().port
  const advertisedHost = host === '0.0.0.0' ? lanAddress() : host
  const baseUrlOf = () => `http://${advertisedHost}:${actualPort}`
  const ssdpServer = ssdp ? await startSsdp({ location: `${baseUrlOf()}/MediaServer.xml` }) : null

  return {
    url: `${baseUrlOf()}/MediaServer.xml`,
    address: `${advertisedHost}:${actualPort}`,
    port: actualPort,
    requests,
    catalogue,
    stop: async () => {
      ssdpServer?.stop()
      server.closeAllConnections()
      await new Promise(resolve => server.close(resolve))
    }
  }
}

export const recordingBytes = (item) => patternBytes({ start: 0, end: item.actualSize, seed: Number(item.id) })

const DEFAULT_QUIRKS = {
  overstate: FETCH_BOX_OVERSTATEMENT,
  resetAtEnd: true,
  rangeNotSatisfiable: true,
  dropFirstAttemptAt: 0,
  alwaysDropAt: 0
}

const buildCatalogue = (shows) => {
  const items = new Map()
  const showFolders = shows.map((show, showIndex) => {
    const showId = String(100 + showIndex)
    const showItems = show.items.map((item, itemIndex) => {
      const id = String(1000 + showIndex * 100 + itemIndex)
      const recordingInProgress = item.stillRecording || item.size <= 0
      const entry = {
        ...item,
        id,
        parentId: showId,
        actualSize: recordingInProgress ? 0 : item.size,
        quirks: item.quirks ?? {}
      }
      items.set(id, entry)
      return entry
    })
    return { id: showId, title: show.title, items: showItems }
  })
  return { showFolders, items }
}

const answerBrowse = async ({ req, res, catalogue, baseUrl }) => {
  const body = await readBody(req)
  const objectId = body.match(/<ObjectID>([^<]*)<\/ObjectID>/)?.[1] ?? '0'
  const didl = didlFor({ objectId, catalogue, baseUrl })
  sendXml(res, browseEnvelope(didl.xml, didl.count))
}

const didlFor = ({ objectId, catalogue, baseUrl }) => {
  if (objectId === '0') {
    return { count: 2, xml: [container({ id: '1', parentId: '0', title: 'Recordings' }), container({ id: '2', parentId: '0', title: 'Live TV' })].join('') }
  }
  if (objectId === '1') {
    return { count: catalogue.showFolders.length, xml: catalogue.showFolders.map(show => container({ id: show.id, parentId: '1', title: show.title })).join('') }
  }
  const show = catalogue.showFolders.find(folder => folder.id === objectId)
  if (!show) return { count: 0, xml: '' }
  return { count: show.items.length, xml: show.items.map(item => itemXml({ item, baseUrl })).join('') }
}

const container = ({ id, parentId, title }) =>
  `<container id="${id}" parentID="${parentId}" restricted="1"><dc:title>${escapeXml(title)}</dc:title><upnp:class>object.container</upnp:class></container>`

const itemXml = ({ item, baseUrl }) => {
  const listedSize = item.actualSize > 0 ? item.actualSize + (item.quirks.overstate ?? FETCH_BOX_OVERSTATEMENT) : -1
  const taskName = item.parentTaskName ? ` parentTaskName="${escapeXml(item.parentTaskName)}"` : ''
  return `<item id="${item.id}" parentID="${item.parentId}" restricted="1"><dc:title>${escapeXml(item.title)}</dc:title>` +
    `<dc:description>${escapeXml(item.title)}</dc:description><upnp:class>object.item.videoItem</upnp:class>` +
    `<res protocolInfo="http-get:*:video/mpeg-tts:DLNA.ORG_PN=AVC_TS_HD_AC3;DLNA.ORG_OP=01" size="${listedSize}" duration="${item.duration ?? '0:30:00'}"${taskName}>` +
    `${baseUrl}/item/${item.id}</res></item>`
}

const browseEnvelope = (didlXml, count) => `<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/">
  <s:Body>
    <u:BrowseResponse xmlns:u="urn:schemas-upnp-org:service:ContentDirectory:1">
      <Result>${escapeXml(`<DIDL-Lite xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:upnp="urn:schemas-upnp-org:metadata-1-0/upnp/" xmlns="urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/">${didlXml}</DIDL-Lite>`)}</Result>
      <NumberReturned>${count}</NumberReturned>
      <TotalMatches>${count}</TotalMatches>
      <UpdateID>1</UpdateID>
    </u:BrowseResponse>
  </s:Body>
</s:Envelope>`

const sendRecording = ({ req, res, item, quirks, attempt, bytesPerSecond }) => {
  const statedSize = item.actualSize + (quirks.overstate ?? 0)
  const start = rangeStartOf(req.headers.range)
  if (item.actualSize <= 0) {
    res.writeHead(200, { 'Content-Type': 'video/mpeg', 'Content-Length': '4398046510080' })
    return res.end()
  }
  if (start !== null && quirks.rangeNotSatisfiable) {
    res.writeHead(416, { 'Content-Range': `bytes */${statedSize}` })
    return res.end()
  }
  if (start !== null && start >= item.actualSize) {
    res.writeHead(416, { 'Content-Range': `bytes */${item.actualSize}` })
    return res.end()
  }
  const from = start ?? 0
  const headers = start === null
    ? { 'Content-Type': 'video/mpeg', 'Content-Length': String(statedSize) }
    : { 'Content-Type': 'video/mpeg', 'Content-Length': String(statedSize - from), 'Content-Range': `bytes ${from}-${statedSize - 1}/${statedSize}` }
  res.writeHead(start === null ? 200 : 206, headers)
  if (req.method === 'HEAD') return res.end()
  const dropAt = quirks.alwaysDropAt || (attempt === 1 ? quirks.dropFirstAttemptAt : 0)
  const end = dropAt > from ? Math.min(dropAt, item.actualSize) : item.actualSize
  streamBytes({ res, item, from, end, bytesPerSecond, finish: () => (end < item.actualSize || quirks.resetAtEnd ? res.socket?.destroy() : res.end()) })
}

const streamBytes = ({ res, item, from, end, bytesPerSecond, finish }) => {
  const chunkSize = 256 * 1024
  const delay = bytesPerSecond > 0 ? Math.round((chunkSize / bytesPerSecond) * 1000) : 0
  let offset = from
  const writeNext = () => {
    if (res.destroyed) return
    if (offset >= end) return setTimeout(finish, 20)
    const next = Math.min(end, offset + chunkSize)
    const chunk = patternBytes({ start: offset, end: next, seed: Number(item.id) })
    offset = next
    const ready = res.write(chunk)
    const proceed = () => (delay ? setTimeout(writeNext, delay) : setImmediate(writeNext))
    if (ready) proceed()
    else res.once('drain', proceed)
  }
  writeNext()
}

const PATTERN_PERIOD = 256
const patternCache = new Map()

const patternBytes = ({ start, end, seed }) => {
  const length = end - start
  const pattern = patternFor({ seed, length: length + PATTERN_PERIOD })
  const offset = start % PATTERN_PERIOD
  return pattern.subarray(offset, offset + length)
}

const patternFor = ({ seed, length }) => {
  const cached = patternCache.get(seed)
  if (cached && cached.length >= length) return cached
  const buffer = Buffer.allocUnsafe(Math.max(length, 1024 * 1024 + PATTERN_PERIOD))
  for (let index = 0; index < buffer.length; index++) buffer[index] = (index * 31 + seed) & 0xff
  patternCache.set(seed, buffer)
  return buffer
}

const startSsdp = async ({ location }) => {
  const server = new NodeSsdp.Server({ location, udn: 'uuid:fetchtv-fake-box', reuseAddr: true, ssdpTtl: 2 })
  server.addUSN('upnp:rootdevice')
  server.addUSN('urn:schemas-upnp-org:device:MediaServer:1')
  await server.start()
  return server
}

const lanAddress = () => Object.values(os.networkInterfaces()).flat()
  .find(entry => entry && entry.family === 'IPv4' && !entry.internal)?.address ?? '127.0.0.1'

const rangeStartOf = (header) => {
  const match = String(header ?? '').match(/bytes=(\d+)-/)
  return match ? Number(match[1]) : null
}

const readBody = (req) => new Promise((resolve, reject) => {
  let body = ''
  req.setEncoding('utf8')
  req.on('data', chunk => { body += chunk })
  req.on('end', () => resolve(body))
  req.on('error', reject)
})

const sendXml = (res, xml) => {
  res.writeHead(200, { 'Content-Type': 'text/xml; charset="utf-8"' })
  res.end(xml)
}

const notFound = (res) => {
  res.writeHead(404)
  res.end()
}

const escapeXml = (text) => String(text)
  .replace(/&/g, '&amp;')
  .replace(/</g, '&lt;')
  .replace(/>/g, '&gt;')
  .replace(/"/g, '&quot;')
