import { test, beforeEach, after } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync, writeFileSync, readdirSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import path from 'node:path'
import nock from 'nock'

import { parseXml, parseLocations, findNode, getFetchRecordings, discoverFetch } from '../fetchtv.js'

const fixturesDir = path.join(path.dirname(fileURLToPath(import.meta.url)), 'fixtures')
const goldenPath = path.join(fixturesDir, 'golden', 'parsed.json')
const isUpdatingGolden = process.env.UPDATE_GOLDEN === '1'
const readFixture = (name) => readFileSync(path.join(fixturesDir, name), 'utf-8')

const HOST = 'http://192.168.1.10:49152'
const CONTROL_PATH = '/upnp/control/ContentDirectory'

const escapeXml = (text) => text
  .replace(/&/g, '&amp;')
  .replace(/</g, '&lt;')
  .replace(/>/g, '&gt;')
  .replace(/"/g, '&quot;')

const browseEnvelope = (didlBody) => `<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/">
  <s:Body>
    <u:BrowseResponse xmlns:u="urn:schemas-upnp-org:service:ContentDirectory:1">
      <Result>${escapeXml(`<DIDL-Lite xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:upnp="urn:schemas-upnp-org:metadata-1-0/upnp/" xmlns:dlna="urn:schemas-dlna-org:metadata-1-0/" xmlns="urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/">${didlBody}</DIDL-Lite>`)}</Result>
      <NumberReturned>1</NumberReturned>
      <TotalMatches>1</TotalMatches>
      <UpdateID>7</UpdateID>
    </u:BrowseResponse>
  </s:Body>
</s:Envelope>`

const edgeCaseRoot = browseEnvelope(
  '<container id="0001" parentID="0" restricted="1" searchable="1" childCount="3"><dc:title>Recordings</dc:title><upnp:class>object.container</upnp:class></container>'
)

const edgeCaseShows = browseEnvelope([
  ['20', '1917'],
  ['21', 'Law &amp; Order: SVU'],
  ['22', 'true'],
  ['23', '007'],
  ['24', 'Grey\'s Anatomy'],
  ['25', 'Grey&#39;s &#38; Co'],
  ['26', 'Hex &#x27;quote&#x22; &#x3C;tag&#x3E; &#x26; &#60;&#62;&#34;'],
  ['27', 'Kept &#X27; &#x3e; &#039; &#160; &#x2019; &amp;#39;']
].map(([id, title]) =>
  `<container id="${id}" parentID="0001" restricted="1"><dc:title>${title}</dc:title><upnp:class>object.container.storageFolder</upnp:class></container>`
).join(''))

const edgeCaseItems = browseEnvelope([
  '<item id="300" parentID="20" restricted="1"><dc:title>1917</dc:title><dc:description>  War film &amp; &quot;epic&quot; &lt;HD&gt; &#169; 2019 &#x2014; ok  </dc:description><upnp:class>object.item.videoItem.movie</upnp:class><dc:date>2026-10-31T20:30:00</dc:date><upnp:channelName>7mate</upnp:channelName>',
  '<res protocolInfo="http-get:*:video/mpeg-tts:DLNA.ORG_PN=AVC_TS_HD_AC3;DLNA.ORG_OP=01;DLNA.ORG_FLAGS=01700000000000000000000000000000" size="4823449600" duration="2:00:13.000" bitrate="669184" resolution="1920x1080" dlna:profileID="AVC_TS_HD_AC3">http://192.168.1.10:49152/web/media/300.ts?x=1&amp;y=2</res>',
  '<res protocolInfo="http-get:*:image/jpeg:DLNA.ORG_PN=JPEG_TN" size="1024">http://192.168.1.10:49152/thumb/300.jpg</res></item>',
  '<item id="301" parentID="20" restricted="1"><dc:title>S01E02 - 0.5e3</dc:title><dc:description></dc:description><upnp:class>object.item.videoItem</upnp:class>',
  '<res protocolInfo="http-get:*:video/mpeg-tts:*" size="-1" duration="0:00:00" parentTaskName="Show S12 E03">http://192.168.1.10:49152/web/media/301.ts</res></item>',
  '<item id="302" parentID="20" restricted="1"><dc:title>0x1F</dc:title><upnp:class>object.item.videoItem</upnp:class>',
  '<res protocolInfo="http-get:*:video/mpeg:*" size="000123" duration="00:45:00">http://192.168.1.10:49152/web/media/302.mpg</res></item>',
  '<item id="303" parentID="20" restricted="1"><dc:title>  Spaced   Title  </dc:title><upnp:class>object.item.audioItem</upnp:class>',
  '<res protocolInfo="http-get:*:audio/mpeg:*" size="1e3" duration="1:02">http://192.168.1.10:49152/web/media/303.mp3</res></item>',
  '<item id="304" parentID="25" restricted="1"><dc:title>Grey&#39;s Anatomy &#38; Co S2 E3</dc:title><dc:description>Double &amp;#39; and &amp;#38; stay escaped</dc:description><upnp:class>object.item.videoItem</upnp:class>',
  '<res protocolInfo="http-get:*:video/mpeg-tts:*" size="2048" duration="0:44:00" parentTaskName="Grey&#39;s S2 E3">http://192.168.1.10:49152/web/media/304.ts?x=1&#38;y=2&#x26;z=3</res></item>'
].join(''))

const numericOuterEnvelope = `<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <u:BrowseResponse xmlns:u="urn:schemas-upnp-org:service:ContentDirectory:1">
      <Result>&#60;DIDL-Lite xmlns:dc=&#34;http://purl.org/dc/elements/1.1/&#34; xmlns=&#34;urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/&#34;&#62;&#60;item id=&#34;400&#34; parentID=&#34;20&#34;&#62;&#60;dc:title&#62;Tom &#38;amp; Jerry&#39;s&#60;/dc:title&#62;&#60;res size=&#34;1&#34;&#62;http://h/a.ts?x=1&#38;amp;y=2&#60;/res&#62;&#60;/item&#62;&#60;/DIDL-Lite&#62;</Result>
      <NumberReturned>1</NumberReturned>
    </u:BrowseResponse>
  </s:Body>
</s:Envelope>`

const edgeCaseMediaServer = readFixture('media-server.xml')
  .replace('<friendlyName>Fetch TV Test Box</friendlyName>', '<friendlyName>Jim&#39;s &#38; Kim&#x27;s Box</friendlyName>')
  .replace('<modelName>Mighty</modelName>', '<modelName note="a&#34;b">Mighty &#x3C;3&#x3E;</modelName>')

const edgeCaseFixtures = {
  'edge-root': edgeCaseRoot,
  'edge-shows': edgeCaseShows,
  'edge-items': edgeCaseItems,
  'edge-numeric-outer': numericOuterEnvelope,
  'edge-media-server': edgeCaseMediaServer
}

const fileFixtures = () => Object.fromEntries(
  readdirSync(fixturesDir)
    .filter(name => name.endsWith('.xml'))
    .sort()
    .map(name => [name, readFixture(name)])
)

const parseWithDidl = (xml) => {
  const outer = parseXml(xml)
  const result = findNode({ obj: outer, path: 'Envelope.Body.BrowseResponse.Result' })
  const inner = typeof result === 'string' ? parseXml(result) : null
  return { outer, inner }
}

const typed = (value) => JSON.parse(JSON.stringify(value, (key, val) => {
  if (typeof val === 'number') return { number: String(val) }
  if (typeof val === 'boolean') return { boolean: String(val) }
  return val
}))

const serveBox = ({ host, fixtureFor }) => {
  nock(host).persist().get('/MediaServer.xml').reply(200, readFixture('media-server.xml'))
  nock(host).persist().post(CONTROL_PATH).reply((uri, body) => {
    const objectId = String(body).match(/<ObjectID>([^<]+)<\/ObjectID>/)?.[1]
    return [200, fixtureFor(objectId)]
  })
}

const listRecordings = async ({ host }) => {
  const location = await discoverFetch({ ip: new URL(host).hostname, port: new URL(host).port })
  let outcome = null
  const recordings = await getFetchRecordings({
    location,
    onProgress: () => {},
    onOutcome: (result) => { outcome = result.outcome }
  })
  return { outcome, recordings }
}

const edgeCaseLocations = async () => {
  const host = 'http://192.168.1.12:49152'
  nock(host).persist().get('/MediaServer.xml').reply(200, edgeCaseMediaServer)
  return parseLocations([`${host}/MediaServer.xml`])
}

const fixtureListing = () => listRecordings({ host: HOST })

const edgeCaseListing = () => listRecordings({ host: 'http://192.168.1.11:49152' })

const buildActual = async () => {
  serveBox({
    host: HOST,
    fixtureFor: (objectId) => (
      objectId === '0' ? readFixture('browse-root.xml') :
      objectId === '1' ? readFixture('browse-recordings.xml') :
      ['10', '11', '12'].includes(objectId) ? readFixture('browse-show-items.xml') :
      readFixture('browse-empty.xml')
    )
  })
  serveBox({
    host: 'http://192.168.1.11:49152',
    fixtureFor: (objectId) => (
      objectId === '0' ? edgeCaseRoot :
      ['1', '0001'].includes(objectId) ? edgeCaseShows :
      edgeCaseItems
    )
  })
  const parsed = Object.fromEntries(
    Object.entries({ ...fileFixtures(), ...edgeCaseFixtures })
      .map(([name, xml]) => [name, parseWithDidl(xml)])
  )
  return typed({
    parsed,
    listings: {
      fixtures: await fixtureListing(),
      edgeCases: await edgeCaseListing()
    },
    locations: await edgeCaseLocations()
  })
}

beforeEach(() => {
  nock.cleanAll()
})

after(() => {
  nock.restore()
})

test('golden: parsed Fetch box XML and listings match the recorded snapshot', async () => {
  const actual = await buildActual()
  if (isUpdatingGolden) writeFileSync(goldenPath, `${JSON.stringify(actual, null, 2)}\n`)
  const expected = JSON.parse(readFileSync(goldenPath, 'utf-8'))
  assert.deepEqual(actual, expected)
})
