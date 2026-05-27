import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import path from 'node:path'

import { parseXml, findNode } from '../fetchtv.js'

const fixturesDir = path.join(path.dirname(fileURLToPath(import.meta.url)), 'fixtures')

const readFixture = (name) =>
  readFileSync(path.join(fixturesDir, name), 'utf-8')

const buildEntityRichBrowseResponse = ({ itemCount }) => {
  const inner = Array.from({ length: itemCount }, (_, i) =>
    `&lt;item id=&quot;${1000 + i}&quot; parentID=&quot;10&quot; restricted=&quot;1&quot;&gt;` +
    `&lt;dc:title&gt;S1 E${i + 1} - Episode ${i + 1}&lt;/dc:title&gt;` +
    `&lt;dc:description&gt;Episode ${i + 1} of Season 1&lt;/dc:description&gt;` +
    `&lt;upnp:class&gt;object.item.videoItem&lt;/upnp:class&gt;` +
    `&lt;res protocolInfo=&quot;http-get:*:video/mpeg-tts:*&quot; size=&quot;1073741824&quot; ` +
    `duration=&quot;1:23:45&quot;&gt;http://192.168.1.10/item/${1000 + i}&lt;/res&gt;` +
    `&lt;/item&gt;`
  ).join('')

  return `<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <u:BrowseResponse xmlns:u="urn:schemas-upnp-org:service:ContentDirectory:1">
      <Result>&lt;DIDL-Lite xmlns:dc=&quot;http://purl.org/dc/elements/1.1/&quot; ` +
      `xmlns:upnp=&quot;urn:schemas-upnp-org:metadata-1-0/upnp/&quot; ` +
      `xmlns=&quot;urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/&quot;&gt;${inner}&lt;/DIDL-Lite&gt;</Result>
      <NumberReturned>${itemCount}</NumberReturned>
      <TotalMatches>${itemCount}</TotalMatches>
      <UpdateID>1</UpdateID>
    </u:BrowseResponse>
  </s:Body>
</s:Envelope>`
}

test('parseXml: returns null for empty / non-string input', () => {
  assert.equal(parseXml(''), null)
  assert.equal(parseXml(null), null)
  assert.equal(parseXml(undefined), null)
  assert.equal(parseXml(42), null)
})

test('parseXml: parses a simple SOAP Browse response', () => {
  const xml = readFixture('browse-root.xml')
  const parsed = parseXml(xml)
  const result = findNode({ obj: parsed, path: 'Envelope.Body.BrowseResponse.Result' })
  assert.equal(typeof result, 'string')
  assert.match(result, /DIDL-Lite/)
})

test('parseXml: REGRESSION — Browse response with >1000 entity references still parses', () => {
  // fast-xml-parser 4.5.5 introduced a default maxTotalExpansions: 1000 cap.
  // 60 items × ~24 entity refs ≈ 1440 expansions in the outer Envelope parse.
  // If this test ever fails, the entity cap regression has returned.
  const xml = buildEntityRichBrowseResponse({ itemCount: 60 })
  const parsed = parseXml(xml)
  assert.ok(parsed, 'expected parsed envelope, got null')
  const result = findNode({ obj: parsed, path: 'Envelope.Body.BrowseResponse.Result' })
  assert.equal(typeof result, 'string', 'Result text must survive entity decode')
  assert.match(result, /S1 E60/, 'Result must include all items, not be truncated')
})

test('parseXml: nested Browse Result re-parses to DIDL-Lite with all items', () => {
  const xml = buildEntityRichBrowseResponse({ itemCount: 60 })
  const outer = parseXml(xml)
  const resultText = findNode({ obj: outer, path: 'Envelope.Body.BrowseResponse.Result' })
  const inner = parseXml(resultText)
  const items = inner['DIDL-Lite'].item
  assert.equal(items.length, 60)
})

test('parseXml: tolerates malformed XML without throwing', () => {
  const result = parseXml('<not-actually-valid')
  assert.equal(result, null)
})
