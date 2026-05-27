import { test } from 'node:test'
import assert from 'node:assert/strict'

import {
  createValidFilename,
  tsToSeconds,
  processFilter,
  sortRecordingsByTitle,
  findNode,
  getXmlAttr,
  getXmlText,
  formatItem,
} from '../fetchtv.js'

test('createValidFilename: strips invalid filesystem characters', () => {
  assert.equal(createValidFilename('foo<bar>:baz'), 'foobarbaz')
  assert.equal(createValidFilename('a/b\\c'), 'abc')
  assert.equal(createValidFilename('with"quotes"|pipes'), 'withquotespipes')
  assert.equal(createValidFilename('ask?star*'), 'askstar')
})

test('createValidFilename: collapses runs of spaces', () => {
  assert.equal(createValidFilename('foo   bar    baz'), 'foo bar baz')
})

test('createValidFilename: strips ASCII control characters incl. tabs/newlines', () => {
  assert.equal(createValidFilename('foo\tbar\nbaz'), 'foobarbaz')
})

test('createValidFilename: trims leading/trailing whitespace', () => {
  assert.equal(createValidFilename('   hello   '), 'hello')
})

test('createValidFilename: replaces "." and ".." with "_"', () => {
  assert.equal(createValidFilename('.'), '_')
  assert.equal(createValidFilename('..'), '_')
})

test('createValidFilename: handles non-string input gracefully', () => {
  assert.equal(createValidFilename(), '')
  assert.equal(createValidFilename(null), '')
  assert.equal(createValidFilename(undefined), '')
})

test('createValidFilename: caps at MAX_FILENAME - 10 chars', () => {
  const long = 'x'.repeat(500)
  assert.equal(createValidFilename(long).length, 245)
})

test('createValidFilename: preserves valid unicode and punctuation', () => {
  assert.equal(createValidFilename("I'm a Celebrity"), "I'm a Celebrity")
  assert.equal(createValidFilename('S6 E2 - Episode 2'), 'S6 E2 - Episode 2')
})

test('tsToSeconds: parses HH:MM:SS', () => {
  assert.equal(tsToSeconds('1:25:30'), 1 * 3600 + 25 * 60 + 30)
  assert.equal(tsToSeconds('0:24:15'), 24 * 60 + 15)
  assert.equal(tsToSeconds('2:00:00'), 7200)
})

test('tsToSeconds: parses MM:SS', () => {
  assert.equal(tsToSeconds('5:30'), 5 * 60 + 30)
})

test('tsToSeconds: parses bare seconds', () => {
  assert.equal(tsToSeconds('42'), 42)
  assert.equal(tsToSeconds('0'), 0)
})

test('tsToSeconds: returns 0 for non-string input', () => {
  assert.equal(tsToSeconds(null), 0)
  assert.equal(tsToSeconds(undefined), 0)
  assert.equal(tsToSeconds(123), 0)
})

test('tsToSeconds: returns 0 for fully malformed strings', () => {
  assert.equal(tsToSeconds('abc'), 0)
  assert.equal(tsToSeconds(''), 0)
})

test('processFilter: lowercases and trims', () => {
  assert.deepEqual(processFilter(['Bluey', '  MasterChef  ']), ['bluey', 'masterchef'])
})

test('processFilter: splits comma-separated entries', () => {
  assert.deepEqual(processFilter(['bluey,masterchef']), ['bluey', 'masterchef'])
})

test('processFilter: drops empty / blank entries', () => {
  assert.deepEqual(processFilter(['', '  ', 'bluey']), ['bluey'])
})

test('processFilter: coerces a single string input via castArray', () => {
  assert.deepEqual(processFilter('Bluey'), ['bluey'])
})

test('processFilter: returns empty array for empty input', () => {
  assert.deepEqual(processFilter([]), [])
})

test('sortRecordingsByTitle: sorts ignoring leading "The "', () => {
  const recordings = [
    { title: 'The Amazing Race' },
    { title: 'Bluey' },
    { title: 'Australian Idol' },
  ]
  const sorted = sortRecordingsByTitle(recordings)
  assert.deepEqual(sorted.map(r => r.title), [
    'The Amazing Race',
    'Australian Idol',
    'Bluey',
  ])
})

test('sortRecordingsByTitle: is case-insensitive', () => {
  const recordings = [{ title: 'bluey' }, { title: 'Australian Idol' }]
  const sorted = sortRecordingsByTitle(recordings)
  assert.deepEqual(sorted.map(r => r.title), ['Australian Idol', 'bluey'])
})

test('sortRecordingsByTitle: does not mutate the input array', () => {
  const recordings = [{ title: 'B' }, { title: 'A' }]
  const sortTitleBefore = recordings.map(r => r.title)
  sortRecordingsByTitle(recordings)
  assert.deepEqual(recordings.map(r => r.title), sortTitleBefore)
})

test('sortRecordingsByTitle: strips sortTitle helper field from output', () => {
  const sorted = sortRecordingsByTitle([{ title: 'Bluey' }])
  assert.equal(sorted[0].sortTitle, undefined)
})

test('findNode: walks a dot-separated path', () => {
  const tree = { a: { b: { c: 'leaf' } } }
  assert.equal(findNode({ obj: tree, path: 'a.b.c' }), 'leaf')
})

test('findNode: matches namespaced keys via :suffix', () => {
  const tree = { 's:Envelope': { 's:Body': { 'u:BrowseResponse': { Result: 'x' } } } }
  const node = findNode({ obj: tree, path: 'Envelope.Body.BrowseResponse.Result' })
  assert.equal(node, 'x')
})

test('findNode: returns undefined for missing paths', () => {
  assert.equal(findNode({ obj: { a: 1 }, path: 'a.b' }), undefined)
  assert.equal(findNode({ obj: null, path: 'a' }), undefined)
  assert.equal(findNode({ obj: { a: 1 }, path: '' }), undefined)
})

test('getXmlAttr: reads @_-prefixed attributes', () => {
  assert.equal(getXmlAttr({ node: { '@_id': '42' }, attrName: 'id' }), '42')
})

test('getXmlAttr: returns defaultValue when attr is missing', () => {
  assert.equal(getXmlAttr({ node: {}, attrName: 'id', defaultValue: 'fallback' }), 'fallback')
  assert.equal(getXmlAttr({ node: null, attrName: 'id', defaultValue: 'x' }), 'x')
})

test('getXmlText: returns primitive node value as string', () => {
  assert.equal(getXmlText({ node: 'hello' }), 'hello')
  assert.equal(getXmlText({ node: 42 }), '42')
  assert.equal(getXmlText({ node: true }), 'true')
})

test('getXmlText: reads #text from object node', () => {
  assert.equal(getXmlText({ node: { '#text': 'inner', '@_attr': 'x' } }), 'inner')
})

test('getXmlText: returns defaultValue for nil node', () => {
  assert.equal(getXmlText({ node: null, defaultValue: 'fallback' }), 'fallback')
  assert.equal(getXmlText({ node: undefined, defaultValue: 'fallback' }), 'fallback')
})

test('formatItem: projects only the public-facing keys', () => {
  const item = {
    id: '100',
    title: 'S1 E1',
    item_type: 'episode',
    duration: 5130,
    size: 1073741824,
    description: 'Pilot',
    show_title: 'Bluey',
    season_number: '1',
    season_number_padded: '01',
    episode_number: '1',
    episode_number_padded: '01',
    ext: 'ts',
    url: 'http://example/100',
    parent_id: '10',
    type: 'object.item.videoItem',
  }
  const result = formatItem(item)
  assert.equal(result.type, 'episode')
  assert.equal(result.url, undefined)
  assert.equal(result.parent_id, undefined)
  assert.equal(result.id, '100')
  assert.equal(result.season_number_padded, '01')
})
