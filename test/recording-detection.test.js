import { test, beforeEach, after } from 'node:test'
import assert from 'node:assert/strict'
import nock from 'nock'

import { isCurrentlyRecording } from '../fetchtv.js'

const HOST = 'http://192.168.1.70:49152'
const URL_PATH = '/item/1'
const FULL_URL = `${HOST}${URL_PATH}`

const MAX_OCTET_RECORDING = 4398046510080

beforeEach(() => {
  nock.cleanAll()
})

after(() => {
  nock.restore()
})

test('isCurrentlyRecording: size = -1 is the in-progress sentinel → true (no HEAD)', async () => {
  const result = await isCurrentlyRecording({ title: 'X', size: -1, url: FULL_URL })
  assert.equal(result, true)
})

test('isCurrentlyRecording: size = 0 is treated as in-progress → true (no HEAD)', async () => {
  const result = await isCurrentlyRecording({ title: 'X', size: 0, url: FULL_URL })
  assert.equal(result, true)
})

test('isCurrentlyRecording: non-finite size → true (no HEAD)', async () => {
  const result = await isCurrentlyRecording({ title: 'X', size: NaN, url: FULL_URL })
  assert.equal(result, true)
})

test('isCurrentlyRecording: comfortably-final size short-circuits to false', async () => {
  const finalSize = 1_000_000_000
  const result = await isCurrentlyRecording({ title: 'X', size: finalSize, url: FULL_URL })
  assert.equal(result, false)
})

test('isCurrentlyRecording: size exactly equal to the recording marker → true', async () => {
  const result = await isCurrentlyRecording({
    title: 'X',
    size: MAX_OCTET_RECORDING,
    url: FULL_URL,
  })
  assert.equal(result, true)
})

test('isCurrentlyRecording: borderline size triggers HEAD, returns true on marker length', async () => {
  nock(HOST).head(URL_PATH).reply(200, '', { 'content-length': String(MAX_OCTET_RECORDING) })
  const result = await isCurrentlyRecording({
    title: 'borderline',
    size: MAX_OCTET_RECORDING - 500_000,
    url: FULL_URL,
  })
  assert.equal(result, true)
})

test('isCurrentlyRecording: borderline size with normal Content-Length returns false', async () => {
  nock(HOST).head(URL_PATH).reply(200, '', { 'content-length': '4000000000' })
  const result = await isCurrentlyRecording({
    title: 'borderline',
    size: MAX_OCTET_RECORDING - 500_000,
    url: FULL_URL,
  })
  assert.equal(result, false)
})

test('isCurrentlyRecording: HEAD 405 falls back to GET stream check', async () => {
  nock(HOST).head(URL_PATH).reply(405)
  nock(HOST).get(URL_PATH).reply(200, 'streamed-data', { 'content-length': String(MAX_OCTET_RECORDING) })
  const result = await isCurrentlyRecording({
    title: 'method-not-allowed',
    size: MAX_OCTET_RECORDING - 100_000,
    url: FULL_URL,
  })
  assert.equal(result, true)
})

test('isCurrentlyRecording: HEAD failure + GET failure returns false (not thrown)', async () => {
  nock(HOST).head(URL_PATH).reply(405)
  nock(HOST).get(URL_PATH).replyWithError('connection reset')
  const result = await isCurrentlyRecording({
    title: 'all-failed',
    size: MAX_OCTET_RECORDING - 100_000,
    url: FULL_URL,
  })
  assert.equal(result, false)
})
