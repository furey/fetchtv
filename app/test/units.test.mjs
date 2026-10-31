import { test } from 'node:test'
import assert from 'node:assert/strict'
import { parseBoxAddress, describeBox } from '../src/boxes.js'
import { plainReason, summariseResults } from '../src/saving.js'

test('address: accepts an IP address, with or without a port or a URL around it', () => {
  assert.deepEqual(parseBoxAddress(' 192.168.1.50 '), { ip: '192.168.1.50', port: 49152 })
  assert.deepEqual(parseBoxAddress('192.168.1.50:50000'), { ip: '192.168.1.50', port: 50000 })
  assert.deepEqual(parseBoxAddress('http://192.168.1.50:49152/MediaServer.xml'), { ip: '192.168.1.50', port: 49152 })
})

test('address: rejects text that is not an address', () => {
  for (const input of ['', '300.1.2.3', '192.168.1', '192.168.1.50:99999', 'my box', undefined]) {
    assert.equal(parseBoxAddress(input), null, String(input))
  }
})

test('box: hides the usual Fetch port and shows any other', () => {
  assert.equal(describeBox({ url: 'http://192.168.1.50:49152/MediaServer.xml', friendlyName: 'Lounge' }).address, '192.168.1.50')
  assert.equal(describeBox({ url: 'http://127.0.0.1:5000/MediaServer.xml' }).address, '127.0.0.1:5000')
  assert.equal(describeBox({ url: 'http://127.0.0.1:5000/MediaServer.xml' }).name, 'Fetch box')
})

test('reasons: each result gets a plain reason with no error codes', () => {
  const reason = (status, extra = {}) => plainReason({ status, ...extra })
  assert.equal(reason('saved'), '')
  assert.equal(reason('already_saved'), 'Already saved in this folder.')
  assert.match(reason('still_recording'), /Still recording/)
  assert.match(reason('locked'), /Another save/)
  assert.equal(reason('cancelled', { warning: 'Stopped before this recording started.' }), 'Not started.')
  assert.match(reason('cancelled', { warning: 'Stopped. Partial file kept for resume.' }), /carry on/)
  assert.match(reason('failed', { error: 'Write error: ENOSPC: no space left on device' }), /ran out of space/)
  assert.match(reason('failed', { error: 'EACCES: permission denied' }), /Could not write/)
  assert.match(reason('failed', { warning: 'The Fetch box rejected the resume of X at 1 MB of 2 MB.' }), /would not carry on/)
  assert.match(reason('failed', { error: 'Download initiation failed: connect ECONNREFUSED 1.2.3.4:49152' }), /stopped answering/)
  assert.match(reason('failed', { warning: 'Download interrupted for X at 50.0%. (Connection reset) Partial file kept for resume.' }), /stopped part way/)
  assert.doesNotMatch(reason('failed', { error: 'something odd' }), /odd/)
})

test('summary: counts saved, skipped, not saved, and stopped', () => {
  const statuses = ['saved', 'saved', 'already_saved', 'failed', 'locked', 'still_recording', 'cancelled']
  assert.deepEqual(summariseResults(statuses.map(status => ({ status }))), { total: 7, saved: 2, alreadySaved: 1, notSaved: 3, stopped: 1 })
})
