import { test } from 'node:test'
import assert from 'node:assert/strict'
import path from 'node:path'

import { processPathTemplate } from '../fetchtv.js'

const baseData = {
  show_title: 'Bluey',
  recording_title: 'S1 E2 - Hospital',
  season_number: '1',
  season_number_padded: '01',
  episode_number: '2',
  episode_number_padded: '02',
  ext: 'ts',
}

test('processPathTemplate: substitutes all standard placeholders', () => {
  const result = processPathTemplate({
    templateString: '${show_title}/${season_number_padded}/${recording_title}.${ext}',
    data: baseData,
  })
  assert.equal(result, path.join('Bluey', '01', 'S1 E2 - Hospital.ts'))
})

test('processPathTemplate: produces the canonical Plex template output', () => {
  const plexTemplate =
    '${show_title}/Season ${season_number}/${show_title} - S${season_number}E${episode_number_padded}.${ext}'
  const result = processPathTemplate({ templateString: plexTemplate, data: baseData })
  assert.equal(
    result,
    path.join('Bluey', 'Season 1', 'Bluey - S1E02.ts'),
  )
})

test('processPathTemplate: sanitizes filesystem-unsafe characters from substituted values', () => {
  const result = processPathTemplate({
    templateString: '${show_title}/${recording_title}.${ext}',
    data: { ...baseData, show_title: 'Foo<>:"|?*Bar', recording_title: 'E1 / part 2' },
  })
  assert.equal(result, path.join('FooBar', 'E1', 'part 2.ts'))
})

test('processPathTemplate: throws when a referenced placeholder has no value', () => {
  assert.throws(
    () =>
      processPathTemplate({
        templateString: '${show_title}/${season_number_padded}/${recording_title}.${ext}',
        data: { ...baseData, season_number_padded: '' },
      }),
    /season_number_padded/,
  )
})

test('processPathTemplate: does not throw when a missing placeholder is not in the template', () => {
  const result = processPathTemplate({
    templateString: '${show_title}/${recording_title}.${ext}',
    data: { ...baseData, season_number_padded: '', episode_number: '', episode_number_padded: '' },
  })
  assert.equal(result, path.join('Bluey', 'S1 E2 - Hospital.ts'))
})

test('processPathTemplate: defaults ext to "ts" when not provided', () => {
  const result = processPathTemplate({
    templateString: '${show_title}/${recording_title}.${ext}',
    data: { ...baseData, ext: undefined },
  })
  assert.equal(result, path.join('Bluey', 'S1 E2 - Hospital.ts'))
})

test('processPathTemplate: collapses double path separators introduced by placeholders', () => {
  const result = processPathTemplate({
    templateString: '${show_title}//${recording_title}.${ext}',
    data: baseData,
  })
  assert.equal(result, path.join('Bluey', 'S1 E2 - Hospital.ts'))
})

test('processPathTemplate: strips leading "../" sequences to prevent directory traversal', () => {
  const result = processPathTemplate({
    templateString: '${show_title}/${recording_title}.${ext}',
    data: { ...baseData, show_title: '../../etc' },
  })
  assert.ok(!result.startsWith('..'))
  assert.ok(!result.startsWith('/'))
  assert.match(result, /etc/)
})

test('processPathTemplate: replaces ".." runs with "_" to neutralize traversal', () => {
  const result = processPathTemplate({
    templateString: '${show_title}/${recording_title}.${ext}',
    data: { ...baseData, recording_title: 'foo..bar' },
  })
  assert.match(result, /foo_bar/)
})
