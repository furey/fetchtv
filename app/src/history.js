import fs from 'node:fs/promises'
import path from 'node:path'
import { loadSavedFiles } from '../../fetchtv.js'

const SIZE_TOLERANCE = 1024

export const createHistory = (folder) => {
  const filePath = path.join(folder, 'history.json')
  let cache = null

  const read = async () => {
    if (cache) return cache
    cache = await fs.readFile(filePath, 'utf8').then(JSON.parse, () => ({ entries: {} }))
    cache.entries = cache.entries ?? {}
    return cache
  }

  const recordFolder = async ({ folder: saveFolder, boxKey, results, now = new Date() }) => {
    const history = await read()
    const savedFiles = await loadSavedFiles(saveFolder)
    results
      .filter(result => result.status === 'saved' || result.status === 'already_saved')
      .forEach(result => {
        const entry = savedFiles[result.id]
        if (typeof entry?.path !== 'string') return
        const key = historyKey({ boxKey, id: result.id })
        const savedAt = result.status === 'already_saved' && history.entries[key]?.savedAt
          ? history.entries[key].savedAt
          : now.toISOString()
        history.entries[key] = { id: result.id, path: path.resolve(saveFolder, entry.path), size: entry.size ?? null, savedAt }
      })
    await fs.mkdir(folder, { recursive: true })
    await fs.writeFile(filePath, JSON.stringify(history, null, 2))
  }

  return { read, recordFolder }
}

export const savedStates = async ({ episodes, history, boxKey, folder }) => {
  const savedFiles = folder ? await loadSavedFiles(folder) : {}
  return Object.fromEntries(await Promise.all(episodes.map(async episode => [
    episode.id,
    await savedStateOf({ candidates: candidatesFor({ episode, history, boxKey, folder, savedFiles }) })
  ])))
}

const candidatesFor = ({ episode, history, boxKey, folder, savedFiles }) => {
  const fromHistory = history.entries[historyKey({ boxKey, id: episode.id })]
  const fromFolder = savedFiles[episode.id]
  const folderCandidate = typeof fromFolder?.path === 'string' && Number(fromFolder.size) > 0
    ? { path: path.resolve(folder, fromFolder.path), size: fromFolder.size, savedAt: null }
    : null
  return [fromHistory, folderCandidate].filter(Boolean)
}

const savedStateOf = async ({ candidates }) => {
  if (candidates.length === 0) return { state: 'new' }
  for (const candidate of candidates) {
    if (await isComplete(candidate)) return { state: 'saved', path: candidate.path, savedAt: candidate.savedAt }
  }
  const latest = candidates[0]
  return { state: 'missing', path: latest.path, savedAt: latest.savedAt }
}

const isComplete = async ({ path: filePath, size }) => {
  const stats = await fs.stat(filePath).catch(() => null)
  if (!stats?.isFile()) return false
  return !(Number(size) > 0) || stats.size >= Number(size) - SIZE_TOLERANCE
}

const historyKey = ({ boxKey, id }) => `${boxKey}|${id}`
