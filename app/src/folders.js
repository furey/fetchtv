import fs from 'node:fs/promises'
import path from 'node:path'
import { loadSavedFiles } from '../../fetchtv.js'

export const describeFolder = async ({ folder, items }) => {
  const existing = await nearestExistingFolder(folder)
  const [freeBytes, canWrite, savedFiles] = await Promise.all([
    freeSpaceAt(existing),
    isWritable(existing),
    loadSavedFiles(folder)
  ])
  const alreadySaved = items.filter(item => isSavedWithSize(savedFiles[item.id]))
  const alreadySavedIds = new Set(alreadySaved.map(item => String(item.id)))
  const toSave = items.filter(item => !alreadySavedIds.has(String(item.id)))
  return {
    folder,
    exists: existing === path.resolve(folder),
    freeBytes,
    canWrite,
    alreadySavedCount: alreadySaved.length,
    neededBytes: sumSizes(toSave),
    toSaveCount: toSave.length
  }
}

const nearestExistingFolder = async (folder) => {
  let current = path.resolve(folder)
  while (true) {
    const stats = await fs.stat(current).catch(() => null)
    if (stats?.isDirectory()) return current
    const parent = path.dirname(current)
    if (parent === current) return current
    current = parent
  }
}

const freeSpaceAt = async (folder) => {
  const stats = await fs.statfs(folder).catch(() => null)
  return stats ? Number(stats.bavail) * Number(stats.bsize) : null
}

const isWritable = (folder) => fs.access(folder, fs.constants.W_OK).then(() => true, () => false)

const isSavedWithSize = (entry) => typeof entry === 'object' && entry !== null && entry.size > 0

const sumSizes = (items) => items.reduce((total, item) => total + (item.size > 0 ? item.size : 0), 0)
