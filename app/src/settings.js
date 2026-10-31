import fs from 'node:fs/promises'
import path from 'node:path'

export const createSettings = (folder) => {
  const filePath = path.join(folder, 'settings.json')
  let cache = null

  const read = async () => {
    if (cache) return cache
    cache = await fs.readFile(filePath, 'utf8').then(JSON.parse, () => ({}))
    return cache
  }

  const update = async (changes) => {
    cache = { ...(await read()), ...changes }
    await fs.mkdir(folder, { recursive: true })
    await fs.writeFile(filePath, JSON.stringify(cache, null, 2))
    return cache
  }

  return { read, update }
}
