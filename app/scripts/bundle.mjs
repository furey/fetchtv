import { build } from 'esbuild'
import fs from 'node:fs/promises'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const appRoot = path.dirname(path.dirname(fileURLToPath(import.meta.url)))
const repoRoot = path.dirname(appRoot)
const distDir = path.join(appRoot, 'dist')

const readRootVersion = async () =>
  JSON.parse(await fs.readFile(path.join(repoRoot, 'package.json'), 'utf8')).version

const bundleMain = async (version) => build({
  entryPoints: [path.join(appRoot, 'src', 'main.js')],
  outfile: path.join(distDir, 'main.mjs'),
  bundle: true,
  platform: 'node',
  format: 'esm',
  target: 'node22',
  external: ['electron'],
  define: { FETCHTV_VERSION: JSON.stringify(version) },
  banner: { js: "import { createRequire as fetchtvCreateRequire } from 'node:module'; const require = fetchtvCreateRequire(import.meta.url);" },
  legalComments: 'none',
  logLevel: 'warning'
})

const copyStatic = async () => {
  await fs.copyFile(path.join(appRoot, 'src', 'preload.cjs'), path.join(distDir, 'preload.cjs'))
  await fs.cp(path.join(appRoot, 'src', 'renderer'), path.join(distDir, 'renderer'), { recursive: true })
}

await fs.rm(distDir, { recursive: true, force: true })
await fs.mkdir(distDir, { recursive: true })
await bundleMain(await readRootVersion())
await copyStatic()
