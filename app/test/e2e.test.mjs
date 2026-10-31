import { test, before, after } from 'node:test'
import assert from 'node:assert/strict'
import fs from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { _electron as electron } from 'playwright-core'
import { MB, demoShows, recordingBytes, startFakeBox } from './fake-box.mjs'

const appRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const screenshotDir = process.env.FETCHTV_SCREENSHOTS_DIR
const packagedExecutable = process.env.FETCHTV_E2E_EXECUTABLE
const skipSsdp = Boolean(process.env.FETCHTV_E2E_SKIP_SSDP)
const UNREACHABLE_ADDRESS = '192.0.2.1'

let tmpRoot = null

before(async () => {
  tmpRoot = await fs.mkdtemp(path.join(os.tmpdir(), 'fetchtv-e2e-'))
  if (screenshotDir) await fs.mkdir(screenshotDir, { recursive: true })
})

after(async () => {
  await fs.rm(tmpRoot, { recursive: true, force: true })
})

test('first run: no box found, type the address, choose episodes, save in the default layout', async () => {
  const box = await startFakeBox()
  const userData = await freshDir('user-data')
  const saveFolder = path.join(await freshDir('saves'), 'Fetch recordings')
  await fs.writeFile(path.join(userData, 'settings.json'), JSON.stringify({ lastAddress: UNREACHABLE_ADDRESS }))
  const app = await launchApp({ userData })
  try {
    const page = await openApp(app)
    await page.getByRole('heading', { name: 'Looking for your Fetch box' }).waitFor()
    await capture({ app, page, name: '1-searching' })
    await page.getByRole('heading', { name: 'No Fetch box found' }).waitFor({ timeout: 20000 })
    await capture({ app, page, name: '1-not-found' })

    await page.getByLabel('IP address of the Fetch box').fill('300.1.2.3')
    await page.getByRole('button', { name: 'Connect' }).click()
    await page.getByText('Type the address as four numbers').waitFor()

    await page.getByLabel('IP address of the Fetch box').fill(box.address)
    await page.getByRole('button', { name: 'Connect' }).click()
    await page.getByRole('heading', { name: 'Choose the recordings to save' }).waitFor()
    await capture({ app, page, name: '2-recordings' })

    await page.getByRole('button', { name: 'Use a different box' }).click()
    await page.getByRole('heading', { name: 'Fetch box found' }).waitFor({ timeout: 20000 })
    await capture({ app, page, name: '1-choose-box' })
    await page.getByRole('button', { name: /Test Fetch Box/ }).click()
    await page.getByRole('heading', { name: 'Choose the recordings to save' }).waitFor()

    const harbour = page.locator('li.show-row[data-show="Harbour Lights"]')
    await harbour.locator('.row-main').first().click()
    await harbour.getByLabel('S1 E4 - Safe Harbour').uncheck()
    assert.equal(await harbour.locator('.row-main input[type="checkbox"]').first().evaluate(input => input.indeterminate), true)
    const coastal = page.locator('li.show-row[data-show="Coastal Kitchen"]')
    await coastal.locator('.row-main').first().click()
    assert.equal(await coastal.getByLabel('S2 E6 - Sourdough').isDisabled(), true)
    await capture({ app, page, name: '2-recordings-episodes' })
    await page.getByText('8 recordings selected').waitFor()

    await page.getByRole('button', { name: 'Next' }).click()
    await page.getByRole('heading', { name: 'Choose where to save' }).waitFor()
    await stubFolderPicker({ app, folder: saveFolder })
    await page.getByRole('button', { name: 'Change folder' }).click()
    await page.locator('#folder-path', { hasText: saveFolder }).waitFor()
    await page.getByText('These recordings need about').waitFor()
    await capture({ app, page, name: '3-folder' })

    await page.getByRole('button', { name: 'Start saving' }).click()
    await page.getByRole('heading', { name: 'All done' }).waitFor({ timeout: 60000 })
    await capture({ app, page, name: '4-finished' })
    assert.deepEqual(await totals(page), { 'Saved': '8', 'Already saved, so skipped': '0', 'Not saved': '0' })

    const expected = [
      'Backyard Builders/S3 E1 - The Shed.ts',
      'Harbour Lights/S1 E1 - The Lighthouse Keeper.ts',
      'Harbour Lights/S1 E3 - Storm Warning.ts',
      'Morning Weather Report/Morning Weather Report.ts',
      'The Night Market/The Night Market.ts'
    ]
    const files = await listFiles(saveFolder)
    for (const file of expected) assert.ok(files.includes(file), `${file} missing from ${files.join(', ')}`)
    assert.equal(files.some(file => file.includes('Safe Harbour')), false)
    assert.equal(files.some(file => file.includes('Sourdough')), false)
    assert.equal(files.some(file => /\.(part|lock)$/.test(file)), false)
    await assertSameBytes({ box, folder: saveFolder, file: 'Harbour Lights/S1 E1 - The Lighthouse Keeper.ts', title: 'S1 E1 - The Lighthouse Keeper' })
    assert.ok(files.includes('fetchtv.json'))
  } finally {
    await app.close()
    await box.stop()
  }
})

test('next run: remembers the box and folder, saves in the Plex layout, then skips what is saved', async () => {
  const box = await startFakeBox({ port: 0 })
  const userData = await freshDir('user-data-plex')
  const saveFolder = await freshDir('plex')
  await fs.writeFile(path.join(userData, 'settings.json'), JSON.stringify({ lastAddress: box.address, lastFolder: saveFolder }))
  const app = await launchApp({ userData })
  try {
    const page = await openApp(app)
    await page.getByRole('heading', { name: 'Choose the recordings to save' }).waitFor()
    await page.getByRole('button', { name: 'Select none' }).click()
    assert.equal(await page.getByRole('button', { name: 'Next' }).isDisabled(), true)
    await page.getByText('Nothing selected').waitFor()
    await page.getByRole('button', { name: 'Select all' }).click()
    await page.getByRole('button', { name: 'Next' }).click()
    await page.locator('#folder-path', { hasText: saveFolder }).waitFor()
    await page.getByRole('switch', { name: 'Name files for Plex, Jellyfin or Kodi' }).click()
    await page.getByText('Backyard Builders/Season 03/Backyard Builders - S03E01.ts').waitFor()
    await capture({ app, page, name: '3-folder-plex' })
    await page.getByRole('button', { name: 'Start saving' }).click()
    await page.getByRole('heading', { name: 'All done' }).waitFor({ timeout: 60000 })

    const files = await listFiles(saveFolder)
    for (const file of [
      'Harbour Lights/Season 01/Harbour Lights - S01E04.ts',
      'Backyard Builders/Season 03/Backyard Builders - S03E02.ts',
      'Coastal Kitchen/Season 02/Coastal Kitchen - S02E05.ts',
      'The Night Market/The Night Market.ts'
    ]) assert.ok(files.includes(file), `${file} missing from ${files.join(', ')}`)

    await page.getByRole('button', { name: 'Save again' }).click()
    await page.getByRole('heading', { name: 'All done' }).waitFor({ timeout: 60000 })
    assert.deepEqual(await totals(page), { 'Saved': '0', 'Already saved, so skipped': '9', 'Not saved': '0' })

    await page.getByRole('button', { name: 'Choose other recordings' }).click()
    await page.getByRole('button', { name: 'Next' }).click()
    await page.getByText('9 of these recordings are already saved in this folder, so the save skips them.').waitFor()
  } finally {
    await app.close()
    await box.stop()
  }
})

test('stop part way, then save again carries on from the partial file', async () => {
  const box = await startFakeBox({
    shows: [{ title: 'Harbour Lights', items: [{ title: 'S1 E1 - The Lighthouse Keeper', size: 24 * MB, duration: '0:58:12' }, { title: 'S1 E2 - Low Tide', size: 24 * MB, duration: '0:57:40' }] }],
    quirks: { rangeNotSatisfiable: false },
    bytesPerSecond: 4 * MB
  })
  const userData = await freshDir('user-data-stop')
  const saveFolder = await freshDir('stop')
  await fs.writeFile(path.join(userData, 'settings.json'), JSON.stringify({ lastAddress: box.address, lastFolder: saveFolder }))
  const app = await launchApp({ userData })
  try {
    const page = await openApp(app)
    await page.getByRole('heading', { name: 'Choose the recordings to save' }).waitFor()
    await page.getByRole('button', { name: 'Next' }).click()
    await page.getByRole('button', { name: 'Start saving' }).click()
    await page.getByRole('heading', { name: 'Saving recordings' }).waitFor()
    await page.locator('.status', { hasText: / MB of / }).first().waitFor()
    await page.waitForTimeout(1500)
    await capture({ app, page, name: '4-saving' })
    await page.getByRole('button', { name: 'Stop saving' }).click()
    await page.getByRole('heading', { name: 'Save stopped' }).waitFor({ timeout: 30000 })
    await capture({ app, page, name: '4-stopped' })

    const files = await listFiles(saveFolder)
    assert.ok(files.some(file => file.endsWith('.ts.part')), `no partial file in ${files.join(', ')}`)
    assert.equal(files.some(file => file.endsWith('.lock')), false)

    const before = box.requests.length
    await page.getByRole('button', { name: 'Save again' }).click()
    await page.getByRole('heading', { name: 'All done' }).waitFor({ timeout: 60000 })
    const resumed = box.requests.slice(before).filter(request => request.range && request.range !== 'bytes=0-')
    assert.ok(resumed.length > 0, 'the second save did not resume with a Range request')
    await assertSameBytes({ box, folder: saveFolder, file: 'Harbour Lights/S1 E1 - The Lighthouse Keeper.ts', title: 'S1 E1 - The Lighthouse Keeper' })
    await assertSameBytes({ box, folder: saveFolder, file: 'Harbour Lights/S1 E2 - Low Tide.ts', title: 'S1 E2 - Low Tide' })
  } finally {
    await app.close()
    await box.stop()
  }
})

test('a recording the box keeps cutting off is listed as not saved, with a plain reason', async () => {
  const shows = demoShows({ size: MB })
  shows[0].items[1].quirks = { alwaysDropAt: Math.round(MB / 2) }
  const box = await startFakeBox({ shows: [shows[0]] })
  const userData = await freshDir('user-data-fail')
  const saveFolder = await freshDir('fail')
  await fs.writeFile(path.join(userData, 'settings.json'), JSON.stringify({ lastAddress: box.address, lastFolder: saveFolder }))
  const app = await launchApp({ userData })
  try {
    const page = await openApp(app)
    await page.getByRole('heading', { name: 'Choose the recordings to save' }).waitFor()
    await page.getByRole('button', { name: 'Next' }).click()
    await page.getByRole('button', { name: 'Start saving' }).click()
    await page.getByRole('heading', { name: 'Finished, but some recordings were not saved' }).waitFor({ timeout: 60000 })
    assert.deepEqual(await totals(page), { 'Saved': '3', 'Already saved, so skipped': '0', 'Not saved': '1' })
    await page.locator('#problem-list').getByText('Harbour Lights: S1 E2 - Low Tide').waitFor()
    await page.locator('#problem-list').getByText('The copy stopped part way, even after trying again.').waitFor()
    await capture({ app, page, name: '4-finished-not-saved' })
  } finally {
    await app.close()
    await box.stop()
  }
})

test('a box with no recordings explains why the list is empty', async () => {
  const box = await startFakeBox({ shows: [] })
  const userData = await freshDir('user-data-empty')
  await fs.writeFile(path.join(userData, 'settings.json'), JSON.stringify({ lastAddress: box.address }))
  const app = await launchApp({ userData })
  try {
    const page = await openApp(app)
    await page.getByRole('heading', { name: 'No recordings on this Fetch box' }).waitFor()
    await page.getByText('A Fetch box whose account is cancelled shows its setup screen on the TV and lists no recordings.').waitFor()
    await capture({ app, page, name: '2-empty' })
  } finally {
    await app.close()
    await box.stop()
  }
})

test('the window is locked down: isolated, sandboxed, no Node in the page, strict CSP', async () => {
  const box = await startFakeBox()
  const userData = await freshDir('user-data-security')
  await fs.writeFile(path.join(userData, 'settings.json'), JSON.stringify({ lastAddress: box.address }))
  const app = await launchApp({ userData })
  try {
    const page = await openApp(app)
    const consoleErrors = []
    page.on('console', message => { if (message.type() === 'error') consoleErrors.push(message.text()) })
    await page.getByRole('heading', { name: 'Choose the recordings to save' }).waitFor()
    const preferences = await app.evaluate(({ BrowserWindow }) => {
      const contents = BrowserWindow.getAllWindows()[0].webContents
      const { contextIsolation, nodeIntegration, sandbox, webSecurity } = contents.getLastWebPreferences()
      return { contextIsolation, nodeIntegration, sandbox, webSecurity }
    })
    assert.deepEqual(preferences, { contextIsolation: true, nodeIntegration: false, sandbox: true, webSecurity: true })
    const pageGlobals = await page.evaluate(() => ({
      require: typeof window.require,
      process: typeof window.process,
      api: Object.keys(window.fetchtv).sort()
    }))
    assert.equal(pageGlobals.require, 'undefined')
    assert.equal(pageGlobals.process, 'undefined')
    assert.ok(pageGlobals.api.every(name => typeof name === 'string') && pageGlobals.api.length === 14)
    const inlineScriptRan = await page.evaluate(() => new Promise(resolve => {
      window.inlineRan = false
      const script = document.createElement('script')
      script.textContent = 'window.inlineRan = true'
      document.body.append(script)
      setTimeout(() => resolve(window.inlineRan), 100)
    }))
    assert.equal(inlineScriptRan, false)
    const remoteLoaded = await page.evaluate(() => fetch('https://example.com/').then(() => true, () => false))
    assert.equal(remoteLoaded, false)
    assert.equal(consoleErrors.filter(text => !/Content Security Policy|example\.com|ERR_BLOCKED/.test(text)).length, 0, consoleErrors.join('\n'))
  } finally {
    await app.close()
    await box.stop()
  }
})

test('discovery: finds a Fetch box on the network without typing an address', { skip: skipSsdp && 'FETCHTV_E2E_SKIP_SSDP is set' }, async () => {
  const box = await startFakeBox({ ssdp: true, name: 'Fake Fetch Box' })
  const userData = await freshDir('user-data-ssdp')
  const app = await launchApp({ userData, discovery: true })
  try {
    const page = await openApp(app)
    const listed = page.getByRole('heading', { name: 'Choose the recordings to save' })
    const choose = page.getByRole('heading', { name: 'Choose your Fetch box' })
    await Promise.race([listed.waitFor({ timeout: 20000 }), choose.waitFor({ timeout: 20000 })])
    if (await choose.isVisible()) {
      await capture({ app, page, name: '1-choose-several' })
      await page.getByRole('button', { name: /Fake Fetch Box/ }).click()
      await listed.waitFor()
    }
    await page.locator('#box-card', { hasText: 'Fake Fetch Box' }).getByText(box.address).waitFor()
  } finally {
    await app.close()
    await box.stop()
  }
})

const launchApp = async ({ userData, discovery = false }) => {
  const env = {
    ...process.env,
    FETCHTV_USER_DATA: userData,
    FETCHTV_DISCOVERY: discovery ? 'on' : 'off',
    FETCHTV_RETRY_DELAYS_MS: '50,50'
  }
  const app = packagedExecutable
    ? await electron.launch({ executablePath: packagedExecutable, env })
    : await electron.launch({ args: [appRoot], cwd: appRoot, env })
  return app
}

const openApp = async (app) => {
  const page = await app.firstWindow()
  await page.getByRole('heading', { name: 'Fetch My Recordings' }).waitFor()
  await page.getByText('It is not made, endorsed, or supported by Fetch TV.').waitFor()
  await capture({ app, page, name: '0-welcome' })
  if (screenshotDir) await captureAtSize({ app, page, name: '0-welcome-minimum', width: 640, height: 560 })
  await page.getByRole('button', { name: 'Start', exact: true }).click()
  return page
}

const captureAtSize = async ({ app, page, name, width, height }) => {
  const original = await app.evaluate(({ BrowserWindow }) => BrowserWindow.getAllWindows()[0].getContentSize())
  await app.evaluate(({ BrowserWindow }, size) => BrowserWindow.getAllWindows()[0].setContentSize(size.width, size.height), { width, height })
  await capture({ app, page, name })
  await app.evaluate(({ BrowserWindow }, size) => BrowserWindow.getAllWindows()[0].setContentSize(size[0], size[1]), original)
}

const stubFolderPicker = ({ app, folder }) => app.evaluate(({ dialog }, chosen) => {
  dialog.showOpenDialog = async () => ({ canceled: false, filePaths: [chosen] })
}, folder)

const capture = async ({ app, page, name }) => {
  if (!screenshotDir) return
  await page.mouse.move(0, 0)
  await page.waitForTimeout(200)
  await page.screenshot({ path: path.join(screenshotDir, `${name}.png`) })
}

const totals = (page) => page.locator('#save-totals').evaluate(list => {
  const terms = [...list.querySelectorAll('dt')].map(term => term.textContent)
  const values = [...list.querySelectorAll('dd')].map(value => value.textContent)
  return Object.fromEntries(terms.map((term, index) => [term, values[index]]))
})

const assertSameBytes = async ({ box, folder, file, title }) => {
  const item = [...box.catalogue.items.values()].find(entry => entry.title === title)
  const saved = await fs.readFile(path.join(folder, file))
  assert.equal(saved.length, item.actualSize)
  assert.ok(saved.equals(recordingBytes(item)), `${file} does not match the bytes the box sent`)
}

const listFiles = async (folder) => {
  const entries = await fs.readdir(folder, { recursive: true, withFileTypes: true })
  return entries
    .filter(entry => entry.isFile())
    .map(entry => path.relative(folder, path.join(entry.parentPath, entry.name)).split(path.sep).join('/'))
    .sort()
}

const freshDir = async (name) => fs.mkdtemp(path.join(tmpRoot, `${name}-`))
