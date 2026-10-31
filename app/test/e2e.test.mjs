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

    await page.getByLabel('Or type its IP address').fill('300.1.2.3')
    await page.getByRole('button', { name: 'Connect' }).click()
    await page.getByText('That is not an IP address.').waitFor()

    await page.getByLabel('Or type its IP address').fill(box.address)
    await page.getByRole('button', { name: 'Connect' }).click()
    await page.getByRole('heading', { name: 'Choose recordings' }).waitFor()
    await capture({ app, page, name: '2-recordings' })

    await page.getByRole('button', { name: 'Change', exact: true }).click()
    await page.getByRole('heading', { name: 'Fetch box found' }).waitFor({ timeout: 20000 })
    await capture({ app, page, name: '1-choose-box' })
    await page.getByRole('button', { name: /Test Fetch Box/ }).click()
    await page.getByRole('heading', { name: 'Choose recordings' }).waitFor()

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
    await page.getByRole('heading', { name: 'Choose a folder' }).waitFor()
    await stubFolderPicker({ app, folder: saveFolder })
    await page.getByRole('button', { name: 'Change', exact: true }).click()
    await page.locator(`#folder-path[data-path="${cssString(saveFolder)}"]`).waitFor()
    await page.locator('#folder-info', { hasText: 'Needs' }).waitFor()
    await capture({ app, page, name: '3-folder' })

    await page.getByRole('button', { name: 'Save', exact: true }).click()
    await page.getByRole('heading', { name: 'All done' }).waitFor({ timeout: 60000 })
    await capture({ app, page, name: '4-finished' })
    assert.deepEqual(await totals(page), { 'Saved': '8', 'Already saved': '0', 'Not saved': '0' })

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
  const saveFolder = path.join(await freshDir('plex'), 'Movies', 'Fetch recordings')
  await fs.writeFile(path.join(userData, 'settings.json'), JSON.stringify({ lastAddress: box.address, lastFolder: saveFolder }))
  const app = await launchApp({ userData })
  try {
    const page = await openApp(app)
    await page.getByRole('heading', { name: 'Choose recordings' }).waitFor()
    await page.getByRole('button', { name: 'Select none' }).click()
    assert.equal(await page.getByRole('button', { name: 'Next' }).isDisabled(), true)
    await page.getByText('Nothing selected').waitFor()
    await page.getByRole('button', { name: 'Select all' }).click()
    await page.getByRole('button', { name: 'Next' }).click()
    await page.locator(`#folder-path[data-path="${cssString(saveFolder)}"]`).waitFor()
    await page.getByRole('switch', { name: 'Names for Plex, Jellyfin or Kodi' }).click()
    const infoButton = page.getByRole('button', { name: 'About names for Plex, Jellyfin or Kodi' })
    await infoButton.click()
    await page.locator('#example-on', { hasText: 'Backyard Builders/Season 03/Backyard Builders - S03E01.ts' }).waitFor()
    await capture({ app, page, name: '3-folder-info' })
    await page.keyboard.press('Escape')
    await page.locator('#info-dialog').waitFor({ state: 'hidden' })
    assert.equal(await infoButton.evaluate(button => document.activeElement === button), true)
    await capture({ app, page, name: '3-folder-plex' })
    await page.getByRole('button', { name: 'Save', exact: true }).click()
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
    assert.deepEqual(await totals(page), { 'Saved': '0', 'Already saved': '9', 'Not saved': '0' })

    await page.getByRole('button', { name: 'Choose other recordings' }).click()
    await page.locator('#saved-summary', { hasText: '0 new · 9 already saved' }).waitFor()
    assert.equal(await page.getByRole('button', { name: 'Next' }).isDisabled(), true)
    await page.getByRole('button', { name: 'Select all' }).click()
    await page.getByRole('button', { name: 'Next' }).click()
    await page.locator('#already-here', { hasText: '9 already here' }).waitFor()
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
    await page.getByRole('heading', { name: 'Choose recordings' }).waitFor()
    await page.getByRole('button', { name: 'Next' }).click()
    await page.getByRole('button', { name: 'Save', exact: true }).click()
    await page.getByRole('heading', { name: 'Saving', exact: true }).waitFor()
    await page.locator('.status', { hasText: / MB of / }).first().waitFor()
    await page.waitForTimeout(1500)
    await capture({ app, page, name: '4-saving' })
    await page.getByRole('button', { name: 'Stop', exact: true }).click()
    await page.getByRole('heading', { name: 'Stopped', exact: true }).waitFor({ timeout: 30000 })
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
    await page.getByRole('heading', { name: 'Choose recordings' }).waitFor()
    await page.getByRole('button', { name: 'Next' }).click()
    await page.getByRole('button', { name: 'Save', exact: true }).click()
    await page.getByRole('heading', { name: 'Some not saved' }).waitFor({ timeout: 60000 })
    assert.deepEqual(await totals(page), { 'Saved': '3', 'Already saved': '0', 'Not saved': '1' })
    await page.locator('#problem-list').getByText('Harbour Lights: S1 E2 - Low Tide').waitFor()
    await page.locator('#problem-list .row-main').first().click()
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
    await page.getByRole('heading', { name: 'No recordings on this box' }).waitFor()
    await capture({ app, page, name: '2-empty' })
    await page.getByRole('button', { name: 'More about: No recordings on this box' }).click()
    await page.locator('#cancelled-account', { hasText: 'A Fetch box whose account is cancelled shows its setup screen on the TV and lists no recordings.' }).waitFor()
    await page.mouse.click(5, 5)
    await page.locator('#info-dialog').waitFor({ state: 'hidden' })
  } finally {
    await app.close()
    await box.stop()
  }
})

test('saved recordings: still there is unticked, moved or deleted is ticked with the reason, new is ticked', async () => {
  const box = await startFakeBox({ shows: [demoShows()[0]] })
  const userData = await freshDir('user-data-history')
  const saveFolder = await freshDir('history')
  await fs.writeFile(path.join(userData, 'settings.json'), JSON.stringify({ lastAddress: box.address, lastFolder: saveFolder }))
  const first = await launchApp({ userData })
  try {
    const page = await openApp(first)
    await page.getByRole('heading', { name: 'Choose recordings' }).waitFor()
    await page.locator('#saved-summary', { hasText: '4 new' }).waitFor()
    await page.getByRole('button', { name: 'Select none' }).click()
    const harbour = page.locator('li.show-row[data-show="Harbour Lights"]')
    await harbour.locator('.row-main').first().click()
    await harbour.getByLabel('S1 E1 - The Lighthouse Keeper').check()
    await harbour.getByLabel('S1 E2 - Low Tide').check()
    await page.getByRole('button', { name: 'Next' }).click()
    await page.getByRole('button', { name: 'Save', exact: true }).click()
    await page.getByRole('heading', { name: 'All done' }).waitFor({ timeout: 60000 })
  } finally {
    await first.close()
  }
  await fs.rm(path.join(saveFolder, 'Harbour Lights', 'S1 E2 - Low Tide.ts'))
  const second = await launchApp({ userData })
  try {
    const page = await openApp(second)
    await page.locator('#saved-summary', { hasText: '2 new · 1 already saved · 1 moved or deleted' }).waitFor()
    const harbour = page.locator('li.show-row[data-show="Harbour Lights"]')
    await harbour.locator('.row-main').first().click()
    const row = (title) => harbour.locator('.episode-row', { hasText: title })
    assert.equal(await row('S1 E1').locator('.row-main').getAttribute('data-saved'), 'saved')
    assert.equal(await harbour.getByLabel('S1 E1 - The Lighthouse Keeper').isChecked(), false)
    await row('S1 E1').getByText('Saved', { exact: true }).waitFor()
    assert.equal(await row('S1 E2').locator('.row-main').getAttribute('data-saved'), 'missing')
    assert.equal(await harbour.getByLabel('S1 E2 - Low Tide').isChecked(), true)
    await row('S1 E2').getByRole('button', { name: 'Moved or deleted' }).click()
    await page.locator('#missing-detail', { hasText: 'to Harbour Lights/S1 E2 - Low Tide.ts. It is no longer there.' }).waitFor()
    assert.equal(await harbour.getByLabel('S1 E2 - Low Tide').isChecked(), true)
    await page.keyboard.press('Escape')
    await page.locator('#info-dialog').waitFor({ state: 'hidden' })
    assert.equal(await row('S1 E3').locator('.row-main').getAttribute('data-saved'), 'new')
    assert.equal(await harbour.getByLabel('S1 E3 - Storm Warning').isChecked(), true)
    await capture({ app: second, page, name: '2-recordings-history' })
  } finally {
    await second.close()
    await box.stop()
  }
})

test('saved recordings: a fetchtv.json in the folder marks files saved by the command-line tool', async () => {
  const box = await startFakeBox({ shows: [demoShows()[0]] })
  const userData = await freshDir('user-data-cli')
  const saveFolder = await freshDir('cli')
  const item = [...box.catalogue.items.values()].find(entry => entry.title === 'S1 E3 - Storm Warning')
  await fs.mkdir(path.join(saveFolder, 'Harbour Lights'), { recursive: true })
  await fs.writeFile(path.join(saveFolder, 'Harbour Lights', 'S1 E3 - Storm Warning.ts'), recordingBytes(item))
  await fs.writeFile(path.join(saveFolder, 'fetchtv.json'), JSON.stringify({ [item.id]: { title: item.title, size: item.actualSize, path: 'Harbour Lights/S1 E3 - Storm Warning.ts' } }))
  await fs.writeFile(path.join(userData, 'settings.json'), JSON.stringify({ lastAddress: box.address, lastFolder: saveFolder }))
  const app = await launchApp({ userData })
  try {
    const page = await openApp(app)
    await page.locator('#saved-summary', { hasText: '3 new · 1 already saved' }).waitFor()
    assert.equal(await page.locator(`.episode-row[data-episode="${item.id}"] .row-main`).getAttribute('data-saved'), 'saved')
  } finally {
    await app.close()
    await box.stop()
  }
})

test('welcome mark: the animation rests on the exact mark.svg shapes, loops, and stops for reduced motion', async () => {
  const userData = await freshDir('user-data-mark')
  const app = await launchApp({ userData })
  try {
    const page = await app.firstWindow()
    await page.getByRole('heading', { name: 'Fetch My Recordings' }).waitFor()
    const markFile = await fs.readFile(path.join(appRoot, 'src', 'renderer', 'mark.svg'), 'utf8')
    const fileShapes = {
      chips: [...markFile.matchAll(/<rect ([^>]*)\/>/g)].map(([, attributes]) => attributesOf(attributes)),
      tray: attributesOf(markFile.match(/<path ([^>]*)\/>/)[1])
    }
    const pageShapes = await page.evaluate(() => {
      const read = (element) => Object.fromEntries([...element.attributes].filter(attribute => attribute.name !== 'class').map(attribute => [attribute.name, attribute.value]))
      const mark = document.getElementById('welcome-mark')
      return { chips: [...mark.querySelectorAll('rect')].map(read), tray: read(mark.querySelector('path')) }
    })
    assert.deepEqual(pageShapes, fileShapes)
    const running = await page.evaluate(() => document.getAnimations().filter(animation => animation.animationName === 'mark-chip-drop').map(animation => animation.effect.getTiming().iterations))
    assert.deepEqual(running, [Infinity, Infinity, Infinity])
    await page.emulateMedia({ reducedMotion: 'reduce' })
    assert.equal(await page.evaluate(() => document.getAnimations().filter(animation => animation.animationName.startsWith('mark-chip')).length), 0)
  } finally {
    await app.close()
  }
})

const attributesOf = (text) => Object.fromEntries([...text.matchAll(/([a-z-]+)="([^"]*)"/g)].map(([, name, value]) => [name, value]))

test('the window is locked down: isolated, sandboxed, no Node in the page, strict CSP', async () => {
  const box = await startFakeBox()
  const userData = await freshDir('user-data-security')
  await fs.writeFile(path.join(userData, 'settings.json'), JSON.stringify({ lastAddress: box.address }))
  const app = await launchApp({ userData })
  try {
    const page = await openApp(app)
    const consoleErrors = []
    page.on('console', message => { if (message.type() === 'error') consoleErrors.push(message.text()) })
    await page.getByRole('heading', { name: 'Choose recordings' }).waitFor()
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
    const listed = page.getByRole('heading', { name: 'Choose recordings' })
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
  await capture({ app, page, name: '0-welcome' })
  if (screenshotDir) await captureAtSize({ app, page, name: '0-welcome-minimum', width: 640, height: 560 })
  await page.getByRole('button', { name: 'About this app' }).click()
  await page.locator('#independence', { hasText: 'It is not made, endorsed, or supported by Fetch TV.' }).waitFor()
  const githubLinks = page.getByRole('button', { name: 'fetchtv on GitHub' })
  assert.equal(await page.locator('#info-body').getByRole('button', { name: 'fetchtv on GitHub' }).getAttribute('title'), 'https://github.com/furey/fetchtv')
  assert.equal(await githubLinks.count(), 2)
  await capture({ app, page, name: '0-about' })
  await page.keyboard.press('Escape')
  await page.locator('#info-dialog').waitFor({ state: 'hidden' })
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
  await page.evaluate(() => { if (!document.querySelector('dialog[open]')) document.activeElement?.blur() })
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

const cssString = (value) => value.replaceAll('\\', '\\\\').replaceAll('"', '\\"')
