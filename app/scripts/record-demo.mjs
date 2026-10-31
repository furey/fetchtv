import fs from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import { parseArgs } from 'node:util'
import { fileURLToPath } from 'node:url'
import { _electron as electron } from 'playwright-core'
import { MB, startFakeBox } from '../test/fake-box.mjs'

const appRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')

const { values } = parseArgs({
  options: {
    executable: { type: 'string' },
    out: { type: 'string' },
    address: { type: 'string', default: '192.168.1.50' }
  }
})

const DEMO_SHOWS = [
  {
    title: 'Harbour Lights',
    items: [
      { title: 'S1 E1 - The Lighthouse Keeper', size: 141 * MB, duration: '0:58:12' },
      { title: 'S1 E2 - Low Tide', size: 143 * MB, duration: '0:57:40' },
      { title: 'S1 E3 - Storm Warning', size: 140 * MB, duration: '0:59:03' },
      { title: 'S1 E4 - Safe Harbour', size: 146 * MB, duration: '1:01:27' }
    ]
  },
  {
    title: 'Backyard Builders',
    items: [
      { title: 'S3 E1 - The Shed', size: 168 * MB, duration: '0:44:10' },
      { title: 'S3 E2 - The Deck', size: 171 * MB, duration: '0:44:55' }
    ]
  },
  { title: 'The Night Market', items: [{ title: 'The Night Market', size: 452 * MB, duration: '1:52:30' }] },
  {
    title: 'Coastal Kitchen',
    items: [
      { title: 'S2 E5 - Oysters', size: 126 * MB, duration: '0:28:45' },
      { title: 'S2 E6 - Sourdough', size: -1, duration: '0:00:00', stillRecording: true }
    ]
  }
]

const main = async () => {
  const out = path.resolve(values.out)
  const framesDir = path.join(out, 'frames')
  await fs.rm(framesDir, { recursive: true, force: true })
  await fs.mkdir(framesDir, { recursive: true })
  const work = await fs.mkdtemp(path.join(os.tmpdir(), 'fetchtv-demo-'))
  const userData = path.join(work, 'user-data')
  const saveFolder = path.join(work, 'Movies', 'Fetch recordings')
  await fs.mkdir(userData, { recursive: true })
  const box = await startFakeBox({ shows: DEMO_SHOWS, name: 'Lounge room box', bytesPerSecond: 200 * MB })
  await fs.writeFile(path.join(userData, 'settings.json'), JSON.stringify({ lastAddress: box.address, lastFolder: saveFolder }))
  const env = { ...process.env, FETCHTV_USER_DATA: userData, FETCHTV_DISCOVERY: 'off' }
  const app = values.executable
    ? await electron.launch({ executablePath: values.executable, env })
    : await electron.launch({ args: [appRoot], cwd: appRoot, env })
  try {
    await app.evaluate(({ shell }) => { shell.openPath = async () => '' })
    const page = await app.firstWindow()
    await page.getByRole('heading', { name: 'Fetch My Recordings' }).waitFor()
    await page.evaluate((address) => {
      state.pendingSearch = state.pendingSearch.then(({ boxes }) => ({ boxes: boxes.map(found => ({ ...found, address })) }))
    }, values.address)
    await installCursor(page)
    await page.evaluate(() => document.fonts.ready)
    const frames = await startScreencast({ page, framesDir })
    const soundLog = await logSounds(page)
    const recordingStart = Date.now()
    await page.evaluate(() => document.getAnimations().forEach(animation => { animation.currentTime = 0 }))
    await drive({ page })
    const sounds = await soundLog()
    const recordingEnd = Date.now()
    const frameList = await frames.stop()
    await fs.writeFile(path.join(out, 'timeline.json'), JSON.stringify({ recordingStart, recordingEnd, frames: frameList, sounds }, null, 2))
  } finally {
    await app.close()
    await box.stop()
    await fs.rm(work, { recursive: true, force: true })
  }
}

const drive = async ({ page }) => {
  await moveTo({ page, x: 560, y: 640 })
  await page.waitForTimeout(2700)
  await clickOn({ page, locator: page.getByRole('button', { name: 'Start', exact: true }) })
  await page.getByRole('heading', { name: 'Choose recordings' }).waitFor({ timeout: 20000 })
  await moveTo({ page, x: 700, y: 330 })
  await page.waitForTimeout(3600)
  const harbour = page.locator('li.show-row[data-show="Harbour Lights"] .row-main').first()
  await hover({ page, locator: harbour })
  await page.waitForTimeout(900)
  await clickOn({ page, locator: harbour })
  await page.waitForTimeout(1400)
  await hover({ page, locator: page.locator('.episode-row', { hasText: 'S1 E2 - Low Tide' }).locator('.row-main') })
  await page.waitForTimeout(700)
  await clickOn({ page, locator: page.getByLabel('S1 E4 - Safe Harbour') })
  await page.waitForTimeout(2900)
  await clickOn({ page, locator: page.getByRole('button', { name: 'Next' }) })
  await page.getByRole('heading', { name: 'Choose a folder' }).waitFor()
  await page.locator('#folder-info', { hasText: 'Needs' }).waitFor()
  await moveTo({ page, x: 420, y: 250 })
  await page.waitForTimeout(3000)
  await clickOn({ page, locator: page.getByRole('switch', { name: 'Names for Plex, Jellyfin or Kodi' }) })
  await page.waitForTimeout(1900)
  await clickOn({ page, locator: page.getByRole('button', { name: 'Save', exact: true }) })
  await page.waitForTimeout(400)
  await moveTo({ page, x: 720, y: 420 })
  await page.getByRole('heading', { name: 'All done' }).waitFor({ timeout: 120000 })
  await page.waitForTimeout(4200)
  await clickOn({ page, locator: page.getByRole('button', { name: 'Open folder' }) })
  await page.waitForTimeout(800)
  await moveTo({ page, x: 520, y: 400 })
  await page.waitForTimeout(2200)
}

const installCursor = (page) => page.evaluate(() => {
  const ns = 'http://www.w3.org/2000/svg'
  const cursor = document.createElement('div')
  cursor.id = 'demo-cursor'
  Object.assign(cursor.style, { position: 'fixed', left: '0', top: '0', width: '22px', height: '22px', zIndex: '2147483647', pointerEvents: 'none', transform: 'translate(-40px, -40px)' })
  const svg = document.createElementNS(ns, 'svg')
  svg.setAttribute('viewBox', '0 0 22 22')
  svg.setAttribute('width', '22')
  svg.setAttribute('height', '22')
  const arrow = document.createElementNS(ns, 'path')
  arrow.setAttribute('d', 'M2 1 L2 17 L6.5 13 L9.5 20 L12.5 18.7 L9.6 11.9 L15.5 11.9 Z')
  arrow.setAttribute('fill', '#ffffff')
  arrow.setAttribute('stroke', '#000000')
  arrow.setAttribute('stroke-width', '1.2')
  arrow.setAttribute('stroke-linejoin', 'round')
  svg.append(arrow)
  cursor.append(svg)
  document.documentElement.append(cursor)
  let position = { x: -40, y: -40 }
  document.addEventListener('mousemove', (event) => {
    position = { x: event.clientX, y: event.clientY }
    cursor.style.transform = `translate(${position.x - 2}px, ${position.y - 1}px)`
  }, true)
  document.addEventListener('mousedown', () => {
    const ring = document.createElement('div')
    Object.assign(ring.style, { position: 'fixed', left: `${position.x - 18}px`, top: `${position.y - 18}px`, width: '36px', height: '36px', borderRadius: '50%', border: '2px solid rgba(92, 225, 198, 0.9)', zIndex: '2147483646', pointerEvents: 'none' })
    document.documentElement.append(ring)
    ring.animate([{ transform: 'scale(0.4)', opacity: 1 }, { transform: 'scale(1)', opacity: 0 }], { duration: 450, easing: 'cubic-bezier(0.2, 0.8, 0.2, 1)' }).finished.then(() => ring.remove())
  }, true)
})

const startScreencast = async ({ page, framesDir }) => {
  const session = await page.context().newCDPSession(page)
  const frames = []
  session.on('Page.screencastFrame', async ({ data, metadata, sessionId }) => {
    const file = path.join(framesDir, `f${String(frames.length).padStart(5, '0')}.jpg`)
    frames.push({ file, time: metadata.timestamp * 1000 })
    await fs.writeFile(file, Buffer.from(data, 'base64'))
    await session.send('Page.screencastFrameAck', { sessionId }).catch(() => null)
  })
  await session.send('Page.startScreencast', { format: 'jpeg', quality: 92, everyNthFrame: 1 })
  return {
    stop: async () => {
      await session.send('Page.stopScreencast').catch(() => null)
      await new Promise(resolve => setTimeout(resolve, 300))
      return frames
    }
  }
}

const logSounds = async (page) => {
  await page.evaluate(() => {
    window.demoSounds = []
    const play = sounds.play
    sounds.play = (name) => {
      if (sounds.isEnabled()) window.demoSounds.push({ name, time: performance.timeOrigin + performance.now() })
      play(name)
    }
  })
  return () => page.evaluate(() => window.demoSounds)
}

const moveTo = async ({ page, x, y }) => {
  await page.mouse.move(x, y, { steps: 28 })
}

const hover = async ({ page, locator }) => {
  const box = await locator.boundingBox()
  await moveTo({ page, x: box.x + Math.min(box.width / 2, 160), y: box.y + box.height / 2 })
}

const clickOn = async ({ page, locator }) => {
  await locator.scrollIntoViewIfNeeded()
  await hover({ page, locator })
  await page.waitForTimeout(350)
  await page.mouse.down()
  await page.waitForTimeout(90)
  await page.mouse.up()
}

await main()
