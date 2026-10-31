import fs from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import { parseArgs } from 'node:util'
import { fileURLToPath } from 'node:url'
import { execFileSync } from 'node:child_process'
import { _electron as electron } from 'playwright-core'
import { MB, startFakeBox } from '../test/fake-box.mjs'

const appRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')

const { values } = parseArgs({
  options: {
    executable: { type: 'string' },
    out: { type: 'string' },
    mp4: { type: 'string' },
    poster: { type: 'string' },
    address: { type: 'string', default: '192.168.1.50' }
  }
})

const DEMO_SHOWS = [
  {
    title: 'Harbour Lights',
    items: [
      { title: 'S1 E1 - The Lighthouse Keeper', size: 78 * MB, duration: '0:58:12' },
      { title: 'S1 E2 - Low Tide', size: 79 * MB, duration: '0:57:40' },
      { title: 'S1 E3 - Storm Warning', size: 77 * MB, duration: '0:59:03' },
      { title: 'S1 E4 - Safe Harbour', size: 80 * MB, duration: '1:01:27' }
    ]
  },
  {
    title: 'Backyard Builders',
    items: [
      { title: 'S3 E1 - The Shed', size: 92 * MB, duration: '0:44:10' },
      { title: 'S3 E2 - The Deck', size: 94 * MB, duration: '0:44:55' }
    ]
  },
  { title: 'The Night Market', items: [{ title: 'The Night Market', size: 249 * MB, duration: '1:52:30' }] },
  {
    title: 'Coastal Kitchen',
    items: [
      { title: 'S2 E5 - Oysters', size: 69 * MB, duration: '0:28:45' },
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
  const box = await startFakeBox({ shows: DEMO_SHOWS, name: 'Lounge room box' })
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
    await renderSounds({ page, out })
    const frames = await startScreencast({ page, framesDir })
    const soundLog = await logSounds(page)
    const recordingStart = Date.now()
    await page.evaluate(() => document.getAnimations().forEach(animation => { animation.currentTime = 0 }))
    await drive({ page })
    const sounds = await soundLog()
    const recordingEnd = Date.now()
    const frameList = await frames.stop()
    const timeline = { recordingStart, recordingEnd, frames: frameList, sounds }
    await fs.writeFile(path.join(out, 'timeline.json'), JSON.stringify(timeline, null, 2))
    await app.close()
    if (values.mp4) await makeVideo({ timeline, out, mp4: path.resolve(values.mp4), poster: values.poster && path.resolve(values.poster) })
    reportStillness(timeline)
  } finally {
    await app.close().catch(() => null)
    await box.stop()
    await fs.rm(work, { recursive: true, force: true })
  }
}

const SOUND_NAMES = ['start', 'found', 'finished', 'failed']

const renderSounds = async ({ page, out }) => {
  for (const name of SOUND_NAMES) {
    const samples = await page.evaluate((sound) => sounds.render(sound), name)
    await fs.writeFile(path.join(out, `${name}.wav`), toWav(samples))
  }
}

const toWav = (samples, rate = 44100) => {
  const data = Buffer.alloc(samples.length * 2)
  samples.forEach((sample, index) => data.writeInt16LE(Math.round(Math.max(-1, Math.min(1, sample)) * 32767), index * 2))
  const header = Buffer.alloc(44)
  header.write('RIFF', 0)
  header.writeUInt32LE(36 + data.length, 4)
  header.write('WAVE', 8)
  header.write('fmt ', 12)
  header.writeUInt32LE(16, 16)
  header.writeUInt16LE(1, 20)
  header.writeUInt16LE(1, 22)
  header.writeUInt32LE(rate, 24)
  header.writeUInt32LE(rate * 2, 28)
  header.writeUInt16LE(2, 32)
  header.writeUInt16LE(16, 34)
  header.write('data', 36)
  header.writeUInt32LE(data.length, 40)
  return Buffer.concat([header, data])
}

const makeVideo = async ({ timeline, out, mp4, poster }) => {
  const { frames, sounds, recordingEnd } = timeline
  const start = frames[0].time
  const length = (recordingEnd - start) / 1000
  const listPath = path.join(out, 'frames.txt')
  const lines = frames.flatMap((frame, index) => {
    const next = index + 1 < frames.length ? frames[index + 1].time : recordingEnd
    return [`file '${frame.file}'`, `duration ${((next - frame.time) / 1000).toFixed(4)}`]
  })
  await fs.writeFile(listPath, [...lines, `file '${frames.at(-1).file}'`].join('\n') + '\n')
  const soundInputs = sounds.flatMap(sound => ['-i', path.join(out, `${sound.name}.wav`)])
  const delayed = sounds.map((sound, index) => {
    const delay = Math.max(0, Math.round(sound.time - start))
    return `[${index + 1}:a]adelay=${delay}|${delay}[s${index}]`
  })
  const audio = sounds.length > 0
    ? `${delayed.join(';')};${sounds.map((_, index) => `[s${index}]`).join('')}amix=inputs=${sounds.length}:normalize=0,apad=whole_dur=${length}[aout]`
    : `anullsrc=r=44100:cl=mono,atrim=duration=${length}[aout]`
  execFileSync('ffmpeg', [
    '-loglevel', 'error', '-y',
    '-f', 'concat', '-safe', '0', '-i', listPath,
    ...soundInputs,
    '-filter_complex', `[0:v]fps=30,scale=1600:-2:flags=lanczos,format=yuv420p[vout];${audio}`,
    '-map', '[vout]', '-map', '[aout]',
    '-c:v', 'libx264', '-preset', 'slow', '-crf', '24', '-tune', 'stillimage',
    '-c:a', 'aac', '-b:a', '96k', '-ar', '44100',
    '-t', String(length), '-movflags', '+faststart', mp4
  ], { stdio: 'inherit' })
  if (poster) execFileSync('ffmpeg', ['-loglevel', 'error', '-y', '-ss', '2.5', '-i', mp4, '-frames:v', '1', poster], { stdio: 'inherit' })
}

const reportStillness = ({ frames, recordingEnd }) => {
  const gaps = frames.slice(1).map((frame, index) => ({ at: (frames[index].time - frames[0].time) / 1000, seconds: (frame.time - frames[index].time) / 1000 }))
  const longest = gaps.reduce((best, gap) => (gap.seconds > best.seconds ? gap : best), { at: 0, seconds: 0 })
  const finalHold = (recordingEnd - frames.at(-1).time) / 1000
  console.log(`Length ${((recordingEnd - frames[0].time) / 1000).toFixed(2)} s. Longest still stretch ${longest.seconds.toFixed(2)} s at ${longest.at.toFixed(2)} s. Final hold ${finalHold.toFixed(2)} s.`)
}

const drive = async ({ page }) => {
  await moveTo({ page, x: 540, y: 620 })
  await page.waitForTimeout(500)
  await clickOn({ page, locator: page.getByRole('button', { name: 'Start', exact: true }) })
  await moveTo({ page, x: 640, y: 420 })
  await page.getByRole('heading', { name: 'Choose recordings' }).waitFor({ timeout: 20000 })
  await page.waitForTimeout(400)
  await hover({ page, locator: page.locator('li.show-row[data-show="Backyard Builders"] .row-main').first() })
  await hover({ page, locator: page.locator('li.show-row[data-show="Coastal Kitchen"] .row-main').first() })
  const harbour = page.locator('li.show-row[data-show="Harbour Lights"] .row-main').first()
  await clickOn({ page, locator: harbour })
  await page.waitForTimeout(300)
  for (const title of ['S1 E1', 'S1 E2', 'S1 E3']) {
    await hover({ page, locator: page.locator('.episode-row', { hasText: title }).locator('.row-main') })
  }
  await clickOn({ page, locator: page.getByLabel('S1 E4 - Safe Harbour') })
  await page.waitForTimeout(600)
  await clickOn({ page, locator: page.getByRole('button', { name: 'Next' }) })
  await page.getByRole('heading', { name: 'Choose a folder' }).waitFor()
  await page.locator('#folder-info', { hasText: 'Needs' }).waitFor()
  await hover({ page, locator: page.locator('#folder-path') })
  await page.waitForTimeout(400)
  await clickOn({ page, locator: page.getByRole('switch', { name: 'Names for Plex, Jellyfin or Kodi' }) })
  await page.waitForTimeout(500)
  await clickOn({ page, locator: page.getByRole('button', { name: 'Save', exact: true }) })
  await moveTo({ page, x: 720, y: 420 })
  await page.getByRole('heading', { name: 'All done' }).waitFor({ timeout: 120000 })
  await hover({ page, locator: page.getByRole('button', { name: 'Open folder' }) })
  await page.waitForTimeout(1500)
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
  await page.mouse.move(x, y, { steps: 40 })
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
