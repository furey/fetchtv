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
      { title: 'S1 E1 - The Lighthouse Keeper', size: 98 * MB, duration: '0:58:12' },
      { title: 'S1 E2 - Low Tide', size: 99 * MB, duration: '0:57:40' },
      { title: 'S1 E3 - Storm Warning', size: 96 * MB, duration: '0:59:03' },
      { title: 'S1 E4 - Safe Harbour', size: 100 * MB, duration: '1:01:27' }
    ]
  },
  {
    title: 'Backyard Builders',
    items: [
      { title: 'S3 E1 - The Shed', size: 115 * MB, duration: '0:44:10' },
      { title: 'S3 E2 - The Deck', size: 118 * MB, duration: '0:44:55' }
    ]
  },
  { title: 'The Night Market', items: [{ title: 'The Night Market', size: 311 * MB, duration: '1:52:30' }] },
  {
    title: 'Coastal Kitchen',
    items: [
      { title: 'S2 E5 - Oysters', size: 86 * MB, duration: '0:28:45' },
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
    await installTracker(page)
    await page.mouse.move(pointer.x, pointer.y)
    pointerLog.push({ time: Date.now(), x: pointer.x, y: pointer.y })
    await page.evaluate(() => document.fonts.ready)
    await renderSounds({ page, out })
    await renderCursors({ page, out })
    const frames = await startScreencast({ page, framesDir })
    const soundLog = await logSounds(page)
    const recordingStart = Date.now()
    await page.evaluate(() => document.getAnimations().forEach(animation => { animation.currentTime = 0 }))
    await drive({ page })
    const sounds = await soundLog()
    const recordingEnd = Date.now()
    const cursor = await page.evaluate(() => window.demoCursor)
    const frameList = await frames.stop()
    const viewportWidth = await page.evaluate(() => window.innerWidth)
    const cursorHotspots = JSON.parse(await fs.readFile(path.join(out, 'cursor-hotspots.json'), 'utf8'))
    const timeline = { recordingStart, recordingEnd, frames: frameList, sounds, cursor, pointerLog, viewportWidth, cursorHotspots }
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

const OUTPUT_WIDTH = 1600
const OUTPUT_FPS = 60
const OFFSCREEN = -400

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
  const scale = OUTPUT_WIDTH / timeline.viewportWidth
  const commandsPath = path.join(out, 'cursor-commands.txt')
  await fs.writeFile(commandsPath, cursorCommands({ timeline, start, length, scale }))
  const soundInputs = sounds.flatMap(sound => ['-i', path.join(out, `${sound.name}.wav`)])
  const firstSound = 3
  const delayed = sounds.map((sound, index) => {
    const delay = Math.max(0, Math.round(sound.time - start))
    return `[${firstSound + index}:a]adelay=${delay}|${delay}[s${index}]`
  })
  const audio = sounds.length > 0
    ? `${delayed.join(';')};${sounds.map((_, index) => `[s${index}]`).join('')}amix=inputs=${sounds.length}:normalize=0,apad=whole_dur=${length}[aout]`
    : `anullsrc=r=44100:cl=mono,atrim=duration=${length}[aout]`
  const video = [
    `[0:v]fps=${OUTPUT_FPS},scale=${OUTPUT_WIDTH}:-2:flags=lanczos,sendcmd=f=${commandsPath}[bg]`,
    `[bg][1:v]overlay@arrow=x=${OFFSCREEN}:y=${OFFSCREEN}:eval=frame[witharrow]`,
    `[witharrow][2:v]overlay@hand=x=${OFFSCREEN}:y=${OFFSCREEN}:eval=frame,format=yuv420p[vout]`
  ].join(';')
  execFileSync('ffmpeg', [
    '-loglevel', 'error', '-y',
    '-f', 'concat', '-safe', '0', '-i', listPath,
    '-loop', '1', '-framerate', String(OUTPUT_FPS), '-i', path.join(out, 'cursor-arrow.png'),
    '-loop', '1', '-framerate', String(OUTPUT_FPS), '-i', path.join(out, 'cursor-hand.png'),
    ...soundInputs,
    '-filter_complex', `${video};${audio}`,
    '-map', '[vout]', '-map', '[aout]',
    '-r', String(OUTPUT_FPS),
    '-c:v', 'libx264', '-preset', 'slow', '-crf', '23', '-tune', 'stillimage',
    '-c:a', 'aac', '-b:a', '96k', '-ar', '44100',
    '-t', String(length), '-movflags', '+faststart', mp4
  ], { stdio: 'inherit' })
  if (poster) execFileSync('ffmpeg', ['-loglevel', 'error', '-y', '-ss', '2.5', '-i', mp4, '-frames:v', '1', poster], { stdio: 'inherit' })
}

const MIN_SHAPE_FRAMES = 6

const cursorCommands = ({ timeline, start, length, scale }) => {
  const hotspots = timeline.cursorHotspots
  const frameCount = Math.ceil(length * OUTPUT_FPS) + 1
  const placed = Array.from({ length: frameCount }, (_, frame) => {
    const time = start + (frame * 1000) / OUTPUT_FPS
    const position = positionAt({ log: timeline.pointerLog, time })
    if (!position) return null
    return { ...position, shape: shapeOf(shapeAt({ samples: timeline.cursor.samples, time })?.cursor) }
  })
  const lines = []
  let previous = ''
  steadyFrames(placed).forEach((sample, frame) => {
    const place = (name) => {
      if (!sample || sample.shape !== name) return [OFFSCREEN, OFFSCREEN]
      return [Math.round(sample.x * scale - hotspots[name].x), Math.round(sample.y * scale - hotspots[name].y)]
    }
    const [ax, ay] = place('arrow')
    const [hx, hy] = place('hand')
    const command = `overlay@arrow x ${ax}, overlay@arrow y ${ay}, overlay@hand x ${hx}, overlay@hand y ${hy}`
    if (command === previous) return
    previous = command
    lines.push(`${(frame / OUTPUT_FPS).toFixed(4)} ${command};`)
  })
  return lines.join('\n') + '\n'
}

const steadyFrames = (placed) => {
  const steady = placed.map(sample => sample && { ...sample })
  let runStart = 0
  for (let frame = 1; frame <= steady.length; frame++) {
    if (frame < steady.length && steady[frame]?.shape === steady[runStart]?.shape) continue
    const isShort = frame - runStart < MIN_SHAPE_FRAMES && runStart > 0 && steady[runStart - 1]
    if (isShort) for (let index = runStart; index < frame; index++) if (steady[index]) steady[index].shape = steady[runStart - 1].shape
    runStart = frame
  }
  return steady
}

const positionAt = ({ log, time }) => {
  if (log.length === 0 || time < log[0].time) return null
  const index = log.findIndex(entry => entry.time > time)
  if (index === -1) return log.at(-1)
  const before = log[index - 1]
  const after = log[index]
  const share = (time - before.time) / Math.max(1, after.time - before.time)
  return { x: before.x + (after.x - before.x) * share, y: before.y + (after.y - before.y) * share }
}

const shapeAt = ({ samples, time }) => {
  const index = samples.findIndex(sample => sample.time > time)
  if (index === 0) return samples[0]
  return index === -1 ? samples.at(-1) : samples[index - 1]
}

const shapeOf = (cursor) => (cursor === 'pointer' ? 'hand' : 'arrow')

const reportStillness = ({ frames, recordingEnd, cursor }) => {
  const moves = cursor.samples.filter((sample, index, all) => index === 0 || sample.x !== all[index - 1].x || sample.y !== all[index - 1].y || sample.cursor !== all[index - 1].cursor)
  const changes = [...frames.map(frame => frame.time), ...moves.map(sample => sample.time)].filter(time => time >= frames[0].time).sort((a, b) => a - b)
  const gaps = changes.slice(1).map((time, index) => ({ at: (changes[index] - frames[0].time) / 1000, seconds: (time - changes[index]) / 1000 }))
  const longest = gaps.reduce((best, gap) => (gap.seconds > best.seconds ? gap : best), { at: 0, seconds: 0 })
  const finalHold = (recordingEnd - changes.at(-1)) / 1000
  console.log(`Length ${((recordingEnd - frames[0].time) / 1000).toFixed(2)} s. Longest still stretch ${longest.seconds.toFixed(2)} s at ${longest.at.toFixed(2)} s. Final hold ${finalHold.toFixed(2)} s.`)
}

const drive = async ({ page }) => {
  await auditCursors(page, 'welcome')
  await page.waitForTimeout(1100)
  await clickOn({ page, locator: page.getByRole('button', { name: 'Start', exact: true }) })
  await humanMove({ page, to: { x: 640, y: 420 } })
  await page.getByRole('heading', { name: 'Choose recordings' }).waitFor({ timeout: 20000 })
  await auditCursors(page, 'recordings')
  await hold(page, 1050)
  await hoverOver({ page, locator: page.locator('li.show-row[data-show="Backyard Builders"] .row-main').first() })
  await hold(page, 250)
  await hoverOver({ page, locator: page.locator('li.show-row[data-show="Coastal Kitchen"] .row-main').first() })
  await clickOn({ page, locator: page.locator('li.show-row[data-show="Harbour Lights"] .row-main').first() })
  await hold(page, 900)
  for (const title of ['S1 E1', 'S1 E2', 'S1 E3']) {
    await hoverOver({ page, locator: page.locator('.episode-row', { hasText: title }).locator('.row-main') })
    await hold(page, 120)
  }
  await clickOn({ page, locator: page.getByLabel('S1 E4 - Safe Harbour') })
  await hold(page, 900)
  await clickOn({ page, locator: page.getByRole('button', { name: 'Next' }) })
  await page.getByRole('heading', { name: 'Choose a folder' }).waitFor()
  await page.locator('#folder-info', { hasText: 'Needs' }).waitFor()
  await auditCursors(page, 'folder')
  await hold(page, 1050)
  await hoverOver({ page, locator: page.locator('#folder-path') })
  await hold(page, 450)
  await hoverOver({ page, locator: page.getByRole('button', { name: 'About names for Plex, Jellyfin or Kodi' }) })
  await hold(page, 500)
  await clickOn({ page, locator: page.getByRole('switch', { name: 'Names for Plex, Jellyfin or Kodi' }) })
  await hold(page, 950)
  await clickOn({ page, locator: page.getByRole('button', { name: 'Save', exact: true }) })
  await hold(page, 300)
  await humanMove({ page, to: { x: 700, y: 430 } })
  const done = page.getByRole('heading', { name: 'All done' })
  const drifts = [{ x: 610, y: 470 }, { x: 660, y: 455 }, { x: 600, y: 485 }, { x: 650, y: 465 }]
  for (let index = 0; !(await done.isVisible()); index++) {
    await hold(page, 1000)
    if (await done.isVisible()) break
    await humanMove({ page, to: drifts[index % drifts.length] })
  }
  await auditCursors(page, 'done')
  await hold(page, 800)
  await hoverOver({ page, locator: page.getByRole('button', { name: 'Open folder' }) })
  await hold(page, 1800)
}

const auditCursors = async (page, label) => {
  const missing = await page.evaluate(() => [...document.querySelectorAll('button, a, input, label, [role="button"], [role="switch"], .row-main.is-clickable')]
    .filter(element => element.offsetParent !== null && !element.disabled)
    .filter(element => getComputedStyle(element).cursor !== 'pointer')
    .map(element => `${element.tagName.toLowerCase()}.${element.className} ${element.textContent.trim().slice(0, 30)}`))
  if (missing.length > 0) console.log(`No pointer cursor on ${label}: ${missing.join(' | ')}`)
}

const random = (() => {
  let seed = 20261101
  return () => {
    seed = (seed + 0x6d2b79f5) | 0
    let value = Math.imul(seed ^ (seed >>> 15), 1 | seed)
    value = (value + Math.imul(value ^ (value >>> 7), 61 | value)) ^ value
    return ((value ^ (value >>> 14)) >>> 0) / 4294967296
  }
})()

const between = (low, high) => low + (high - low) * random()

const hold = (page, ms) => page.waitForTimeout(Math.round(ms * between(0.92, 1.08)))

const pointer = { x: 760, y: 560 }
const pointerLog = []

const minimumJerk = (t) => t * t * t * (10 - 15 * t + 6 * t * t)

const easeOutCubic = (t) => 1 - (1 - t) ** 3

const humanMove = async ({ page, to }) => {
  const from = { ...pointer }
  const dx = to.x - from.x
  const dy = to.y - from.y
  const distance = Math.hypot(dx, dy)
  if (distance < 2) return
  const duration = Math.min(700, Math.max(300, 230 + 160 * Math.log2(1 + distance / 60))) * between(0.9, 1.1)
  const direction = { x: dx / distance, y: dy / distance }
  const overshoot = Math.min(7, distance * 0.035) * between(0.5, 1)
  const end = { x: to.x + direction.x * overshoot, y: to.y + direction.y * overshoot }
  const bend = distance * between(0.05, 0.12) * (random() < 0.5 ? -1 : 1)
  const control = { x: (from.x + end.x) / 2 - direction.y * bend, y: (from.y + end.y) / 2 + direction.x * bend }
  await animatePointer({ page, duration, at: (t) => {
    const s = minimumJerk(t)
    const inverse = 1 - s
    return {
      x: inverse * inverse * from.x + 2 * inverse * s * control.x + s * s * end.x,
      y: inverse * inverse * from.y + 2 * inverse * s * control.y + s * s * end.y
    }
  } })
  if (overshoot < 0.5) return
  await animatePointer({ page, duration: between(100, 150), at: (t) => {
    const s = easeOutCubic(t)
    return { x: end.x + (to.x - end.x) * s, y: end.y + (to.y - end.y) * s }
  } })
}

const animatePointer = async ({ page, duration, at }) => {
  const started = Date.now()
  while (true) {
    const elapsed = Date.now() - started
    const t = Math.min(1, elapsed / duration)
    const point = at(t)
    pointer.x = point.x
    pointer.y = point.y
    pointerLog.push({ time: started + elapsed, x: point.x, y: point.y })
    await page.mouse.move(point.x, point.y)
    if (t >= 1) return
    const wait = 16 - ((Date.now() - started) - elapsed)
    if (wait > 0) await new Promise(resolve => setTimeout(resolve, wait))
  }
}

const targetPoint = async (locator) => {
  await locator.scrollIntoViewIfNeeded()
  const box = await locator.boundingBox()
  const x = box.x + Math.min(box.width / 2, 160) + between(-4, 4)
  const y = box.y + box.height / 2 + between(-2, 2)
  return { x, y }
}

const hoverOver = async ({ page, locator }) => {
  await humanMove({ page, to: await targetPoint(locator) })
}

const clickOn = async ({ page, locator }) => {
  await humanMove({ page, to: await targetPoint(locator) })
  await page.waitForTimeout(Math.round(between(80, 150)))
  await page.mouse.down()
  await page.waitForTimeout(Math.round(between(80, 120)))
  await page.mouse.up()
}

const installTracker = (page) => page.evaluate(() => {
  window.demoCursor = { samples: [] }
  let position = null
  const record = () => {
    if (!position) return
    const element = document.elementFromPoint(position.x, position.y)
    const cursor = element ? getComputedStyle(element).cursor : 'auto'
    window.demoCursor.samples.push({ time: performance.timeOrigin + performance.now(), x: position.x, y: position.y, cursor })
  }
  document.addEventListener('mousemove', (event) => {
    position = { x: event.clientX, y: event.clientY }
    record()
  }, true)
  setInterval(record, 40)
  document.addEventListener('mousedown', () => {
    if (!position) return
    const ring = document.createElement('div')
    Object.assign(ring.style, { position: 'fixed', left: `${position.x - 12}px`, top: `${position.y - 12}px`, width: '24px', height: '24px', borderRadius: '50%', border: '1.5px solid rgba(92, 225, 198, 0.55)', zIndex: '2147483646', pointerEvents: 'none' })
    document.documentElement.append(ring)
    ring.animate([{ transform: 'scale(0.5)', opacity: 0.9 }, { transform: 'scale(1)', opacity: 0 }], { duration: 380, easing: 'cubic-bezier(0.2, 0.8, 0.2, 1)' }).finished.then(() => ring.remove())
  }, true)
})

const CURSOR_SCALE = 1.6
const CURSOR_PADDING = 8

const renderCursors = async ({ page, out }) => {
  const drawn = await page.evaluate(({ scale, padding }) => {
    const shapes = {
      arrow: {
        width: 15, height: 22, hotspot: { x: 1.5, y: 1.5 },
        path: 'M1.5 1.5 L1.5 17.6 L5.4 13.9 L8.2 20.4 L11 19.2 L8.3 12.9 L13.6 12.9 Z',
        fill: '#000000', stroke: '#ffffff', strokeWidth: 1.5
      },
      hand: {
        width: 18, height: 21, hotspot: { x: 6.6, y: 1.4 },
        path: 'M5.8 2.4 C5.8 0.9 7.6 0.9 7.6 2.4 L7.6 9.2 L8 9.2 L8 7.6 C8 6.4 9.8 6.4 9.8 7.6 L9.8 9.4 L10.2 9.4 L10.2 8.3 C10.2 7.1 12 7.1 12 8.3 L12 9.8 L12.4 9.8 L12.4 9.1 C12.4 7.9 14.2 7.9 14.2 9.1 L14.2 14 C14.2 17.3 12.4 19.4 9.8 19.4 L8.4 19.4 C6.9 19.4 5.9 18.7 5 17.4 L2.2 13.1 C1.5 12 3 10.8 4 11.7 L5.8 13.4 Z',
        fill: '#ffffff', stroke: '#000000', strokeWidth: 1.05
      }
    }
    return Object.fromEntries(Object.entries(shapes).map(([name, shape]) => {
      const canvas = document.createElement('canvas')
      canvas.width = Math.ceil(shape.width * scale + padding * 2)
      canvas.height = Math.ceil(shape.height * scale + padding * 2)
      const context = canvas.getContext('2d')
      context.translate(padding, padding)
      context.scale(scale, scale)
      const outline = new Path2D(shape.path)
      context.lineJoin = 'round'
      context.lineCap = 'round'
      context.shadowColor = 'rgba(0, 0, 0, 0.45)'
      context.shadowBlur = 3 * scale
      context.shadowOffsetY = 0.8 * scale
      context.strokeStyle = shape.stroke
      context.lineWidth = shape.strokeWidth * 2
      if (name === 'arrow') context.stroke(outline)
      else { context.fillStyle = shape.fill; context.fill(outline) }
      context.shadowColor = 'transparent'
      context.fillStyle = shape.fill
      context.fill(outline)
      if (name === 'hand') { context.lineWidth = shape.strokeWidth; context.stroke(outline) }
      return [name, { data: canvas.toDataURL('image/png'), hotspot: { x: padding + shape.hotspot.x * scale, y: padding + shape.hotspot.y * scale } }]
    }))
  }, { scale: CURSOR_SCALE, padding: CURSOR_PADDING })
  const hotspots = {}
  for (const [name, { data, hotspot }] of Object.entries(drawn)) {
    await fs.writeFile(path.join(out, `cursor-${name}.png`), Buffer.from(data.split(',')[1], 'base64'))
    hotspots[name] = hotspot
  }
  await fs.writeFile(path.join(out, 'cursor-hotspots.json'), JSON.stringify(hotspots))
}

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

await main()
