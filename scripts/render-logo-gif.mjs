import fs from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import { execFile } from 'node:child_process'
import { parseArgs, promisify } from 'node:util'
import { fileURLToPath } from 'node:url'

const scriptDir = path.dirname(fileURLToPath(import.meta.url))
const { values } = parseArgs({
  options: {
    fmr: { type: 'string', default: path.join(scriptDir, '..', '..', 'fetch-my-recordings') },
    layout: { type: 'string', default: 'tall' },
    out: { type: 'string', default: path.join(scriptDir, '..', 'fetchtv-icon-animated.gif') },
    still: { type: 'string' },
    hold: { type: 'string', default: '2' },
    empty: { type: 'string', default: '0.35' },
    fps: { type: 'string', default: '33.333333' }
  }
})

const { chromium } = await import('playwright-core')

const CYCLE_MS = 6000
const SETTLED_MS = 1340
const LEAVE_START_MS = 4200
const LEAVE_END_MS = 4680
const GRID = 32
const ROUND_CHIP_CORNERS = false
const OUTPUT_SIZE = 256
const CELLS_PER_MARK_UNIT = (800 / 64) / (1024 / GRID)
const TILE_RADIUS_CELLS = 200 / (1024 / GRID)
const COLOURS = {
  tile: '#141c24', blue: '#3ba4ff', orange: '#f28c28', yellow: '#f5c542', tray: '#5ce1c6'
}

const LAYOUTS = {
  tight: { chipHeight: 3, gap: 1, firstRow: 9, wallTopOffset: 5 },
  loose: { chipHeight: 3, gap: 2, firstRow: 8, wallTopOffset: 6 },
  tall: { chipHeight: 4, gap: 2, firstRow: 6, wallTopOffset: 7 }
}

const main = async () => {
  const layout = spriteLayout(LAYOUTS[values.layout])
  const fps = Number(values.fps)
  const frameTimes = appTimeline({
    fps, holdMs: Number(values.hold) * 1000, emptyMs: Number(values.empty) * 1000
  })
  const shots = await renderFrames({ frameTimes, layout })
  if (values.still) await fs.writeFile(path.resolve(values.still), shots[0])
  const frames = mergeRepeatedFrames({ shots, frameMs: 1000 / fps })
  const work = await fs.mkdtemp(path.join(os.tmpdir(), 'fetchtv-pixel-icon-'))
  await encodeGif({ frames, dir: work, out: path.resolve(values.out) })
  await fs.rm(work, { recursive: true })
  console.log(`${values.layout}: ${frameTimes.length} steps as ${frames.length} GIF frames: ${values.out}`)
}

const spriteLayout = ({ chipHeight, gap, firstRow, wallTopOffset }) => {
  const wallThickness = 2
  const trayLeft = 6
  const trayRight = GRID - 1 - trayLeft
  const chipLeft = trayLeft + wallThickness + gap
  const chipWidth = trayRight - wallThickness - gap - chipLeft + 1
  const chipTop = (index) => firstRow + index * (chipHeight + gap)
  const floorTop = chipTop(3)
  return {
    chipHeight,
    chipWidth,
    chipLeft,
    chips: ['blue', 'orange', 'yellow'].map((name, index) => ({ name, restTop: chipTop(index) })),
    tray: {
      left: trayLeft, right: trayRight, wallTop: firstRow + wallTopOffset,
      floorTop, floorBottom: floorTop + wallThickness - 1, wallThickness
    }
  }
}

const appTimeline = ({ fps, holdMs, emptyMs }) => {
  const frameMs = 1000 / fps
  const steps = ({ fromMs, durationMs }) => Array.from(
    { length: Math.round(durationMs / frameMs) },
    (_, index) => fromMs + index * frameMs
  )
  const hold = steps({ fromMs: SETTLED_MS, durationMs: holdMs }).map(() => SETTLED_MS)
  const leave = steps({ fromMs: LEAVE_START_MS, durationMs: LEAVE_END_MS - LEAVE_START_MS + emptyMs })
  const enter = steps({ fromMs: 0, durationMs: SETTLED_MS })
  return [...hold, ...leave, ...enter]
}

const renderFrames = async ({ frameTimes, layout }) => {
  const browser = await chromium.launch({ headless: true })
  try {
    const page = await browser.newPage()
    await page.setContent(await motionPage())
    await page.evaluate(() => document.getAnimations().forEach(animation => animation.pause()))
    const shots = []
    for (const time of frameTimes) {
      const motion = await page.evaluate(sampleMotion, time + CYCLE_MS)
      const dataUrl = await page.evaluate(drawSprite, {
        sprite: spriteCells({ layout, motion }), grid: GRID, outputSize: OUTPUT_SIZE
      })
      shots.push(Buffer.from(dataUrl.split(',')[1], 'base64'))
    }
    return shots
  } finally {
    await browser.close()
  }
}

const sampleMotion = (steadyStateTime) => {
  document.getAnimations().forEach(animation => { animation.currentTime = steadyStateTime })
  const transformOf = (element) => {
    const matrix = new DOMMatrixReadOnly(getComputedStyle(element).transform === 'none'
      ? undefined
      : getComputedStyle(element).transform)
    return { scaleX: matrix.a, scaleY: matrix.d, translateY: matrix.f }
  }
  const group = document.querySelector('.mark-chips')
  const groupOpacity = Number(getComputedStyle(group).opacity)
  return {
    group: { ...transformOf(group), opacity: groupOpacity },
    chips: Object.fromEntries([...document.querySelectorAll('.mark-chip')].map(chip => [
      chip.dataset.name,
      { ...transformOf(chip), opacity: Number(getComputedStyle(chip).opacity) * groupOpacity }
    ]))
  }
}

const spriteCells = ({ layout, motion }) => {
  const cells = new Map()
  const paint = ({ left, top, width, height, colour, coverage = 1 }) => {
    for (let row = top; row < top + height; row++) {
      for (let column = left; column < left + width; column++) {
        if (insideTile(column, row) && bayerOn({ column, row, coverage })) cells.set(`${column},${row}`, colour)
      }
    }
  }
  const erase = (column, row) => cells.delete(`${column},${row}`)
  const unpaint = (column, row) => paint({ left: column, top: row, width: 1, height: 1, colour: COLOURS.tile })
  paintTile({ paint, erase })
  layout.chips.forEach(({ name, restTop }) => {
    const chipMotion = motion.chips[name]
    if (chipMotion.opacity <= 0) return
    paintChip({ paint, unpaint, layout, restTop, chipMotion, groupMotion: motion.group, colour: COLOURS[name] })
  })
  paintTray({ paint, unpaint, tray: layout.tray })
  return [...cells.entries()].map(([key, colour]) => [...key.split(',').map(Number), colour])
}

const BAYER_4X4 = [
  [0, 8, 2, 10],
  [12, 4, 14, 6],
  [3, 11, 1, 9],
  [15, 7, 13, 5]
]

const bayerOn = ({ column, row, coverage }) =>
  coverage > (BAYER_4X4[((row % 4) + 4) % 4][((column % 4) + 4) % 4] + 0.5) / 16

const paintTile = ({ paint, erase }) => {
  paint({ left: 0, top: 0, width: GRID, height: GRID, colour: COLOURS.tile })
  for (let row = 0; row < GRID; row++) {
    for (let column = 0; column < GRID; column++) {
      if (outsideRoundedTile(column, row)) erase(column, row)
    }
  }
}

const LEAVE_RISE_CELLS = 5

const leaveRiseCells = (groupOpacity) => {
  const progress = 1 - groupOpacity
  return Math.round(LEAVE_RISE_CELLS * (1 - (1 - progress) ** 2))
}

const insideTile = (column, row) =>
  column >= 0 && row >= 0 && column < GRID && row < GRID && !outsideRoundedTile(column, row)

const outsideRoundedTile = (column, row) => {
  const nearest = (index) => Math.min(index + 0.5, GRID - index - 0.5)
  const dx = TILE_RADIUS_CELLS - nearest(column)
  const dy = TILE_RADIUS_CELLS - nearest(row)
  return dx > 0 && dy > 0 && Math.hypot(dx, dy) > TILE_RADIUS_CELLS
}

const paintChip = ({ paint, unpaint, layout, restTop, chipMotion, groupMotion, colour }) => {
  const width = steppedSize({ size: layout.chipWidth, scale: chipMotion.scaleX, step: 2, threshold: 0.05 })
  const height = steppedSize({ size: layout.chipHeight, scale: chipMotion.scaleY, step: 1, threshold: 0.1 })
  const shift = Math.round(chipMotion.translateY * CELLS_PER_MARK_UNIT) - leaveRiseCells(groupMotion.opacity)
  const bottom = restTop + layout.chipHeight + shift
  const left = layout.chipLeft - (width - layout.chipWidth) / 2
  const top = bottom - height
  paint({ left, top, width, height, colour, coverage: chipMotion.opacity })
  if (!ROUND_CHIP_CORNERS || height < 3) return
  ;[[left, top], [left + width - 1, top], [left, bottom - 1], [left + width - 1, bottom - 1]]
    .forEach(([column, row]) => unpaint(column, row))
}

const steppedSize = ({ size, scale, step, threshold }) => {
  if (Math.abs(scale - 1) <= threshold) return size
  return size + step * Math.sign(scale - 1)
}

const paintTray = ({ paint, unpaint, tray }) => {
  const { left, right, wallTop, floorTop, floorBottom, wallThickness } = tray
  const wallHeight = floorBottom - wallTop + 1
  paint({ left, top: wallTop, width: wallThickness, height: wallHeight, colour: COLOURS.tray })
  paint({ left: right - wallThickness + 1, top: wallTop, width: wallThickness, height: wallHeight, colour: COLOURS.tray })
  paint({ left, top: floorTop, width: right - left + 1, height: wallThickness, colour: COLOURS.tray })
  unpaint(left, floorBottom)
  unpaint(right, floorBottom)
}

const drawSprite = ({ sprite, grid, outputSize }) => {
  const canvas = document.querySelector('canvas') ?? document.body.appendChild(document.createElement('canvas'))
  canvas.width = outputSize
  canvas.height = outputSize
  const context = canvas.getContext('2d')
  const cell = outputSize / grid
  context.clearRect(0, 0, outputSize, outputSize)
  sprite.forEach(([column, row, colour]) => {
    context.fillStyle = colour
    context.fillRect(column * cell, row * cell, cell, cell)
  })
  return canvas.toDataURL('image/png')
}

const motionPage = async () => {
  const styles = await fs.readFile(path.join(values.fmr, 'src', 'renderer', 'styles.css'), 'utf8')
  const mark = await fs.readFile(path.join(values.fmr, 'src', 'renderer', 'mark.svg'), 'utf8')
  return `<!doctype html><html><head><style>${markAnimationCss(styles)}</style></head><body>
  <svg width="64" height="64" viewBox="0 0 64 64" class="mark is-animated">${animatedMarkInner(mark)}</svg>
  </body></html>`
}

const markAnimationCss = (styles) => {
  const start = styles.indexOf('.mark.is-animated {')
  const end = styles.indexOf('@media (prefers-reduced-motion: reduce)', start)
  if (start < 0 || end < 0) throw new Error('Mark animation CSS not found in styles.css')
  return styles.slice(start, end)
}

const animatedMarkInner = (mark) => {
  const chipNames = { '#3ba4ff': 'blue', '#f28c28': 'orange', '#f5c542': 'yellow' }
  const chips = [...mark.matchAll(/<rect [^>]*fill="(#[0-9a-f]{6})"\/>/g)]
    .map(([rect, fill]) => rect.replace('<rect ', `<rect class="mark-chip is-${chipNames[fill]}" data-name="${chipNames[fill]}" `))
  const tray = mark.match(/<path [^>]*\/>/)[0]
  if (chips.length !== 3 || !tray) throw new Error('Unexpected mark.svg shape')
  return `<g class="mark-chips">${chips.join('')}</g>${tray}`
}

const mergeRepeatedFrames = ({ shots, frameMs }) => shots.reduce((frames, shot) => {
  const previous = frames.at(-1)
  if (previous?.shot.equals(shot)) {
    previous.durationMs += frameMs
    return frames
  }
  return [...frames, { shot, durationMs: frameMs }]
}, [])

const encodeGif = async ({ frames, dir, out }) => {
  await fs.mkdir(path.dirname(out), { recursive: true })
  for (const [index, { shot }] of frames.entries()) await fs.writeFile(framePath(dir, index), shot)
  const input = ['-framerate', '50', '-i', path.join(dir, 'frame-%04d.png')]
  const palette = path.join(dir, 'palette.png')
  await run('ffmpeg', ['-y', '-v', 'error', ...input,
    '-vf', 'palettegen=reserve_transparent=1:stats_mode=full', palette])
  const encoded = path.join(dir, 'encoded.gif')
  await run('ffmpeg', ['-y', '-v', 'error', ...input, '-i', palette,
    '-lavfi', 'paletteuse=dither=none:alpha_threshold=128', '-loop', '0', encoded])
  const gif = await fs.readFile(encoded)
  const delays = frames.map(({ durationMs }) => Math.round(durationMs / 10))
  await fs.writeFile(out, withFrameDelays({ gif, delays }))
}

const withFrameDelays = ({ gif, delays }) => {
  const patched = Buffer.from(gif)
  const offsets = frameDelayOffsets(patched)
  if (offsets.length !== delays.length) throw new Error(`GIF has ${offsets.length} frames, expected ${delays.length}`)
  offsets.forEach((offset, index) => patched.writeUInt16LE(delays[index], offset))
  return patched
}

const frameDelayOffsets = (gif) => {
  const offsets = []
  let position = 13 + colourTableSize(gif[10])
  while (gif[position] !== 0x3b) {
    if (gif[position] === 0x21) {
      if (gif[position + 1] === 0xf9) offsets.push(position + 4)
      position = afterSubBlocks(gif, position + 2)
    } else if (gif[position] === 0x2c) {
      position = afterSubBlocks(gif, position + 10 + colourTableSize(gif[position + 9]) + 1)
    } else {
      throw new Error(`Unexpected GIF block 0x${gif[position].toString(16)} at ${position}`)
    }
  }
  return offsets
}

const colourTableSize = (packedFields) => (packedFields & 0x80 ? 3 * 2 ** ((packedFields & 0x07) + 1) : 0)

const afterSubBlocks = (gif, start) => {
  let position = start
  while (gif[position] !== 0) position += gif[position] + 1
  return position + 1
}

const framePath = (dir, index) => path.join(dir, `frame-${String(index).padStart(4, '0')}.png`)

const run = promisify(execFile)

await main()
