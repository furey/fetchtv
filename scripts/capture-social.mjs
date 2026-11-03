import fs from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import { spawn } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { chromium } from 'playwright-core'
import xterm from '@xterm/headless'

const SCRIPT_DIR = path.dirname(fileURLToPath(import.meta.url))
const FETCHTV_ROOT = path.resolve(process.env.FETCHTV_ROOT ?? path.join(SCRIPT_DIR, '..'))
const FMR_ROOT = path.resolve(process.env.FMR_ROOT ?? path.join(FETCHTV_ROOT, '../fetch-my-recordings'))
const OUT = path.resolve(process.env.SOCIAL_OUT ?? path.join(FETCHTV_ROOT, 'docs/img/social-preview.png'))

const REPO_URL = 'github.com/furey/fetchtv'
const TAGLINES = {
  a: ['Copy your Fetch&nbsp;TV<br>recordings via the&nbsp;terminal.<br>Watch them on your&nbsp;TV.'],
  b: ['Save Fetch&nbsp;TV recordings with one&nbsp;command.']
}
const TAGLINE = TAGLINES[process.env.SOCIAL_TAGLINE ?? 'a']

const card = { width: 1280, height: 640 }
const TERMINAL = { cols: 120, rows: 40 }
const SHOWN_ADDRESS = '192.168.1.50'
const SHOW = 'Master Cook'
const SAVE_DIR = 'recordings'
const LIST_SECONDS = 3
const SAVE_SECONDS = 11

const main = async () => {
  const [assets, screen] = await Promise.all([loadAssets(), captureSession()])
  const browser = await chromium.launch()
  const context = await browser.newContext({ viewport: card, deviceScaleFactor: 1 })
  const page = await context.newPage()
  await page.setContent(cardHtml({ assets, terminalHtml: terminalHtml(screen) }))
  await page.evaluate(() => document.fonts.ready)
  await fs.mkdir(path.dirname(OUT), { recursive: true })
  await page.screenshot({ path: OUT, type: 'png' })
  await browser.close()
  console.log(`[social] wrote ${OUT}`)
}

const captureSession = async () => {
  const { MB, demoShows, startFakeBox } = await import(path.join(FMR_ROOT, 'test/fake-box.mjs'))
  const box = await startFakeBox({ shows: demoShows({ size: 140 * MB }), name: 'Lounge room box', port: 49152, bytesPerSecond: 9 * MB })
  const work = await fs.mkdtemp(path.join(os.tmpdir(), 'fetchtv-social-'))
  await fs.mkdir(path.join(work, SAVE_DIR))
  const listArgs = ['recordings', `--ip=${SHOWN_ADDRESS}`, '-s', SHOW]
  const saveArgs = [...listArgs, `--save=${SAVE_DIR}`]
  const realArgs = (args) => args.map((arg) => arg.replace(SHOWN_ADDRESS, '127.0.0.1'))
  try {
    const listing = await runInPty({ args: realArgs(listArgs), cwd: work, seconds: LIST_SECONDS })
    const saving = await runInPty({ args: realArgs(saveArgs), cwd: work, seconds: SAVE_SECONDS })
    const transcript = [
      promptLine(listArgs), showAddress(listing),
      '\r\n', promptLine(saveArgs), showAddress(saving)
    ].join('')
    return readScreen(transcript)
  } finally {
    await box.stop()
    await fs.rm(work, { recursive: true, force: true })
  }
}

const runInPty = ({ args, cwd, seconds }) => new Promise((resolve, reject) => {
  const child = spawn('python3', ['-I', '-c', PTY_RUNNER, String(TERMINAL.rows), String(TERMINAL.cols), String(seconds),
    process.execPath, path.join(FETCHTV_ROOT, 'fetchtv.js'), ...args], { cwd, stdio: ['ignore', 'pipe', 'inherit'] })
  const chunks = []
  child.stdout.on('data', (chunk) => chunks.push(chunk))
  child.on('error', reject)
  child.on('close', () => resolve(Buffer.concat(chunks).toString('utf8')))
})

const promptLine = (args) => {
  const shown = args.map((arg) => (arg.includes(' ') ? `"${arg}"` : arg)).join(' ')
  return `\x1b[90m$\x1b[39m npx fetchtv ${shown}\r\n`
}

const showAddress = (text) => text.replaceAll('127.0.0.1', SHOWN_ADDRESS)

const readScreen = async (transcript) => {
  const term = new xterm.Terminal({ ...TERMINAL, allowProposedApi: true })
  await new Promise((resolve) => term.write(transcript, resolve))
  const buffer = term.buffer.active
  const lines = Array.from({ length: buffer.length }, (_, y) => buffer.getLine(y))
  const lastUsed = lines.findLastIndex((line) => line.translateToString(true).trim() !== '')
  return lines.slice(0, lastUsed + 1).map(lineRuns)
}

const lineRuns = (line) => {
  const runs = []
  for (let x = 0; x < line.length; x++) {
    const cell = line.getCell(x)
    if (cell.getWidth() === 0) continue
    const style = { color: cellColour(cell), bold: Boolean(cell.isBold()) }
    const last = runs.at(-1)
    if (last && last.color === style.color && last.bold === style.bold) last.text += cell.getChars() || ' '
    else runs.push({ ...style, text: cell.getChars() || ' ' })
  }
  return runs.map((run) => ({ ...run, text: run.text })).filter((run, index, all) => index < all.length - 1 || run.text.trim())
}

const ANSI_COLOURS = {
  1: '#ff7a7a', 2: '#5ce1c6', 3: '#f5c542', 4: '#3ba4ff', 5: '#e08cf2', 6: '#5ce1c6', 7: '#d6dde3',
  8: '#7d8a97', 9: '#ff7a7a', 10: '#5ce1c6', 11: '#f5c542', 12: '#3ba4ff', 13: '#e08cf2', 14: '#5ce1c6', 15: '#f2f5f7'
}

const cellColour = (cell) => {
  if (cell.isFgPalette()) return ANSI_COLOURS[cell.getFgColor()] ?? '#d6dde3'
  if (cell.isFgRGB()) return `#${cell.getFgColor().toString(16).padStart(6, '0')}`
  return '#d6dde3'
}

const terminalHtml = (screen) => screen.map((runs) => `<div class="line">${runs.map(runHtml).join('')}</div>`).join('')

const runHtml = ({ color, bold, text }) => `<span style="color:${color}${bold ? ';font-weight:700' : ''}">${escapeHtml(text)}</span>`

const escapeHtml = (text) => text.replace(/[&<>"]/g, (char) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' })[char])

const loadAssets = async () => {
  const fontDir = path.join(FMR_ROOT, 'src/renderer/fonts')
  const [figtree, mono] = await Promise.all([
    fs.readFile(path.join(fontDir, 'figtree-latin-variable.woff2')),
    fs.readFile(path.join(fontDir, 'geist-mono-latin-500-normal.woff2'))
  ])
  return { figtree: figtree.toString('base64'), mono: mono.toString('base64') }
}

const layoutOf = () => {
  const unit = card.height / 630
  return {
    left: Math.round(64 * unit),
    copyWidth: Math.round(470 * unit),
    wordmarkSize: Math.round(88 * unit),
    headlineSize: Math.round(35 * unit),
    urlSize: Math.round(22 * unit),
    urlBottom: Math.round(56 * unit),
    frameLeft: card.width - Math.round(628 * unit),
    frameTop: Math.round(48 * unit),
    frameWidth: Math.round(690 * unit),
    barHeight: Math.round(34 * unit),
    dot: Math.round(11 * unit),
    radius: Math.round(14 * unit),
    termSize: 12.5 * unit
  }
}

const cardHtml = ({ assets, terminalHtml }) => {
  const l = layoutOf()
  return `<!doctype html>
<html>
<head>
<meta charset="utf-8">
<style>
@font-face {
  font-family: "Figtree";
  font-weight: 300 900;
  src: url(data:font/woff2;base64,${assets.figtree}) format("woff2");
}
@font-face {
  font-family: "Geist Mono";
  font-weight: 500;
  src: url(data:font/woff2;base64,${assets.mono}) format("woff2");
}
* { box-sizing: border-box; margin: 0; }
html, body { width: ${card.width}px; height: ${card.height}px; overflow: hidden; }
body {
  position: relative;
  background: #0b1117;
  color: #f2f5f7;
  font-family: "Figtree", sans-serif;
}
.copy {
  position: absolute;
  left: ${l.left}px;
  top: 0;
  bottom: ${l.urlBottom + l.urlSize}px;
  width: ${l.copyWidth}px;
  display: flex;
  flex-direction: column;
  justify-content: center;
}
.wordmark {
  display: flex;
  align-items: baseline;
  margin-bottom: ${Math.round(l.wordmarkSize * 0.4)}px;
  font-size: ${l.wordmarkSize}px;
  font-weight: 800;
  letter-spacing: -0.02em;
  line-height: 1;
  white-space: nowrap;
}
.wordmark .cursor {
  display: inline-block;
  width: 0.42em;
  height: 0.12em;
  margin-left: 0.08em;
  border-radius: 0.03em;
  background: #5ce1c6;
}
h1 {
  font-size: ${l.headlineSize}px;
  font-weight: 800;
  line-height: 1.2;
  letter-spacing: -0.015em;
  text-wrap: balance;
}
h1 span { display: block; text-wrap: balance; white-space: nowrap; }
.url {
  position: absolute;
  left: ${l.left}px;
  bottom: ${l.urlBottom}px;
  color: #a9b4be;
  font-family: "Geist Mono", monospace;
  font-weight: 500;
  font-size: ${l.urlSize}px;
}
.frame {
  position: absolute;
  left: ${l.frameLeft}px;
  top: ${l.frameTop}px;
  width: ${l.frameWidth}px;
  height: ${card.height}px;
  border: 1px solid rgba(255, 255, 255, 0.08);
  border-radius: ${l.radius}px;
  overflow: hidden;
  background: #0e151c;
}
.bar {
  display: flex;
  align-items: center;
  gap: ${Math.round(l.dot * 0.75)}px;
  height: ${l.barHeight}px;
  padding: 0 ${Math.round(l.dot * 1.3)}px;
  background: #141c24;
  border-bottom: 1px solid rgba(255, 255, 255, 0.08);
}
.bar i { width: ${l.dot}px; height: ${l.dot}px; border-radius: 50%; background: #26323e; }
.terminal {
  padding: ${Math.round(l.termSize * 1.3)}px ${Math.round(l.termSize * 1.5)}px;
  font-family: "Geist Mono", "Apple Color Emoji", monospace;
  font-weight: 500;
  font-size: ${l.termSize}px;
  line-height: 1.45;
  white-space: pre;
  color: #d6dde3;
}
.line { min-height: 1.45em; }
</style>
</head>
<body>
  <div class="copy">
    <div class="wordmark">fetchtv<span class="cursor"></span></div>
    <h1>${TAGLINE.map((sentence) => `<span>${sentence}</span>`).join(' ')}</h1>
  </div>
  <span class="url">${REPO_URL}</span>
  <div class="frame">
    <div class="bar"><i></i><i></i><i></i></div>
    <div class="terminal">${terminalHtml}</div>
  </div>
</body>
</html>`
}

const PTY_RUNNER = `
import os, pty, sys, time, fcntl, termios, struct, signal, select
rows, cols, seconds = int(sys.argv[1]), int(sys.argv[2]), float(sys.argv[3])
pid, fd = pty.fork()
if pid == 0:
    os.environ['FORCE_COLOR'] = '1'
    os.environ['TERM'] = 'xterm-256color'
    os.execvp(sys.argv[4], sys.argv[4:])
fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack('HHHH', rows, cols, 0, 0))
os.kill(pid, signal.SIGWINCH)
end = time.time() + seconds
while time.time() < end:
    ready, _, _ = select.select([fd], [], [], 0.1)
    if fd not in ready: continue
    try: chunk = os.read(fd, 65536)
    except OSError: break
    if not chunk: break
    sys.stdout.buffer.write(chunk)
try: os.kill(pid, signal.SIGKILL)
except ProcessLookupError: pass
`

await main()
