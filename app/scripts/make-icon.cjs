const { app, BrowserWindow } = require('electron')
const fs = require('node:fs')
const path = require('node:path')

const appRoot = path.join(__dirname, '..')
const buildDir = path.join(appRoot, 'build')
const markInner = fs.readFileSync(path.join(appRoot, 'src', 'renderer', 'mark.svg'), 'utf8').replace(/<\/?svg[^>]*>/g, '')
const TILE = '#141c24'
const LINUX_SIZES = [16, 24, 32, 48, 64, 128, 256, 512, 1024]
const ICO_SIZES = [16, 24, 32, 48, 64, 128, 256]
const SMALL_ICON_LIMIT = 48

const macIcon = (size) => `<svg xmlns="http://www.w3.org/2000/svg" width="${size}" height="${size}" viewBox="0 0 1024 1024">
  <rect x="100" y="100" width="824" height="824" rx="185" fill="${TILE}"/>
  <svg x="192" y="192" width="640" height="640" viewBox="0 0 64 64">${markInner}</svg></svg>`

const fullIcon = (size) => `<svg xmlns="http://www.w3.org/2000/svg" width="${size}" height="${size}" viewBox="0 0 1024 1024">
  <rect width="1024" height="1024" rx="200" fill="${TILE}"/>
  <svg x="112" y="112" width="800" height="800" viewBox="0 0 64 64">${markInner}</svg></svg>`

const smallIcon = (size) => `<svg xmlns="http://www.w3.org/2000/svg" width="${size}" height="${size}" viewBox="0 0 64 64">
  <rect width="64" height="64" rx="12" fill="${TILE}"/>
  <svg x="-2" y="-2" width="68" height="68" viewBox="0 0 64 64">${markInner}</svg></svg>`

const iconFor = (size) => (size <= SMALL_ICON_LIMIT ? smallIcon(size) : fullIcon(size))

const renderSvg = async ({ window, svg, size }) => {
  await window.loadURL(`data:text/html,${encodeURIComponent(`<html><body style="margin:0;background:transparent">${svg}</body></html>`)}`)
  await new Promise(resolve => setTimeout(resolve, 150))
  const image = await window.webContents.capturePage({ x: 0, y: 0, width: size, height: size })
  return image.getSize().width === size ? image.toPNG() : image.resize({ width: size, height: size, quality: 'best' }).toPNG()
}

const writeIco = (entries) => {
  const header = Buffer.alloc(6 + entries.length * 16)
  header.writeUInt16LE(0, 0)
  header.writeUInt16LE(1, 2)
  header.writeUInt16LE(entries.length, 4)
  let offset = header.length
  entries.forEach(({ size, png }, index) => {
    const at = 6 + index * 16
    header.writeUInt8(size >= 256 ? 0 : size, at)
    header.writeUInt8(size >= 256 ? 0 : size, at + 1)
    header.writeUInt16LE(1, at + 4)
    header.writeUInt16LE(32, at + 6)
    header.writeUInt32LE(png.length, at + 8)
    header.writeUInt32LE(offset, at + 12)
    offset += png.length
  })
  fs.writeFileSync(path.join(buildDir, 'icon.ico'), Buffer.concat([header, ...entries.map(entry => entry.png)]))
}

app.disableHardwareAcceleration()
app.whenReady().then(async () => {
  const window = new BrowserWindow({ width: 1024, height: 1024, show: false, frame: false, transparent: true, webPreferences: { offscreen: true } })
  fs.writeFileSync(path.join(buildDir, 'icon.png'), await renderSvg({ window, svg: macIcon(1024), size: 1024 }))
  fs.mkdirSync(path.join(buildDir, 'icons'), { recursive: true })
  const pngs = {}
  for (const size of LINUX_SIZES) {
    pngs[size] = await renderSvg({ window, svg: iconFor(size), size })
    fs.writeFileSync(path.join(buildDir, 'icons', `${size}x${size}.png`), pngs[size])
  }
  writeIco(ICO_SIZES.map(size => ({ size, png: pngs[size] })))
  app.quit()
})
