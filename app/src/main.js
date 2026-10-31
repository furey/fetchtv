import { app, BrowserWindow, Menu, dialog, ipcMain, nativeTheme, powerSaveBlocker, session, shell } from 'electron'
import path from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import { syncCleanupLockFiles } from '../../fetchtv.js'
import { connectToAddress, describeBox, findBoxes, parseBoxAddress } from './boxes.js'
import { describeFolder } from './folders.js'
import { listRecordings, pickEpisodes } from './recordings.js'
import { startSave } from './saving.js'
import { createSettings } from './settings.js'

const PROJECT_URL = 'https://github.com/furey/fetchtv'
const distDir = path.dirname(fileURLToPath(import.meta.url))
const rendererPage = path.join(distDir, 'renderer', 'index.html')
const rendererUrl = pathToFileURL(rendererPage).href

const state = {
  window: null,
  boxes: new Map(),
  shows: [],
  summaries: [],
  activeSave: null,
  savedFolder: null,
  closingAfterStop: false
}

const main = () => {
  if (process.env.FETCHTV_USER_DATA) app.setPath('userData', process.env.FETCHTV_USER_DATA)
  if (!app.requestSingleInstanceLock()) return app.quit()
  app.enableSandbox()
  app.setAboutPanelOptions({
    applicationName: 'Fetch My Recordings',
    applicationVersion: FETCHTV_VERSION,
    copyright: 'Free and open source under the GPL-3.0-or-later licence.',
    credits: 'Built on the fetchtv command-line tool. Fetch My Recordings is an independent, free tool made by a Fetch customer. It is not made, endorsed, or supported by Fetch TV. Fetch is a trademark of its owner.'
  })
  app.on('second-instance', focusWindow)
  app.on('window-all-closed', () => app.quit())
  app.on('will-quit', () => syncCleanupLockFiles())
  app.on('web-contents-created', (_event, contents) => lockDownContents(contents))
  app.whenReady().then(start)
}

const start = () => {
  nativeTheme.themeSource = 'dark'
  const settings = createSettings(app.getPath('userData'))
  blockRemoteRequests()
  Menu.setApplicationMenu(buildMenu())
  registerIpc(settings)
  state.window = createWindow()
}

const createWindow = () => {
  const window = new BrowserWindow({
    width: 1000,
    height: 820,
    minWidth: 640,
    minHeight: 560,
    title: 'Fetch My Recordings',
    show: false,
    autoHideMenuBar: true,
    backgroundColor: '#0b1117',
    webPreferences: {
      preload: path.join(distDir, 'preload.cjs'),
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: true,
      webSecurity: true,
      spellcheck: false
    }
  })
  window.once('ready-to-show', () => window.show())
  window.on('close', (event) => confirmCloseDuringSave({ event, window }))
  window.loadFile(rendererPage)
  return window
}

const registerIpc = (settings) => {
  handle('app:info', async () => ({
    version: FETCHTV_VERSION,
    settings: await settings.read(),
    defaultFolder: defaultSaveFolder()
  }))

  handle('boxes:find', async () => {
    const { lastAddress } = await settings.read()
    const locations = await findBoxes({
      rememberedAddress: lastAddress,
      searchNetwork: process.env.FETCHTV_DISCOVERY !== 'off'
    })
    return { boxes: locations.map(rememberBox) }
  })

  handle('boxes:connect', async ({ address }) => {
    if (!parseBoxAddress(address)) return { error: 'invalid-address' }
    const location = await connectToAddress(address)
    if (!location) return { error: 'no-answer' }
    return { box: rememberBox(location) }
  })

  handle('recordings:list', async ({ boxId }) => {
    const location = state.boxes.get(boxId)
    if (!location) return { error: 'unknown-box' }
    const onProgress = (progress) => send('recordings:progress', progress)
    try {
      const { shows, summaries } = await listRecordings({ location, onProgress })
      state.shows = shows
      state.summaries = summaries
      await settings.update({ lastAddress: describeBox(location).address })
      return { shows: summaries }
    } catch {
      return { error: 'list-failed' }
    }
  })

  handle('folder:choose', async ({ current }) => {
    const { canceled, filePaths } = await dialog.showOpenDialog(state.window, {
      title: 'Choose a folder for your recordings',
      buttonLabel: 'Use this folder',
      defaultPath: typeof current === 'string' ? current : defaultSaveFolder(),
      properties: ['openDirectory', 'createDirectory']
    })
    if (canceled || !filePaths?.[0]) return {}
    return { folder: filePaths[0] }
  })

  handle('folder:describe', async ({ folder, episodeIds }) => {
    if (!isUsableFolder(folder)) return { error: 'invalid-folder' }
    return describeFolder({ folder, items: selectedItems(episodeIds) })
  })

  handle('save:start', async ({ folder, forPlex, episodeIds }) => {
    if (state.activeSave) return { error: 'already-saving' }
    if (!isUsableFolder(folder)) return { error: 'invalid-folder' }
    const recordings = pickEpisodes({ shows: state.shows, episodeIds: toIdList(episodeIds) })
    await settings.update({ lastFolder: folder, forPlex: Boolean(forPlex) })
    return runSave({ recordings, folder, forPlex: Boolean(forPlex) })
  })

  handle('save:cancel', async () => {
    state.activeSave?.cancel()
    return {}
  })

  handle('folder:open', async () => {
    if (!state.savedFolder) return {}
    await shell.openPath(state.savedFolder)
    return {}
  })

  handle('settings:sound', async ({ on }) => {
    await settings.update({ sound: Boolean(on) })
    return {}
  })

  handle('link:project', async () => {
    await shell.openExternal(PROJECT_URL)
    return {}
  })
}

const runSave = async ({ recordings, folder, forPlex }) => {
  const blockerId = powerSaveBlocker.start('prevent-app-suspension')
  const save = startSave({
    recordings,
    folder,
    forPlex,
    retryDelays: retryDelaysFromEnv(),
    onProgress: ({ id, bytes, total }) => send('save:progress', { id: String(id), bytes, total }),
    onResult: (result) => send('save:result', result)
  })
  state.activeSave = save
  state.savedFolder = folder
  try {
    return { summary: await save.done }
  } catch (error) {
    return { error: error.code === 'ENOSPC' ? 'no-space' : 'save-failed' }
  } finally {
    state.activeSave = null
    powerSaveBlocker.stop(blockerId)
  }
}

const confirmCloseDuringSave = async ({ event, window }) => {
  if (!state.activeSave || state.closingAfterStop) return
  event.preventDefault()
  const { response } = await dialog.showMessageBox(window, {
    type: 'question',
    buttons: ['Keep saving', 'Stop and close'],
    defaultId: 0,
    cancelId: 0,
    message: 'Recordings are still saving.',
    detail: 'If you stop now, a recording that is part way keeps what it has. Save the same recordings to the same folder next time to carry on from there.'
  })
  if (response !== 1 || !state.activeSave) return
  const save = state.activeSave
  save.cancel()
  await save.done.catch(() => null)
  state.closingAfterStop = true
  window.close()
}

const handle = (channel, handler) => {
  ipcMain.handle(channel, async (event, args = {}) => {
    if (!isTrustedSender(event)) throw new Error('Untrusted sender')
    return handler(typeof args === 'object' && args !== null ? args : {})
  })
}

const isTrustedSender = (event) => comparableUrl(event.senderFrame?.url ?? '') === comparableUrl(rendererUrl)

const comparableUrl = (url) => {
  const withoutHash = decodeURI(url.split('#')[0])
  return process.platform === 'win32' ? withoutHash.toLowerCase() : withoutHash
}

const send = (channel, payload) => {
  if (state.window && !state.window.isDestroyed()) state.window.webContents.send(channel, payload)
}

const rememberBox = (location) => {
  state.boxes.set(location.url, location)
  return describeBox(location)
}

const selectedItems = (episodeIds) => {
  const wanted = new Set(toIdList(episodeIds))
  return state.summaries.flatMap(show => show.episodes).filter(episode => wanted.has(episode.id))
}

const toIdList = (ids) => (Array.isArray(ids) ? ids : []).map(String)

const isUsableFolder = (folder) => typeof folder === 'string' && path.isAbsolute(folder)

const defaultSaveFolder = () => {
  const videos = safePath('videos') ?? safePath('home')
  return path.join(videos, 'Fetch recordings')
}

const safePath = (name) => {
  try {
    return app.getPath(name)
  } catch {
    return null
  }
}

const retryDelaysFromEnv = () => {
  const text = process.env.FETCHTV_RETRY_DELAYS_MS
  if (!text) return undefined
  return text.split(',').map(Number).filter(Number.isFinite)
}

const focusWindow = () => {
  if (!state.window) return
  if (state.window.isMinimized()) state.window.restore()
  state.window.focus()
}

const lockDownContents = (contents) => {
  contents.on('will-navigate', (event) => event.preventDefault())
  contents.on('will-attach-webview', (event) => event.preventDefault())
  contents.setWindowOpenHandler(() => ({ action: 'deny' }))
}

const blockRemoteRequests = () => {
  session.defaultSession.setPermissionRequestHandler((_contents, _permission, callback) => callback(false))
  session.defaultSession.setPermissionCheckHandler(() => false)
  session.defaultSession.webRequest.onBeforeRequest(
    { urls: ['http://*/*', 'https://*/*', 'ws://*/*', 'wss://*/*'] },
    (_details, callback) => callback({ cancel: true })
  )
}

const buildMenu = () => Menu.buildFromTemplate([
  ...(process.platform === 'darwin' ? [{ role: 'appMenu' }] : [{ role: 'fileMenu' }]),
  { role: 'editMenu' },
  { role: 'windowMenu' }
])

main()
