'use strict'

const api = window.fetchtv

const state = {
  info: null,
  step: 1,
  boxes: [],
  box: null,
  shows: [],
  selected: new Set(),
  open: new Set(),
  folder: '',
  forPlex: false,
  folderInfo: null,
  save: null
}

const start = async () => {
  state.info = await api.appInfo()
  state.folder = state.info.settings.lastFolder || state.info.defaultFolder
  state.forPlex = Boolean(state.info.settings.forPlex)
  document.getElementById('about-line').textContent =
    `fetchtv ${state.info.version} · Free software (GPL-3.0-or-later) · Not affiliated with Fetch TV`
  document.getElementById('project-link').addEventListener('click', () => api.openProjectPage())
  api.onListProgress(showListProgress)
  api.onSaveProgress(updateSaveProgress)
  api.onSaveResult(updateSaveResult)
  searchForBoxes({ autoPick: true })
}

const searchForBoxes = async ({ autoPick }) => {
  showScreen(1, searchingScreen())
  const { boxes } = await api.findBoxes()
  state.boxes = boxes
  if (autoPick && boxes.length === 1) return useBox(boxes[0])
  showScreen(1, boxes.length === 0 ? notFoundScreen() : chooseBoxScreen())
}

const useBox = async (box) => {
  state.box = box
  state.shows = []
  state.selected = new Set()
  state.open = new Set()
  showScreen(2, loadingListScreen())
  const result = await api.listRecordings({ boxId: box.id })
  if (result.error) return showScreen(2, listFailedScreen())
  state.shows = result.shows
  state.selected = new Set(savableEpisodes(state.shows).map(episode => episode.id))
  showScreen(2, state.shows.length === 0 ? emptyListScreen() : recordingsScreen())
}

const connectByAddress = async ({ address, form }) => {
  const message = form.querySelector('.form-message')
  const button = form.querySelector('button[type="submit"]')
  message.textContent = ''
  button.disabled = true
  button.textContent = 'Connecting…'
  const result = await api.connectToBox({ address })
  button.disabled = false
  button.textContent = 'Connect'
  if (result.box) return useBox(result.box)
  message.textContent = result.error === 'invalid-address'
    ? 'Type the address as four numbers with dots between them, such as 192.168.1.50.'
    : `No Fetch box answered at ${address.trim()}. Check the address, and check that the box is turned on.`
}

const goToFolderStep = async () => {
  showScreen(3, folderScreen())
  await refreshFolderInfo()
}

const refreshFolderInfo = async () => {
  state.folderInfo = null
  renderFolderInfo()
  const info = await api.describeFolder({ folder: state.folder, episodeIds: [...state.selected] })
  state.folderInfo = info.error ? null : info
  renderFolderInfo()
}

const changeFolder = async () => {
  const { folder } = await api.chooseFolder({ current: state.folder })
  if (!folder) return
  state.folder = folder
  document.getElementById('folder-path').textContent = folder
  await refreshFolderInfo()
}

const beginSave = async () => {
  const order = savableEpisodes(state.shows).filter(episode => state.selected.has(episode.id))
  state.save = {
    order,
    rows: new Map(order.map(episode => [episode.id, { status: 'waiting', bytes: 0, total: episode.size, firstBytes: null, reason: '' }])),
    elements: new Map(),
    startedAt: Date.now(),
    stopping: false,
    summary: null
  }
  showScreen(4, savingScreen())
  const result = await api.startSave({ folder: state.folder, forPlex: state.forPlex, episodeIds: order.map(episode => episode.id) })
  state.save.summary = result.summary ?? null
  state.save.error = result.error ?? null
  showScreen(4, finishedScreen())
}

const stopSave = async (button) => {
  state.save.stopping = true
  button.disabled = true
  button.textContent = 'Stopping…'
  await api.cancelSave()
}

const searchingScreen = () => ({
  content: [
    h('h1', {}, 'Looking for your Fetch box'),
    h('p', {}, 'fetchtv is searching your home network. This takes up to 10 seconds.'),
    busyBar()
  ]
})

const notFoundScreen = () => ({
  content: [
    h('h1', {}, 'No Fetch box found'),
    h('p', {}, 'Check that:'),
    h('ul', { className: 'plain' },
      h('li', {}, 'the Fetch box is turned on'),
      h('li', {}, 'this computer is on the same Wi-Fi or home network as the Fetch box')),
    h('p', {}, 'Then search again, or type the IP address of the Fetch box below.'),
    h('p', {}, h('button', { type: 'button', className: 'button', onclick: () => searchForBoxes({ autoPick: true }) }, 'Search again')),
    addressForm()
  ]
})

const chooseBoxScreen = () => ({
  content: [
    h('h1', {}, state.boxes.length > 1 ? 'Choose your Fetch box' : 'Fetch box found'),
    h('p', {}, state.boxes.length > 1
      ? 'There is more than one Fetch box on your network. Choose the one with your recordings.'
      : 'Choose the box to copy recordings from.'),
    h('ul', { className: 'table' }, ...state.boxes.map(box => h('li', { className: 'row' },
      h('button', { type: 'button', className: 'choice', onclick: () => useBox(box) },
        h('span', { className: 'row-text' },
          h('span', { className: 'row-title' }, box.name),
          h('span', { className: 'row-detail' }, [box.model, `IP address ${box.address}`].filter(Boolean).join(' · ')))))
    )),
    h('p', { className: 'small' }, h('button', { type: 'button', className: 'link', onclick: () => searchForBoxes({ autoPick: false }) }, 'Search again')),
    addressForm()
  ]
})

const addressForm = () => {
  const input = h('input', {
    id: 'box-address',
    type: 'text',
    inputMode: 'decimal',
    autocomplete: 'off',
    spellcheck: false,
    value: state.info.settings.lastAddress ?? ''
  })
  const form = h('form', { className: 'address-form', novalidate: true },
    h('h2', {}, 'Type the IP address'),
    h('div', { className: 'field' },
      h('label', { htmlFor: 'box-address' }, 'IP address of the Fetch box'),
      input,
      h('button', { type: 'submit', className: 'button primary' }, 'Connect')),
    h('p', { className: 'form-message error-text', role: 'alert' }),
    h('p', { className: 'muted small' },
      'The IP address is four numbers with dots between them, such as 192.168.1.50. ' +
      'To find it, look in your router’s list of connected devices, or in the network details of the Fetch box’s settings menu.'))
  form.addEventListener('submit', (event) => {
    event.preventDefault()
    connectByAddress({ address: input.value, form })
  })
  return form
}

const loadingListScreen = () => ({
  content: [
    boxLine(),
    h('h1', {}, 'Reading your recordings'),
    h('p', { id: 'list-progress' }, 'Asking the Fetch box for its list of recordings…'),
    busyBar()
  ]
})

const showListProgress = ({ done, total }) => {
  const line = document.getElementById('list-progress')
  if (line && total > 0) line.textContent = `Reading the list of recordings: ${done} of ${total} shows`
}

const emptyListScreen = () => ({
  content: [
    boxLine(),
    h('h1', {}, 'No recordings on this Fetch box'),
    h('p', {}, 'The box answered, but it lists no recordings to copy.'),
    h('p', {}, 'A Fetch box whose account is cancelled shows its setup screen on the TV and lists no recordings.'),
    h('p', {}, h('button', { type: 'button', className: 'button', onclick: () => useBox(state.box) }, 'Check again'))
  ]
})

const listFailedScreen = () => ({
  content: [
    boxLine(),
    h('h1', {}, 'Could not read the recordings'),
    h('p', {}, 'The Fetch box did not send its list of recordings. Check that the box is turned on, then try again.'),
    h('p', {}, h('button', { type: 'button', className: 'button', onclick: () => useBox(state.box) }, 'Try again'))
  ]
})

const boxLine = () => h('p', { className: 'muted small' },
  `Fetch box: ${state.box.name}, IP address ${state.box.address} `,
  h('button', { type: 'button', className: 'link', onclick: () => searchForBoxes({ autoPick: false }) }, 'Use a different box'))

const recordingsScreen = () => {
  const summary = h('span', { className: 'summary', id: 'selection-summary' })
  const next = h('button', { type: 'button', className: 'button primary', id: 'next-to-folder', onclick: goToFolderStep }, 'Next')
  const rows = state.shows.map(showRow)
  const refreshAll = () => {
    rows.forEach(row => row.refresh())
    refreshSelectionSummary({ summary, next })
  }
  rows.forEach(row => { row.onChange = refreshAll })
  refreshSelectionSummary({ summary, next })
  return {
    content: [
      boxLine(),
      h('h1', {}, 'Choose the recordings to save'),
      h('div', { className: 'toolbar' },
        h('button', { type: 'button', className: 'button', onclick: () => { selectAll(); refreshAll() } }, 'Select all'),
        h('button', { type: 'button', className: 'button', onclick: () => { state.selected.clear(); refreshAll() } }, 'Select none')),
      h('ul', { className: 'table', id: 'show-list' }, ...rows.map(row => row.element))
    ],
    actions: [
      summary,
      h('button', { type: 'button', className: 'button', onclick: () => searchForBoxes({ autoPick: false }) }, 'Back'),
      next
    ]
  }
}

const showRow = (show) => {
  const savable = show.episodes.filter(episode => !episode.stillRecording)
  const checkbox = h('input', { type: 'checkbox', disabled: savable.length === 0 })
  const row = { onChange: () => {} }
  const episodeRows = show.episodes.map(episode => episodeRow({ episode, onChange: () => row.onChange() }))
  const drawer = h('ul', { className: 'drawer', hidden: !state.open.has(show.id) }, ...episodeRows.map(item => item.element))
  const toggle = h('button', {
    type: 'button',
    className: 'button row-button',
    'aria-expanded': String(state.open.has(show.id)),
    onclick: () => toggleDrawer({ show, drawer, toggle })
  }, state.open.has(show.id) ? 'Hide episodes' : 'Choose episodes')

  checkbox.addEventListener('change', () => {
    savable.forEach(episode => checkbox.checked ? state.selected.add(episode.id) : state.selected.delete(episode.id))
    row.onChange()
  })

  row.refresh = () => {
    const chosen = savable.filter(episode => state.selected.has(episode.id)).length
    checkbox.checked = chosen > 0 && chosen === savable.length
    checkbox.indeterminate = chosen > 0 && chosen < savable.length
    episodeRows.forEach(item => item.refresh())
  }
  row.refresh()

  row.element = h('li', { className: 'row show-row', 'data-show': show.title },
    h('div', { className: 'row-main' },
      h('label', { className: `row-pick${savable.length === 0 ? ' disabled' : ''}` },
        checkbox,
        h('span', { className: 'row-text' },
          h('span', { className: 'row-title' }, show.title),
          h('span', { className: 'row-detail' }, showDetail(show)))),
      show.episodes.length > 1 ? toggle : null),
    show.episodes.length > 1 ? drawer : null)
  return row
}

const episodeRow = ({ episode, onChange }) => {
  const checkbox = h('input', { type: 'checkbox', disabled: episode.stillRecording })
  checkbox.addEventListener('change', () => {
    checkbox.checked ? state.selected.add(episode.id) : state.selected.delete(episode.id)
    onChange()
  })
  const detail = episode.stillRecording
    ? 'Still recording. Save it after the recording ends.'
    : [formatDuration(episode.duration), formatBytes(episode.size)].filter(Boolean).join(' · ')
  return {
    element: h('li', { className: 'row' },
      h('div', { className: 'row-main' },
        h('label', { className: `row-pick${episode.stillRecording ? ' disabled' : ''}` },
          checkbox,
          h('span', { className: 'row-text' },
            h('span', { className: 'row-title' }, episode.title),
            h('span', { className: 'row-detail' }, detail))))),
    refresh: () => { checkbox.checked = state.selected.has(episode.id) }
  }
}

const toggleDrawer = ({ show, drawer, toggle }) => {
  const isOpen = state.open.has(show.id)
  isOpen ? state.open.delete(show.id) : state.open.add(show.id)
  drawer.hidden = isOpen
  toggle.setAttribute('aria-expanded', String(!isOpen))
  toggle.textContent = isOpen ? 'Choose episodes' : 'Hide episodes'
}

const refreshSelectionSummary = ({ summary, next }) => {
  const chosen = savableEpisodes(state.shows).filter(episode => state.selected.has(episode.id))
  next.disabled = chosen.length === 0
  summary.textContent = chosen.length === 0
    ? 'Nothing selected'
    : `${plural(chosen.length, 'recording')} selected · ${formatBytes(sumSizes(chosen))}`
}

const selectAll = () => savableEpisodes(state.shows).forEach(episode => state.selected.add(episode.id))

const showDetail = (show) => {
  const recording = show.episodes.filter(episode => episode.stillRecording).length
  const parts = [plural(show.episodes.length, 'recording'), formatBytes(sumSizes(show.episodes))]
  if (recording > 0) parts.push(`${recording} still recording`)
  return parts.join(' · ')
}

const folderScreen = () => {
  const plexSwitch = h('button', {
    type: 'button',
    role: 'switch',
    className: 'switch',
    id: 'plex-switch',
    'aria-checked': String(state.forPlex),
    'aria-labelledby': 'plex-label',
    'aria-describedby': 'plex-help'
  })
  const example = h('span', { className: 'example', id: 'layout-example' })
  const refreshExample = () => {
    plexSwitch.setAttribute('aria-checked', String(state.forPlex))
    example.textContent = layoutExample()
  }
  plexSwitch.addEventListener('click', () => {
    state.forPlex = !state.forPlex
    refreshExample()
  })
  refreshExample()
  const chosen = savableEpisodes(state.shows).filter(episode => state.selected.has(episode.id))
  return {
    content: [
      h('h1', {}, 'Choose where to save'),
      h('p', {}, `fetchtv saves ${plural(chosen.length, 'recording')} (${formatBytes(sumSizes(chosen))}) into this folder, with a folder for each show.`),
      h('div', { className: 'folder' },
        h('span', { className: 'folder-path', id: 'folder-path' }, state.folder),
        h('button', { type: 'button', className: 'button', id: 'change-folder', onclick: changeFolder }, 'Change folder')),
      h('div', { id: 'folder-info', 'aria-live': 'polite' }),
      h('div', { className: 'switch-row' },
        plexSwitch,
        h('div', {},
          h('span', { className: 'switch-label', id: 'plex-label', onclick: () => plexSwitch.click() }, 'Name files for Plex, Jellyfin or Kodi'),
          h('p', { className: 'muted small', id: 'plex-help' },
            'Media library apps such as Plex, Jellyfin and Kodi need a folder for each season and the season and episode number in each file name. ' +
            'Turn this on if you will watch the recordings in one of those apps. A recording without a season and episode number, such as a film, keeps its own name.'),
          h('p', { className: 'small' }, 'Files are saved like this: ', example)))
    ],
    actions: [
      h('span', { className: 'spacer' }),
      h('button', { type: 'button', className: 'button', onclick: () => showScreen(2, recordingsScreen()) }, 'Back'),
      h('button', { type: 'button', className: 'button primary', id: 'start-save', onclick: beginSave }, 'Start saving')
    ]
  }
}

const renderFolderInfo = () => {
  const box = document.getElementById('folder-info')
  if (!box) return
  const info = state.folderInfo
  const startButton = document.getElementById('start-save')
  if (!info) {
    box.replaceChildren(h('p', { className: 'muted' }, 'Checking the free space in this folder…'))
    if (startButton) startButton.disabled = true
    return
  }
  if (startButton) startButton.disabled = !info.canWrite
  box.replaceChildren(...folderInfoLines(info))
}

const folderInfoLines = (info) => {
  if (!info.canWrite) return [h('p', { className: 'notice', role: 'alert' }, 'fetchtv cannot save to this folder. Choose another folder.')]
  const lines = []
  const free = info.freeBytes === null ? 'unknown' : formatBytes(info.freeBytes)
  lines.push(h('p', {}, `Free space: ${free}. These recordings need about ${formatBytes(info.neededBytes)}.`))
  if (info.alreadySavedCount > 0) {
    const one = info.alreadySavedCount === 1
    lines.push(h('p', {}, `${info.alreadySavedCount} of these recordings ${one ? 'is' : 'are'} already saved in this folder, so fetchtv skips ${one ? 'it' : 'them'}.`))
  }
  if (info.freeBytes !== null && info.neededBytes > info.freeBytes) {
    lines.push(h('p', { className: 'notice', role: 'alert' },
      'There is not enough free space for all of these recordings. fetchtv saves what fits. To save them all, choose a folder on a bigger drive, or go back and choose fewer recordings.'))
  }
  if (!info.exists) lines.push(h('p', { className: 'muted small' }, 'This folder does not exist yet. fetchtv makes it when the save starts.'))
  return lines
}

const layoutExample = () => {
  const episode = savableEpisodes(state.shows).find(item => state.selected.has(item.id))
  const show = state.shows.find(item => item.episodes.includes(episode))
  if (!episode || !show) return ''
  const numbers = episode.title.match(/S(?:eason)?\s*(\d{1,3})\s*E(?:pisode)?\s*(\d{1,3})/i)
  if (state.forPlex && numbers) {
    const season = numbers[1].padStart(2, '0')
    const number = numbers[2].padStart(2, '0')
    return `${show.title}/Season ${season}/${show.title} - S${season}E${number}.ts`
  }
  return `${show.title}/${episode.title}.ts`
}

const savingScreen = () => {
  const list = h('ul', { className: 'table' }, ...state.save.order.map(episode => saveRow(episode)))
  const stopButton = h('button', { type: 'button', className: 'button', id: 'stop-save' }, 'Stop saving')
  stopButton.addEventListener('click', () => stopSave(stopButton))
  return {
    content: [
      h('h1', {}, 'Saving recordings'),
      h('p', { id: 'overall-line' }, ''),
      h('div', { className: 'progress', role: 'progressbar', 'aria-label': 'All recordings', id: 'overall-bar' }, h('span', {})),
      h('p', { className: 'muted small', id: 'eta-line' }, 'This computer stays awake until the save finishes. Keep it on the same network as the Fetch box.'),
      list
    ],
    actions: [h('span', { className: 'spacer' }), stopButton],
    after: refreshOverall
  }
}

const saveRow = (episode) => {
  const status = h('span', { className: 'status' }, 'Waiting')
  const bar = h('div', { className: 'progress small-bar', hidden: true }, h('span', {}))
  state.save.elements.set(episode.id, { status, bar })
  const show = state.shows.find(item => item.episodes.includes(episode))
  return h('li', { className: 'row save-row', 'data-episode': episode.id },
    h('div', { className: 'row-main' },
      h('span', { className: 'row-text' },
        h('span', { className: 'row-title' }, `${show?.title ?? ''}: ${episode.title}`),
        h('span', { className: 'row-detail' }, formatBytes(episode.size)),
        bar),
      status))
}

const updateSaveProgress = ({ id, bytes, total }) => {
  const row = state.save?.rows.get(id)
  if (!row || isFinal(row.status)) return
  if (row.firstBytes === null) row.firstBytes = bytes
  row.status = 'saving'
  row.bytes = bytes
  row.total = total > 1 ? total : row.total
  const elements = state.save.elements.get(id)
  if (elements) {
    elements.status.textContent = `${formatBytes(bytes)} of ${formatBytes(row.total)}`
    elements.bar.hidden = false
    elements.bar.firstChild.style.width = `${percent(bytes, row.total)}%`
  }
  refreshOverall()
}

const updateSaveResult = ({ id, status, reason }) => {
  const row = state.save?.rows.get(id)
  if (!row) return
  row.status = status
  row.reason = reason
  if (status === 'saved' || status === 'already_saved') row.bytes = row.total
  const elements = state.save.elements.get(id)
  if (elements) {
    elements.bar.hidden = true
    elements.status.textContent = statusLabel(row)
    elements.status.className = `status ${statusClass(status)}`
  }
  refreshOverall()
}

const refreshOverall = () => {
  const rows = [...state.save.rows.values()]
  const total = rows.reduce((sum, row) => sum + row.total, 0)
  const done = rows.reduce((sum, row) => sum + Math.min(row.bytes, row.total), 0)
  const finished = rows.filter(row => isFinal(row.status)).length
  const line = document.getElementById('overall-line')
  const bar = document.getElementById('overall-bar')
  if (!line || !bar) return
  line.textContent = `${finished} of ${plural(rows.length, 'recording')} done · ${formatBytes(done)} of ${formatBytes(total)}`
  bar.firstChild.style.width = `${percent(done, total)}%`
  bar.setAttribute('aria-valuenow', String(Math.round(percent(done, total))))
  const eta = timeLeft({ rows, total, done })
  if (eta) document.getElementById('eta-line').textContent = `${eta} This computer stays awake until the save finishes.`
}

const timeLeft = ({ rows, total, done }) => {
  const elapsed = (Date.now() - state.save.startedAt) / 1000
  const copied = rows.reduce((sum, row) => sum + (row.firstBytes === null ? 0 : Math.max(0, Math.min(row.bytes, row.total) - row.firstBytes)), 0)
  if (elapsed < 10 || copied <= 0) return ''
  const seconds = (total - done) / (copied / elapsed)
  if (seconds < 60) return 'Less than a minute left.'
  const minutes = Math.round(seconds / 60)
  if (minutes < 60) return `About ${plural(minutes, 'minute')} left.`
  const hours = Math.floor(minutes / 60)
  const rest = minutes % 60
  return `About ${plural(hours, 'hour')}${rest ? ` ${plural(rest, 'minute')}` : ''} left.`
}

const finishedScreen = () => {
  const summary = state.save.summary
  if (!summary) return saveErrorScreen()
  const remaining = summary.notSaved + summary.stopped
  const title = summary.stopped > 0 ? 'Save stopped' : remaining > 0 ? 'Finished, but some recordings were not saved' : 'All done'
  const problemRows = state.save.order.filter(episode => ['failed', 'locked', 'still_recording', 'cancelled'].includes(state.save.rows.get(episode.id)?.status))
  return {
    content: [
      h('h1', {}, title),
      h('dl', { className: 'totals', id: 'save-totals' },
        h('dt', {}, 'Saved'), h('dd', {}, String(summary.saved)),
        h('dt', {}, 'Already saved, so skipped'), h('dd', {}, String(summary.alreadySaved)),
        h('dt', {}, 'Not saved'), h('dd', {}, String(summary.notSaved)),
        ...(summary.stopped > 0 ? [h('dt', {}, 'Stopped'), h('dd', {}, String(summary.stopped))] : [])),
      h('p', {}, `The recordings are in ${state.folder}`),
      remaining > 0
        ? h('p', {}, 'To finish the rest, press Save again. fetchtv skips the recordings that are already saved and carries on with the others.')
        : h('p', {}, 'The files play in VLC, a free video player, or in a media library app such as Plex, Jellyfin or Kodi.'),
      problemRows.length > 0 ? h('h2', {}, 'Not saved') : null,
      problemRows.length > 0
        ? h('ul', { className: 'table', id: 'problem-list' }, ...problemRows.map(episode => problemRow(episode)))
        : null
    ],
    actions: [
      h('button', { type: 'button', className: 'button', id: 'open-folder', onclick: () => api.openSavedFolder() }, 'Open folder'),
      h('span', { className: 'spacer' }),
      h('button', { type: 'button', className: 'button', onclick: () => showScreen(2, recordingsScreen()) }, 'Choose other recordings'),
      h('button', { type: 'button', className: `button${remaining > 0 ? ' primary' : ''}`, id: 'save-again', onclick: beginSave }, 'Save again')
    ]
  }
}

const saveErrorScreen = () => ({
  content: [
    h('h1', {}, 'The save could not start'),
    h('p', {}, state.save.error === 'no-space'
      ? 'The folder has no free space. Choose a folder on a bigger drive.'
      : 'fetchtv could not save into this folder. Choose another folder, then try again.')
  ],
  actions: [
    h('span', { className: 'spacer' }),
    h('button', { type: 'button', className: 'button primary', onclick: goToFolderStep }, 'Choose a folder')
  ]
})

const problemRow = (episode) => {
  const row = state.save.rows.get(episode.id)
  const show = state.shows.find(item => item.episodes.includes(episode))
  return h('li', { className: 'row' },
    h('div', { className: 'row-main' },
      h('span', { className: 'row-text' },
        h('span', { className: 'row-title' }, `${show?.title ?? ''}: ${episode.title}`),
        h('span', { className: 'row-detail' }, row.reason || 'Not started.'))))
}

const statusLabel = (row) => {
  if (row.status === 'saved') return 'Saved'
  if (row.status === 'already_saved') return 'Already saved'
  if (row.status === 'cancelled') return row.reason === 'Not started.' ? 'Not started' : 'Stopped'
  return `Not saved. ${row.reason}`
}

const statusClass = (status) => {
  if (status === 'saved' || status === 'already_saved') return 'saved'
  if (status === 'cancelled') return ''
  return 'failed'
}

const isFinal = (status) => status !== 'waiting' && status !== 'saving'

const showScreen = (step, screen) => {
  state.step = step
  document.querySelectorAll('#steps li').forEach(item => {
    if (Number(item.dataset.step) === step) item.setAttribute('aria-current', 'step')
    else item.removeAttribute('aria-current')
  })
  const view = document.getElementById('view')
  view.replaceChildren(...screen.content.filter(Boolean))
  view.scrollTop = 0
  document.getElementById('actions').replaceChildren(...(screen.actions ?? []).filter(Boolean))
  if (screen.after) screen.after()
  view.focus({ preventScroll: true })
}

const busyBar = () => h('div', { className: 'progress busy', role: 'progressbar', 'aria-label': 'Working' }, h('span', {}))

const savableEpisodes = (shows) => shows.flatMap(show => show.episodes).filter(episode => !episode.stillRecording)

const sumSizes = (episodes) => episodes.reduce((total, episode) => total + (episode.size || 0), 0)

const percent = (part, whole) => (whole > 0 ? Math.min(100, (part / whole) * 100) : 0)

const plural = (count, word) => `${count} ${word}${count === 1 ? '' : 's'}`

const formatBytes = (bytes) => {
  if (!bytes || bytes < 0) return '0 MB'
  if (bytes >= 1e12) return `${(bytes / 1e12).toFixed(2)} TB`
  if (bytes >= 1e9) return `${(bytes / 1e9).toFixed(1)} GB`
  return `${Math.max(1, Math.round(bytes / 1e6))} MB`
}

const formatDuration = (seconds) => {
  if (!seconds) return ''
  const minutes = Math.round(seconds / 60)
  if (minutes < 60) return `${minutes} min`
  return `${Math.floor(minutes / 60)} hr ${minutes % 60} min`
}

const h = (tag, props = {}, ...children) => {
  const element = document.createElement(tag)
  Object.entries(props).forEach(([key, value]) => setProp({ element, key, value }))
  children.flat().filter(child => child !== null && child !== undefined && child !== false)
    .forEach(child => element.append(child instanceof Node ? child : document.createTextNode(String(child))))
  return element
}

const setProp = ({ element, key, value }) => {
  if (value === undefined || value === null) return
  if (key.startsWith('on') && typeof value === 'function') return element.addEventListener(key.slice(2), value)
  if (key.includes('-') || key === 'role' || key === 'novalidate') return element.setAttribute(key, String(value))
  element[key] = value
}

start()
