'use strict'

const api = window.fetchtv

const state = {
  info: null,
  step: 0,
  pendingSearch: null,
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
  document.getElementById('about-line').textContent = `Fetch My Recordings ${state.info.version} · Not affiliated with Fetch TV`
  document.getElementById('footer-github').replaceWith(githubLink())
  setUpSoundToggle(state.info.settings.sound !== false)
  setUpDialog()
  pauseWhenHidden()
  api.onListProgress(showListProgress)
  api.onSaveProgress(updateSaveProgress)
  api.onSaveResult(updateSaveResult)
  state.pendingSearch = api.findBoxes()
  showScreen(0, welcomeScreen())
}

const setUpSoundToggle = (on) => {
  const toggle = document.getElementById('sound-toggle')
  const apply = (value) => {
    sounds.setEnabled(value)
    toggle.setAttribute('aria-checked', String(value))
  }
  apply(on)
  toggle.addEventListener('click', () => {
    const value = !sounds.isEnabled()
    apply(value)
    api.setSound({ on: value })
  })
}

const begin = () => {
  sounds.play('start')
  searchForBoxes({ autoPick: true })
}

const searchForBoxes = async ({ autoPick }) => {
  showScreen(1, searchingScreen())
  const search = state.pendingSearch ?? api.findBoxes()
  state.pendingSearch = null
  const { boxes } = await search
  state.boxes = boxes
  sounds.play(boxes.length === 0 ? 'failed' : 'found')
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
  if (result.error) {
    sounds.play('failed')
    return showScreen(2, listFailedScreen())
  }
  state.shows = result.shows
  state.selected = new Set(savableEpisodes(state.shows).filter(isWanted).map(episode => episode.id))
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
  if (result.box) {
    sounds.play('found')
    return useBox(result.box)
  }
  if (result.error !== 'invalid-address') sounds.play('failed')
  message.textContent = result.error === 'invalid-address'
    ? 'That is not an IP address.'
    : `No box answered at ${address.trim()}.`
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
  document.getElementById('folder-path').replaceWith(folderLabel())
  await refreshFolderInfo()
}

const beginSave = async () => {
  const order = savableEpisodes(state.shows).filter(episode => state.selected.has(episode.id))
  state.save = {
    order,
    rows: new Map(order.map(episode => [episode.id, { status: 'waiting', bytes: 0, total: episode.size, firstBytes: null, reason: '' }])),
    elements: new Map(),
    startedAt: Date.now(),
    summary: null
  }
  showScreen(4, savingScreen())
  const result = await api.startSave({ folder: state.folder, forPlex: state.forPlex, episodeIds: order.map(episode => episode.id) })
  state.save.summary = result.summary ?? null
  state.save.error = result.error ?? null
  playFinishSound(state.save.summary)
  showScreen(4, finishedScreen())
}

const playFinishSound = (summary) => {
  if (!summary) return sounds.play('failed')
  if (summary.stopped > 0) return
  sounds.play(summary.notSaved > 0 ? 'failed' : 'finished')
}

const stopSave = async (button) => {
  button.disabled = true
  button.textContent = 'Stopping…'
  await api.cancelSave()
}

const welcomeScreen = () => ({
  content: [
    h('section', { className: 'welcome' },
      animatedMark(),
      h('h1', {}, 'Fetch My Recordings'),
      h('p', { className: 'lead' }, 'Copy the recordings on your Fetch box to this computer.'),
      h('button', { type: 'button', className: 'link', id: 'about-app', onclick: openAbout }, 'About this app'))
  ],
  actions: [
    h('span', { className: 'spacer' }),
    h('button', { type: 'button', className: 'btn primary', id: 'start', onclick: begin }, 'Start'),
    h('span', { className: 'spacer' })
  ]
})

const openAbout = () => openInfo({
  title: 'About this app',
  body: [
    h('ol', { className: 'how' },
      h('li', {}, 'Find the Fetch box on your home network'),
      h('li', {}, 'Choose the recordings you want'),
      h('li', {}, 'Choose a folder, then save')),
    h('p', {}, 'Free and open source, built on the fetchtv command-line tool. It talks only to your Fetch box, on your home network. It needs no Fetch account, and it deletes nothing from the box.'),
    h('p', { id: 'independence' }, 'Fetch My Recordings is an independent, free tool made by a Fetch customer. It is not made, endorsed, or supported by Fetch TV. Fetch is a trademark of its owner.'),
    h('p', { className: 'meta' }, `Version ${state.info.version}. Licence GPL-3.0-or-later. GitHub mark from GitHub Octicons, MIT licence.`),
    h('p', {}, githubLink())
  ]
})

const searchingScreen = () => ({
  content: [
    h('h1', {}, 'Looking for your Fetch box'),
    h('div', { className: 'progress is-busy searching', role: 'progressbar', 'aria-label': 'Searching' }, h('span', {}))
  ]
})

const notFoundScreen = () => ({
  content: [
    illustration('no-box'),
    headingWithInfo({ text: 'No Fetch box found', info: notFoundInfo }),
    h('p', {}, h('button', { type: 'button', className: 'btn', onclick: () => searchForBoxes({ autoPick: true }) }, 'Search again')),
    addressForm()
  ]
})

const notFoundInfo = () => openInfo({
  title: 'No Fetch box found',
  body: [
    h('p', {}, 'Check that:'),
    h('ul', { className: 'checks' },
      h('li', {}, 'the Fetch box is turned on'),
      h('li', {}, 'this computer is on the same Wi-Fi or home network as the Fetch box')),
    h('p', {}, 'Then search again, or type the IP address of the box.')
  ]
})

const chooseBoxScreen = () => ({
  content: [
    h('h1', {}, state.boxes.length > 1 ? 'Choose your Fetch box' : 'Fetch box found'),
    h('ul', { className: 'table' }, ...state.boxes.map(box => h('li', { className: 'row' },
      h('button', { type: 'button', className: 'choice', onclick: () => useBox(box) },
        h('span', { className: 'box-dot' }),
        h('span', { className: 'row-text' },
          h('span', { className: 'row-title' }, box.name),
          h('span', { className: 'row-detail mono' }, box.address)))))),
    h('p', {}, h('button', { type: 'button', className: 'link', onclick: () => searchForBoxes({ autoPick: false }) }, 'Search again')),
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
  const form = h('form', { className: 'panel address-form', novalidate: true },
    h('div', { className: 'field' },
      h('span', { className: 'label-row' },
        h('label', { htmlFor: 'box-address' }, 'Or type its IP address'),
        infoButton({ label: 'Where to find the IP address', onclick: ipInfo })),
      input,
      h('button', { type: 'submit', className: 'btn' }, 'Connect')),
    h('p', { className: 'form-message', role: 'alert' }))
  form.addEventListener('submit', (event) => {
    event.preventDefault()
    connectByAddress({ address: input.value, form })
  })
  return form
}

const ipInfo = () => openInfo({
  title: 'The IP address of your Fetch box',
  body: [
    h('p', {}, 'It is four numbers with dots between them, such as 192.168.1.50.'),
    h('p', {}, 'Find it in your router’s list of connected devices, or in the network details of the Fetch box’s settings menu.')
  ]
})

const loadingListScreen = () => ({
  content: [
    boxCard(),
    h('h1', {}, 'Reading your recordings'),
    h('p', { className: 'meta', id: 'list-progress' }, ''),
    h('div', { className: 'progress is-busy searching', role: 'progressbar', 'aria-label': 'Reading' }, h('span', {}))
  ]
})

const showListProgress = ({ done, total }) => {
  const line = document.getElementById('list-progress')
  if (line && total > 0) line.textContent = `${done} of ${total} shows`
}

const emptyListScreen = () => ({
  content: [
    boxCard(),
    illustration('no-recordings'),
    headingWithInfo({ text: 'No recordings on this box', info: emptyInfo }),
    h('p', {}, h('button', { type: 'button', className: 'btn', onclick: () => useBox(state.box) }, 'Check again'))
  ]
})

const emptyInfo = () => openInfo({
  title: 'No recordings on this box',
  body: [
    h('p', {}, 'The box answered, but it lists no recordings to copy.'),
    h('p', { id: 'cancelled-account' }, 'A Fetch box whose account is cancelled shows its setup screen on the TV and lists no recordings.')
  ]
})

const listFailedScreen = () => ({
  content: [
    boxCard(),
    h('h1', {}, 'Could not read the recordings'),
    h('p', {}, h('button', { type: 'button', className: 'btn', onclick: () => useBox(state.box) }, 'Try again'))
  ]
})

const boxCard = () => h('div', { className: 'box-card', id: 'box-card' },
  h('span', { className: 'box-dot' }),
  h('span', { className: 'box-text' },
    h('span', { className: 'box-name' }, state.box.name),
    h('span', { className: 'box-address mono' }, state.box.address)),
  h('button', { type: 'button', className: 'link', onclick: () => searchForBoxes({ autoPick: false }) }, 'Change'))

const recordingsScreen = () => {
  const summary = h('span', { className: 'summary', id: 'selection-summary' })
  const next = h('button', { type: 'button', className: 'btn primary', id: 'next-to-folder', onclick: goToFolderStep }, 'Next')
  const allButton = h('button', { type: 'button', className: 'segment', onclick: () => { selectAll(); refreshAll() } }, 'Select all')
  const noneButton = h('button', { type: 'button', className: 'segment', onclick: () => { state.selected.clear(); refreshAll() } }, 'Select none')
  const rows = state.shows.map(showRow)
  const refreshAll = () => {
    rows.forEach(row => row.refresh())
    refreshSelectionSummary({ summary, next, allButton, noneButton })
  }
  rows.forEach(row => { row.onChange = refreshAll })
  refreshSelectionSummary({ summary, next, allButton, noneButton })
  return {
    content: [
      boxCard(),
      h('h1', {}, 'Choose recordings'),
      h('p', { className: 'meta', id: 'saved-summary' }, savedSummaryLine()),
      h('div', { className: 'toolbar' }, h('div', { className: 'segmented', role: 'group', 'aria-label': 'Selection' }, allButton, noneButton)),
      h('ul', { className: 'table', id: 'show-list' }, ...rows.map(row => row.element))
    ],
    actions: [
      summary,
      h('button', { type: 'button', className: 'btn quiet', onclick: () => searchForBoxes({ autoPick: false }) }, 'Back'),
      next
    ]
  }
}

const savedSummaryLine = () => {
  const episodes = savableEpisodes(state.shows)
  const count = (savedState) => episodes.filter(episode => savedStateOf(episode) === savedState).length
  return [
    `${count('new')} new`,
    count('saved') > 0 && `${count('saved')} already saved`,
    count('missing') > 0 && `${count('missing')} moved or deleted`
  ].filter(Boolean).join(' · ')
}

const showRow = (show) => {
  const savable = show.episodes.filter(episode => !episode.stillRecording)
  const checkbox = h('input', { type: 'checkbox', disabled: savable.length === 0, 'aria-label': show.title })
  const row = { onChange: () => {} }
  const episodeRows = show.episodes.map(episode => episodeRow({ episode, onChange: () => row.onChange() }))
  const drawer = h('ul', { className: 'drawer', hidden: !state.open.has(show.id) }, ...episodeRows.map(item => item.element))
  const side = h('span', { className: 'row-side-value mono' })
  const reveal = h('span', { className: 'row-reveal' })
  const detail = h('span', { className: 'row-detail' })
  const main = h('div', { className: 'row-main is-clickable', role: 'button', tabindex: '0', 'aria-expanded': String(state.open.has(show.id)) },
    h('label', { className: 'check', onclick: (event) => event.stopPropagation() }, checkbox),
    h('span', { className: 'row-text' }, h('span', { className: 'row-title' }, show.title), detail),
    h('span', { className: 'row-side' }, reveal, h('br', {}), side))

  const toggle = () => toggleDrawer({ show, drawer, main, reveal })
  main.addEventListener('click', toggle)
  main.addEventListener('keydown', (event) => {
    if (event.target !== main || (event.key !== 'Enter' && event.key !== ' ')) return
    event.preventDefault()
    toggle()
  })

  checkbox.addEventListener('change', () => {
    savable.forEach(episode => checkbox.checked ? state.selected.add(episode.id) : state.selected.delete(episode.id))
    row.onChange()
  })

  row.refresh = () => {
    const chosen = savable.filter(episode => state.selected.has(episode.id)).length
    checkbox.checked = chosen > 0 && chosen === savable.length
    checkbox.indeterminate = chosen > 0 && chosen < savable.length
    side.textContent = `${chosen} of ${show.episodes.length}`
    detail.textContent = showDetail(show)
    reveal.textContent = state.open.has(show.id) ? 'Hide' : 'Show'
    episodeRows.forEach(item => item.refresh())
  }
  row.refresh()

  row.element = h('li', { className: 'row show-row', 'data-show': show.title }, main, drawer)
  return row
}

const episodeRow = ({ episode, onChange }) => {
  const checkbox = h('input', { type: 'checkbox', disabled: episode.stillRecording, 'aria-label': episode.title })
  checkbox.addEventListener('change', () => {
    checkbox.checked ? state.selected.add(episode.id) : state.selected.delete(episode.id)
    onChange()
  })
  const savedState = savedStateOf(episode)
  const main = h('div', { className: `row-main${episode.stillRecording ? '' : ' is-clickable'}`, 'data-saved': savedState },
    h('label', { className: 'check', onclick: (event) => event.stopPropagation() }, checkbox),
    h('span', { className: 'row-text' },
      h('span', { className: 'row-title' }, episode.title),
      h('span', { className: 'row-detail' }, episodeDetail(episode))),
    episodeSide({ episode, savedState }))
  main.addEventListener('click', () => { if (!checkbox.disabled) checkbox.click() })
  return {
    element: h('li', { className: 'row episode-row', 'data-episode': episode.id }, main),
    refresh: () => { checkbox.checked = state.selected.has(episode.id) }
  }
}

const episodeDetail = (episode) => (episode.stillRecording ? 'Still recording' : formatDuration(episode.duration))

const episodeSide = ({ episode, savedState }) => {
  if (episode.stillRecording) return h('span', { className: 'row-side' })
  if (savedState === 'saved') return h('span', { className: 'row-side is-saved' }, 'Saved')
  if (savedState === 'missing') {
    return h('span', { className: 'row-side' }, h('button', {
      type: 'button',
      className: 'status-button is-warning',
      onclick: (event) => {
        event.stopPropagation()
        missingInfo(episode)
      }
    }, 'Moved or deleted'))
  }
  return h('span', { className: 'row-side mono' }, formatBytes(episode.size))
}

const missingInfo = (episode) => {
  const { path: filePath, savedAt } = episode.saved
  openInfo({
    title: 'Moved or deleted',
    body: [
      h('p', { id: 'missing-detail' }, `Saved ${savedDate(savedAt)}to ${lastParts(filePath, 2)}. It is no longer there.`),
      h('p', { className: 'example selectable' }, filePath),
      h('p', { className: 'meta' }, 'It is ticked, so Save copies it again.')
    ]
  })
}

const savedDate = (iso) => (iso ? `on ${new Date(iso).toLocaleDateString('en-AU', { day: 'numeric', month: 'short', year: 'numeric' })} ` : '')

const lastParts = (filePath, count) => filePath.split(/[\\/]/).filter(Boolean).slice(-count).join('/')

const savedStateOf = (episode) => episode.saved?.state ?? 'new'

const isWanted = (episode) => savedStateOf(episode) !== 'saved'

const toggleDrawer = ({ show, drawer, main, reveal }) => {
  const isOpen = state.open.has(show.id)
  isOpen ? state.open.delete(show.id) : state.open.add(show.id)
  drawer.hidden = isOpen
  main.setAttribute('aria-expanded', String(!isOpen))
  reveal.textContent = isOpen ? 'Show' : 'Hide'
}

const refreshSelectionSummary = ({ summary, next, allButton, noneButton }) => {
  const all = savableEpisodes(state.shows)
  const chosen = all.filter(episode => state.selected.has(episode.id))
  next.disabled = chosen.length === 0
  allButton.setAttribute('aria-pressed', String(chosen.length === all.length && all.length > 0))
  noneButton.setAttribute('aria-pressed', String(chosen.length === 0))
  summary.replaceChildren(...(chosen.length === 0
    ? ['Nothing selected']
    : [h('strong', {}, `${plural(chosen.length, 'recording')} selected`), ' · ', h('span', { className: 'mono' }, formatBytes(sumSizes(chosen)))]))
}

const selectAll = () => savableEpisodes(state.shows).forEach(episode => state.selected.add(episode.id))

const showDetail = (show) => {
  const savable = show.episodes.filter(episode => !episode.stillRecording)
  const saved = savable.filter(episode => savedStateOf(episode) === 'saved').length
  if (savable.length > 0 && saved === savable.length) return 'All saved'
  return `${plural(show.episodes.length, 'recording')} · ${formatBytes(sumSizes(show.episodes))}`
}

const folderScreen = () => {
  const plexSwitch = h('button', {
    type: 'button',
    role: 'switch',
    className: 'toggle',
    id: 'plex-switch',
    'aria-checked': String(state.forPlex),
    'aria-labelledby': 'plex-label'
  }, h('span', { className: 'toggle-track' }, h('span', { className: 'toggle-knob' })))
  plexSwitch.addEventListener('click', () => {
    state.forPlex = !state.forPlex
    plexSwitch.setAttribute('aria-checked', String(state.forPlex))
  })
  return {
    content: [
      h('h1', {}, 'Choose a folder'),
      h('div', { className: 'panel' },
        h('div', { className: 'folder' },
          folderLabel(),
          infoButton({ label: 'Full path of the folder', onclick: folderInfo }),
          h('button', { type: 'button', className: 'btn', id: 'change-folder', onclick: changeFolder }, 'Change')),
        h('div', { id: 'folder-info', 'aria-live': 'polite' })),
      h('div', { className: 'setting' },
        plexSwitch,
        h('span', { className: 'setting-title', id: 'plex-label', onclick: () => plexSwitch.click() }, 'Names for Plex, Jellyfin or Kodi'),
        infoButton({ label: 'About names for Plex, Jellyfin or Kodi', onclick: plexInfo }))
    ],
    actions: [
      h('span', { className: 'spacer' }),
      h('button', { type: 'button', className: 'btn quiet', onclick: () => showScreen(2, recordingsScreen()) }, 'Back'),
      h('button', { type: 'button', className: 'btn primary', id: 'start-save', onclick: beginSave }, 'Save')
    ]
  }
}

const folderLabel = () => h('span', { className: 'folder-label', id: 'folder-path', title: state.folder, 'data-path': state.folder },
  h('span', { className: 'folder-name' }, lastParts(state.folder, 1)),
  h('span', { className: 'folder-parent' }, `in ${lastParts(parentOf(state.folder), 1) || 'this computer'}`))

const parentOf = (folder) => folder.replace(/[\\/]+$/, '').replace(/[\\/][^\\/]*$/, '')

const folderInfo = () => openInfo({
  title: 'Folder',
  body: [h('p', { className: 'example selectable', id: 'folder-full-path' }, state.folder)]
})

const plexInfo = () => openInfo({
  title: 'Names for Plex, Jellyfin or Kodi',
  body: [
    h('p', {}, 'Media library apps need a folder for each season and the season and episode number in each file name. Turn this on if you watch recordings in one of them.'),
    h('p', {}, 'On'),
    h('p', { className: 'example', id: 'example-on' }, layoutExample({ forPlex: true })),
    h('p', {}, 'Off'),
    h('p', { className: 'example', id: 'example-off' }, layoutExample({ forPlex: false })),
    h('p', { className: 'meta' }, 'A recording without a season and episode number, such as a film, keeps its own name.')
  ]
})

const renderFolderInfo = () => {
  const box = document.getElementById('folder-info')
  if (!box) return
  const info = state.folderInfo
  const startButton = document.getElementById('start-save')
  if (!info) {
    box.replaceChildren(h('p', { className: 'meta space' }, 'Checking free space…'))
    if (startButton) startButton.disabled = true
    return
  }
  if (startButton) startButton.disabled = !info.canWrite
  box.replaceChildren(...folderInfoLines(info))
}

const folderInfoLines = (info) => {
  if (!info.canWrite) return [h('p', { className: 'notice', role: 'alert' }, 'Saving to this folder is not allowed.')]
  const free = info.freeBytes === null ? 'unknown' : formatBytes(info.freeBytes)
  const parts = [
    h('span', {}, 'Needs ', h('strong', {}, formatBytes(info.neededBytes))),
    h('span', {}, 'Free ', h('strong', {}, free)),
    info.alreadySavedCount > 0 ? h('span', { id: 'already-here' }, h('strong', {}, String(info.alreadySavedCount)), ' already here') : null
  ]
  const lines = [h('div', { className: 'space' }, ...parts)]
  if (info.freeBytes !== null && info.neededBytes > info.freeBytes) {
    lines.push(h('p', { className: 'notice', role: 'alert' }, 'Not enough space for all of these.'))
  }
  return lines
}

const layoutExample = ({ forPlex }) => {
  const episodes = savableEpisodes(state.shows).filter(item => state.selected.has(item.id))
  const episode = episodes.find(item => /S(?:eason)?\s*\d{1,3}\s*E(?:pisode)?\s*\d{1,3}/i.test(item.title)) ?? episodes[0]
  const show = state.shows.find(item => item.episodes.includes(episode))
  if (!episode || !show) return ''
  const numbers = episode.title.match(/S(?:eason)?\s*(\d{1,3})\s*E(?:pisode)?\s*(\d{1,3})/i)
  if (forPlex && numbers) {
    const season = numbers[1].padStart(2, '0')
    const number = numbers[2].padStart(2, '0')
    return `${show.title}/Season ${season}/${show.title} - S${season}E${number}.ts`
  }
  return `${show.title}/${episode.title}.ts`
}

const savingScreen = () => {
  const list = h('ul', { className: 'table' }, ...state.save.order.map(episode => saveRow(episode)))
  const stopButton = h('button', { type: 'button', className: 'btn stop', id: 'stop-save' }, 'Stop')
  stopButton.addEventListener('click', () => stopSave(stopButton))
  return {
    content: [
      headingWithInfo({ text: 'Saving', info: savingInfo }),
      h('div', { className: 'overall' },
        h('span', { className: 'overall-percent', id: 'overall-percent' }, '0%'),
        h('span', { className: 'overall-text', id: 'overall-line' }, '')),
      h('div', { className: 'progress is-overall', role: 'progressbar', 'aria-label': 'All recordings', id: 'overall-bar' }, h('span', {})),
      list
    ],
    actions: [h('span', { className: 'spacer' }), stopButton],
    after: refreshOverall
  }
}

const savingInfo = () => openInfo({
  title: 'While it saves',
  body: [
    h('p', {}, 'This computer stays awake until the save finishes. Keep it on the same network as the Fetch box.'),
    h('p', {}, 'If you stop, a recording that is part way keeps what it has. Save the same recordings to the same folder again to carry on from there.')
  ]
})

const saveRow = (episode) => {
  const status = h('span', { className: 'status' }, 'Waiting')
  const bar = h('div', { className: 'progress', hidden: true }, h('span', {}))
  state.save.elements.set(episode.id, { status, bar })
  const show = state.shows.find(item => item.episodes.includes(episode))
  return h('li', { className: 'row save-row', 'data-episode': episode.id },
    h('div', { className: 'row-main' },
      h('span', { className: 'row-text' },
        h('span', { className: 'row-title' }, `${show?.title ?? ''}: ${episode.title}`),
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
    elements.status.className = 'status is-saving'
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
  const share = percent(done, total)
  document.getElementById('overall-percent').textContent = `${Math.floor(share)}%`
  const eta = timeLeft({ rows, total, done })
  line.textContent = [`${finished} of ${rows.length} done`, `${formatBytes(done)} of ${formatBytes(total)}`, eta].filter(Boolean).join(' · ')
  bar.firstChild.style.width = `${share}%`
  bar.setAttribute('aria-valuenow', String(Math.round(share)))
}

const timeLeft = ({ rows, total, done }) => {
  const elapsed = (Date.now() - state.save.startedAt) / 1000
  const copied = rows.reduce((sum, row) => sum + (row.firstBytes === null ? 0 : Math.max(0, Math.min(row.bytes, row.total) - row.firstBytes)), 0)
  if (elapsed < 10 || copied <= 0) return ''
  const seconds = (total - done) / (copied / elapsed)
  if (seconds < 60) return 'under a minute left'
  const minutes = Math.round(seconds / 60)
  if (minutes < 60) return `about ${minutes} min left`
  return `about ${Math.floor(minutes / 60)} hr ${minutes % 60} min left`
}

const finishedScreen = () => {
  const summary = state.save.summary
  if (!summary) return saveErrorScreen()
  const remaining = summary.notSaved + summary.stopped
  const title = summary.stopped > 0 ? 'Stopped' : remaining > 0 ? 'Some not saved' : 'All done'
  const problemRows = state.save.order.filter(episode => ['failed', 'locked', 'still_recording', 'cancelled'].includes(state.save.rows.get(episode.id)?.status))
  return {
    content: [
      headingWithInfo({ text: title, info: finishedInfo }),
      h('dl', { className: 'stats', id: 'save-totals' },
        stat({ label: 'Saved', value: summary.saved, tone: 'is-good' }),
        stat({ label: 'Already saved', value: summary.alreadySaved }),
        stat({ label: 'Not saved', value: summary.notSaved, tone: summary.notSaved > 0 ? 'is-bad' : '' }),
        summary.stopped > 0 ? stat({ label: 'Stopped', value: summary.stopped }) : null),
      problemRows.length > 0
        ? h('ul', { className: 'table', id: 'problem-list' }, ...problemRows.map(episode => problemRow(episode)))
        : null
    ],
    actions: [
      h('button', { type: 'button', className: `btn${remaining > 0 ? '' : ' primary'}`, id: 'open-folder', onclick: () => api.openSavedFolder() }, 'Open folder'),
      h('span', { className: 'spacer' }),
      h('button', { type: 'button', className: 'btn quiet', onclick: () => useBox(state.box) }, 'Choose other recordings'),
      h('button', { type: 'button', className: `btn${remaining > 0 ? ' primary' : ''}`, id: 'save-again', onclick: beginSave }, 'Save again')
    ]
  }
}

const finishedInfo = () => openInfo({
  title: 'Your recordings',
  body: [
    h('p', {}, 'They are in this folder:'),
    h('p', { className: 'example' }, state.folder),
    h('p', {}, 'They play in VLC, a free video player, or in a media library app such as Plex, Jellyfin or Kodi.'),
    h('p', {}, 'Save again skips the recordings that are already saved and carries on with the rest.')
  ]
})

const stat = ({ label, value, tone = '' }) => h('div', { className: `stat ${tone}` }, h('dt', {}, label), h('dd', {}, String(value)))

const saveErrorScreen = () => ({
  content: [
    h('h1', {}, 'Could not save here'),
    h('p', { className: 'lead' }, state.save.error === 'no-space' ? 'The folder is full.' : 'Choose another folder.')
  ],
  actions: [
    h('span', { className: 'spacer' }),
    h('button', { type: 'button', className: 'btn primary', onclick: goToFolderStep }, 'Choose a folder')
  ]
})

const problemRow = (episode) => {
  const row = state.save.rows.get(episode.id)
  const show = state.shows.find(item => item.episodes.includes(episode))
  const reason = h('p', { className: 'drawer-text', hidden: true }, row.reason || 'Not started.')
  const reveal = h('span', { className: 'row-reveal' }, 'Why')
  const main = h('div', { className: 'row-main is-clickable', role: 'button', tabindex: '0', 'aria-expanded': 'false' },
    h('span', { className: 'row-text' },
      h('span', { className: 'row-title' }, `${show?.title ?? ''}: ${episode.title}`),
      h('span', { className: 'row-detail' }, statusLabel(row))),
    h('span', { className: 'row-side' }, reveal))
  const toggle = () => {
    reason.hidden = !reason.hidden
    main.setAttribute('aria-expanded', String(!reason.hidden))
    reveal.textContent = reason.hidden ? 'Why' : 'Hide'
  }
  main.addEventListener('click', toggle)
  main.addEventListener('keydown', (event) => {
    if (event.key !== 'Enter' && event.key !== ' ') return
    event.preventDefault()
    toggle()
  })
  return h('li', { className: 'row problem-row' }, main, reason)
}

const statusLabel = (row) => {
  if (row.status === 'saved') return 'Saved'
  if (row.status === 'already_saved') return 'Already saved'
  if (row.status === 'cancelled') return row.reason === 'Not started.' ? 'Not started' : 'Stopped'
  return 'Not saved'
}

const statusClass = (status) => {
  if (status === 'saved' || status === 'already_saved') return 'saved'
  if (status === 'cancelled') return ''
  return 'failed'
}

const isFinal = (status) => status !== 'waiting' && status !== 'saving'

const headingWithInfo = ({ text, info }) => h('div', { className: 'heading-row' },
  h('h1', {}, text),
  infoButton({ label: `More about: ${text}`, onclick: info }))

const infoButton = ({ label, onclick }) => h('button', { type: 'button', className: 'info-button', 'aria-label': label, title: label, onclick }, 'i')

const setUpDialog = () => {
  const dialog = document.getElementById('info-dialog')
  dialog.addEventListener('click', (event) => { if (event.target === dialog) dialog.close() })
  document.getElementById('info-close').addEventListener('click', () => dialog.close())
}

const openInfo = ({ title, body }) => {
  const dialog = document.getElementById('info-dialog')
  const opener = document.activeElement
  document.getElementById('info-title').textContent = title
  document.getElementById('info-body').replaceChildren(...body.filter(Boolean))
  dialog.addEventListener('close', () => opener?.focus?.(), { once: true })
  dialog.showModal()
  document.getElementById('info-close').focus()
}

const showScreen = (step, screen) => {
  state.step = step
  document.body.classList.toggle('is-welcome', step === 0)
  document.querySelectorAll('#steps li').forEach(item => {
    const itemStep = Number(item.dataset.step)
    item.classList.toggle('is-done', itemStep < step)
    if (itemStep === step) item.setAttribute('aria-current', 'step')
    else item.removeAttribute('aria-current')
  })
  const view = document.getElementById('view')
  view.replaceChildren(...screen.content.filter(Boolean))
  view.scrollTop = 0
  const actions = (screen.actions ?? []).filter(Boolean)
  document.getElementById('actions').replaceChildren(...(actions.length ? [h('div', { className: 'actions-inner' }, ...actions)] : []))
  if (screen.after) screen.after()
  view.focus({ preventScroll: true })
}

const PROJECT_URL = 'https://github.com/furey/fetchtv'
const GITHUB_MARK_PATH = 'M6.766 11.328c-2.063-.25-3.516-1.734-3.516-3.656 0-.781.281-1.625.75-2.188-.203-.515-.172-1.609.063-2.062.625-.078 1.468.25 1.968.703.594-.187 1.219-.281 1.985-.281.765 0 1.39.094 1.953.265.484-.437 1.344-.765 1.969-.687.218.422.25 1.515.046 2.047.5.593.766 1.39.766 2.203 0 1.922-1.453 3.375-3.547 3.64.531.344.89 1.094.89 1.954v1.625c0 .468.391.734.86.547C13.781 14.359 16 11.53 16 8.03 16 3.61 12.406 0 7.984 0 3.563 0 0 3.61 0 8.031a7.88 7.88 0 0 0 5.172 7.422c.422.156.828-.125.828-.547v-1.25c-.219.094-.5.156-.75.156-1.031 0-1.64-.562-2.078-1.609-.172-.422-.36-.672-.719-.719-.187-.015-.25-.093-.25-.187 0-.188.313-.328.625-.328.453 0 .844.281 1.25.86.313.452.64.655 1.031.655s.641-.14 1-.5c.266-.265.47-.5.657-.656'

const githubLink = () => {
  const icon = document.createElementNS('http://www.w3.org/2000/svg', 'svg')
  icon.setAttribute('viewBox', '0 0 16 16')
  icon.setAttribute('width', '16')
  icon.setAttribute('height', '16')
  icon.setAttribute('fill', 'currentColor')
  icon.setAttribute('aria-hidden', 'true')
  const mark = document.createElementNS('http://www.w3.org/2000/svg', 'path')
  mark.setAttribute('d', GITHUB_MARK_PATH)
  icon.append(mark)
  return h('button', {
    type: 'button',
    className: 'github-link',
    'aria-label': 'fetchtv on GitHub',
    title: PROJECT_URL,
    onclick: () => api.openProjectPage()
  }, icon, h('span', {}, 'furey/fetchtv'))
}

const SVG_NS = 'http://www.w3.org/2000/svg'
const MARK_CHIPS = [
  { name: 'blue', y: 11, fill: '#3ba4ff' },
  { name: 'orange', y: 23, fill: '#f28c28' },
  { name: 'yellow', y: 35, fill: '#f5c542' }
]
const MARK_TRAY = 'M9 28V44Q9 50 15 50H49Q55 50 55 44V28'

const svgElement = (tag, attributes) => {
  const element = document.createElementNS(SVG_NS, tag)
  Object.entries(attributes).forEach(([key, value]) => element.setAttribute(key, String(value)))
  return element
}

const animatedMark = () => {
  const svg = svgElement('svg', { viewBox: '0 0 64 64', width: 104, height: 104, class: 'mark is-large is-animated', role: 'img', 'aria-label': 'Fetch My Recordings', id: 'welcome-mark' })
  const chips = svgElement('g', { class: 'mark-chips' })
  MARK_CHIPS.forEach(chip => chips.append(svgElement('rect', { x: 16, y: chip.y, width: 32, height: 8, rx: 2.5, fill: chip.fill, class: `mark-chip is-${chip.name}` })))
  svg.append(chips)
  svg.append(svgElement('path', { d: MARK_TRAY, fill: 'none', stroke: '#5ce1c6', 'stroke-width': 6, 'stroke-linecap': 'round', 'stroke-linejoin': 'round', class: 'mark-tray' }))
  return svg
}

const pauseWhenHidden = () => {
  const apply = () => document.body.classList.toggle('is-hidden', document.visibilityState === 'hidden')
  document.addEventListener('visibilitychange', apply)
  apply()
}

const markElement = ({ large = false } = {}) => h('img', { className: `mark${large ? ' is-large' : ''}`, src: 'mark.svg', alt: '', width: large ? 104 : 36, height: large ? 104 : 36 })

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
  if (key.includes('-') || key === 'role' || key === 'novalidate' || key === 'tabindex') return element.setAttribute(key, String(value))
  element[key] = value
}

start()
