#!/usr/bin/env node

import os from 'os'
import fsc from 'fs'
import _ from 'lodash'
import path from 'path'
import axios from 'axios'
import { URL } from 'url'
import yargs from 'yargs'
import chalk from 'chalk'
import fs from 'fs/promises'
import debugLib from 'debug'
import Table from 'cli-table3'
import prettyMs from 'pretty-ms'
import { filesize } from 'filesize'
import cliProgress from 'cli-progress'
import { hideBin } from 'yargs/helpers'
import { XMLParser, XMLValidator } from 'fast-xml-parser'

import NodeSsdp from 'node-ssdp'
const { Client: SsdpClient } = NodeSsdp

const debug = debugLib('fetchtv')
const debugXml = debugLib('fetchtv:xml')
const debugNetwork = debugLib('fetchtv:network')
const debugTemplate = debugLib('fetchtv:template')

let argv = {}
let activeMultiBar = null
let isShuttingDown = false
let progressBarActive = false
const requestCache = new Map()
const activeLockFiles = new Set()

const BROWSE_RETRIES = 3
const MAX_FILENAME = 255
const FETCHTV_PORT = 49152
const CONST_LOCK = '.lock'
const MIN_BROWSE_DELAY = 50
const NO_NUMBER_DEFAULT = ''
const REQUEST_TIMEOUT = 15000
const DISCOVERY_TIMEOUT = 3000
const BROWSE_RETRY_DELAY = 1500
const MAX_CONCURRENT_BROWSE = 5
const ADAPTIVE_DELAY_FACTOR = 1.5
const INITIAL_BROWSE_CONCURRENCY = 3
const SAVE_FILE_NAME = 'fetchtv.json'
const MAX_OCTET_RECORDING = 4398046510080
const FETCH_MANUFACTURER_URL = 'http://www.fetch.com/'
const MAX_CONCURRENT_DOWNLOADS = Math.min(os.cpus().length, 10)
const UPNP_CONTENT_DIRECTORY_URN = 'urn:schemas-upnp-org:service:ContentDirectory:1'

const REQUEST_QUEUE_PRIORITY = {
  ROOT: 0,
  SHOWS_FOLDER: 1,
  SHOW: 2,
  RECORDING_CHECK: 3
}

const httpClient = axios.create({
  timeout: REQUEST_TIMEOUT,
  maxContentLength: Infinity,
  keepAlive: true
})

const main = async () => {
  registerExitHandlers()

  argv = await yargs(hideBin(process.argv))
    .middleware(argv => {
      const command = argv._[0]
      if (!command) return
      const commandMatch = ['info', 'recordings', 'shows'].find(cmd => cmd.startsWith(command))
      if (commandMatch) argv._[0] = commandMatch
    })
    .command('info', 'Returns Fetch TV server details')
    .command('recordings', 'List episode recordings')
    .command('shows', 'List show titles and not the episodes within')
    .option('ip', { type: 'string', description: 'Specify the IP Address of the Fetch TV server' })
    .option('port', { type: 'number', default: FETCHTV_PORT, description: 'Specify the port of the Fetch TV server' })
    .option('show', { type: 'array', default: [], description: 'Filter recordings to show titles containing the specified text (repeatable)' })
    .option('exclude', { type: 'array', default: [], description: 'Filter recordings to show titles NOT containing the specified text (repeatable)' })
    .option('title', { type: 'array', default: [], description: 'Filter recordings to episode titles containing the specified text (repeatable)' })
    .option('is-recording', { type: 'boolean', description: 'Filter recordings to only those that are currently recording' })
    .option('save', { type: 'string', description: 'Save recordings to the specified path' })
    .option('template', { type: 'string', description: 'Template for save path/filename structure (uses --save as base path)'})
    .option('for-plex', { type: 'boolean', default: false, description: 'Use Plex-compatible template for saving recordings (overrides --template)' })
    .option('overwrite', { type: 'boolean', default: false, description: 'Overwrite existing files when saving' })
    .option('json', { type: 'boolean', default: false, description: 'Output show/recording/save results in JSON' })
    .option('debug', { type: 'boolean', default: false, description: 'Enable verbose logging for debugging'})
    .help()
    .alias('h', 'help')
    .alias('s', 'show')
    .alias('t', 'title')
    .alias('e', 'exclude')
    .alias('o', 'overwrite')
    .alias('j', 'json')
    .alias('d', 'debug')
    .epilog('Note: Comma-separated values for filters (--show, --exclude, --title) are NOT supported. Instead, repeat option as needed.\nTemplate Variables: ${show_title}, ${recording_title}, ${season_number[_padded]}, ${episode_number}, ${episode_number[_padded]}, ${ext}')
    .wrap(process.stdout.columns ? Math.min(process.stdout.columns, 135) : 135)
    .argv

  if (argv.debug) {
    debugLib.enable('fetchtv*')
    logWarning('*** Debug Mode Enabled ***')
    debug('Initial argv state: %O', argv)
  }

  const plexTemplate = '${show_title}/Season ${season_number}/${show_title} - S${season_number}E${episode_number_padded}.${ext}'
  let effectiveTemplate = argv.template

  if (argv.forPlex) {
    if (!argv.save) {
      logWarning('--for-plex option ignored because --save was not specified.')
    } else {
      if (argv.template && argv.template !== plexTemplate) {
        logWarning(`--for-plex overrides the provided --template option. Using Plex template.`)
      }
      effectiveTemplate = plexTemplate
      debug('Using --for-plex template: %s', effectiveTemplate)
    }
  }

  logHeading(`Started: ${new Date().toLocaleString()}`)

  const fetchServer = await discoverFetch({ ip: argv.ip, port: argv.port })
  if (!fetchServer) {
    logHeading(`Done (Discovery Failed): ${new Date().toLocaleString()}`)
    process.exit(1)
  }

  const hasInfoCommand = argv._[0] === 'info'
  const hasRecordingsCommand = argv._[0] === 'recordings'
  const hasShowsCommand = argv._[0] === 'shows'
  const wantsRecordingsAction = hasRecordingsCommand || hasShowsCommand || argv.isRecording || argv.save

  if (hasInfoCommand) printInfo(fetchServer)

  if (wantsRecordingsAction) {
    const filters = {
      folderFilter: processFilter(argv.show),
      excludeFilter: processFilter(argv.exclude),
      titleFilter: processFilter(argv.title),
      showsOnly: hasShowsCommand,
      isRecordingFilter: argv.isRecording
    }

    log(`Retrieving Fetch TV ${hasShowsCommand ? 'shows' : 'recordings'}…`)
    const recordings = await getFetchRecordings({ location: fetchServer, filters })

    if (!argv.save) {
      printRecordings({ recordings, jsonOutput: argv.json, showsOnly: hasShowsCommand })
    } else {
      await handleSaveAction({
        recordings,
        savePath: path.resolve(argv.save),
        template: effectiveTemplate,
        overwrite: argv.overwrite,
        jsonOutput: argv.json
      })
    }
  } else if (!hasInfoCommand) {
    logWarning('No action specified. Use info, recordings, shows, --is-recording, or --save. Use --help for options.')
  }

  logHeading(`Done: ${new Date().toLocaleString()}`)
}

const discoverFetch = async ({ ip, port }) => {
  const locations = new Set()

  if (ip) {
    locations.add(`http://${ip}:${port}/MediaServer.xml`)
  } else {
    log('Looking for Fetch TV servers…')
    const client = new SsdpClient()
    client.on('response', (headers) => {
      if (headers.LOCATION) locations.add(headers.LOCATION)
      debug('SSDP Response Headers: %O', headers)
    })

    try {
      client.search('ssdp:all')
      await new Promise(resolve => setTimeout(resolve, DISCOVERY_TIMEOUT))
      client.stop()
    } catch (err) {
      logError(`SSDP discovery failed: ${err.message}`)
      client.stop()
      return null
    }
  }

  if (locations.size === 0) {
    logError('Discovery failed: No UPnP devices found.')
    return null
  }

  const parsedLocations = await parseLocations([...locations])
  const fetchServer = parsedLocations.find(loc => loc.manufacturerURL === FETCH_MANUFACTURER_URL)

  if (!fetchServer) {
    logError('Discovery failed: Unable to locate Fetch TV UPnP service.')
    return null
  }

  const url = new URL(fetchServer.url)
  const hostname = url.hostname
  if (hostname) {
    log(`Fetch TV IP address: ${chalk.magentaBright(hostname)}`)
    if (!ip) log(chalk.gray(`Tip: Run future commands with "--ip=${hostname}" to skip server discovery.`))
  }

  log(`Device Description: ${chalk.magentaBright(fetchServer.url)}`)

  return fetchServer
}

const getFetchRecordings = async ({ location, filters }) => {
  const { folderFilter, excludeFilter, titleFilter, showsOnly, isRecordingFilter } = filters

  const apiService = await getApiService(location)
  if (!apiService) {
    logError('Could not find "ContentDirectory" service.')
    return []
  }

  const requestManager = createRequestManager({ debug: argv.debug })

  const baseFolders = await requestManager.enqueue(
    () => findDirectories({ apiService, objectId: '0' }),
    REQUEST_QUEUE_PRIORITY.ROOT
  )
  if (!baseFolders) {
    logError('Failed to browse root directory (ObjectID 0) after retries. Cannot list recordings.')
    return []
  }

  const recordingsFolder = baseFolders.find(f => f.title === 'Recordings')
  if (!recordingsFolder) {
    logWarning('No "Recordings" directory found in processed base directories.')
    return []
  }

  const showFolders = await requestManager.enqueue(
    () => findDirectories({ apiService, objectId: recordingsFolder.id }),
    REQUEST_QUEUE_PRIORITY.SHOWS_FOLDER
  )
  if (!showFolders) {
    logError(`Failed to browse the main "Recordings" directory (ObjectID ${recordingsFolder.id}) after retries. Cannot list shows.`)
    return []
  }

  const filteredShows = showFolders.filter(show => {
    const titleLower = show.title.toLowerCase()
    const include = !folderFilter.length || folderFilter.some(f => titleLower.includes(f))
    const exclude = excludeFilter.length && excludeFilter.some(e => titleLower.includes(e))
    return include && !exclude
  })

  if (showsOnly || filteredShows.length === 0)
    return filteredShows.map(show => ({ ...show, items: [] }))

  const totalShows = filteredShows.length
  let processedShows = 0

  log(`Processing ${totalShows} Fetch TV show directories…`)
  progressBarActive = true
  const progressBar = new cliProgress.SingleBar({
    format: `Shows Progress |${chalk.cyan('{bar}')}| {percentage}% | {value}/{total} Shows`,
    barCompleteChar: '\u2588',
    barIncompleteChar: '\u2591'
  }, cliProgress.Presets.shades_classic)

  progressBar.start(totalShows, 0)

  const batchSize = 5
  const results = []

  for (let i = 0; i < filteredShows.length; i += batchSize) {
    const batch = filteredShows.slice(i, i + batchSize)
    const batchPromises = batch.map(async show => {
      let items = await requestManager.enqueue(() => findItems({ apiService, objectId: show.id, showTitle: show.title }))

      if (titleFilter.length > 0)
        items = items.filter(item => titleFilter.some(t => item.title.toLowerCase().includes(t)))

      if (isRecordingFilter && items.length > 0) {
        const recordingItems = []
        for (const item of items) {
          const isRecording = await requestManager.enqueue(
            () => isCurrentlyRecording(item),
            REQUEST_QUEUE_PRIORITY.RECORDING_CHECK
          )
          if (isRecording) recordingItems.push(item)
        }
        items = recordingItems
      }

      processedShows++
      progressBar.update(processedShows)

      if (!items || items.length === 0) return null
      return { ...show, items }
    })

    const batchResults = await Promise.all(batchPromises)
    results.push(...batchResults.filter(Boolean))
  }

  progressBar.stop()
  progressBarActive = false

  return results
}

const saveRecordings = async ({ recordings, savePath, template, overwrite }) => {
  const savedFilesDb = await loadSavedFiles(savePath)
  const jsonResults = []
  const tasks = []

  log(`Preparing to save/resume up to ${recordings.flatMap(s => s.items || []).length} recording(s)...`)

  for (const show of recordings) {
    if (!show.items || show.items.length === 0) continue

    for (const item of show.items) {
      let filePath
      let showDirPath
      const templateData = {
        show_title: show.title,
        recording_title: item.title,
        season_number: item.season_number || '',
        season_number_padded: item.season_number_padded || '',
        episode_number: item.episode_number || '',
        episode_number_padded: item.episode_number_padded || '',
        ext: item.ext || 'ts'
      }

      if (template) {
        const relativePath = processPathTemplate({ templateString: template, data: templateData })
        filePath = path.resolve(savePath, relativePath)
        showDirPath = path.dirname(filePath)
      } else {
        const showDirName = createValidFilename(show.title)
        showDirPath = path.join(savePath, showDirName)
        const itemFileName = `${createValidFilename(templateData.recording_title)}.${templateData.ext || 'mpeg'}`
        filePath = path.join(showDirPath, itemFileName)
      }
      const lockFilePath = `${filePath}${CONST_LOCK}`

      let lockStillExistsAfterCheck = false
      if (fsc.existsSync(lockFilePath)) {
        const isStale = await isLockFileStale(lockFilePath)
        if (isStale) {
          log(chalk.yellow(`Removing stale lock file for ${item.title}`))
          try {
            fsc.unlinkSync(lockFilePath)
            debug('Removed stale lock file: %s', lockFilePath)
            lockStillExistsAfterCheck = false
          } catch (err) {
            debug('Error removing stale lock file: %O', err)
            logWarning(`Failed to remove stale lock file for ${item.title}, skipping item.`)
            lockStillExistsAfterCheck = true
          }
        } else {
          lockStillExistsAfterCheck = true
        }
      } else {
        lockStillExistsAfterCheck = false
      }

      if (lockStillExistsAfterCheck) {
        log(chalk.gray(`Skipping ${item.title} (lock file exists or failed to remove stale lock).`))
        if (argv.json) jsonResults.push({
          item: formatItem(item),
          recorded: false,
          warning: 'Skipped (lock file active or stale lock removal failed)'
        })
        continue
      }

      const isCompletedFile = savedFilesDb[item.id] && !overwrite
      let hasPartialFile = false
      try {
        if (fsc.existsSync(filePath) && (!savedFilesDb[item.id] || overwrite)) {
          const stats = await fs.stat(filePath)
          hasPartialFile = item.size > 0 && stats.size < item.size
        }
      } catch (err) {
        if (err.code !== 'ENOENT') {
          debug('Error checking for partial file %s: %O', filePath, err)
        }
        hasPartialFile = false
      }

      if (isCompletedFile && !hasPartialFile) {
        log(chalk.gray(`Skipping already saved: ${show.title} / ${item.title}`))
        if (argv.json) jsonResults.push({
          item: formatItem(item),
          recorded: false,
          warning: 'Skipped (already saved and not partial)'
        })
        continue
      }

      tasks.push({ item, filePath, showDirPath, showTitle: show.title })
    }
  }

  if (tasks.length === 0) {
    log('There is nothing new to record or resume.')
    return jsonResults
  }

  log(`Saving/Resuming ${tasks.length} recording${tasks.length > 1 ? 's' : ''}…`)

  progressBarActive = true
  const multiBar = new cliProgress.MultiBar({
    clearOnComplete: false,
    hideCursor: true,
    format: ' {bar} | {percentage}% | {filename}'
  }, cliProgress.Presets.shades_classic)

  activeMultiBar = multiBar
  const activePromises = new Set()

  for (const task of tasks) {
    try {
      await fs.mkdir(path.dirname(task.filePath), { recursive: true })
    } catch (mkdirErr) {
      logError(`Failed to create directory for ${task.item.title}: ${mkdirErr.message}`)
      jsonResults.push({
        item: formatItem(task.item),
        recorded: false,
        error: `Directory creation failed: ${mkdirErr.message}`
      })
      continue
    }

    while (activePromises.size >= MAX_CONCURRENT_DOWNLOADS) {
      await Promise.race(activePromises)
    }

    const progressBar = multiBar.create(task.item.size || 1, 0, {
      filename: chalk.whiteBright(path.basename(task.filePath).slice(0, 30).padEnd(30)),
      speed: 'N/A',
      eta: 'N/A',
      value: filesize(0),
      total: filesize(task.item.size || 0)
    })

    progressBar.options.format = `{filename} |${chalk.cyan('{bar}')}| {percentage}% | {value}/{total} | {speed}/s | ETA: {eta}`

    const promise = downloadFile({ item: task.item, filePath: task.filePath, progressBar, overwrite })
      .then(async (downloadResult) => {
        const resultEntry = {
          item: formatItem(task.item),
          recorded: downloadResult.recorded,
          resumed: downloadResult.resumed || false
        }

        if (downloadResult.warning) resultEntry.warning = downloadResult.warning
        if (downloadResult.error) resultEntry.error = downloadResult.error
        jsonResults.push(resultEntry)

        if (downloadResult.recorded) {
          await addSavedFile({ savePath, savedFilesDb, item: task.item })
        }
      })
      .catch((error) => {
        logError(`Unexpected error processing save result for ${task.item.title}: ${error.message}`)
        jsonResults.push({
          item: formatItem(task.item),
          recorded: false,
          error: `Processing error: ${error.message}`
        })

        try {
          const lockFilePath = `${task.filePath}${CONST_LOCK}`
          if (fsc.existsSync(lockFilePath)) {
            fsc.unlinkSync(lockFilePath)
            untrackLockFile(lockFilePath)
          }
        } catch (cleanupErr) {
          debug('Error cleaning lock file after promise error: %O', cleanupErr)
        }

        if (progressBar) progressBar.stop()
      })
      .finally(() => {
        activePromises.delete(promise)
      })

    activePromises.add(promise)
  }

  registerExitHandlers()

  try {
    await Promise.allSettled(activePromises)
  } finally {
    if (!isShuttingDown && activeMultiBar) {
      multiBar.stop()
      activeMultiBar = null
      progressBarActive = false
    }
  }

  return jsonResults
}

const printInfo = (fetchServer) => {
  const table = new Table({ head: [chalk.cyan('Field'), chalk.cyan('Value')] })
  table.push(
    { 'Type': fetchServer.deviceType || 'N/A' },
    { 'Name': fetchServer.friendlyName || 'N/A' },
    { 'Manufacturer': fetchServer.manufacturer || 'N/A' },
    { 'Manufacturer URL': fetchServer.manufacturerURL || 'N/A' },
    { 'Model': fetchServer.modelName || 'N/A' },
    { 'Model Desc': fetchServer.modelDescription || 'N/A' },
    { 'Model No': fetchServer.modelNumber || 'N/A' }
  )
  log(table.toString())
}

const printRecordings = ({ recordings, jsonOutput, showsOnly }) => {
  const sortedRecordings = sortRecordingsByTitle(recordings)

  if (jsonOutput) {
    const output = sortedRecordings.map(rec => {
      const item = { id: rec.id, title: rec.title }
      if (!showsOnly) item.items = rec.items?.map(formatItem)
      return item
    })

    logHeading('Start JSON Output', 'greenBright')
    console.log(JSON.stringify(output, null, 2))
    return logHeading('End JSON Output', 'greenBright')
  }

  const context = showsOnly ? 'Shows' : 'Recordings'
  logHeading(`Listing ${context}`)

  if (!sortedRecordings || sortedRecordings.length === 0)
    return logWarning(`No ${context} found matching criteria!`)

  sortedRecordings.forEach(recording => {
    const bullet = showsOnly ? '' : '📁 '
    log(chalk.green(`${bullet}${recording.title}`))
    if (!recording.items || recording.items.length === 0) {
      if (!showsOnly) log(chalk.gray('  (No items listed based on current filters)'))
      return
    }

    recording.items.forEach(item => {
      const durationStr = new Date(item.duration * 1000).toISOString().substr(11, 8)
      const sizeFormatted = filesize(item.size)
      log(`  ${chalk.whiteBright(item.title)} ${chalk.gray(`${durationStr} ${sizeFormatted}`)}`)
    })
  })
}

const handleSaveAction = async ({ recordings, savePath, template, overwrite, jsonOutput }) => {
  logHeading('Saving Recordings')

  try {
    try {
      const stats = await fs.stat(savePath)
      if (!stats.isDirectory()) {
        logError(`Save path "${savePath}" exists but is not a directory.`)
        process.exit(1)
      }
    } catch (statError) {
      if (statError.code === 'ENOENT') {
        log(chalk.gray(`Save path "${savePath}" does not exist. Creating it now…`))
        await fs.mkdir(savePath, { recursive: true })
      } else {
        throw statError
      }
    }

    const jsonResult = await saveRecordings({ recordings, savePath, template, overwrite })

    if (!jsonOutput) return

    logHeading('Start JSON Output', 'greenBright')
    console.log(JSON.stringify(jsonResult, null, 2))
    logHeading('End JSON Output', 'greenBright')
  } catch (saveError) {
    logError(`Error during save process: ${saveError.message}`)
    if (argv.debug) console.error(saveError.stack)
    process.exit(1)
  }
}

const trackLockFile = (lockPath) => {
  activeLockFiles.add(lockPath)
  debug('Tracking lock file: %s (Active: %d)', lockPath, activeLockFiles.size)
}

const untrackLockFile = (lockPath) => {
  const deleted = activeLockFiles.delete(lockPath)
  if (deleted) {
    debug('Untracked lock file: %s (Active: %d)', lockPath, activeLockFiles.size)
  } else {
    debug('Attempted to untrack non-tracked lock file: %s', lockPath)
  }
}

const syncCleanupLockFiles = () => {
  let count = 0
  if (activeLockFiles.size === 0) {
    debug('Sync cleanup: No active lock files to remove.')
    return 0
  }
  debug('Sync cleanup: Removing %d active lock file(s)...', activeLockFiles.size)
  const filesToRemove = Array.from(activeLockFiles)
  for (const lockPath of filesToRemove) {
    try {
      if (fsc.existsSync(lockPath)) {
        fsc.unlinkSync(lockPath)
        count++
        debug('Sync removed lock file: %s', lockPath)
      } else {
        debug('Sync cleanup: Lock file already removed: %s', lockPath)
      }
    } catch (err) {
      console.error(chalk.red(`Sync cleanup error: Failed to remove lock file ${lockPath}: ${err.message}`))
      debug('Sync cleanup error details: %O', err)
    }
  }
  activeLockFiles.clear()
  debug('Sync cleanup: Finished. Removed %d file(s). Active set cleared.', count)
  return count
}

const registerExitHandlers = (() => {
  let registered = false
  return () => {
    if (registered) return

    const exitHandler = (code) => {
      debug('Exit handler triggered with code: %s', code)
      const count = syncCleanupLockFiles()
      if (count > 0) {
        console.log(chalk.yellow(`Cleaned up ${count} lock file(s) on exit.`))
      }
    }

    const signalHandler = (signal) => {
      if (isShuttingDown) return
      isShuttingDown = true
      debug('Signal handler triggered: %s', signal)

      if (activeMultiBar) {
        activeMultiBar.stop()
        activeMultiBar = null
        progressBarActive = false
        process.stdout.write('\r\x1b[K')
      } else if (progressBarActive) {
        progressBarActive = false
        process.stdout.write('\r\x1b[K')
      }

      console.log(chalk.yellow('\nInterrupt signal received. Attempting graceful shutdown...'))

      const count = syncCleanupLockFiles()
      if (count > 0) {
        console.log(chalk.yellow(`Cleaned up ${count} lock file(s).`))
      } else {
        console.log(chalk.gray('No active lock files to clean up.'))
      }

      console.log(chalk.yellow('Exiting now.'))
      setTimeout(() => {
        process.exit(130)
      }, 300)
    }

    process.on('SIGINT', () => signalHandler('SIGINT'))
    process.on('SIGTERM', () => signalHandler('SIGTERM'))
    process.on('exit', exitHandler)

    process.on('uncaughtException', (err, origin) => {
      logError(`\n--- UNCAUGHT EXCEPTION ---`)
      logError(`Origin: ${origin}`)
      console.error(err)
      syncCleanupLockFiles()
      process.exit(1)
    })
    process.on('unhandledRejection', (reason, promise) => {
      logError(`\n--- UNHANDLED REJECTION ---`)
      console.error('Reason:', reason)
      syncCleanupLockFiles()
      process.exit(1)
    })

    registered = true
    debug('Exit handlers registered.')
  }
})()

const getApiService = async (location) => {
  const device = location?._rawDeviceXml
  if (!device?.serviceList?.service) return null

  let services = device.serviceList.service
  if (!Array.isArray(services)) services = [services]

  const cds = services.find(s => s.serviceType === UPNP_CONTENT_DIRECTORY_URN)
  if (!cds) return null

  const controlURL = getXmlText({ node: cds.controlURL })
  const serviceType = getXmlText({ node: cds.serviceType })

  if (!controlURL || !serviceType) return null

  try {
    const baseUrl = new URL(location.url)
    const absoluteControlUrl = new URL(controlURL, baseUrl).toString()
    return { cd_ctr: absoluteControlUrl, cd_service: serviceType }
  } catch (err) {
    logError(`Error constructing service URLs: ${err.message}`)
    return null
  }
}

const createRequestManager = ({
  initialConcurrency = INITIAL_BROWSE_CONCURRENCY,
  maxConcurrency = MAX_CONCURRENT_BROWSE,
  minDelay = MIN_BROWSE_DELAY,
  initialDelay = MIN_BROWSE_DELAY,
  adaptiveDelayFactor = ADAPTIVE_DELAY_FACTOR,
  debug: managerDebug = false
} = {}) => {
  const state = {
    concurrency: initialConcurrency,
    maxConcurrency: maxConcurrency,
    minDelay: minDelay,
    currentDelay: initialDelay,
    adaptiveDelayFactor: adaptiveDelayFactor,
    active: 0,
    queue: [],
    lastRequestTime: 0,
    responseStats: [],
    failureCount: 0,
    debug: managerDebug
  }

  const updateStats = ({ duration, success }) => {
    if (!success) {
      state.failureCount++
      if (state.failureCount > 2 && state.concurrency > 1) {
        state.concurrency = Math.max(1, Math.floor(state.concurrency * 0.75))
        state.currentDelay = Math.min(state.currentDelay * 1.5, 1000)
        if (state.debug) debug('Multiple failures detected, reducing concurrency to %d, increasing delay to %d',
          state.concurrency, state.currentDelay)
      }
    } else {
      state.failureCount = Math.max(0, state.failureCount - 0.5)
    }

    state.responseStats.push(duration)
    if (state.responseStats.length > 10) state.responseStats.shift()

    const avgDuration = state.responseStats.reduce((sum, d) => sum + d, 0) / state.responseStats.length

    if (avgDuration < 250 && state.concurrency < state.maxConcurrency && state.failureCount === 0) {
      state.concurrency = Math.min(state.concurrency + 1, state.maxConcurrency)
      if (state.debug) debug('Increasing concurrency to %d', state.concurrency)
    } else if (avgDuration > 500 && state.concurrency > 1) {
      state.concurrency = Math.max(state.concurrency - 1, 1)
      state.currentDelay = Math.min(state.currentDelay * state.adaptiveDelayFactor, 800)
      if (state.debug) debug('Decreasing concurrency to %d, increasing delay to %d',
        state.concurrency, state.currentDelay)
    }
  }

  const processQueue = async () => {
    if (state.active >= state.concurrency || state.queue.length === 0) return

    const now = Date.now()
    const timeSinceLastRequest = now - state.lastRequestTime

    if (timeSinceLastRequest < state.currentDelay) {
      setTimeout(() => processQueue(), state.currentDelay - timeSinceLastRequest)
      return
    }

    state.active++
    state.lastRequestTime = now
    const start = now
    const { fn, resolve, reject } = state.queue.shift()

    try {
      const result = await fn()
      const duration = Date.now() - start
      updateStats({ duration, success: true })
      resolve(result)
    } catch (error) {
      updateStats({ duration: 600, success: false })
      reject(error)
    } finally {
      state.active--
      processQueue()
    }
  }

  const enqueue = (fn, priority = REQUEST_QUEUE_PRIORITY.SHOW) =>
    new Promise((resolve, reject) => {
      state.queue.push({ fn, priority, resolve, reject })
      state.queue.sort((a, b) => a.priority - b.priority)
      processQueue()
    })

  return { enqueue }
}

const createBrowsePayload = ({ serviceType, objectId, requestedCount = 0 }) => `<?xml version="1.0" encoding="utf-8" standalone="yes"?>
<s:Envelope s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/" xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
 <s:Body>
  <u:Browse xmlns:u="${serviceType}">
   <ObjectID>${objectId}</ObjectID>
   <BrowseFlag>BrowseDirectChildren</BrowseFlag>
   <Filter>*</Filter>
   <StartingIndex>0</StartingIndex>
   <RequestedCount>${requestedCount}</RequestedCount>
   <SortCriteria></SortCriteria>
  </u:Browse>
 </s:Body>
</s:Envelope>`

const browseRequest = async ({ apiService, objectId = '0' }) => {
  const { cd_ctr: controlUrl, cd_service: serviceType } = apiService

  const cacheKey = `${objectId}`
  if (requestCache.has(cacheKey)) return requestCache.get(cacheKey)

  const payload = createBrowsePayload({ serviceType, objectId })
  const headers = {
    'Content-Type': 'text/xmlcharset="utf-8"',
    'SOAPAction': `"${serviceType}#Browse"`
  }

  let lastError = null
  let delayBase = BROWSE_RETRY_DELAY

  for (let attempt = 1; attempt <= BROWSE_RETRIES + 1; attempt++) {
    try {
      if (attempt > 1) {
        if (attempt > 2) log(chalk.yellow(`Retrying browse for ObjectID ${objectId} (Attempt ${attempt-1}/${BROWSE_RETRIES})…`))
        await new Promise(resolve => setTimeout(resolve, delayBase))
        delayBase = Math.min(delayBase * 2, 5000)
      }

      const response = await httpClient.post(controlUrl, payload, {
        headers,
        timeout: attempt === 1 ? REQUEST_TIMEOUT / 2 : REQUEST_TIMEOUT
      })

      const parsedResponse = parseXml(response.data)
      debug('Browse Response SOAP (ObjectID: %s, Attempt: %d): %O', objectId, attempt, parsedResponse)

      const resultText = findNode({ obj: parsedResponse, path: 'Envelope.Body.BrowseResponse.Result' })
      if (!resultText || typeof resultText !== 'string') {
        if (attempt > 1) logWarning(`Could not find valid Result string in Browse response for ObjectID ${objectId} on attempt ${attempt-1}`)
        lastError = new Error('Could not find valid Result string in Browse response')
        continue
      }

      const resultXml = parseXml(resultText)
      if (!resultXml || !resultXml['DIDL-Lite']) {
        if (attempt > 1) logWarning(`Failed to parse embedded DIDL-Lite XML for ObjectID ${objectId} on attempt ${attempt-1}`)
        lastError = new Error('Failed to parse embedded DIDL-Lite XML')
        continue
      }

      debug('Browse Response DIDL-Lite (ObjectID: %s, Attempt: %d): %O', objectId, attempt, resultXml['DIDL-Lite'])

      requestCache.set(cacheKey, resultXml['DIDL-Lite'])
      return resultXml['DIDL-Lite']

    } catch (err) {
      lastError = err
      if (attempt > 1) logWarning(`Browse attempt ${attempt-1} failed for ObjectID ${objectId}: ${err.message}`)
      debug('Browse Error Details (ObjectID: %s, Attempt: %d): %O', objectId, attempt, err)
      if (attempt === 1) continue
    }
  }

  logError(`Browse request failed definitively for ObjectID ${objectId} after ${BROWSE_RETRIES} attempts. Last error: ${lastError?.message || 'Unknown'}`)
  return null
}

const findDirectories = async ({ apiService, objectId = '0' }) => {
  const didlLite = await browseRequest({ apiService, objectId })
  if (!didlLite) return null
  if (!didlLite.container) return []

  let containers = didlLite.container
  if (!Array.isArray(containers)) containers = [containers]

  return containers.filter(Boolean).map(container => ({
    title: getXmlText({ node: container.title }),
    id: getXmlAttr({ node: container, attrName: 'id', defaultValue: NO_NUMBER_DEFAULT }),
    parent_id: getXmlAttr({ node: container, attrName: 'parentID', defaultValue: NO_NUMBER_DEFAULT }),
    items: []
  })).filter(c => c.id && c.title)
}

const findItems = async ({ apiService, objectId, showTitle = 'Unknown Show' }) => {
  const didlLite = await browseRequest({ apiService, objectId })
  if (!didlLite) return []
  if (!didlLite.item) return []

  let items = didlLite.item
  if (!Array.isArray(items)) items = [items]

  return items.filter(Boolean).map(item => {
    const resNode = item.res
    const actualRes = Array.isArray(resNode) ? resNode[0] : resNode
    const itemClass = getXmlText({ node: item.class }) ?? ''
    const isVideo = itemClass.includes('videoItem')
    const title = getXmlText({ node: item.title })

    let seasonNumber = null
    let seasonNumberPadded = null
    let episodeNumber = null
    let episodeNumberPadded = null

    const seMatch = title.match(/S(?:eason)?\s*(\d{1,3})\s*E(?:pisode)?\s*(\d{1,3})/i)
    if (seMatch) {
      seasonNumber = seMatch[1]
      seasonNumberPadded = seasonNumber.padStart(2, '0')
      episodeNumber = seMatch[2]
      episodeNumberPadded = episodeNumber.padStart(2, '0')
      debug('Extracted S/E from title "%s": S%s E%s', title, seasonNumber, episodeNumber)
    } else {
      const parentTaskName = getXmlAttr({ node: actualRes, attrName: 'parentTaskName' })
      const parentMatch = parentTaskName?.match(/S(\d{1,3})\s*E(\d{1,3})/i)
      if (parentMatch) {
        seasonNumber = parentMatch[1]
        seasonNumberPadded = seasonNumber.padStart(2, '0')
        episodeNumber = parentMatch[2]
        episodeNumberPadded = episodeNumber.padStart(2, '0')
        debug('Extracted S/E from parentTaskName "%s": S%s E%s', parentTaskName, seasonNumber, episodeNumber)
      } else {
        debug('Could not extract S/E numbers from title: "%s" or parentTaskName: "%s"', title, parentTaskName)
      }
    }

    let ext = 'mpeg'
    const protocolInfo = getXmlAttr({ node: actualRes, attrName: 'protocolInfo', defaultValue: '' })
    if (protocolInfo.includes('mpeg-tts') || protocolInfo.includes('AVC_TS')) {
      ext = 'ts'
    } else if (protocolInfo.includes('video/mp4')) {
      ext = 'mp4'
    } else if (protocolInfo.includes('video/mpeg')) {
      ext = 'mpg'
    }
    debug('Determined extension for "%s" based on protocolInfo "%s": %s', title, protocolInfo, ext)

    return {
      type: itemClass,
      title: title,
      id: getXmlAttr({ node: item, attrName: 'id', defaultValue: NO_NUMBER_DEFAULT }),
      parent_id: getXmlAttr({ node: item, attrName: 'parentID', defaultValue: NO_NUMBER_DEFAULT }),
      description: getXmlText({ node: item.description }),
      url: getXmlText({ node: actualRes }),
      size: parseInt(getXmlAttr({ node: actualRes, attrName: 'size', defaultValue: '0' }), 10),
      duration: tsToSeconds(getXmlAttr({ node: actualRes, attrName: 'duration' })),
      show_title: showTitle,
      season_number: seasonNumber,
      season_number_padded: seasonNumberPadded,
      episode_number: episodeNumber,
      episode_number_padded: episodeNumberPadded,
      ext: ext,
      item_type: isVideo && seasonNumber && episodeNumber ? 'episode' : (isVideo ? 'movie' : 'other')
    }
  }).filter(i => i.id && i.url && i.title)
}

const isCurrentlyRecording = async (item) => {
  if (item.size > 0 && item.size < MAX_OCTET_RECORDING - 1000000) {
    debug('Skipping recording check for %s, size (%s) seems final.', item.title, filesize(item.size))
    return false
  }
  if (item.size === MAX_OCTET_RECORDING) {
    debug('Item size for %s matches recording marker exactly (%s).', item.title, filesize(item.size))
    return true
  }

  debug('Performing HEAD/GET request to check recording status for %s (initial size: %s)', item.title, filesize(item.size))

  try {
    const response = await httpClient.head(item.url, { timeout: REQUEST_TIMEOUT / 2 })
    debug('HEAD Response Headers for %s: %O', item.title, response.headers)
    const contentLength = parseInt(response.headers['content-length'] ?? '-1', 10)
    const isRecordingSize = contentLength === MAX_OCTET_RECORDING
    debug('HEAD check for %s: Content-Length=%d, IsRecordingMarker=%s', item.title, contentLength, isRecordingSize)
    return isRecordingSize
  } catch (headError) {
    debug('HEAD request failed for %s: %O', item.title, headError)
    if (headError.response?.status === 405 || headError.code === 'ECONNABORTED' || !headError.response) {
      debug('HEAD failed for %s, attempting GET fallback check...', item.title)
      try {
        const response = await httpClient.get(item.url, {
          timeout: REQUEST_TIMEOUT,
          responseType: 'stream'
        })
        const contentLength = parseInt(response.headers['content-length'] ?? '-1', 10)
        response.data.destroy()
        const isRecordingSize = contentLength === MAX_OCTET_RECORDING
        debug('GET (fallback) check for %s: Content-Length=%d, IsRecordingMarker=%s', item.title, contentLength, isRecordingSize)
        return isRecordingSize
      } catch (getErr) {
        logWarning(`Recording check failed for ${item.title} (HEAD and GET failed): ${getErr.message}`)
        debug('GET (fallback) error for %s: %O', item.title, getErr)
        return false
      }
    } else {
      logWarning(`HEAD check failed for ${item.title}: ${headError.message}`)
      return false
    }
  }
}

const downloadFile = async ({ item, filePath, progressBar, overwrite = false }) => {
  const lockFilePath = `${filePath}${CONST_LOCK}`
  let writer = null
  let responseStream = null
  let existingSize = 0
  let isResuming = false
  let validResumable = false
  let totalLength = 0

  try {
    try {
      if (fsc.existsSync(filePath)) {
        if (overwrite) {
          log(chalk.yellow(`Overwriting: ${item.title}`))
          try {
            fsc.unlinkSync(filePath)
            debug('Deleted existing file for overwrite: %s', filePath)
          } catch (unlinkErr) {
            logWarning(`Could not delete existing file for overwrite: ${unlinkErr.message}`)
          }
          isResuming = false
          existingSize = 0
        } else {
          const stats = await fs.stat(filePath)
          existingSize = stats.size
          if (existingSize > 0) {
            isResuming = true
            validResumable = true
            debug('File exists, attempting resume from %s', filesize(existingSize))
          } else {
            isResuming = false
            existingSize = 0
            debug('File exists but is empty, starting fresh download.')
          }
        }
      } else {
        isResuming = false
        existingSize = 0
      }
    } catch (statErr) {
      debug('Error checking file stats for resume: %O', statErr)
      logWarning(`Could not check existing file status for ${item.title}: ${statErr.message}. Starting fresh download.`)
      existingSize = 0
      isResuming = false
    }

    try {
      if (fsc.existsSync(lockFilePath)) {
        const isStale = await isLockFileStale(lockFilePath)
        if (isStale) {
          log(chalk.yellow(`Removing stale lock file during download setup: ${path.basename(lockFilePath)}`))
          fsc.unlinkSync(lockFilePath)
        } else {
          logError(`Active lock file found for ${item.title}. Aborting download.`)
          if (progressBar) progressBar.stop()
          return { recorded: false, error: 'Active lock file found, download aborted.' }
        }
      }
      fsc.writeFileSync(lockFilePath, '')
      trackLockFile(lockFilePath)
      debug('Created lock file: %s', lockFilePath)
    } catch (lockErr) {
      logError(`Failed to create lock file for ${item.title}: ${lockErr.message}`)
      debug('Lock file creation error details: %O', lockErr)
      if (progressBar) progressBar.stop()
      return { recorded: false, error: `Failed to create lock file: ${lockErr.message}` }
    }

    if (isResuming && validResumable) {
      log(`${chalk.cyan(`Resuming download for: ${path.basename(filePath)}`)} ${chalk.gray(`(from ${filesize(existingSize)})`)}`)
    } else if (!isResuming) {
      log(`${chalk.cyan(`Starting download for: ${path.basename(filePath)}`)}`)
    }

    const headers = {}
    if (isResuming && existingSize > 0) {
      headers.Range = `bytes=${existingSize}-`
      debug('Adding Range header: %s', headers.Range)
    }

    const response = await httpClient.get(item.url, {
      responseType: 'stream',
      timeout: REQUEST_TIMEOUT * 20,
      headers,
      validateStatus: function (status) {
        return status === 200 || status === 206
      },
    })

    responseStream = response.data

    if (response.status === 206) {
      const contentRange = response.headers['content-range']
      if (contentRange && contentRange.includes('/')) {
        totalLength = parseInt(contentRange.split('/')[1], 10)
        debug('Partial content detected. Total length from Content-Range: %d (%s)', totalLength, filesize(totalLength))
      } else {
        logWarning(`Could not determine total size from partial response for ${item.title}. Progress bar may be inaccurate.`)
        totalLength = item.size || 0
        debug('Using original item size as fallback total length: %d', totalLength)
      }
    } else {
      totalLength = parseInt(response.headers['content-length'] || '0', 10)
      if (isResuming && existingSize > 0) {
        logWarning(`Server returned 200 OK despite Range request for ${item.title}. Restarting download from beginning.`)
        isResuming = false
        existingSize = 0
        try {
          fsc.unlinkSync(filePath)
          debug('Deleted existing file content due to 200 OK on resume attempt.')
        } catch(unlinkErr) {
          logWarning(`Could not clear existing file after failed resume attempt: ${unlinkErr.message}`)
        }
      }
      debug('Full content response. Total length from Content-Length: %d (%s)', totalLength, filesize(totalLength))
    }

    debug('Download Headers for %s: %O', item.title, response.headers)
    debug('Resume Status: %s, existing size: %d, response status: %d',
      isResuming ? 'yes' : 'no', existingSize, response.status)

    if (totalLength === MAX_OCTET_RECORDING && !isResuming) {
      logWarning(`Skipping ${item.title}, it appears to be currently recording (size matches marker).`)
      responseStream.destroy()

      try {
        if (fsc.existsSync(lockFilePath)) {
          fsc.unlinkSync(lockFilePath)
          untrackLockFile(lockFilePath)
          debug('Removed lock file on skip (currently recording).')
        }
      } catch (delErr) {
        logWarning(`Could not delete lock file on skip (currently recording) ${lockFilePath}: ${delErr.message}`)
      }

      if (progressBar) progressBar.stop()
      return { recorded: false, warning: 'Skipping item, size indicates it\'s currently recording' }
    }

    if (totalLength === 0 && !isResuming) {
      logWarning(`Skipping ${item.title}, content length is zero.`)
      responseStream.destroy()

      try {
        if (fsc.existsSync(lockFilePath)) {
          fsc.unlinkSync(lockFilePath)
          untrackLockFile(lockFilePath)
          debug('Removed lock file on skip (zero size).')
        }
      } catch (delErr) {
        logWarning(`Could not delete lock file on zero size skip ${lockFilePath}: ${delErr.message}`)
      }

      if (progressBar) progressBar.stop()
      return { recorded: false, warning: 'Skipping item, content length is zero' }
    }

    writer = fsc.createWriteStream(filePath, { flags: isResuming ? 'a' : 'w' })

    writer.on('open', () => debug('Write stream opened for %s with flags: %s', filePath, writer.flags))
    writer.on('close', () => debug('Write stream closed for %s', filePath))

    if (progressBar) {
      const barTotal = Math.max(totalLength || 0, existingSize, 1)
      progressBar.setTotal(barTotal)
      progressBar.update(existingSize, {
        speed: 'N/A',
        eta: 'N/A',
        value: filesize(existingSize),
        total: filesize(barTotal)
      })
      debug('Progress bar initialized. Current: %d, Total: %d', existingSize, barTotal)
    }

    let downloadedLength = existingSize
    let lastUpdateTime = progressBar?.startTime || Date.now()
    const PROGRESS_UPDATE_INTERVAL = 250

    responseStream.on('data', (chunk) => {
      if (isShuttingDown) {
        if (!responseStream.destroyed) responseStream.destroy()
        return
      }
      downloadedLength += chunk.length
      if (!progressBar) return

      const now = Date.now()
      const startTime = progressBar.startTime || lastUpdateTime
      if (now - lastUpdateTime > PROGRESS_UPDATE_INTERVAL || downloadedLength === totalLength) {
        const elapsedSecondsTotal = (now - startTime) / 1000
        const bytesDownloadedThisSession = downloadedLength - existingSize
        const speed = elapsedSecondsTotal > 0.1 ? bytesDownloadedThisSession / elapsedSecondsTotal : 0

        const bytesRemaining = Math.max(0, totalLength - downloadedLength)
        const etaSeconds = (speed > 0 && bytesRemaining > 0) ? bytesRemaining / speed : Infinity

        if (!isShuttingDown) {
          const currentProgressBarValue = Math.min(downloadedLength, progressBar.getTotal())
          progressBar.update(currentProgressBarValue, {
            speed: filesize(speed),
            eta: etaSeconds === Infinity ? '∞' : prettyMs(etaSeconds * 1000, { compact: true }),
            value: filesize(currentProgressBarValue),
            total: filesize(progressBar.getTotal())
          })
        }
        lastUpdateTime = now
      }
    })

    responseStream.pipe(writer)

    return new Promise((resolve, reject) => {
      writer.on('finish', async () => {
        if (isShuttingDown) {
          debug('Write stream finished during shutdown for %s', item.title)
          return
        }
        debug('Write stream finished successfully for %s', item.title)
        if (progressBar) progressBar.update(progressBar.getTotal())
        if (progressBar) progressBar.stop()

        await new Promise(resolve => setTimeout(resolve, 150))

        let finalSize = -1
        try {
          const finalStats = await fs.stat(filePath)
          finalSize = finalStats.size
          debug('Final file size for %s: %d bytes', item.title, finalSize)

          try {
            if (fsc.existsSync(lockFilePath)) {
              fsc.unlinkSync(lockFilePath)
              untrackLockFile(lockFilePath)
              debug('Removed lock file after successful save: %s', lockFilePath)
            }
          } catch (unlinkErr) {
            logWarning(`Could not remove lock file after successful save for ${item.title}: ${unlinkErr.message}`)
            resolve({
              recorded: true,
              resumed: isResuming,
              warning: `Save complete, but failed to remove lock file: ${unlinkErr.message}`
            })
            return
          }

          if (totalLength > 0 && finalSize !== totalLength) {
            const sizeDiff = Math.abs(finalSize - totalLength)
            const tolerance = 1024
            if (sizeDiff > tolerance) {
              logWarning(`Warning for ${item.title}: Final file size (${filesize(finalSize)}) doesn't match expected size (${filesize(totalLength)}) by ${filesize(sizeDiff)}.`)
              resolve({
                recorded: true,
                resumed: isResuming,
                warning: `File size mismatch: final=${filesize(finalSize)}, expected=${filesize(totalLength)}`
              })
            } else {
              debug('Final file size is within tolerance of expected size.')
              resolve({ recorded: true, resumed: isResuming })
            }
          } else if (totalLength === 0 && finalSize > 0) {
            debug('Original content length was 0, but final size is %s. Treating as success.', filesize(finalSize))
            resolve({ recorded: true, resumed: isResuming })
          } else {
            debug('Final file size matches expected size.')
            resolve({ recorded: true, resumed: isResuming })
          }
        } catch (verifyErr) {
          logError(`Error verifying final file size for ${item.title}: ${verifyErr.message}`)
          debug('File size verification error details: %O', verifyErr)
          try {
            if (fsc.existsSync(lockFilePath)) {
              fsc.unlinkSync(lockFilePath)
              untrackLockFile(lockFilePath)
              debug('Removed lock file after verification error.')
            }
          } catch (unlinkErr) {
            logWarning(`Could not remove lock file after verification error for ${item.title}: ${unlinkErr.message}`)
          }
          resolve({
            recorded: true,
            resumed: isResuming,
            warning: `Write finished, but failed to verify final size: ${verifyErr.message}`
          })
        }
      })

      writer.on('error', (err) => {
        if (isShuttingDown) {
          debug('Write stream error during shutdown for %s: %s', item.title, err.message)
          return
        }
        logError(`Error writing file ${path.basename(filePath)}: ${err.message}`)
        debug('File Write Error Details (%s): %O', item.title, err)
        if (progressBar) progressBar.stop()

        if (responseStream && !responseStream.destroyed) {
          responseStream.destroy()
          debug('Destroyed response stream due to writer error.')
        }

        try {
          if (fsc.existsSync(lockFilePath)) {
            fsc.unlinkSync(lockFilePath)
            untrackLockFile(lockFilePath)
            debug('Removed lock file after write error.')
          }
        } catch (delErr) {
          logWarning(`Could not delete lock file ${lockFilePath} on write error: ${delErr.message}`)
        }

        reject(new Error(`Write error: ${err.message}`))
      })

      responseStream.on('error', (err) => {
        if (isShuttingDown) {
          debug('Response stream error during shutdown for %s: %s', item.title, err.message)
          return
        }
        const completionPercentage = totalLength > 0 ? Math.min(100, (downloadedLength / totalLength) * 100) : 0
        debug('Response Stream Error Details (%s): Code: %s, Message: %s', item.title, err.code, err.message)

        if (progressBar) {
          progressBar.update(Math.min(downloadedLength, progressBar.getTotal()))
          progressBar.stop()
        }

        if (writer && !writer.closed) {
          writer.end(() => {
            debug('Writer stream closed after response stream error for %s.', item.title)

            try {
              if (fsc.existsSync(lockFilePath)) {
                fsc.unlinkSync(lockFilePath)
                untrackLockFile(lockFilePath)
                debug('Removed lock file after response stream error.')
              }
            } catch (cleanupErr) {
              logWarning(`Lock file cleanup failed after response stream error: ${cleanupErr.message}`)
            }

            const isECONNRESET = err.code === 'ECONNRESET'
            const isTimeout = err.code === 'ECONNABORTED' || (err.message && err.message.toLowerCase().includes('timeout'))
            const isPrematureClose = err.message && err.message.includes('Premature close')
            const isNearlyComplete = completionPercentage > 98

            let warningMessage = `Download interrupted for ${item.title} at ${completionPercentage.toFixed(1)}%.`
            if (isECONNRESET) warningMessage += ' (Connection reset)'
            else if (isTimeout) warningMessage += ' (Timeout)'
            else if (isPrematureClose) warningMessage += ' (Premature close)'
            else warningMessage += ` (Error: ${err.message})`

            if (isNearlyComplete) {
              logWarning(`${warningMessage} - File may be usable.`)
              resolve({ recorded: false, resumed: isResuming, warning: `${warningMessage} - File may be usable.` })
            } else {
              logError(`${warningMessage} - Will attempt resume on next run.`)
              resolve({ recorded: false, resumed: isResuming, warning: `${warningMessage} - Can be resumed.` })
            }
          })
        } else {
          try {
            if (fsc.existsSync(lockFilePath)) {
              fsc.unlinkSync(lockFilePath)
              untrackLockFile(lockFilePath)
              debug('Removed lock file after response stream error (writer already closed).')
            }
          } catch (cleanupErr) {
            logWarning(`Lock file cleanup failed after response stream error (writer already closed): ${cleanupErr.message}`)
          }
          logError(`Download interrupted for ${item.title} (Error: ${err.message}) - Will attempt resume.`)
          resolve({ recorded: false, resumed: isResuming, warning: `Download interrupted (${err.message}) - Can be resumed.` })
        }
      })

      responseStream.on('end', () => {
        if (isShuttingDown) {
          debug('Response stream ended during shutdown for %s', item.title)
          return
        }
        debug('Response stream ended for %s.', item.title)
      })
    })
  } catch (error) {
    logError(`Failed to initiate download for ${item.title}: ${error.message}`)
    debug('Outer Download Error (%s): %O', item.title, error)

    if (progressBar) progressBar.stop()

    if (responseStream && !responseStream.destroyed) responseStream.destroy()
    if (writer && !writer.closed) writer.close()

    try {
      if (fsc.existsSync(lockFilePath)) {
        fsc.unlinkSync(lockFilePath)
        untrackLockFile(lockFilePath)
        debug('Cleaned up lock file in outer catch block.')
      }
    } catch (cleanupErr) {
      logWarning(`Could not clean up lock file in outer catch block: ${cleanupErr.message}`)
    }

    return { recorded: false, error: `Download initiation failed: ${error.message}` }
  }
}

const parseXml = (xmlString) => {
  if (!xmlString || typeof xmlString !== 'string') return null

  const xmlParser = new XMLParser({
    ignoreAttributes: false,
    attributeNamePrefix: '@_',
    textNodeName: '#text',
    parseAttributeValue: true,
    removeNSPrefix: true,
    allowBooleanAttributes: true
  })

  try {
    return xmlParser.parse(xmlString)
  } catch (error) {
    logError(`XML parsing error: ${error.message}`)
    if (argv.debug) debugXml('XML String causing parse error: %s', xmlString.slice(0, 500) + '...')
    return null
  }
}

const parseLocations = async (locationsUrls) => {
  const xmlParserOptions = {
    ignoreAttributes: false,
    attributeNamePrefix: '@_',
    textNodeName: '#text',
    parseAttributeValue: true,
    removeNSPrefix: true,
    allowBooleanAttributes: true
  }

  const fetchAndParse = async (url) => {
    const xmlParser = new XMLParser(xmlParserOptions)
    try {
      const response = await httpClient.get(url, { timeout: REQUEST_TIMEOUT })
      if (XMLValidator.validate(response.data) !== true) {
        logWarning(`Invalid XML from ${url}`)
        return null
      }
      const xmlRoot = xmlParser.parse(response.data)
      debugXml('Parsed XML from %s: %O', url, xmlRoot)
      const device = xmlRoot?.root?.device
      if (!device) {
        logWarning(`Could not find device info in XML from ${url}`)
        return null
      }

      return {
        url: url,
        deviceType: getXmlText({ node: device.deviceType }),
        friendlyName: getXmlText({ node: device.friendlyName }),
        manufacturer: getXmlText({ node: device.manufacturer }),
        manufacturerURL: getXmlText({ node: device.manufacturerURL }),
        modelDescription: getXmlText({ node: device.modelDescription }),
        modelName: getXmlText({ node: device.modelName }),
        modelNumber: getXmlText({ node: device.modelNumber }),
        _rawDeviceXml: device
      }
    } catch (err) {
      if (err.code === 'ECONNABORTED' || err.message.includes('timeout')) {
        logWarning(`Timeout reading from ${url}`)
      } else {
        logWarning(`Connection error for ${url}: ${err.message}`)
      }
      debugNetwork('Error fetching/parsing %s: %O', url, err)
      return null
    }
  }

  const promises = locationsUrls.map(url => fetchAndParse(url))
  const settledResults = await Promise.all(promises)
  return settledResults.filter(result => result !== null)
}

const getXmlAttr = ({ node, attrName, defaultValue = '' }) =>
  node?.[`@_${attrName}`] ?? defaultValue

const getXmlText = ({ node, defaultValue = '' }) => {
  if (_.isNil(node)) return defaultValue
  if (_.isObject(node) && _.has(node, '#text')) return node['#text'] ?? defaultValue
  if (_.isString(node) || _.isNumber(node) || _.isBoolean(node)) return _.toString(node)
  return defaultValue
}

const findNode = ({ obj, path }) => {
  if (!obj || typeof obj !== 'object' || !path) return undefined
  return path.split('.').reduce((current, key) => {
    if (_.isNil(current)) return undefined
    const foundKey = _.find(_.keys(current), k => k === key || _.endsWith(k, `:${key}`))
    return foundKey ? current[foundKey] : undefined
  }, obj)
}

const processFilter = (arr) =>
  _(arr)
    .castArray()
    .flatMap(f => typeof f === 'string' ? f.split(',') : [f])
    .map(s => String(s).trim().toLowerCase())
    .compact()
    .value()

const sortRecordingsByTitle = (recordings) => {
  return _.chain(recordings)
    .cloneDeep()
    .map(recording => ({
      ...recording,
      sortTitle: _.toLower(recording.title).startsWith('the ')
        ? _.toLower(recording.title).slice(4)
        : _.toLower(recording.title)
    }))
    .sortBy('sortTitle')
    .map(item => _.omit(item, 'sortTitle'))
    .value()
}

const formatItem = (item) => ({
  id: item.id,
  title: item.title,
  type: item.item_type,
  duration: item.duration,
  size: item.size,
  description: item.description,
  show_title: item.show_title,
  season_number: item.season_number,
  season_number_padded: item.season_number_padded,
  episode_number: item.episode_number,
  episode_number_padded: item.episode_number_padded,
  ext: item.ext
})

const createValidFilename = (filename = '') => {
  let result = _.chain(filename)
    .toString()
    .trim()
    .replace(/[<>:"/\\|?*\x00-\x1F]/g, '')
    .replace(/\s+/g, ' ')
    .value()

  if (result === '.' || result === '..') result = '_'
  return result.slice(0, MAX_FILENAME - 10)
}

const tsToSeconds = (ts = '0') => {
  if (typeof ts !== 'string') return 0
  try {
    return ts.split(':').reverse().reduce((acc, unit, i) => {
      const value = parseFloat(unit)
      return isNaN(value) ? acc : acc + value * (60 ** i)
    }, 0)
  } catch (e) {
    debug('Error parsing timestamp: %O', { timestamp: ts, error: e })
    return 0
  }
}

const loadSavedFiles = async (savePath) => {
  const filePath = path.join(savePath, SAVE_FILE_NAME)
  try {
    await fs.access(filePath)
    const content = await fs.readFile(filePath, 'utf-8')
    return JSON.parse(content) || {}
  } catch (error) {
    if (error.code === 'ENOENT') return {}
    logWarning(`Could not load ${SAVE_FILE_NAME}: ${error.message}. Starting fresh.`)
    return {}
  }
}

const addSavedFile = async ({ savePath, savedFilesDb, item }) => {
  savedFilesDb[item.id] = item.title
  const filePath = path.join(savePath, SAVE_FILE_NAME)
  try {
    await fs.mkdir(savePath, { recursive: true })
    await fs.writeFile(filePath, JSON.stringify(savedFilesDb, null, 2), 'utf-8')
  } catch (error) {
    logError(`Error writing ${SAVE_FILE_NAME}: ${error.message}`)
  }
}

const isLockFileStale = async (lockFilePath) => {
  try {
    const stats = await fs.stat(lockFilePath)
    const lockAge = Date.now() - stats.mtimeMs
    return lockAge > 600000
  } catch (err) {
    debug('Error checking lock file stats: %O', err)
    return true
  }
}

const processPathTemplate = ({ templateString, data }) => {
  debugTemplate('Processing template: %s', templateString)
  debugTemplate('Template data: %O', data)

  let processedPath = templateString

  const placeholders = {
    '${show_title}': data.show_title || '',
    '${recording_title}': data.recording_title || '',
    '${season_number}': data.season_number || '',
    '${season_number_padded}': data.season_number_padded || '',
    '${episode_number}': data.episode_number || '',
    '${episode_number_padded}': data.episode_number_padded || '',
    '${ext}': data.ext || 'ts',
  }

  for (const [placeholder, value] of Object.entries(placeholders)) {
    const sanitizedValue = value.split(/[\/\\]/).map(part => createValidFilename(part)).join(path.sep)
    processedPath = processedPath.replaceAll(placeholder, sanitizedValue)
    debugTemplate('Replaced "%s" with (sanitized) "%s"', placeholder, sanitizedValue)
  }

  processedPath = processedPath
    .replace(/[<>:"|?*\x00-\x1F]/g, '')
    .replace(/[\/\\]{2,}/g, path.sep)
    .replace(/[.]{2,}/g, '_')
    .replace(/^[.\/\\]+/, '')
    .replace(/[.\/\\]+$/, '')

  if (processedPath.length > MAX_FILENAME * 3) {
    logWarning(`Generated path seems very long, potentially problematic: ${processedPath.slice(0, 100)}...`)
  }

  debugTemplate('Final processed path segment: %s', processedPath)
  return processedPath
}

const log = message => console.log(message)
const logWarning = message => console.log(chalk.yellow.bold(message))
const logError = message => console.log(chalk.red.bold(message))
const logHeading = (title, color = 'blueBright') => console.log(chalk[color].bold(`=== ${title} ===`))

main().catch(error => {
  if (activeMultiBar) {
    activeMultiBar.stop()
    activeMultiBar = null
  }
  progressBarActive = false
  process.stdout.write('\r\x1b[K')

  logError('\n--- An unexpected error occurred ---')
  logError(error.message)
  if (argv.debug || !(error.response || error.request || error.code)) {
    console.error(error.stack)
  } else if (error.response) {
    logError(`Status: ${error.response.status} ${error.response.statusText}`)
    logError(`URL: ${error.config?.url || error.request?.path || 'N/A'}`)
    if (error.response.data) {
      const responseData = typeof error.response.data === 'string'
        ? error.response.data.slice(0, 300) + (error.response.data.length > 300 ? '...' : '')
        : '[Object/Stream]'
      logError(`Response Data: ${responseData}`)
    }
  } else if (error.request) {
    logError('Request made but no response received.')
    logError(`URL: ${error.config?.url || error.request?.path || 'N/A'}`)
    if (error.code) logError(`Error Code: ${error.code}`)
  } else if (error.code) {
    logError(`Error Code: ${error.code}`)
    if (error.path) logError(`Path: ${error.path}`)
  }
  logError('----------------------------------')

  syncCleanupLockFiles()
  process.exit(1)
})
