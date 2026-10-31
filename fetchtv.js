#!/usr/bin/env node

import fsc from 'fs'
import ora from 'ora'
import _ from 'lodash'
import path from 'path'
import axios from 'axios'
import { URL, pathToFileURL } from 'url'
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
const activeLockFiles = new Set()

const BROWSE_RETRIES = 3
const MAX_FILENAME = 255
const FETCHTV_PORT = 49152
const CONST_LOCK = '.lock'
const PART_SUFFIX = '.part'
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
const SIZE_TOLERANCE = 1024
const RANGE_NOT_SATISFIABLE = 416
const RESUME_TIMEOUT = REQUEST_TIMEOUT * 2
const MIN_CONCURRENT_DOWNLOADS = 1
const MAX_CONCURRENT_DOWNLOADS = 10
const DEFAULT_CONCURRENT_DOWNLOADS = 2
const DOWNLOAD_RETRY_DELAYS = [5000, 15000]
const UPNP_CONTENT_DIRECTORY_URN = 'urn:schemas-upnp-org:service:ContentDirectory:1'

const REQUEST_QUEUE_PRIORITY = {
  ROOT: 0,
  SHOWS_FOLDER: 1,
  SHOW: 2,
  RECORDING_CHECK: 3
}

const SAVE_STATUS = {
  SAVED: 'saved',
  ALREADY_SAVED: 'already_saved',
  STILL_RECORDING: 'still_recording',
  LOCKED: 'locked',
  FAILED: 'failed'
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
    .option('for-plex', { type: 'boolean', default: false, description: 'Save as Show/Season 01/Show - S01E02.ext, the Plex and Freetvarr layout (overrides --template)' })
    .option('overwrite', { type: 'boolean', default: false, description: 'Save again over files that fetchtv saved earlier (never over other files)' })
    .option('concurrency', { type: 'number', default: DEFAULT_CONCURRENT_DOWNLOADS, description: `Number of recordings to save at the same time (${MIN_CONCURRENT_DOWNLOADS}-${MAX_CONCURRENT_DOWNLOADS})` })
    .option('json', { type: 'boolean', default: false, description: 'Output show/recording/save results in JSON' })
    .option('debug', { type: 'boolean', default: false, description: 'Enable verbose logging for debugging'})
    .check(argv => {
      if (isValidConcurrency(argv.concurrency)) return true
      throw new Error(`--concurrency must be a whole number from ${MIN_CONCURRENT_DOWNLOADS} to ${MAX_CONCURRENT_DOWNLOADS}.`)
    })
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

  const forPlex = Boolean(argv.forPlex && argv.save)
  const effectiveTemplate = forPlex ? null : argv.template

  if (argv.forPlex && !argv.save) logWarning('--for-plex option ignored because --save was not specified.')
  if (forPlex && argv.template) logWarning('--for-plex overrides the provided --template option. Using the Plex layout.')

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
        forPlex,
        overwrite: argv.overwrite,
        concurrency: argv.concurrency,
        jsonOutput: argv.json
      })
    }
  } else if (!hasInfoCommand) {
    logWarning('No action specified. Use info, recordings, shows, --is-recording, or --save. Use --help for options.')
  }

  logHeading(`Done: ${new Date().toLocaleString()}`)
}

const discoverFetchServers = async ({ timeoutMs = DISCOVERY_TIMEOUT } = {}) => {
  const locations = new Set()
  const client = new SsdpClient()
  client.on('response', (headers) => {
    if (headers.LOCATION) locations.add(headers.LOCATION)
  })

  try {
    client.search('ssdp:all')
    await new Promise(resolve => setTimeout(resolve, timeoutMs))
  } finally {
    client.stop()
  }

  if (locations.size === 0) return []
  const parsedLocations = await parseLocations([...locations])
  return parsedLocations.filter(loc => loc.manufacturerURL === FETCH_MANUFACTURER_URL)
}

const discoverFetch = async ({ ip, port }) => {
  const spinner = ora('Looking for Fetch TV servers…').start()
  const locations = new Set()

  if (ip) {
    locations.add(`http://${ip}:${port}/MediaServer.xml`)
  } else {
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
      spinner.fail('SSDP discovery failed.')
      logError(`SSDP discovery failed: ${err.message}`)
      client.stop()
      return null
    }
  }

  if (locations.size === 0) {
    spinner.fail('No Fetch TV servers found.')
    logError('Discovery failed: No UPnP devices found.')
    return null
  }

  const parsedLocations = await parseLocations([...locations])
  const fetchServer = parsedLocations.find(loc => loc.manufacturerURL === FETCH_MANUFACTURER_URL)

  if (!fetchServer) {
    spinner.fail('No Fetch TV servers found.')
    logError('Discovery failed: Unable to locate Fetch TV UPnP service.')
    return null
  }

  spinner.succeed('Fetch TV found.')

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

  progressBarActive = true
  const progressBar = new cliProgress.SingleBar({
    format: `Processing |${chalk.cyan('{bar}')}| {percentage}% | {value}/{total} Shows`,
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

const plexFilePath = async ({ savePath, showFolder, templateData, listFoldersOf }) => {
  const { season_number, episode_number, ext } = templateData
  if (!season_number || !episode_number) return null
  const season = Number(season_number)
  const showPath = path.join(savePath, showFolder)
  const seasonFolder = seasonFolderFor({ season, folders: await listFoldersOf(showPath) })
  const fileName = `${showFolder} - S${padNumber(season)}E${padNumber(Number(episode_number))}.${ext}`
  return path.join(showPath, seasonFolder, fileName)
}

const padNumber = (number) => String(number).padStart(2, '0')

const seasonFolderFor = ({ season, folders }) => {
  const padded = `Season ${padNumber(season)}`
  const matching = [...folders].sort().filter(folder => seasonNumberOf(folder) === season)
  return matching.find(folder => folder === padded) ?? matching[0] ?? padded
}

const seasonNumberOf = (folderName) => {
  const match = SEASON_FOLDER_PATTERN.exec(folderName)
  return match ? Number(match[1]) : null
}

const SEASON_FOLDER_PATTERN = /^season\s*(\d+)$/i

const existingFolderFor = ({ title, folders }) => {
  const wanted = createValidFilename(title).toLowerCase()
  const sorted = [...folders].sort()
  const exact = sorted.find(folder => folder.toLowerCase() === wanted)
  if (exact) return exact
  const suffixed = sorted.filter(folder => isSuffixedFolderOf({ folder, wanted }))
  return suffixed.length === 1 ? suffixed[0] : undefined
}

const isSuffixedFolderOf = ({ folder, wanted }) => {
  const name = folder.toLowerCase()
  return name.startsWith(`${wanted} `) && YEAR_OR_COUNTRY_SUFFIX.test(name.slice(wanted.length))
}

const YEAR_OR_COUNTRY_SUFFIX = /^(?: \((?:\d{4}|[a-z]{2,3})\)){1,2}$/

const createFolderLister = () => {
  const listings = new Map()
  return (dirPath) => {
    if (!listings.has(dirPath)) listings.set(dirPath, listFolders(dirPath))
    return listings.get(dirPath)
  }
}

const listFolders = async (dirPath) => {
  try {
    const entries = await fs.readdir(dirPath, { withFileTypes: true })
    return entries.filter(entry => entry.isDirectory()).map(entry => entry.name).sort()
  } catch (err) {
    if (err.code !== 'ENOENT') debug('Could not list folders in %s: %O', dirPath, err)
    return []
  }
}

const saveRecordings = async ({
  recordings,
  savePath,
  template,
  forPlex = false,
  overwrite,
  concurrency = DEFAULT_CONCURRENT_DOWNLOADS,
  retryDelays = DOWNLOAD_RETRY_DELAYS
}) => {
  const savedFilesDb = await loadSavedFiles(savePath)
  const listFoldersOf = createFolderLister()
  const ownerByPath = ownersBySavedPath(savedFilesDb)
  const claimedPaths = new Set()
  const jsonResults = []
  const tasks = []

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

      const showFolder = existingFolderFor({ title: show.title, folders: await listFoldersOf(savePath) })
        || createValidFilename(show.title)

      const defaultFilePath = () => {
        const itemFileName = `${createValidFilename(buildDefaultFileBase(templateData))}.${templateData.ext}`
        return path.join(savePath, showFolder, itemFileName)
      }

      let plannedPath
      if (forPlex) {
        plannedPath = await plexFilePath({ savePath, showFolder, templateData, listFoldersOf })
        if (!plannedPath) {
          logWarning(`${item.title} has no season and episode number. Using the default name for it.`)
          plannedPath = defaultFilePath()
        }
      } else if (template) {
        try {
          const relativePath = processPathTemplate({ templateString: template, data: templateData })
          plannedPath = path.resolve(savePath, relativePath)
        } catch (err) {
          logWarning(`${err.message} Using the default name for ${item.title}.`)
          plannedPath = defaultFilePath()
        }
      } else {
        plannedPath = defaultFilePath()
      }

      const target = await chooseSaveTarget({ plannedPath, savePath, item, savedFilesDb, ownerByPath, claimedPaths })
      filePath = target.filePath
      claimedPaths.add(filePath)
      if (target.takenPath) {
        log(chalk.yellow(`${path.relative(savePath, target.takenPath)} already exists and fetchtv did not save it. Saving as ${path.basename(filePath)} instead.`))
      }
      showDirPath = path.dirname(filePath)
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
        jsonResults.push({
          item: formatItem(item),
          recorded: false,
          status: SAVE_STATUS.LOCKED,
          warning: 'Skipped (lock file active or stale lock removal failed)'
        })
        continue
      }

      const savedEntry = savedFilesDb[item.id]
      if (savedEntry && !overwrite) {
        const isComplete = await isSavedFileComplete({ filePath, savedEntry, item })
        if (isComplete) {
          log(chalk.gray(`Skipping already saved: ${show.title} / ${item.title}`))
          jsonResults.push({
            item: formatItem(item),
            recorded: false,
            status: SAVE_STATUS.ALREADY_SAVED,
            warning: 'Skipped (already saved and complete)'
          })
          continue
        }
        log(chalk.yellow(`Saved file is missing or incomplete, saving again: ${show.title} / ${item.title}`))
      }

      tasks.push({ item, filePath, showDirPath, showTitle: show.title })
    }
  }

  if (tasks.length === 0) {
    log('There is nothing new to record or resume.')
    return jsonResults
  }

  log(`Saving ${tasks.length} recording${tasks.length > 1 ? 's' : ''}…`)

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
        status: SAVE_STATUS.FAILED,
        error: `Directory creation failed: ${mkdirErr.message}`
      })
      continue
    }

    while (activePromises.size >= concurrency) {
      await Promise.race(activePromises)
    }

    const progressBar = multiBar.create(task.item.size || 1, 0, {
      filename: chalk.whiteBright(path.basename(task.filePath).slice(0, 30).padEnd(30)),
      speed: 'N/A',
      pbETA: 'N/A',
      pbValue: filesize(0, { spacer: '' }),
      pbTotal: filesize(task.item.size || 0, { spacer: '' })
    })

    progressBar.options.format = `{filename} |${chalk.cyan('{bar}')}| {percentage}% | {pbValue}/{pbTotal} | {speed}/s | ETA: {pbETA}`

    if (isPositiveSize(task.item.size)) {
      await recordSaveStart({ savePath, savedFilesDb, item: task.item, filePath: task.filePath })
    }

    const promise = downloadFileWithRetries({ item: task.item, filePath: task.filePath, progressBar, overwrite, retryDelays })
      .then(async (downloadResult) => {
        const resultEntry = {
          item: formatItem(task.item),
          recorded: downloadResult.recorded,
          resumed: downloadResult.resumed || false,
          status: getDownloadStatus(downloadResult)
        }

        if (downloadResult.attempts > 1) resultEntry.attempts = downloadResult.attempts
        if (downloadResult.warning) resultEntry.warning = downloadResult.warning
        if (downloadResult.error) resultEntry.error = downloadResult.error
        jsonResults.push(resultEntry)

        if (downloadResult.recorded) {
          await addSavedFile({ savePath, savedFilesDb, item: task.item, size: downloadResult.size, filePath: task.filePath })
        }
      })
      .catch((error) => {
        logError(`Unexpected error processing save result for ${task.item.title}: ${error.message}`)
        jsonResults.push({
          item: formatItem(task.item),
          recorded: false,
          status: SAVE_STATUS.FAILED,
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

const downloadFileWithRetries = async ({ item, filePath, progressBar, overwrite, retryDelays = DOWNLOAD_RETRY_DELAYS }) => {
  const maxAttempts = retryDelays.length + 1
  let result = await downloadFile({ item, filePath, progressBar, overwrite })
  let attempts = 1

  while (result.retryable && attempts < maxAttempts && !isShuttingDown) {
    const delay = retryDelays[attempts - 1]
    attempts++
    log(chalk.yellow(`Retrying ${item.title} in ${formatSeconds(delay)} (attempt ${attempts} of ${maxAttempts})…`))
    await new Promise(resolve => setTimeout(resolve, delay))
    result = await downloadFile({ item, filePath, progressBar, overwrite: false })
  }

  return { ...result, attempts }
}

const getDownloadStatus = (downloadResult) => {
  if (downloadResult.recorded) return SAVE_STATUS.SAVED
  if (downloadResult.stillRecording) return SAVE_STATUS.STILL_RECORDING
  if (downloadResult.locked) return SAVE_STATUS.LOCKED
  return SAVE_STATUS.FAILED
}

const isSavedFileComplete = async ({ filePath, savedEntry, item }) => {
  try {
    const stats = await fs.stat(filePath)
    const expectedSizes = [getSavedEntrySize(savedEntry), item.size].filter(isPositiveSize)
    return expectedSizes.every(expectedSize => stats.size >= expectedSize - SIZE_TOLERANCE)
  } catch (err) {
    if (err.code !== 'ENOENT') debug('Error checking saved file %s: %O', filePath, err)
    return false
  }
}

const getSavedEntrySize = (savedEntry) =>
  typeof savedEntry === 'object' && savedEntry !== null ? savedEntry.size : undefined

const isPositiveSize = (size) => Number.isFinite(size) && size > 0

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

  if (!sortedRecordings || sortedRecordings.length === 0) {
    logWarning(`No ${context} found matching criteria!`)
    return logWarning('A Fetch box whose account is cancelled shows its setup screen on the TV and lists no recordings.')
  }

  sortedRecordings.forEach(recording => {
    const bullet = showsOnly ? '' : '📁 '
    log(chalk.green(`${bullet}${recording.title}`))
    if (!recording.items || recording.items.length === 0) {
      if (!showsOnly) log(chalk.gray('  (No items listed based on current filters)'))
      return
    }

    recording.items.forEach(item => {
      const durationStr = new Date(item.duration * 1000).toISOString().substr(11, 8)
      const sizeFormatted = filesize(item.size, { spacer: '' })
      log(`  ${chalk.whiteBright(item.title)} ${chalk.gray(`${durationStr} ${sizeFormatted}`)}`)
    })
  })
}

const handleSaveAction = async ({ recordings, savePath, template, forPlex, overwrite, concurrency, jsonOutput, retryDelays }) => {
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

    const results = await saveRecordings({ recordings, savePath, template, forPlex, overwrite, concurrency, retryDelays })
    const summary = summariseSaveResults(results)

    if (summary.failed > 0) process.exitCode = 1

    if (jsonOutput) {
      logHeading('Start JSON Output', 'greenBright')
      console.log(JSON.stringify({ summary, results }, null, 2))
      logHeading('End JSON Output', 'greenBright')
    }

    const summaryLine = formatSaveSummary(summary)
    if (summary.remaining > 0) logWarning(summaryLine)
    else log(chalk.green(summaryLine))

    return summary
  } catch (saveError) {
    logError(`Error during save process: ${saveError.message}`)
    if (argv.debug) console.error(saveError.stack)
    process.exit(1)
  }
}

const summariseSaveResults = (results) => {
  const countStatus = (status) => results.filter(result => result.status === status).length
  const summary = {
    total: results.length,
    saved: countStatus(SAVE_STATUS.SAVED),
    alreadySaved: countStatus(SAVE_STATUS.ALREADY_SAVED),
    failed: countStatus(SAVE_STATUS.FAILED),
    stillRecording: countStatus(SAVE_STATUS.STILL_RECORDING),
    locked: countStatus(SAVE_STATUS.LOCKED)
  }
  return { ...summary, remaining: summary.failed + summary.stillRecording + summary.locked }
}

const formatSaveSummary = ({ total, saved, alreadySaved, failed, stillRecording, locked, remaining }) => {
  const savedPart = alreadySaved > 0
    ? `Saved ${saved} of ${total}, ${alreadySaved} already saved.`
    : `Saved ${saved} of ${total}.`
  const leftParts = [
    failed > 0 && `${failed} failed`,
    stillRecording > 0 && `${stillRecording} still recording`,
    locked > 0 && `${locked} in use by another save`
  ].filter(Boolean)

  if (remaining === 0) return savedPart
  return `${savedPart} ${leftParts.join(', ')}. Run the same command again to finish the remaining ${remaining}.`
}

const isValidConcurrency = (value) =>
  Number.isInteger(value) && value >= MIN_CONCURRENT_DOWNLOADS && value <= MAX_CONCURRENT_DOWNLOADS

const formatSeconds = (ms) => `${Math.round(ms / 1000)} s`

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
  debug('Sync cleanup: Removing %d active lock file(s)…', activeLockFiles.size)
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

      console.log(chalk.yellow('\nInterrupt signal received. Attempting graceful shutdown…'))

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
      if (!resultXml || !('DIDL-Lite' in resultXml)) {
        if (attempt > 1) logWarning(`Failed to parse embedded DIDL-Lite XML for ObjectID ${objectId} on attempt ${attempt-1}`)
        lastError = new Error('Failed to parse embedded DIDL-Lite XML')
        continue
      }

      debug('Browse Response DIDL-Lite (ObjectID: %s, Attempt: %d): %O', objectId, attempt, resultXml['DIDL-Lite'])

      return resultXml['DIDL-Lite'] || {}

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

    let ext = 'ts'
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
  // Fetch TV uses size <= 0 (typically -1) as an "in progress, size unknown" sentinel
  // for items whose recording has started but not finished. Treat as definitely-recording
  // without a HEAD probe — the item URL may still serve partial bytes, so HEAD would lie.
  if (!Number.isFinite(item.size) || item.size <= 0) {
    debug('Item %s has non-positive size (%s); treating as currently recording.', item.title, item.size)
    return true
  }
  if (item.size > 0 && item.size < MAX_OCTET_RECORDING - 1000000) {
    debug('Skipping recording check for %s, size (%s) seems final.', item.title, filesize(item.size, { spacer: '' }))
    return false
  }
  if (item.size === MAX_OCTET_RECORDING) {
    debug('Item size for %s matches recording marker exactly (%s).', item.title, filesize(item.size, { spacer: '' }))
    return true
  }

  debug('Performing HEAD/GET request to check recording status for %s (initial size: %s)', item.title, filesize(item.size, { spacer: '' }))

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
      debug('HEAD failed for %s, attempting GET fallback check…', item.title)
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
  // Refuse to download items whose UPnP listing reports a non-positive size
  // (Fetch TV's "-1" sentinel for in-progress recordings) — the stream would
  // serve truncated bytes and we'd silently end up with an incomplete file.
  if (!Number.isFinite(item.size) || item.size <= 0) {
    logWarning(`Skipping ${item.title}, size is non-positive (${item.size}), likely still recording.`)
    return { recorded: false, stillRecording: true, warning: `Skipping item, size ${item.size} indicates it's currently recording` }
  }

  const lockFilePath = `${filePath}${CONST_LOCK}`
  const partFilePath = `${filePath}${PART_SUFFIX}`
  let writer = null
  let responseStream = null
  let existingSize = 0
  let isResuming = false
  let validResumable = false
  let totalLength = 0

  try {
    try {
      if (fsc.existsSync(lockFilePath)) {
        const isStale = await isLockFileStale(lockFilePath)
        if (isStale) {
          log(chalk.yellow(`Removing stale lock file during download setup: ${path.basename(lockFilePath)}`))
          fsc.unlinkSync(lockFilePath)
        } else {
          logError(`Active lock file found for ${item.title}. Aborting download.`)
          if (progressBar) progressBar.stop()
          return { recorded: false, locked: true, error: 'Active lock file found, download aborted.' }
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

    try {
      if (overwrite) {
        if (fsc.existsSync(partFilePath)) fsc.unlinkSync(partFilePath)
        if (fsc.existsSync(filePath)) log(chalk.yellow(`Overwriting: ${item.title}`))
      } else {
        await adoptInPlacePartial({ item, filePath, partFilePath })
        existingSize = fsc.existsSync(partFilePath) ? (await fs.stat(partFilePath)).size : 0
        isResuming = existingSize > 0
        validResumable = isResuming
        if (isResuming) debug('Partial file exists, attempting resume from %s', filesize(existingSize, { spacer: '' }))
      }
    } catch (statErr) {
      debug('Error checking file stats for resume: %O', statErr)
      logWarning(`Could not check existing file status for ${item.title}: ${statErr.message}. Starting fresh download.`)
      existingSize = 0
      isResuming = false
    }

    if (isResuming && validResumable) {
      log(`${chalk.cyan(`Resuming download for: ${path.basename(filePath)}`)} ${chalk.gray(`(from ${filesize(existingSize, { spacer: '' })})`)}`)
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
      timeout: isResuming ? RESUME_TIMEOUT : REQUEST_TIMEOUT * 20,
      headers,
      validateStatus: function (status) {
        return status === 200 || status === 206 || (isResuming && status === RANGE_NOT_SATISFIABLE)
      },
    })

    responseStream = response.data

    if (response.status === RANGE_NOT_SATISFIABLE) {
      responseStream.destroy()
      if (progressBar) {
        progressBar.update(progressBar.getTotal())
        progressBar.stop()
      }
      const servedSize = Math.max(rangeNotSatisfiableTotal(response) ?? 0, item.size)
      if (existingSize >= servedSize - SIZE_TOLERANCE) {
        debug('Box answered the resume of %s with 416: the partial file already holds every byte it serves.', item.title)
        return await promotePartFile({ item, partFilePath, filePath, lockFilePath, size: existingSize, resumed: true })
      }
      return discardRejectedPartial({ item, partFilePath, lockFilePath, existingSize, servedSize })
    }

    if (response.status === 206) {
      const contentRange = response.headers['content-range']
      if (contentRange && contentRange.includes('/')) {
        totalLength = parseInt(contentRange.split('/')[1], 10)
        debug('Partial content detected. Total length from Content-Range: %d (%s)', totalLength, filesize(totalLength, { spacer: '' }))
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
          fsc.unlinkSync(partFilePath)
          debug('Deleted existing partial file due to 200 OK on resume attempt.')
        } catch(unlinkErr) {
          logWarning(`Could not clear existing file after failed resume attempt: ${unlinkErr.message}`)
        }
      }
      debug('Full content response. Total length from Content-Length: %d (%s)', totalLength, filesize(totalLength, { spacer: '' }))
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
      return { recorded: false, stillRecording: true, warning: 'Skipping item, size indicates it\'s currently recording' }
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

    writer = fsc.createWriteStream(partFilePath, { flags: isResuming ? 'a' : 'w' })

    writer.on('open', () => debug('Write stream opened for %s with flags: %s', partFilePath, writer.flags))
    writer.on('close', () => debug('Write stream closed for %s', partFilePath))

    if (progressBar) {
      const barTotal = Math.max(totalLength || 0, existingSize, 1)
      progressBar.setTotal(barTotal)
      progressBar.update(existingSize, {
        speed: 'N/A',
        pbETA: 'N/A',
        pbValue: filesize(existingSize, { spacer: 0 }),
        pbTotal: filesize(barTotal, { spacer: 0 })
      })
      debug('Progress bar initialized. Current: %d, Total: %d', existingSize, barTotal)
    }

    let downloadedLength = existingSize
    let hasStreamFailed = false
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
            speed: filesize(speed, { spacer: 0 }),
            pbETA: etaSeconds === Infinity ? '∞' : prettyMs(etaSeconds * 1000, { secondsDecimalDigits: 0 }),
            pbValue: filesize(currentProgressBarValue, { spacer: 0 }),
            pbTotal: filesize(progressBar.getTotal(), { spacer: 0 })
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
        if (hasStreamFailed) return
        debug('Write stream finished for %s', item.title)
        if (progressBar) progressBar.stop()

        await new Promise(resolve => setTimeout(resolve, 150))

        const lockWarning = removeLockFileAfterDownload({ item, lockFilePath })
        const expectedSize = Math.max(totalLength, isPositiveSize(item.size) ? item.size : 0)

        let finalSize = -1
        try {
          const finalStats = await fs.stat(partFilePath)
          finalSize = finalStats.size
          debug('Final file size for %s: %d bytes (expected %d)', item.title, finalSize, expectedSize)
        } catch (verifyErr) {
          logError(`Error verifying final file size for ${item.title}: ${verifyErr.message}`)
          debug('File size verification error details: %O', verifyErr)
          resolve({
            recorded: false,
            resumed: isResuming,
            retryable: true,
            warning: `Write finished, but failed to verify final size: ${verifyErr.message}`
          })
          return
        }

        const sizeDiff = Math.abs(finalSize - expectedSize)
        if (expectedSize > 0 && sizeDiff > SIZE_TOLERANCE) {
          const sizeWarning = `File size mismatch: final=${filesize(finalSize, { spacer: '' })}, expected=${filesize(expectedSize, { spacer: '' })}`
          logWarning(`${item.title} is incomplete. ${sizeWarning}. Partial file kept for resume.`)
          resolve({
            recorded: false,
            resumed: isResuming,
            retryable: finalSize < expectedSize,
            warning: sizeWarning
          })
          return
        }

        if (expectedSize === 0 && finalSize <= 0) {
          logWarning(`${item.title} finished with an empty file.`)
          resolve({ recorded: false, resumed: isResuming, retryable: true, warning: 'Download finished with an empty file' })
          return
        }

        try {
          await fs.rename(partFilePath, filePath)
        } catch (renameErr) {
          logError(`Could not move ${path.basename(partFilePath)} to ${path.basename(filePath)}: ${renameErr.message}`)
          resolve({ recorded: false, resumed: isResuming, warning: `Saved, but could not rename the partial file: ${renameErr.message}` })
          return
        }

        if (progressBar) progressBar.update(progressBar.getTotal())
        const successResult = { recorded: true, resumed: isResuming, size: finalSize }
        if (lockWarning) successResult.warning = lockWarning
        resolve(successResult)
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
        if (totalLength > SIZE_TOLERANCE && downloadedLength >= totalLength - SIZE_TOLERANCE) {
          debug('Box closed the connection for %s %d bytes short of its stated size; treating the copy as complete.', item.title, totalLength - downloadedLength)
          if (progressBar) progressBar.update(progressBar.getTotal(), { pbValue: filesize(downloadedLength, { spacer: 0 }) })
          if (writer && !writer.writableEnded) writer.end()
          return
        }
        hasStreamFailed = true
        const completionPercentage = totalLength > 0 ? Math.min(100, (downloadedLength / totalLength) * 100) : 0
        debug('Response Stream Error Details (%s): Code: %s, Message: %s', item.title, err.code, err.message)

        if (progressBar) {
          progressBar.update(Math.min(downloadedLength, progressBar.getTotal()))
          progressBar.stop()
        }

        const interruptedWarning = describeInterruption({ item, err, completionPercentage })
        const interruptedResult = {
          recorded: false,
          resumed: isResuming,
          retryable: true,
          warning: `${interruptedWarning} Partial file kept for resume.`
        }

        const finishInterrupted = () => {
          removeLockFileAfterDownload({ item, lockFilePath })
          logError(interruptedResult.warning)
          resolve(interruptedResult)
        }

        if (writer && !writer.closed) {
          writer.end(() => {
            debug('Writer stream closed after response stream error for %s.', item.title)
            finishInterrupted()
          })
        } else {
          finishInterrupted()
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
    if (isResumeTimeoutAtEnd({ error, isResuming, existingSize, item })) {
      debug('Resume of %s timed out with the partial file already at the listed size; treating the copy as complete.', item.title)
      if (progressBar) {
        progressBar.update(progressBar.getTotal())
        progressBar.stop()
      }
      return await promotePartFile({ item, partFilePath, filePath, lockFilePath, size: existingSize, resumed: true })
    }
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

    return { recorded: false, retryable: !error.response, error: `Download initiation failed: ${error.message}` }
  }
}

const rangeNotSatisfiableTotal = (response) => {
  const total = parseInt(String(response.headers['content-range'] ?? '').split('/')[1], 10)
  return isPositiveSize(total) ? total : null
}

const discardRejectedPartial = ({ item, partFilePath, lockFilePath, existingSize, servedSize }) => {
  const warning = `The Fetch box rejected the resume of ${item.title} at ${filesize(existingSize, { spacer: '' })} of ${filesize(servedSize, { spacer: '' })}. Partial file discarded; the next attempt starts again.`
  logWarning(warning)
  try {
    fsc.unlinkSync(partFilePath)
  } catch (unlinkErr) {
    logWarning(`Could not delete ${path.basename(partFilePath)}: ${unlinkErr.message}`)
  }
  removeLockFileAfterDownload({ item, lockFilePath })
  return { recorded: false, resumed: true, retryable: true, warning }
}

const isResumeTimeoutAtEnd = ({ error, isResuming, existingSize, item }) =>
  isResuming && !error.response && error.code === 'ECONNABORTED' &&
  isPositiveSize(item.size) && existingSize >= item.size - SIZE_TOLERANCE

const promotePartFile = async ({ item, partFilePath, filePath, lockFilePath, size, resumed }) => {
  const lockWarning = removeLockFileAfterDownload({ item, lockFilePath })
  try {
    await fs.rename(partFilePath, filePath)
  } catch (renameErr) {
    logError(`Could not move ${path.basename(partFilePath)} to ${path.basename(filePath)}: ${renameErr.message}`)
    return { recorded: false, resumed, warning: `Saved, but could not rename the partial file: ${renameErr.message}` }
  }
  const result = { recorded: true, resumed, size }
  if (lockWarning) result.warning = lockWarning
  return result
}

const adoptInPlacePartial = async ({ item, filePath, partFilePath }) => {
  if (fsc.existsSync(partFilePath) || !fsc.existsSync(filePath)) return
  const { size } = await fs.stat(filePath)
  const isShorterThanRecording = size > 0 && isPositiveSize(item.size) && size < item.size - SIZE_TOLERANCE
  if (!isShorterThanRecording) return
  await fs.rename(filePath, partFilePath)
  debug('Moved in-place partial %s to %s', filePath, partFilePath)
}

const describeInterruption = ({ item, err, completionPercentage }) => {
  const message = `Download interrupted for ${item.title} at ${completionPercentage.toFixed(1)}%.`
  if (err.code === 'ECONNRESET') return `${message} (Connection reset)`
  if (err.code === 'ECONNABORTED' || err.message?.toLowerCase().includes('timeout')) return `${message} (Timeout)`
  if (err.message?.includes('Premature close')) return `${message} (Premature close)`
  return `${message} (Error: ${err.message})`
}

const removeLockFileAfterDownload = ({ item, lockFilePath }) => {
  try {
    if (fsc.existsSync(lockFilePath)) {
      fsc.unlinkSync(lockFilePath)
      untrackLockFile(lockFilePath)
      debug('Removed lock file after download: %s', lockFilePath)
    }
    return null
  } catch (unlinkErr) {
    logWarning(`Could not remove lock file for ${item.title}: ${unlinkErr.message}`)
    return `Save complete, but failed to remove lock file: ${unlinkErr.message}`
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
    allowBooleanAttributes: true,
    processEntities: { enabled: true }
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
    allowBooleanAttributes: true,
    processEntities: { enabled: true }
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
    .replace(/[<>:"/\\|?*\x00-\x1F]/g, '')
    .replace(/\s+/g, ' ')
    .trim()
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

const addSavedFile = async ({ savePath, savedFilesDb, item, size, filePath }) => {
  const savedSize = isPositiveSize(size) ? size : item.size
  savedFilesDb[item.id] = {
    title: item.title,
    ...(isPositiveSize(savedSize) && { size: savedSize }),
    ...savedPathField({ savePath, filePath })
  }
  await writeSavedFiles({ savePath, savedFilesDb })
}

const recordSaveStart = async ({ savePath, savedFilesDb, item, filePath }) => {
  savedFilesDb[item.id] = { title: item.title, ...savedPathField({ savePath, filePath }) }
  await writeSavedFiles({ savePath, savedFilesDb })
}

const savedPathField = ({ savePath, filePath }) =>
  filePath ? { path: toSavedPath({ savePath, filePath }) } : {}

const toSavedPath = ({ savePath, filePath }) =>
  path.relative(savePath, filePath).split(path.sep).join('/')

const savedFileWrites = new Map()

const writeSavedFiles = ({ savePath, savedFilesDb }) => {
  const dbPath = path.join(savePath, SAVE_FILE_NAME)
  const previous = savedFileWrites.get(dbPath) ?? Promise.resolve()
  const next = previous.then(() => replaceSavedFiles({ savePath, dbPath, savedFilesDb }))
  savedFileWrites.set(dbPath, next)
  return next
}

const replaceSavedFiles = async ({ savePath, dbPath, savedFilesDb }) => {
  const tempPath = `${dbPath}.tmp`
  try {
    await fs.mkdir(savePath, { recursive: true })
    await fs.writeFile(tempPath, JSON.stringify(savedFilesDb, null, 2), 'utf-8')
    await fs.rename(tempPath, dbPath)
  } catch (error) {
    logError(`Error writing ${SAVE_FILE_NAME}: ${error.message}`)
  }
}

const ownersBySavedPath = (savedFilesDb) => new Map(
  Object.entries(savedFilesDb)
    .filter(([, entry]) => typeof entry?.path === 'string')
    .map(([id, entry]) => [entry.path, id])
)

const isLegacySavedEntry = (entry) =>
  typeof entry === 'string' || (typeof entry === 'object' && entry !== null && typeof entry.path !== 'string')

const chooseSaveTarget = async ({ plannedPath, savePath, item, savedFilesDb, ownerByPath, claimedPaths }) => {
  const itemId = String(item.id)
  const entry = savedFilesDb[item.id]
  const recordedPath = typeof entry?.path === 'string' ? path.resolve(savePath, entry.path) : null
  const isRecordedCopy = recordedPath && withoutCopySuffix(recordedPath) === plannedPath && !claimedPaths.has(recordedPath)
  if (isRecordedCopy) return { filePath: recordedPath, takenPath: null }

  for (let copy = 1; ; copy++) {
    const filePath = withCopySuffix(plannedPath, copy)
    const owner = ownerByPath.get(toSavedPath({ savePath, filePath }))
    const takenPath = copy > 1 ? plannedPath : null
    if (claimedPaths.has(filePath)) continue
    if (owner && owner !== itemId) continue
    if (owner === itemId || (copy === 1 && isLegacySavedEntry(entry))) return { filePath, takenPath }
    if (!(await anyExists([filePath, `${filePath}${PART_SUFFIX}`]))) return { filePath, takenPath }
  }
}

const withCopySuffix = (filePath, copy) => {
  if (copy <= 1) return filePath
  const ext = path.extname(filePath)
  return path.join(path.dirname(filePath), `${path.basename(filePath, ext)} - copy ${copy}${ext}`)
}

const withoutCopySuffix = (filePath) => {
  const ext = path.extname(filePath)
  const base = path.basename(filePath, ext).replace(/ - copy \d+$/, '')
  return path.join(path.dirname(filePath), `${base}${ext}`)
}

const anyExists = async (filePaths) => {
  const found = await Promise.all(filePaths.map(filePath => fs.lstat(filePath).then(() => true, () => false)))
  return found.some(Boolean)
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

const EPISODE_TAG_PATTERN = /S(?:eason)?\s*\d{1,3}\s*E(?:pisode)?\s*\d{1,3}/i

const buildDefaultFileBase = ({ recording_title, season_number_padded, episode_number_padded }) => {
  const hasEpisodeNumbers = season_number_padded && episode_number_padded
  if (!hasEpisodeNumbers || EPISODE_TAG_PATTERN.test(recording_title)) return recording_title
  return `S${season_number_padded}E${episode_number_padded} - ${recording_title}`
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

  const emptyPlaceholders = Object.keys(placeholders).filter(placeholder => !placeholders[placeholder])
  for (const emptyPlaceholder of emptyPlaceholders) {
    if (processedPath.includes(emptyPlaceholder)) {
      throw new Error(`Template contains placeholder "${emptyPlaceholder}" without a matching value.`)
    }
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
    logWarning(`Generated path seems very long, potentially problematic: ${processedPath.slice(0, 100)}…`)
  }

  debugTemplate('Final processed path segment: %s', processedPath)
  return processedPath
}

const log = message => console.log(message)
const logWarning = message => console.log(chalk.yellow.bold(message))
const logError = message => console.log(chalk.red.bold(message))
const logHeading = (title, color = 'blueBright') => console.log(chalk[color].bold(`=== ${title} ===`))

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
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
}

export {
  discoverFetchServers,
  discoverFetch,
  getFetchRecordings,
  downloadFile,
  isCurrentlyRecording,
  formatItem,
  createValidFilename,
  processPathTemplate,
  parseXml,
  parseLocations,
  findNode,
  getXmlAttr,
  getXmlText,
  processFilter,
  sortRecordingsByTitle,
  tsToSeconds,
  getApiService,
  findItems,
  findDirectories,
  browseRequest,
  isLockFileStale,
  loadSavedFiles,
  addSavedFile,
  createRequestManager,
  saveRecordings,
  handleSaveAction,
  summariseSaveResults,
  formatSaveSummary,
  httpClient,
}
