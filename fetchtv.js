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
    .epilog('Note: Comma-separated values for filters (--show, --exclude, --title) are NOT supported. Instead, repeat option as needed.')
    .wrap(process.stdout.columns ? Math.min(process.stdout.columns, 100) : 100)
    .argv

  if (argv.debug) {
    debugLib.enable('fetchtv*')
    logWarning('*** Debug Mode Enabled ***')
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
    const recordings = await getFetchRecordings(fetchServer, filters)

    if (!argv.save) {
      printRecordings(recordings, { jsonOutput: argv.json, showsOnly: hasShowsCommand })
    } else {
      await handleSaveAction(recordings, {
        savePath: path.resolve(argv.save),
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

const getFetchRecordings = async (location, { folderFilter, excludeFilter, titleFilter, showsOnly, isRecordingFilter }) => {
  const apiService = await getApiService(location)
  if (!apiService) {
    logError('Could not find "ContentDirectory" service.')
    return []
  }

  const requestManager = createRequestManager({
    debug: argv.debug,
    initialConcurrency: 2,
    initialDelay: 150,
    adaptiveDelayFactor: 1.2
  })

  const baseFolders = await requestManager.enqueue(
    () => findDirectories(apiService, '0'),
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
    () => findDirectories(apiService, recordingsFolder.id),
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
      let items = await requestManager.enqueue(() => findItems(apiService, show.id))

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

const handleSaveAction = async (recordings, { savePath, overwrite, jsonOutput }) => {
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

    const jsonResult = await saveRecordings(recordings, { savePath, overwrite })

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

const saveRecordings = async (recordings, { savePath, overwrite }) => {
  const savedFilesDb = await loadSavedFiles(savePath)
  const jsonResults = []
  const tasks = []

  for (const show of recordings) {
    if (!show.items || show.items.length === 0) continue

    for (const item of show.items) {
      const showDirName = createValidFilename(show.title)
      const showDirPath = path.join(savePath, showDirName)
      const itemFileName = `${createValidFilename(item.title)}.mpeg`
      const filePath = path.join(showDirPath, itemFileName)
      const lockFilePath = `${filePath}${CONST_LOCK}`

      if (fsc.existsSync(lockFilePath)) {
        const isStale = await isLockFileStale(lockFilePath)
        if (isStale) {
          log(chalk.yellow(`Removing stale lock file for ${item.title}`))
          try {
            fsc.unlinkSync(lockFilePath)
            debug('Removed stale lock file: %s', lockFilePath)
          } catch (err) {
            debug('Error removing stale lock file: %O', err)
          }
        }
      }
    }
  }

  for (const show of recordings) {
    if (!show.items || show.items.length === 0) continue

    for (const item of show.items) {
      const showDirName = createValidFilename(show.title)
      const showDirPath = path.join(savePath, showDirName)
      const itemFileName = `${createValidFilename(item.title)}.mpeg`
      const filePath = path.join(showDirPath, itemFileName)
      const lockFilePath = `${filePath}${CONST_LOCK}`
      const lockExists = fsc.existsSync(lockFilePath)
      const isCompletedFile = savedFilesDb[item.id] && !overwrite

      let hasPartialFile = false
      try {
        if (fsc.existsSync(filePath) && !isCompletedFile) {
          const stats = await fs.stat(filePath)
          hasPartialFile = stats.size > 0 && stats.size < item.size
        }
      } catch (err) {
        debug('Error checking for partial file %s: %O', filePath, err)
      }

      if (lockExists) {
        log(chalk.gray(`Already writing ${item.title} (lock file exists), skipping.`))
        if (argv.json) jsonResults.push({
          item: formatItem(item),
          recorded: false,
          warning: 'Already writing (lock file exists) skipping'
        })
        continue
      }

      if (isCompletedFile && !hasPartialFile) {
        log(chalk.gray(`Skipping already saved: ${show.title} / ${item.title}`))
        if (argv.json) jsonResults.push({
          item: formatItem(item),
          recorded: false,
          warning: 'Skipped (already saved)'
        })
        continue
      }

      tasks.push({ item, filePath, showDirPath, showTitle: show.title })
    }
  }

  if (tasks.length === 0) {
    log('There is nothing new to record.')
    return jsonResults
  }

  log(`Saving ${tasks.length} new recording${tasks.length > 1 ? 's' : ''}…`)

  progressBarActive = true
  const multiBar = new cliProgress.MultiBar({
    clearOnComplete: false,
    hideCursor: true,
    format: ' {bar} | {percentage}% | {filename}'
  }, cliProgress.Presets.shades_classic)

  activeMultiBar = multiBar
  const activePromises = new Set()

  for (const task of tasks) {
    while (activePromises.size >= MAX_CONCURRENT_DOWNLOADS)
      await Promise.race(activePromises)

    const progressBar = multiBar.create(task.item.size || 0, 0, {
      filename: chalk.whiteBright(path.basename(task.filePath).slice(0, 20).padEnd(20)),
      speed: 'N/A',
      eta: 'N/A',
      value: filesize(0),
      total: filesize(task.item.size || 0)
    })

    progressBar.options.format = `{filename} |${chalk.cyan('{bar}')}| {percentage}% | {value}/{total} | {speed}/s | ETA: {eta}`

    const promise = downloadFile(task.item, task.filePath, progressBar, overwrite)
      .then(async (downloadResult) => {
        const resultEntry = {
          item: formatItem(task.item),
          recorded: downloadResult.recorded,
          resumed: downloadResult.resumed || false
        }

        if (downloadResult.warning) resultEntry.warning = downloadResult.warning
        if (downloadResult.error) resultEntry.error = downloadResult.error
        jsonResults.push(resultEntry)

        if (downloadResult.recorded)
          await addSavedFile(savePath, savedFilesDb, task.item)
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
    if (!isShuttingDown) {
      multiBar.stop()
      activeMultiBar = null
      progressBarActive = false
    }
  }

  return jsonResults
}

const downloadFile = async (item, filePath, progressBar, overwrite = false) => {
  const lockFilePath = `${filePath}${CONST_LOCK}`
  let writer = null
  let responseStream = null
  let existingSize = 0
  let isResuming = false
  let validResumable = false
  let totalLength = 0

  try {
    await fs.mkdir(path.dirname(filePath), { recursive: true })

    try {
      if (fsc.existsSync(filePath)) {
        if (overwrite) {
          console.log(chalk.yellow(`Overwriting: ${item.title}`))
          fsc.unlinkSync(filePath)
          isResuming = false
          existingSize = 0
        } else {
          const stats = await fs.stat(filePath)
          existingSize = stats.size
          if (existingSize > 0) {
            try {
              const fileHandle = await fs.open(filePath, 'r')
              const buffer = Buffer.alloc(Math.min(8192, existingSize))
              await fileHandle.read(buffer, 0, buffer.length, Math.max(0, existingSize - buffer.length))
              await fileHandle.close()

              isResuming = true
              validResumable = true
            } catch (validErr) {
              debug('Error validating file for resumption: %O', validErr)
              isResuming = false
              existingSize = 0
            }
          }
        }
      }
    } catch (statErr) {
      debug('Error checking file stats for resume: %O', statErr)
      existingSize = 0
      isResuming = false
    }

    try {
      if (fsc.existsSync(lockFilePath)) {
        fsc.unlinkSync(lockFilePath)
        debug('Removed existing lock file: %s', lockFilePath)
      }
    } catch (lockErr) {
      debug('Error removing existing lock file: %O', lockErr)
    }

    fsc.writeFileSync(lockFilePath, '')
    trackLockFile(lockFilePath)

    if (isResuming && validResumable) {
      console.log(`${chalk.cyan(`Resuming download for: ${item.title}`)} ${chalk.gray(`(from ${filesize(existingSize)})`)}`)
    } else if (isResuming && !validResumable) {
      console.log(chalk.yellow(`Cannot resume potentially corrupted file for ${item.title}, restarting download`))
      isResuming = false
      existingSize = 0
    }

    const headers = {}
    if (isResuming && existingSize > 0) headers.Range = `bytes=${existingSize}-`

    const response = await httpClient.get(item.url, {
      responseType: 'stream',
      timeout: REQUEST_TIMEOUT * 20,
      headers
    })

    responseStream = response.data
    totalLength = isResuming
      ? parseInt(response.headers['content-length'] || '0', 10) + existingSize
      : parseInt(response.headers['content-length'] || '0', 10)

    debug('Download Headers for %s: %O', item.title, response.headers)
    debug('Resume Status: %s, existing size: %d, response status: %d',
          isResuming ? 'yes' : 'no', existingSize, response.status)

    if (totalLength === MAX_OCTET_RECORDING) {
      logWarning(`Skipping ${item.title}, it appears to be currently recording.`)
      responseStream.destroy()

      try {
        fsc.unlinkSync(lockFilePath)
        untrackLockFile(lockFilePath)
      } catch (delErr) {
        logWarning(`Could not delete lock file on skip ${lockFilePath}: ${delErr.message}`)
      }

      if (progressBar) progressBar.stop()
      return { recorded: false, warning: "Skipping item, it's currently recording" }
    }

    if (totalLength === 0 && !isResuming) {
      logWarning(`Skipping ${item.title}, content length is zero.`)
      responseStream.destroy()

      try {
        fsc.unlinkSync(lockFilePath)
        untrackLockFile(lockFilePath)
      } catch (delErr) {
        logWarning(`Could not delete lock file on zero size ${lockFilePath}: ${delErr.message}`)
      }

      if (progressBar) progressBar.stop()
      return { recorded: false, warning: 'Skipping item, content length is zero' }
    }

    writer = isResuming
      ? fsc.createWriteStream(filePath, { flags: 'a' })
      : fsc.createWriteStream(filePath)

    if (progressBar) {
      progressBar.setTotal(totalLength)
      progressBar.update(existingSize, {
        speed: 'N/A',
        eta: 'N/A',
        value: filesize(existingSize),
        total: filesize(totalLength)
      })
    }

    let downloadedLength = existingSize
    let lastUpdateTime = progressBar?.startTime || Date.now()

    responseStream.on('data', (chunk) => {
      if (isShuttingDown) return
      downloadedLength += chunk.length
      if (!progressBar) return

      const now = Date.now()
      const startTime = progressBar.startTime || lastUpdateTime
      if (now - lastUpdateTime > 250 || downloadedLength === totalLength) {
        const elapsedSeconds = (now - startTime) / 1000
        const bytesAfterResume = downloadedLength - existingSize
        const speed = elapsedSeconds > 0 ? bytesAfterResume / elapsedSeconds : 0
        const bytesRemaining = totalLength - downloadedLength
        const etaSeconds = (speed > 0 && bytesRemaining > 0) ? bytesRemaining / speed : Infinity

        if (!isShuttingDown) {
          progressBar.update(downloadedLength, {
            speed: filesize(speed),
            eta: etaSeconds === Infinity ? '∞' : prettyMs(etaSeconds * 1000, { compact: true }),
            value: filesize(downloadedLength)
          })
        }

        lastUpdateTime = now
      }
    })

    responseStream.pipe(writer)

    return new Promise((resolve, reject) => {
      writer.on('finish', async () => {
        if (progressBar) progressBar.stop()
        try {
          await new Promise(resolve => setTimeout(resolve, 100))
          if (fsc.existsSync(lockFilePath)) {
            fsc.unlinkSync(lockFilePath)
            untrackLockFile(lockFilePath)
          }

          try {
            const finalStats = await fs.stat(filePath)
            if (finalStats.size !== totalLength && totalLength > 0) {
              logWarning(`Warning: Final file size (${filesize(finalStats.size)}) doesn't match expected size (${filesize(totalLength)})`)
              return resolve({
                recorded: true,
                resumed: isResuming,
                warning: `File may be incomplete: size=${filesize(finalStats.size)}, expected=${filesize(totalLength)}`
              })
            }
          } catch (verifyErr) {
            debug('Error verifying final file size: %O', verifyErr)
          }

          resolve({ recorded: true, resumed: isResuming })
        } catch (unlinkErr) {
          const errMessage = `Could not remove lock file after successful save: ${unlinkErr.message}`
          logWarning(errMessage)
          resolve({ recorded: true, resumed: isResuming, warning: errMessage })
        }
      })

      writer.on('error', (err) => {
        if (progressBar) progressBar.stop()
        logError(`Error writing file ${path.basename(filePath)}: ${err.message}`)
        debug('File Write Error Details (%s): %O', item.title, err)
        if (responseStream && !responseStream.destroyed) responseStream.destroy()

        try {
          if (fsc.existsSync(lockFilePath)) {
            fsc.unlinkSync(lockFilePath)
            untrackLockFile(lockFilePath)
          }
        } catch (delErr) {
          logWarning(`Could not delete lock file ${lockFilePath} on write error: ${delErr.message}`)
        }

        if (!isResuming) {
          fs.unlink(filePath).catch(delErr =>
            logWarning(`Could not delete partial file ${filePath} on write error: ${delErr.message}`)
          )
        }

        reject(new Error(`Write error: ${err.message}`))
      })

      responseStream.on('error', (err) => {
        const completionPercentage = totalLength > 0 ? (downloadedLength / totalLength) * 100 : 0
        const isFullyComplete = completionPercentage >= 99.9
        const isNearlyComplete = completionPercentage > 98

        if (progressBar) {
          if (isFullyComplete) progressBar.update(totalLength)
          progressBar.stop()

          setTimeout(() => {
            process.stdout.write('\r\x1b[K')

            if (isFullyComplete) {
              log(`${item.title} save completed successfully.`)
            } else if (isNearlyComplete) {
              logWarning(`Save for ${item.title} interrupted at ${completionPercentage.toFixed(1)}% - File should be usable.`)
            } else {
              logError(`Error saving ${item.title}: ${err.message}`)
              log(chalk.gray(`Partial file kept for future resume - run command again to continue downloading.`))
            }

            debug('Save Stream Error Details (%s): %O', item.title, err)

            setTimeout(() => {
              if (writer && !writer.closed) writer.close()

              try {
                if (fsc.existsSync(lockFilePath)) {
                  fsc.unlinkSync(lockFilePath)
                  untrackLockFile(lockFilePath)
                }
              } catch (cleanupErr) {
                logWarning(`Lock file cleanup failed: ${cleanupErr.message}`)
              }

              if (isFullyComplete) {
                resolve({ recorded: true, resumed: isResuming })
              } else if (
                isNearlyComplete ||
                err.message.includes('Premature close') ||
                err.message.includes('IncompleteRead') ||
                err.code === 'ECONNRESET'
              ) {
                const warning = isNearlyComplete
                  ? `Save interrupted at ${completionPercentage.toFixed(1)}% - file should be usable.`
                  : 'Save interrupted - can be resumed on next run.'
                resolve({ recorded: false, resumed: isResuming, warning })
              } else {
                resolve({
                  recorded: false,
                  resumed: isResuming,
                  warning: `Download interrupted at ${completionPercentage.toFixed(1)}% - will attempt resume on next run.`
                })
              }
            }, 300)
          }, 100)
        } else {
          debug('Save Stream Error Details (%s): %O', item.title, err)
          if (writer && !writer.closed) writer.close()

          try {
            if (fsc.existsSync(lockFilePath)) {
              fsc.unlinkSync(lockFilePath)
              untrackLockFile(lockFilePath)
            }
          } catch (cleanupErr) {
            logWarning(`Lock file cleanup failed: ${cleanupErr.message}`)
          }

          if (isFullyComplete) {
            resolve({ recorded: true, resumed: isResuming })
          } else {
            resolve({
              recorded: false,
              resumed: isResuming,
              warning: 'Save interrupted - can be resumed on next run'
            })
          }
        }
      })
    })
  } catch (error) {
    if (progressBar) progressBar.stop()
    if (responseStream && !responseStream.destroyed) responseStream.destroy()
    if (writer && !writer.closed) writer.close()
    debug('Outer Save Error (%s): %O', item.title, error)

    try {
      if (fsc.existsSync(lockFilePath)) {
        fsc.unlinkSync(lockFilePath)
        untrackLockFile(lockFilePath)
      }
    } catch (cleanupErr) {
      logWarning(`Could not clean up lock file on error: ${cleanupErr.message}`)
    }

    logError(`Failed to initiate save for ${item.title}: ${error.message}`)
    return { recorded: false, error: `Initiation failed: ${error.message}` }
  }
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

const printRecordings = (recordings, { jsonOutput, showsOnly }) => {
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

const processFilter = (arr) =>
  _(arr)
    .castArray()
    .flatMap(f => typeof f === 'string' ? f.split(',') : [f])
    .map(s => String(s).trim().toLowerCase())
    .compact()
    .value()

const getApiService = async (location) => {
  const device = location?._rawDeviceXml
  if (!device?.serviceList?.service) return null

  let services = device.serviceList.service
  if (!Array.isArray(services)) services = [services]

  const cds = services.find(s => s.serviceType === UPNP_CONTENT_DIRECTORY_URN)
  if (!cds) return null

  const controlURL = getXmlText(cds.controlURL)
  const serviceType = getXmlText(cds.serviceType)

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

const createRequestManager = (options = {}) => {
  const state = {
    concurrency: options.initialConcurrency || INITIAL_BROWSE_CONCURRENCY,
    maxConcurrency: options.maxConcurrency || MAX_CONCURRENT_BROWSE,
    minDelay: options.minDelay || MIN_BROWSE_DELAY,
    currentDelay: options.initialDelay || options.minDelay || MIN_BROWSE_DELAY,
    adaptiveDelayFactor: options.adaptiveDelayFactor || ADAPTIVE_DELAY_FACTOR,
    active: 0,
    queue: [],
    lastRequestTime: 0,
    responseStats: [],
    failureCount: 0,
    debug: options.debug || false
  }

  const updateStats = (duration, success) => {
    if (!success) {
      state.failureCount++
      if (state.failureCount > 2 && state.concurrency > 1) {
        state.concurrency = Math.max(1, Math.floor(state.concurrency * 0.75))
        state.currentDelay = Math.min(state.currentDelay * 1.5, 1000)
        debug('Multiple failures detected, reducing concurrency to %d, increasing delay to %d',
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
      updateStats(duration, true)
      resolve(result)
    } catch (error) {
      updateStats(600, false)
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

const browseRequest = async (apiService, objectId = '0') => {
  const { cd_ctr: controlUrl, cd_service: serviceType } = apiService

  const cacheKey = `${objectId}`
  if (requestCache.has(cacheKey)) return requestCache.get(cacheKey)

  const payload = createBrowsePayload(serviceType, objectId)
  const headers = {
    'Content-Type': 'text/xml;charset="utf-8"',
    'SOAPAction': `"${serviceType}#Browse"`
  }

  let lastError = null
  let delayBase = BROWSE_RETRY_DELAY

  for (let attempt = 1; attempt <= BROWSE_RETRIES + 1; attempt++) {
    try {
      if (attempt > 2) {
        log(chalk.yellow(`Retrying browse for ObjectID ${objectId} (Attempt ${attempt-1}/${BROWSE_RETRIES})…`))
        await new Promise(resolve => setTimeout(resolve, delayBase))
        delayBase = Math.min(delayBase * 2, 5000)
      }

      const response = await httpClient.post(controlUrl, payload, {
        headers,
        timeout: attempt === 1 ? REQUEST_TIMEOUT / 2 : REQUEST_TIMEOUT
      })

      const parsedResponse = parseXml(response.data)
      debug('Browse Response SOAP (ObjectID: %s, Attempt: %d): %O', objectId, attempt, parsedResponse)

      const resultText = findNode(parsedResponse, 'Envelope.Body.BrowseResponse.Result')
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

const createBrowsePayload = (serviceType, objectId, requestedCount = 0) => `<?xml version="1.0" encoding="utf-8" standalone="yes"?>
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

const findDirectories = async (apiService, objectId = '0') => {
  const didlLite = await browseRequest(apiService, objectId)
  if (!didlLite) return null
  if (!didlLite.container) return []

  let containers = didlLite.container
  if (!Array.isArray(containers)) containers = [containers]

  return containers.map(container => ({
    title: getXmlText(container.title),
    id: getXmlAttr(container, 'id', NO_NUMBER_DEFAULT),
    parent_id: getXmlAttr(container, 'parentID', NO_NUMBER_DEFAULT),
    items: []
  }))
}

const findItems = async (apiService, objectId) => {
  const didlLite = await browseRequest(apiService, objectId)
  if (!didlLite) return []
  if (!didlLite.item) return []

  let items = didlLite.item
  if (!Array.isArray(items)) items = [items]

  return items.map(item => {
    const resNode = item.res
    const actualRes = Array.isArray(resNode) ? resNode[0] : resNode
    const itemClass = getXmlText(item.class) ?? ''
    const isVideo = itemClass.includes('videoItem')
    const title = getXmlText(item.title)

    return {
      type: itemClass,
      title: title,
      id: getXmlAttr(item, 'id', NO_NUMBER_DEFAULT),
      parent_id: getXmlAttr(item, 'parentID', NO_NUMBER_DEFAULT),
      description: getXmlText(item.description),
      url: getXmlText(actualRes),
      size: parseInt(getXmlAttr(actualRes, 'size', '0'), 10),
      duration: tsToSeconds(getXmlAttr(actualRes, 'duration')),
      parent_name: getXmlAttr(actualRes, 'parentTaskName'),
      item_type: isVideo && /^S\d+ E\d+/i.test(title) ? 'episode' : (isVideo ? 'movie' : 'other')
    }
  })
}

const isCurrentlyRecording = async (item) => {
  try {
    const response = await httpClient.head(item.url, { timeout: REQUEST_TIMEOUT })
    debug('HEAD Response Headers for %s: %O', item.title, response.headers)
    const contentLength = parseInt(response.headers['content-length'] ?? '0', 10)
    return contentLength === MAX_OCTET_RECORDING
  } catch (headError) {
    debug('HEAD request failed for %s: %O', item.title, headError)
    if (headError.response && headError.response.status === 405) {
      try {
        const response = await httpClient.get(item.url, {
          timeout: REQUEST_TIMEOUT,
          responseType: 'stream'
        })
        debug('GET (fallback) Response Headers for %s: %O', item.title, response.headers)
        const contentLength = parseInt(response.headers['content-length'] ?? '0', 10)
        response.data.destroy()
        return contentLength === MAX_OCTET_RECORDING
      } catch (getErr) {
        logWarning(`GET check failed for ${item.title} after HEAD failed: ${getErr.message}`)
        debug('GET (fallback) error for %s: %O', item.title, getErr)
        return false
      }
    } else {
      logWarning(`HEAD check failed for ${item.title}: ${headError.message}`)
      return false
    }
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

const addSavedFile = async (savePath, savedFilesDb, item) => {
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
      if (XMLValidator.validate(response.data) !== true) return logWarning(`Invalid XML from ${url}`)
      const xmlRoot = xmlParser.parse(response.data)
      debugXml('Parsed XML from %s: %O', url, xmlRoot)
      const device = xmlRoot?.root?.device
      if (!device) return logWarning(`Could not find device info in XML from ${url}`)

      return {
        url: url,
        deviceType: getXmlText(device.deviceType),
        friendlyName: getXmlText(device.friendlyName),
        manufacturer: getXmlText(device.manufacturer),
        manufacturerURL: getXmlText(device.manufacturerURL),
        modelDescription: getXmlText(device.modelDescription),
        modelName: getXmlText(device.modelName),
        modelNumber: getXmlText(device.modelNumber),
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
  description: item.description
})

const getXmlAttr = (node, attrName, defaultValue = '') =>
  node?.[`@_${attrName}`] ?? defaultValue

const getXmlText = (node, defaultValue = '') => {
  if (_.isNil(node)) return defaultValue
  if (_.isObject(node) && _.has(node, '#text')) return node['#text'] ?? defaultValue
  if (_.isString(node) || _.isNumber(node) || _.isBoolean(node)) return _.toString(node)
  return defaultValue
}

const findNode = (obj, path) => {
  if (!obj || typeof obj !== 'object' || !path) return undefined
  return path.split('.').reduce((current, key) => {
    if (_.isNil(current)) return undefined
    const foundKey = _.find(_.keys(current), k => k === key || _.endsWith(k, `:${key}`))
    return foundKey ? current[foundKey] : undefined
  }, obj)
}

const createValidFilename = (filename = '') => {
  let result = _.chain(filename)
    .toString()
    .trim()
    .replace(/[<>:"/\\|?*\x00-\x1F]/g, '')
    .replace(/\s+/g, '_')
    .replace(/\.(?![^.]*$)/g, '_')
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

const log = message => {
  if (progressBarActive) process.stdout.write('\r\n')
  console.log(message)
}

const logWarning = message => {
  if (progressBarActive) process.stdout.write('\r\n')
  console.log(chalk.yellow.bold(message))
}

const logError = message => {
  if (progressBarActive) process.stdout.write('\r\n')
  console.log(chalk.red.bold(message))
}

const logHeading = (title, color = 'blueBright') => {
  if (progressBarActive) process.stdout.write('\r\n')
  console.log(chalk[color].bold(`=== ${title} ===`))
}

const registerExitHandlers = (() => {
  let registered = false
  return () => {
    if (registered) return

    const exitHandler = () => {
      syncCleanupLockFiles()
    }

    const signalHandler = () => {
      isShuttingDown = true

      if (activeMultiBar) {
        activeMultiBar.stop()
        activeMultiBar = null
      }

      process.stdout.write('\r\x1b[K')
      progressBarActive = false

      if (activeLockFiles.size > 0) {
        console.log(chalk.yellow('Interrupt detected. Cleaning up lock files…'))
        const count = syncCleanupLockFiles()
        console.log(chalk.yellow(`Cleaned up ${count} lock files.`))
      }

      setTimeout(() => {
        process.exit(0)
      }, 200)
    }

    process.on('SIGINT', signalHandler)
    process.on('SIGTERM', signalHandler)
    process.on('exit', exitHandler)

    registered = true
  }
})()

const trackLockFile = (lockPath) => {
  activeLockFiles.add(lockPath)
  debug('Added lock file to tracking: %s', lockPath)
}

const untrackLockFile = (lockPath) => {
  activeLockFiles.delete(lockPath)
  debug('Removed lock file from tracking: %s', lockPath)
}

const syncCleanupLockFiles = () => {
  let count = 0
  for (const lockPath of activeLockFiles) {
    try {
      if (fsc.existsSync(lockPath)) {
        fsc.unlinkSync(lockPath)
        count++
        debug('Removed lock file: %s', lockPath)
      }
    } catch (err) {
      debug('Error removing lock file: %s', lockPath)
    }
  }
  activeLockFiles.clear()
  return count
}

main().catch(error => {
  logError('\n--- An unexpected error occurred ---')
  logError(error.message)
  if (argv.debug || !(error.response || error.request)) {
    console.error(error.stack)
  } else if (error.response) {
    logError(`Status: ${error.response.status} ${error.response.statusText}`)
    logError(`URL: ${error.config?.url || 'N/A'}`)
    if (error.response.data) logError(`Response Data: ${typeof error.response.data === 'string' ? error.response.data.slice(0, 200) + '...' : '[Object/Stream]'}`)
  } else if (error.request) {
    logError('Request made but no response received.')
    logError(`URL: ${error.config?.url || 'N/A'}`)
    if (error.code) logError(`Error Code: ${error.code}`)
  }
  logError('----------------------------------')
  process.exit(1)
})
