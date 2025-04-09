#!/usr/bin/env node

import fsc from 'fs'
import path from 'path'
import axios from 'axios'
import { URL } from 'url'
import yargs from 'yargs'
import chalk from 'chalk'
import fs from 'fs/promises'
import Table from 'cli-table3'
import cliProgress from 'cli-progress'
import { hideBin } from 'yargs/helpers'
import { XMLParser, XMLValidator } from 'fast-xml-parser'
import { filesize } from 'filesize'
import prettyMs from 'pretty-ms'
import debugLib from 'debug'
import _ from 'lodash'

import NodeSsdp from 'node-ssdp'
const { Client: SsdpClient } = NodeSsdp

const debug = debugLib('fetchtv')
const debugXml = debugLib('fetchtv:xml')
const debugNetwork = debugLib('fetchtv:network')

let argv = {}

const DISCOVERY_TIMEOUT = 3000
const REQUEST_TIMEOUT = 15000
const BROWSE_RETRIES = 3
const BROWSE_RETRY_DELAY = 1500
const BROWSE_INTER_DELAY = 150
const ISRECORDING_CHECK_DELAY = 200
const SAVE_FILE_NAME = 'fetchtv.json'
const MAX_CONCURRENT_DOWNLOADS = 3
const FETCHTV_PORT = 49152
const CONST_LOCK = '.lock'
const MAX_FILENAME = 255
const MAX_OCTET_RECORDING = 4398046510080
const NO_NUMBER_DEFAULT = ''
const FETCH_MANUFACTURER_URL = 'http://www.fetch.com/'
const UPNP_CONTENT_DIRECTORY_URN = 'urn:schemas-upnp-org:service:ContentDirectory:1'

const main = async () => {
  argv = await yargs(hideBin(process.argv))
    .option('info', { type: 'boolean', description: 'Attempts auto-discovery and returns the Fetch TV details' })
    .option('ip', { type: 'string', description: 'Specify the IP Address of the Fetch TV' })
    .option('port', { type: 'number', default: FETCHTV_PORT, description: 'Specify the port of the Fetch TV' })
    .option('recordings', { type: 'boolean', description: 'List recordings' })
    .option('shows', { type: 'boolean', description: 'List show titles (and not the episodes within)' })
    .option('folder', { type: 'array', default: [], description: 'Only include recordings where the show title contains the specified text (repeatable)' })
    .option('title', { type: 'array', default: [], description: 'Only include recordings where the episode title contains the specified text (repeatable)' })
    .option('exclude', { type: 'array', default: [], description: "Don't include show titles containing the specified text (repeatable)" })
    .option('isrecording', { type: 'boolean', description: 'List only items that are currently recording' })
    .option('save', { type: 'string', description: 'Save recordings to the specified path' })
    .option('overwrite', { type: 'boolean', default: false, description: 'Will save and overwrite any existing files' })
    .option('json', { type: 'boolean', default: false, description: 'Output show/recording/save results in JSON' })
    .option('debug', { type: 'boolean', default: false, description: 'Enable detailed debug logging of API responses'})
    .help()
    .alias('h', 'help')
    .alias('i', 'info')
    .alias('r', 'recordings')
    .alias('s', 'shows')
    .alias('f', 'folder')
    .alias('t', 'title')
    .alias('e', 'exclude')
    .alias('o', 'overwrite')
    .alias('j', 'json')
    .alias('d', 'debug')
    .epilog('Note: Comma-separated values for filters (--folder, --exclude, --title) are NOT supported. Use multiple options instead.')
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

  if (argv.info) printInfo(fetchServer)

  const wantsRecordingsAction = argv.recordings || argv.shows || argv.isrecording || argv.save

  if (wantsRecordingsAction) {
    const filters = {
      folderFilter: processFilter(argv.folder),
      excludeFilter: processFilter(argv.exclude),
      titleFilter: processFilter(argv.title),
      showsOnly: argv.shows,
      isRecordingFilter: argv.isrecording
    }

    log(`Getting Fetch TV ${argv.shows ? 'shows' : 'recordings'}…`)
    const recordings = await getFetchRecordings(fetchServer, filters)

    if (!argv.save) {
      printRecordings(recordings, { jsonOutput: argv.json })
    } else {
      await handleSaveAction(recordings, {
        savePath: path.resolve(argv.save),
        overwrite: argv.overwrite,
        jsonOutput: argv.json
      })
    }
  } else if (!argv.info) {
    logWarning('No action specified. Use --info, --recordings, --shows, --isrecording, or --save. Use --help for options.')
  }

  logHeading(`Done: ${new Date().toLocaleString()}`)
}

const processFilter = (arr) => _(arr)
  .castArray()
  .flatMap(f => typeof f === 'string' ? f.split(',') : [f])
  .map(s => _.toString(s).trim().toLowerCase())
  .compact()
  .value()

const printInfo = (fetchServer) => {
  const table = new Table({ head: [chalk.cyan('Field'), chalk.cyan('Value')] })
  const fields = ['deviceType', 'friendlyName', 'manufacturer', 'manufacturerURL',
                  'modelName', 'modelDescription', 'modelNumber']

  fields.forEach(field => {
    const displayName = field.replace(/([A-Z])/g, ' $1').replace(/^./, str => str.toUpperCase())
    table.push({ [displayName]: fetchServer[field] || 'N/A' })
  })

  log(table.toString())
}

const printRecordings = (recordings, { jsonOutput }) => {
  const sortedRecordings = sortRecordingsByTitle(recordings)

  if (jsonOutput) {
    const output = sortedRecordings.map(rec => {
      const item = {
        id: rec.id,
        title: rec.title
      }
      if (!argv.shows) item.items = rec.items?.map(formatItem)
      return item
    })
    console.log(JSON.stringify(output, null, 2))
    return
  }

  const context = argv.shows ? 'Shows' : 'Recordings'
  logHeading(`Listing ${context}`)
  if (!sortedRecordings || sortedRecordings.length === 0) {
    logWarning(`No ${context} found matching criteria!`)
    return
  }

  sortedRecordings.forEach(recording => {
    const bullet = argv.shows ? '' : '📁 '
    log(chalk.green(`${bullet}${recording.title}`))
    if (recording.items && recording.items.length > 0) {
      recording.items.forEach(item => {
        const durationStr = new Date(item.duration * 1000).toISOString().substr(11, 8)
        const sizeFormatted = filesize(item.size)
        log(`  ${chalk.whiteBright(item.title)} (${chalk.gray(`${durationStr}, ${sizeFormatted}`)})`)
      })
    } else {
      if (!argv.shows) log(chalk.gray('  (No items listed based on current filters)'))
    }
  })
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
        log(`Save path "${savePath}" does not exist, creating it.`)
        await fs.mkdir(savePath, { recursive: true })
      } else {
        throw statError
      }
    }

    const jsonResult = await saveRecordings(recordings, { savePath, overwrite })
    if (jsonOutput) console.log(JSON.stringify(jsonResult, null, 2))
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

  // Build task list
  for (const show of recordings) {
    if (!show.items || show.items.length === 0) continue

    for (const item of show.items) {
      if (overwrite || !savedFilesDb[item.id]) {
        const showDirName = createValidFilename(show.title)
        const showDirPath = path.join(savePath, showDirName)
        const itemFileName = `${createValidFilename(item.title)}.mpeg`
        const filePath = path.join(showDirPath, itemFileName)
        tasks.push({ item, filePath, showDirPath, showTitle: show.title })
      } else {
        log(chalk.gray(`Skipping already saved: ${show.title} / ${item.title}`))
        if (argv.json) jsonResults.push({ item: formatItem(item), recorded: false, warning: 'Skipped (already saved)' })
      }
    }
  }

  if (tasks.length === 0) {
    log('There is nothing new to record.')
    return jsonResults
  }

  log(`Preparing to download ${tasks.length} new recordings…`)

  const multiBar = new cliProgress.MultiBar({
    clearOnComplete: false,
    hideCursor: true,
    format: ' {bar} | {percentage}% | {filename}'
  }, cliProgress.Presets.shades_classic)

  // Handle downloads with concurrency limit
  const activePromises = new Set()

  for (const task of tasks) {
    while (activePromises.size >= MAX_CONCURRENT_DOWNLOADS) {
      await Promise.race(activePromises)
    }

    const progressBar = multiBar.create(task.item.size || 0, 0, {
      filename: path.basename(task.filePath).slice(0, 25).padEnd(25),
      speed: 'N/A',
      eta: 'N/A',
      value: filesize(0),
      total: filesize(task.item.size || 0)
    })

    progressBar.options.format = `{filename} |${chalk.cyan('{bar}')}| {percentage}% | ETA: {eta} | {value}/{total} | Speed: {speed}`

    const promise = downloadFile(task.item, task.filePath, progressBar)
      .then(async (downloadResult) => {
        const resultEntry = { item: formatItem(task.item), recorded: downloadResult.recorded }
        if (downloadResult.warning) resultEntry.warning = downloadResult.warning
        if (downloadResult.error) resultEntry.error = downloadResult.error

        jsonResults.push(resultEntry)
        if (downloadResult.recorded) {
          await addSavedFile(savePath, savedFilesDb, task.item)
        }
      })
      .catch((error) => {
        logError(`Unexpected error processing download result for ${task.item.title}: ${error.message}`)
        jsonResults.push({ item: formatItem(task.item), recorded: false, error: `Processing error: ${error.message}` })
        if (progressBar) progressBar.stop()
      })
      .finally(() => {
        activePromises.delete(promise)
      })

    activePromises.add(promise)
  }

  await Promise.allSettled(activePromises)
  multiBar.stop()

  return jsonResults
}

const discoverFetch = async ({ ip, port }) => {
  log('Starting discovery…')
  const locations = new Set()

  if (ip) {
    locations.add(`http://${ip}:${port}/MediaServer.xml`)
  } else {
    const client = new SsdpClient()
    client.on('response', (headers, statusCode, rinfo) => {
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

  log('Discovery successful!')

  const url = new URL(fetchServer.url)
  const hostname = url.hostname
  if (hostname) log(`Fetch TV IP Address: ${chalk.magentaBright(hostname)}`)
  log(`Device Description Document: ${chalk.magentaBright(fetchServer.url)}`)

  return fetchServer
}

const getFetchRecordings = async (location, { folderFilter, excludeFilter, titleFilter, showsOnly, isRecordingFilter }) => {
  const apiService = await getApiService(location)
  if (!apiService) {
    logError('Could not find ContentDirectory service.')
    return []
  }

  const baseFolders = await findDirectories(apiService, '0')
  if (baseFolders === null) {
    logError('Failed to browse root folder (ObjectID 0) after retries. Cannot list recordings.')
    return []
  }

  const recordingsFolder = baseFolders.find(f => f.title === 'Recordings')
  if (!recordingsFolder) {
    logWarning('No "Recordings" folder found in processed base folders.')
    return []
  }

  await new Promise(resolve => setTimeout(resolve, BROWSE_INTER_DELAY))

  const showFolders = await findDirectories(apiService, recordingsFolder.id)
  if (showFolders === null) {
    logError(`Failed to browse the main "Recordings" folder (ObjectID ${recordingsFolder.id}) after retries. Cannot list shows.`)
    return []
  }

  const results = []

  for (const show of showFolders) {
    const titleLower = show.title.toLowerCase()
    const include = !folderFilter.length || folderFilter.some(f => titleLower.includes(f))
    const exclude = excludeFilter.length && excludeFilter.some(e => titleLower.includes(e))

    if (!include || exclude) continue

    let items = []
    if (!showsOnly) {
      await new Promise(resolve => setTimeout(resolve, BROWSE_INTER_DELAY))
      items = await findItems(apiService, show.id)

      if (titleFilter.length > 0)
        items = items.filter(item => titleFilter.some(t => item.title.toLowerCase().includes(t)))

      if (isRecordingFilter) {
        const recordingItems = []
        log(`Checking recording status for items in: ${chalk.green(show.title)}`)
        for (const item of items) {
          await new Promise(resolve => setTimeout(resolve, ISRECORDING_CHECK_DELAY))
          if (await isCurrentlyRecording(item)) recordingItems.push(item)
        }
        items = recordingItems
        if (items.length === 0 && !showsOnly) continue
      }
    }

    if (showsOnly || (items && items.length > 0))
      results.push({ ...show, items: items })
  }

  return results
}

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

const browseRequest = async (apiService, objectId = '0') => {
  const { cd_ctr: controlUrl, cd_service: serviceType } = apiService
  const payload = `<?xml version="1.0" encoding="utf-8" standalone="yes"?>
<s:Envelope s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/" xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
 <s:Body>
  <u:Browse xmlns:u="${serviceType}">
   <ObjectID>${objectId}</ObjectID>
   <BrowseFlag>BrowseDirectChildren</BrowseFlag>
   <Filter>*</Filter>
   <StartingIndex>0</StartingIndex>
   <RequestedCount>0</RequestedCount>
   <SortCriteria></SortCriteria>
  </u:Browse>
 </s:Body>
</s:Envelope>`

  const headers = {
    'Content-Type': 'text/xml;charset="utf-8"',
    'SOAPAction': `"${serviceType}#Browse"`
  }

  let lastError = null
  for (let attempt = 1; attempt <= BROWSE_RETRIES; attempt++) {
    try {
      if (attempt > 1) log(chalk.yellow(`Retrying browse for ObjectID ${objectId} (Attempt ${attempt}/${BROWSE_RETRIES})…`))

      const response = await axios.post(controlUrl, payload, { headers, timeout: REQUEST_TIMEOUT })
      const parsedResponse = parseXml(response.data)
      debug('Browse Response SOAP (ObjectID: %s, Attempt: %d): %O', objectId, attempt, parsedResponse)

      const resultText = findNode(parsedResponse, 'Envelope.Body.BrowseResponse.Result')
      if (!resultText || typeof resultText !== 'string') {
        logWarning(`Could not find valid Result string in Browse response for ObjectID ${objectId} on attempt ${attempt}`)
        lastError = new Error('Could not find valid Result string in Browse response')
        if (attempt < BROWSE_RETRIES) await new Promise(resolve => setTimeout(resolve, BROWSE_RETRY_DELAY))
        continue
      }

      const resultXml = parseXml(resultText)
      if (!resultXml || !resultXml['DIDL-Lite']) {
        logWarning(`Failed to parse embedded DIDL-Lite XML for ObjectID ${objectId} on attempt ${attempt}`)
        lastError = new Error('Failed to parse embedded DIDL-Lite XML')
        if (attempt < BROWSE_RETRIES) await new Promise(resolve => setTimeout(resolve, BROWSE_RETRY_DELAY))
        continue
      }

      debug('Browse Response DIDL-Lite (ObjectID: %s, Attempt: %d): %O', objectId, attempt, resultXml['DIDL-Lite'])
      return resultXml['DIDL-Lite']

    } catch (err) {
      lastError = err
      logWarning(`Browse attempt ${attempt} failed for ObjectID ${objectId}: ${err.message}`)
      debug('Browse Error Details (ObjectID: %s, Attempt: %d): %O', objectId, attempt, err)
      if (attempt < BROWSE_RETRIES) await new Promise(resolve => setTimeout(resolve, BROWSE_RETRY_DELAY))
    }
  }

  logError(`Browse request failed definitively for ObjectID ${objectId} after ${BROWSE_RETRIES} attempts. Last error: ${lastError?.message || 'Unknown'}`)
  return null
}

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
    const response = await axios.head(item.url, { timeout: REQUEST_TIMEOUT })
    debug('HEAD Response Headers for %s: %O', item.title, response.headers)
    const contentLength = parseInt(response.headers['content-length'] ?? '0', 10)
    return contentLength === MAX_OCTET_RECORDING
  } catch (headError) {
    debug('HEAD request failed for %s: %O', item.title, headError)
    if (headError.response && headError.response.status === 405) {
      try {
        const response = await axios.get(item.url, {
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

const downloadFile = async (item, filePath, progressBar) => {
  const lockFilePath = `${filePath}${CONST_LOCK}`
  let writer = null
  let responseStream = null

  try {
    if (fsc.existsSync(lockFilePath)) {
      logWarning(`Already writing ${item.title} (lock file exists), skipping.`)
      if (progressBar) progressBar.stop()
      return { recorded: false, warning: 'Already writing (lock file exists) skipping' }
    }

    await fs.mkdir(path.dirname(filePath), { recursive: true })
    await fs.writeFile(lockFilePath, '')

    const response = await axios.get(item.url, {
      responseType: 'stream',
      timeout: REQUEST_TIMEOUT * 20
    })

    responseStream = response.data
    const totalLength = parseInt(response.headers['content-length'] ?? '0', 10)
    debug('Download Headers for %s: %O', item.title, response.headers)

    if (totalLength === MAX_OCTET_RECORDING) {
      logWarning(`Skipping ${item.title}, it appears to be currently recording.`)
      responseStream.destroy()
      await fs.unlink(lockFilePath).catch(delErr => logWarning(`Could not delete lock file on skip ${lockFilePath}: ${delErr.message}`))
      if (progressBar) progressBar.stop()
      return { recorded: false, warning: "Skipping item, it's currently recording" }
    }

    if (totalLength === 0) {
      logWarning(`Skipping ${item.title}, content length is zero.`)
      responseStream.destroy()
      await fs.unlink(lockFilePath).catch(delErr => logWarning(`Could not delete lock file on zero size ${lockFilePath}: ${delErr.message}`))
      if (progressBar) progressBar.stop()
      return { recorded: false, warning: 'Skipping item, content length is zero' }
    }

    writer = fsc.createWriteStream(filePath)

    if (progressBar) {
      progressBar.setTotal(totalLength)
      progressBar.update(0, {
        speed: 'N/A',
        eta: 'N/A',
        value: filesize(0),
        total: filesize(totalLength)
      })
    }

    let downloadedLength = 0
    let lastUpdateTime = progressBar?.startTime || Date.now()

    responseStream.on('data', (chunk) => {
      downloadedLength += chunk.length
      if (progressBar) {
        const now = Date.now()
        const startTime = progressBar.startTime || lastUpdateTime
        if (now - lastUpdateTime > 250 || downloadedLength === totalLength) {
          const elapsedSeconds = (now - startTime) / 1000
          const speed = elapsedSeconds > 0 ? downloadedLength / elapsedSeconds : 0
          const bytesRemaining = totalLength - downloadedLength
          const etaSeconds = (speed > 0 && bytesRemaining > 0) ? bytesRemaining / speed : Infinity

          progressBar.update(downloadedLength, {
            speed: filesize(speed) + '/s',
            eta: etaSeconds === Infinity ? '∞' : prettyMs(etaSeconds * 1000, { compact: true }),
            value: filesize(downloadedLength)
          })

          lastUpdateTime = now
        }
      }
    })

    responseStream.pipe(writer)

    return new Promise((resolve, reject) => {
      writer.on('finish', async () => {
        if (progressBar) progressBar.stop()
        try {
          // Add a small delay before removing the lock file
          await new Promise(resolve => setTimeout(resolve, 100))
          // Check if lock file still exists before trying to remove it
          if (fsc.existsSync(lockFilePath)) {
            await fs.unlink(lockFilePath)
          }
          resolve({ recorded: true })
        } catch (unlinkErr) {
          const errMessage = `Could not remove lock file after successful download: ${unlinkErr.message}`
          logWarning(errMessage)
          resolve({ recorded: true, warning: errMessage })
        }
      })

      writer.on('error', (err) => {
        if (progressBar) progressBar.stop()
        logError(`Error writing file ${path.basename(filePath)}: ${err.message}`)
        debug('File Write Error Details (%s): %O', item.title, err)
        if (responseStream && !responseStream.destroyed) responseStream.destroy()
        fs.unlink(lockFilePath).catch(delErr => logWarning(`Could not delete lock file ${lockFilePath} on write error: ${delErr.message}`))
        fs.unlink(filePath).catch(delErr => logWarning(`Could not delete partial file ${filePath} on write error: ${delErr.message}`))
        reject(new Error(`Write error: ${err.message}`))
      })

      responseStream.on('error', (err) => {
        if (progressBar) progressBar.stop()

        // Calculate completion percentage
        const completionPercentage = totalLength > 0 ? (downloadedLength / totalLength) * 100 : 0
        const isNearlyComplete = completionPercentage > 98

        if (isNearlyComplete) {
          logWarning(`Download for ${item.title} interrupted at ${completionPercentage.toFixed(1)}% - File should be usable`)
        } else {
          logError(`Error downloading ${item.title}: ${err.message}`)
        }

        debug('Download Stream Error Details (%s): %O', item.title, err)

        // Gracefully close the writer after a delay
        setTimeout(() => {
          if (writer && !writer.closed) writer.close()

          // Don't delete the file if it's nearly complete
          if (!isNearlyComplete) {
            try {
              if (fsc.existsSync(filePath)) fsc.unlinkSync(filePath)
            } catch (cleanupErr) {
              logWarning(`Partial file cleanup failed: ${cleanupErr.message}`)
            }
          }

          try {
            if (fsc.existsSync(lockFilePath)) fsc.unlinkSync(lockFilePath)
          } catch (cleanupErr) {
            logWarning(`Lock file cleanup failed: ${cleanupErr.message}`)
          }

          // Always treat nearly complete downloads as successful
          if (isNearlyComplete || err.message.includes('Premature close') ||
              err.message.includes('IncompleteRead') || err.code === 'ECONNRESET') {
            const warning = isNearlyComplete ?
              'Download nearly complete when interrupted - file should be usable' :
              'Download may be incomplete (Network/FetchTV issue). Check file size.'
            resolve({ recorded: true, warning })
          } else {
            reject(new Error(`Download error: ${err.message}`))
          }
        }, 500) // Add 500ms delay before cleanup
      })
    })

  } catch (error) {
    if (progressBar) progressBar.stop()
    if (responseStream && !responseStream.destroyed) responseStream.destroy()
    if (writer && !writer.closed) writer.close()
    debug('Outer Download Error (%s): %O', item.title, error)

    try {
    if (fsc.existsSync(lockFilePath)) {
      // Use sync version to ensure cleanup happens
      try { fsc.unlinkSync(lockFilePath) } catch (e) {}
    }
    if (fsc.existsSync(filePath)) {
      try { fsc.unlinkSync(filePath) } catch (e) {}
    }
  } catch (cleanupErr) {
    logWarning(`Could not clean up files on error: ${cleanupErr.message}`)
  }

    logError(`Failed to initiate download for ${item.title}: ${error.message}`)
    return { recorded: false, error: `Initiation failed: ${error.message}` }
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
      const response = await axios.get(url, { timeout: REQUEST_TIMEOUT })
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
    const isValid = XMLValidator.validate(xmlString)

    if (isValid === true) return xmlParser.parse(xmlString)

    if (argv.debug && isValid !== true) {
      logWarning(`XML validation failed: ${isValid.err.msg}`)
      debugXml('Invalid XML String: %s', xmlString.slice(0, 500) + '...')
    } else if (isValid !== true) {
      logWarning('Invalid XML received.')
    }
    return null

  } catch (error) {
    logError(`XML parsing error: ${error.message}`)
    debugXml('XML String causing parse error: %s', xmlString.slice(0, 500) + '...')
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

const formatItem = (item) => ({
  id: item.id,
  title: item.title,
  type: item.item_type,
  duration: item.duration,
  size: item.size,
  description: item.description
})

const log = (message) => console.log(message)
const logWarning = (message) => console.log(chalk.yellow.bold(message))
const logError = (message) => console.log(chalk.red.bold(message))
const logHeading = (title) => console.log(chalk.blueBright.bold(`=== ${title} ===`))

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
