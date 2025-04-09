#!/usr/bin/env node

import fsc from 'fs'
import path from 'path'
import util from 'util'
import axios from 'axios'
import { URL } from 'url'
import yargs from 'yargs'
import chalk from 'chalk'
import fs from 'fs/promises'
import Table from 'cli-table3'
import cliProgress from 'cli-progress'
import { hideBin } from 'yargs/helpers'
import { XMLParser, XMLValidator } from 'fast-xml-parser'

import NodeSsdp from 'node-ssdp'
const { Client: SsdpClient } = NodeSsdp

let argv = {}

const DISCOVERY_TIMEOUT = 3000
const REQUEST_TIMEOUT = 15000
const BROWSE_RETRIES = 3
const BROWSE_RETRY_DELAY = 1500
const BROWSE_INTER_DELAY = 150
const SAVE_FILE_NAME = 'fetchtv.json'
const FETCHTV_PORT = 49152
const CONST_LOCK = '.lock'
const MAX_FILENAME = 255
const MAX_OCTET_RECORDING = 4398046510080
const NO_NUMBER_DEFAULT = ''
const FETCH_MANUFACTURER_URL = 'http://www.fetch.com/'
const UPNP_DEVICE_URN = 'urn:schemas-upnp-org:device-1-0'
const UPNP_SERVICE_URN = 'urn:schemas-upnp-org:service-1-0'
const UPNP_METADATA_URN = 'urn:schemas-upnp-org:metadata-1-0/upnp/'
const DIDL_LITE_URN = 'urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/'
const DC_ELEMENTS_URN = 'http://purl.org/dc/elements/1.1/'
const CONTENT_DIRECTORY_URN = 'urn:schemas-upnp-org:service:ContentDirectory:1'

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

  logHeading(`Started: ${new Date().toLocaleString()}`)
  if (argv.debug) logWarning('*** Debug Mode Enabled ***')

  const fetchServer = await discoverFetch({ ip: argv.ip, port: argv.port })

  if (!fetchServer) {
    logHeading(`Done (Discovery Failed): ${new Date().toLocaleString()}`)
    process.exit(1)
  }

  if (argv.info) printInfo(fetchServer)

  const wantsRecordingsAction = argv.recordings || argv.shows || argv.isrecording || argv.save

  if (wantsRecordingsAction) {
    const processFilter = (arr) => (Array.isArray(arr) ? arr : [arr])
      .flatMap(f => typeof f === 'string' ? f.split(',') : [f])
      .map(s => String(s).trim().toLowerCase())
      .filter(Boolean)

    const folderFilter = processFilter(argv.folder)
    const excludeFilter = processFilter(argv.exclude)
    const titleFilter = processFilter(argv.title)

    log(`Getting Fetch TV ${argv.shows ? 'shows' : 'recordings'}…`)
    const recordings = await getFetchRecordings(fetchServer, {
      folderFilter,
      excludeFilter,
      titleFilter,
      showsOnly: argv.shows,
      isRecordingFilter: argv.isrecording
    })

    if (!argv.save) {
      printRecordings(recordings, { jsonOutput: argv.json })
    } else {
      const savePath = path.resolve(argv.save)
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

        const jsonResult = await saveRecordings(recordings, { savePath, overwrite: argv.overwrite })
        if (argv.json) {
          console.log(JSON.stringify(jsonResult, null, 2))
        }
      } catch (saveError) {
        logError(`Error during save process: ${saveError.message}`)
        process.exit(1)
      }
    }
  } else if (!argv.info) {
    logWarning('No action specified. Use --info, --recordings, --shows, --isrecording, or --save. Use --help for options.')
  }

  logHeading(`Done: ${new Date().toLocaleString()}`)
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

const printRecordings = (recordings, { jsonOutput }) => {
  const sortedRecordings = sortRecordingsByTitle(recordings)

  if (jsonOutput) {
    const output = sortedRecordings.map(rec => ({
      id: rec.id,
      title: rec.title,
      items: rec.items?.map(formatItem) ?? null
    }))
    console.log(JSON.stringify(output, null, 2))
    return
  }

  const context = argv.shows ? 'shows' : 'recordings'
  logHeading(`Listing ${context}…`)
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
        const sizeMB = item.size > 0 ? (item.size / (1024 * 1024)).toFixed(2) : '0.00'
        log(`  ${chalk.whiteBright(item.title)} (${chalk.gray(`${durationStr}, ${sizeMB} MB`)})`)
      })
    } else {
      if (!argv.shows) log(chalk.gray('  (No items listed based on current filters)'))
    }
  })
}

const sortRecordingsByTitle = (recordings) =>
  [...recordings]
    .map(recording => {
      const recordingCopy = { ...recording }
      const title = recordingCopy.title.toLowerCase()
      recordingCopy.sortTitle = title.startsWith('the ') ? title.slice(4) : title
      return recordingCopy
    })
    .sort((a, b) => a.sortTitle.localeCompare(b.sortTitle))
    .map(recording => {
      const { sortTitle, ...rest } = recording
      return rest
    })

const saveRecordings = async (recordings, { savePath, overwrite }) => {
  logHeading('Saving recordings')
  let someToRecord = false
  const savedFilesDb = await loadSavedFiles(savePath)
  const jsonResults = []

  for (const show of recordings) {
    if (!show.items || show.items.length === 0) continue

    const showDirName = createValidFilename(show.title)
    const showDirPath = path.join(savePath, showDirName)

    for (const item of show.items) {
      if (overwrite || !savedFilesDb[item.id]) {
        someToRecord = true
        const itemFileName = `${createValidFilename(item.title)}.mpeg`
        const filePath = path.join(showDirPath, itemFileName)

        log(`\nPreparing to save: ${show.title} / ${item.title}`)
        const downloadResult = await downloadFile(item, filePath)
        const resultEntry = { item: formatItem(item), recorded: downloadResult.recorded }
        if (downloadResult.warning) resultEntry.warning = downloadResult.warning
        if (downloadResult.error) resultEntry.error = downloadResult.error

        jsonResults.push(resultEntry)

        if (downloadResult.recorded) await addSavedFile(savePath, savedFilesDb, item)
      } else {
        log(chalk.gray(`Skipping already saved: ${show.title} / ${item.title}`))
        if (argv.json) jsonResults.push({ item: formatItem(item), recorded: false, warning: 'Skipped (already saved)' })
      }
    }
  }

  if (!someToRecord) log('There is nothing new to record.')

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
      debugLog('SSDP Response Headers', headers)
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
    logError(`Failed to browse the main 'Recordings' folder (ObjectID ${recordingsFolder.id}) after retries. Cannot list shows.`)
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

      if (titleFilter.length > 0) items = items.filter(item => titleFilter.some(t => item.title.toLowerCase().includes(t)))

      if (isRecordingFilter) {
        const recordingItems = []
        log(`Checking recording status for items in "${show.title}"…`)
        for (const item of items) if (await isCurrentlyRecording(item)) recordingItems.push(item)
        items = recordingItems
        if (items.length === 0 && !showsOnly) continue
      }
    }

    if (showsOnly || (items && items.length > 0)) results.push({ ...show, items: items })
  }

  return results
}

const getApiService = async (location) => {
  const device = location?._rawDeviceXml
  if (!device?.serviceList?.service) return null

  let services = device.serviceList.service
  if (!Array.isArray(services)) services = [services]

  const cds = services.find(s => s.serviceType === CONTENT_DIRECTORY_URN)
  if (!cds) return null

  const controlURL = cds.controlURL
  const serviceType = cds.serviceType

  if (!controlURL || !serviceType) return null

  try {
    const baseUrl = new URL(location.url)
    const absoluteControlUrl = new URL(controlURL, baseUrl).toString()

    return {
      cd_ctr: absoluteControlUrl,
      cd_service: serviceType
    }
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

      const response = await axios.post(controlUrl, payload, { headers: headers, timeout: REQUEST_TIMEOUT })
      const parsedResponse = parseXml(response.data)
      debugLog(`Browse Response SOAP Envelope (ObjectID: ${objectId}, Attempt: ${attempt})`, parsedResponse)

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

      debugLog(`Browse Response DIDL-Lite (ObjectID: ${objectId}, Attempt: ${attempt})`, resultXml['DIDL-Lite'])
      return resultXml['DIDL-Lite']

    } catch (err) {
      lastError = err
      logWarning(`Browse attempt ${attempt} failed for ObjectID ${objectId}: ${err.message}`)
      debugLog(`Browse Error Details (ObjectID: ${objectId}, Attempt: ${attempt})`, err)
      if (attempt < BROWSE_RETRIES) await new Promise(resolve => setTimeout(resolve, BROWSE_RETRY_DELAY))
    }
  }

  logError(`Browse request failed definitively for ObjectID ${objectId} after ${BROWSE_RETRIES} attempts.`)
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
    const itemClass = getXmlText(item.class) ?? ''
    const isVideo = itemClass.includes('videoItem')
    const title = getXmlText(item.title)

    return {
      type: itemClass,
      title: title,
      id: getXmlAttr(item, 'id', NO_NUMBER_DEFAULT),
      parent_id: getXmlAttr(item, 'parentID', NO_NUMBER_DEFAULT),
      description: getXmlText(item.description),
      url: getXmlText(resNode),
      size: parseInt(getXmlAttr(resNode, 'size', '0'), 10),
      duration: tsToSeconds(getXmlAttr(resNode, 'duration')),
      parent_name: getXmlAttr(resNode, 'parentTaskName'),
      item_type: isVideo && /^S\d+ E\d+/i.test(title) ? 'episode' : (isVideo ? 'movie' : 'other')
    }
  })
}

const isCurrentlyRecording = async (item) => {
  try {
    const response = await axios.head(item.url, { timeout: REQUEST_TIMEOUT })
    debugLog(`HEAD Response Headers for ${item.title}`, response.headers)
    const contentLength = parseInt(response.headers['content-length'] ?? '0', 10)
    return contentLength === MAX_OCTET_RECORDING
  } catch (headError) {
    debugLog(`HEAD request failed for ${item.title}`, headError)
    if (headError.response && headError.response.status === 405) {
      try {
        const response = await axios.get(item.url, {
          timeout: REQUEST_TIMEOUT,
          responseType: 'stream'
        })
        debugLog(`GET (fallback) Response Headers for ${item.title}`, response.headers)
        const contentLength = parseInt(response.headers['content-length'] ?? '0', 10)
        response.data.destroy()
        return contentLength === MAX_OCTET_RECORDING
      } catch (getErr) {
        logWarning(`GET check failed for ${item.title} after HEAD failed: ${getErr.message}`)
        debugLog(`GET (fallback) error for ${item.title}`, getErr)
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

const downloadFile = async (item, filePath) => {
  const lockFilePath = `${filePath}${CONST_LOCK}`

  let writer = null
  let responseStream = null

  const progressBar = new cliProgress.SingleBar({
    format: `Downloading ${item.title} |${chalk.cyan('{bar}')}| {percentage}% || {value}/{total} Bytes || Speed: {speed}`,
    barCompleteChar: '\u2588',
    barIncompleteChar: '\u2591',
    hideCursor: true,
    etaBuffer: 100,
    formatValue: (v, options, type) => (type === 'total' || type === 'value') ? cliProgress.Format.bytes(v) : v,
    formatTotal: (t, options, type) => (type === 'total') ? cliProgress.Format.bytes(t) : t
  }, cliProgress.Presets.shades_classic)

  try {
    if (fsc.existsSync(lockFilePath)) {
      logWarning(`Already writing ${item.title} (lock file exists), skipping.`)
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
    debugLog(`Download Headers for ${item.title}`, response.headers)

    if (totalLength === MAX_OCTET_RECORDING) {
      logWarning(`Skipping ${item.title}, it appears to be currently recording.`)
      responseStream.destroy()
      await fs.unlink(lockFilePath).catch(delErr => logWarning(`Could not delete lock file on skip ${lockFilePath}: ${delErr.message}`))
      return { recorded: false, warning: "Skipping item, it's currently recording" }
    }

    if (totalLength === 0) {
      logWarning(`Skipping ${item.title}, content length is zero.`)
      responseStream.destroy()
      await fs.unlink(lockFilePath).catch(delErr => logWarning(`Could not delete lock file on zero size ${lockFilePath}: ${delErr.message}`))
      return { recorded: false, warning: 'Skipping item, content length is zero' }
    }

    writer = fsc.createWriteStream(filePath)

    progressBar.start(totalLength, 0, { speed: 'N/A' })
    let downloadedLength = 0
    let lastUpdateTime = Date.now()

    responseStream.on('data', (chunk) => {
      downloadedLength += chunk.length
      const now = Date.now()
      if (now - lastUpdateTime > 250 || downloadedLength === totalLength) {
        const elapsedSeconds = (now - progressBar.startTime) / 1000
        const speed = elapsedSeconds > 0 ? downloadedLength / elapsedSeconds : 0
        progressBar.update(downloadedLength, { speed: `${cliProgress.Format.bytes(speed)}/s` })
        lastUpdateTime = now
      }
    })

    responseStream.pipe(writer)

    return new Promise((resolve, reject) => {
      writer.on('finish', async () => {
        progressBar.stop()
        log(`Finished writing: ${path.basename(filePath)}`)
        try {
          await fs.unlink(lockFilePath)
          resolve({ recorded: true })
        } catch (unlinkErr) {
          logWarning(`Could not remove lock file ${lockFilePath} after successful download: ${unlinkErr.message}`)
          resolve({ recorded: true, warning: `Could not remove lock file: ${unlinkErr.message}` })
        }
      })

      writer.on('error', (err) => {
        progressBar.stop()
        logError(`Error writing file ${path.basename(filePath)}: ${err.message}`)
        debugLog(`File Write Error Details (${item.title})`, err)
        if (responseStream) responseStream.destroy()
        fs.unlink(lockFilePath).catch(delErr => logWarning(`Could not delete lock file ${lockFilePath} on write error: ${delErr.message}`))
        fs.unlink(filePath).catch(delErr => logWarning(`Could not delete partial file ${filePath} on write error: ${delErr.message}`))
        reject(err)
      })

      responseStream.on('error', (err) => {
        progressBar.stop()
        logError(`Error downloading ${item.title}: ${err.message}`)
        debugLog(`Download Stream Error Details (${item.title})`, err)

        if (writer && !writer.closed) writer.close()

        fs.unlink(lockFilePath).catch(delErr => logWarning(`Could not delete lock file ${lockFilePath} on download error: ${delErr.message}`))
        fs.unlink(filePath).catch(delErr => logWarning(`Could not delete partial file ${filePath} on download error: ${delErr.message}`))

        if (err.message.includes('Premature close') || err.message.includes('IncompleteRead') || err.code === 'ECONNRESET') {
          logWarning(`Download for ${item.title} may be incomplete (Network/FetchTV issue?). Check file size.`)
          resolve({ recorded: true, warning: 'Final read might be short (FetchTV/Network issue?). File might be okay but check size.' })
        } else {
          reject(err)
        }
      })
    })

  } catch (error) {
    progressBar.stop()
    if (responseStream) responseStream.destroy()
    if (writer && !writer.closed) writer.close()
    debugLog(`Outer Download Error (${item.title})`, error)

    try {
      if (fsc.existsSync(lockFilePath)) await fs.unlink(lockFilePath)
    } catch (cleanupErr) {
      logWarning(`Could not clean up lock file ${lockFilePath} on error: ${cleanupErr.message}`)
    }

    try {
      if (fsc.existsSync(filePath)) await fs.unlink(filePath)
    } catch (cleanupErr) {
      logWarning(`Could not clean up partial file ${filePath} on error: ${cleanupErr.message}`)
    }

    logError(`Failed to download ${item.title}: ${error.message}`)
    return { recorded: false, error: error.message }
  }
}

const parseLocations = async (locationsUrls) => {
  const results = []
  const xmlParserOptions = { ignoreAttributes: false, attributeNamePrefix: '@_', textNodeName: '#text', parseAttributeValue: true, removeNSPrefix: true }
  const xmlParser = new XMLParser(xmlParserOptions)

  for (const url of locationsUrls) {
    try {
      const response = await axios.get(url, { timeout: REQUEST_TIMEOUT })
      if (XMLValidator.validate(response.data) !== true) {
        logWarning(`Invalid XML from ${url}`)
        continue
      }
      const xmlRoot = xmlParser.parse(response.data)
      debugLog(`Parsed XML from ${url}`, xmlRoot)

      const device = xmlRoot?.root?.device
      if (!device) {
        logWarning(`Could not find device info in XML from ${url}`)
        continue
      }

      const loc = {
        url: url,
        deviceType: device.deviceType,
        friendlyName: device.friendlyName,
        manufacturer: device.manufacturer,
        manufacturerURL: device.manufacturerURL,
        modelDescription: device.modelDescription,
        modelName: device.modelName,
        modelNumber: device.modelNumber,
        _rawDeviceXml: device
      }
      results.push(loc)
    } catch (err) {
      if (err.code === 'ECONNABORTED' || err.message.includes('timeout')) {
        logWarning(`Timeout reading from ${url}`)
      } else {
        logWarning(`Connection error for ${url}: ${err.message}`)
      }
      debugLog(`Error fetching/parsing ${url}`, err)
    }
  }
  return results
}

const parseXml = (xmlString) => {
  if (!xmlString || typeof xmlString !== 'string') return null
  const xmlParser = new XMLParser({
    ignoreAttributes: false,
    attributeNamePrefix: '@_',
    textNodeName: '#text',
    parseAttributeValue: true,
    removeNSPrefix: true
  })
  try {
    const isLikelyEmbedded = xmlString.trim().startsWith('<DIDL-Lite')
    if (isLikelyEmbedded) {
      if (XMLValidator.validate(xmlString) === true) return xmlParser.parse(xmlString)
      logWarning('Failed to validate embedded DIDL-Lite XML.')
      return null
    } else {
      if (XMLValidator.validate(xmlString) !== true) {
        logWarning('Invalid XML received (not DIDL-Lite or Envelope?).')
        return null
      }
      return xmlParser.parse(xmlString)
    }
  } catch (error) {
    logError(`XML parsing error: ${error.message}`)
    return null
  }
}

const getXmlAttr = (node, attrName, defaultValue = '') =>
  node?.[`@_${attrName}`] ?? defaultValue

const getXmlText = (node, defaultValue = '') => {
  if (typeof node === 'object' && node !== null && node.hasOwnProperty('#text')) return node['#text']
  if (typeof node === 'string') return node
  return defaultValue
}

const findNode = (obj, path) =>
  path.split('.').reduce((current, key) => {
    const foundKey = Object.keys(current || {}).find(k => k.endsWith(key))
    return current && foundKey ? current[foundKey] : undefined
  }, obj)

const createValidFilename = (filename = '') => {
  let result = filename.toString().trim()
  result = result.replace(/[<>:"/\\|?*]/g, '')
  result = result.replace(/[\t ]+/g, '_')
  return result.slice(0, MAX_FILENAME)
}

const tsToSeconds = (ts = '0') => {
  try {
    return ts.split(':').reverse().reduce((acc, unit, i) => acc + parseFloat(unit) * (60 ** i), 0)
  } catch (e) {
    return 0
  }
}

const log = (message) => console.log(message)
const logWarning = (message) => console.log(chalk.yellow.bold(message))
const logError = (message) => console.log(chalk.red.bold(message))
const logHeading = (title) => console.log(chalk.blueBright.bold(title))

const debugLog = (label, data) => {
  if (!argv.debug) return
  console.log(chalk.magentaBright(`\n--- DEBUG: ${label} ---`))
  console.log(util.inspect(data, { showHidden: false, depth: null, colors: true }))
  console.log(chalk.magentaBright(`--- END DEBUG: ${label} ---\n`))
}

const formatItem = (item) => ({
  id: item.id,
  title: item.title,
  type: item.item_type,
  duration: item.duration,
  size: item.size,
  description: item.description
})

main().catch(error => {
  logError('\n--- An unexpected error occurred ---')
  logError(error.message)
  if (argv.debug || !error.response) {
    console.error(error.stack)
  } else if (error.response) {
    logError(`Status: ${error.response.status} ${error.response.statusText}`)
    logError(`URL: ${error.config.url}`)
    if (error.response.data) logError(`Response Data: ${typeof error.response.data === 'string' ? error.response.data.slice(0, 200) + '...' : '[Object/Stream]'}`)
  }
  logError('----------------------------------')
  process.exit(1)
})
