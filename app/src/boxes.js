import { discoverFetch, discoverFetchServers } from '../../fetchtv.js'

export const FETCH_PORT = 49152

export const findBoxes = async ({ rememberedAddress, searchNetwork = true }) => {
  const [remembered, found] = await Promise.all([
    rememberedAddress ? connectToAddress(rememberedAddress) : null,
    searchNetwork ? searchNetworkTwice() : []
  ])
  return uniqueByUrl([remembered, ...found].filter(Boolean))
}

export const connectToAddress = async (address) => {
  const target = parseBoxAddress(address)
  if (!target) return null
  return withTimeout(discoverFetch(target), CONNECT_TIMEOUT_MS)
}

export const parseBoxAddress = (input) => {
  const text = String(input ?? '').trim().replace(/^https?:\/\//i, '').replace(/\/.*$/, '')
  const match = text.match(/^([a-z0-9.-]+)(?::(\d{1,5}))?$/i)
  if (!match) return null
  const [, ip, portText] = match
  const port = portText ? Number(portText) : FETCH_PORT
  if (port < 1 || port > 65535) return null
  if (/^[\d.]+$/.test(ip) && !isIpv4(ip)) return null
  return { ip, port }
}

export const describeBox = (location) => {
  const url = new URL(location.url)
  const port = Number(url.port || 80)
  return {
    id: location.url,
    name: location.friendlyName || 'Fetch box',
    model: location.modelName || '',
    address: port === FETCH_PORT ? url.hostname : `${url.hostname}:${port}`
  }
}

const SEARCH_ROUND_MS = 4000
const CONNECT_TIMEOUT_MS = 8000

const searchNetworkTwice = async () => {
  const first = await discoverFetchServers({ timeoutMs: SEARCH_ROUND_MS })
  if (first.length > 0) return first
  return discoverFetchServers({ timeoutMs: SEARCH_ROUND_MS })
}

const uniqueByUrl = (locations) => [...new Map(locations.map(location => [location.url, location])).values()]

const isIpv4 = (text) => {
  const parts = text.split('.')
  return parts.length === 4 && parts.every(part => /^\d{1,3}$/.test(part) && Number(part) <= 255)
}

const withTimeout = (promise, ms) => Promise.race([
  promise.catch(() => null),
  new Promise(resolve => setTimeout(() => resolve(null), ms))
])
