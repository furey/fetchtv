import { parseArgs } from 'node:util'
import { MB, demoShows, startFakeBox } from './fake-box.mjs'

const { values } = parseArgs({
  allowNegative: true,
  options: {
    port: { type: 'string', default: '49152' },
    host: { type: 'string', default: '0.0.0.0' },
    'size-mb': { type: 'string', default: '3' },
    'mb-per-second': { type: 'string', default: '0' },
    ssdp: { type: 'boolean', default: true },
    empty: { type: 'boolean', default: false }
  }
})

const box = await startFakeBox({
  shows: values.empty ? [] : demoShows({ size: Number(values['size-mb']) * MB }),
  name: 'Fake Fetch Box',
  host: values.host,
  port: Number(values.port),
  ssdp: values.ssdp,
  bytesPerSecond: Number(values['mb-per-second']) * MB
})

console.log(`Fake Fetch box at ${box.address} (${box.url}). Press Ctrl+C to stop.`)

process.on('SIGINT', async () => {
  await box.stop()
  process.exit(0)
})
