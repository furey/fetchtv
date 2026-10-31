import fs from 'node:fs/promises'
import { saveRecordings } from '../../fetchtv.js'

export const startSave = ({ recordings, folder, forPlex, retryDelays, onProgress, onResult }) => {
  const controller = new AbortController()
  const done = runSave({ recordings, folder, forPlex, retryDelays, onProgress, onResult, signal: controller.signal })
  return { cancel: () => controller.abort(), done }
}

export const describeResult = (result) => ({
  id: String(result.item.id),
  status: result.status,
  reason: plainReason(result)
})

export const summariseResults = (described) => {
  const count = (statuses) => described.filter(result => statuses.includes(result.status)).length
  return {
    total: described.length,
    saved: count(['saved']),
    alreadySaved: count(['already_saved']),
    notSaved: count(['failed', 'locked', 'still_recording']),
    stopped: count(['cancelled'])
  }
}

export const plainReason = (result) => {
  const text = `${result.error ?? ''} ${result.warning ?? ''}`
  if (result.status === 'saved') return ''
  if (result.status === 'already_saved') return 'Already saved in this folder.'
  if (result.status === 'still_recording') return 'Still recording on the box. Save it after the recording ends.'
  if (result.status === 'locked') return 'Another save is using this recording right now.'
  if (result.status === 'cancelled') return result.warning?.includes('before') ? 'Not started.' : 'Stopped part way. Save again to carry on from where it stopped.'
  return failureReason(text)
}

const failureReason = (text) => {
  if (/ENOSPC/.test(text)) return 'The folder ran out of space.'
  if (/EACCES|EPERM|EROFS/.test(text)) return 'Could not write to this folder. Choose another folder.'
  if (/rejected the resume/.test(text)) return 'The box would not carry on from where the copy stopped. Save again to start this recording again.'
  if (/ECONNREFUSED|EHOSTUNREACH|ENETUNREACH|ENOTFOUND/.test(text)) return 'The box stopped answering. Check that it is turned on and connected, then save again.'
  if (/status code 4\d\d|status code 5\d\d/.test(text)) return 'The box would not send this recording.'
  if (/interrupted|mismatch|incomplete|Timeout|timeout|reset|Premature|empty file/.test(text)) return 'The copy stopped part way, even after trying again. Save again to carry on from where it stopped.'
  return 'The copy did not finish. Save again to try once more.'
}

const runSave = async ({ recordings, folder, forPlex, retryDelays, onProgress, onResult, signal }) => {
  await fs.mkdir(folder, { recursive: true })
  const described = []
  await saveRecordings({
    recordings,
    savePath: folder,
    forPlex,
    retryDelays,
    onProgress,
    onResult: (result) => {
      const entry = describeResult(result)
      described.push(entry)
      onResult(entry)
    },
    signal
  })
  return { summary: summariseResults(described), results: described }
}
