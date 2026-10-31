import { getFetchRecordings, isCurrentlyRecording, sortRecordingsByTitle } from '../../fetchtv.js'

export const listRecordings = async ({ location, onProgress }) => {
  const shows = sortRecordingsByTitle(await getFetchRecordings({ location, filters: {}, onProgress }))
  const summaries = await Promise.all(shows.map(summariseShow))
  return { shows, summaries }
}

export const pickEpisodes = ({ shows, episodeIds }) => {
  const wanted = new Set(episodeIds.map(String))
  return shows
    .map(show => ({ ...show, items: show.items.filter(item => wanted.has(String(item.id))) }))
    .filter(show => show.items.length > 0)
}

const summariseShow = async (show) => ({
  id: String(show.id),
  title: show.title,
  episodes: await Promise.all(show.items.map(summariseEpisode))
})

const summariseEpisode = async (item) => ({
  id: String(item.id),
  title: item.title,
  size: Number.isFinite(item.size) && item.size > 0 ? item.size : 0,
  duration: item.duration || 0,
  stillRecording: await isCurrentlyRecording(item)
})
