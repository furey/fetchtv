'use strict'

const SOUND_NOTES = {
  start: [{ at: 0, hz: 880, length: 0.12, level: 0.35 }],
  found: [
    { at: 0, hz: 659.25, length: 0.5, level: 0.8 },
    { at: 0.12, hz: 880, length: 0.55, level: 0.8 }
  ],
  finished: [
    { at: 0, hz: 523.25, length: 0.6, level: 0.7 },
    { at: 0.11, hz: 659.25, length: 0.6, level: 0.7 },
    { at: 0.22, hz: 783.99, length: 0.5, level: 0.75 }
  ],
  failed: [
    { at: 0, hz: 392, length: 0.45, level: 0.7, soft: true },
    { at: 0.16, hz: 329.63, length: 0.5, level: 0.7, soft: true }
  ]
}

const SOUND_VOLUME = 0.12
const SOUND_SECONDS = 0.8

const scheduleSound = ({ context, name, startTime = context.currentTime }) => {
  const output = context.createGain()
  output.gain.value = SOUND_VOLUME
  output.connect(context.destination)
  SOUND_NOTES[name].forEach(note => scheduleNote({ context, output, note, startTime }))
}

const scheduleNote = ({ context, output, note, startTime }) => {
  const begin = startTime + note.at
  const end = begin + note.length
  const envelope = context.createGain()
  envelope.gain.setValueAtTime(0.0001, begin)
  envelope.gain.exponentialRampToValueAtTime(note.level, begin + 0.012)
  envelope.gain.exponentialRampToValueAtTime(0.0001, end)
  const filter = context.createBiquadFilter()
  filter.type = 'lowpass'
  filter.frequency.value = note.soft ? 1200 : 3200
  envelope.connect(filter)
  filter.connect(output)
  const partials = note.soft ? [[1, 1]] : [[1, 1], [2, 0.18], [3, 0.05]]
  partials.forEach(([multiple, level]) => {
    const oscillator = context.createOscillator()
    const partialGain = context.createGain()
    oscillator.type = note.soft ? 'triangle' : 'sine'
    oscillator.frequency.value = note.hz * multiple
    partialGain.gain.value = level
    oscillator.connect(partialGain)
    partialGain.connect(envelope)
    oscillator.start(begin)
    oscillator.stop(end + 0.02)
  })
}

const createSounds = () => {
  let enabled = true
  let context = null
  const play = (name) => {
    if (!enabled || !SOUND_NOTES[name]) return
    try {
      context = context ?? new AudioContext()
      if (context.state === 'suspended') context.resume()
      scheduleSound({ context, name })
    } catch {
      context = null
    }
  }
  const render = async (name) => {
    const sampleRate = 44100
    const offline = new OfflineAudioContext(1, Math.ceil(sampleRate * SOUND_SECONDS), sampleRate)
    scheduleSound({ context: offline, name, startTime: 0 })
    const buffer = await offline.startRendering()
    return Array.from(buffer.getChannelData(0))
  }
  return {
    play,
    render,
    setEnabled: (on) => { enabled = Boolean(on) },
    isEnabled: () => enabled
  }
}

const sounds = createSounds()
