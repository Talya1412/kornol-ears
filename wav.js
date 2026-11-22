'use strict';

const SAMPLE_RATE = 48000;
const CHANNELS = 2;
const BYTES_PER_FRAME_SAMPLE = 2; // s16le
const MAX_PLAYBACK_SAMPLES = SAMPLE_RATE * 5 * 60;

function downmixStereoToMono(stereoPcm) {
  const frames = Math.floor(stereoPcm.length / (CHANNELS * BYTES_PER_FRAME_SAMPLE));
  const mono = Buffer.alloc(frames * BYTES_PER_FRAME_SAMPLE);
  for (let i = 0; i < frames; i++) {
    const l = stereoPcm.readInt16LE(i * CHANNELS * BYTES_PER_FRAME_SAMPLE);
    const r = stereoPcm.readInt16LE((i * CHANNELS + 1) * BYTES_PER_FRAME_SAMPLE);
    mono.writeInt16LE(Math.round((l + r) / 2), i * BYTES_PER_FRAME_SAMPLE);
  }
  return mono;
}

function encodeWavPcm16Mono(pcm, sampleRate) {
  const header = Buffer.alloc(44);
  header.write('RIFF', 0, 'ascii');
  header.writeUInt32LE(36 + pcm.length, 4);
  header.write('WAVE', 8, 'ascii');
  header.write('fmt ', 12, 'ascii');
  header.writeUInt32LE(16, 16); // fmt chunk size
  header.writeUInt16LE(1, 20); // audio format: PCM
  header.writeUInt16LE(1, 22); // channels: mono
  header.writeUInt32LE(sampleRate, 24);
  header.writeUInt32LE(sampleRate * BYTES_PER_FRAME_SAMPLE, 28); // byte rate
  header.writeUInt16LE(BYTES_PER_FRAME_SAMPLE, 32); // block align
  header.writeUInt16LE(16, 34); // bits per sample
  header.write('data', 36, 'ascii');
  header.writeUInt32LE(pcm.length, 40);
  return Buffer.concat([header, pcm]);
}

// Parse a WAV buffer and return its audio as 48 kHz / 16-bit / mono PCM.
// Other sample rates are converted by sample duplication for integer
// ratios (24k -> x2, 16k -> x3) and nearest-neighbour otherwise, which
// is plenty for speech.
function decodeWavToPcm48k(buf) {
  if (buf.length < 12 || buf.toString('ascii', 0, 4) !== 'RIFF' || buf.toString('ascii', 8, 12) !== 'WAVE') {
    throw new Error('not a RIFF/WAVE buffer');
  }
  const end = buf.readUInt32LE(4) + 8;
  if (end < 12 || end > buf.length) throw new Error('truncated WAV container');

  let format = null;
  let data = null;
  let off = 12;
  while (off + 8 <= end) {
    const id = buf.toString('ascii', off, off + 4);
    const size = buf.readUInt32LE(off + 4);
    const body = off + 8;
    const next = body + size + (size % 2);
    if (next > end) throw new Error(`truncated WAV chunk ${id}`);
    if (id === 'fmt ') {
      if (size < 16) throw new Error('incomplete WAV format chunk');
      const audioFormat = buf.readUInt16LE(body);
      if (audioFormat !== 1) throw new Error(`unsupported WAV format ${audioFormat} (need PCM)`);
      format = {
        channels: buf.readUInt16LE(body + 2),
        sampleRate: buf.readUInt32LE(body + 4),
        blockAlign: buf.readUInt16LE(body + 12),
        bitsPerSample: buf.readUInt16LE(body + 14),
      };
    } else if (id === 'data') {
      data = buf.subarray(body, body + size);
      break;
    }
    off = next;
  }
  if (!format) throw new Error('WAV has no format chunk before data');
  if (!data) throw new Error('WAV has no data chunk');
  const { channels, sampleRate, blockAlign, bitsPerSample } = format;
  if (bitsPerSample !== 16) throw new Error(`unsupported WAV bits ${bitsPerSample} (need 16)`);
  if (channels !== 1 && channels !== CHANNELS) throw new Error(`unsupported WAV channels ${channels}`);
  if (!sampleRate) throw new Error('invalid WAV sample rate');
  if (blockAlign !== channels * BYTES_PER_FRAME_SAMPLE || data.length % blockAlign !== 0) {
    throw new Error('WAV data does not contain complete sample frames');
  }

  const inSamples = data.length / blockAlign;
  const ratio = SAMPLE_RATE / sampleRate;
  const outSamples = Math.floor(inSamples * ratio);
  if (outSamples > MAX_PLAYBACK_SAMPLES) throw new Error('WAV exceeds playback length limit');

  const mono = channels === CHANNELS ? downmixStereoToMono(data) : data;
  if (sampleRate === SAMPLE_RATE) return { pcm: mono, sampleRate: SAMPLE_RATE };
  const out = Buffer.alloc(outSamples * BYTES_PER_FRAME_SAMPLE);
  for (let i = 0; i < outSamples; i++) {
    const src = Math.min(inSamples - 1, Math.floor(i / ratio));
    out.writeInt16LE(mono.readInt16LE(src * BYTES_PER_FRAME_SAMPLE), i * BYTES_PER_FRAME_SAMPLE);
  }
  return { pcm: out, sampleRate: SAMPLE_RATE };
}

module.exports = {
  SAMPLE_RATE, CHANNELS, BYTES_PER_FRAME_SAMPLE,
  downmixStereoToMono, encodeWavPcm16Mono, decodeWavToPcm48k,
};
