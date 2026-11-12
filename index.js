#!/usr/bin/env node
/*
 * kornol-ears — minimal Node.js "ears" proof test for KORNOL voice realtime.
 *
 * Proves, inside a UDP-capable host (GitHub Codespace):
 *   1. The bot can JOIN a Discord voice channel post-DAVE (2026-03-01).
 *   2. It can PLAY a generated tone (send path).
 *   3. It can RECEIVE + DAVE-decrypt + Opus-decode other users' speech
 *      (receive path — impossible from the Python stack), proven by
 *      RMS > 0 on real utterances recorded in results/utterances.jsonl.
 *
 * Intents: Guilds + GuildVoiceStates. When VOICE_LOOP=1 (v2 conversation
 * relay), GuildMessages + MessageContent are added so the process can
 * post heard utterances to a relay text channel and pick up replies.
 * Token comes ONLY from env DISCORD_BOT_TOKEN and is never printed.
 *
 * v4: when BUS_URL + BUS_TOKEN + BUS_SESSION are set, utterances,
 * greetings, fillers and chunked replies travel over a WebSocket bus
 * instead of the Discord relay channel (see createBusClient for the
 * fixed wire protocol). Any bus failure falls back to the Discord
 * transport for the rest of the session; an utterance is never lost
 * because the bus died. v4 also adds filler playback, barge-in, a
 * 650 ms silence close and a 20 s per-utterance force-flush.
 *
 * v4.1: the bus client sends a {"t":"ping"} keepalive every 30 s (an
 * idle bus socket otherwise dies on a ~2 min read timeout), and
 * LIVE_MODE=1 switches the uplink to streaming: while the bus is live,
 * decoded speech is downmixed/decimated to 16 kHz mono and shipped in
 * ~100 ms PCM frames ({t:'pcm'} + binary, {t:'pcm_end'} at segment
 * end) instead of per-utterance WAV files. If the bus is down or
 * unconfigured, LIVE mode falls back to the normal v4 utterance flow.
 *
 * Dry mode: `node index.js --dry` loads every module, verifies
 * @snazzah/davey loads and opusscript can encode+decode a generated
 * tone (roundtrip RMS check), prints "DRY OK" and exits. No network.
 */
'use strict';

const fs = require('fs');
const path = require('path');
const { Readable } = require('stream');
const { exec } = require('child_process');

const BASE = __dirname;
const RESULTS_DIR = path.join(BASE, 'results');
const SAMPLE_RATE = 48000;
const CHANNELS = 2;
const BYTES_PER_FRAME_SAMPLE = 2; // s16le
const LIVE_FRAME_BYTES = 3200; // 100 ms of 16 kHz s16le mono (LIVE mode uplink)

function log(...parts) {
  console.log(new Date().toISOString(), ...parts);
}

// ---------------------------------------------------------------- dry mode
function dryRun() {
  const t0 = Date.now();
  // Load every module the real run needs (constructs nothing network-bound).
  const discord = require('discord.js');
  const voice = require('@discordjs/voice');
  const OpusScript = require('opusscript');

  let daveyInfo = 'unknown';
  try {
    const davey = require('@snazzah/davey');
    const keys = Object.keys(davey || {});
    daveyInfo = `loaded (exports: ${keys.slice(0, 8).join(',') || 'none'})`;
    // If the binding exposes a version / session constructor name, surface it.
    if (davey.version) daveyInfo += ` version=${davey.version}`;
  } catch (e) {
    console.error('DRY FAIL: @snazzah/davey failed to load:', e.message);
    process.exit(1);
  }

  try {
    require('libsodium-wrappers');
  } catch (e) {
    console.error('DRY FAIL: libsodium-wrappers failed to load:', e.message);
    process.exit(1);
  }

  try {
    require('ws');
  } catch (e) {
    console.error('DRY FAIL: ws failed to load:', e.message);
    process.exit(1);
  }

  // Generate a 1-second 440Hz+880Hz stereo tone (same generator as live mode).
  const pcm = generateTonePcm(1);
  // opusscript roundtrip: encode in 20 ms frames (960 samples/channel), decode back.
  const encoder = new OpusScript(SAMPLE_RATE, CHANNELS, OpusScript.Application.AUDIO);
  const decoder = new OpusScript(SAMPLE_RATE, CHANNELS, OpusScript.Application.AUDIO);
  const frameSamples = 960; // per channel
  const frameBytes = frameSamples * CHANNELS * BYTES_PER_FRAME_SAMPLE;
  let decoded = 0;
  let sumSq = 0;
  let peak = 0;
  let packets = 0;
  for (let off = 0; off + frameBytes <= pcm.length; off += frameBytes) {
    const frame = pcm.subarray(off, off + frameBytes);
    const packet = encoder.encode(frame, frameSamples);
    packets++;
    const out = decoder.decode(packet, frameSamples);
    decoded += out.length;
    for (let i = 0; i + 1 < out.length; i += 2) {
      const v = out.readInt16LE(i);
      sumSq += v * v;
      const a = Math.abs(v);
      if (a > peak) peak = a;
    }
  }
  try { encoder.delete(); decoder.delete(); } catch (_) {}
  const totalSamples = decoded / BYTES_PER_FRAME_SAMPLE;
  // WAV relay roundtrip: encode 0.1s of mono sine, parse it back, and
  // confirm the sample count survives (this is the v2 EARS_UTT format).
  const monoSamples = Math.floor(SAMPLE_RATE * 0.1);
  const monoPcm = Buffer.alloc(monoSamples * BYTES_PER_FRAME_SAMPLE);
  for (let i = 0; i < monoSamples; i++) {
    monoPcm.writeInt16LE(
      Math.round(0.3 * 32767 * Math.sin(2 * Math.PI * 440 * (i / SAMPLE_RATE))),
      i * BYTES_PER_FRAME_SAMPLE
    );
  }
  const wavBuf = encodeWavPcm16Mono(monoPcm, SAMPLE_RATE);
  const roundtrip = decodeWavToPcm48k(wavBuf);
  const wavOutSamples = roundtrip.pcm.length / BYTES_PER_FRAME_SAMPLE;
  console.log(`DRY wav roundtrip: samplesIn=${monoSamples} samplesOut=${wavOutSamples} rate=${roundtrip.sampleRate}`);
  if (wavOutSamples !== monoSamples) {
    console.error('DRY FAIL: WAV roundtrip sample count mismatch');
    process.exit(1);
  }

  const rms = totalSamples > 0 ? Math.sqrt(sumSq / totalSamples) : 0;

  let voiceVersion = 'loaded';
  try {
    voiceVersion = JSON.parse(fs.readFileSync(path.join(BASE, 'node_modules', '@discordjs', 'voice', 'package.json'), 'utf8')).version;
  } catch (_) {}
  console.log(`DRY modules: discord.js=${discord.version || 'loaded'} @discordjs/voice=${voiceVersion}`);
  console.log(`DRY davey: ${daveyInfo}`);
  console.log(`DRY opus roundtrip: packets=${packets} pcmIn=${pcm.length}B pcmOut=${decoded}B rms=${rms.toFixed(1)} peak=${peak}`);
  if (!(rms > 100 && peak > 1000)) {
    console.error('DRY FAIL: opus roundtrip RMS/peak too low — decode is not producing the tone');
    process.exit(1);
  }
  if (process.env.BUS_SELFTEST === '1') {
    busSelfTest()
      .then(() => {
        console.log('BUS SELFTEST OK');
        console.log(`DRY OK (${Date.now() - t0} ms)`);
        process.exit(0);
      })
      .catch((e) => {
        console.error('BUS SELFTEST FAIL:', e.message);
        process.exit(1);
      });
    return;
  }
  console.log(`DRY OK (${Date.now() - t0} ms)`);
  process.exit(0);
}

// ------------------------------------------------------- bus self-test
// BUS_SELFTEST=1 + --dry: stand up a throwaway ws server on an ephemeral
// port and drive the REAL BusClient through connect -> utt -> reply, so
// the bus code paths (URL shape, frame order, text/binary pairing) are
// exercised offline with no Discord involved.
async function busSelfTest() {
  const { WebSocketServer } = require('ws');
  const wss = new WebSocketServer({ port: 0 });
  await new Promise((resolve) => wss.on('listening', resolve));
  const port = wss.address().port;
  const pcm = Buffer.alloc(Math.floor(SAMPLE_RATE * 0.05) * BYTES_PER_FRAME_SAMPLE);
  const wav = encodeWavPcm16Mono(pcm, SAMPLE_RATE);
  const pcmFrame = Buffer.alloc(LIVE_FRAME_BYTES); // 100 ms of 16 kHz s16le
  let client = null;
  try {
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('bus self-test timed out')), 8000);
      const fail = (e) => { clearTimeout(timer); reject(e); };
      let pendingFrame = null; // last text frame; the next binary belongs to it
      let replyDone = false;
      let pcmDone = false;
      const checkDone = () => {
        if (replyDone && pcmDone) { clearTimeout(timer); resolve(); }
      };
      wss.on('connection', (sock) => {
        // Exercise the keepalive path: the client must ignore this.
        sock.send(JSON.stringify({ t: 'ping' }));
        sock.on('message', (data, isBinary) => {
          try {
            if (!isBinary) {
              const frame = JSON.parse(data.toString('utf8'));
              if (frame.t === 'utt') {
                if (frame.seq !== 1 || frame.user !== '42' || frame.lang !== 'en') {
                  fail(new Error(`bad utt frame ${JSON.stringify(frame)}`));
                  return;
                }
                pendingFrame = frame;
              } else if (frame.t === 'pcm') {
                if (frame.user !== '42' || typeof frame.name !== 'string') {
                  fail(new Error(`bad pcm frame ${JSON.stringify(frame)}`));
                  return;
                }
                pendingFrame = frame;
              }
              return;
            }
            if (!pendingFrame) { fail(new Error('server got binary before any text frame')); return; }
            const bin = Buffer.from(data);
            if (pendingFrame.t === 'utt') {
              if (!bin.equals(wav)) { fail(new Error('server got wrong utt WAV bytes')); return; }
              const seq = pendingFrame.seq;
              pendingFrame = null;
              sock.send(JSON.stringify({ t: 'reply', seq, chunk: 0, last: true, heard: 'selftest', text: 'selftest' }));
              sock.send(wav);
            } else if (pendingFrame.t === 'pcm') {
              if (bin.length % 2 !== 0) { fail(new Error(`pcm binary length ${bin.length} is not a multiple of 2`)); return; }
              if (!bin.equals(pcmFrame)) { fail(new Error('server got wrong pcm bytes')); return; }
              pendingFrame = null;
              pcmDone = true;
              checkDone();
            }
          } catch (e) { fail(e); }
        });
      });
      client = createBusClient({
        url: `ws://127.0.0.1:${port}`,
        session: 'selftest',
        token: 'selftest-token',
        onReply: (frame, replyWav) => {
          try {
            if (frame.seq !== 1) { fail(new Error(`reply seq ${frame.seq} != 1`)); return; }
            if (!replyWav.equals(wav)) { fail(new Error('client got wrong reply WAV bytes')); return; }
            replyDone = true;
            checkDone();
          } catch (e) { fail(e); }
        },
        onFiller: () => fail(new Error('unexpected filler in self-test')),
        onSkip: () => fail(new Error('unexpected skip in self-test')),
        onDown: (err) => fail(err || new Error('bus went down during self-test')),
      });
      client.connect()
        .then(() => {
          client.sendUtt({ seq: 1, user: '42', name: 'selftest', guild: '7', lang: 'en', dur: 0.05 }, wav);
          client.sendPcm('42', 'selftest', pcmFrame);
        })
        .catch(fail);
    });
  } finally {
    if (client) client.close();
    try { wss.close(); } catch (_) {}
  }
}

// ------------------------------------------------------------- tone helper
function generateTonePcm(seconds) {
  const totalSamples = Math.floor(SAMPLE_RATE * seconds);
  const buf = Buffer.alloc(totalSamples * CHANNELS * BYTES_PER_FRAME_SAMPLE);
  for (let i = 0; i < totalSamples; i++) {
    const t = i / SAMPLE_RATE;
    const v = Math.round(
      0.35 * 32767 * (Math.sin(2 * Math.PI * 440 * t) + Math.sin(2 * Math.PI * 880 * t)) / 2 * 2 * 0.5
    );
    const clamped = Math.max(-32768, Math.min(32767, v));
    for (let ch = 0; ch < CHANNELS; ch++) {
      buf.writeInt16LE(clamped, (i * CHANNELS + ch) * BYTES_PER_FRAME_SAMPLE);
    }
  }
  return buf;
}

// ------------------------------------------------------------- WAV helpers
// The relay speaks canonical 48 kHz / 16-bit / MONO WAV. The receiver
// decodes Discord Opus to 48 kHz STEREO PCM, so utterances are downmixed
// before encoding; reply WAVs (often 24 kHz mono from Gemini TTS) are
// parsed back and upsampled to 48 kHz mono for playback.

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

// Decimate 48 kHz mono s16le to 16 kHz by averaging each group of 3
// samples. `rest` carries the trailing 0-2 samples of the previous
// chunk that did not complete a group; returns { out, rest } the same
// way, so a continuous stream decimates correctly across chunk edges.
function decimate48kTo16k(monoPcm, rest) {
  const src = rest && rest.length ? Buffer.concat([rest, monoPcm]) : monoPcm;
  const inSamples = Math.floor(src.length / BYTES_PER_FRAME_SAMPLE);
  const groups = Math.floor(inSamples / 3);
  const out = Buffer.alloc(groups * BYTES_PER_FRAME_SAMPLE);
  for (let g = 0; g < groups; g++) {
    const i = g * 3 * BYTES_PER_FRAME_SAMPLE;
    const v = Math.round(
      (src.readInt16LE(i) + src.readInt16LE(i + BYTES_PER_FRAME_SAMPLE) +
        src.readInt16LE(i + 2 * BYTES_PER_FRAME_SAMPLE)) / 3
    );
    out.writeInt16LE(v, g * BYTES_PER_FRAME_SAMPLE);
  }
  return { out, rest: Buffer.from(src.subarray(groups * 3 * BYTES_PER_FRAME_SAMPLE)) };
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
  if (buf.length < 44 || buf.toString('ascii', 0, 4) !== 'RIFF' || buf.toString('ascii', 8, 12) !== 'WAVE') {
    throw new Error('not a RIFF/WAVE buffer');
  }
  let channels = 1;
  let sampleRate = SAMPLE_RATE;
  let bitsPerSample = 16;
  let data = null;
  let off = 12;
  while (off + 8 <= buf.length) {
    const id = buf.toString('ascii', off, off + 4);
    const size = buf.readUInt32LE(off + 4);
    const body = off + 8;
    if (id === 'fmt ') {
      const audioFormat = buf.readUInt16LE(body);
      if (audioFormat !== 1) throw new Error(`unsupported WAV format ${audioFormat} (need PCM)`);
      channels = buf.readUInt16LE(body + 2);
      sampleRate = buf.readUInt32LE(body + 4);
      bitsPerSample = buf.readUInt16LE(body + 14);
    } else if (id === 'data') {
      data = buf.subarray(body, Math.min(body + size, buf.length));
      break;
    }
    off = body + size + (size % 2); // chunks are word-aligned
  }
  if (!data) throw new Error('WAV has no data chunk');
  if (bitsPerSample !== 16) throw new Error(`unsupported WAV bits ${bitsPerSample} (need 16)`);

  let mono;
  if (channels === 2) mono = downmixStereoToMono(data);
  else if (channels === 1) mono = data;
  else throw new Error(`unsupported WAV channels ${channels}`);

  if (sampleRate === SAMPLE_RATE) return { pcm: mono, sampleRate };
  const inSamples = Math.floor(mono.length / BYTES_PER_FRAME_SAMPLE);
  const ratio = SAMPLE_RATE / sampleRate;
  const outSamples = Math.floor(inSamples * ratio);
  const out = Buffer.alloc(outSamples * BYTES_PER_FRAME_SAMPLE);
  for (let i = 0; i < outSamples; i++) {
    const src = Math.min(inSamples - 1, Math.floor(i / ratio));
    out.writeInt16LE(mono.readInt16LE(src * BYTES_PER_FRAME_SAMPLE), i * BYTES_PER_FRAME_SAMPLE);
  }
  return { pcm: out, sampleRate };
}

// ------------------------------------------------------------ bus client
// v4 bus transport. Wire protocol (fixed by the brain side — do not
// invent variants):
//   Connect: WebSocket to
//     `${url}/bus?session=${session}&role=ears&token=${token}`
//   Text frames are JSON; a binary frame always belongs to the text
//   frame immediately before it.
//   Outgoing: {t:'utt',seq,user,name,guild,lang,dur} + binary WAV
//             {t:'greet',guild,lang}
//             {t:'barge',seq}
//             {t:'pcm',user,name} + binary 16 kHz s16le (LIVE mode)
//             {t:'pcm_end',user}            (LIVE mode, no binary)
//             {t:'ping'} every 30 s while open (keepalive)
//   Incoming: {t:'filler',seq} + binary WAV
//             {t:'reply',seq,chunk,last,heard,text} + binary WAV
//             {t:'skip',seq}            (no binary follows)
//             {t:'ping'}                (ignored)
// The client is deliberately policy-free: main() decides when the bus
// is used and when to fall back to Discord. The client reports through
// onDown — fired at most once — that it can no longer be used (connect
// failure or a mid-session drop); there is no reconnection, a drop
// means Discord transport for the rest of the session.
function createBusClient({ url, session, token, onReply, onFiller, onSkip, onDown }) {
  const WebSocket = require('ws');
  let sock = null;
  let open = false;
  let down = false;
  let pending = null; // last filler/reply text frame awaiting its binary
  let pingTimer = null;

  // Keepalive: an idle bus socket dies on a ~2 min read timeout
  // (observed live), so while the socket is open a {"t":"ping"} text
  // frame goes out every 30 s. Incoming pings are ignored.
  function startPing() {
    stopPing();
    pingTimer = setInterval(() => {
      try {
        if (sock && sock.readyState === WebSocket.OPEN) {
          sock.send(JSON.stringify({ t: 'ping' }));
        }
      } catch (_) {}
    }, 30_000);
    if (pingTimer.unref) pingTimer.unref();
  }

  function stopPing() {
    if (pingTimer) { clearInterval(pingTimer); pingTimer = null; }
  }

  function markDown(err) {
    if (down) return;
    down = true;
    open = false;
    stopPing();
    if (onDown) { try { onDown(err); } catch (_) {} }
  }

  function handleText(text) {
    let frame;
    try { frame = JSON.parse(text); } catch (_) { log('BUS BAD FRAME (not JSON)'); return; }
    if (!frame || typeof frame.t !== 'string') { log('BUS BAD FRAME (no type)'); return; }
    if (frame.t === 'ping') return; // keepalive: no binary follows, nothing to do
    if (frame.t === 'filler' || frame.t === 'reply') {
      pending = frame; // its WAV arrives as the very next binary frame
    } else if (frame.t === 'skip') {
      log(`BUS SKIP seq=${frame.seq}`);
      if (onSkip) { try { onSkip(frame); } catch (e) { log('BUS SKIP HANDLER FAIL:', e.message); } }
    } else {
      log(`BUS UNKNOWN FRAME t=${frame.t}`);
    }
  }

  function handleBinary(buf) {
    if (!pending) { log(`BUS ORPHAN BINARY ${buf.length} bytes (dropped)`); return; }
    const frame = pending;
    pending = null;
    try {
      if (frame.t === 'filler') { if (onFiller) onFiller(frame, buf); }
      else if (onReply) onReply(frame, buf);
    } catch (e) {
      log('BUS FRAME HANDLER FAIL:', e.message);
    }
  }

  const client = {
    connect() {
      return new Promise((resolve, reject) => {
        const full = `${url}/bus?session=${encodeURIComponent(session)}&role=ears&token=${encodeURIComponent(token)}`;
        let settled = false;
        try {
          sock = new WebSocket(full);
        } catch (e) {
          markDown(e);
          reject(e);
          return;
        }
        sock.on('open', () => {
          open = true;
          settled = true;
          log(`BUS CONNECTED session=${session}`);
          startPing();
          resolve();
        });
        sock.on('message', (data, isBinary) => {
          if (isBinary) handleBinary(Buffer.isBuffer(data) ? data : Buffer.from(data));
          else handleText(data.toString('utf8'));
        });
        sock.on('error', (e) => {
          log('BUS ERROR:', e.message);
          if (!settled) { settled = true; reject(e); }
          markDown(e);
        });
        sock.on('close', () => {
          if (open) log('BUS CLOSED');
          if (!settled) { settled = true; reject(new Error('bus socket closed before open')); }
          markDown(new Error('bus socket closed'));
        });
      });
    },
    usable() {
      return open && !down && sock !== null && sock.readyState === WebSocket.OPEN;
    },
    isDown() { return down; },
    // sendJson/sendUtt throw synchronously when the socket is not
    // writable, so callers can re-send the same payload over Discord.
    sendJson(obj) {
      if (!client.usable()) throw new Error('bus not connected');
      sock.send(JSON.stringify(obj));
    },
    sendUtt(frame, wav) {
      if (!client.usable()) throw new Error('bus not connected');
      sock.send(JSON.stringify(Object.assign({ t: 'utt' }, frame)));
      sock.send(wav);
    },
    // LIVE mode uplink: one ~100 ms frame of 16 kHz mono s16le PCM,
    // announced by its text frame. Throws like sendUtt when the socket
    // is not writable, so callers can stop streaming the segment.
    sendPcm(userId, name, buf) {
      if (!client.usable()) throw new Error('bus not connected');
      sock.send(JSON.stringify({ t: 'pcm', user: userId, name }));
      sock.send(buf);
    },
    sendGreet(guild, lang) { client.sendJson({ t: 'greet', guild, lang }); },
    sendBarge(seq) { client.sendJson({ t: 'barge', seq }); },
    close() {
      // Intentional close: mark down WITHOUT firing onDown (this is not
      // a failure anyone needs to fall back from).
      down = true;
      open = false;
      stopPing();
      try { if (sock) sock.close(); } catch (_) {}
    },
  };
  return client;
}

// ------------------------------------------------------------------- main
async function main() {
  const token = process.env.DISCORD_BOT_TOKEN;
  if (!token) {
    console.error('FATAL: DISCORD_BOT_TOKEN env var is required (never hardcode it)');
    process.exit(2);
  }
  const TEST_GUILD_ID = (process.env.TEST_GUILD_ID || '').trim() || null;
  const TEST_VOICE_CHANNEL_ID = (process.env.TEST_VOICE_CHANNEL_ID || '').trim() || null;
  const OWNER_USER_ID = (process.env.OWNER_USER_ID || '').trim() || null;
  const RELAY_CHANNEL_ID = (process.env.RELAY_CHANNEL_ID || '943298520335675462').trim();
  const VOICE_LOOP = process.env.VOICE_LOOP === '1';
  // v4.1 LIVE mode: stream decoded speech over the bus as 16 kHz PCM
  // frames instead of per-utterance WAV files. Engages per segment
  // only while the bus is actually usable; otherwise the segment takes
  // the normal v4 utterance path (bus down / unconfigured = v4).
  const LIVE_MODE = process.env.LIVE_MODE === '1';
  // v3: language the brain should greet the room in after a join.
  const GREET_LANG = (process.env.GREET_LANG || 'es').trim() || 'es';
  // v4: GREET_LANG doubles as the session language hint carried with
  // every utterance (the bus utt frame's lang field / EARS_UTT lang=).
  const SESSION_LANG = GREET_LANG;
  // v4 bus transport config: all three must be set to engage the bus.
  const BUS_URL = (process.env.BUS_URL || '').trim();
  const BUS_TOKEN = (process.env.BUS_TOKEN || '').trim();
  const BUS_SESSION = (process.env.BUS_SESSION || '').trim();
  const BUS_CONFIGURED = Boolean(BUS_URL && BUS_TOKEN && BUS_SESSION);
  let runMinutes = parseFloat(process.env.RUN_MINUTES || '45');
  if (!Number.isFinite(runMinutes) || runMinutes <= 0) runMinutes = 45;
  if (runMinutes > 45) runMinutes = 45; // hard cap

  fs.mkdirSync(RESULTS_DIR, { recursive: true });
  const utterancesPath = path.join(RESULTS_DIR, 'utterances.jsonl');

  const { Client, GatewayIntentBits, Events } = require('discord.js');
  const voice = require('@discordjs/voice');
  const {
    joinVoiceChannel, createAudioPlayer, createAudioResource, StreamType,
    VoiceConnectionStatus, EndBehaviorType, entersState, NoSubscriberBehavior,
  } = voice;
  const OpusScript = require('opusscript');
  try { await require('libsodium-wrappers').ready; } catch (_) {}

  const state = {
    startedAt: new Date().toISOString(),
    ready: false,
    connectionStates: new Set(),
    tonePlayed: false,
    utteranceCount: 0,
    perUser: {},
    maxRms: 0,
    relaySent: 0,
    repliesPlayed: 0,
    greetSent: 0,
    fillersPlayed: 0,
    barges: 0,
    busUsed: false,
    pcmFramesSent: 0,
  };

  let connection = null;
  let currentChannelId = null;
  let currentGuildId = null;
  let tonePlayedForConnection = false;
  // v3 empty-channel tracking: non-bot users in the bot's current channel,
  // and when that set last became empty (null while anyone is present).
  let humanMembers = new Set();
  let emptySince = null;
  // v3: the brain greets at most once per join.
  let greetedForConnection = false;
  let finishing = false;
  const activeSubscriptions = new Set();

  const intents = [GatewayIntentBits.Guilds, GatewayIntentBits.GuildVoiceStates];
  if (VOICE_LOOP) {
    // v2 relay: post utterances to / read replies from the relay channel.
    // MessageContent is a privileged intent — only requested when the
    // loop is on, so a v1-style run needs no extra portal toggles.
    intents.push(GatewayIntentBits.GuildMessages, GatewayIntentBits.MessageContent);
  }
  const client = new Client({ intents });

  function daveStatus(conn) {
    // Best-effort: @discordjs/voice exposes DAVE session info differently
    // across patch versions; log whatever is reachable, never crash on it.
    try {
      const s = conn.state || {};
      const cand = s.daveSession || s.dave || conn.daveSession || conn.joinConfig && conn.joinConfig.dave;
      if (cand) return `DAVE present (${typeof cand === 'object' ? Object.keys(cand).join(',') : String(cand)})`;
      if (typeof s.status === 'string') return `DAVE not exposed by API (state.status=${s.status})`;
    } catch (_) {}
    return 'DAVE not exposed by API';
  }

  async function playTone(conn) {
    if (tonePlayedForConnection) return;
    tonePlayedForConnection = true;
    const player = createAudioPlayer({ behaviors: { noSubscriber: NoSubscriberBehavior.Play } });
    const pcm = generateTonePcm(1);
    const stream = Readable.from([pcm]);
    const resource = createAudioResource(stream, { inputType: StreamType.Raw });
    conn.subscribe(player);
    await new Promise((resolve) => {
      const done = () => resolve();
      player.once('idle', done);
      player.once('error', (e) => { log('TONE ERROR', e.message); done(); });
      setTimeout(done, 4000); // never hang on playback
      player.play(resource);
    });
    state.tonePlayed = true;
    log('TONE PLAYED');
  }

  // ------------------------------------------------------- v2 voice loop
  let relayChannel = null;
  let busClient = null;
  const nameCache = new Map();
  // Playback items: { pcm, seq, kind, countReply }. seq is the bus
  // utterance seq the audio answers (0 for Discord-mode replies and
  // fillers, which carry no seq). countReply marks the item whose
  // natural completion closes a reply: the last bus chunk of a seq, or
  // any Discord-mode reply.
  const replyQueue = [];
  // Non-null while any reply/filler audio is sounding:
  // { player, seq, barged }.
  let currentPlayback = null;
  // userId -> timestamp of the last decoded speech packet; drives the
  // "still speaking 450 ms later" half of the barge-in check.
  const lastSpeechAt = new Map();

  async function getRelayChannel() {
    if (relayChannel) return relayChannel;
    const ch = await client.channels.fetch(RELAY_CHANNEL_ID);
    if (!ch || typeof ch.send !== 'function') {
      throw new Error(`relay channel ${RELAY_CHANNEL_ID} is not sendable`);
    }
    relayChannel = ch;
    return ch;
  }

  async function getSpeakerName(userId) {
    if (nameCache.has(userId)) return nameCache.get(userId);
    let raw = '';
    try {
      const u = await client.users.fetch(userId);
      raw = (u && u.username) || '';
    } catch (_) {}
    const clean = raw.replace(/[^A-Za-z0-9_.-]/g, '').slice(0, 32) || 'unknown';
    nameCache.set(userId, clean);
    return clean;
  }

  // Post one finished utterance to the brain: over the bus when it is
  // live, otherwise to the relay channel as a mono WAV. Thresholds are
  // re-checked here; anything failing them stays log-only. Every
  // relayed utterance takes the next per-session seq (starting at 1),
  // whichever transport carries it; the seq is what filler / reply /
  // skip / barge frames refer back to.
  let uttSeq = 0;
  async function maybeRelay(userId, stereoPcm, durationSec, rms) {
    if (durationSec < 0.4 || rms < 150) return;
    const seq = ++uttSeq;
    const wav = encodeWavPcm16Mono(downmixStereoToMono(stereoPcm), SAMPLE_RATE);
    const name = await getSpeakerName(userId);
    if (busClient && busClient.usable()) {
      try {
        busClient.sendUtt(
          { seq, user: userId, name, guild: currentGuildId, lang: SESSION_LANG, dur: Number(durationSec.toFixed(2)) },
          wav
        );
        state.relaySent++;
        log(`BUS UTT SENT seq=${seq} user=${userId} dur=${durationSec.toFixed(2)} bytes=${wav.length}`);
        return;
      } catch (e) {
        // Never lose an utterance because the bus died mid-send:
        // fall through and re-send the same audio over Discord.
        log(`BUS UTT SEND FAIL seq=${seq} (${e.message}); re-sending over Discord`);
      }
    }
    const channel = await getRelayChannel();
    await channel.send({
      content: `EARS_UTT user=${userId} name=${name} guild=${currentGuildId} lang=${SESSION_LANG}`,
      files: [{ attachment: wav, name: `utt-${userId}-${Date.now()}.wav` }],
    });
    state.relaySent++;
    log(`RELAY SENT user=${userId} dur=${durationSec.toFixed(2)} bytes=${wav.length}`);
  }

  // Play one item now; resolves true only if it played to its natural
  // end (false on error, timeout, missing connection or barge-in).
  function startPlayback(item) {
    // StreamType.Raw expects 48 kHz STEREO s16le (the same format the
    // tone path feeds it), so duplicate each mono sample into L+R.
    const monoPcm = item.pcm;
    const frames = Math.floor(monoPcm.length / BYTES_PER_FRAME_SAMPLE);
    const stereo = Buffer.alloc(frames * CHANNELS * BYTES_PER_FRAME_SAMPLE);
    for (let i = 0; i < frames; i++) {
      const v = monoPcm.readInt16LE(i * BYTES_PER_FRAME_SAMPLE);
      stereo.writeInt16LE(v, i * CHANNELS * BYTES_PER_FRAME_SAMPLE);
      stereo.writeInt16LE(v, (i * CHANNELS + 1) * BYTES_PER_FRAME_SAMPLE);
    }
    return new Promise((resolve) => {
      if (!connection) { resolve(false); return; }
      let rec = null;
      let timer = null;
      let finished = false;
      const done = (natural) => {
        if (finished) return;
        finished = true;
        if (timer) clearTimeout(timer);
        if (rec && currentPlayback === rec) currentPlayback = null;
        resolve(natural && !(rec && rec.barged));
      };
      try {
        const player = createAudioPlayer({ behaviors: { noSubscriber: NoSubscriberBehavior.Play } });
        const resource = createAudioResource(Readable.from([stereo]), { inputType: StreamType.Raw });
        connection.subscribe(player);
        rec = { player, seq: item.seq, barged: false };
        currentPlayback = rec;
        const timeoutMs = Math.floor(stereo.length / (SAMPLE_RATE * CHANNELS * BYTES_PER_FRAME_SAMPLE) * 1000) + 5000;
        timer = setTimeout(() => done(true), timeoutMs); // never hang the queue
        player.once('idle', () => done(true));
        player.once('error', (e) => { log('PLAY FAIL:', e.message); done(false); });
        if (item.kind === 'filler') state.fillersPlayed++;
        player.play(resource);
      } catch (e) {
        log('PLAY FAIL:', e.message);
        done(false);
      }
    });
  }

  // Serialize reply playback: one item at a time, in arrival order.
  // Bus replies arrive as ordered chunks over one socket and Discord
  // replies as whole messages; both feed this same FIFO, so a later
  // seq's chunks can never overtake an earlier unfinished seq.
  function pumpReplyQueue() {
    if (currentPlayback) return;
    const item = replyQueue.shift();
    if (!item) return;
    startPlayback(item)
      .then((natural) => {
        if (item.kind === 'reply') {
          if (item.countReply && natural) state.repliesPlayed++;
          log(`REPLY PLAYED seq=${item.seq} bytes=${item.pcm.length}`);
        }
      })
      .catch((e) => log('REPLY PLAY FAIL:', e.message))
      .finally(() => { pumpReplyQueue(); });
  }

  // A filler is a short bridge clip from the brain (either transport).
  // It plays only when the way is completely clear — player idle AND
  // queue empty — and is dropped silently otherwise: a late filler is
  // worse than none.
  function playFiller(item) {
    if (currentPlayback || replyQueue.length > 0) return;
    startPlayback(item)
      .catch((e) => log('FILLER PLAY FAIL:', e.message))
      .finally(() => { pumpReplyQueue(); });
  }

  // Barge-in: while audio is playing, a human who starts speaking and
  // is still speaking 450 ms later cuts the bot off — playback stops,
  // the queue is cleared, and the bus (if live) is told which seq was
  // interrupted. Speech that ends inside the 450 ms window never
  // barges: the check re-reads the last decoded-packet time, which only
  // advances while the user is actually talking. The bot never hears
  // itself, so no echo guard is needed.
  const bargeTimers = new Map();
  function maybeScheduleBarge(userId) {
    if (!currentPlayback) return;
    if (bargeTimers.has(userId)) return;
    const timer = setTimeout(() => {
      bargeTimers.delete(userId);
      if (!currentPlayback) return;
      const last = lastSpeechAt.get(userId) || 0;
      if (Date.now() - last > 450) return; // already stopped talking
      const seqPlaying = currentPlayback.seq;
      currentPlayback.barged = true;
      try { currentPlayback.player.stop(); } catch (_) {}
      replyQueue.length = 0;
      state.barges++;
      log(`BARGE-IN user=${userId}`);
      if (busClient && busClient.usable()) {
        try { busClient.sendBarge(seqPlaying); } catch (e) { log('BUS BARGE SEND FAIL:', e.message); }
      }
    }, 450);
    bargeTimers.set(userId, timer);
  }

  // LIVE mode uplink for one user segment: convert each decoded 48 kHz
  // stereo packet to 16 kHz mono and ship it in ~100 ms frames while
  // the bus is live. `live` is the per-segment state created in
  // subscribeToUser ({ buf, carry, streamed, dead, name }): buf holds
  // decimated bytes not yet filling a frame, carry the sub-group
  // sample remainder. Throws propagate to the caller, which marks the
  // segment dead and lets it finish as a normal utterance.
  function pushLivePcm(userId, live, stereoPcm) {
    const mono = downmixStereoToMono(stereoPcm);
    const { out, rest } = decimate48kTo16k(mono, live.carry);
    live.carry = rest;
    if (out.length) live.buf = live.buf.length ? Buffer.concat([live.buf, out]) : out;
    while (live.buf.length >= LIVE_FRAME_BYTES) {
      const frame = live.buf.subarray(0, LIVE_FRAME_BYTES);
      live.buf = live.buf.subarray(LIVE_FRAME_BYTES);
      busClient.sendPcm(userId, live.name || 'unknown', frame);
      live.streamed = true;
      state.pcmFramesSent++;
    }
  }

  function subscribeToUser(userId) {
    if (!connection || userId === client.user.id) return;
    if (activeSubscriptions.has(userId)) return;
    activeSubscriptions.add(userId);
    let opusStream;
    try {
      opusStream = connection.receiver.subscribe(userId, {
        end: { behavior: EndBehaviorType.AfterSilence, duration: 650 },
      });
    } catch (e) {
      activeSubscriptions.delete(userId);
      log(`SUBSCRIBE FAIL user=${userId}:`, e.message);
      return;
    }
    // v4 force-flush: one continuous 20 s stream is cut so the normal
    // end/close path fires with the audio collected so far — a single
    // monologue must not stall the whole room behind it. The usual
    // >=0.4 s / RMS gates still apply to whatever was collected.
    const flushTimer = setTimeout(() => {
      log(`UTTERANCE FLUSH user=${userId} at 20s`);
      try { opusStream.destroy(); } catch (_) {}
    }, 20_000);
    const decoder = new OpusScript(SAMPLE_RATE, CHANNELS, OpusScript.Application.AUDIO);
    let pcmBytes = 0;
    let sumSq = 0;
    let peak = 0;
    let sampleCount = 0;
    let utteranceFinished = false;
    const pcmChunks = []; // decoded PCM kept only when VOICE_LOOP may relay it
    // LIVE mode per-segment streaming state (see pushLivePcm). The
    // display name is resolved once up front; frames sent before it
    // lands carry 'unknown' and later frames the real name.
    const live = { buf: Buffer.alloc(0), carry: Buffer.alloc(0), streamed: false, dead: false, name: null };
    if (LIVE_MODE) {
      getSpeakerName(userId).then((n) => { live.name = n; }).catch(() => {});
    }
    opusStream.on('data', (packet) => {
      try {
        const out = decoder.decode(packet);
        if (!out || !out.length) return;
        lastSpeechAt.set(userId, Date.now());
        pcmBytes += out.length;
        if (VOICE_LOOP) pcmChunks.push(Buffer.from(out));
        if (LIVE_MODE && !live.dead && busClient && busClient.usable()) {
          try {
            pushLivePcm(userId, live, out);
          } catch (e) {
            // Stop streaming this segment; it finishes as a normal
            // v4 utterance instead, so the speech is never lost.
            live.dead = true;
            log(`LIVE PCM SEND FAIL user=${userId}: ${e.message}`);
          }
        }
        for (let i = 0; i + 1 < out.length; i += 2) {
          const v = out.readInt16LE(i);
          sumSq += v * v;
          const a = Math.abs(v);
          if (a > peak) peak = a;
          sampleCount++;
        }
      } catch (_) {
        // A corrupt/empty packet at stream edges is expected; ignore it.
      }
    });
    const finishUtterance = () => {
      // 'end', 'close' and 'error' can race for the same utterance; only
      // the first invocation counts (v1 could double-log when both the
      // end and close events fired; v2 must never double-relay).
      if (utteranceFinished) return;
      utteranceFinished = true;
      clearTimeout(flushTimer);
      try { decoder.delete(); } catch (_) {}
      activeSubscriptions.delete(userId);
      if (pcmBytes === 0 || sampleCount === 0) return;
      const durationSec = pcmBytes / (SAMPLE_RATE * CHANNELS * BYTES_PER_FRAME_SAMPLE);
      const rms = Math.sqrt(sumSq / sampleCount);
      const row = {
        ts: new Date().toISOString(),
        user_id: userId,
        duration_sec: Number(durationSec.toFixed(3)),
        pcm_bytes: pcmBytes,
        rms: Number(rms.toFixed(1)),
        peak,
      };
      try { fs.appendFileSync(utterancesPath, JSON.stringify(row) + '\n'); } catch (e) {
        log('UTTERANCE WRITE FAIL:', e.message);
      }
      state.utteranceCount++;
      state.perUser[userId] = (state.perUser[userId] || 0) + 1;
      if (rms > state.maxRms) state.maxRms = rms;
      log(`UTTERANCE user=${userId} dur=${row.duration_sec} rms=${row.rms} peak=${peak}`);
      if (VOICE_LOOP && pcmChunks.length) {
        const fullPcm = Buffer.concat(pcmChunks);
        if (LIVE_MODE && live.streamed) {
          // LIVE mode carried this segment: flush the sub-frame tail
          // as one last pcm frame and close the segment with pcm_end —
          // no EARS_UTT / WAV relay. If the bus died at the last
          // moment, fall back to the whole-utterance relay so the
          // speech still reaches the brain exactly once.
          (async () => {
            try {
              if (!busClient || !busClient.usable()) throw new Error('bus down at segment end');
              if (live.buf.length > 0) {
                busClient.sendPcm(userId, live.name || (await getSpeakerName(userId)), live.buf);
                state.pcmFramesSent++;
                live.buf = Buffer.alloc(0);
              }
              busClient.sendJson({ t: 'pcm_end', user: userId });
              log(`LIVE SEGMENT END user=${userId} dur=${row.duration_sec}`);
            } catch (e) {
              log(`LIVE END FALLBACK user=${userId}: ${e.message}; relaying utterance instead`);
              await maybeRelay(userId, fullPcm, row.duration_sec, rms);
            }
          })().catch((e) => log('RELAY SEND FAIL:', e.message));
        } else {
          // Fire-and-forget: relaying must never block the receive loop.
          maybeRelay(userId, fullPcm, row.duration_sec, rms)
            .catch((e) => log('RELAY SEND FAIL:', e.message));
        }
      }
    };
    opusStream.once('end', finishUtterance);
    opusStream.once('close', finishUtterance);
    opusStream.once('error', (e) => { log(`RECEIVE ERROR user=${userId}:`, e.message); finishUtterance(); });
  }

  // v3: (re)seed the human set from the guild's voice-state cache for
  // the channel being joined. The tracker handler keeps it live from
  // there; an initially-empty channel still gets the 180s grace.
  function refreshHumans(guild, channelId) {
    const found = new Set();
    try {
      for (const vs of guild.voiceStates.cache.values()) {
        if (vs.channelId !== channelId) continue;
        const u = vs.member && vs.member.user;
        if (u && u.bot) continue; // bots (incl. the bot itself) never count
        found.add(vs.id);
      }
    } catch (_) {}
    humanMembers = found;
    emptySince = found.size === 0 ? Date.now() : null;
  }

  async function joinChannel(guild, channelId, phase) {
    if (connection && currentChannelId === channelId) return;
    if (connection) {
      try { connection.destroy(); } catch (_) {}
      connection = null;
    }
    tonePlayedForConnection = false;
    greetedForConnection = false;
    connection = joinVoiceChannel({
      channelId,
      guildId: guild.id,
      adapterCreator: guild.voiceAdapterCreator,
      selfDeaf: false,
      selfMute: false,
    });
    currentChannelId = channelId;
    currentGuildId = guild.id;
    connection.on('stateChange', (oldS, newS) => {
      state.connectionStates.add(newS.status);
      log(`VC STATE ${oldS.status} -> ${newS.status} (${daveStatus(connection)})`);
    });
    connection.on('error', (e) => log('VC ERROR:', e.message));
    // Receive path: subscribe per speaking user.
    connection.receiver.speaking.on('start', (userId) => {
      if (userId === client.user.id) return;
      subscribeToUser(userId);
      maybeScheduleBarge(userId);
    });
    log(`JOINING channel=${channelId} guild=${guild.id} phase=${phase}`);
    try {
      await entersState(connection, VoiceConnectionStatus.Ready, 30_000);
      log(`VC READY channel=${channelId} (${daveStatus(connection)})`);
    } catch (e) {
      log('VC READY TIMEOUT:', e.message);
    }
    await playTone(connection);
    // v3: start the empty-channel clock from whoever is already here.
    refreshHumans(guild, channelId);
    // v3: ask the brain to greet the room, once per join. Fire-and-
    // forget so a slow relay post can never stall the receiver.
    if (VOICE_LOOP && !greetedForConnection) {
      greetedForConnection = true;
      (async () => {
        // Bus first when it is live; the Discord greet is the fallback
        // (bus down, not configured, or the send itself threw).
        if (busClient && busClient.usable()) {
          try {
            busClient.sendGreet(guild.id, SESSION_LANG);
            state.greetSent++;
            log(`GREET SENT (bus) lang=${SESSION_LANG}`);
            return;
          } catch (e) {
            log('GREET BUS SEND FAIL:', e.message);
          }
        }
        try {
          const channel = await getRelayChannel();
          await channel.send({ content: `EARS_GREET guild=${guild.id} lang=${GREET_LANG}` });
          state.greetSent++;
          log(`GREET SENT lang=${GREET_LANG}`);
        } catch (e) {
          log('GREET SEND FAIL:', e.message);
        }
      })();
    }
  }

  async function gitPushResults() {
    // Best-effort: commit + push results/ back to the repo (Codespaces has
    // git credentials for the repo it was created from). Never fatal.
    const cmd = 'git config user.email "ears@kornol.local"; git config user.name "KORNOL ears"; ' +
      'git add results && (git diff --cached --quiet || git commit -m "ears test results") && git push origin HEAD:main';
    return new Promise((resolve) => {
      exec(cmd, { cwd: BASE, timeout: 60_000 }, (err, stdout, stderr) => {
        if (err) log('GIT PUSH FAILED (best-effort):', (stderr || err.message || '').toString().trim().slice(0, 300));
        else log('GIT PUSH OK');
        resolve();
      });
    });
  }

  let botMoved = false;

  async function finish(reason) {
    if (finishing) return;
    finishing = true;
    log(`FINISHING reason=${reason}`);
    const summary = {
      started_at: state.startedAt,
      finished_at: new Date().toISOString(),
      ready: state.ready,
      connection_states_seen: [...state.connectionStates],
      tone_played: state.tonePlayed,
      utterance_count: state.utteranceCount,
      per_user_utterances: state.perUser,
      max_rms: Number(state.maxRms.toFixed(1)),
      relay_sent: state.relaySent,
      replies_played: state.repliesPlayed,
      greet_sent: state.greetSent,
      fillers_played: state.fillersPlayed,
      barges: state.barges,
      bus_used: state.busUsed,
      bot_moved: botMoved,
      live_mode: LIVE_MODE,
      pcm_frames_sent: state.pcmFramesSent,
      finish_reason: reason,
    };
    try {
      fs.writeFileSync(path.join(RESULTS_DIR, 'summary.json'), JSON.stringify(summary, null, 2) + '\n');
      log('SUMMARY WRITTEN', JSON.stringify(summary));
    } catch (e) {
      log('SUMMARY WRITE FAIL:', e.message);
    }
    try { if (connection) connection.destroy(); } catch (_) {}
    await gitPushResults();
    try { client.destroy(); } catch (_) {}
    process.exit(0);
  }

  process.once('SIGTERM', () => finish('SIGTERM'));
  process.once('SIGINT', () => finish('SIGINT'));
  setTimeout(() => finish(`runtime cap ${runMinutes}m`), runMinutes * 60 * 1000);

  // Heartbeat: a log line every 30s also keeps the codespace idle timer reset.
  const startedMs = Date.now();
  setInterval(() => {
    log(`HEARTBEAT uptime=${Math.floor((Date.now() - startedMs) / 1000)}s utterances=${state.utteranceCount} connected=${Boolean(connection)}`);
    // v3 standing rule: she leaves when the room has been empty for 180s.
    if (connection && emptySince !== null && Date.now() - emptySince >= 180_000) {
      log('AUTO-LEAVE channel empty for 180s');
      finish('channel empty');
    }
    // v4.2 poll: the bot's own cached voice state is ground truth.
    if (connection && currentChannelId && client.user) {
      try {
        const g = client.guilds.cache.get(currentGuildId);
        const vs = g && g.voiceStates.cache.get(client.user.id);
        if (vs && vs.channelId && vs.channelId !== currentChannelId) {
          botMoved = true;
          log(`BOT MOVED (poll) joined=${currentChannelId} now=${vs.channelId}`);
          finish('bot moved');
        }
      } catch (_) {}
    }
  }, 30_000);

  // Progress phone-home: every 45s, force-add the live log + utterance
  // records and push, so progress is visible from outside the codespace
  // while the run is still going. Best-effort, never fatal, no overlap.
  let progressInFlight = false;
  setInterval(() => {
    if (progressInFlight || finishing) return;
    progressInFlight = true;
    const cmd = 'git config user.email "ears@kornol.local"; git config user.name "KORNOL ears"; ' +
      'git add -f results/run.log results/utterances.jsonl results/summary.json 2>/dev/null; ' +
      '(git diff --cached --quiet || git commit -m "ears progress") && git push origin HEAD:main';
    exec(cmd, { cwd: BASE, timeout: 45_000 }, (err) => {
      if (err) log('PROGRESS PUSH FAILED (best-effort)');
      progressInFlight = false;
    });
  }, 45_000);

  client.once(Events.ClientReady, async (c) => {
    state.ready = true;
    log(`EARS READY as ${c.user.tag}`);
    // Phase A: join the fixed test channel immediately, if configured.
    if (TEST_VOICE_CHANNEL_ID) {
      try {
        const channel = await client.channels.fetch(TEST_VOICE_CHANNEL_ID);
        if (channel && channel.guild) {
          await joinChannel(channel.guild, channel.id, 'A');
        } else {
          log(`PHASE A FAIL: channel ${TEST_VOICE_CHANNEL_ID} not found or not in a guild`);
        }
      } catch (e) {
        log('PHASE A ERROR:', e.message);
      }
    } else {
      log('PHASE A SKIPPED: TEST_VOICE_CHANNEL_ID not set (waiting for owner, Phase B)');
    }
  });

  // Phase B: follow the owner into a voice channel in the test guild.
  client.on(Events.VoiceStateUpdate, async (oldS, newS) => {
    try {
      if (!OWNER_USER_ID) return;
      if (newS.member && newS.member.user && newS.member.user.id !== OWNER_USER_ID) return;
      if (!newS.channelId) return; // owner left voice
      if (TEST_GUILD_ID && newS.guild.id !== TEST_GUILD_ID) return;
      if (currentChannelId === newS.channelId) return;
      await joinChannel(newS.guild, newS.channelId, 'B');
    } catch (e) {
      log('PHASE B ERROR:', e.message);
    }
  });

  // v3: keep the human set for the bot's channel current. Only joins,
  // leaves and moves touching currentChannelId matter; bots (including
  // the bot itself) never count as company.
  client.on(Events.VoiceStateUpdate, (oldS, newS) => {
    try {
      if (!currentChannelId) return;
      const userId = newS.id;
      if (client.user && userId === client.user.id) {
        // v4.2: she was moved (or disconnected) by someone else. The
        // human tracker below keys on currentChannelId, so a move left
        // her watching a room she was no longer in (live incident
        // 2026-11-12: dragged to another channel, sat alone until the
        // runtime cap). End the session instead.
        if (newS.channelId !== currentChannelId) {
          botMoved = true;
          log(`BOT MOVED joined=${currentChannelId} now=${newS.channelId}`);
          finish('bot moved');
        }
        return;
      }
      const user = (newS.member && newS.member.user) || (oldS.member && oldS.member.user);
      if (user && user.bot) return;
      const inChannel = newS.channelId === currentChannelId;
      const wasInChannel = oldS.channelId === currentChannelId;
      if (inChannel && !wasInChannel) {
        humanMembers.add(userId);
        emptySince = null;
      } else if (!inChannel && wasInChannel) {
        humanMembers.delete(userId);
        if (humanMembers.size === 0 && emptySince === null) {
          emptySince = Date.now();
        }
      }
    } catch (_) {}
  });

  // v2 relay replies: an EARS_REPLY message in the relay channel with a
  // .wav attachment is the brain's answer — play it into the voice
  // channel. NOTE: replies are posted by a separate process using the
  // SAME bot account, so there is deliberately NO author filter here;
  // the guards are the channel id, the EARS_REPLY prefix, and "newer
  // than this process start". The bot's own EARS_UTT uploads never
  // match the prefix, so they cannot trigger playback.
  client.on(Events.MessageCreate, async (msg) => {
    try {
      if (!VOICE_LOOP) return;
      if (!msg || msg.channelId !== RELAY_CHANNEL_ID) return;
      if (typeof msg.content !== 'string') return;
      const isReply = msg.content.startsWith('EARS_REPLY');
      const isFiller = !isReply && msg.content.startsWith('EARS_FILLER');
      if (!isReply && !isFiller) return;
      const kindLabel = isFiller ? 'FILLER' : 'REPLY';
      if (msg.createdTimestamp < startedMs) return; // never replay history
      const att = msg.attachments
        ? msg.attachments.find((a) => a.name && a.name.toLowerCase().endsWith('.wav'))
        : null;
      if (!att) return;
      const res = await fetch(att.url);
      if (!res.ok) { log(`${kindLabel} PLAY FAIL:`, `attachment fetch HTTP ${res.status}`); return; }
      const wavBuf = Buffer.from(await res.arrayBuffer());
      const { pcm } = decodeWavToPcm48k(wavBuf);
      if (!pcm.length) { log(`${kindLabel} PLAY FAIL:`, 'empty PCM after WAV decode'); return; }
      if (isFiller) {
        // Discord-mode fillers carry no seq; playFiller drops this
        // silently unless the player is idle and the queue is empty.
        playFiller({ pcm, seq: 0, kind: 'filler', countReply: false });
      } else {
        replyQueue.push({ pcm, seq: 0, kind: 'reply', countReply: true });
        pumpReplyQueue();
      }
    } catch (e) {
      log('REPLY PLAY FAIL:', e.message);
    }
  });

  // v4 bus: engage only in loop mode with the full config present. The
  // Discord handlers above stay armed regardless — they are both the
  // no-bus transport and the fallback if the bus drops mid-session.
  if (VOICE_LOOP && BUS_CONFIGURED) {
    busClient = createBusClient({
      url: BUS_URL,
      session: BUS_SESSION,
      token: BUS_TOKEN,
      onReply: (frame, wavBuf) => {
        try {
          const { pcm } = decodeWavToPcm48k(wavBuf);
          if (!pcm.length) { log(`BUS REPLY EMPTY seq=${frame.seq} chunk=${frame.chunk}`); return; }
          log(`BUS REPLY seq=${frame.seq} chunk=${frame.chunk} last=${Boolean(frame.last)} heard=${JSON.stringify(String(frame.heard || '').slice(0, 80))}`);
          replyQueue.push({ pcm, seq: frame.seq || 0, kind: 'reply', countReply: Boolean(frame.last) });
          pumpReplyQueue();
        } catch (e) {
          log('BUS REPLY FAIL:', e.message);
        }
      },
      onFiller: (frame, wavBuf) => {
        try {
          const { pcm } = decodeWavToPcm48k(wavBuf);
          if (!pcm.length) return;
          playFiller({ pcm, seq: frame.seq || 0, kind: 'filler', countReply: false });
        } catch (e) {
          log('BUS FILLER FAIL:', e.message);
        }
      },
      onSkip: () => {},
      onDown: () => {
        // Fired at most once by the client (connect failure or a
        // mid-session drop). From here on everything — utterances,
        // greetings — rides the Discord relay channel instead.
        log('BUS FALLBACK: bus unusable; Discord relay transport for the rest of the session');
      },
    });
    busClient.connect()
      .then(() => { state.busUsed = true; })
      .catch(() => {}); // the failure itself is reported via onDown
  }

  await client.login(token);
}

// Dry mode triggers: explicit --dry flag, OR being require()d rather than
// run directly (e.g. the smoke test `node -e "require('./index.js')"`),
// which must never open a network connection.
if (process.argv.includes('--dry') || require.main !== module) {
  dryRun();
} else {
  main().catch((e) => {
    console.error('FATAL:', e && e.message ? e.message : e);
    process.exit(1);
  });
}
