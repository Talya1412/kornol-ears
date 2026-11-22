'use strict';

const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const fs = require('node:fs');
const { createRequire } = require('node:module');
const path = require('node:path');
const { PassThrough } = require('node:stream');
const test = require('node:test');
const vm = require('node:vm');

const indexPath = path.resolve(__dirname, '../index.js');
const indexSource = fs.readFileSync(indexPath, 'utf8');
const flushEvents = () => new Promise((resolve) => setImmediate(resolve));

function monoWav() {
  const wav = Buffer.alloc(46);
  wav.write('RIFF');
  wav.writeUInt32LE(38, 4);
  wav.write('WAVEfmt ', 8);
  wav.writeUInt32LE(16, 16);
  wav.writeUInt16LE(1, 20);
  wav.writeUInt16LE(1, 22);
  wav.writeUInt32LE(48_000, 24);
  wav.writeUInt32LE(96_000, 28);
  wav.writeUInt16LE(2, 32);
  wav.writeUInt16LE(16, 34);
  wav.write('data', 36);
  wav.writeUInt32LE(2, 40);
  wav.writeInt16LE(1_000, 44);
  return wav;
}

async function startSession({ env = {}, ready = async () => {}, audio, fixedChannel = true } = {}) {
  const client = new EventEmitter();
  client.user = { id: 'bot', tag: 'test-bot' };
  client.login = async () => {};
  client.destroy = () => {};
  const guild = { id: 'guild', voiceStates: { cache: new Map() } };
  const channel = { id: 'fixed', guild, isVoiceBased: () => fixedChannel };
  const relayed = [];
  client.channels = { fetch: async (id) => id === channel.id ? channel : {
    send: async (message) => { relayed.push(message); },
  } };
  const joins = [];
  const players = [];
  const subscriptions = [];
  let socket;
  class FakeWebSocket extends EventEmitter {
    static OPEN = 1;
    readyState = FakeWebSocket.OPEN;
    constructor() {
      super();
      socket = this;
      setImmediate(() => this.emit('open'));
    }
    send() {}
    close() { this.emit('close'); }
  }
  class FakeOpus {
    static Application = { AUDIO: 'audio' };
    delete() {}
  }
  const voice = {
    joinVoiceChannel({ channelId }) {
      const connection = new EventEmitter();
      connection.state = { status: 'ready' };
      connection.receiver = {
        speaking: new EventEmitter(),
        subscribe(userId) {
          const stream = new PassThrough();
          subscriptions.push({ userId, stream });
          return stream;
        },
      };
      connection.destroy = () => { connection.state.status = 'destroyed'; };
      connection.subscribe = () => ({ unsubscribe() {} });
      joins.push({ channelId, connection });
      return connection;
    },
    createAudioPlayer() {
      const player = new EventEmitter();
      player.play = () => {
        players.push(player);
        if (audio) audio(player, players.length);
        else { player.emit('playing'); player.emit('idle'); }
      };
      player.stop = () => { player.stopped = true; player.emit('idle'); };
      return player;
    },
    createAudioResource: () => ({}),
    entersState: ready,
    VoiceConnectionStatus: { Ready: 'ready' },
    EndBehaviorType: { AfterSilence: 'after-silence' },
    StreamType: { Raw: 'raw' },
    NoSubscriberBehavior: { Play: 'play' },
  };
  const fakeProcess = new EventEmitter();
  fakeProcess.argv = [];
  fakeProcess.env = {
    DISCORD_BOT_TOKEN: 'test-token', TEST_GUILD_ID: 'guild', OWNER_USER_ID: 'owner',
    TEST_VOICE_CHANNEL_ID: 'fixed', RELAY_CHANNEL_ID: 'relay', VOICE_LOOP: '1', ...env,
  };
  fakeProcess.exit = (code) => assert.equal(code, 0);
  const entry = {};
  const logs = [];
  let nextTimer = 0;
  const timers = new Map();
  let downloads = 0;
  const localRequire = createRequire(indexPath);
  const fakeRequire = (name) => {
    if (name === 'fs') return { mkdirSync() {}, writeFileSync() {} };
    if (name === 'child_process') return { exec() { throw new Error('Unexpected Git operation'); } };
    if (name === 'discord.js') return {
      Client: function () { return client; }, GatewayIntentBits: {},
      Events: { ClientReady: 'ready', VoiceStateUpdate: 'voice', MessageCreate: 'message' },
    };
    if (name === '@discordjs/voice') return voice;
    if (name === 'opusscript') return FakeOpus;
    if (name === 'libsodium-wrappers') return {};
    if (name === 'ws') return FakeWebSocket;
    return localRequire(name);
  };
  fakeRequire.main = entry;
  vm.runInNewContext(indexSource, {
    __dirname: path.dirname(indexPath), require: fakeRequire, module: entry, process: fakeProcess, Buffer,
    console: { log: (...message) => logs.push(message.join(' ')), error: (...message) => logs.push(message.join(' ')) },
    fetch: async () => {
      downloads++;
      return { ok: true, arrayBuffer: async () => monoWav() };
    },
    setTimeout(callback, ms) { const id = ++nextTimer; timers.set(id, { callback, ms }); return id; },
    clearTimeout(id) { timers.delete(id); },
    setInterval() { return { unref() {} }; }, clearInterval() {},
  }, { filename: indexPath });
  await flushEvents();
  return {
    client, guild, joins, logs, players, relayed, socket, subscriptions, downloads: () => downloads,
    expire(ms) {
      for (const [id, timer] of timers) {
        if (timer.ms !== ms) continue;
        timers.delete(id);
        timer.callback();
      }
    },
  };
}

test('owner following uses the voice-state user ID and actual channel changes', async () => {
  const { client, guild, joins } = await startSession();
  client.emit('voice', { channelId: null }, { id: 'guest', channelId: 'guest-room', guild });
  client.emit('voice', { channelId: 'owner-room' }, { id: 'owner', channelId: 'owner-room', guild });
  await flushEvents();
  assert.equal(joins.length, 0);

  client.emit('voice', { channelId: null }, { id: 'owner', channelId: 'owner-room', guild });
  await flushEvents();
  assert.deepEqual(joins.map(({ channelId }) => channelId), ['owner-room']);
});

test('an end frame closes active capture and refuses new capture during playback drain', async () => {
  const session = await startSession({ env: { BUS_URL: 'ws://unused.test', BUS_TOKEN: 'mock', BUS_SESSION: 'test' } });
  session.client.emit('ready', session.client);
  await flushEvents();
  const speaking = session.joins[0].connection.receiver.speaking;
  speaking.emit('start', 'first-user');
  assert.equal(session.subscriptions.length, 1);

  session.socket.emit('message', Buffer.from(JSON.stringify({ t: 'end' })), false);
  speaking.emit('start', 'second-user');
  assert.equal(session.subscriptions.length, 1);
  assert.equal(session.subscriptions[0].stream.destroyed, true);
});

test('fixed-channel configuration requires a voice channel', async () => {
  const { client, joins } = await startSession({ fixedChannel: false });
  client.emit('ready', client);
  await flushEvents();
  assert.equal(joins.length, 0);
});

test('a voice connection that never becomes ready cannot play or greet', async () => {
  const session = await startSession({ ready: async () => { throw new Error('voice timeout'); } });
  session.client.emit('ready', session.client);
  await flushEvents();
  assert.equal(session.players.length, 0);
  assert.equal(session.relayed.length, 0);
  assert.equal(session.joins[0].connection.state.status, 'destroyed');
});

test('an earlier join cannot start audio on a later connection', async () => {
  const resolveReady = [];
  const { client, guild, players } = await startSession({
    ready: () => new Promise((resolve) => { resolveReady.push(resolve); }),
  });
  client.emit('voice', { channelId: null }, { id: 'owner', channelId: 'first-room', guild });
  client.emit('voice', { channelId: 'first-room' }, { id: 'owner', channelId: 'second-room', guild });
  resolveReady[0]();
  await flushEvents();
  assert.equal(players.length, 0);
  resolveReady[1]();
  await flushEvents();
  assert.equal(players.length, 1);
});

test('a timed-out test tone is stopped and is not reported as played', async () => {
  const session = await startSession({ audio: () => {} });
  session.client.emit('ready', session.client);
  await flushEvents();
  session.expire(4_000);
  await flushEvents();
  assert.equal(session.players[0].stopped, true);
  assert.ok(!session.logs.some((line) => line.endsWith('TONE PLAYED')));
});

test('relay playback only downloads audio from the configured bot account', async () => {
  const session = await startSession();
  session.client.emit('ready', session.client);
  await flushEvents();
  const reply = {
    channelId: 'relay', content: 'EARS_REPLY', createdTimestamp: Date.now(),
    attachments: [{ name: 'voice.wav', url: 'https://example.test/voice.wav' }],
  };
  session.client.emit('message', { ...reply, author: { id: 'guest' } });
  await flushEvents();
  assert.equal(session.downloads(), 0);
  session.client.emit('message', { ...reply, author: session.client.user });
  await flushEvents();
  assert.equal(session.downloads(), 1);
  assert.equal(session.players.length, 2);
});

test('a stalled relay reply is stopped instead of being counted as played', async () => {
  const session = await startSession({
    audio: (player, count) => {
      player.emit('playing');
      if (count === 1) player.emit('idle');
    },
  });
  session.client.emit('ready', session.client);
  await flushEvents();
  session.client.emit('message', {
    channelId: 'relay', author: session.client.user, content: 'EARS_REPLY', createdTimestamp: Date.now(),
    attachments: [{ name: 'voice.wav', url: 'https://example.test/voice.wav' }],
  });
  await flushEvents();
  session.expire(5_000);
  await flushEvents();
  assert.equal(session.players[1].stopped, true);
  assert.ok(!session.logs.some((line) => line.includes('REPLY PLAYED')));
});
