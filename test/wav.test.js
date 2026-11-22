'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const { decodeWavToPcm48k, encodeWavPcm16Mono } = require('../wav');

test('24 kHz reply reports its resampled 48 kHz format', () => {
  const input = Buffer.alloc(4);
  input.writeInt16LE(100, 0);
  input.writeInt16LE(-200, 2);

  const output = decodeWavToPcm48k(encodeWavPcm16Mono(input, 24_000));

  assert.equal(output.sampleRate, 48_000);
  assert.deepEqual([0, 2, 4, 6].map((offset) => output.pcm.readInt16LE(offset)), [100, 100, -200, -200]);
});

test('incomplete audio is rejected instead of being played as a truncated reply', () => {
  const wav = encodeWavPcm16Mono(Buffer.alloc(8), 48_000);

  assert.throws(() => decodeWavToPcm48k(wav.subarray(0, -2)), /truncated/i);
});

test('PCM data without format information is rejected', () => {
  const wav = encodeWavPcm16Mono(Buffer.alloc(8), 48_000);
  wav.write('JUNK', 12, 'ascii');

  assert.throws(() => decodeWavToPcm48k(wav), /format chunk/i);
});
