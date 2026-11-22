'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');
const { createResultsPublisher } = require('../results-publisher');

test('only selected reports enter the commit and final publication waits for progress', async (t) => {
  const cwd = await fs.mkdtemp(path.join(os.tmpdir(), 'ears-test-'));
  t.after(() => fs.rm(cwd, { recursive: true, force: true }));
  await fs.mkdir(path.join(cwd, 'results'));
  await fs.writeFile(path.join(cwd, 'results', 'summary.json'), '{}');
  await fs.writeFile(path.join(cwd, 'results', 'run.log'), 'diagnostics');
  const releaseFirstPush = Promise.withResolvers();
  const firstPushStarted = Promise.withResolvers();
  const calls = [];
  const publish = createResultsPublisher(cwd, async (program, args) => {
    assert.equal(program, 'git');
    calls.push(args);
    if (args[0] === 'diff') throw Object.assign(new Error('changes staged'), { code: 1 });
    if (args[0] === 'push' && calls.length === 4) {
      firstPushStarted.resolve();
      await releaseFirstPush.promise;
    }
  });

  const progress = publish('progress');
  const final = publish('final');
  await firstPushStarted.promise;
  assert.equal(calls.length, 4);
  releaseFirstPush.resolve();
  await Promise.all([progress, final]);

  assert.deepEqual(calls.map(([cmd]) => cmd), ['add', 'diff', '-c', 'push', 'add', 'diff', '-c', 'push']);
  for (const args of calls.filter(([cmd]) => cmd !== 'push')) {
    assert.deepEqual(args.slice(args.indexOf('--') + 1), ['results/summary.json']);
  }
  assert.ok(calls.filter((args) => args.includes('commit')).every((args) => args.includes('--only')));
});
