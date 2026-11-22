'use strict';

const { execFile } = require('node:child_process');
const { existsSync } = require('node:fs');
const path = require('node:path');
const { promisify } = require('node:util');

const execFileAsync = promisify(execFile);
const RESULT_FILES = ['results/utterances.jsonl', 'results/summary.json'];

function createResultsPublisher(cwd, run = execFileAsync) {
  let previous = Promise.resolve();
  const git = (...args) => run('git', args, { cwd, timeout: 60_000 });

  async function publish(message) {
    const files = RESULT_FILES.filter((file) => existsSync(path.join(cwd, file)));
    if (files.length === 0) return;

    await git('add', '--', ...files);
    try {
      await git('diff', '--cached', '--quiet', '--', ...files);
    } catch (error) {
      if (error.code !== 1) throw error;
      await git(
        '-c', 'user.email=ears@kornol.local', '-c', 'user.name=KORNOL ears',
        'commit', '--only', '-m', message, '--', ...files,
      );
    }
    await git('push', 'origin', 'HEAD:main');
  }

  return (message) => {
    const current = previous.then(() => publish(message));
    previous = current.catch(() => {});
    return current;
  };
}

module.exports = { createResultsPublisher };
