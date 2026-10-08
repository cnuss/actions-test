'use strict';

const fs = require('fs');
const http = require('http');
const path = require('path');
const { spawn } = require('child_process');

const portFile = path.join(process.env.RUNNER_TEMP, 'state-spike.port');

if (process.argv[2] === 'serve') {
  const server = http.createServer((req, res) => {
    if (req.url === '/shutdown') { res.end('bye'); setTimeout(() => process.exit(0), 50); return; }
    res.end(`alive pid=${process.pid}`);
  });
  server.listen(0, '127.0.0.1', () => fs.writeFileSync(portFile, String(server.address().port)));
} else if (process.env.STATE_post === 'true') {
  const port = fs.readFileSync(portFile, 'utf8');
  http.get(`http://127.0.0.1:${port}/shutdown`, (res) => {
    res.resume();
    res.on('end', () => {
      setTimeout(() => {
        http.get(`http://127.0.0.1:${port}/`).on('error', () => console.log('post: server stopped')).on('response', () => {
          console.log('::error::post: server still answering after shutdown');
          process.exitCode = 1;
        });
      }, 500);
    });
  }).on('error', (err) => { console.log(`::error::post: could not reach server: ${err.message}`); process.exitCode = 1; });
} else {
  fs.appendFileSync(process.env.GITHUB_STATE, 'post=true\n');
  const child = spawn(process.execPath, [__filename, 'serve'], { detached: true, stdio: 'ignore' });
  child.unref();
  const deadline = Date.now() + 5000;
  const wait = () => {
    if (fs.existsSync(portFile)) { console.log(`main: server on ${fs.readFileSync(portFile, 'utf8')}`); return; }
    if (Date.now() > deadline) { console.log('::error::main: server did not start'); process.exitCode = 1; return; }
    setTimeout(wait, 100);
  };
  wait();
}
