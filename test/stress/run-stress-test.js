import { spawn, execSync } from 'node:child_process'
import { resolve, dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { existsSync, readFileSync, rmSync } from 'node:fs'
import { mkdir, rm, writeFile } from 'node:fs/promises'
import { parseArgs } from 'node:util'
import * as readline from 'node:readline'
import * as lib from '../e2e/lib.js'

const __dirname = dirname(fileURLToPath(import.meta.url))
const PROJECT_ROOT = resolve(__dirname, '..', '..')

const { values: opts } = parseArgs({
  options: {
    count: { type: 'string', default: '20000' },
    'ckl-dir': { type: 'string', default: join(__dirname, 'ckl-files') },
    'cargo-size': { type: 'string', default: '10' },
    'keep-files': { type: 'boolean', default: false },
    timeout: { type: 'string', default: '600000' },
    generate: { type: 'boolean', default: false }
  }
})

const FILE_COUNT = parseInt(opts.count, 10)
const CKL_DIR = resolve(opts['ckl-dir'])
const CARGO_SIZE = parseInt(opts['cargo-size'], 10)
const KEEP_FILES = opts['keep-files']
const TIMEOUT = parseInt(opts.timeout, 10)
const HISTORY_FILE = join(__dirname, 'stress-history.txt')

// Tracking state
let maxCargoDepth = 0
let batchCount = 0
const depthSamples = []
const startTime = Date.now()

function elapsed () {
  return ((Date.now() - startTime) / 1000).toFixed(0)
}

function getRssKb (pid) {
  try {
    const status = readFileSync(`/proc/${pid}/status`, 'utf8')
    const match = status.match(/VmRSS:\s+(\d+)\s+kB/)
    return match ? parseInt(match[1], 10) : null
  } catch {
    return null
  }
}

async function generateCkls () {
  console.log(`\n--- Generating ${FILE_COUNT} CKL files ---`)
  const genScript = join(__dirname, 'generate-ckls.js')
  execSync(
    `node ${genScript} --count ${FILE_COUNT} --output-dir ${CKL_DIR}`,
    { stdio: 'inherit' }
  )
}

async function main () {
  let db, auth, api, watcher

  try {
    // Generate CKL files if needed
    if (opts.generate || !existsSync(CKL_DIR)) {
      await generateCkls()
    } else {
      console.log(`Using existing CKL files in ${CKL_DIR}`)
    }

    // Start infrastructure
    console.log('\n--- Starting infrastructure ---')
    console.log('Starting auth server...')
    await lib.initNetwork()
    auth = await lib.startAuth()
    console.log('Auth server ready on port 8080')

    console.log('Starting database (this may take a minute)...')
    db = await lib.startDb()
    console.log('Database ready')

    console.log('Starting STIG Manager API...')
    api = await lib.startApi()
    console.log('API ready on port 54001')

    // Create collection and user
    console.log('Creating test collection and watcher user...')
    const { user, collection } = await lib.initWatcherTestCollection()
    console.log(`Collection ${collection.collectionId} created`)

    // Upload the VPN STIG
    console.log('Uploading VPN STIG...')
    await lib.uploadTestStig('VPN_STIG.xml')
    console.log('STIG uploaded')

    // Clear history file
    await writeFile(HISTORY_FILE, '', 'utf8')

    // Spawn watcher
    console.log(`\n--- Starting watcher (${FILE_COUNT} files, cargoSize=${CARGO_SIZE}) ---`)
    const args = [
      join(PROJECT_ROOT, 'index.js'),
      '--mode', 'scan',
      '--one-shot',
      '--path', CKL_DIR,
      '--api', 'http://localhost:54001/api',
      '--authority', 'http://localhost:8080',
      '--client-id', 'stigman-watcher',
      '--collection-id', String(collection.collectionId),
      '--cargo-size', String(CARGO_SIZE),
      '--cargo-delay', '2000',
      '--log-level', 'verbose',
      '--history-file', HISTORY_FILE,
      '--history-write-interval', '30000'
    ]

    const watcherProcess = spawn(process.execPath, args, {
      env: {
        WATCHER_CLIENT_SECRET: '954fd71a-dad6-47ab-8035-060268f3d396'
      }
    })

    watcher = watcherProcess

    // Status interval
    const statusInterval = setInterval(() => {
      const rss = getRssKb(watcherProcess.pid)
      const rssMb = rss ? (rss / 1024).toFixed(0) : '?'
      console.log(
        `[${elapsed()}s] batches: ${batchCount} | maxCargoDepth: ${maxCargoDepth} | rss: ${rssMb}MB`
      )
    }, 5000)

    // Timeout
    const timeoutTimer = setTimeout(() => {
      console.log(`\nTimeout reached (${TIMEOUT}ms), killing watcher`)
      watcherProcess.kill()
    }, TIMEOUT)

    // Parse log output
    const rl = readline.createInterface({
      input: watcherProcess.stdout,
      crlfDelay: Infinity
    })

    rl.on('line', line => {
      try {
        const json = JSON.parse(line)
        if (json.component === 'cargo' && typeof json.cargoDepth === 'number') {
          if (json.cargoDepth > maxCargoDepth) {
            maxCargoDepth = json.cargoDepth
          }
          depthSamples.push({
            elapsed: parseInt(elapsed(), 10),
            depth: json.cargoDepth,
            message: json.message
          })
          if (json.message === 'batch started') {
            batchCount++
          }
        }
      } catch {
        // non-JSON line, ignore
      }
    })

    // Capture stderr
    watcherProcess.stderr.on('data', data => {
      process.stderr.write(data)
    })

    // Wait for exit
    const exitCode = await new Promise((resolve) => {
      watcherProcess.on('close', code => resolve(code))
    })

    clearInterval(statusInterval)
    clearTimeout(timeoutTimer)

    // Print results
    const threshold = 2 * CARGO_SIZE
    const bounded = maxCargoDepth <= threshold + 20 // allow some concurrency overshoot
    console.log(`
=== Stress Test Results ===
Files:        ${FILE_COUNT}
Cargo size:   ${CARGO_SIZE}
Threshold:    ${threshold} (2 * cargoSize)
Duration:     ${elapsed()}s
Batches:      ${batchCount}
Max depth:    ${maxCargoDepth}${bounded ? '' : '  ← UNBOUNDED'}
Bounded:      ${bounded ? 'YES' : `NO (max depth >> 2 * cargoSize = ${threshold})`}
Exit code:    ${exitCode}
`)

    // Show depth progression (sampled)
    if (depthSamples.length > 0) {
      console.log('Depth samples (batch started):')
      const startedSamples = depthSamples.filter(s => s.message === 'batch started')
      // Show first 10, middle, and last 10
      const show = []
      if (startedSamples.length <= 25) {
        show.push(...startedSamples)
      } else {
        show.push(...startedSamples.slice(0, 10))
        show.push({ elapsed: '...', depth: '...', message: '...' })
        const mid = Math.floor(startedSamples.length / 2)
        show.push(...startedSamples.slice(mid - 2, mid + 3))
        show.push({ elapsed: '...', depth: '...', message: '...' })
        show.push(...startedSamples.slice(-10))
      }
      for (const s of show) {
        console.log(`  [${s.elapsed}s] depth: ${s.depth}`)
      }
    }
  } catch (err) {
    console.error('Stress test failed:', err)
  } finally {
    // Cleanup
    console.log('\n--- Cleaning up ---')
    if (watcher && watcher.exitCode === null) {
      watcher.kill()
    }
    const toStop = [api, auth, db].filter(Boolean)
    if (toStop.length) {
      await lib.stopProcesses(toStop)
      console.log('Infrastructure stopped')
    }
    if (!KEEP_FILES && existsSync(CKL_DIR) && opts.generate) {
      console.log(`Removing generated CKL files from ${CKL_DIR}`)
      rmSync(CKL_DIR, { recursive: true, force: true })
    }
    if (existsSync(HISTORY_FILE)) {
      rmSync(HISTORY_FILE, { force: true })
    }
  }
}

main().catch(err => {
  console.error(err)
  process.exit(1)
})
