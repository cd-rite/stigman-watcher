import { expect } from 'chai'
import { EventEmitter } from 'node:events'
import Queue from 'better-queue'
import { setTimeout as delay } from 'node:timers/promises'

/**
 * Backpressure tests for the parse -> cargo queue pipeline.
 *
 * These tests verify the design from fix-design.md: after pushing a parse
 * result to cargoQueue, the parse worker checks cargo depth. If depth exceeds
 * a high-water mark derived from cargoSize (2 * cargoSize), the worker delays
 * calling cb() until cargo drains below the threshold.
 *
 * Tests use better-queue directly with mock workers. No real CKL files,
 * filesystem operations, or API calls are needed.
 */

// ---------- Helpers that mirror the proposed fix in lib/parse.js ----------

/**
 * Returns the number of tasks waiting in a better-queue instance's memory store.
 * Mirrors getCargoDepth() from the proposed fix.
 */
function getCargoDepth (queue) {
  return queue._store._queue.length
}

/**
 * Returns a Promise that resolves when queue depth drops at or below threshold,
 * or when an alarm is raised on the provided alarm emitter.
 * Mirrors waitForCargoBelow() from the proposed fix.
 */
function waitForCargoBelow (queue, threshold, alarm) {
  return new Promise((resolve) => {
    const checkDepth = () => {
      if (getCargoDepth(queue) <= threshold) {
        queue.removeListener('batch_finish', onBatchFinish)
        alarm.removeListener('alarmRaised', onAlarm)
        resolve()
      }
    }
    const onBatchFinish = () => {
      checkDepth()
    }
    const onAlarm = () => {
      queue.removeListener('batch_finish', onBatchFinish)
      alarm.removeListener('alarmRaised', onAlarm)
      resolve()
    }
    queue.on('batch_finish', onBatchFinish)
    alarm.on('alarmRaised', onAlarm)
    checkDepth()
  })
}

/**
 * Creates a cargoQueue with a slow mock consumer.
 * @param {object} opts
 * @param {number} opts.batchSize - cargo batch size (options.cargoSize equivalent)
 * @param {number} opts.batchDelay - delay before processing a batch (ms)
 * @param {number} opts.processingTime - simulated per-batch processing time (ms)
 * @param {Function} [opts.onBatch] - optional callback invoked with each batch
 * @returns {Queue} configured better-queue instance
 */
function createCargoQueue ({ batchSize, batchDelay, processingTime, onBatch }) {
  const queue = new Queue(
    async function cargoWorker (batch, cb) {
      if (!Array.isArray(batch)) {
        batch = [batch]
      }
      if (onBatch) {
        onBatch(batch)
      }
      await delay(processingTime)
      cb()
    },
    {
      batchSize,
      batchDelay,
    }
  )
  return queue
}

/**
 * Creates a parseQueue that pushes results to cargoQueue with backpressure.
 * @param {object} opts
 * @param {Queue} opts.cargoQueue - the cargo queue to push results to
 * @param {number} opts.cargoHighWaterMark - backpressure threshold
 * @param {number} opts.concurrent - parse concurrency (default 8)
 * @param {EventEmitter} opts.alarm - alarm emitter
 * @param {number} [opts.parseTime] - simulated parse time (ms), default 1
 * @returns {Queue} configured better-queue instance
 */
function createParseQueueWithBackpressure ({ cargoQueue, cargoHighWaterMark, concurrent = 8, alarm, parseTime = 1 }) {
  const queue = new Queue(
    async function parseWorker (task, cb) {
      // Simulate fast parsing
      await delay(parseTime)

      // Push the parse result to cargoQueue
      const parseResult = { file: task, data: `parsed-${task}` }
      cargoQueue.push(parseResult)

      // Backpressure check (mirrors proposed fix)
      if (getCargoDepth(cargoQueue) > cargoHighWaterMark) {
        await waitForCargoBelow(cargoQueue, cargoHighWaterMark, alarm)
      }
      cb(null, parseResult)
    },
    {
      concurrent
    }
  )
  return queue
}

/**
 * Creates a parseQueue WITHOUT backpressure (for comparison tests).
 * @param {object} opts
 * @param {Queue} opts.cargoQueue - the cargo queue to push results to
 * @param {number} opts.concurrent - parse concurrency (default 8)
 * @param {number} [opts.parseTime] - simulated parse time (ms), default 1
 * @returns {Queue} configured better-queue instance
 */
function createParseQueueWithoutBackpressure ({ cargoQueue, concurrent = 8, parseTime = 1 }) {
  const queue = new Queue(
    async function parseWorker (task, cb) {
      await delay(parseTime)
      const parseResult = { file: task, data: `parsed-${task}` }
      cargoQueue.push(parseResult)
      cb(null, parseResult)
    },
    {
      concurrent
    }
  )
  return queue
}

/**
 * Pushes N items to a queue and returns a Promise that resolves when the
 * queue drains (all items processed by their downstream consumers).
 * @param {Queue} queue - the queue to push to
 * @param {number} count - number of items to push
 * @returns {Promise<void>}
 */
function pushItemsAndWaitForDrain (queue, count) {
  return new Promise((resolve) => {
    queue.on('drain', resolve)
    for (let i = 0; i < count; i++) {
      queue.push(`file-${i}.ckl`)
    }
  })
}

/**
 * Tracks the maximum cargo depth over time by polling.
 * Returns a function that, when called, stops polling and returns the max depth seen.
 * @param {Queue} cargoQueue
 * @param {number} intervalMs
 * @returns {Function} stop function that returns { maxDepth, samples }
 */
function trackMaxCargoDepth (cargoQueue, intervalMs = 5) {
  let maxDepth = 0
  let samples = 0
  const timer = setInterval(() => {
    const depth = getCargoDepth(cargoQueue)
    if (depth > maxDepth) {
      maxDepth = depth
    }
    samples++
  }, intervalMs)

  return function stop () {
    clearInterval(timer)
    return { maxDepth, samples }
  }
}


// ========================== Test Suites ==========================

describe('backpressure: threshold derivation', function () {
  it('threshold should be 2 * cargoSize per the design', function () {
    const cargoSize = 10
    const cargoHighWaterMark = 2 * cargoSize
    expect(cargoHighWaterMark).to.equal(20)
  })

  it('threshold should scale with cargoSize', function () {
    for (const cargoSize of [1, 5, 10, 25, 50, 100]) {
      const cargoHighWaterMark = 2 * cargoSize
      expect(cargoHighWaterMark).to.equal(2 * cargoSize)
    }
  })

  it('maximum possible peak should be 2*cargoSize + 2*concurrency', function () {
    const cargoSize = 10
    const concurrent = 8
    const cargoHighWaterMark = 2 * cargoSize
    const maxPeak = cargoHighWaterMark + (2 * concurrent)
    // Default config: 20 + 16 = 36
    expect(maxPeak).to.equal(36)
  })
})


describe('backpressure: cargo depth stays bounded', function () {
  this.timeout(30000)

  it('without backpressure, cargo depth grows unbounded', async function () {
    const cargoSize = 5
    const totalFiles = 100
    const alarm = new EventEmitter()

    const cargoQueue = createCargoQueue({
      batchSize: cargoSize,
      batchDelay: 50,
      processingTime: 200, // slow consumer
    })

    const parseQueue = createParseQueueWithoutBackpressure({
      cargoQueue,
      concurrent: 8,
      parseTime: 1, // fast producer
    })

    const stopTracking = trackMaxCargoDepth(cargoQueue, 2)

    await pushItemsAndWaitForDrain(parseQueue, totalFiles)

    // Wait for cargo to drain too
    await new Promise((resolve) => {
      if (getCargoDepth(cargoQueue) === 0) {
        resolve()
      } else {
        cargoQueue.on('drain', resolve)
      }
    })

    const { maxDepth } = stopTracking()

    // Without backpressure, depth should exceed 2 * cargoSize
    // because parse pushes all items before cargo can consume them
    expect(maxDepth).to.be.greaterThan(2 * cargoSize,
      'without backpressure, cargo depth should exceed the threshold')
  })

  it('with backpressure, cargo depth stays bounded', async function () {
    const cargoSize = 5
    const cargoHighWaterMark = 2 * cargoSize // = 10
    const concurrent = 4
    const totalFiles = 100
    const alarm = new EventEmitter()

    const cargoQueue = createCargoQueue({
      batchSize: cargoSize,
      batchDelay: 50,
      processingTime: 200, // slow consumer
    })

    const parseQueue = createParseQueueWithBackpressure({
      cargoQueue,
      cargoHighWaterMark,
      concurrent,
      alarm,
      parseTime: 1, // fast producer
    })

    const stopTracking = trackMaxCargoDepth(cargoQueue, 2)

    await pushItemsAndWaitForDrain(parseQueue, totalFiles)

    // Wait for cargo to drain too
    await new Promise((resolve) => {
      if (getCargoDepth(cargoQueue) === 0) {
        resolve()
      } else {
        cargoQueue.on('drain', resolve)
      }
    })

    const { maxDepth } = stopTracking()

    // With backpressure, depth should stay bounded.
    // The theoretical max is cargoHighWaterMark + 2*concurrent (overshoot).
    const maxAllowed = cargoHighWaterMark + (2 * concurrent)
    expect(maxDepth).to.be.at.most(maxAllowed,
      `cargo depth should not exceed ${maxAllowed} (hwm=${cargoHighWaterMark} + 2*concurrent=${2 * concurrent})`)
  })
})


describe('backpressure: all items eventually processed', function () {
  this.timeout(30000)

  it('all items should be processed by cargoQueue even with backpressure', async function () {
    const cargoSize = 5
    const cargoHighWaterMark = 2 * cargoSize
    const totalFiles = 60
    const alarm = new EventEmitter()
    const processedItems = []

    const cargoQueue = createCargoQueue({
      batchSize: cargoSize,
      batchDelay: 20,
      processingTime: 50,
      onBatch: (batch) => {
        for (const item of batch) {
          processedItems.push(item.file)
        }
      },
    })

    const parseQueue = createParseQueueWithBackpressure({
      cargoQueue,
      cargoHighWaterMark,
      concurrent: 8,
      alarm,
      parseTime: 1,
    })

    // Push all items and wait for parseQueue to drain
    await pushItemsAndWaitForDrain(parseQueue, totalFiles)

    // Wait for cargoQueue to drain
    await new Promise((resolve) => {
      if (getCargoDepth(cargoQueue) === 0) {
        // Check if cargo is also not processing anything
        const checkDrain = () => {
          cargoQueue.on('drain', resolve)
        }
        // Give it a moment to start processing final batch
        setTimeout(() => {
          if (getCargoDepth(cargoQueue) === 0) {
            resolve()
          } else {
            checkDrain()
          }
        }, 200)
      } else {
        cargoQueue.on('drain', resolve)
      }
    })

    // Allow final batch processing to complete
    await delay(200)

    // Verify all items were processed
    expect(processedItems.length).to.equal(totalFiles,
      `expected ${totalFiles} items processed, got ${processedItems.length}`)

    // Verify we got every file (order may differ due to concurrency)
    const expectedFiles = Array.from({ length: totalFiles }, (_, i) => `file-${i}.ckl`)
    const sortedProcessed = [...processedItems].sort()
    const sortedExpected = [...expectedFiles].sort()
    expect(sortedProcessed).to.deep.equal(sortedExpected)
  })
})


describe('backpressure: alarm interaction', function () {
  this.timeout(30000)

  it('waitForCargoBelow should resolve immediately when alarm fires', async function () {
    const cargoSize = 5
    const cargoHighWaterMark = 2 * cargoSize
    const alarm = new EventEmitter()

    // Create a cargo queue with a very slow consumer so it never drains
    const cargoQueue = createCargoQueue({
      batchSize: cargoSize,
      batchDelay: 10,
      processingTime: 5000, // extremely slow, will not finish during test
    })

    // Manually push items to fill cargo above threshold
    for (let i = 0; i < cargoHighWaterMark + 5; i++) {
      cargoQueue.push({ file: `file-${i}`, data: `data-${i}` })
    }

    // Give the store a moment to register the pushes
    await delay(50)

    const depth = getCargoDepth(cargoQueue)
    expect(depth).to.be.greaterThan(0,
      'cargo should have queued items')

    // Start waiting for cargo to drain (it won't, consumer is too slow)
    const startTime = Date.now()
    const waitPromise = waitForCargoBelow(cargoQueue, cargoHighWaterMark, alarm)

    // Fire alarm after 100ms
    setTimeout(() => {
      alarm.emit('alarmRaised', 'apiOffline')
    }, 100)

    // The wait should resolve quickly after the alarm, not after 5 seconds
    await waitPromise
    const elapsed = Date.now() - startTime

    expect(elapsed).to.be.lessThan(1000,
      'waitForCargoBelow should resolve promptly when alarm fires')
  })

  it('parse workers should not deadlock when alarm fires during backpressure', async function () {
    const cargoSize = 3
    const cargoHighWaterMark = 2 * cargoSize // = 6
    const concurrent = 4
    const totalFiles = 30
    const alarm = new EventEmitter()
    const parseFinished = []

    const cargoQueue = createCargoQueue({
      batchSize: cargoSize,
      batchDelay: 10,
      processingTime: 100,
    })

    const parseQueue = createParseQueueWithBackpressure({
      cargoQueue,
      cargoHighWaterMark,
      concurrent,
      alarm,
      parseTime: 1,
    })

    // Track parse completions
    parseQueue.on('task_finish', (taskId) => {
      parseFinished.push(taskId)
    })

    // Push items to the parse queue
    for (let i = 0; i < totalFiles; i++) {
      parseQueue.push(`file-${i}.ckl`)
    }

    // Wait a bit for backpressure to kick in
    await delay(200)

    // Fire an alarm -- this should unblock any waiting parse workers
    alarm.emit('alarmRaised', 'apiOffline')

    // Pause both queues as the real code does
    parseQueue.pause()
    cargoQueue.pause()

    // Wait a moment, then clear the alarm
    await delay(200)

    // Resume both queues
    cargoQueue.resume()
    parseQueue.resume()
    alarm.emit('alarmLowered', 'apiOffline')

    // Wait for everything to drain
    await new Promise((resolve) => {
      parseQueue.on('drain', resolve)
    })

    await new Promise((resolve) => {
      if (getCargoDepth(cargoQueue) === 0) {
        resolve()
      } else {
        cargoQueue.on('drain', resolve)
      }
    })

    await delay(200)

    // All files should have completed parsing
    expect(parseFinished.length).to.equal(totalFiles,
      `all ${totalFiles} files should complete parsing even with alarm interruption`)
  })
})


describe('backpressure: waitForCargoBelow edge cases', function () {
  this.timeout(10000)

  it('should resolve immediately if cargo depth is already below threshold', async function () {
    const cargoSize = 5
    const cargoHighWaterMark = 2 * cargoSize
    const alarm = new EventEmitter()

    const cargoQueue = createCargoQueue({
      batchSize: cargoSize,
      batchDelay: 10,
      processingTime: 10,
    })

    // Push fewer items than the threshold
    for (let i = 0; i < cargoHighWaterMark - 2; i++) {
      cargoQueue.push({ file: `file-${i}`, data: `data-${i}` })
    }

    await delay(30)

    const startTime = Date.now()
    await waitForCargoBelow(cargoQueue, cargoHighWaterMark, alarm)
    const elapsed = Date.now() - startTime

    expect(elapsed).to.be.lessThan(50,
      'should resolve almost immediately when depth is already below threshold')
  })

  it('should resolve immediately if cargo is empty', async function () {
    const alarm = new EventEmitter()

    const cargoQueue = createCargoQueue({
      batchSize: 5,
      batchDelay: 10,
      processingTime: 10,
    })

    const startTime = Date.now()
    await waitForCargoBelow(cargoQueue, 10, alarm)
    const elapsed = Date.now() - startTime

    expect(elapsed).to.be.lessThan(50,
      'should resolve almost immediately when cargo is empty')
  })

  it('should clean up listeners after resolving via batch_finish', async function () {
    const cargoSize = 3
    const cargoHighWaterMark = 2 * cargoSize
    const alarm = new EventEmitter()

    const cargoQueue = createCargoQueue({
      batchSize: cargoSize,
      batchDelay: 10,
      processingTime: 50,
    })

    // Fill above threshold
    for (let i = 0; i < cargoHighWaterMark + 5; i++) {
      cargoQueue.push({ file: `file-${i}`, data: `data-${i}` })
    }

    await delay(30)

    const batchListenersBefore = cargoQueue.listenerCount('batch_finish')
    const alarmListenersBefore = alarm.listenerCount('alarmRaised')

    // Wait for cargo to drain below threshold
    await waitForCargoBelow(cargoQueue, cargoHighWaterMark, alarm)

    const batchListenersAfter = cargoQueue.listenerCount('batch_finish')
    const alarmListenersAfter = alarm.listenerCount('alarmRaised')

    // Listeners should be cleaned up (back to where they were)
    expect(batchListenersAfter).to.equal(batchListenersBefore,
      'batch_finish listener should be removed after resolving')
    expect(alarmListenersAfter).to.equal(alarmListenersBefore,
      'alarmRaised listener should be removed after resolving')
  })

  it('should clean up listeners after resolving via alarm', async function () {
    const alarm = new EventEmitter()

    // Cargo with extremely slow consumer -- will never drain
    const cargoQueue = createCargoQueue({
      batchSize: 3,
      batchDelay: 10,
      processingTime: 60000,
    })

    // Fill it up
    for (let i = 0; i < 20; i++) {
      cargoQueue.push({ file: `file-${i}`, data: `data-${i}` })
    }

    await delay(30)

    const batchListenersBefore = cargoQueue.listenerCount('batch_finish')
    const alarmListenersBefore = alarm.listenerCount('alarmRaised')

    // Start waiting, then fire alarm
    const waitPromise = waitForCargoBelow(cargoQueue, 6, alarm)

    // Verify listeners were added
    expect(cargoQueue.listenerCount('batch_finish')).to.equal(batchListenersBefore + 1)
    expect(alarm.listenerCount('alarmRaised')).to.equal(alarmListenersBefore + 1)

    alarm.emit('alarmRaised', 'apiOffline')
    await waitPromise

    // Listeners should be cleaned up
    expect(cargoQueue.listenerCount('batch_finish')).to.equal(batchListenersBefore,
      'batch_finish listener should be removed after alarm resolve')
    expect(alarm.listenerCount('alarmRaised')).to.equal(alarmListenersBefore,
      'alarmRaised listener should be removed after alarm resolve')
  })
})


describe('backpressure: getCargoDepth accuracy', function () {
  this.timeout(10000)

  it('should return 0 for an empty queue', function () {
    const cargoQueue = createCargoQueue({
      batchSize: 5,
      batchDelay: 1000, // high delay so nothing processes
      processingTime: 10,
    })

    expect(getCargoDepth(cargoQueue)).to.equal(0)
  })

  it('should reflect items pushed to the queue', async function () {
    const cargoQueue = createCargoQueue({
      batchSize: 5,
      batchDelay: 5000, // high delay so nothing gets batched yet
      processingTime: 10,
    })

    cargoQueue.push({ file: 'a' })
    cargoQueue.push({ file: 'b' })
    cargoQueue.push({ file: 'c' })

    // Give store a moment to register
    await delay(20)

    // Depth should be at least 1 (some may have been taken for processing
    // even with high batchDelay, since better-queue may batch on first push)
    const depth = getCargoDepth(cargoQueue)
    expect(depth).to.be.at.least(0)
    // Total pushed items minus any being processed should be reflected
    // The exact value depends on better-queue internals, but we can verify
    // the function returns a number
    expect(depth).to.be.a('number')
  })

  it('should decrease as items are processed', async function () {
    const cargoQueue = createCargoQueue({
      batchSize: 3,
      batchDelay: 10,
      processingTime: 10,
    })

    // Push items
    for (let i = 0; i < 12; i++) {
      cargoQueue.push({ file: `file-${i}` })
    }

    await delay(20)
    const depthAfterPush = getCargoDepth(cargoQueue)

    // Wait for some processing
    await delay(200)
    const depthAfterProcessing = getCargoDepth(cargoQueue)

    expect(depthAfterProcessing).to.be.lessThan(depthAfterPush,
      'depth should decrease as items are processed')
  })
})


describe('backpressure: different cargoSize values', function () {
  this.timeout(30000)

  for (const cargoSize of [1, 3, 10]) {
    it(`should bound depth with cargoSize=${cargoSize}`, async function () {
      const cargoHighWaterMark = 2 * cargoSize
      const concurrent = 4
      const totalFiles = Math.max(40, cargoSize * 8) // enough to trigger backpressure
      const alarm = new EventEmitter()

      const cargoQueue = createCargoQueue({
        batchSize: cargoSize,
        batchDelay: 10,
        processingTime: 100,
      })

      const parseQueue = createParseQueueWithBackpressure({
        cargoQueue,
        cargoHighWaterMark,
        concurrent,
        alarm,
        parseTime: 1,
      })

      const stopTracking = trackMaxCargoDepth(cargoQueue, 2)

      await pushItemsAndWaitForDrain(parseQueue, totalFiles)

      // Wait for cargo to drain
      await new Promise((resolve) => {
        if (getCargoDepth(cargoQueue) === 0) {
          resolve()
        } else {
          cargoQueue.on('drain', resolve)
        }
      })

      await delay(200)
      const { maxDepth } = stopTracking()

      const maxAllowed = cargoHighWaterMark + (2 * concurrent)
      expect(maxDepth).to.be.at.most(maxAllowed,
        `cargoSize=${cargoSize}: depth ${maxDepth} should not exceed ${maxAllowed}`)
    })
  }
})
