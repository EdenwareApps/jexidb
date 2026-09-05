import { Database } from '../src/Database.mjs'
import { isIoTimeoutError } from '../src/utils/ioTimeout.mjs'
import fs from 'fs'
import path from 'path'
import { Readable } from 'stream'

/**
 * Regression tests for the AbortError crash (item 4 of the concurrency report):
 *
 * A timed-out index rebuild used to call `stream.destroy(new Error('AbortError'))`
 * followed by `rl.close()`. Because readline closes before the stream 'error'
 * event fires (and no 'error' listener exists), Node escalates it to an
 * `uncaughtException`, crashing the host process. The fix destroys the stream
 * without an error payload, detects `signal.aborted` after the loop and throws
 * a normalized, retriable I/O timeout error instead.
 */
describe('AbortError / IO timeout regression', () => {
  let testDir
  let db
  let createReadStreamSpy
  let uncaughtHandler

  beforeEach(() => {
    testDir = path.join(process.cwd(), 'test-files', 'abort-error-regression')
    fs.mkdirSync(testDir, { recursive: true })
  })

  afterEach(async () => {
    if (createReadStreamSpy) {
      createReadStreamSpy.mockRestore()
      createReadStreamSpy = null
    }

    if (uncaughtHandler) {
      process.removeListener('uncaughtException', uncaughtHandler)
      uncaughtHandler = null
    }

    if (db && !db.destroyed) {
      await db.waitForOperations()
      await db.close()
    }

    if (fs.existsSync(testDir)) {
      try {
        fs.rmSync(testDir, { recursive: true, force: true })
      } catch (error) {
        console.warn('Could not clean up test directory:', testDir)
      }
    }
  })

  function captureUncaught() {
    const captured = []
    uncaughtHandler = (error) => {
      captured.push(error)
      console.error('>>> uncaughtException in test:', error && error.name, error && error.message)
    }
    process.on('uncaughtException', uncaughtHandler)
    return captured
  }

  test('aborted index rebuild rejects with retriable timeout error and never leaks uncaughtException', async () => {
    const dbPath = path.join(testDir, 'rebuild-timeout.jdb')

    db = new Database(dbPath, {
      fields: { id: 'string', name: 'string' },
      indexes: { name: 'string' },
      clear: true,
      create: true,
      allowIndexRebuild: true,
      ioTimeoutMs: 50,
      maxRetries: 1,
      debugMode: false
    })
    await db.init()

    const captured = captureUncaught()

    // Simulate a stuck disk read: the rebuild stream never emits and never ends,
    // so the abort timer has to fire to unblock the read.
    createReadStreamSpy = jest.spyOn(fs, 'createReadStream').mockImplementation(() => {
      return new Readable({
        read() {
          // Intentionally never emit data to simulate a stuck stream.
        }
      })
    })

    // The rebuild path is what runs inline inside find()/count() when the index
    // needs rebuilding. It must reject with a normalized, retriable error -
    // never crash the process with an uncaughtException.
    await expect(db._rebuildIndexesWithRetry()).rejects.toMatchObject({ code: 'ETIMEDOUT' })

    // Give the event loop a moment to surface any stray unhandled stream error.
    await new Promise(resolve => setTimeout(resolve, 150))

    expect(captured).toHaveLength(0)
  })

  test('ioTimeout helper normalizes error detection across shapes', () => {
    // Real Node shapes: name 'AbortError' | code 'ETIMEDOUT' | message 'AbortError'
    expect(isIoTimeoutError(null)).toBe(false)

    const byName = new Error('x')
    byName.name = 'AbortError'
    expect(isIoTimeoutError(byName)).toBe(true)

    const byCode = new Error('I/O timeout after 50ms')
    byCode.code = 'ETIMEDOUT'
    expect(isIoTimeoutError(byCode)).toBe(true)

    const byMessage = new Error('AbortError')
    expect(isIoTimeoutError(byMessage)).toBe(true)

    expect(isIoTimeoutError(new Error('boom'))).toBe(false)
  })
})
