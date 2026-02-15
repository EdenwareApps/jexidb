import { Database } from '../src/Database.mjs'
import fs from 'fs'
import path from 'path'
import { Readable } from 'stream'

describe('IO Timeout Slow Stream', () => {
  let testDir
  let db
  let createReadStreamSpy

  beforeEach(() => {
    testDir = path.join(process.cwd(), 'test-files', 'io-timeout-slow-stream')
    fs.mkdirSync(testDir, { recursive: true })
  })

  afterEach(async () => {
    if (createReadStreamSpy) {
      createReadStreamSpy.mockRestore()
      createReadStreamSpy = null
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

  test('find() aborts slow streaming read within timeout', async () => {
    const dbPath = path.join(testDir, 'slow-stream.jdb')

    db = new Database(dbPath, {
      fields: {
        id: 'string',
        name: 'string'
      },
      clear: true,
      create: true,
      debugMode: false
    })

    await db.init()
    await db.insert({ id: '1', name: 'alpha' })
    await db.save()

    createReadStreamSpy = jest.spyOn(fs, 'createReadStream').mockImplementation(() => {
      return new Readable({
        read() {
          // Intentionally never emit data to simulate a stuck stream.
        }
      })
    })

    const start = Date.now()
    const results = await db.find({}, { ioTimeoutMs: 50, maxRetries: 1 })
    const elapsed = Date.now() - start

    expect(Array.isArray(results)).toBe(true)
    expect(results.length).toBe(0)
    expect(elapsed).toBeLessThan(500)
  })
})
