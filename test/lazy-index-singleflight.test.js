import { Database } from '../src/Database.mjs'
import fs from 'fs'
import path from 'path'

/**
 * Item 5 of the concurrency report: single-flight lazy index load.
 *
 * - N concurrent find/count/score on the same Database must share a single
 *   on-disk .idx load (no redundant cold loads).
 * - A rejected/aborted load must never poison the single-flight promise for
 *   future attempts, and must degrade gracefully (never an unhandled error).
 */
describe('Lazy index single-flight', () => {
  let testDir

  beforeEach(() => {
    testDir = path.join(process.cwd(), 'test-files', 'lazy-index-singleflight')
    fs.mkdirSync(testDir, { recursive: true })
  })

  afterEach(async () => {
    if (fs.existsSync(testDir)) {
      try {
        fs.rmSync(testDir, { recursive: true, force: true })
      } catch (error) {
        console.warn('Could not clean up test directory:', testDir)
      }
    }
  })

  async function createSeededDb(dbPath) {
    const db = new Database(dbPath, {
      fields: { id: 'string', name: 'string' },
      indexes: { name: 'string' },
      clear: true,
      create: true,
      debugMode: false
    })
    await db.init()
    await db.insert({ id: '1', name: 'alpha' })
    await db.insert({ id: '2', name: 'beta' })
    await db.save()
    // Load the index into memory
    await db.find({ name: 'alpha' })
    return db
  }

  async function closeDb(db) {
    if (db && !db.destroyed) {
      try {
        await db.waitForOperations()
        await db.close()
      } catch (error) {
        console.warn('closeDb warning:', error.message)
      }
    }
  }

  test('concurrent finds after idle-unload share a single on-disk index load', async () => {
    const dbPath = path.join(testDir, 'singleflight.jdb')
    const db = await createSeededDb(dbPath)

    // Simulate idle-unload so the next reads must cold-load the index
    db.indexManager.unload()
    expect(db.indexManager.indexLoaded).toBe(false)

    const loadSpy = jest.spyOn(db, '_loadIndexDataFromFile')

    try {
      const results = await Promise.all([
        db.find({ name: 'alpha' }),
        db.find({ name: 'beta' }),
        db.find({ name: 'alpha' }),
        db.find({ name: 'gamma' }),
        db.find({ name: 'alpha' })
      ])

      // Exactly ONE disk read of the .idx for all 5 concurrent finds
      expect(loadSpy).toHaveBeenCalledTimes(1)
      expect(db.indexManager.indexLoaded).toBe(true)

      // alpha/beta are found; gamma does not exist
      expect(results[0].length).toBe(1)
      expect(results[1].length).toBe(1)
      expect(results[3].length).toBe(0)
    } finally {
      loadSpy.mockRestore()
    }

    await closeDb(db)
  })

  test('a failed lazy load degrades gracefully and does not poison future loads', async () => {
    const dbPath = path.join(testDir, 'poisoned.jdb')
    const db = await createSeededDb(dbPath)

    db.indexManager.unload()
    const loadSpy = jest
      .spyOn(db, '_loadIndexDataFromFile')
      .mockRejectedValueOnce(new Error('simulated read failure'))

    try {
      // First load fails but must NOT reject and must NOT leave the DB poisoned
      await expect(db._ensureLazyIndexLoaded()).resolves.toBeUndefined()
      expect(db.indexManager.indexLoaded).toBe(true)

      // Force another unload and verify a subsequent find cold-loads fresh data
      db.indexManager.unload()
      const results = await db.find({ name: 'alpha' })
      expect(results.length).toBe(1)
      expect(db.indexManager.indexLoaded).toBe(true)
    } finally {
      loadSpy.mockRestore()
    }

    await closeDb(db)
  })
})
