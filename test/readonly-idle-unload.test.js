import { Database } from '../src/Database.mjs'
import fs from 'fs'
import path from 'path'

/**
 * Item 6 of the concurrency report: race idle-unload × leitura / read-only.
 *
 * - After an idle-unload, the next find()/count() must recover the index from
 *   the on-disk .idx file (lazy load) instead of forcing a full rebuild or
 *   throwing when allowIndexRebuild is false (the default).
 * - Opening a DB with readOnly: true must never create, write, rebuild or
 *   auto-flush - reads only.
 */
describe('Idle-unload recovery and read-only mode', () => {
  let testDir

  beforeEach(() => {
    testDir = path.join(process.cwd(), 'test-files', 'readonly-idle-unload')
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

  test('find() after idle-unload recovers from disk index (no rebuild, no throw) with default allowIndexRebuild=false', async () => {
    const dbPath = path.join(testDir, 'idle-unload.jdb')
    const db = new Database(dbPath, {
      fields: { id: 'string', name: 'string' },
      indexes: { name: 'string' },
      clear: true,
      create: true,
      debugMode: false
      // allowIndexRebuild defaults to false
    })
    await db.init()
    await db.insert({ id: '1', name: 'alpha' })
    await db.insert({ id: '2', name: 'beta' })
    await db.save()

    // Load the index into memory once
    let results = await db.find({ name: 'alpha' })
    expect(results.length).toBe(1)

    // Simulate the idle-unload that IndexManager runs after indexIdleUnloadMs
    db.indexManager.unload()
    expect(db.indexManager.indexLoaded).toBe(false)
    expect(db._indexRebuildNeeded).toBe(true)

    // Guard against a full rebuild being triggered by the read
    const rebuildOriginalSpy = jest.spyOn(db, '_rebuildIndexesOriginal')
    const rebuildRetrySpy = jest.spyOn(db, '_rebuildIndexesWithRetry')

    try {
      // Before the fix this threw ("Index rebuild required but disabled")
      results = await db.find({ name: 'alpha' })
      expect(results.length).toBe(1)
      expect(results[0].name).toBe('alpha')

      // Index must have been restored from the on-disk .idx (not rebuilt)
      expect(db.indexManager.indexLoaded).toBe(true)
      expect(rebuildOriginalSpy).not.toHaveBeenCalled()
      expect(rebuildRetrySpy).not.toHaveBeenCalled()
    } finally {
      rebuildOriginalSpy.mockRestore()
      rebuildRetrySpy.mockRestore()
    }

    await closeDb(db)
  })

  test('read-only database serves reads but blocks writes and never writes to disk', async () => {
    const dbPath = path.join(testDir, 'readonly.jdb')

    // Seed with a writable database
    const writer = new Database(dbPath, {
      fields: { id: 'string', name: 'string' },
      indexes: { name: 'string' },
      clear: true,
      create: true,
      debugMode: false
    })
    await writer.init()
    await writer.insert({ id: '1', name: 'alpha' })
    await writer.save()
    await closeDb(writer)

    const idxPath = dbPath.replace('.jdb', '.idx.jdb')
    const dataBefore = fs.readFileSync(dbPath)
    const idxBefore = fs.readFileSync(idxPath)

    const db = new Database(dbPath, {
      fields: { id: 'string', name: 'string' },
      indexes: { name: 'string' },
      create: false,
      readOnly: true,
      debugMode: false
    })
    await db.init()

    // Reads work
    const results = await db.find({ name: 'alpha' })
    expect(results.length).toBe(1)

    // Writes are rejected
    await expect(db.insert({ id: '2', name: 'beta' })).rejects.toThrow(/read-only/i)
    await expect(db.insertBatch([{ id: '3', name: 'gamma' }])).rejects.toThrow(/read-only/i)

    // save() must not alter any file on disk
    await db.save()
    const dataAfter = fs.readFileSync(dbPath)
    const idxAfter = fs.readFileSync(idxPath)
    expect(dataAfter.equals(dataBefore)).toBe(true)
    expect(idxAfter.equals(idxBefore)).toBe(true)

    await closeDb(db)
  })

  test('read-only open of a missing file is rejected', async () => {
    const dbPath = path.join(testDir, 'does-not-exist.jdb')
    const db = new Database(dbPath, {
      fields: { id: 'string', name: 'string' },
      indexes: { name: 'string' },
      create: false,
      readOnly: true,
      debugMode: false
    })
    await expect(db.init()).rejects.toThrow(/does not exist/)
    await closeDb(db)
  })
})
