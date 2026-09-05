import { Database } from '../src/Database.mjs'
import fs from 'fs'
import path from 'path'

/**
 * Items 1/3 of the concurrency report:
 *
 * - 1b: save() persists the data file atomically (temp + rename), never
 *   leaving temp/sentinel files behind, so a concurrent reader never sees a
 *   half-written file.
 * - 1a: a reader detects that the data file was atomically swapped by another
 *   process (stat size/mtime vs the signature captured when the offsets were
 *   loaded) and reloads offsets + index cleanly instead of reading stale byte
 *   ranges.
 * - 3:  a writer sentinel (`<file>.updating.jdb`) makes readers report "busy"
 *   and skip any in-place refresh/repair while the writer is active.
 */
describe('Atomic save and read-side swap detection', () => {
  let testDir

  beforeEach(() => {
    testDir = path.join(process.cwd(), 'test-files', 'atomic-save-and-swap')
    fs.mkdirSync(testDir, { recursive: true })
  })

  afterEach(() => {
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

  test('save() persists atomically without leaving temp/sentinel files behind', async () => {
    const dbPath = path.join(testDir, 'atomic.jdb')
    const db = new Database(dbPath, {
      fields: { id: 'string', name: 'string' },
      indexes: { name: 'string' },
      updatingSentinel: true,
      clear: true,
      create: true,
      debugMode: false
    })
    await db.init()
    await db.insert({ id: '1', name: 'alpha' })
    await db.insert({ id: '2', name: 'beta' })
    await db.save()

    // No leftover temp or sentinel after the atomic swap (data AND index file)
    expect(fs.existsSync(dbPath + '.tmp')).toBe(false)
    expect(fs.existsSync(dbPath.replace(/\.jdb$/i, '.updating.jdb'))).toBe(false)
    expect(fs.existsSync(dbPath.replace(/\.jdb$/i, '.idx.jdb') + '.tmp')).toBe(false)

    const results = await db.find({ name: 'alpha' })
    expect(results.length).toBe(1)
    await closeDb(db)
  })

  test('reader detects an atomic file swap and reloads offsets + index cleanly', async () => {
    const readerDir = path.join(testDir, 'reader')
    fs.mkdirSync(readerDir, { recursive: true })
    const readerPath = path.join(readerDir, 'swap.jdb')

    // v1 dataset (two records)
    const v1 = new Database(readerPath, {
      fields: { id: 'string', name: 'string' },
      indexes: { name: 'string' },
      clear: true,
      create: true,
      debugMode: false
    })
    await v1.init()
    await v1.insert({ id: '1', name: 'alpha' })
    await v1.insert({ id: '2', name: 'beta' })
    await v1.save()
    await closeDb(v1)

    // Reader opens v1
    const reader = new Database(readerPath, {
      fields: { id: 'string', name: 'string' },
      indexes: { name: 'string' },
      create: false,
      debugMode: false
    })
    await reader.init()
    expect((await reader.find({ name: 'alpha' })).length).toBe(1)

    // v2 dataset built elsewhere (simulating another writer process)
    const writerDir = path.join(testDir, 'writer')
    fs.mkdirSync(writerDir, { recursive: true })
    const writerPath = path.join(writerDir, 'swap.jdb')
    const w2 = new Database(writerPath, {
      fields: { id: 'string', name: 'string' },
      indexes: { name: 'string' },
      clear: true,
      create: true,
      debugMode: false
    })
    await w2.init()
    await w2.insert({ id: '10', name: 'zeta' })
    await w2.save()
    await closeDb(w2)

    // Simulate an atomic cross-process swap of .jdb + .idx
    fs.copyFileSync(writerPath, readerPath)
    fs.copyFileSync(writerPath.replace(/\.jdb$/i, '.idx.jdb'), readerPath.replace(/\.jdb$/i, '.idx.jdb'))

    // Force a fresh stat (bypass the 250ms fingerprint TTL used in production)
    reader._dataFingerprint = null

    // The in-memory index still describes v1 (stale) right after the swap...
    expect(reader.indexManager.query({ name: 'zeta' }).size).toBe(0)

    // ...but swap detection must detect the change and reload cleanly
    const status = await reader._refreshFromDiskIfChanged()
    expect(status).toBe('changed')

    // After the clean reload the in-memory index reflects v2
    expect(reader.indexManager.query({ name: 'zeta' }).size).toBeGreaterThan(0)
    expect(reader.indexManager.query({ name: 'alpha' }).size).toBe(0)

    // Public reads now see v2 data, not v1
    const zeta = await reader.find({ name: 'zeta' })
    expect(zeta.length).toBe(1)
    expect(zeta[0].id).toBe('10')
    const alpha = await reader.find({ name: 'alpha' })
    expect(alpha.length).toBe(0)

    await closeDb(reader)
  })

  test('writer sentinel makes the reader report busy and skip in-place refresh', async () => {
    const dbPath = path.join(testDir, 'sentinel.jdb')
    const db = new Database(dbPath, {
      fields: { id: 'string', name: 'string' },
      indexes: { name: 'string' },
      updatingSentinel: true,
      clear: true,
      create: true,
      debugMode: false
    })
    await db.init()
    await db.insert({ id: '1', name: 'alpha' })
    await db.save()
    await db.find({ name: 'alpha' })

    const sentinelPath = dbPath.replace(/\.jdb$/i, '.updating.jdb')
    expect(fs.existsSync(sentinelPath)).toBe(false)

    // Simulate an active writer
    fs.writeFileSync(sentinelPath, String(Date.now()))
    try {
      expect(await db.hasUpdatingSentinel()).toBe(true)

      // Force a detected swap (artificial signature mismatch) - but because a
      // writer sentinel exists, refresh must report 'busy' (no in-place repair)
      db._offsetsFileSignature = {
        size: (db._offsetsFileSignature ? db._offsetsFileSignature.size : 0) + 1,
        mtimeMs: 1
      }
      db._dataFingerprint = null
      expect(await db._refreshFromDiskIfChanged()).toBe('busy')
    } finally {
      fs.unlinkSync(sentinelPath)
    }

    // Sentinel removed -> refresh is no longer busy
    db._dataFingerprint = null
    expect(await db._refreshFromDiskIfChanged()).not.toBe('busy')

    await closeDb(db)
  })

  test('walk() refreshes offsets after an atomic swap (sees the whole new file)', async () => {
    const readerDir = path.join(testDir, 'reader-walk')
    fs.mkdirSync(readerDir, { recursive: true })
    const readerPath = path.join(readerDir, 'walk.jdb')

    // v1: ONE record so that without a refresh walk would only read one line of v2
    const v1 = new Database(readerPath, {
      fields: { id: 'string', name: 'string' },
      indexes: { name: 'string' },
      clear: true,
      create: true,
      debugMode: false
    })
    await v1.init()
    await v1.insert({ id: '1', name: 'alpha' })
    await v1.save()
    await closeDb(v1)

    const reader = new Database(readerPath, {
      fields: { id: 'string', name: 'string' },
      indexes: { name: 'string' },
      create: false,
      debugMode: false
    })
    await reader.init()

    // v2: TWO records (larger than v1) built elsewhere
    const writerDir = path.join(testDir, 'writer-walk')
    fs.mkdirSync(writerDir, { recursive: true })
    const writerPath = path.join(writerDir, 'walk.jdb')
    const w2 = new Database(writerPath, {
      fields: { id: 'string', name: 'string' },
      indexes: { name: 'string' },
      clear: true,
      create: true,
      debugMode: false
    })
    await w2.init()
    await w2.insert({ id: '10', name: 'zeta' })
    await w2.insert({ id: '11', name: 'zap' })
    await w2.save()
    await closeDb(w2)

    // Simulate an atomic cross-process swap of .jdb + .idx
    fs.copyFileSync(writerPath, readerPath)
    fs.copyFileSync(writerPath.replace(/\.jdb$/i, '.idx.jdb'), readerPath.replace(/\.jdb$/i, '.idx.jdb'))
    reader._dataFingerprint = null

    const seen = []
    for await (const entry of reader.walk({})) {
      if (entry && entry.name) seen.push(entry.name)
    }
    expect(seen.sort()).toEqual(['zap', 'zeta'])
    await closeDb(reader)
  })
})
