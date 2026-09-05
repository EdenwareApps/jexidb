import { Database } from '../src/Database.mjs'
import fs from 'fs'
import path from 'path'

/**
 * Item 2 of the concurrency report: integrity corrections must never run inside
 * the read hot path (find()). When the in-memory totalLines drifts from the
 * persisted offset count, find() reconciles it in memory only - it must NOT
 * rewrite the .idx file on every read (which is catastrophic under a concurrent
 * writer).
 */
describe('find() read path does not write the index file', () => {
  let testDir

  beforeEach(() => {
    testDir = path.join(process.cwd(), 'test-files', 'find-no-disk-write')
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
    // Load index into memory and reconcile totals
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

  test('find() reconciles totalLines in memory but never rewrites the .idx file', async () => {
    const dbPath = path.join(testDir, 'no-write.jdb')
    const db = await createSeededDb(dbPath)

    const idxPath = dbPath.replace('.jdb', '.idx.jdb')
    const before = fs.readFileSync(idxPath)

    // Introduce an in-memory inconsistency (offsets are authoritative)
    const correctionsBefore = db.integrityCorrections.dataIntegrity
    db.indexManager.setTotalLines(db.offsets.length + 7)
    expect(db.indexManager.totalLines).not.toBe(db.offsets.length)

    // find() must repair the in-memory totalLines without touching disk
    const results = await db.find({ name: 'alpha' })
    expect(results.length).toBe(1)
    expect(db.indexManager.totalLines).toBe(db.offsets.length)

    // The .idx file must be byte-identical: no write happened on the read path
    const after = fs.readFileSync(idxPath)
    expect(after.equals(before)).toBe(true)

    // A second find must NOT re-trigger the correction (reconciled once)
    const correctionsMid = db.integrityCorrections.dataIntegrity
    expect(correctionsMid).toBe(correctionsBefore + 1)
    await db.find({ name: 'beta' })
    expect(db.integrityCorrections.dataIntegrity).toBe(correctionsMid)

    await closeDb(db)
  })
})
