import { Database } from '../src/Database.mjs'
import fs from 'fs'
import path from 'path'

/**
 * Follow-up coverage for items 1a/3: when a term-mapped database file is
 * atomically swapped by another process, `score()` must refresh offsets, index
 * AND term mapping before computing scores - otherwise the new terms are
 * unknown to the in-memory TermManager and nothing matches.
 */
describe('score() and term-mapping reload after a file swap', () => {
  let testDir

  beforeEach(() => {
    testDir = path.join(process.cwd(), 'test-files', 'swap-score-term-mapping')
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

  async function seed(dbPath, records) {
    const db = new Database(dbPath, {
      fields: { id: 'string', tags: 'array:string' },
      indexes: { tags: 'array:string' },
      clear: true,
      create: true,
      debugMode: false
    })
    await db.init()
    for (const record of records) {
      await db.insert({ id: record.id, tags: record.tags })
    }
    await db.save()
    return db
  }

  test('score() reloads term mapping + index after an atomic swap', async () => {
    const readerDir = path.join(testDir, 'reader')
    fs.mkdirSync(readerDir, { recursive: true })
    const readerPath = path.join(readerDir, 'terms.jdb')

    // v1 uses terms alpha/beta
    const v1 = await seed(readerPath, [{ id: '1', tags: ['alpha', 'beta'] }])
    await closeDb(v1)

    const reader = new Database(readerPath, {
      fields: { id: 'string', tags: 'array:string' },
      indexes: { tags: 'array:string' },
      create: false,
      debugMode: false
    })
    await reader.init()

    // Sanity: v1 is scoreable
    const before = await reader.score('tags', { alpha: 1 })
    expect(before.length).toBe(1)

    // v2 (different terms: zeta/zap) built elsewhere
    const writerDir = path.join(testDir, 'writer')
    fs.mkdirSync(writerDir, { recursive: true })
    const writerPath = path.join(writerDir, 'terms.jdb')
    const w2 = await seed(writerPath, [{ id: '10', tags: ['zeta', 'zap'] }])
    await closeDb(w2)

    // Simulate an atomic cross-process swap of .jdb + .idx
    fs.copyFileSync(writerPath, readerPath)
    fs.copyFileSync(writerPath.replace(/\.jdb$/i, '.idx.jdb'), readerPath.replace(/\.jdb$/i, '.idx.jdb'))
    reader._dataFingerprint = null

    // "zeta" is unknown to the OLD in-memory TermManager. score() must refresh
    // offsets + index + term mapping so the new term resolves to the new record.
    const res = await reader.score('tags', { zeta: 1 })
    expect(res.length).toBe(1)
    expect(res[0].id).toBe('10')

    // Term mapping was reloaded: stored term IDs restored to words
    const tags = Array.isArray(res[0].tags) ? res[0].tags : []
    expect(tags).toContain('zeta')

    await closeDb(reader)
  })
})
