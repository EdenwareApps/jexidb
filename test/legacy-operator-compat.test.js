import { Database } from '../src/Database.mjs'
import fs from 'fs'

describe('Legacy operator compatibility', () => {
  let testDbPath
  let testIdxPath

  beforeEach(() => {
    const uniqueSuffix = `${Date.now()}-${Math.random().toString(36).slice(2)}`
    testDbPath = `test-legacy-operators-${uniqueSuffix}.jdb`
    testIdxPath = testDbPath.replace('.jdb', '.idx.jdb')
  })

  afterEach(() => {
    for (const filePath of [testDbPath, testIdxPath]) {
      if (filePath && fs.existsSync(filePath)) {
        try {
          fs.unlinkSync(filePath)
        } catch (error) {
          console.warn(`⚠️  Failed to delete ${filePath}: ${error.message}`)
        }
      }
    }
  })

  test('should support string comparison operators for find/count', async () => {
    const db = new Database(testDbPath, {
      indexes: {
        start: 'number',
        end: 'number'
      },
      termMapping: true,
      debugMode: false
    })
    await db.init()

    const now = Math.floor(Date.now() / 1000)

    await db.insert({
      id: '1',
      start: now - 60,
      end: now + 60
    })

    await db.insert({
      id: '2',
      start: now - 3600,
      end: now - 300
    })

    const legacyCount = await db.count({ end: { '>': now } })
    const mongoCount = await db.count({ end: { $gt: now } })
    expect(legacyCount).toBe(1)
    expect(legacyCount).toBe(mongoCount)

    const legacyResults = await db.find({
      start: { '<=': now },
      end: { '>': now }
    })
    const canonicalResults = await db.find({
      start: { $lte: now },
      end: { $gt: now }
    })
    const mongoResults = await db.find({
      start: { $lte: now },
      end: { $gt: now }
    })

    expect(legacyResults.map(record => record.id)).toEqual(['1'])
    expect(canonicalResults.map(record => record.id)).toEqual(['1'])
    expect(mongoResults.map(record => record.id)).toEqual(['1'])

    await db.save()
    await db.destroy()
  })

  test('should support string inequality operator', async () => {
    const db = new Database(testDbPath, {
      indexes: {
        end: 'number'
      },
      termMapping: true,
      debugMode: false
    })
    await db.init()

    const now = Math.floor(Date.now() / 1000)

    await db.insert({
      id: 'A',
      end: now + 120
    })

    await db.insert({
      id: 'B',
      end: now + 240
    })

    const legacyResults = await db.find({
      end: { '!=': now + 120 }
    })

    expect(legacyResults.map(record => record.id)).toEqual(['B'])

    await db.save()
    await db.destroy()
  })

  test('should support mongo-style comparison operators when using indexes', async () => {
    const db = new Database(testDbPath, {
      indexes: {
        numericField: 'number'
      },
      debugMode: false
    })
    await db.init()

    await db.insert({ id: '10', numericField: 100 })
    await db.insert({ id: '20', numericField: 200 })

    await db.save()
    await db.close()

    const reopenedDb = new Database(testDbPath, {
      create: false,
      indexes: {
        numericField: 'number'
      },
      debugMode: false
    })
    await reopenedDb.init()

    const greaterThanResults = await reopenedDb.find({ numericField: { $gt: 150 } })
    expect(greaterThanResults.map(record => record.id)).toEqual(['20'])

    const greaterOrEqualResults = await reopenedDb.find({ numericField: { $gte: 200 } })
    expect(greaterOrEqualResults.map(record => record.id)).toEqual(['20'])

    const lessThanResults = await reopenedDb.find({ numericField: { $lt: 150 } })
    expect(lessThanResults.map(record => record.id)).toEqual(['10'])

    const lessOrEqualResults = await reopenedDb.find({ numericField: { $lte: 100 } })
    expect(lessOrEqualResults.map(record => record.id)).toEqual(['10'])

    const notEqualResults = await reopenedDb.find({ numericField: { $ne: 100 } })
    expect(notEqualResults.map(record => record.id)).toEqual(['20'])

    const countResults = await reopenedDb.count({ numericField: { $gt: 50, $lt: 250 } })
    expect(countResults).toBe(2)

    await reopenedDb.destroy()
  })
})

