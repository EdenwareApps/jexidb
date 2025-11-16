import { Database } from '../src/Database.mjs'
import fs from 'fs'

describe('Term mapping index line number regression', () => {
  let testDbPath
  let testIdxPath

  beforeEach(() => {
    const uniqueSuffix = `${Date.now()}-${Math.random().toString(36).slice(2)}`
    testDbPath = `test-line-number-regression-${uniqueSuffix}.jdb`
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

  test('should index new term mapping values appended after reload', async () => {
    // Seed database with a first record and persist it to disk
    const initialDb = new Database(testDbPath, {
      indexes: { nameTerms: 'array:string' },
      debugMode: false
    })
    await initialDb.init()
    await initialDb.insert({
      id: '1',
      name: 'CANAL DO CLIENTE HD',
      nameTerms: ['cliente', 'canal']
    })
    await initialDb.save()
    await initialDb.close()

    // Reopen the same database and append a new record with different terms
    const db = new Database(testDbPath, {
      indexes: { nameTerms: 'array:string' },
      debugMode: false
    })
    await db.init()

    const firstQuery = await db.find({ nameTerms: 'cliente' })
    expect(firstQuery.map(record => record.id)).toEqual(['1'])

    await db.insert({
      id: '2',
      name: 'Telecine Fun HD',
      nameTerms: ['telecine', 'fun']
    })
    await db.save()

    const telecineResults = await db.find({ nameTerms: 'telecine' })
    const clienteResults = await db.find({ nameTerms: 'cliente' })

    expect(telecineResults.map(record => record.id)).toEqual(['2'])
    expect(clienteResults.map(record => record.id)).toEqual(['1'])

    await db.destroy()
  })

  test('should remove old term mapping entries when record is updated', async () => {
    const db = new Database(testDbPath, {
      indexes: { nameTerms: 'array:string' },
      debugMode: false
    })
    await db.init()

    await db.insert({
      id: '1',
      name: 'CANAL DO CLIENTE HD',
      nameTerms: ['cliente', 'canal']
    })
    await db.save()

    await db.update({ id: '1' }, {
      name: 'Telecine Fun HD',
      nameTerms: ['telecine', 'fun']
    })

    await db.save()

    const telecineResults = await db.find({ nameTerms: 'telecine' })
    const clienteResults = await db.find({ nameTerms: 'cliente' })
    const combinedResults = await db.find({ nameTerms: { $all: ['telecine', 'fun'] } })

    expect(telecineResults.map(record => record.id)).toEqual(['1'])
    expect(clienteResults.map(record => record.id)).toEqual([])
    expect(combinedResults.map(record => record.id)).toEqual(['1'])

    await db.destroy()
  })

})

