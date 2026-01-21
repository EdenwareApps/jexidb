import fs from 'fs'
import path from 'path'
import { Database } from '../src/Database.mjs'

const cleanUp = (filePath) => {
  try {
    if (fs.existsSync(filePath)) {
      fs.unlinkSync(filePath)
    }
  } catch (error) {
    // Ignore cleanup errors
  }
}

describe('Coverage Method Tests', () => {
  let db
  let testDbPath
  let testIdxPath

  beforeEach(async () => {
    const testId = Math.random().toString(36).substring(2, 10)
    testDbPath = path.join(process.cwd(), `test-coverage-${testId}.jdb`)
    testIdxPath = path.join(process.cwd(), `test-coverage-${testId}.idx.jdb`)

    cleanUp(testDbPath)
    cleanUp(testIdxPath)

    db = new Database(testDbPath, {
      fields: {
        name: 'string',
        nameTerms: 'array:string',
        genre: 'string',
        mediaType: 'string'
      },
      indexes: {
        nameTerms: 'array:string',
        genre: 'string',
        mediaType: 'string'
      }
    })
    await db.init()
  })

  afterEach(async () => {
    if (db) {
      await db.close()
      db = null
    }

    cleanUp(testDbPath)
    cleanUp(testIdxPath)
  })

  describe('Grouped Coverage', () => {
    test('calculates percentage with include and exclude groups', async () => {
      await db.insert({ id: 1, title: 'Rede Brasil', nameTerms: ['rede', 'brasil', 'sul'] })
      await db.insert({ id: 2, title: 'Band SP', nameTerms: ['band', 'sp'] })
      await db.save()

      const coverage = await db.coverage('nameTerms', [
        { terms: ['sbt'], excludes: [] },
        { terms: ['rede', 'brasil'], excludes: ['norte'] },
        { terms: ['tv', 'sancouper'], excludes: [] },
        { terms: ['band'] }
      ])

      expect(coverage).toBeCloseTo(50)
    })

    test('applies excludes before counting matches', async () => {
      await db.insert({ id: 1, title: 'Rede Norte', nameTerms: ['rede', 'brasil', 'norte'] })
      await db.insert({ id: 2, title: 'Rede Sul', nameTerms: ['rede', 'sul'] })
      await db.save()

      const coverage = await db.coverage('nameTerms', [
        { terms: ['rede', 'brasil'], excludes: ['norte'] },
        { terms: ['rede'] }
      ])

      expect(coverage).toBeCloseTo(50)
    })

    test('works with string indexed field and optional excludes', async () => {
      await db.insert({ id: 1, title: 'Song A', genre: 'samba' })
      await db.insert({ id: 2, title: 'Song B', genre: 'pagode' })
      await db.insert({ id: 3, title: 'Song C', genre: 'rock' })
      await db.save()

      const coverage = await db.coverage('genre', [
        { terms: ['samba'] },
        { terms: ['pagode'], excludes: [] },
        { terms: ['rock'], excludes: ['rock'] },
        { terms: ['funk'] }
      ])

      expect(coverage).toBeCloseTo(50)
    })

    test('validates that filter criteria only use indexed fields', async () => {
      await db.insert({ id: 1, title: 'Test', nameTerms: ['test'], genre: 'rock', mediaType: 'live' })
      await db.save()

      // Should throw error for non-indexed field
      await expect(db.coverage('nameTerms', [{ terms: ['test'] }], { nonIndexedField: 'value' }))
        .rejects.toThrow('Filter field "nonIndexedField" must be indexed')
    })

    test('filters coverage calculation with indexed field criteria', async () => {
      await db.insert({ id: 1, title: 'Live Show', nameTerms: ['show', 'live'], genre: 'rock', mediaType: 'live' })
      await db.insert({ id: 2, title: 'VOD Movie', nameTerms: ['movie', 'vod'], genre: 'drama', mediaType: 'vod' })
      await db.insert({ id: 3, title: 'Live Concert', nameTerms: ['concert', 'live'], genre: 'rock', mediaType: 'live' })
      await db.save()

      // Without filter - should match 100% for 'live' term
      const coverageAll = await db.coverage('nameTerms', [
        { terms: ['live'] }
      ])
      expect(coverageAll).toBe(100)

      // With filter for live media only - should still match 100%
      const coverageLive = await db.coverage('nameTerms', [
        { terms: ['live'] }
      ], { mediaType: 'live' })
      expect(coverageLive).toBe(100)

      // With filter for vod media only - should match 0% for 'live' term
      const coverageVod = await db.coverage('nameTerms', [
        { terms: ['live'] }
      ], { mediaType: 'vod' })
      expect(coverageVod).toBe(0)
    })

    test('supports array values in filter criteria for OR matching', async () => {
      await db.insert({ id: 1, title: 'Live Show', nameTerms: ['show'], genre: 'rock', mediaType: 'live' })
      await db.insert({ id: 2, title: 'VOD Movie', nameTerms: ['movie'], genre: 'drama', mediaType: 'vod' })
      await db.insert({ id: 3, title: 'Premium Show', nameTerms: ['show'], genre: 'drama', mediaType: 'premium' })
      await db.save()

      // Filter for both 'live' and 'premium' media types
      const coverage = await db.coverage('nameTerms', [
        { terms: ['show'] }
      ], { mediaType: ['live', 'premium'] })

      // Should match 2 out of 2 records with 'show' term that have live or premium mediaType
      expect(coverage).toBe(100)
    })

    test('combines multiple filter criteria with AND logic', async () => {
      await db.insert({ id: 1, title: 'Rock Live', nameTerms: ['rock'], genre: 'rock', mediaType: 'live' })
      await db.insert({ id: 2, title: 'Rock VOD', nameTerms: ['rock'], genre: 'rock', mediaType: 'vod' })
      await db.insert({ id: 3, title: 'Pop Live', nameTerms: ['pop'], genre: 'pop', mediaType: 'live' })
      await db.save()

      // Filter for both genre='rock' AND mediaType='live'
      const coverage = await db.coverage('nameTerms', [
        { terms: ['rock'] }
      ], { genre: 'rock', mediaType: 'live' })

      // Should match only the first record
      expect(coverage).toBe(100)
    })

    test('returns 0 when filter matches no records', async () => {
      await db.insert({ id: 1, title: 'Test', nameTerms: ['test'], genre: 'rock', mediaType: 'live' })
      await db.save()

      // Filter for non-existent media type
      const coverage = await db.coverage('nameTerms', [
        { terms: ['test'] }
      ], { mediaType: 'nonexistent' })

      expect(coverage).toBe(0)
    })
  })
})

