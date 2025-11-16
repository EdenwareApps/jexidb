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
      indexes: {
        nameTerms: 'array:string',
        genre: 'string'
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
  })
})

