import { Database } from '../src/Database.mjs'
import fs from 'fs'

describe('Indexed query fallback when index data missing', () => {
  let testDbPath
  let testIdxPath

  beforeEach(() => {
    const uniqueSuffix = `${Date.now()}-${Math.random().toString(36).slice(2)}`
    testDbPath = `test-missing-index-${uniqueSuffix}.jdb`
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

  test('should fall back to streaming when indexed field data is missing', async () => {
    const db = new Database(testDbPath, {
      indexes: {
        channel: 'string',
        start: 'number',
        end: 'number'
      },
      debugMode: false
    })
    await db.init()

    await db.insert({
      id: '1',
      channel: 'Sample Channel',
      start: 1000,
      end: 2000
    })

    await db.insert({
      id: '2',
      channel: 'Another Channel',
      start: 3000,
      end: 4000
    })

    await db.save()
    await db.close()

    const idxContentRaw = fs.readFileSync(testIdxPath, 'utf8')
    const idxContent = JSON.parse(idxContentRaw)

    if (!idxContent.index) {
      idxContent.index = {}
    }
    if (!idxContent.index.data) {
      idxContent.index.data = {}
    }

    // Simulate missing index data for channel field while keeping other index data intact
    idxContent.index.data.channel = {}

    fs.writeFileSync(testIdxPath, JSON.stringify(idxContent, null, 2), 'utf8')

    const reopenedDb = new Database(testDbPath, {
      create: false,
      indexes: {
        channel: 'string',
        start: 'number',
        end: 'number'
      },
      allowIndexRebuild: true, // Enable rebuild for partial index corruption
      debugMode: false
    })

    await reopenedDb.init()

    const count = await reopenedDb.count({ channel: 'Sample Channel' })
    expect(count).toBe(1)

    const results = await reopenedDb.find({ channel: 'Sample Channel' })
    expect(results.map(record => record.id)).toEqual(['1'])

    await reopenedDb.destroy()
  })
})

