import { Database } from '../src/Database.mjs'
import fs from 'fs'

describe('Explicit indexes with comparison operators', () => {
  let testDbPath
  let testIdxPath

  beforeEach(() => {
    const uniqueSuffix = `${Date.now()}-${Math.random().toString(36).slice(2)}`
    testDbPath = `test-explicit-index-${uniqueSuffix}.jdb`
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

  test('should reuse persisted index configuration when explicit indexes array is provided', async () => {
    const seedDb = new Database(testDbPath, {
      fields: { id: 'string', channel: 'string', end: 'number', start: 'number', terms: 'array:string' },
      indexes: {
        channel: 'string',
        end: 'number',
        start: 'number',
        terms: 'array:string'
      },
      debugMode: false
    })
    await seedDb.init()

    const now = Math.floor(Date.now() / 1000)

    await seedDb.insert({
      id: '1',
      channel: 'Example Channel',
      end: now + 120
    })

    await seedDb.insert({
      id: '2',
      channel: 'Example Channel',
      end: now - 120
    })

    await seedDb.insert({
      id: '3',
      channel: 'Other Channel',
      start: now + 1800,
      end: now + 3600,
      terms: ['other']
    })

    await seedDb.save()
    await seedDb.close()

    const reopenedDb = new Database(testDbPath, {fields: {"channel":"string","end":"string"},
      
      create: false,
      indexes: ['channel', 'end'],
      debugMode: false
    })

    await reopenedDb.init()

    const count = await reopenedDb.count({
      channel: 'Example Channel',
      end: { $gt: now }
    })
    expect(count).toBe(1)

    const results = await reopenedDb.find({
      channel: 'Example Channel',
      end: { $gt: now }
    })
    expect(results.map(record => record.id)).toEqual(['1'])

    await reopenedDb.destroy()
  })

  test('should return results for equality mixed with comparison operators after reopening with persisted indexes', async () => {
    const seedDb = new Database(testDbPath, {
      fields: { id: 'string', channel: 'string', start: 'number', end: 'number', terms: 'array:string' },
      indexes: {
        channel: 'string',
        start: 'number',
        end: 'number',
        terms: 'array:string'
      },
      indexedQueryMode: 'permissive',
      debugMode: false
    })
    await seedDb.init()

    const now = Math.floor(Date.now() / 1000)

    await seedDb.insert({
      id: 'future-1',
      channel: 'São Paulo|SP  Disney Channel',
      start: now + 600,
      end: now + 3600,
      terms: ['disney', 'kids']
    })
    await seedDb.insert({
      id: 'past-1',
      channel: 'São Paulo|SP  Disney Channel',
      start: now - 3600,
      end: now - 300,
      terms: ['disney', 'classic']
    })
    await seedDb.insert({
      id: 'present-1',
      channel: 'São Paulo|SP  Disney Channel',
      start: now - 60,
      end: now + 60,
      terms: ['disney', 'now']
    })
    await seedDb.insert({
      id: 'other-1',
      channel: 'Other Channel',
      start: now + 600,
      end: now + 3600,
      terms: ['other']
    })

    await seedDb.save()
    await seedDb.close()

    const reopenedDb = new Database(testDbPath, {
      create: false,
      fields: { id: 'string', channel: 'string', start: 'number', end: 'number', terms: 'array:string' },
      indexes: {
        channel: 'string',
        start: 'number',
        end: 'number',
        terms: 'array:string'
      },
      indexedQueryMode: 'permissive',
      debugMode: false
    })

    await reopenedDb.init()

    // Simulate regression: channel index line numbers loaded as strings while numeric index stays numeric
    const reopenedChannelIndex = reopenedDb.indexManager.index.data.channel
    for (const term in reopenedChannelIndex) {
      const entry = reopenedChannelIndex[term]
      if (entry?.set instanceof Set) {
        reopenedChannelIndex[term].set = new Set(Array.from(entry.set).map(value => String(value)))
      }
    }

    const disneyChannel = 'São Paulo|SP  Disney Channel'

    const equalityCount = await reopenedDb.count({ channel: disneyChannel })
    expect(equalityCount).toBe(3)

    // Ensure persisted channel index still contains stringified line numbers
    const storedChannelLineTypes = Object.values(reopenedChannelIndex).flatMap(entry => {
      if (entry?.set instanceof Set) {
        return Array.from(entry.set).map(value => typeof value)
      }
      return []
    })
    expect(storedChannelLineTypes.every(type => type === 'string')).toBe(true)

    // Normalization should convert returned line numbers to numeric form
    const channelLineTypes = Array.from(reopenedDb.indexManager.query({ channel: disneyChannel })).map(value => typeof value)
    expect(channelLineTypes.every(type => type === 'number')).toBe(true)

    const endLineTypes = Array.from(reopenedDb.indexManager.query({ end: { $gt: now } })).map(value => typeof value)
    expect(endLineTypes.every(type => type === 'number')).toBe(true)

    const operators = [
      { criteria: { channel: disneyChannel, end: { $gt: now } }, expectedIds: ['future-1', 'present-1'] },
      { criteria: { channel: disneyChannel, end: { '>': now } }, expectedIds: ['future-1', 'present-1'] },
      { criteria: { channel: disneyChannel, end: { $gte: now } }, expectedIds: ['future-1', 'present-1'] },
      { criteria: { channel: disneyChannel, end: { $lt: now } }, expectedIds: ['past-1'] },
      { criteria: { channel: disneyChannel, end: { '<': now } }, expectedIds: ['past-1'] },
      { criteria: { channel: disneyChannel, end: { $lte: now } }, expectedIds: ['past-1'] },
      { criteria: { channel: disneyChannel, end: { '<=': now } }, expectedIds: ['past-1'] },
      { criteria: { channel: disneyChannel, start: { $lte: now } }, expectedIds: ['past-1', 'present-1'] },
      { criteria: { channel: disneyChannel, start: { '<': now } }, expectedIds: ['past-1', 'present-1'] }
    ]

    for (const { criteria, expectedIds } of operators) {
      const count = await reopenedDb.count(criteria)
      expect(count).toBe(expectedIds.length)

      const results = await reopenedDb.find(criteria)
      expect(results.map(record => record.id).sort()).toEqual([...expectedIds].sort())
    }

    const termsComparisonCount = await reopenedDb.count({
      terms: { $in: ['disney'] },
      end: { $gt: now }
    })
    expect(termsComparisonCount).toBe(2)

    const termsComparisonIds = (await reopenedDb.find({
      terms: { $in: ['disney'] },
      end: { $gt: now }
    })).map(record => record.id).sort()
    expect(termsComparisonIds).toEqual(['future-1', 'present-1'])

    const integrationCheckCount = await reopenedDb.count({
      channel: disneyChannel,
      end: { $gt: now },
      start: { $lte: now + 3600 }
    })
    expect(integrationCheckCount).toBe(2)

    await reopenedDb.destroy()
  })
})

