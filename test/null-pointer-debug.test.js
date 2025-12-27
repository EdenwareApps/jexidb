import { Database } from '../src/Database.mjs'
import fs from 'fs'
import path from 'path'

describe('Null Pointer Debug Test', () => {
  let testDir
  let db

  beforeEach(() => {
    testDir = path.join(process.cwd(), 'test-files', 'null-pointer-debug')
    fs.mkdirSync(testDir, { recursive: true })
  })

  afterEach(async () => {
    if (db && !db.destroyed) {
      await db.waitForOperations()
      await db.close()
    }
    if (fs.existsSync(testDir)) {
      try {
        fs.rmSync(testDir, { recursive: true, force: true })
      } catch (error) {
        console.warn('Could not clean up test directory:', testDir)
      }
    }
  })

  test('test null pointer exception with $in queries', async () => {
    const dbPath = path.join(testDir, 'null-pointer-test.jdb')

    db = new Database(dbPath, {
      fields: {
        id: 'string',
        tags: 'string', // This could be an array field
        category: 'string'
      },
      indexes: {
        tags: 'string',
        category: 'string'
      },
      clear: true,
      create: true,
      debugMode: true
    })
    await db.init()

    // Insert some test records
    const records = [
      { id: '1', tags: 'tag1', category: 'A' },
      { id: '2', tags: 'tag2', category: 'B' },
      { id: '3', tags: 'tag3', category: 'A' }
    ]

    for (const record of records) {
      await db.insert(record)
    }
    await db.save()

    console.log('Testing various query patterns that might cause null pointer...')

    try {
      // Test 1: Normal query
      console.log('Test 1: Normal query')
      const result1 = await db.find({ category: 'A' })
      console.log('Result:', result1.length, 'records')

      // Test 2: Query with null value
      console.log('Test 2: Query with null value')
      const result2 = await db.find({ category: null })
      console.log('Result:', result2.length, 'records')

      // Test 3: Query with undefined value
      console.log('Test 3: Query with undefined value')
      const result3 = await db.find({ category: undefined })
      console.log('Result:', result3.length, 'records')

      // Test 4: Query with object that has null
      console.log('Test 4: Query with object containing null')
      const result4 = await db.find({ tags: { $in: null } })
      console.log('Result:', result4.length, 'records')

      // Test 5: Query with empty array
      console.log('Test 5: Query with empty $in array')
      const result5 = await db.find({ tags: { $in: [] } })
      console.log('Result:', result5.length, 'records')

      // Test 6: Query with null in $in array
      console.log('Test 6: Query with null in $in array')
      const result6 = await db.find({ tags: { $in: [null] } })
      console.log('Result:', result6.length, 'records')

    } catch (error) {
      console.error('Error occurred:', error.message)
      console.error('Stack trace:', error.stack)
      throw error
    }

    expect(true).toBe(true) // If we get here, no null pointer exceptions
  })
})




