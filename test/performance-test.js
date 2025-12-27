import { Database } from '../src/Database.mjs'
import fs from 'fs'
import path from 'path'

describe('Performance Test - Corrections Impact', () => {
  let testDir
  let db

  beforeEach(() => {
    testDir = path.join(process.cwd(), 'test-files', 'performance-test')
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

  test('measure find() performance after corrections', async () => {
    const dbPath = path.join(testDir, 'perf-test.jdb')

    db = new Database(dbPath, {
      fields: {
        id: 'string',
        name: 'string',
        value: 'number',
        category: 'string'
      },
      indexes: {
        category: 'string'
      },
      clear: true,
      create: true
    })
    await db.init()

    // Insert 1000 records
    console.log('Inserting 1000 records...')
    const records = []
    for (let i = 0; i < 1000; i++) {
      records.push({
        id: `record-${i}`,
        name: `Name ${i}`,
        value: Math.random() * 1000,
        category: `cat-${i % 10}`
      })
    }

    await db.insertBatch(records)
    await db.save()

    console.log('Testing find() performance...')

    // Test 1: Find all records
    const start1 = Date.now()
    const allRecords = await db.find({})
    const time1 = Date.now() - start1
    console.log(`Find all (${allRecords.length} records): ${time1}ms`)

    // Test 2: Find with criteria
    const start2 = Date.now()
    const filteredRecords = await db.find({ category: 'cat-5' })
    const time2 = Date.now() - start2
    console.log(`Find filtered (${filteredRecords.length} records): ${time2}ms`)

    // Test 3: Multiple finds to test cache consistency
    const start3 = Date.now()
    for (let i = 0; i < 10; i++) {
      await db.find({ category: `cat-${i % 10}` })
    }
    const time3 = Date.now() - start3
    console.log(`10 consecutive filtered finds: ${time3}ms`)

    // Update some records and test find again
    console.log('Testing update + find consistency...')
    const updateStart = Date.now()
    const recordToUpdate = allRecords[0]
    recordToUpdate.value = 9999
    await db.update(recordToUpdate)
    await db.save()
    const updateTime = Date.now() - updateStart
    console.log(`Update + save: ${updateTime}ms`)

    // Verify the update is reflected
    const verifyStart = Date.now()
    const updatedRecord = await db.findOne({ id: recordToUpdate.id })
    const verifyTime = Date.now() - verifyStart
    console.log(`Find updated record: ${verifyTime}ms, value: ${updatedRecord?.value}`)

    expect(updatedRecord.value).toBe(9999)
    expect(time1).toBeLessThan(500) // Should be reasonably fast
    expect(time2).toBeLessThan(200) // Should be fast with index
  })
})




