import { Database } from '../src/Database.mjs'
import fs from 'fs'
import path from 'path'

describe('Delete Persistence Across Restarts Test', () => {
  let testDir
  let db

  beforeEach(() => {
    testDir = path.join(process.cwd(), 'test-files', 'delete-persistence-restart')
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

  test('delete() should persist across database restarts with memory-efficient operations', async () => {
    const dbPath = path.join(testDir, 'restart-test.jdb')

    // First session: insert and delete records with memory-efficient operations
    db = new Database(dbPath, {
      fields: {
        name: 'string',
        email: 'string'
      },
      indexes: {
        email: 'string'
      },
      clear: true,
      create: true
    })
    await db.init()

    // Insert records in small batches to manage memory
    const totalRecords = 50

    for (let i = 0; i < totalRecords; i++) {
      await db.insert({
        name: `User ${i}`,
        email: `user${i}@test.com`
      })

      // Save every 10 records to avoid large memory buffers
      if (i % 10 === 9) {
        await db.save()
        if (global.gc) global.gc()
      }
    }
    await db.save() // Final save

    console.log(`Inserted ${totalRecords} records`)

    // Verify insertion worked
    const beforeDelete = await db.find({})
    expect(beforeDelete.length).toBe(totalRecords)

    // Delete every 5th record
    const deleteIds = []
    for (let i = 0; i < totalRecords; i += 5) {
      deleteIds.push(beforeDelete[i].id)
    }

    for (const id of deleteIds) {
      const result = await db.delete({ id })
      expect(result.length).toBe(1)
    }
    await db.save()

    console.log(`Deleted ${deleteIds.length} records`)

    // Verify deletions
    const afterDelete = await db.find({})
    expect(afterDelete.length).toBe(totalRecords - deleteIds.length)

    await db.close()

    // Second session: verify persistence
    console.log('\n=== Second session (restart) ===')
    db = new Database(dbPath, {
      fields: {
        name: 'string',
        email: 'string'
      },
      indexes: {
        email: 'string'
      },
      create: true
    })
    await db.init()

    // Verify persistence
    const afterRestart = await db.find({})
    expect(afterRestart.length).toBe(totalRecords - deleteIds.length)

    // Verify specific records that should exist
    const sampleRecord = afterRestart[0]
    expect(sampleRecord).toBeDefined()
    expect(sampleRecord.name).toMatch(/^User \d+$/)
    expect(sampleRecord.email).toMatch(/^user\d+@test\.com$/)

    console.log(`Memory-efficient delete persistence test completed successfully`)
  })
})