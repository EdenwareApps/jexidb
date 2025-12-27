import { Database } from '../src/Database.mjs'
import fs from 'fs'
import path from 'path'

describe('Update Cache Debug Test', () => {
  let testDir
  let db

  beforeEach(() => {
    testDir = path.join(process.cwd(), 'test-files', 'update-cache-debug')
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

  test('debug update cache issue', async () => {
    const dbPath = path.join(testDir, 'update-cache-test.jdb')

    db = new Database(dbPath, {
      fields: {
        id: 'string',
        name: 'string',
        value: 'number'
      },
      clear: true,
      create: true,
      debugMode: true
    })
    await db.init()

    // Insert initial record
    const initialRecord = { id: 'test-1', name: 'Test', value: 100 }
    console.log('1. Inserting initial record:', initialRecord)

    await db.insert(initialRecord)
    await db.save()

    // Read back
    let records = await db.find({})
    console.log('After insert - records:', records.length, 'value:', records[0].value)
    console.log('writeBuffer length:', db.writeBuffer.length)

    // Update the record
    console.log('\n2. Updating record...')
    const recordToUpdate = records[0]
    recordToUpdate.value = 200
    console.log('Updating value from 100 to 200')
    console.log('Record to update:', { id: recordToUpdate.id, value: recordToUpdate.value })

    await db.update({ id: recordToUpdate.id }, { value: 200 })
    console.log('After update - writeBuffer length:', db.writeBuffer.length)
    if (db.writeBuffer.length > 0) {
      console.log('WriteBuffer content:', db.writeBuffer.map(r => ({ id: r.id, value: r.value })))
    }

    // Read immediately after update (before save)
    records = await db.find({})
    console.log('After update (before save) - records:', records.length, 'value:', records[0].value)

    // Save the changes
    console.log('\n3. Saving changes...')
    await db.save()
    console.log('After save - writeBuffer length:', db.writeBuffer.length)

    // Read after save
    records = await db.find({})
    console.log('After save - records:', records.length, 'value:', records[0].value)

    // Check file content
    if (fs.existsSync(dbPath)) {
      const fileContent = fs.readFileSync(dbPath, 'utf8')
      console.log('File content:', fileContent.trim())
    }

    expect(records[0].value).toBe(200)
  })

  test('test multiple updates scenario', async () => {
    const dbPath = path.join(testDir, 'multiple-updates.jdb')

    db = new Database(dbPath, {
      fields: {
        id: 'string',
        counter: 'number'
      },
      clear: true,
      create: true,
      debugMode: true
    })
    await db.init()

    // Insert record
    await db.insert({ id: 'counter', counter: 0 })
    await db.save()

    // Multiple updates
    for (let i = 1; i <= 5; i++) {
      await db.update({ id: 'counter' }, { counter: i })
      await db.save()

      const updated = await db.findOne({ id: 'counter' })
      console.log(`Update ${i}: expected ${i}, got ${updated.counter}`)
      expect(updated.counter).toBe(i)
    }
  })
})



