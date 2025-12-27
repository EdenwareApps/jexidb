import { Database } from '../src/Database.mjs'
import fs from 'fs'
import path from 'path'

describe('Delete Persistence Bug Test', () => {
  let testDir
  let db

  beforeEach(() => {
    testDir = path.join(process.cwd(), 'test-files', 'delete-persistence-bug')
    fs.mkdirSync(testDir, { recursive: true })
  })

  afterEach(async () => {
    if (db && !db.destroyed) {
      await db.waitForOperations()
      await db.close()
    }
    // Clean up test files
    if (fs.existsSync(testDir)) {
      try {
        fs.rmSync(testDir, { recursive: true, force: true })
      } catch (error) {
        console.warn('Could not clean up test directory:', testDir)
      }
    }
  })

  test('delete() should remove records from find() after save()', async () => {
    const dbPath = path.join(testDir, 'delete-test.jdb')
    
    const config = {
      fields: {
        id: 'string',
        user_id: 'string',
        email: 'string',
        name: 'string',
        status: 'string'
      },
      indexes: {
        user_id: 'string',
        email: 'string'
      },
      allowIndexRebuild: true,
      debugMode: true
    }
    
    db = new Database(dbPath, config)
    await db.init()
    
    // Insert test record
    const testRecord = {
      id: 'test-record-123',
      user_id: 'user-abc-456',
      email: 'test@example.com',
      name: 'Test User',
      status: 'active'
    }
    
    console.log('1. Inserting test record...')
    await db.insert(testRecord)
    await db.save()
    
    // Verify insertion
    const afterInsert = await db.find({})
    expect(afterInsert.length).toBe(1)
    expect(afterInsert[0].email).toBe('test@example.com')
    console.log(`   Records in database: ${afterInsert.length}`)
    
    // Attempt deletion by id
    console.log('\n2. Attempting to delete by id field...')
    const deleteResult1 = await db.delete({ id: testRecord.id })
    console.log(`   delete() returned: ${JSON.stringify(deleteResult1)}`)
    expect(deleteResult1.length).toBeGreaterThan(0)
    
    await db.save()
    
    const afterDelete1 = await db.find({})
    console.log(`   Records after delete: ${afterDelete1.length}`)
    console.log(`   Record still exists: ${afterDelete1.length > 0 ? 'YES ❌' : 'NO ✅'}`)
    
    // CRITICAL: The record should NOT be found after delete + save
    expect(afterDelete1.length).toBe(0)
    
    if (afterDelete1.length > 0) {
      console.log(`   Found record: ${afterDelete1[0].email}`)
      console.log(`   BUG CONFIRMED: Record still exists after delete + save`)
    }
  })

  test('delete() should work with indexed fields', async () => {
    const dbPath = path.join(testDir, 'delete-indexed-test.jdb')
    
    const config = {
      fields: {
        id: 'string',
        user_id: 'string',
        email: 'string',
        name: 'string'
      },
      indexes: {
        user_id: 'string',
        email: 'string'
      },
      allowIndexRebuild: true
    }
    
    db = new Database(dbPath, config)
    await db.init()
    
    // Insert test record
    const testRecord = {
      id: 'test-record-456',
      user_id: 'user-xyz-789',
      email: 'test2@example.com',
      name: 'Test User 2'
    }
    
    await db.insert(testRecord)
    await db.save()
    
    // Verify insertion
    const afterInsert = await db.find({})
    expect(afterInsert.length).toBe(1)
    
    // Delete by indexed field
    const deleteResult = await db.delete({ user_id: testRecord.user_id })
    expect(deleteResult.length).toBe(1)
    
    await db.save()
    
    // Verify deletion
    const afterDelete = await db.find({})
    expect(afterDelete.length).toBe(0)
    
    // Also verify by email (another indexed field)
    const afterDeleteByEmail = await db.find({ email: testRecord.email })
    expect(afterDeleteByEmail.length).toBe(0)
  })

  test('delete() should persist after database close and reopen', async () => {
    const dbPath = path.join(testDir, 'delete-persistence-test.jdb')
    
    const config = {
      fields: {
        id: 'string',
        name: 'string',
        email: 'string'
      },
      indexes: {
        email: 'string'
      },
      allowIndexRebuild: true
    }
    
    // First session: insert and delete
    db = new Database(dbPath, { ...config, clear: true, create: true })
    await db.init()
    
    const testRecord = {
      id: 'persist-test-123',
      name: 'Persist Test',
      email: 'persist@test.com'
    }
    
    await db.insert(testRecord)
    await db.save()
    
    const deleteResult = await db.delete({ id: testRecord.id })
    expect(deleteResult.length).toBe(1)
    
    await db.save()
    await db.close()
    
    // Second session: verify deletion persisted
    db = new Database(dbPath, config)
    await db.init()
    
    const afterReopen = await db.find({})
    expect(afterReopen.length).toBe(0)
    
    const foundById = await db.find({ id: testRecord.id })
    expect(foundById.length).toBe(0)
    
    const foundByEmail = await db.find({ email: testRecord.email })
    expect(foundByEmail.length).toBe(0)
  })

  test('delete() should work with multiple records', async () => {
    const dbPath = path.join(testDir, 'delete-multiple-test.jdb')
    
    const config = {
      fields: {
        id: 'string',
        name: 'string',
        category: 'string'
      },
      allowIndexRebuild: true
    }
    
    db = new Database(dbPath, { ...config, clear: true, create: true })
    await db.init()
    
    // Insert multiple records
    const records = [
      { id: 'r1', name: 'Record 1', category: 'A' },
      { id: 'r2', name: 'Record 2', category: 'A' },
      { id: 'r3', name: 'Record 3', category: 'B' },
      { id: 'r4', name: 'Record 4', category: 'B' }
    ]
    
    for (const record of records) {
      await db.insert(record)
    }
    await db.save()
    
    expect((await db.find({})).length).toBe(4)
    
    // Delete by category
    const deleteResult = await db.delete({ category: 'A' })
    expect(deleteResult.length).toBe(2)
    
    await db.save()
    
    const afterDelete = await db.find({})
    expect(afterDelete.length).toBe(2)
    
    const categoryA = await db.find({ category: 'A' })
    expect(categoryA.length).toBe(0)
    
    const categoryB = await db.find({ category: 'B' })
    expect(categoryB.length).toBe(2)
  })
})
