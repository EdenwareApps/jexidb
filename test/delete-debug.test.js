import { Database } from '../src/Database.mjs'
import fs from 'fs'
import path from 'path'

describe('Delete Debug Test', () => {
  let testDir
  let db

  beforeEach(() => {
    testDir = path.join(process.cwd(), 'test-files', 'delete-debug')
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

  test('debug delete issue', async () => {
    const dbPath = path.join(testDir, 'debug.jdb')
    
    db = new Database(dbPath, { 
      fields: { id: 'string', name: 'string' },
      clear: true, 
      create: true,
      debugMode: true
    })
    await db.init()
    
    // Insert record
    const record = await db.insert({ id: 'test-1', name: 'Test' })
    await db.save()
    
    console.log('\n=== After insert ===')
    const afterInsert = await db.find({})
    console.log(`Records: ${afterInsert.length}`)
    console.log(`IndexManager.totalLines: ${db.indexManager.totalLines}`)
    console.log(`offsets.length: ${db.offsets.length}`)
    
    // Delete record
    console.log('\n=== Deleting ===')
    const deleteResult = await db.delete({ id: 'test-1' })
    console.log(`Delete result: ${JSON.stringify(deleteResult)}`)
    console.log(`deletedIds: ${Array.from(db.deletedIds)}`)
    
    await db.save()
    
    console.log('\n=== After delete + save ===')
    console.log(`IndexManager.totalLines: ${db.indexManager.totalLines}`)
    console.log(`offsets.length: ${db.offsets.length}`)
    console.log(`deletedIds: ${Array.from(db.deletedIds)}`)
    
    const afterDelete = await db.find({})
    console.log(`Records: ${afterDelete.length}`)
    
    // Read file directly
    if (fs.existsSync(dbPath)) {
      const fileContent = fs.readFileSync(dbPath, 'utf8')
      const lines = fileContent.split('\n').filter(l => l.trim())
      console.log(`File lines: ${lines.length}`)
      console.log(`File content: ${fileContent.substring(0, 200)}`)
    }
    
    expect(afterDelete.length).toBe(0)
  })
})
