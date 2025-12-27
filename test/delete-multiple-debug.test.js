import { Database } from '../src/Database.mjs'
import fs from 'fs'
import path from 'path'

describe('Delete Multiple Debug Test', () => {
  let testDir
  let db

  beforeEach(() => {
    testDir = path.join(process.cwd(), 'test-files', 'delete-multiple-debug')
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

  test('debug multiple delete issue', async () => {
    const dbPath = path.join(testDir, 'debug-multiple.jdb')
    
    db = new Database(dbPath, { 
      fields: { id: 'string', name: 'string', category: 'string' },
      clear: true, 
      create: true,
      debugMode: true
    })
    await db.init()
    
    // Insert 4 records
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
    
    console.log('\n=== After insert ===')
    const afterInsert = await db.find({})
    console.log(`Records: ${afterInsert.length}`)
    console.log(`IndexManager.totalLines: ${db.indexManager.totalLines}`)
    console.log(`offsets.length: ${db.offsets.length}`)
    console.log(`Record IDs: ${afterInsert.map(r => r.id).join(', ')}`)
    
    // Delete category A (should delete r1 and r2)
    console.log('\n=== Deleting category A ===')
    const deleteResult = await db.delete({ category: 'A' })
    console.log(`Delete result: ${JSON.stringify(deleteResult)}`)
    console.log(`deletedIds: ${Array.from(db.deletedIds)}`)
    console.log(`Expected deleted: r1, r2`)
    
    await db.save()
    
    console.log('\n=== After delete + save ===')
    console.log(`IndexManager.totalLines: ${db.indexManager.totalLines}`)
    console.log(`offsets.length: ${db.offsets.length}`)
    console.log(`deletedIds: ${Array.from(db.deletedIds)}`)
    
    const afterDelete = await db.find({})
    console.log(`Records found: ${afterDelete.length} (expected: 2)`)
    console.log(`Record IDs found: ${afterDelete.map(r => r.id).join(', ')}`)
    console.log(`Expected IDs: r3, r4`)
    
    // Check category A
    const categoryA = await db.find({ category: 'A' })
    console.log(`Category A records: ${categoryA.length} (expected: 0)`)
    
    // Check category B
    const categoryB = await db.find({ category: 'B' })
    console.log(`Category B records: ${categoryB.length} (expected: 2)`)
    
    // Read file directly
    if (fs.existsSync(dbPath)) {
      const fileContent = fs.readFileSync(dbPath, 'utf8')
      const lines = fileContent.split('\n').filter(l => l.trim())
      console.log(`\nFile lines: ${lines.length} (expected: 2)`)
      lines.forEach((line, i) => {
        try {
          const record = JSON.parse(line)
          console.log(`  Line ${i}: id=${record.id}, category=${record.category}`)
        } catch (e) {
          console.log(`  Line ${i}: [parse error]`)
        }
      })
    }
    
    expect(afterDelete.length).toBe(2)
    expect(categoryA.length).toBe(0)
    expect(categoryB.length).toBe(2)
  })
})
