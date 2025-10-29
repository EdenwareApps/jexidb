import { Database } from '../src/Database.mjs'
import fs from 'fs'

describe('Index File Persistence', () => {
  let testDbPath
  let testIdxPath

  beforeEach(() => {
    testDbPath = `test-index-persistence-${Date.now()}-${Math.random()}.jdb`
    testIdxPath = testDbPath.replace('.jdb', '.idx.jdb')
  })

  afterEach(() => {
    // Clean up test files
    const filesToClean = [testDbPath, testIdxPath]
    filesToClean.forEach(filePath => {
      if (fs.existsSync(filePath)) {
        try {
          fs.unlinkSync(filePath)
        } catch (error) {
          console.warn(`Warning: Could not delete ${filePath}: ${error.message}`)
        }
      }
    })
  })

  test('should generate .idx files with actual index data after database destruction', async () => {
    // Create database with indexes
    const db = new Database(testDbPath, {
      indexes: { name: 'string', category: 'string', tags: 'array' },
      debugMode: false
    })
    
    await db.init()

    // Insert test data with various field types including accented characters
    const testData = [
      { id: 1, name: 'João Silva', category: 'usuário', tags: ['admin', 'ativo'] },
      { id: 2, name: 'José Santos', category: 'usuário', tags: ['membro', 'ativo'] },
      { id: 3, name: 'Maria Antônia', category: 'administrador', tags: ['admin', 'super'] },
      { id: 4, name: 'Ana Carolina', category: 'usuário', tags: ['membro'] },
      { id: 5, name: 'Carlos Eduardo', category: 'convidado', tags: ['visitante'] },
      { id: 6, name: 'François Dubois', category: 'usuário', tags: ['francês', 'ativo'] },
      { id: 7, name: 'José María', category: 'administrador', tags: ['espanhol', 'admin'] }
    ]

    for (const record of testData) {
      await db.insert(record)
    }

    // Verify data was inserted
    expect(db.length).toBe(7)

    // Force index building by performing queries
    const userResults = await db.find({ category: 'usuário' })
    expect(userResults.length).toBe(4)

    const adminTagResults = await db.find({ tags: { $contains: 'admin' } })
    expect(adminTagResults.length).toBe(3)

    // Destroy the database instance (this should save indexes)
    await db.close()

    // Verify that .idx file was created
    expect(fs.existsSync(testIdxPath)).toBe(true)

    // Read and verify the .idx file contains actual index data
    const idxFileContent = fs.readFileSync(testIdxPath, 'utf8')
    expect(idxFileContent).toBeTruthy()
    expect(idxFileContent.length).toBeGreaterThan(0)

    // The .idx file should contain a single JSON object with combined index and offsets
    let combinedData
    try {
      combinedData = JSON.parse(idxFileContent)
    } catch (parseError) {
      throw new Error(`Failed to parse .idx file content: ${parseError.message}`)
    }

    // Verify the structure contains index and offsets
    expect(combinedData).toBeDefined()
    expect(combinedData.index).toBeDefined()
    expect(combinedData.offsets).toBeDefined()
    expect(Array.isArray(combinedData.offsets)).toBe(true)
    expect(combinedData.offsets.length).toBe(7) // Database uses offsets for efficient file operations

    // Verify the index data contains our indexed fields
    const indexData = combinedData.index.data
    expect(indexData).toBeDefined()
    expect(typeof indexData).toBe('object')

    // Check each indexed field has data
    const expectedFields = ['name', 'category', 'tags']
    for (const field of expectedFields) {
      expect(indexData[field]).toBeDefined()
      expect(typeof indexData[field]).toBe('object')
      
      // Verify the field index contains actual values from our test data
      const fieldIndex = indexData[field]
      const fieldKeys = Object.keys(fieldIndex)
      expect(fieldKeys.length).toBeGreaterThan(0)
      
      if (field === 'category') {
        // With term mapping, category field uses term IDs instead of original strings
        // Just verify that we have some term IDs (numeric strings)
        const hasTermIds = fieldKeys.some(key => /^\d+$/.test(key))
        expect(hasTermIds).toBe(true)
      } else if (field === 'tags') {
        // Should contain tag entries like 'admin', 'membro', 'ativo', etc.
        const hasExpectedValues = fieldKeys.some(key => 
          key === 'admin' || key === 'membro' || key === 'ativo' || key === 'super' || key === 'visitante' || key === 'francês' || key === 'espanhol'
        )
        expect(hasExpectedValues).toBe(true)
      } else if (field === 'name') {
        // With term mapping, name field uses term IDs instead of original strings
        // Just verify that we have some term IDs (numeric strings)
        const hasTermIds = fieldKeys.some(key => /^\d+$/.test(key))
        expect(hasTermIds).toBe(true)
      }
    }

    // Create a new database instance with the same path to verify indexes are loaded
    const db2 = new Database(testDbPath, {
      indexes: { name: 'string', category: 'string', tags: 'array' },
      debugMode: false
    })
    
    await db2.init()

    // Verify the new instance can use the persisted indexes
    const reloadedUserResults = await db2.find({ category: 'usuário' })
    expect(reloadedUserResults.length).toBe(4)

    const reloadedAdminTagResults = await db2.find({ tags: { $contains: 'admin' } })
    expect(reloadedAdminTagResults.length).toBe(3)

    // Verify data integrity
    expect(db2.length).toBe(7)

    await db2.destroy()
  })

  test('should handle empty database with indexes', async () => {
    const db = new Database(testDbPath, {
      indexes: { field1: 'string', field2: 'string' },
      debugMode: false
    })
    
    await db.init()
    
    // Don't insert any data, just destroy
    await db.close()

    // .idx file SHOULD be created for databases with indexes, even if empty
    // This ensures the database structure is complete
    expect(fs.existsSync(testIdxPath)).toBe(true)

    // Verify we can still recreate the database and it works correctly
    const db2 = new Database(testDbPath, {
      indexes: { field1: 'string', field2: 'string' },
      debugMode: false
    })
    
    await db2.init()
    
    // Database should be empty
    expect(db2.length).toBe(0)
    
    // Should be able to query (will use streaming since no indexes exist)
    const results = await db2.find({ field1: 'nonexistent' })
    expect(results.length).toBe(0)
    
    await db2.destroy()
  })

  test('should persist complex index structures', async () => {
    const db = new Database(testDbPath, {
      indexes: { simpleField: 'string', arrayField: 'array', nestedField: 'object' },
      debugMode: false
    })
    
    await db.init()

    // Insert data with complex structures
    await db.insert({
      id: 1,
      simpleField: 'simple_value',
      arrayField: ['item1', 'item2', 'item3'],
      nestedField: { subfield: 'nested_value' }
    })

    await db.insert({
      id: 2,
      simpleField: 'another_value',
      arrayField: ['item2', 'item4'],
      nestedField: { subfield: 'another_nested' }
    })

    // Force index usage with queries
    await db.find({ simpleField: 'simple_value' })
    await db.find({ arrayField: { $contains: 'item2' } })

    await db.close()

    // Verify .idx file was created and has content
    expect(fs.existsSync(testIdxPath)).toBe(true)
    
    const idxFileContent = fs.readFileSync(testIdxPath, 'utf8')
    expect(idxFileContent.length).toBeGreaterThan(0)

    // Verify we can recreate and use the database
    const db2 = new Database(testDbPath, {
      indexes: { simpleField: 'string', arrayField: 'array', nestedField: 'object' },
      debugMode: false
    })
    
    await db2.init()

    const results = await db2.find({ simpleField: 'simple_value' })
    expect(results.length).toBe(1)
    expect(results[0].id).toBe(1)

    await db2.destroy()
  })

  test('should maintain index consistency after multiple operations', async () => {
    const db = new Database(testDbPath, {
      indexes: { status: 'string', priority: 'string' },
      debugMode: false
    })
    
    await db.init()

    // Insert test data with different status and priority combinations
    await db.insert({ id: 1, status: 'active', priority: 'high' })
    await db.insert({ id: 2, status: 'inactive', priority: 'low' })
    await db.insert({ id: 3, status: 'pending', priority: 'medium' })
    await db.insert({ id: 4, status: 'active', priority: 'low' })

    // Query to ensure indexes are built
    const activeResults1 = await db.find({ status: 'active' })
    expect(activeResults1.length).toBe(2) // id 1 and id 4

    const highPriorityResults1 = await db.find({ priority: 'high' })
    expect(highPriorityResults1.length).toBe(1) // id 1

    await db.close()

    // Verify index file persistence
    expect(fs.existsSync(testIdxPath)).toBe(true)

    // Read and verify the index file contains the expected data
    const idxFileContent = fs.readFileSync(testIdxPath, 'utf8')
    const combinedData = JSON.parse(idxFileContent)
    
    expect(combinedData.index).toBeDefined()
    expect(combinedData.offsets).toBeDefined()
    expect(combinedData.offsets.length).toBe(4) // Database uses offsets for efficient file operations

    const indexData = combinedData.index.data
    expect(indexData.status).toBeDefined()
    expect(indexData.priority).toBeDefined()

    // Verify status index contains our test values
    // With term mapping, status field uses term IDs instead of original strings
    const statusKeys = Object.keys(indexData.status)
    expect(statusKeys.length).toBeGreaterThan(0)
    // Verify we have term IDs (numeric strings)
    const hasStatusTermIds = statusKeys.some(key => /^\d+$/.test(key))
    expect(hasStatusTermIds).toBe(true)

    // Verify priority index contains our test values
    // With term mapping, priority field uses term IDs instead of original strings
    const priorityKeys = Object.keys(indexData.priority)
    expect(priorityKeys.length).toBeGreaterThan(0)
    // Verify we have term IDs (numeric strings)
    const hasPriorityTermIds = priorityKeys.some(key => /^\d+$/.test(key))
    expect(hasPriorityTermIds).toBe(true)

    // Recreate database and verify consistency
    const db2 = new Database(testDbPath, {
      indexes: { status: 'string', priority: 'string' },
      debugMode: false
    })
    
    await db2.init()

    // Verify data integrity
    expect(db2.length).toBe(4)

    // Test queries work correctly with reloaded indexes
    const activeResults2 = await db2.find({ status: 'active' })
    expect(activeResults2.length).toBe(2)
    expect(activeResults2.map(r => r.id).sort()).toEqual([1, 4])

    const highPriorityResults2 = await db2.find({ priority: 'high' })
    expect(highPriorityResults2.length).toBe(1)
    expect(highPriorityResults2[0].id).toBe(1)

    const pendingResults = await db2.find({ status: 'pending' })
    expect(pendingResults.length).toBe(1)
    expect(pendingResults[0].id).toBe(3)

    await db2.destroy()
  })
})
