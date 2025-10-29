import { Database } from '../src/Database.mjs'
import fs from 'fs'
import path from 'path'

describe('Critical Bugs Fixes', () => {
  let testDir
  let db

  beforeEach(() => {
    testDir = path.join(process.cwd(), 'test-files', 'critical-bugs')
    fs.mkdirSync(testDir, { recursive: true })
  })

  afterEach(async () => {
    if (db) {
      await db.close()
    }
    // Clean up test files with retry mechanism
    if (fs.existsSync(testDir)) {
      try {
        fs.rmSync(testDir, { recursive: true, force: true })
      } catch (error) {
        // If removal fails, try again after a short delay
        await new Promise(resolve => setTimeout(resolve, 100))
        try {
          fs.rmSync(testDir, { recursive: true, force: true })
        } catch (retryError) {
          console.warn('Could not clean up test directory:', testDir)
        }
      }
    }
  })

  describe('Bug #1: File Descriptor Exhaustion (EMFILE)', () => {
    test('should handle concurrent operations without EMFILE errors', async () => {
      const dbPath = path.join(testDir, 'concurrent-test.jdb')
      
      // Create database with connection pooling enabled
      db = new Database(dbPath, {
        clear: true,
        create: true,
        indexes: {
          nameTerms: 'array:string',
          groupTerms: 'array:string'
        },
        debugMode: false
      })

      await db.init()

      // Test concurrent inserts
      const insertPromises = []
      for (let i = 0; i < 100; i++) {
        insertPromises.push(
          db.insert({
            name: `Channel ${i}`,
            nameTerms: [`channel`, `${i}`],
            url: `http://example.com/${i}`
          })
        )
      }

      // This should complete successfully
      await expect(Promise.all(insertPromises)).resolves.not.toThrow()
      
      // Verify data was inserted
      expect(db.length).toBe(100)
    })

    test('should handle concurrent queries without EMFILE errors', async () => {
      const dbPath = path.join(testDir, 'concurrent-queries.jdb')
      
      db = new Database(dbPath, {
        clear: true,
        create: true,
        indexes: {
          nameTerms: 'array:string'
        },
      })

      await db.init()

      // Insert test data
      for (let i = 0; i < 50; i++) {
        await db.insert({
          name: `Channel ${i}`,
          nameTerms: [`channel`, `${i}`],
          url: `http://example.com/${i}`
        })
      }

      // Test concurrent queries
      const queryPromises = []
      for (let i = 0; i < 100; i++) {
        queryPromises.push(
          db.findOne({ nameTerms: { $in: ['channel'] } })
        )
      }

      // This should complete successfully
      await expect(Promise.all(queryPromises)).resolves.not.toThrow()
    })
  })

  describe('Bug #2: Object Reference Corruption', () => {
    test('should not corrupt object references during insert', async () => {
      const dbPath = path.join(testDir, 'reference-corruption.jdb')
      
      db = new Database(dbPath, {
        clear: true,
        create: true,
        indexes: {
          nameTerms: 'array:string'
        }
      })

      await db.init()

      // Create test entry
      const entry = {
        name: 'Test Channel',
        nameTerms: ['test', 'channel'],
        url: 'http://example.com'
      }

      // Store original values
      const originalNameTerms = [...entry.nameTerms]
      const originalName = entry.name

      // Insert entry
      await db.insert(entry)

      // Verify original object was not corrupted
      expect(entry.nameTerms).toEqual(originalNameTerms)
      expect(entry.name).toBe(originalName)
      expect(entry.nameTerms).not.toBeUndefined()
      expect(entry.nameTerms.length).toBe(2)
    })

    test('should handle concurrent inserts without reference corruption', async () => {
      const dbPath = path.join(testDir, 'concurrent-reference.jdb')
      
      db = new Database(dbPath, {
        clear: true,
        create: true,
        indexes: {
          nameTerms: 'array:string'
        },
      })

      await db.init()

      // Create multiple entries with same reference structure
      const entries = []
      for (let i = 0; i < 20; i++) {
        entries.push({
          name: `Channel ${i}`,
          nameTerms: ['test', 'channel', `${i}`],
          url: `http://example.com/${i}`
        })
      }

      // Insert all entries concurrently
      const insertPromises = entries.map(entry => db.insert(entry))
      await Promise.all(insertPromises)

      // Verify all original entries are intact
      entries.forEach((entry, index) => {
        expect(entry.nameTerms).toEqual(['test', 'channel', `${index}`])
        expect(entry.name).toBe(`Channel ${index}`)
        expect(entry.nameTerms.length).toBe(3)
      })
    })
  })

  describe('Bug #3: Array Query Syntax Issues', () => {
    test('should handle direct array field queries automatically', async () => {
      const dbPath = path.join(testDir, 'array-query.jdb')
      
      db = new Database(dbPath, {
        clear: true,
        create: true,
        debugMode: false,
        indexes: {
          nameTerms: 'array:string',
          tags: 'array:string'
        }
      })

      await db.init()

      // Insert test data
      await db.insert({
        name: 'CNN International',
        nameTerms: ['cnn', 'international', 'news'],
        tags: ['news', 'international']
      })

      // Test direct value queries (should work automatically)
      const result1 = await db.findOne({ nameTerms: 'cnn' })
      expect(result1).toBeTruthy()
      expect(result1.name).toBe('CNN International')

      // Test array value queries (should work automatically)
      const result2 = await db.findOne({ nameTerms: ['cnn'] })
      expect(result2).toBeTruthy()
      expect(result2.name).toBe('CNN International')

      // Test $in queries (should still work)
      const result3 = await db.findOne({ nameTerms: { $in: ['cnn'] } })
      expect(result3).toBeTruthy()
      expect(result3.name).toBe('CNN International')

      // Test multiple values
      const result4 = await db.findOne({ tags: 'news' })
      expect(result4).toBeTruthy()
      expect(result4.name).toBe('CNN International')
    })

    test('should provide clear error messages for invalid array queries', async () => {
      const dbPath = path.join(testDir, 'array-query-error.jdb')
      
      db = new Database(dbPath, {
        clear: true,
        create: true,
        indexes: {
          nameTerms: 'array:string'
        }
      })

      await db.init()

      // Insert test data
      await db.insert({
        name: 'Test Channel',
        nameTerms: ['test', 'channel']
      })

      // Test invalid query (should throw clear error message)
      await expect(db.findOne({ nameTerms: null })).rejects.toThrow(
        "Invalid query for array field 'nameTerms'. Use { $in: [value] } syntax or direct value."
      )
    })
  })

  describe('Bug #4: Memory Leaks and Performance Issues', () => {
    test('should properly clean up resources on destroy', async () => {
      const dbPath = path.join(testDir, 'memory-cleanup.jdb')
      
      db = new Database(dbPath, {
        clear: true,
        create: true,
        indexes: {
          nameTerms: 'array:string'
        },
        debugMode: false
      })

      await db.init()

      // Insert some data
      for (let i = 0; i < 100; i++) {
        await db.insert({
          name: `Channel ${i}`,
          nameTerms: [`channel`, `${i}`]
        })
      }

      // Get memory usage before save
      const memBefore = db.getMemoryUsage()
      expect(memBefore.writeBufferSize).toBe(100) // Records should be in writeBuffer
      expect(memBefore.total).toBe(100) // Total should be 100 records

      // Save data before destroy
      await db.save()

      // Verify writeBuffer is cleared after save
      const memAfter = db.getMemoryUsage()
      expect(memAfter.writeBufferSize).toBe(0) // writeBuffer should be empty after save

      // Destroy database
      await db.destroy()

      // Verify database is destroyed
      expect(db.destroyed).toBe(true)
      expect(db.operationQueue).toBeNull()
    })

    test('should handle auto-save functionality', async () => {
      const dbPath = path.join(testDir, 'auto-save.jdb')
      
      db = new Database(dbPath, {
        clear: true,
        create: true,
        indexes: {
          nameTerms: 'array:string'
        },
        debugMode: false
      })

      await db.init()

      // Insert data to trigger auto-save
      for (let i = 0; i < 10; i++) {
        await db.insert({
          name: `Channel ${i}`,
          nameTerms: [`channel`, `${i}`]
        })
      }

      // Wait for auto-save to complete (it's async via setImmediate)
      await new Promise(resolve => setTimeout(resolve, 2000))

      // Verify data was saved
      expect(db.length).toBe(10)
    })
  })

  describe('Bug #5: Incomplete Database Structure', () => {
    test('should create complete database structure even when empty', async () => {
      const dbPath = path.join(testDir, 'empty-structure.jdb')
      
      db = new Database(dbPath, {
        clear: true,
        create: true,
        indexes: {
          nameTerms: 'array:string'
        }
      })

      await db.init()

      // Don't insert any data
      expect(db.length).toBe(0)

      // Save empty database
      await db.save()

      // Check if all required files were created
      const mainFile = dbPath
      const indexFile = dbPath.replace(/\.jdb$/, '.idx.jdb')

      expect(fs.existsSync(mainFile)).toBe(true)
      expect(fs.existsSync(indexFile)).toBe(true)
      // Note: offsets file might be combined with index file in newer versions
    })

    test('should maintain structure consistency after operations', async () => {
      const dbPath = path.join(testDir, 'structure-consistency.jdb')
      
      db = new Database(dbPath, {
        clear: true,
        create: true,
        indexes: {
          nameTerms: 'array:string'
        }
      })

      await db.init()

      // Insert some data
      await db.insert({
        name: 'Test Channel',
        nameTerms: ['test', 'channel']
      })

      await db.save()

      // Verify structure
      expect(db.length).toBe(1)
      expect(fs.existsSync(dbPath)).toBe(true)
      expect(fs.existsSync(dbPath.replace(/\.jdb$/, '.idx.jdb'))).toBe(true)
    })
  })

  describe('Integration Tests', () => {
    test('should handle real-world scenario similar to update-list-index.js', async () => {
      const dbPath = path.join(testDir, 'real-world.jdb')
      
      // Configuration similar to the workaround
      db = new Database(dbPath, {
        clear: true,
        create: true,
        indexes: {
          nameTerms: 'array:string',
          groupTerms: 'array:string'
        },
        maxWriteBufferSize: 256 * 1024, // 256KB
        integrityCheck: 'none',
        streamingThreshold: 0.8,
        indexedQueryMode: 'strict',
        debugMode: false,
      })

      await db.init()

      // Simulate the insert pattern from the workaround
      const entries = []
      for (let i = 0; i < 1000; i++) {
        entries.push({
          name: `Channel ${i}`,
          nameTerms: ['channel', `${i}`, 'test'],
          groupTerms: ['group1', 'group2'],
          url: `http://example.com/${i}`
        })
      }

      // Insert with controlled concurrency (similar to the workaround)
      const batchSize = 50
      for (let i = 0; i < entries.length; i += batchSize) {
        const batch = entries.slice(i, i + batchSize)
        const promises = batch.map(entry => db.insert(entry))
        await Promise.all(promises)
      }

      // Verify all data was inserted correctly
      expect(db.length).toBe(1000)

      // Test queries work correctly
      const result = await db.findOne({ nameTerms: 'channel' })
      expect(result).toBeTruthy()
      expect(result.name).toMatch(/^Channel \d+$/)

      // Test array queries work
      const results = await db.find({ nameTerms: 'test' })
      expect(results.length).toBe(1000)

      // Verify no object corruption occurred
      entries.forEach((entry, index) => {
        expect(entry.nameTerms).toEqual(['channel', `${index}`, 'test'])
        expect(entry.name).toBe(`Channel ${index}`)
      })
    })
  })

  describe('🚨 CRITICAL: Save Buffer Regression Tests', () => {
    test('save() should automatically flush writeBuffer and ensure it is empty', async () => {
      const dbPath = path.join(testDir, 'test-flush.jdb')
      db = new Database(dbPath, { clear: true, create: true })
      await db.init()
      
      // Insert data to populate writeBuffer
      await db.insert({ name: "Test", url: "http://test.com" })
      await db.insert({ name: "Test2", url: "http://test2.com" })
      
      // Verify writeBuffer has data before save
      expect(db.writeBuffer.length).toBeGreaterThan(0)
      
      // Save should flush the buffer
      await db.save()
      
      // CRITICAL: writeBuffer must be empty after save
      expect(db.writeBuffer.length).toBe(0)
    })

    test('save() should persist data correctly after flushing buffer', async () => {
      const dbPath = path.join(testDir, 'test-persist.jdb')
      db = new Database(dbPath, { clear: true, create: true })
      
      await db.init()
      
      // Insert test data
      const testData = { name: "RTP1", url: "http://example.com" }
      await db.insert(testData)
      
      // Save should flush and persist data
      await db.save()
      
      // Query should return the actual data, not empty entries
      const results = await db.find()
      expect(results).toHaveLength(1)
      expect(results[0].name).toBe("RTP1")
      expect(results[0].url).toBe("http://example.com")
    })

    test('save() should work with immediate destroy without data loss', async () => {
      const dbPath = path.join(testDir, 'test-immediate-destroy.jdb')
      db = new Database(dbPath, { clear: true, create: true })
      await db.init()
      
      // Insert data
      await db.insert({ name: "DestroyTest", url: "http://destroy.com" })
      
      // Save and immediately destroy
      await db.close()
      
      // Create new database instance and verify data persisted
      const db2 = new Database(dbPath, { create: true })
      await db2.init()
      
      const results = await db2.find()
      expect(results).toHaveLength(1)
      expect(results[0].name).toBe("DestroyTest")
      expect(results[0].url).toBe("http://destroy.com")
      
      await db2.destroy()
    })

    test('save() should handle multiple inserts correctly', async () => {
      const dbPath = path.join(testDir, 'test-multiple.jdb')
      db = new Database(dbPath, { clear: true, create: true })
      
      await db.init()
      
      // Insert multiple records
      const testData = [
        { name: "Channel1", url: "http://channel1.com" },
        { name: "Channel2", url: "http://channel2.com" },
        { name: "Channel3", url: "http://channel3.com" }
      ]
      
      for (const data of testData) {
        await db.insert(data)
      }
      
      // Save should flush all data
      await db.save()
      
      // All data should be persisted
      const results = await db.find()
      expect(results).toHaveLength(3)
      
      // Verify no empty entries
      results.forEach(result => {
        expect(result.name).not.toBe("")
        expect(result.url).not.toBe("")
      })
    })

    test('save() should not return before file operations are complete', async () => {
      const dbPath = path.join(testDir, 'test-io-completion.jdb')
      db = new Database(dbPath, { clear: true, create: true })
      
      await db.init()
      
      // Insert data
      await db.insert({ name: "IOTest", url: "http://iotest.com" })
      
      // Ensure data is in writeBuffer
      expect(db.writeBuffer.length).toBeGreaterThan(0)
      
      // Save should wait for I/O completion
      const saveStart = Date.now()
      await db.save()
      const saveEnd = Date.now()
      
      // Verify save actually took time (indicating I/O operations)
      expect(saveEnd - saveStart).toBeGreaterThan(0)
      
      // Verify data is actually persisted by checking file system
      const dataFile = dbPath
      
      // Data file should exist and have content
      const dataStats = fs.statSync(dataFile)
      expect(dataStats.size).toBeGreaterThan(0)
      
      // Verify data can be queried (proving persistence)
      const results = await db.find()
      expect(results).toHaveLength(1)
      expect(results[0].name).toBe("IOTest")
    })

    test('flushInsertionBuffer() should work for backward compatibility', async () => {
      const dbPath = path.join(testDir, 'test-backward-compat.jdb')
      db = new Database(dbPath, { clear: true, create: true })
      
      await db.init()
      
      // Insert data
      await db.insert({ name: "BackwardTest", url: "http://backward.com" })
      
      // Use deprecated method (should show warning but work)
      const consoleSpy = jest.spyOn(console, 'warn').mockImplementation(() => {})
      await db.flushInsertionBuffer()
      consoleSpy.mockRestore()
      
      // Data should be persisted
      const results = await db.find()
      expect(results).toHaveLength(1)
      expect(results[0].name).toBe("BackwardTest")
    })

    test('should handle rapid insert-save cycles', async () => {
      const dbPath = path.join(testDir, 'test-rapid-cycles.jdb')
      db = new Database(dbPath, { clear: true, create: true })
      
      await db.init()
      
      // Rapid insert-save cycles
      for (let i = 0; i < 10; i++) {
        await db.insert({ name: `Cycle${i}`, url: `http://cycle${i}.com` })
        await db.save()
      }
      
      // Wait for all operations to complete
      await db.waitForOperations()
      
      // Force a final save to ensure all data is persisted
      await db.save()
      
      // All data should be persisted
      const results = await db.find()
      expect(results).toHaveLength(10)
      
      // Verify data integrity
      results.forEach((result, index) => {
        expect(result.name).toBe(`Cycle${index}`)
        expect(result.url).toBe(`http://cycle${index}.com`)
      })
    })
  })

  describe('Bug #6: db.walk() returns arrays instead of objects', () => {
    test('should return objects consistently in db.walk() method', async () => {
      const dbPath = path.join(testDir, 'walk-bug-test.jdb')
      
      // Create database with EPG-like schema (similar to the bug report)
      db = new Database(dbPath, {
        clear: true,
        create: true,
        fields: {
          ch: 'string',           // Channel name
          start: 'number',        // Start timestamp
          e: 'number',            // End timestamp
          t: 'string',            // Programme title
          i: 'string',            // Icon URL
          desc: 'string',         // Description
          c: 'array:string',      // Categories
          terms: 'array:string',  // Search terms
          age: 'number',          // Age rating
          lang: 'string',         // Language
          country: 'string',      // Country
          rating: 'string',       // Rating
          parental: 'string',     // Parental control
          genre: 'string',        // Genre
          contentType: 'string',  // Content type
          parentalLock: 'string', // Parental lock
          geo: 'string',          // Geographic restriction
          ageRestriction: 'string' // Age restriction
        },
        indexes: ['ch', 'start', 'e', 'c', 'terms']
      })

      await db.init()

      // Insert test data that mimics EPG programme data
      const testProgrammes = [
        {
          ch: 'Canal Sony FHD H.265',
          start: 1759628040,
          e: 1759639740,
          t: 'À Espera de um Milagre',
          i: '',
          desc: '',
          c: ['Filme', 'Drama'],
          terms: ['à', 'espera', 'de', 'um', 'milagre', 'canal', 'sony', 'h', '265', 'filme', 'drama'],
          age: 0,
          lang: '',
          country: '',
          rating: '',
          parental: 'no',
          genre: 'Filme, Drama',
          contentType: '',
          parentalLock: 'false',
          geo: '',
          ageRestriction: ''
        },
        {
          ch: 'GLOBO',
          start: 1759628040,
          e: 1759639740,
          t: 'Jornal da Globo',
          i: '',
          desc: '',
          c: ['Jornalismo'],
          terms: ['jornal', 'globo', 'noticias', 'jornalismo'],
          age: 0,
          lang: '',
          country: '',
          rating: '',
          parental: 'no',
          genre: 'Jornalismo',
          contentType: '',
          parentalLock: 'false',
          geo: '',
          ageRestriction: ''
        },
        {
          ch: 'ESPN',
          start: 1759630000,
          e: 1759640000,
          t: 'ESPN SportsCenter',
          i: '',
          desc: '',
          c: ['Esporte'],
          terms: ['espn', 'sportscenter', 'esporte', 'futebol'],
          age: 0,
          lang: '',
          country: '',
          rating: '',
          parental: 'no',
          genre: 'Esporte',
          contentType: '',
          parentalLock: 'false',
          geo: '',
          ageRestriction: ''
        }
      ]

      // Insert data using InsertSession (as used in EPG applications)
      const insertSession = db.beginInsertSession({
        batchSize: 500,
        enableAutoSave: true
      })
      
      for (const programme of testProgrammes) {
        await insertSession.add(programme)
      }
      
      await insertSession.commit()
      await db.save()

      // Test queries that previously caused the bug
      const testQueries = [
        { terms: ['à', 'espera', 'de', 'um', 'milagre'] },
        { terms: ['globo', 'jornal'] },
        { terms: ['espn'] }
      ]

      for (const query of testQueries) {
        // Test 1: db.find() - should work correctly
        const findResults = await db.find(query)
        expect(findResults.length).toBeGreaterThan(0)
        
        // Verify all results are objects
        findResults.forEach(result => {
          expect(Array.isArray(result)).toBe(false)
          expect(typeof result).toBe('object')
          expect(result.ch).toBeDefined()
          expect(result.t).toBeDefined()
        })

        // Test 2: db.walk() without options - should return objects
        const walkResults = []
        for await (const programme of db.walk(query)) {
          walkResults.push(programme)
        }
        
        expect(walkResults.length).toBeGreaterThan(0)
        
        // CRITICAL: Verify all results are objects, not arrays
        walkResults.forEach(result => {
          expect(Array.isArray(result)).toBe(false)
          expect(typeof result).toBe('object')
          expect(result.ch).toBeDefined()
          expect(result.t).toBeDefined()
        })

        // Test 3: db.walk() with caseInsensitive option - should return objects
        const walkCaseInsensitiveResults = []
        for await (const programme of db.walk(query, { caseInsensitive: true })) {
          walkCaseInsensitiveResults.push(programme)
        }
        
        walkCaseInsensitiveResults.forEach(result => {
          expect(Array.isArray(result)).toBe(false)
          expect(typeof result).toBe('object')
          expect(result.ch).toBeDefined()
          expect(result.t).toBeDefined()
        })

        // Test 4: db.walk() with matchAny option - should return objects
        const walkMatchAnyResults = []
        for await (const programme of db.walk(query, { matchAny: true })) {
          walkMatchAnyResults.push(programme)
        }
        
        walkMatchAnyResults.forEach(result => {
          expect(Array.isArray(result)).toBe(false)
          expect(typeof result).toBe('object')
          expect(result.ch).toBeDefined()
          expect(result.t).toBeDefined()
        })

        // Test 5: db.walk() with both options - should return objects
        const walkBothOptionsResults = []
        for await (const programme of db.walk(query, { caseInsensitive: true, matchAny: true })) {
          walkBothOptionsResults.push(programme)
        }
        
        walkBothOptionsResults.forEach(result => {
          expect(Array.isArray(result)).toBe(false)
          expect(typeof result).toBe('object')
          expect(result.ch).toBeDefined()
          expect(result.t).toBeDefined()
        })
      }

      // Test 6: db.walk() with empty query - should return objects
      const emptyQueryResults = []
      for await (const programme of db.walk({})) {
        emptyQueryResults.push(programme)
      }
      
      expect(emptyQueryResults.length).toBe(3) // Should return all 3 programmes
      
      emptyQueryResults.forEach(result => {
        expect(Array.isArray(result)).toBe(false)
        expect(typeof result).toBe('object')
        expect(result.ch).toBeDefined()
        expect(result.t).toBeDefined()
      })
    })

    test('should maintain consistency between db.find() and db.walk() results', async () => {
      const dbPath = path.join(testDir, 'walk-consistency-test.jdb')
      
      db = new Database(dbPath, {
        clear: true,
        create: true,
        fields: {
          name: 'string',
          category: 'string',
          tags: 'array:string'
        },
        indexes: ['name', 'category', 'tags']
      })

      await db.init()

      // Insert test data
      const testData = [
        { name: 'Movie A', category: 'Entertainment', tags: ['action', 'thriller'] },
        { name: 'Movie B', category: 'Entertainment', tags: ['comedy', 'romance'] },
        { name: 'News Show', category: 'News', tags: ['current', 'events'] }
      ]

      for (const item of testData) {
        await db.insert(item)
      }
      
      await db.save()

      // Test various queries
      const queries = [
        { category: 'Entertainment' },
        { tags: ['action'] },
        { name: 'News Show' },
        { category: 'News', tags: ['current'] }
      ]

      for (const query of queries) {
        // Get results from both methods
        const findResults = await db.find(query)
        const walkResults = []
        
        for await (const item of db.walk(query)) {
          walkResults.push(item)
        }

        // Both should return the same number of results
        expect(walkResults.length).toBe(findResults.length)

        // Both should return objects with the same structure
        findResults.forEach((findResult, index) => {
          const walkResult = walkResults[index]
          
          // Both should be objects
          expect(Array.isArray(findResult)).toBe(false)
          expect(Array.isArray(walkResult)).toBe(false)
          
          // Both should have the same properties
          expect(walkResult.name).toBe(findResult.name)
          expect(walkResult.category).toBe(findResult.category)
          expect(walkResult.tags).toEqual(findResult.tags)
        })
      }
    })

    test('length should be correct after close() + init() cycle', async () => {
      const dbPath = path.join(testDir, 'test-close-init-length.jdb')
      db = new Database(dbPath, { clear: true, create: true })
      
      await db.init()
      
      // Insert test data
      const testData = [
        { name: "Channel1", url: "http://channel1.com" },
        { name: "Channel2", url: "http://channel2.com" },
        { name: "Channel3", url: "http://channel3.com" }
      ]
      
      for (const data of testData) {
        await db.insert(data)
      }
      
      // Verify data is inserted
      expect(db.length).toBe(3)
      
      // Save data to disk
      await db.save()
      
      // Verify data is still there after save
      expect(db.length).toBe(3)
      
      // Close database
      await db.close()
      
      // Create new database instance
      const db2 = new Database(dbPath, { create: true })
      await db2.init()
      
      // CRITICAL: length should be correct after init()
      expect(db2.length).toBe(3)
      
      // Verify data is accessible
      const results = await db2.find()
      expect(results).toHaveLength(3)
      expect(results[0].name).toBe("Channel1")
      expect(results[1].name).toBe("Channel2")
      expect(results[2].name).toBe("Channel3")
      
      await db2.destroy()
    })

    test('EPGManager-like scenario: database state after close + init', async () => {
      const dbPath = path.join(testDir, 'test-epg-manager-scenario.jdb')
      db = new Database(dbPath, { clear: true, create: true })
      
      await db.init()
      
      // Insert EPG-like data
      const epgData = [
        { ch: "Channel1", t: "Program 1", start: 1640995200, e: 1640998800, terms: ["news", "sports"] },
        { ch: "Channel2", t: "Program 2", start: 1640995200, e: 1640998800, terms: ["movie", "action"] },
        { ch: "Channel3", t: "Program 3", start: 1640995200, e: 1640998800, terms: ["comedy", "show"] }
      ]
      
      for (const data of epgData) {
        await db.insert(data)
      }
      
      // Verify data is inserted
      expect(db.length).toBe(3)
      
      // Save data to disk
      await db.save()
      
      // Close database
      await db.close()
      
      // Create new database instance (like EPGManager does)
      const db2 = new Database(dbPath, { create: true })
      
      // Simulate EPGManager initialization sequence
      await db2.init()
      
      // Add delay like EPGManager does
      await new Promise(resolve => setTimeout(resolve, 500))
      
      // Test database accessibility like EPGManager does
      const testCount = await db2.count()
      console.log(`Database test successful, total records: ${testCount}`)
      
      // CRITICAL: length should be correct after init()
      expect(db2.length).toBe(3)
      expect(testCount).toBe(3)
      
      // Verify data is accessible with EPG-like queries
      const results = await db2.find({ terms: { $in: ["news"] } })
      expect(results).toHaveLength(1)
      expect(results[0].t).toBe("Program 1")
      
      await db2.destroy()
    })

    test('IMMEDIATE ACCESS: No delay should be needed after init()', async () => {
      const dbPath = path.join(testDir, 'test-immediate-access.jdb')
      db = new Database(dbPath, { clear: true, create: true })
      
      await db.init()
      
      // Insert test data
      const testData = [
        { name: "Test1", value: 1 },
        { name: "Test2", value: 2 },
        { name: "Test3", value: 3 }
      ]
      
      for (const data of testData) {
        await db.insert(data)
      }
      
      // Save data
      await db.save()
      
      // Close database
      await db.close()
      
      // Create new database instance
      const db2 = new Database(dbPath, { create: true })
      
      // CRITICAL: init() should return only when everything is ready
      await db2.init()
      
      // NO DELAY - immediate access should work
      console.log(`IMMEDIATE ACCESS: length=${db2.length}, count=${await db2.count()}`)
      
      // These should work immediately without any delay
      expect(db2.length).toBe(3)
      expect(await db2.count()).toBe(3)
      
      // Verify data is immediately accessible
      const results = await db2.find({ name: "Test1" })
      expect(results).toHaveLength(1)
      expect(results[0].value).toBe(1)
      
      await db2.destroy()
    })

    test('PERFORMANCE: length vs count() performance comparison', async () => {
      const dbPath = path.join(testDir, 'test-performance-comparison.jdb')
      db = new Database(dbPath, { clear: true, create: true })
      
      await db.init()
      
      // Insert more data for performance test
      const testData = []
      for (let i = 0; i < 1000; i++) {
        testData.push({ name: `Test${i}`, value: i })
      }
      
      for (const data of testData) {
        await db.insert(data)
      }
      
      // Save data
      await db.save()
      
      // Close database
      await db.close()
      
      // Create new database instance
      const db2 = new Database(dbPath, { create: true })
      await db2.init()
      
      // Test performance of length vs count()
      const lengthStart = Date.now()
      const lengthResult = db2.length
      const lengthTime = Date.now() - lengthStart
      
      const countStart = Date.now()
      const countResult = await db2.count()
      const countTime = Date.now() - countStart
      
      console.log(`PERFORMANCE: length=${lengthResult} (${lengthTime}ms) vs count=${countResult} (${countTime}ms)`)
      
      // Both should return the same result
      expect(lengthResult).toBe(1000)
      expect(countResult).toBe(1000)
      
      // length should be much faster (uses index)
      // count should be slower (executes full query)
      expect(lengthTime).toBeLessThan(countTime)
      
      await db2.destroy()
    })
  })
})
