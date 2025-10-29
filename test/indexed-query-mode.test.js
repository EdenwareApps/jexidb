import { Database } from '../src/Database.mjs'
import fs from 'fs'

describe('Indexed Query Mode Control', () => {
  let db
  let testDbPath

  beforeEach(async () => {
    testDbPath = `test-indexed-mode-${Date.now()}-${Math.random()}.jdb`
    db = new Database(testDbPath, {
      indexes: { name: 'string', age: 'number' },
      indexedQueryMode: 'permissive',
      debugMode: false
    })
    await db.init()
    
    // Insert test data
    await db.insert({ name: 'John', age: 25, title: 'Developer' })
    await db.insert({ name: 'Jane', age: 30, title: 'Manager' })
  })

  afterEach(async () => {
    if (db && !db.destroyed) {
      try {
        await db.close()
      } catch (error) {
        // Ignore destroy errors in tests
        console.warn('Destroy error in test cleanup:', error.message)
      }
    }
    // Clean up test files with error handling
    try {
      if (fs.existsSync(testDbPath)) {
        fs.unlinkSync(testDbPath)
      }
      if (fs.existsSync(testDbPath.replace('.jdb', '.idx.jdb'))) {
        fs.unlinkSync(testDbPath.replace('.jdb', '.idx.jdb'))
      }
    } catch (error) {
      // Ignore file cleanup errors in tests
      console.warn('File cleanup error in test:', error.message)
    }
  })

  describe('Permissive Mode (Default)', () => {
    test('should allow queries on indexed fields', async () => {
      const results = await db.find({ name: 'John' })
      expect(results).toHaveLength(1)
      expect(results[0].name).toBe('John')
    })

    test('should allow queries on non-indexed fields (streaming)', async () => {
      const results = await db.find({ title: 'Developer' })
      expect(results).toHaveLength(1)
      expect(results[0].title).toBe('Developer')
    })

    test('should allow mixed queries with indexed and non-indexed fields', async () => {
      const results = await db.find({ name: 'John', title: 'Developer' })
      expect(results).toHaveLength(1)
      expect(results[0].name).toBe('John')
      expect(results[0].title).toBe('Developer')
    })

    test('should allow empty criteria queries', async () => {
      const results = await db.find({})
      expect(results).toHaveLength(2)
    })

    test('should work with findOne on non-indexed fields', async () => {
      const result = await db.findOne({ title: 'Manager' })
      expect(result).toBeTruthy()
      expect(result.title).toBe('Manager')
    })

    test('should work with count on non-indexed fields', async () => {
      const count = await db.count({ title: 'Developer' })
      expect(count).toBe(1)
    })
  })

  describe('Strict Mode', () => {
    beforeEach(async () => {
      if (db && !db.destroyed) {
        await db.close()
      }
      testDbPath = `test-indexed-mode-strict-${Date.now()}-${Math.random()}.jdb`
      db = new Database(testDbPath, {
        indexes: { name: 'string', age: 'number' },
        indexedQueryMode: 'strict',
        debugMode: false
      })
      await db.init()
      
      // Insert test data
      await db.insert({ name: 'John', age: 25, title: 'Developer' })
      await db.insert({ name: 'Jane', age: 30, title: 'Manager' })
    })

    test('should allow queries on indexed fields', async () => {
      const results = await db.find({ name: 'John' })
      expect(results).toHaveLength(1)
      expect(results[0].name).toBe('John')
    })

    test('should allow queries on multiple indexed fields', async () => {
      const results = await db.find({ name: 'John', age: 25 })
      expect(results).toHaveLength(1)
      expect(results[0].name).toBe('John')
      expect(results[0].age).toBe(25)
    })

    test('should throw error for queries on non-indexed fields', async () => {
      await expect(db.find({ title: 'Developer' })).rejects.toThrow(
        "Strict indexed mode: Field 'title' is not indexed. Available indexed fields: name, age"
      )
    })

    test('should throw error for mixed queries with non-indexed fields', async () => {
      await expect(db.find({ name: 'John', title: 'Developer' })).rejects.toThrow(
        "Strict indexed mode: Field 'title' is not indexed. Available indexed fields: name, age"
      )
    })

    test('should allow empty criteria queries', async () => {
      const results = await db.find({})
      expect(results).toHaveLength(2)
    })

    test('should throw error for findOne on non-indexed fields', async () => {
      await expect(db.findOne({ title: 'Manager' })).rejects.toThrow(
        "Strict indexed mode: Field 'title' is not indexed. Available indexed fields: name, age"
      )
    })

    test('should throw error for count on non-indexed fields', async () => {
      await expect(db.count({ title: 'Developer' })).rejects.toThrow(
        "Strict indexed mode: Field 'title' is not indexed. Available indexed fields: name, age"
      )
    })

    test('should work with findOne on indexed fields', async () => {
      const result = await db.findOne({ name: 'John' })
      expect(result).toBeTruthy()
      expect(result.name).toBe('John')
    })

    test('should work with count on indexed fields', async () => {
      const count = await db.count({ age: 30 })
      expect(count).toBe(1)
    })
  })

  describe('Logical Operators', () => {
    beforeEach(async () => {
      if (db && !db.destroyed) {
        await db.close()
      }
      testDbPath = `test-indexed-mode-logical-${Date.now()}-${Math.random()}.jdb`
      db = new Database(testDbPath, {
        indexes: { name: 'string', age: 'number' },
        indexedQueryMode: 'strict',
        debugMode: false
      })
      await db.init()
      
      // Insert test data
      await db.insert({ name: 'John', age: 25, title: 'Developer' })
      await db.insert({ name: 'Jane', age: 30, title: 'Manager' })
    })

    test('should allow $or with indexed fields', async () => {
      const results = await db.find({
        $or: [
          { name: 'John' },
          { age: 30 }
        ]
      })
      expect(results).toHaveLength(2)
    })

    test('should throw error for $or with non-indexed fields', async () => {
      await expect(db.find({
        $or: [
          { title: 'Developer' },
          { title: 'Manager' }
        ]
      })).rejects.toThrow(
        "Strict indexed mode: Field 'title' is not indexed. Available indexed fields: name, age"
      )
    })

    test('should allow $and with indexed fields', async () => {
      const results = await db.find({
        $and: [
          { name: 'John' },
          { age: 25 }
        ]
      })
      expect(results).toHaveLength(1)
      expect(results[0].name).toBe('John')
      expect(results[0].age).toBe(25)
    })

    test('should throw error for $and with non-indexed fields', async () => {
      await expect(db.find({
        $and: [
          { name: 'John' },
          { title: 'Developer' }
        ]
      })).rejects.toThrow(
        "Strict indexed mode: Field 'title' is not indexed. Available indexed fields: name, age"
      )
    })

    test('should allow $not with indexed fields', async () => {
      const results = await db.find({
        $not: { name: 'John' }
      })
      expect(results).toHaveLength(1)
      expect(results[0].name).toBe('Jane')
    })

    test('should throw error for $not with non-indexed fields', async () => {
      await expect(db.find({
        $not: { title: 'Developer' }
      })).rejects.toThrow(
        "Strict indexed mode: Field 'title' is not indexed. Available indexed fields: name, age"
      )
    })
  })

  describe('Default Behavior (No Mode Specified)', () => {
    beforeEach(async () => {
      if (db && !db.destroyed) {
        await db.close()
      }
      testDbPath = `test-indexed-mode-default-${Date.now()}-${Math.random()}.jdb`
      db = new Database(testDbPath, {
        indexes: { name: 'string', age: 'number' }
      })
      await db.init()
      
      // Insert test data
      await db.insert({ name: 'John', age: 25, title: 'Developer' })
      await db.insert({ name: 'Jane', age: 30, title: 'Manager' })
    })

    test('should default to permissive mode', async () => {
      // Should allow non-indexed field queries
      const results = await db.find({ title: 'Developer' })
      expect(results).toHaveLength(1)
      expect(results[0].title).toBe('Developer')
    })

    test('should allow indexed field queries', async () => {
      const results = await db.find({ name: 'John' })
      expect(results).toHaveLength(1)
      expect(results[0].name).toBe('John')
    })
  })

  describe('Error Messages', () => {
    beforeEach(async () => {
      if (db && !db.destroyed) {
        await db.close()
      }
      testDbPath = `test-indexed-mode-errors-${Date.now()}-${Math.random()}.jdb`
      db = new Database(testDbPath, {
        indexes: { name: 'string', age: 'number' },
        indexedQueryMode: 'strict',
        debugMode: false
      })
      await db.init()
      
      // Insert test data
      await db.insert({ name: 'John', age: 25, title: 'Developer' })
      await db.insert({ name: 'Jane', age: 30, title: 'Manager' })
    })

    test('should provide clear error message for single non-indexed field', async () => {
      await expect(db.find({ title: 'Developer' })).rejects.toThrow(
        "Strict indexed mode: Field 'title' is not indexed. Available indexed fields: name, age"
      )
    })

    test('should provide clear error message for multiple non-indexed fields', async () => {
      await expect(db.find({ title: 'Developer', department: 'Engineering' })).rejects.toThrow(
        "Strict indexed mode: Fields 'title', 'department' are not indexed. Available indexed fields: name, age"
      )
    })

    test('should list all available indexed fields in error message', async () => {
      const moreIndexesPath = `test-more-indexes-${Date.now()}-${Math.random()}.jdb`
      const dbWithMoreIndexes = new Database(moreIndexesPath, {
        indexes: { name: 'string', age: 'number', email: 'string', salary: 'number' },
        indexedQueryMode: 'strict',
        debugMode: false
      })
      await dbWithMoreIndexes.init()
      
      await expect(dbWithMoreIndexes.find({ title: 'Developer' })).rejects.toThrow(
        "Strict indexed mode: Field 'title' is not indexed. Available indexed fields: name, age, email, salary"
      )
      
      await dbWithMoreIndexes.destroy()
      // Clean up test files
      if (fs.existsSync(moreIndexesPath)) {
        fs.unlinkSync(moreIndexesPath)
      }
      if (fs.existsSync(moreIndexesPath.replace('.jdb', '.idx.jdb'))) {
        fs.unlinkSync(moreIndexesPath.replace('.jdb', '.idx.jdb'))
      }
    })
  })

  describe('Edge Cases', () => {
    beforeEach(async () => {
      if (db && !db.destroyed) {
        await db.close()
      }
      testDbPath = `test-indexed-mode-edges-${Date.now()}-${Math.random()}.jdb`
      db = new Database(testDbPath, {
        indexes: { name: 'string', age: 'number' },
        indexedQueryMode: 'strict',
        debugMode: false
      })
      await db.init()
      
      // Insert test data
      await db.insert({ name: 'John', age: 25, title: 'Developer' })
      await db.insert({ name: 'Jane', age: 30, title: 'Manager' })
    })

    test('should handle null criteria gracefully', async () => {
      const results = await db.find(null)
      expect(Array.isArray(results)).toBe(true)
    })

    test('should handle undefined criteria gracefully', async () => {
      const results = await db.find(undefined)
      expect(Array.isArray(results)).toBe(true)
    })

    test('should handle empty object criteria', async () => {
      const results = await db.find({})
      expect(Array.isArray(results)).toBe(true)
    })

    test('should handle criteria with only logical operators', async () => {
      const results = await db.find({
        $or: [
          { name: 'John' },
          { age: 30 }
        ]
      })
      expect(results).toHaveLength(2)
    })
  })
})
