import fs from 'fs'
import path from 'path'
import { Database } from '../src/Database.mjs'

// Clean up test files
const cleanUp = (filePath) => {
  try {
    if (fs.existsSync(filePath)) {
      fs.unlinkSync(filePath)
    }
  } catch (error) {
    // Ignore cleanup errors
  }
}

describe('Score Method Tests', () => {
  let db
  let testDbPath
  let testIdxPath

  beforeEach(async () => {
    const testId = Math.random().toString(36).substring(7)
    testDbPath = path.join(process.cwd(), `test-score-${testId}.jdb`)
    testIdxPath = path.join(process.cwd(), `test-score-${testId}.idx.jdb`)
    
    // Clean up any existing files
    cleanUp(testDbPath)
    cleanUp(testIdxPath)
    
    // Create and initialize database
    db = new Database(testDbPath, {
      indexes: {
        'terms': 'array:string'
      }
    })
    await db.init()
  })

  afterEach(async () => {
    if (db) {
      await db.close()
      db = null
    }
    
    // Clean up test files
    cleanUp(testDbPath)
    cleanUp(testIdxPath)
  })

  describe('Basic Score Functionality', () => {
    test('should score records based on terms in array field', async () => {
      // Insert test data
      await db.insert({ id: 1, title: 'Action Movie', terms: ['action', 'movie'] })
      await db.insert({ id: 2, title: 'Comedy Show', terms: ['comedy', 'show'] })
      await db.insert({ id: 3, title: 'Action Comedy', terms: ['action', 'comedy'] })
      await db.insert({ id: 4, title: 'Documentary', terms: ['documentary'] })
      await db.save()

      // Score records
      const results = await db.score('terms', {
        'action': 1.0,
        'comedy': 0.8
      })

      expect(results).toHaveLength(3)
      
      // Check first result (Action Comedy - highest score: 1.8)
      expect(results[0].title).toBe('Action Comedy')
      expect(results[0].score).toBe(1.8)
      expect(results[0]._).toBeDefined()
      
      // Check second result (Action Movie - score: 1.0)
      expect(results[1].title).toBe('Action Movie')
      expect(results[1].score).toBe(1.0)
      
      // Check third result (Comedy Show - score: 0.8)
      expect(results[2].title).toBe('Comedy Show')
      expect(results[2].score).toBe(0.8)
    })

    test('should exclude records with zero scores', async () => {
      await db.insert({ id: 1, title: 'Item 1', terms: ['tech'] })
      await db.insert({ id: 2, title: 'Item 2', terms: ['science'] })
      await db.insert({ id: 3, title: 'Item 3', terms: ['news'] })
      await db.save()

      const results = await db.score('terms', {
        'tech': 1.0
      })

      expect(results).toHaveLength(1)
      expect(results[0].title).toBe('Item 1')
    })

    test('should handle decimal weights', async () => {
      await db.insert({ id: 1, title: 'High Priority', terms: ['urgent', 'important'] })
      await db.insert({ id: 2, title: 'Normal Priority', terms: ['normal'] })
      await db.save()

      const results = await db.score('terms', {
        'urgent': 0.9,
        'important': 0.7,
        'normal': 0.3
      })

      expect(results).toHaveLength(2)
      expect(results[0].title).toBe('High Priority')
      expect(results[0].score).toBe(1.6)
      expect(results[1].title).toBe('Normal Priority')
      expect(results[1].score).toBe(0.3)
    })
  })

  describe('Options Tests', () => {
    test('should respect limit option', async () => {
      await db.insert({ id: 1, title: 'Item 1', terms: ['a'] })
      await db.insert({ id: 2, title: 'Item 2', terms: ['a', 'b'] })
      await db.insert({ id: 3, title: 'Item 3', terms: ['a', 'b', 'c'] })
      await db.insert({ id: 4, title: 'Item 4', terms: ['a'] })
      await db.save()

      const results = await db.score('terms', {
        'a': 1.0,
        'b': 2.0,
        'c': 3.0
      }, { limit: 2 })

      expect(results).toHaveLength(2)
    })

    test('should respect sort ascending option', async () => {
      await db.insert({ id: 1, title: 'High', terms: ['high'] })
      await db.insert({ id: 2, title: 'Medium', terms: ['medium'] })
      await db.insert({ id: 3, title: 'Low', terms: ['low'] })
      await db.save()

      const results = await db.score('terms', {
        'high': 3.0,
        'medium': 2.0,
        'low': 1.0
      }, { sort: 'asc' })

      expect(results).toHaveLength(3)
      expect(results[0].title).toBe('Low')
      expect(results[1].title).toBe('Medium')
      expect(results[2].title).toBe('High')
    })

    test('should not include score when includeScore is false', async () => {
      await db.insert({ id: 1, title: 'Test', terms: ['a'] })
      await db.save()

      const results = await db.score('terms', {
        'a': 1.0
      }, { includeScore: false })

      expect(results).toHaveLength(1)
      expect(results[0].score).toBeUndefined()
      expect(results[0]._).toBeDefined()
    })

    test('should default to including score', async () => {
      await db.insert({ id: 1, title: 'Test', terms: ['a'] })
      await db.save()

      const results = await db.score('terms', {
        'a': 1.0
      })

      expect(results).toHaveLength(1)
      expect(results[0].score).toBe(1.0)
    })
  })

  describe('Mode Options', () => {
    test('should support max mode', async () => {
      await db.insert({ id: 1, title: 'Action Only', terms: ['action'] })
      await db.insert({ id: 2, title: 'Action Comedy', terms: ['action', 'comedy'] })
      await db.insert({ id: 3, title: 'Comedy Only', terms: ['comedy'] })
      await db.save()

      const results = await db.score('terms', {
        'action': 2.0,
        'comedy': 1.0
      }, { mode: 'max' })

      expect(results).toHaveLength(3)
      expect(results[0].title).toBe('Action Only')
      expect(results[0].score).toBe(2.0)
      expect(results[1].title).toBe('Action Comedy')
      expect(results[1].score).toBe(2.0)
      expect(results[2].title).toBe('Comedy Only')
      expect(results[2].score).toBe(1.0)
    })

    test('should support avg mode', async () => {
      await db.insert({ id: 1, title: 'Mixed', terms: ['action', 'comedy'] })
      await db.insert({ id: 2, title: 'Action Only', terms: ['action'] })
      await db.insert({ id: 3, title: 'Comedy Only', terms: ['comedy'] })
      await db.save()

      const results = await db.score('terms', {
        'action': 1.5,
        'comedy': 0.9
      }, { mode: 'avg' })

      expect(results).toHaveLength(3)
      expect(results[0].title).toBe('Action Only')
      expect(results[0].score).toBeCloseTo(1.5)
      expect(results[1].title).toBe('Mixed')
      expect(results[1].score).toBeCloseTo((1.5 + 0.9) / 2)
      expect(results[2].title).toBe('Comedy Only')
      expect(results[2].score).toBeCloseTo(0.9)
    })

    test('should support first mode with term priority', async () => {
      await db.insert({ id: 1, title: 'High Priority', terms: ['primary', 'secondary'] })
      await db.insert({ id: 2, title: 'Secondary Only', terms: ['secondary'] })
      await db.insert({ id: 3, title: 'Unmatched', terms: ['other'] })
      await db.save()

      const results = await db.score('terms', {
        'primary': 5,
        'secondary': 2
      }, { mode: 'first' })

      expect(results).toHaveLength(2)
      expect(results[0].title).toBe('High Priority')
      expect(results[0].score).toBe(5)
      expect(results[1].title).toBe('Secondary Only')
      expect(results[1].score).toBe(2)
    })
  })

  describe('Edge Cases', () => {
    test('should return empty array for empty scores', async () => {
      await db.insert({ id: 1, title: 'Test', terms: ['a'] })
      await db.save()

      const results = await db.score('terms', {})
      expect(results).toHaveLength(0)
    })

    test('should return empty array when no terms match', async () => {
      await db.insert({ id: 1, title: 'Test', terms: ['a'] })
      await db.save()

      const results = await db.score('terms', {
        'nonexistent': 1.0
      })
      
      expect(results).toHaveLength(0)
    })

    test('should handle empty database', async () => {
      const results = await db.score('terms', {
        'a': 1.0
      })

      expect(results).toHaveLength(0)
    })

    test('should handle multiple occurrences of same term', async () => {
      await db.insert({ id: 1, title: 'Test 1', terms: ['important', 'important'] })
      await db.insert({ id: 2, title: 'Test 2', terms: ['important'] })
      await db.save()

      const results = await db.score('terms', {
        'important': 1.0
      })

      // Both should have score 1.0 (duplicates in array don't multiply score)
      expect(results).toHaveLength(2)
    })
  })

  describe('Error Handling', () => {
    test('should throw error for invalid fieldName', async () => {
      await expect(db.score('', { 'a': 1.0 })).rejects.toThrow('non-empty string')
      await expect(db.score(null, { 'a': 1.0 })).rejects.toThrow('non-empty string')
    })

    test('should throw error for non-indexed field', async () => {
      await expect(db.score('nonexistent', { 'a': 1.0 }))
        .rejects.toThrow('not indexed')
    })

    test('should throw error for invalid scores object', async () => {
      await expect(db.score('terms', null)).rejects.toThrow('must be an object')
      await expect(db.score('terms', [])).rejects.toThrow('must be an object')
    })

    test('should throw error for non-numeric scores', async () => {
      await expect(db.score('terms', { 'a': 'invalid' }))
        .rejects.toThrow('must be a number')
    })
  })
})
