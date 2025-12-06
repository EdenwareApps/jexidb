import { describe, test, expect, beforeEach, afterEach } from '@jest/globals'
import { Database } from '../src/Database.mjs'
import path from 'path'

describe('RegExp Queries on Array Fields', () => {
  let testDir
  let db

  beforeEach(async () => {
    testDir = path.join(process.cwd(), 'test-dbs')
  })

  afterEach(async () => {
    if (db) {
      await db.save()
      await db.destroy()
      db = null
    }
  })

  describe('Bug Fix: RegExp on Array Fields', () => {
    test('should correctly filter array fields with RegExp queries', async () => {
      const dbPath = path.join(testDir, 'regex-array-fields.jdb')
      
      db = new Database(dbPath, {
        fields: { name: 'string', nameTerms: 'array:string', tags: 'array:string' },
        clear: true,
        create: true,
        debugMode: false
      })

      await db.init()

      // Insert test data
      await db.insert({ name: 'Globo', nameTerms: ['globo'] })
      await db.insert({ name: 'FlixHD', nameTerms: ['flixhd'] })
      await db.insert({ name: 'Netflix', nameTerms: ['netflix'] })
      await db.insert({ name: 'Global News', nameTerms: ['global', 'news'] })

      // Test 1: RegExp query that should match multiple records
      const results1 = await db.find({ nameTerms: new RegExp('glob', 'i') })
      expect(results1.length).toBe(2)
      expect(results1.some(r => r.name === 'Globo')).toBe(true)
      expect(results1.some(r => r.name === 'Global News')).toBe(true)

      // Test 2: RegExp query with impossible pattern
      const results2 = await db.find({ nameTerms: new RegExp('IMPOSSIBLE_PATTERN_12345', 'i') })
      expect(results2.length).toBe(0)

      // Test 3: RegExp query that matches multiple records with "flix"
      const results3 = await db.find({ nameTerms: new RegExp('flix', 'i') })
      expect(results3.length).toBe(2)
      expect(results3.some(r => r.name === 'FlixHD')).toBe(true)
      expect(results3.some(r => r.name === 'Netflix')).toBe(true)

      // Test 4: RegExp query with anchor (start of string)
      const results4 = await db.find({ nameTerms: new RegExp('^flix', 'i') })
      expect(results4.length).toBe(1)
      expect(results4[0].name).toBe('FlixHD')

      // Test 5: RegExp query with case sensitivity
      const results5 = await db.find({ nameTerms: new RegExp('GLOBO') })
      expect(results5.length).toBe(0) // Should not match because it's case-sensitive

      const results6 = await db.find({ nameTerms: new RegExp('GLOBO', 'i') })
      expect(results6.length).toBe(1) // Should match because it's case-insensitive
      expect(results6[0].name).toBe('Globo')
    })

    test('should correctly handle RegExp queries with multi-element arrays', async () => {
      const dbPath = path.join(testDir, 'regex-multi-array.jdb')
      
      db = new Database(dbPath, {
        fields: { name: 'string', nameTerms: 'array:string', tags: 'array:string' },
        clear: true,
        create: true,
        debugMode: false
      })

      await db.init()

      // Insert test data with multi-element arrays
      await db.insert({ name: 'Test 1', tags: ['javascript', 'nodejs', 'backend'] })
      await db.insert({ name: 'Test 2', tags: ['python', 'django', 'backend'] })
      await db.insert({ name: 'Test 3', tags: ['javascript', 'react', 'frontend'] })
      await db.insert({ name: 'Test 4', tags: ['ruby', 'rails', 'backend'] })

      // Test: RegExp query that matches first element
      const results1 = await db.find({ tags: new RegExp('java', 'i') })
      expect(results1.length).toBe(2)
      expect(results1.some(r => r.name === 'Test 1')).toBe(true)
      expect(results1.some(r => r.name === 'Test 3')).toBe(true)

      // Test: RegExp query that matches middle element
      const results2 = await db.find({ tags: new RegExp('react', 'i') })
      expect(results2.length).toBe(1)
      expect(results2[0].name).toBe('Test 3')

      // Test: RegExp query that matches last element
      const results3 = await db.find({ tags: new RegExp('backend', 'i') })
      expect(results3.length).toBe(3)
      expect(results3.some(r => r.name === 'Test 1')).toBe(true)
      expect(results3.some(r => r.name === 'Test 2')).toBe(true)
      expect(results3.some(r => r.name === 'Test 4')).toBe(true)
    })

    test('should correctly handle $regex operator on array fields', async () => {
      const dbPath = path.join(testDir, 'regex-operator-array.jdb')
      
      db = new Database(dbPath, {
        fields: { name: 'string', nameTerms: 'array:string', tags: 'array:string' },
        clear: true,
        create: true,
        debugMode: false
      })

      await db.init()

      // Insert test data
      await db.insert({ name: 'Globo', nameTerms: ['globo'] })
      await db.insert({ name: 'FlixHD', nameTerms: ['flixhd'] })
      await db.insert({ name: 'Netflix', nameTerms: ['netflix'] })
      await db.insert({ name: 'Global News', nameTerms: ['global', 'news'] })

      // Test: $regex operator query
      const results = await db.find({ nameTerms: { $regex: 'glob' } })
      expect(results.length).toBe(2)
      expect(results.some(r => r.name === 'Globo')).toBe(true)
      expect(results.some(r => r.name === 'Global News')).toBe(true)
    })
  })
})
