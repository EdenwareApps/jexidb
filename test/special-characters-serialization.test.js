/**
 * Test serialization of values containing newlines, commas, and special characters
 * with term mapping enabled
 */

import { Database } from '../src/Database.mjs'
import fs from 'fs'
import path from 'path'

describe('Special Characters Serialization', () => {
  let testDir
  let dbPath
  let db

  beforeEach(() => {
    testDir = path.join(process.cwd(), 'test-files', 'special-chars')
    fs.mkdirSync(testDir, { recursive: true })
    dbPath = path.join(testDir, `special-chars-${Date.now()}-${Math.random()}.jdb`)
  })

  afterEach(async () => {
    if (db) {
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

  test('should support values with newlines and commas in string fields', async () => {
    const dbConfig = {
      create: true,
      fields: {
        id: 'string',
        name: 'string',
        description: 'string',
        tags: 'array:string',
        content: 'string'
      },
      indexes: {
        tags: 'array:string' // This enables term mapping for tags
      }
    }

    db = new Database(dbPath, dbConfig)
    await db.init()

    // Test data with various special characters
    const testData = [
      {
        id: 'test1',
        name: 'Simple Name',
        description: 'Simple description',
        tags: ['tag1', 'tag2'],
        content: 'Simple content'
      },
      {
        id: 'test2',
        name: 'Name with, commas',
        description: 'Description with\nnewlines\nand, commas',
        tags: ['tag,with,commas', 'tag\nwith\nnewlines'],
        content: 'Content with\nmultiple\nlines,\ncommas,\nand special chars: àáâãäåæçèéêë\nñáéíóú'
      },
      {
        id: 'test3',
        name: 'Complex Name: "quotes" and \'apostrophes\'',
        description: 'Description with "double quotes" and \'single quotes\'\nplus newlines',
        tags: ['tag with "quotes"', 'tag with \'apostrophes\'', 'tag,with,commas\nand\nnewlines'],
        content: 'Complex content with:\n- Newlines\n- Commas, semicolons; colons:\n- Quotes: "double" and \'single\'\n- Special chars: @#$%^&*()[]{}'
      },
      {
        id: 'test4',
        name: 'JSON-like content',
        description: '{"key": "value", "array": [1,2,3]}\n{"another": "object"}',
        tags: ['json', 'object', 'array,string'],
        content: 'Content that looks like JSON: {"property": "value with, commas and\nnewlines"}'
      }
    ]

    // Insert test data
    for (const data of testData) {
      await db.insert(data)
    }
    await db.save()

    // Verify data integrity by reading from file
    const fileContent = fs.readFileSync(dbPath, 'utf8')
    const lines = fileContent.trim().split('\n')

    expect(lines.length).toBe(testData.length)

    // Verify each line is valid JSON
    for (const line of lines) {
      expect(() => JSON.parse(line)).not.toThrow()
    }

    // Test db.walk() - should not throw parsing errors
    let recordCount = 0
    for await (const record of db.walk({})) {
      expect(record).toBeDefined()
      expect(typeof record).toBe('object')
      expect(record.id).toBeDefined()
      expect(record.name).toBeDefined()

      // Find the original test data for this record
      const original = testData.find(d => d.id === record.id)
      expect(original).toBeDefined()

      // Verify that special characters are preserved (sanitization escapes but preserves meaning)
      expect(record.name).toBe(original.name)
      // Description with newlines should be escaped in storage but unescaped when read
      expect(record.description).toBe(original.description)
      expect(record.content).toBe(original.content)

      // Verify array fields (term mapping should preserve values)
      expect(Array.isArray(record.tags)).toBe(true)
      expect(record.tags.length).toBe(original.tags.length)

      recordCount++
    }

    expect(recordCount).toBe(testData.length)

    // Test querying with special characters
    const queryResult = await db.find({
      tags: 'tag,with,commas' // Query using term that contains commas
    })

    expect(queryResult.length).toBeGreaterThan(0)
    expect(queryResult[0].tags).toContain('tag,with,commas')
  })

  test('should handle term mapping with special characters in array values', async () => {
    const dbConfig = {
      create: true,
      fields: {
        id: 'string',
        categories: 'array:string',
        keywords: 'array:string'
      },
      indexes: {
        categories: 'array:string',
        keywords: 'array:string'
      }
    }

    db = new Database(dbPath, dbConfig)
    await db.init()

    const specialData = [
      {
        id: 'special1',
        categories: ['cat,with,commas', 'cat\nwith\nnewlines', 'cat "with quotes"'],
        keywords: ['key,with,commas', 'key\nwith\nnewlines']
      },
      {
        id: 'special2',
        categories: ['another,category', 'different\ncategory'],
        keywords: ['matching,key', 'different\nkeyword']
      }
    ]

    // Insert data
    for (const data of specialData) {
      await db.insert(data)
    }
    await db.save()

    // Test querying by category with commas
    const result1 = await db.find({
      categories: 'cat,with,commas'
    })
    expect(result1.length).toBe(1)
    expect(result1[0].id).toBe('special1')

    // Test querying by keyword with newlines
    const result2 = await db.find({
      keywords: 'key\nwith\nnewlines'
    })
    expect(result2.length).toBe(1)
    expect(result2[0].id).toBe('special1')

    // Test querying by category with quotes
    const result3 = await db.find({
      categories: 'cat "with quotes"'
    })
    expect(result3.length).toBe(1)
    expect(result3[0].id).toBe('special1')

    // Verify that walk preserves the original values
    for await (const record of db.walk({})) {
      const original = specialData.find(d => d.id === record.id)
      expect(original).toBeDefined()

      // Categories and keywords should be preserved exactly
      expect(record.categories).toEqual(original.categories)
      expect(record.keywords).toEqual(original.keywords)
    }
  })

  test('should handle edge case: strings ending with backslash before quotes', async () => {
    const dbConfig = {
      create: true,
      fields: {
        id: 'string',
        tricky: 'string',
        arrayField: 'array:string'
      },
      indexes: {
        arrayField: 'array:string'
      }
    }

    db = new Database(dbPath, dbConfig)
    await db.init()

    // Edge cases that could break JSON parsing
    const edgeCases = [
      {
        id: 'edge1',
        tricky: 'String ending with backslash\\',
        arrayField: ['backslash\\', 'comma,', 'newline\n']
      },
      {
        id: 'edge2',
        tricky: 'String with \\"escaped quotes\\" and backslash\\\\',
        arrayField: ['escaped "quotes"', 'backslash\\\\end']
      },
      {
        id: 'edge3',
        tricky: 'Very long string with multiple lines\nline2\nline3\nand commas, semicolons; and colons:',
        arrayField: ['multi\nline\narray', 'value,with,commas']
      }
    ]

    // Insert edge case data
    for (const data of edgeCases) {
      await db.insert(data)
    }
    await db.save()

    // Verify walk works
    let count = 0
    for await (const record of db.walk({})) {
      const original = edgeCases.find(e => e.id === record.id)
      expect(original).toBeDefined()

      expect(record.tricky).toBe(original.tricky)
      expect(record.arrayField).toEqual(original.arrayField)

      count++
    }

    expect(count).toBe(edgeCases.length)

    // Verify file contains valid JSON lines
    const fileContent = fs.readFileSync(dbPath, 'utf8')
    const lines = fileContent.trim().split('\n')

    for (const line of lines) {
      expect(() => JSON.parse(line)).not.toThrow()
    }
  })
})