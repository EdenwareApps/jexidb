/**
 * Test sanitization of problematic characters that break JSON parsing
 * Based on the characters identified in jexidb-problematic-characters-report.md
 */

import { Database } from '../src/Database.mjs'
import fs from 'fs'
import path from 'path'

describe('Problematic Characters Sanitization', () => {
  let testDir
  let dbPath
  let db

  beforeEach(() => {
    testDir = path.join(process.cwd(), 'test-files', 'problematic-chars')
    fs.mkdirSync(testDir, { recursive: true })
    dbPath = path.join(testDir, `problematic-chars-${Date.now()}-${Math.random()}.jdb`)
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

  test('should sanitize control characters', async () => {
    const dbConfig = {
      create: true,
      fields: {
        id: 'string',
        name: 'string',
        description: 'string'
      }
    }

    db = new Database(dbPath, dbConfig)
    await db.init()

    // Test data with control characters that break JSON
    const problematicData = [
      {
        id: 'control1',
        name: 'Name\x00\x01\x02\x03\x04\x05\x06\x07\x08Test', // NUL, SOH, STX, ETX, EOT, ENQ, ACK, BEL
        description: 'Desc\x0B\x0C\x0ETest' // VT, FF, SO
      },
      {
        id: 'control2',
        name: 'Name\x0F\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1A\x1B\x1C\x1D\x1E\x1FTest', // More control chars
        description: 'Desc\x7F\x80\x81Test' // DEL and C1 controls
      }
    ]

    // Insert problematic data - should be sanitized automatically
    for (const data of problematicData) {
      await db.insert(data)
    }
    await db.save()

    // Verify data integrity by reading from file
    const fileContent = fs.readFileSync(dbPath, 'utf8')
    const lines = fileContent.trim().split('\n')

    expect(lines.length).toBe(problematicData.length)

    // Verify each line is valid JSON and control characters were removed
    for (const line of lines) {
      expect(() => JSON.parse(line)).not.toThrow()

      const parsed = JSON.parse(line)
      // JexiDB stores data in array format internally
      expect(Array.isArray(parsed)).toBe(true)
      expect(parsed.length).toBe(3) // id, name, description

      const [id, name, description] = parsed
      expect(name).not.toContain('\x00')
      expect(name).not.toContain('\x01')
      expect(description).not.toContain('\x0B')
      expect(description).not.toContain('\x7F')
    }

    // Test db.walk() - should not throw parsing errors
    let recordCount = 0
    for await (const record of db.walk({})) {
      expect(record).toBeDefined()
      expect(typeof record.name).toBe('string')
      expect(typeof record.description).toBe('string')

      // Control characters should be removed
      expect(record.name).not.toMatch(/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]/)
      expect(record.description).not.toMatch(/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]/)

      recordCount++
    }

    expect(recordCount).toBe(problematicData.length)
  })

  test('should escape quotes and backslashes', async () => {
    const dbConfig = {
      create: true,
      fields: {
        id: 'string',
        name: 'string',
        url: 'string'
      }
    }

    db = new Database(dbPath, dbConfig)
    await db.init()

    const problematicData = [
      {
        id: 'quotes1',
        name: 'Channel "Premium" HD',
        url: 'http://example.com/path'
      },
      {
        id: 'quotes2',
        name: 'Channel with "quotes" and \'apostrophes\'',
        url: 'http://example.com\\path\\with\\backslashes'
      },
      {
        id: 'quotes3',
        name: 'Complex "name" with \'both\' types',
        url: 'http://example.com/path?param="value"&other=\'test\''
      }
    ]

    // Insert data - should be sanitized automatically
    for (const data of problematicData) {
      await db.insert(data)
    }
    await db.save()

    // Verify JSON validity
    const fileContent = fs.readFileSync(dbPath, 'utf8')
    const lines = fileContent.trim().split('\n')

    for (const line of lines) {
      expect(() => JSON.parse(line)).not.toThrow()

      const parsed = JSON.parse(line)
      expect(Array.isArray(parsed)).toBe(true)
      expect(parsed.length).toBe(3) // id, name, url
    }

    // Test walk - at least one record should contain quotes and apostrophes
    let foundQuotes = false
    let foundApostrophes = false
    let recordCount = 0
    for await (const record of db.walk({})) {
      if (record.name.includes('"')) foundQuotes = true
      if (record.name.includes('\'')) foundApostrophes = true
      recordCount++
    }

    expect(foundQuotes).toBe(true) // At least one record should contain quotes
    expect(foundApostrophes).toBe(true) // At least one record should contain apostrophes

    expect(recordCount).toBe(problematicData.length)
  })

  test('should handle newlines and formatting characters', async () => {
    const dbConfig = {
      create: true,
      fields: {
        id: 'string',
        description: 'string',
        notes: 'string'
      }
    }

    db = new Database(dbPath, dbConfig)
    await db.init()

    const problematicData = [
      {
        id: 'newlines1',
        description: 'Line 1\nLine 2\nLine 3',
        notes: 'Note with\ttabs\tand\r\nCRLF\r\nsequences'
      },
      {
        id: 'newlines2',
        description: 'Description\nwith\nmultiple\nlines',
        notes: 'Mixed\t\n\rcontent'
      }
    ]

    // Insert data - should be sanitized automatically
    for (const data of problematicData) {
      await db.insert(data)
    }
    await db.save()

    // Verify JSON validity - should not break NDJSON format
    const fileContent = fs.readFileSync(dbPath, 'utf8')
    const lines = fileContent.trim().split('\n')

    expect(lines.length).toBe(problematicData.length)

    // Check that all lines are valid JSON and contain expected data
    for (const line of lines) {
      expect(() => JSON.parse(line)).not.toThrow()

      const parsed = JSON.parse(line)
      expect(Array.isArray(parsed)).toBe(true)
      expect(parsed.length).toBe(3) // id, description, notes

      const [id, description, notes] = parsed
      // After sanitization and JSON parsing, newlines should be preserved as literal characters
      expect(typeof description).toBe('string')
      expect(description).toContain('\n') // Should contain newlines
    }

    // Test walk
    let recordCount = 0
    let foundNewlines = false
    for await (const record of db.walk({})) {
      expect(typeof record.description).toBe('string')
      expect(typeof record.notes).toBe('string')
      // After sanitization, newlines should be preserved as literal characters
      if (record.description.includes('\n')) {
        foundNewlines = true
      }
      recordCount++
    }

    expect(foundNewlines).toBe(true) // At least one record should contain newlines

    expect(recordCount).toBe(problematicData.length)
  })

  test('should handle very long strings', async () => {
    const dbConfig = {
      create: true,
      fields: {
        id: 'string',
        longText: 'string'
      }
    }

    db = new Database(dbPath, dbConfig)
    await db.init()

    // Create a very long string (15,000 characters)
    const veryLongString = 'A'.repeat(15000) + ' with "quotes" and \n newlines'

    const testData = {
      id: 'long1',
      longText: veryLongString
    }

    // Insert data - should be truncated to 10,000 characters
    await db.insert(testData)
    await db.save()

    // Verify JSON validity
    const fileContent = fs.readFileSync(dbPath, 'utf8')
    const lines = fileContent.trim().split('\n')

    for (const line of lines) {
      expect(() => JSON.parse(line)).not.toThrow()

      const parsed = JSON.parse(line)
      expect(Array.isArray(parsed)).toBe(true)
      expect(parsed.length).toBe(2) // id, longText

      const [id, longText] = parsed
      expect(longText.length).toBeLessThanOrEqual(10000)
      // The string should be truncated and may or may not contain the quote part
      expect(typeof longText).toBe('string')
    }

    // Test walk
    for await (const record of db.walk({})) {
      expect(record.longText.length).toBeLessThanOrEqual(10000)
      expect(typeof record.longText).toBe('string')
    }
  })

  test('should handle arrays with problematic content', async () => {
    const dbConfig = {
      create: true,
      fields: {
        id: 'string',
        tags: 'array:string',
        categories: 'array:string'
      },
      indexes: {
        tags: 'array:string'
      }
    }

    db = new Database(dbPath, dbConfig)
    await db.init()

    const testData = {
      id: 'array1',
      tags: ['tag,with,commas', 'tag\nwith\nnewlines', 'tag"with"quotes', 'tag\\with\\backslashes'],
      categories: ['cat1', null, undefined, '', 'cat2'] // Should filter out null/undefined/empty
    }

    await db.insert(testData)
    await db.save()

    // Verify JSON validity
    const fileContent = fs.readFileSync(dbPath, 'utf8')
    const lines = fileContent.trim().split('\n')

    for (const line of lines) {
      expect(() => JSON.parse(line)).not.toThrow()

      const parsed = JSON.parse(line)
      expect(Array.isArray(parsed)).toBe(true)
      // Note: With array conversion, the structure might be different
      // The important thing is that it's valid JSON
    }

    // Test querying by tag with special characters
    const result = await db.find({
      tags: 'tag,with,commas'
    })
    expect(result.length).toBe(1)
    expect(result[0].id).toBe('array1')

    // Test walk
    for await (const record of db.walk({})) {
      expect(Array.isArray(record.tags)).toBe(true)
      expect(Array.isArray(record.categories)).toBe(true)
      expect(record.categories.length).toBe(2) // Only 'cat1' and 'cat2'
    }
  })

  test('should handle complex nested objects with problematic characters', async () => {
    const dbConfig = {
      create: true,
      fields: {
        id: 'string',
        metadata: 'string',
        config: 'string'
      }
    }

    db = new Database(dbPath, dbConfig)
    await db.init()

    const testData = {
      id: 'complex1',
      metadata: '{"server": "host\\path", "port": 8080, "desc": "Server with \\backslashes\\ and "quotes""}',
      config: 'Config with\nmultiple\nlines\nand "quotes"\nand\ttabs'
    }

    await db.insert(testData)
    await db.save()

    // Verify JSON validity
    const fileContent = fs.readFileSync(dbPath, 'utf8')
    const lines = fileContent.trim().split('\n')

    for (const line of lines) {
      expect(() => JSON.parse(line)).not.toThrow()

      const parsed = JSON.parse(line)
      expect(Array.isArray(parsed)).toBe(true)
      expect(parsed.length).toBe(3) // id, metadata, config

      const [id, metadata, config] = parsed
      expect(typeof metadata).toBe('string')
      expect(typeof config).toBe('string')
      // After sanitization and JSON parsing, special characters should be preserved
      expect(config).toContain('multiple')
      expect(config).toContain('lines')
      expect(config).toContain('quotes')
    }

    // Test walk
    for await (const record of db.walk({})) {
      expect(typeof record.metadata).toBe('string')
      expect(typeof record.config).toBe('string')
      // After sanitization, values should be preserved correctly
      expect(record.config).toContain('\n')
      expect(record.metadata).toContain('backslashes')
    }
  })
})