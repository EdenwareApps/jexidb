/**
 * Regression test for JexiDB walk() incomplete line reading bug
 *
 * This test ensures that db.walk() reads complete JSON lines without
 * causing "Expected ',' or ']'" and "Unterminated string" parsing errors.
 *
 * Bug Report: Incomplete Line Reading in db.walk()
 * https://github.com/jexidb/jexidb/issues/[issue-number]
 */

import { Database } from '../src/Database.mjs'
import fs from 'fs'
import path from 'path'

describe('Regression Test: db.walk() Incomplete Line Reading', () => {
  let testDir
  let dbPath
  let db

  beforeEach(() => {
    testDir = path.join(process.cwd(), 'test-files', 'walk-regression')
    fs.mkdirSync(testDir, { recursive: true })
    dbPath = path.join(testDir, `walk-regression-${Date.now()}-${Math.random()}.jdb`)
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

  test('should read complete JSON lines without parsing errors', async () => {
    const dbConfig = {
      create: true,
      fields: {
        url: 'string',
        name: 'string',
        groups: 'array:string',
        metadata: 'string'
      }
    }

    // Generate test data with complex JSON that could trigger the bug
    const testData = [
      {
        url: 'http://example.com/1',
        name: 'Simple Channel',
        groups: ['Group A'],
        metadata: 'simple'
      },
      {
        url: 'http://example.com/2',
        name: 'Channel with "quotes" in name',
        groups: ['Group A', 'Group B'],
        metadata: 'with quotes'
      },
      {
        url: 'http://example.com/3',
        name: 'Channel with special chars: àáâãäåæçèéêë',
        groups: ['Special Chars', 'Unicode'],
        metadata: 'unicode chars'
      },
      {
        url: 'http://example.com/4',
        name: 'Very long channel name that might cause buffer issues when reading from disk storage files',
        groups: ['Long Names', 'Buffer Test', 'Edge Cases'],
        metadata: 'long name test'
      },
      {
        url: 'http://example.com/5',
        name: 'Channel with commas, and other punctuation! @#$%^&*()',
        groups: ['Punctuation', 'Special', 'Symbols'],
        metadata: 'punctuation test'
      },
      {
        url: 'http://example.com/6',
        name: 'Nested "quotes" and \'apostrophes\' everywhere',
        groups: ['Nested', 'Quotes', 'Complex'],
        metadata: 'nested quotes'
      }
    ]

    // Create and initialize database
    db = new Database(dbPath, dbConfig)
    await db.init()

    // Insert test data
    for (const data of testData) {
      await db.insert(data)
    }
    await db.save()

    // Test db.walk() - this should NOT throw JSON parsing errors
    let successCount = 0
    let errorCount = 0
    const errors = []

    for await (const record of db.walk({})) {
      try {
        // Validate record structure
        expect(record).toBeDefined()
        expect(typeof record).toBe('object')
        expect(record.name).toBeDefined()
        expect(record.url).toBeDefined()

        successCount++
      } catch (err) {
        errorCount++
        errors.push(`Walk iteration error: ${err.message}`)
      }
    }

    // Assert no errors occurred
    expect(errorCount).toBe(0)
    expect(errors.length).toBe(0)
    expect(successCount).toBe(testData.length)

    // Verify data integrity on disk
    const fileContent = fs.readFileSync(dbPath, 'utf8')
    const lines = fileContent.trim().split('\n')

    expect(lines.length).toBe(testData.length)

    // Verify all lines are valid JSON
    let validLines = 0
    let invalidLines = 0

    for (const line of lines) {
      if (!line.trim()) continue

      try {
        JSON.parse(line)
        validLines++
      } catch (err) {
        invalidLines++
        console.error(`Invalid JSON line: ${err.message}`)
        console.error(`Content: ${line.substring(0, 100)}...`)
      }
    }

    expect(validLines).toBe(testData.length)
    expect(invalidLines).toBe(0)
  })

  test('should handle edge cases with complex JSON structures', async () => {
    const dbConfig = {
      create: true,
      fields: {
        id: 'string',
        data: 'array:string',
        nested: 'string'
      }
    }

    const edgeCaseData = [
      {
        id: 'edge1',
        data: ['simple'],
        nested: '{"key": "value"}'
      },
      {
        id: 'edge2',
        data: ['with "quotes"', 'with \'single quotes\'', 'with,commas', 'with\\backslashes'],
        nested: '{"complex": "value with \\"quotes\\" and \\\\backslashes"}'
      },
      {
        id: 'edge3',
        data: ['multiline\ncontent', 'unicode: ñáéíóú', 'special: @#$%^&*()'],
        nested: '{"array": [1,2,3], "object": {"nested": true}}'
      }
    ]

    db = new Database(dbPath, dbConfig)
    await db.init()

    // Insert edge case data
    for (const data of edgeCaseData) {
      await db.insert(data)
    }
    await db.save()

    // Walk through data - should not throw parsing errors
    let count = 0
    for await (const record of db.walk({})) {
      expect(record).toBeDefined()
      expect(record.id).toBeDefined()
      expect(Array.isArray(record.data)).toBe(true)
      count++
    }

    expect(count).toBe(edgeCaseData.length)
  })
});