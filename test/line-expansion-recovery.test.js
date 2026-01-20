/**
 * Test line expansion recovery - ensures truncated JSON lines are recovered
 * instead of being lost during deserialization errors
 */

import { Database } from '../src/Database.mjs'
import fs from 'fs'
import path from 'path'

describe('Line Expansion Recovery', () => {
  let testDir
  let dbPath
  let db

  beforeEach(() => {
    testDir = path.join(process.cwd(), 'test-files', 'line-expansion')
    fs.mkdirSync(testDir, { recursive: true })
    dbPath = path.join(testDir, `line-expansion-${Date.now()}-${Math.random()}.jdb`)
  })

  afterEach(async () => {
    if (db) {
      await db.destroy()
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

  test('should recover truncated JSON lines by expanding read until newline', async () => {
    const dbConfig = {
      create: true,
      fields: {
        id: 'string',
        data: 'string',
        metadata: 'string'
      }
    }

    db = new Database(dbPath, dbConfig)
    await db.init()

    // Create test data with long, complex JSON that might be truncated by range reading
    const testData = [
      {
        id: 'record1',
        data: 'Simple data',
        metadata: 'Simple metadata'
      },
      {
        id: 'record2',
        data: 'Data with "quotes" and special chars àáâãäåæçèéêë',
        metadata: 'Metadata with numbers 123456789 and symbols @#$%^&*()'
      },
      {
        id: 'record3',
        data: 'Very long data that might cause truncation issues when reading with fixed buffer sizes and range calculations',
        metadata: '{"nested": {"object": "with", "multiple": "levels", "and": ["arrays", "too"]}}'
      },
      {
        id: 'record4',
        data: 'Another long entry with commas, semicolons; colons: and various punctuation marks!',
        metadata: 'Final metadata entry with more data to ensure proper range boundary testing'
      }
    ]

    // Insert test data
    for (const data of testData) {
      await db.insert(data)
    }
    await db.save()

    // Force close and reopen to ensure file-based reading
    await db.close()

    db = new Database(dbPath, {
      fields: {
        id: 'string',
        data: 'string',
        metadata: 'string'
      }
    })
    await db.init()

    // Test walk - should recover all entries even if some ranges are truncated
    let recordCount = 0
    const foundIds = []

    for await (const record of db.walk({})) {
      expect(record).toBeDefined()
      expect(typeof record).toBe('object')
      expect(record.id).toBeDefined()
      expect(record.data).toBeDefined()
      expect(record.metadata).toBeDefined()

      // Verify the data matches what we inserted
      const original = testData.find(d => d.id === record.id)
      expect(original).toBeDefined()
      expect(record.data).toBe(original.data)
      expect(record.metadata).toBe(original.metadata)

      foundIds.push(record.id)
      recordCount++
    }

    // Should have found all records
    expect(recordCount).toBe(testData.length)
    expect(foundIds.sort()).toEqual(['record1', 'record2', 'record3', 'record4'].sort())

    console.log(`✅ Successfully recovered ${recordCount} records through line expansion`)
  })

  test('should handle edge case where line expansion reaches EOF', async () => {
    const dbConfig = {
      create: true,
      fields: {
        id: 'string',
        content: 'string'
      }
    }

    db = new Database(dbPath, dbConfig)
    await db.init()

    // Create data where the last line might not end with newline
    const testData = [
      {
        id: 'first',
        content: 'First line content'
      },
      {
        id: 'last',
        content: 'Last line content without trailing newline'
      }
    ]

    // Insert data
    for (const data of testData) {
      await db.insert(data)
    }
    await db.save()

    // Manually remove the trailing newline from the last line to simulate truncation
    const fileContent = fs.readFileSync(dbPath, 'utf8')
    const lines = fileContent.split('\n')
    if (lines.length > 1 && lines[lines.length - 2].trim()) {
      // Remove newline from second-to-last line (which should be the last data line)
      const modifiedContent = lines.slice(0, -2).join('\n') + '\n' + lines[lines.length - 2] + lines[lines.length - 1]
      fs.writeFileSync(dbPath, modifiedContent)
    }

    // Force close and reopen
    await db.close()

    db = new Database(dbPath, {
      fields: {
        id: 'string',
        content: 'string'
      }
    })
    await db.init()

    // Test walk - should handle EOF gracefully
    let recordCount = 0
    for await (const record of db.walk({})) {
      expect(record).toBeDefined()
      recordCount++
    }

    // Should still read the records (the system should handle EOF gracefully)
    expect(recordCount).toBeGreaterThan(0)
    console.log(`✅ Handled EOF gracefully, read ${recordCount} records`)
  })

  test('should prevent infinite reading with malformed files', async () => {
    const dbConfig = {
      create: true,
      fields: {
        id: 'string',
        data: 'string'
      }
    }

    db = new Database(dbPath, dbConfig)
    await db.init()

    // Create a simple record
    const testData = {
      id: 'test',
      data: 'test data'
    }

    await db.insert(testData)
    await db.save()

    // Manually corrupt the file by adding incomplete JSON without newline
    let fileContent = fs.readFileSync(dbPath, 'utf8')
    fileContent = fileContent.trim() + '{"incomplete": "json"' // Add incomplete JSON
    fs.writeFileSync(dbPath, fileContent)

    // Force close and reopen
    await db.close()

    db = new Database(dbPath, {
      fields: {
        id: 'string',
        data: 'string'
      }
    })
    await db.init()

    // Test walk - should not hang indefinitely and should read valid records
    let recordCount = 0
    let startTime = Date.now()

    try {
      for await (const record of db.walk({})) {
        expect(record).toBeDefined()
        recordCount++
      }
    } catch (error) {
      // Should not hang, but might throw error for malformed data
      console.log(`Expected error for malformed data: ${error.message}`)
    }

    let endTime = Date.now()
    let duration = endTime - startTime

    // Should complete within reasonable time (not hang)
    expect(duration).toBeLessThan(5000) // 5 seconds max
    console.log(`✅ Completed in ${duration}ms without hanging`)
  })
})