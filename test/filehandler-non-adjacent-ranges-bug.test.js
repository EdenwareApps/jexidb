import { Database } from '../src/Database.mjs'
import fs from 'fs'
import path from 'path'

describe('FileHandler readGroupedRange Bug - Non-adjacent Ranges', () => {
  let testDir
  let db

  beforeEach(() => {
    testDir = path.join(process.cwd(), 'test-files', 'filehandler-bug')
    fs.mkdirSync(testDir, { recursive: true })
  })

  afterEach(async () => {
    if (db) {
      await db.destroy()
    }
    if (fs.existsSync(testDir)) {
      try {
        fs.rmSync(testDir, { recursive: true, force: true })
      } catch (error) {
        // Ignore cleanup errors
      }
    }
  })

  test('should detect deserialization error when readGroupedRange extracts incomplete lines', async () => {
    // This test directly reproduces the bug where substring() extracts incomplete lines
    // from non-adjacent ranges in the same buffer
    
    const dbPath = path.join(testDir, 'bug-reproduction.jdb')
    
    if (fs.existsSync(dbPath)) {
      fs.unlinkSync(dbPath)
    }
    if (fs.existsSync(dbPath + '.idx')) {
      fs.unlinkSync(dbPath + '.idx')
    }

    db = new Database(dbPath, {
      clear: true,
      create: true,
      fields: {
        id: 'number',
        url: 'string',
        name: 'string',
        logo: 'string',
        category: 'string',
        nameTerms: 'array:string'
      },
      indexes: {
        nameTerms: 'array:string'
      },
      debugMode: false
    })

    await db.init()

    // Create a scenario that will force non-adjacent ranges to be grouped together
    // Insert many records, with only a few matching the query
    // The matching records should be far apart to create large gaps
    
    const matchingRecords = [
      { id: 1, url: 'http://olxi0fko.ukminlt.fun/iptv/HV7NY9RSQCHYZK/1066/index.m3u8', name: 'Дорама', logo: '', category: 'кино', nameTerms: ['кино'] },
      { id: 50, url: 'http://olxi0fko.ukminlt.fun/iptv/HV7NY9RSQCHYZK/1066/index.m3u8', name: 'Дорама', logo: '', category: 'кино', nameTerms: ['кино'] },
      { id: 100, url: 'http://olxi0fko.ukminlt.fun/iptv/HV7NY9RSQCHYZK/1066/index.m3u8', name: 'Дорама', logo: '', category: 'кино', nameTerms: ['кино'] },
      { id: 150, url: 'http://olxi0fko.ukminlt.fun/iptv/HV7NY9RSQCHYZK/1066/index.m3u8', name: 'Дорама', logo: '', category: 'кино', nameTerms: ['кино'] },
      { id: 200, url: 'http://olxi0fko.ukminlt.fun/iptv/HV7NY9RSQCHYZK/1066/index.m3u8', name: 'Дорама', logo: '', category: 'кино', nameTerms: ['кино'] }
    ]

    // Insert non-matching records between them to create gaps
    for (let i = 1; i <= 200; i++) {
      if (matchingRecords.find(r => r.id === i)) {
        await db.insert(matchingRecords.find(r => r.id === i))
      } else {
        await db.insert({
          id: i,
          url: `http://example.com/${i}/index.m3u8`,
          name: `Channel ${i}`,
          logo: '',
          category: 'other',
          nameTerms: [`other${i}`]
        })
      }
    }

    await db.save()
    await db.close()

    // Reopen to force file-based reading
    db = new Database(dbPath, {
      fields: {
        id: 'number',
        url: 'string',
        name: 'string',
        logo: 'string',
        category: 'string',
        nameTerms: 'array:string'
      },
      indexes: {
        nameTerms: 'array:string'
      },
      debugMode: false
    })

    await db.init()

    // Query that returns only the 5 matching records (id: 1, 50, 100, 150, 200)
    // These are non-adjacent and will be grouped together if they fit in 512KB
    const query = { nameTerms: 'кино' }
    const results = []
    const errors = []

    try {
      for await (const record of db.walk(query)) {
        // Try to access the record - this will fail if deserialization was incomplete
        const recordStr = JSON.stringify(record)
        
        // Check if record is complete (has all expected fields)
        if (!record.id || !record.url || !record.name || !record.category || !record.nameTerms) {
          errors.push(`Incomplete record: ${recordStr.substring(0, 200)}`)
        }
        
        // Verify UTF-8 characters are intact
        if (record.name !== 'Дорама') {
          errors.push(`UTF-8 corruption: expected 'Дорама', got '${record.name}'`)
        }
        
        results.push(record)
      }
    } catch (error) {
      // This is the bug we're looking for!
      const errorMsg = error.message || ''
      
      if (errorMsg.includes('Failed to deserialize') || 
          errorMsg.includes('Unexpected non-whitespace') ||
          errorMsg.includes('JSON') ||
          errorMsg.includes('position')) {
        
        // FAIL THE TEST - bug detected!
        throw new Error(`🐛 BUG CONFIRMED: Deserialization error with non-adjacent ranges!\n\n` +
          `Error: ${errorMsg}\n\n` +
          `Root Cause: readGroupedRange() in FileHandler.mjs line 311 uses:\n` +
          `  content.substring(relativeStart, relativeEnd)\n\n` +
          `When multiple non-adjacent ranges are in the same buffer, substring() can extract:\n` +
          `1. Truncated lines (ending mid-UTF-8 character)\n` +
          `2. Multiple lines concatenated together\n` +
          `3. Part of one line + part of another\n\n` +
          `The fix must extract complete lines by finding newline boundaries.\n` +
          `Errors collected: ${errors.join('; ')}`)
      }
      
      // Re-throw other errors
      throw error
    }

    // If we get here, verify no errors occurred
    if (errors.length > 0) {
      throw new Error(`Validation errors detected:\n${errors.join('\n')}`)
    }

    // Should have found 5 records
    expect(results.length).toBe(5)
    
    // Verify all results are correct
    results.forEach(result => {
      expect(result.name).toBe('Дорама')
      expect(result.category).toBe('кино')
      expect(result.nameTerms).toContain('кино')
    })

    await db.destroy()
  })
})

