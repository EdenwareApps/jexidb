/**
 * $not Operator with $and on Array Fields Test
 * 
 * Bug Report: https://github.com/yourrepo/jexidb/issues/XXX
 * 
 * Issue: When using $not with $and on array fields in strict mode,
 * queries return empty results even when matching documents exist.
 * 
 * Root Cause: IndexManager.query() did not handle the $not operator,
 * treating it as an unknown field and returning an empty set.
 * Additionally, when fields existed at both root level and inside $and,
 * only the $and conditions were being processed.
 * 
 * Fix: Added proper $not handling in IndexManager.query() that:
 * 1. Gets all possible line numbers from database offsets
 * 2. Queries for the $not condition
 * 3. Returns the complement (all lines except those matching $not)
 * 4. Intersects with other root-level conditions if present
 * Also fixed $and to properly intersect with root-level fields.
 */

import { Database } from '../src/Database.mjs'
import { describe, it, expect, beforeEach, afterEach } from '@jest/globals'
import fs from 'fs'

describe('$not Operator with $and on Array Fields', () => {
  let db
  const testFile = './test-files/not-operator-test.jdb'
  const testIdxFile = './test-files/not-operator-test.idx.jdb'

  beforeEach(async () => {
    // Clean up test files
    try {
      if (fs.existsSync(testFile)) fs.unlinkSync(testFile)
      if (fs.existsSync(testIdxFile)) fs.unlinkSync(testIdxFile)
    } catch (err) {
      // Ignore cleanup errors
    }

    // Create database with array field
    db = new Database(testFile, {
      clear: true,
      create: true,
      integrityCheck: 'none',
      indexedQueryMode: 'strict',
      fields: {
        name: 'string',
        nameTerms: 'array:string',
      },
      indexes: ['name', 'nameTerms']
    })

    await db.init()

    // Insert test data
    const testData = [
      { name: 'SBT Nacional', nameTerms: ['sbt'] },
      { name: 'SBT HD', nameTerms: ['sbt'] },
      { name: 'SBT Radio', nameTerms: ['sbt', 'radio'] },
      { name: 'SBT FM', nameTerms: ['sbt', 'fm'] },
      { name: 'Radio FM', nameTerms: ['radio', 'fm'] },
      { name: 'Globo', nameTerms: ['globo'] },
    ]

    for (const doc of testData) {
      await db.insert(doc)
    }

    await db.flush()
    await db.close()

    // Re-open database
    db = new Database(testFile, {
      create: false,
      integrityCheck: 'none',
      indexedQueryMode: 'strict',
      fields: {
        name: 'string',
        nameTerms: 'array:string',
      },
      indexes: ['name', 'nameTerms']
    })
    
    await db.init()
  })

  afterEach(async () => {
    if (db && !db.destroyed) {
      try {
        await db.destroy()
      } catch (err) {
        // Ignore destroy errors
      }
    }

    // Clean up test files
    try {
      if (fs.existsSync(testFile)) fs.unlinkSync(testFile)
      if (fs.existsSync(testIdxFile)) fs.unlinkSync(testIdxFile)
    } catch (err) {
      // Ignore cleanup errors
    }
  })

  it('should handle $not with $and (positive condition first)', async () => {
    const query = {
      $and: [
        { nameTerms: { $in: ['sbt'] } },
        { $not: { nameTerms: { $in: ['radio', 'fm'] } } }
      ]
    }

    const results = await db.find(query)
    
    expect(results).toHaveLength(2)
    expect(results[0].name).toBe('SBT Nacional')
    expect(results[1].name).toBe('SBT HD')
  })

  it('should handle $not with $and (negative condition first)', async () => {
    const query = {
      $and: [
        { $not: { nameTerms: { $in: ['radio', 'fm'] } } },
        { nameTerms: { $in: ['sbt'] } }
      ]
    }

    const results = await db.find(query)
    
    expect(results).toHaveLength(2)
    expect(results[0].name).toBe('SBT Nacional')
    expect(results[1].name).toBe('SBT HD')
  })

  it('should handle $not WITHOUT $and (root level)', async () => {
    const query = {
      nameTerms: { $in: ['sbt'] },
      $not: { nameTerms: { $in: ['radio', 'fm'] } }
    }

    const results = await db.find(query)
    
    expect(results).toHaveLength(2)
    expect(results[0].name).toBe('SBT Nacional')
    expect(results[1].name).toBe('SBT HD')
  })

  it('should handle multiple $not in $and with root-level field', async () => {
    const query = {
      nameTerms: { $in: ['sbt'] },
      $and: [
        { $not: { nameTerms: 'radio' } },
        { $not: { nameTerms: 'fm' } }
      ]
    }

    const results = await db.find(query)
    
    expect(results).toHaveLength(2)
    expect(results[0].name).toBe('SBT Nacional')
    expect(results[1].name).toBe('SBT HD')
  })

  it('should handle $not with single value', async () => {
    const query = {
      $and: [
        { nameTerms: { $in: ['sbt'] } },
        { $not: { nameTerms: 'radio' } }
      ]
    }

    const results = await db.find(query)
    
    expect(results).toHaveLength(3)
    const names = results.map(r => r.name).sort()
    expect(names).toEqual(['SBT FM', 'SBT HD', 'SBT Nacional'])
  })

  it('should handle complex $not queries with multiple conditions', async () => {
    const query = {
      $and: [
        { nameTerms: { $in: ['sbt', 'globo'] } },
        { $not: { nameTerms: { $in: ['radio', 'fm'] } } }
      ]
    }

    const results = await db.find(query)
    
    expect(results).toHaveLength(3)
    const names = results.map(r => r.name).sort()
    expect(names).toEqual(['Globo', 'SBT HD', 'SBT Nacional'])
  })

  it('should handle $not that excludes all results', async () => {
    const query = {
      $and: [
        { nameTerms: { $in: ['sbt'] } },
        { $not: { nameTerms: 'sbt' } }
      ]
    }

    const results = await db.find(query)
    
    expect(results).toHaveLength(0)
  })

  it('should handle $not with non-existent values', async () => {
    const query = {
      $and: [
        { nameTerms: { $in: ['sbt'] } },
        { $not: { nameTerms: { $in: ['nonexistent', 'invalid'] } } }
      ]
    }

    const results = await db.find(query)
    
    expect(results).toHaveLength(4)
    const names = results.map(r => r.name).sort()
    expect(names).toEqual(['SBT FM', 'SBT HD', 'SBT Nacional', 'SBT Radio'])
  })

  it('should handle $nin operator in strict mode', async () => {
    const query = {
      nameTerms: { $nin: ['radio', 'fm'] }
    }

    const results = await db.find(query)
    
    expect(results).toHaveLength(3)
    const names = results.map(r => r.name).sort()
    expect(names).toEqual(['Globo', 'SBT HD', 'SBT Nacional'])
  })

  it('should handle $nin with $in in strict mode', async () => {
    const query = {
      $and: [
        { nameTerms: { $in: ['sbt'] } },
        { nameTerms: { $nin: ['radio', 'fm'] } }
      ]
    }

    const results = await db.find(query)
    
    expect(results).toHaveLength(2)
    expect(results[0].name).toBe('SBT Nacional')
    expect(results[1].name).toBe('SBT HD')
  })

  it('should handle $nin with single value', async () => {
    const query = {
      nameTerms: { $nin: ['radio'] }
    }

    const results = await db.find(query)
    
    expect(results).toHaveLength(4)
    const names = results.map(r => r.name).sort()
    expect(names).toEqual(['Globo', 'SBT FM', 'SBT HD', 'SBT Nacional'])
  })

  it('should produce same results for $nin and $not+$in', async () => {
    // Query with $nin
    const ninQuery = {
      nameTerms: { $nin: ['radio', 'fm'] }
    }
    
    // Equivalent query with $not + $in
    const notQuery = {
      $not: { nameTerms: { $in: ['radio', 'fm'] } }
    }

    const ninResults = await db.find(ninQuery)
    const notResults = await db.find(notQuery)
    
    expect(ninResults).toHaveLength(notResults.length)
    
    const ninNames = ninResults.map(r => r.name).sort()
    const notNames = notResults.map(r => r.name).sort()
    expect(ninNames).toEqual(notNames)
  })
})

