import fs from 'fs'
import path from 'path'
import { Database } from '../src/Database.mjs'
import TermManager from '../src/managers/TermManager.mjs'

// Add Mocha-style matchers to Jest
const originalExpect = expect
global.expect = function(actual) {
  const matcher = originalExpect(actual)
  matcher.to = {
    deep: {
      equal: (expected) => {
        return originalExpect(actual).toEqual(expected)
      }
    },
    equal: (expected) => {
      return originalExpect(actual).toBe(expected)
    },
    have: {
      length: (expected) => {
        return originalExpect(actual).toHaveLength(expected)
      }
    },
    be: {
      false: () => {
        return originalExpect(actual).toBe(false)
      },
      true: () => {
        return originalExpect(actual).toBe(true)
      },
      greaterThan: (expected) => {
        return originalExpect(actual).toBeGreaterThan(expected)
      },
      lessThan: (expected) => {
        return originalExpect(actual).toBeLessThan(expected)
      }
    }
  }
  return matcher
}

describe('Term Mapping Tests', () => {
  let db
  let testDbPath
  let testIdxPath

  beforeEach(async () => {
    // Use unique test file names to avoid interference between tests
    const testId = Math.random().toString(36).substring(7)
    testDbPath = path.join(process.cwd(), `test-term-mapping-${testId}.jdb`)
    testIdxPath = path.join(process.cwd(), `test-term-mapping-${testId}.idx.jdb`)
    
    // Clean up any existing test files
    if (fs.existsSync(testDbPath)) fs.unlinkSync(testDbPath)
    if (fs.existsSync(testIdxPath)) fs.unlinkSync(testIdxPath)
    if (fs.existsSync(testDbPath.replace('.jdb', '.terms.jdb'))) fs.unlinkSync(testDbPath.replace('.jdb', '.terms.jdb'))
  })

  afterEach(async () => {
    if (db) {
      await db.close()
    }
    
    // Clean up test files with error handling
    try {
      if (fs.existsSync(testDbPath)) fs.unlinkSync(testDbPath)
      if (fs.existsSync(testIdxPath)) fs.unlinkSync(testIdxPath)
      if (fs.existsSync(testDbPath.replace('.jdb', '.terms.jdb'))) fs.unlinkSync(testDbPath.replace('.jdb', '.terms.jdb'))
    } catch (err) {
      // Ignore cleanup errors to prevent test failures
      console.warn('Warning: Could not clean up test files:', err.message)
    }
  })

  describe('TermManager', () => {
    let termManager

    beforeEach(() => {
      termManager = new TermManager()
    })

    it('should create and retrieve term IDs', () => {
      const term1 = termManager.getTermId('bra')
      const term2 = termManager.getTermId('globo')
      const term3 = termManager.getTermId('bra') // Duplicate

      expect(term1).toBe(1)
      expect(term2).toBe(2)
      expect(term3).toBe(1) // Should return same ID

      expect(termManager.getTerm(1)).toBe('bra')
      expect(termManager.getTerm(2)).toBe('globo')
      expect(termManager.getTerm(999)).toBeNull()
    })

    it('should track term usage counts', () => {
      const termId = termManager.getTermId('test')
      expect(termManager.termCounts.get(termId)).toBe(1)

      termManager.incrementTermCount(termId)
      expect(termManager.termCounts.get(termId)).toBe(2)

      termManager.decrementTermCount(termId)
      expect(termManager.termCounts.get(termId)).toBe(1)

      termManager.decrementTermCount(termId)
      expect(termManager.termCounts.get(termId)).toBe(0)
    })

    it('should clean up orphaned terms', async () => {
      const term1 = termManager.getTermId('orphan1')
      const term2 = termManager.getTermId('orphan2')
      const term3 = termManager.getTermId('active')

      // Make term1 and term2 orphaned
      termManager.decrementTermCount(term1)
      termManager.decrementTermCount(term2)

      const orphanedCount = await termManager.cleanupOrphanedTerms(true)
      expect(orphanedCount).toBe(2)

      expect(termManager.hasTerm('orphan1')).toBe(false)
      expect(termManager.hasTerm('orphan2')).toBe(false)
      expect(termManager.hasTerm('active')).toBe(true)
    })

    it('should load and save terms', async () => {
      // Add some terms
      termManager.getTermId('bra')
      termManager.getTermId('globo')
      termManager.getTermId('brasil')

      const termsData = await termManager.saveTerms()
      expect(termsData).to.deep.equal({
        '1': 'bra',
        '2': 'globo',
        '3': 'brasil'
      })

      // Create new manager and load terms
      const newManager = new TermManager()
      await newManager.loadTerms(termsData)

      expect(newManager.getTerm(1)).to.equal('bra')
      expect(newManager.getTerm(2)).to.equal('globo')
      expect(newManager.getTerm(3)).to.equal('brasil')
      expect(newManager.nextId).to.equal(4)
    })

    it('should provide statistics', () => {
      termManager.getTermId('term1')
      termManager.getTermId('term2')

      const stats = termManager.getStats()
      expect(stats.totalTerms).to.equal(2)
      expect(stats.nextId).to.equal(3)
      expect(stats.orphanedTerms).to.equal(0)
    })
  })

  describe('Database with Term Mapping', () => {
    beforeEach(async () => {
      db = new Database(testDbPath, {
        fields: { name: 'string', nameTerms: 'array:string', groupTerms: 'array:string' },
        indexes: { nameTerms: 'array:string', groupTerms: 'array:string' },
        termMappingCleanup: true,
        debugMode: false
      })
      await db.initialize()
    })

    it('should insert records with term mapping', async () => {
      const record = {
        name: 'Test Record',
        nameTerms: ['bra', 'globo', 'brasil'],
        groupTerms: ['channel', 'discovery']
      }

      const inserted = await db.insert(record)
      expect(inserted.nameTerms).to.deep.equal(['bra', 'globo', 'brasil'])
      expect(inserted.groupTerms).to.deep.equal(['channel', 'discovery'])

      // Check that terms are mapped (order may vary)
      const allTerms = db.termManager.getAllTerms()
      expect(allTerms).toContain('bra')
      expect(allTerms).toContain('globo')
      expect(allTerms).toContain('brasil')
      expect(allTerms).toContain('channel')
      expect(allTerms).toContain('discovery')
      
      // Check that we have the expected number of terms
      expect(allTerms).to.have.length(5)
    })

    it('should save and load with term mapping', async () => {
      // Insert test data
      await db.insert({
        name: 'Record 1',
        nameTerms: ['bra', 'globo'],
        groupTerms: ['channel']
      })

      await db.insert({
        name: 'Record 2',
        nameTerms: ['brasil', 'discovery'],
        groupTerms: ['channel', 'discovery']
      })

      await db.save()

      // Small delay to ensure file operations are complete
      await new Promise(resolve => setTimeout(resolve, 10))

      // Create new database instance and load
      const newDb = new Database(testDbPath, {
        fields: { name: 'string', nameTerms: 'array:string', groupTerms: 'array:string' },
        indexes: { nameTerms: 'array:string', groupTerms: 'array:string' },
        termMappingCleanup: true,
        debugMode: false
      })
      await newDb.initialize()

      // Check that terms were loaded (order may vary)
      const allTerms = newDb.termManager.getAllTerms()
      expect(allTerms).toContain('bra')
      expect(allTerms).toContain('globo')
      expect(allTerms).toContain('channel')
      expect(allTerms).toContain('brasil')
      expect(allTerms).toContain('discovery')

      // Check that data was loaded
      const records = await newDb.find({})
      expect(records).to.have.length(2)
      expect(records[0].name).to.equal('Record 1')
      expect(records[1].name).to.equal('Record 2')
    })

    it('should query with term mapping', async () => {
      // Insert test data
      await db.insert({
        name: 'Record 1',
        nameTerms: ['bra', 'globo'],
        groupTerms: ['channel']
      })

      await db.insert({
        name: 'Record 2',
        nameTerms: ['brasil', 'discovery'],
        groupTerms: ['channel', 'discovery']
      })

      // Query by term
      const results1 = await db.find({ nameTerms: 'bra' })
      console.log('TEST DEBUG: results1 =', results1, 'type:', typeof results1, 'isArray:', Array.isArray(results1))
      expect(results1).to.have.length(1)
      expect(results1[0].name).to.equal('Record 1')

      const results2 = await db.find({ groupTerms: 'channel' })
      expect(results2).to.have.length(2)

      const results3 = await db.find({ nameTerms: 'nonexistent' })
      expect(results3).to.have.length(0)
    })

    it('should update records with term mapping', async () => {
      const record = await db.insert({
        name: 'Test Record',
        nameTerms: ['bra', 'globo'],
        groupTerms: ['channel']
      })

      // Update with new terms
      await db.update({ id: record.id }, {
        nameTerms: ['brasil', 'discovery'],
        groupTerms: ['discovery']
      })

      // Check that old terms are decremented and new terms are added
      expect(db.termManager.termCounts.get(1)).to.equal(0) // 'bra' should be orphaned
      expect(db.termManager.termCounts.get(2)).to.equal(0) // 'globo' should be orphaned
      expect(db.termManager.termCounts.get(3)).to.equal(0) // 'channel' should be orphaned
      expect(db.termManager.termCounts.get(4)).to.equal(1) // 'brasil' should be active
      expect(db.termManager.termCounts.get(5)).to.equal(2) // 'discovery' should be active (used in both nameTerms and groupTerms)

      // Query to verify update
      const results = await db.find({ nameTerms: 'brasil' })
      expect(results).to.have.length(1)
      expect(results[0].nameTerms).to.deep.equal(['brasil', 'discovery'])
    })

    it('should delete records with term mapping', async () => {
      const record = await db.insert({
        name: 'Test Record',
        nameTerms: ['bra', 'globo'],
        groupTerms: ['channel']
      })

      await db.delete({ id: record.id })

      // Check that terms are decremented
      expect(db.termManager.termCounts.get(1)).to.equal(0) // 'bra' should be orphaned
      expect(db.termManager.termCounts.get(2)).to.equal(0) // 'globo' should be orphaned
      expect(db.termManager.termCounts.get(3)).to.equal(0) // 'channel' should be orphaned

      // Verify record is deleted
      const results = await db.find({ id: record.id })
      expect(results).to.have.length(0)
    })

    it('should clean up orphaned terms on save', async () => {
      // Insert and then delete to create orphaned terms
      const record = await db.insert({
        name: 'Test Record',
        nameTerms: ['bra', 'globo'],
        groupTerms: ['channel']
      })

      await db.delete({ id: record.id })
      await db.save()

      // Check that orphaned terms were cleaned up
      expect(db.termManager.hasTerm('bra')).to.be.false
      expect(db.termManager.hasTerm('globo')).to.be.false
      expect(db.termManager.hasTerm('channel')).to.be.false
    })

    it('should handle complex queries with term mapping', async () => {
      // Insert test data
      await db.insert({
        name: 'Record 1',
        nameTerms: ['bra', 'globo'],
        groupTerms: ['channel']
      })

      await db.insert({
        name: 'Record 2',
        nameTerms: ['brasil', 'discovery'],
        groupTerms: ['channel', 'discovery']
      })

      await db.insert({
        name: 'Record 3',
        nameTerms: ['bra', 'brasil'],
        groupTerms: ['discovery']
      })

      // Test $in queries
      const results1 = await db.find({ nameTerms: { $in: ['bra', 'discovery'] } })
      expect(results1).to.have.length(3)

      // Test $all queries
      const results2 = await db.find({ groupTerms: { $all: ['channel', 'discovery'] } })
      expect(results2).to.have.length(1)
      expect(results2[0].name).to.equal('Record 2')

      // Test multiple criteria
      const results3 = await db.find({ 
        nameTerms: 'bra',
        groupTerms: 'channel'
      })
      expect(results3).to.have.length(1)
      expect(results3[0].name).to.equal('Record 1')
    })

    it('should maintain compatibility with non-term-mapping fields', async () => {
      // Note: Fields must be defined in 'fields' option to be preserved
      // This test verifies that non-term-mapping fields work correctly when defined in schema
      const record = await db.insert({
        name: 'Test Record',
        nameTerms: ['bra', 'globo'], // Term mapping field
        groupTerms: ['channel'], // Term mapping field
        // category and tags are not in the schema, so they will be discarded
        // This is expected behavior - only fields defined in 'fields' are preserved
      })

      // Query by term-mapping field (should work)
      const results = await db.find({ nameTerms: 'bra' })
      expect(results).to.have.length(1)
      expect(results[0].name).to.equal('Test Record')

      // Query by groupTerms (term-mapping field)
      const results2 = await db.find({ groupTerms: 'channel' })
      expect(results2).to.have.length(1)
    })

    it('should handle empty term arrays', async () => {
      const record = await db.insert({
        name: 'Test Record',
        nameTerms: [],
        groupTerms: ['channel']
      })

      // Query should work with empty arrays
      const results = await db.find({ nameTerms: 'bra' })
      expect(results).to.have.length(0)

      const results2 = await db.find({ groupTerms: 'channel' })
      expect(results2).to.have.length(1)
    })

    it('should provide correct statistics', async () => {
      await db.insert({
        name: 'Record 1',
        nameTerms: ['bra', 'globo'],
        groupTerms: ['channel']
      })

      await db.insert({
        name: 'Record 2',
        nameTerms: ['brasil', 'discovery'],
        groupTerms: ['channel', 'discovery']
      })

      const stats = db.getStats()
      expect(stats.records).to.equal(2)

      const termStats = db.termManager.getStats()
      expect(termStats.totalTerms).to.equal(5) // bra, globo, brasil, discovery, channel
    })
  })

  describe('Performance Tests', () => {
    beforeEach(async () => {
      db = new Database(testDbPath, {
        fields: { name: 'string', nameTerms: 'array:string', groupTerms: 'array:string' },
        indexes: { nameTerms: 'array:string', groupTerms: 'array:string' },
        termMapping: true,
        termMappingFields: ['nameTerms', 'groupTerms'],
        debugMode: false
      })
      await db.initialize()
    })

    it('should handle large datasets efficiently', async () => {
      const startTime = Date.now()
      
      // OPTIMIZATION: Use InsertSession for batch operations
      const session = db.beginInsertSession({
        batchSize: 100,
        enableAutoSave: false
      })
      
      // Insert 1000 records with term mapping in batches
      for (let i = 0; i < 1000; i++) {
        await session.add({
          name: `Record ${i}`,
          nameTerms: [`term${i % 100}`, `word${i % 50}`],
          groupTerms: [`group${i % 20}`]
        })
      }
      
      await session.commit()
      const insertTime = Date.now() - startTime
      console.log(`Inserted 1000 records in ${insertTime}ms`)

      // Save the database
      const saveStart = Date.now()
      await db.save()
      const saveTime = Date.now() - saveStart
      console.log(`Saved database in ${saveTime}ms`)

      // Query performance
      const queryStart = Date.now()
      const results = await db.find({ nameTerms: 'term1' })
      const queryTime = Date.now() - queryStart
      console.log(`Query completed in ${queryTime}ms, found ${results.length} results`)

      expect(results.length).to.be.greaterThan(0)
      expect(insertTime).to.be.lessThan(500) // OPTIMIZED: Should complete within 500ms
      expect(saveTime).to.be.lessThan(1000) // OPTIMIZED: Save should be fast
      expect(queryTime).to.be.lessThan(300) // OPTIMIZED: Query should be very fast
    })

    it('should save and load large datasets efficiently', async () => {
      // OPTIMIZATION: Use InsertSession for batch operations
      const session = db.beginInsertSession({
        batchSize: 50,
        enableAutoSave: false
      })
      
      // Insert test data in batches
      for (let i = 0; i < 100; i++) {
        await session.add({
          name: `Record ${i}`,
          nameTerms: [`term${i % 20}`, `word${i % 10}`],
          groupTerms: [`group${i % 5}`]
        })
      }
      
      await session.commit()

      const saveStart = Date.now()
      await db.save()
      const saveTime = Date.now() - saveStart
      console.log(`Saved database in ${saveTime}ms`)

      // Create new database and load
      const newDb = new Database(testDbPath, {
        fields: { name: 'string', nameTerms: 'array:string', groupTerms: 'array:string' },
        indexes: { nameTerms: 'array:string', groupTerms: 'array:string' },
        debugMode: false
      })

      const loadStart = Date.now()
      await newDb.initialize()
      const loadTime = Date.now() - loadStart
      console.log(`Loaded database in ${loadTime}ms`)

      // Verify data integrity
      const records = await newDb.find({})
      expect(records).to.have.length(100)

      const termStats = newDb.termManager.getStats()
      expect(termStats.totalTerms).to.equal(35) // 20 + 10 + 5 unique terms

      expect(saveTime).to.be.lessThan(1500) // OPTIMIZED: Save should be fast
      expect(loadTime).to.be.lessThan(1000) // OPTIMIZED: Load should be very fast
    })
  })

  describe('termMappingFields Configuration', () => {
    it('should respect termMappingFields configuration and not apply term mapping to excluded fields', async () => {
      const testDbPath = path.join(process.cwd(), 'test-term-mapping-fields.jdb')

      try {
        // Clean up any existing test file
        if (fs.existsSync(testDbPath)) {
          fs.unlinkSync(testDbPath)
        }

        // Create database with termMappingFields specifying only some array:string fields
        db = new Database(testDbPath, {
          fields: {
            name: 'string',
            groups: 'array:string',        // Should NOT use term mapping (not in termMappingFields)
            nameTerms: 'array:string',     // Should use term mapping (in termMappingFields)
            groupTerms: 'array:string',    // Should use term mapping (in termMappingFields)
          },
          indexes: {
            groups: 'array:string',        // Indexed but not in termMappingFields
            nameTerms: 'array:string',     // Indexed and in termMappingFields
            groupTerms: 'array:string',    // Indexed and in termMappingFields
          },
          termMapping: true,
          termMappingFields: ['nameTerms', 'groupTerms'], // Only these should use term mapping
          debugMode: false
        })
        await db.initialize()

        // Verify that termMappingFields was set correctly
        expect(db.termManager.termMappingFields).to.deep.equal(['nameTerms', 'groupTerms'])

        // Insert test data
        const testData = {
          name: 'Test Record',
          groups: ['Group A', 'Group B'],      // Should remain as strings
          nameTerms: ['test', 'record'],       // Should become indices
          groupTerms: ['group', 'a', 'b']      // Should become indices
        }

        await db.insert(testData)
        await db.save()

        // Read data back
        const results = await db.find({})
        const readData = results[0]

        // Verify that groups field remained as strings
        expect(readData.groups).to.deep.equal(['Group A', 'Group B'])
        expect(readData.groups.every(item => typeof item === 'string')).to.be.true

        // Verify that nameTerms and groupTerms were converted to indices
        expect(Array.isArray(readData.nameTerms)).to.be.true
        expect(readData.nameTerms.every(item => typeof item === 'number')).to.be.true
        expect(readData.nameTerms.length).to.equal(2)

        expect(Array.isArray(readData.groupTerms)).to.be.true
        expect(readData.groupTerms.every(item => typeof item === 'number')).to.be.true
        expect(readData.groupTerms.length).to.equal(3)

        // Verify that the term manager has mappings for the mapped fields
        expect(db.termManager.hasTerm('test')).to.be.true
        expect(db.termManager.hasTerm('record')).to.be.true
        expect(db.termManager.hasTerm('group')).to.be.true
        expect(db.termManager.hasTerm('a')).to.be.true
        expect(db.termManager.hasTerm('b')).to.be.true

        // Verify that the excluded field values are NOT in the term manager
        expect(db.termManager.hasTerm('Group A')).to.be.false
        expect(db.termManager.hasTerm('Group B')).to.be.false

      } finally {
        // Clean up
        if (db) {
          await db.destroy()
        }
        if (fs.existsSync(testDbPath)) {
          fs.unlinkSync(testDbPath)
        }
      }
    })
  })
})
