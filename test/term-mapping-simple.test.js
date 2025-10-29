import fs from 'fs'
import path from 'path'
import { Database } from '../src/Database.mjs'
import TermManager from '../src/managers/TermManager.mjs'

describe('Term Mapping Tests', () => {
  let db
  let testDbPath
  let testIdxPath

  beforeEach(async () => {
    testDbPath = path.join(process.cwd(), 'test-term-mapping.jdb')
    testIdxPath = path.join(process.cwd(), 'test-term-mapping.idx.jdb')
    
    // Clean up any existing test files
    if (fs.existsSync(testDbPath)) fs.unlinkSync(testDbPath)
    if (fs.existsSync(testIdxPath)) fs.unlinkSync(testIdxPath)
  })

  afterEach(async () => {
    if (db) {
      await db.close()
    }
    
    // Clean up test files
    if (fs.existsSync(testDbPath)) fs.unlinkSync(testDbPath)
    if (fs.existsSync(testIdxPath)) fs.unlinkSync(testIdxPath)
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
      expect(termsData).toEqual({
        '1': 'bra',
        '2': 'globo',
        '3': 'brasil'
      })

      // Create new manager and load terms
      const newManager = new TermManager()
      await newManager.loadTerms(termsData)

      expect(newManager.getTerm(1)).toBe('bra')
      expect(newManager.getTerm(2)).toBe('globo')
      expect(newManager.getTerm(3)).toBe('brasil')
      expect(newManager.nextId).toBe(4)
    })

    it('should provide statistics', () => {
      termManager.getTermId('term1')
      termManager.getTermId('term2')

      const stats = termManager.getStats()
      expect(stats.totalTerms).toBe(2)
      expect(stats.nextId).toBe(3)
      expect(stats.orphanedTerms).toBe(0)
    })
  })

  describe('Database with Term Mapping', () => {
    beforeEach(async () => {
      db = new Database(testDbPath, {
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
      expect(inserted.nameTerms).toEqual(['bra', 'globo', 'brasil'])
      expect(inserted.groupTerms).toEqual(['channel', 'discovery'])

      // Check that terms are mapped (order may vary)
      const allTerms = ['bra', 'globo', 'brasil', 'channel', 'discovery']
      const mappedTerms = []
      for (let i = 1; i <= 5; i++) {
        const term = db.termManager.getTerm(i)
        if (term) mappedTerms.push(term)
      }
      expect(mappedTerms.sort()).toEqual(allTerms.sort())
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
      expect(results1).toHaveLength(1)
      expect(results1[0].name).toBe('Record 1')

      const results2 = await db.find({ groupTerms: 'channel' })
      expect(results2).toHaveLength(2)

      const results3 = await db.find({ nameTerms: 'nonexistent' })
      expect(results3).toHaveLength(0)
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
      expect(db.termManager.termCounts.get(1)).toBe(0) // 'bra' should be orphaned
      expect(db.termManager.termCounts.get(2)).toBe(0) // 'globo' should be orphaned
      expect(db.termManager.termCounts.get(3)).toBe(0) // 'channel' should be orphaned
      expect(db.termManager.termCounts.get(4)).toBe(1) // 'brasil' should be active
      expect(db.termManager.termCounts.get(5)).toBe(2) // 'discovery' should be active (used in both nameTerms and groupTerms)

      // Query to verify update
      const results = await db.find({ nameTerms: 'brasil' })
      expect(results).toHaveLength(1)
      expect(results[0].nameTerms).toEqual(['brasil', 'discovery'])
    })

    it('should delete records with term mapping', async () => {
      const record = await db.insert({
        name: 'Test Record',
        nameTerms: ['bra', 'globo'],
        groupTerms: ['channel']
      })

      await db.delete({ id: record.id })

      // Check that terms are decremented
      expect(db.termManager.termCounts.get(1)).toBe(0) // 'bra' should be orphaned
      expect(db.termManager.termCounts.get(2)).toBe(0) // 'globo' should be orphaned
      expect(db.termManager.termCounts.get(3)).toBe(0) // 'channel' should be orphaned

      // Verify record is deleted
      const results = await db.find({ id: record.id })
      expect(results).toHaveLength(0)
    })

    it('should maintain compatibility with non-term-mapping fields', async () => {
      const record = await db.insert({
        name: 'Test Record',
        nameTerms: ['bra', 'globo'], // Term mapping field
        groupTerms: ['channel'], // Term mapping field
        category: 'news', // Non-term mapping field
        tags: ['urgent', 'breaking'] // Non-term mapping field
      })

      // Query by non-term-mapping field
      const results = await db.find({ category: 'news' })
      expect(results).toHaveLength(1)
      expect(results[0].name).toBe('Test Record')

      // Query by tags (non-term-mapping array field)
      const results2 = await db.find({ tags: 'urgent' })
      expect(results2).toHaveLength(1)
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
      expect(stats.records).toBe(2)

      const termStats = db.termManager.getStats()
      expect(termStats.totalTerms).toBe(5) // bra, globo, brasil, discovery, channel
    })
  })
})
