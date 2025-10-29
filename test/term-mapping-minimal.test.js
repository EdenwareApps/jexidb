import fs from 'fs'
import path from 'path'
import TermManager from '../src/managers/TermManager.mjs'

describe('Term Mapping - Minimal Tests', () => {
  let termManager

  beforeEach(() => {
    termManager = new TermManager()
  })

  describe('TermManager', () => {
    it('should create and retrieve term IDs', () => {
      const id1 = termManager.getTermId('bra')
      const id2 = termManager.getTermId('globo')
      const id3 = termManager.getTermId('bra') // Same term

      expect(id1).toBe(1)
      expect(id2).toBe(2)
      expect(id3).toBe(1) // Should return same ID

      expect(termManager.getTerm(1)).toBe('bra')
      expect(termManager.getTerm(2)).toBe('globo')
    })

    it('should track term usage counts', () => {
      const id1 = termManager.getTermId('bra')
      const id2 = termManager.getTermId('bra')
      const id3 = termManager.getTermId('bra')

      expect(termManager.termCounts.get(id1)).toBe(3)
    })

    it('should clean up orphaned terms', () => {
      const id1 = termManager.getTermId('bra')
      const id2 = termManager.getTermId('globo')

      // Decrement counts to make them orphaned
      termManager.decrementTermCount(id1)
      termManager.decrementTermCount(id1)
      termManager.decrementTermCount(id2)

      const orphanedCount = termManager.cleanupOrphanedTerms(true)
      expect(orphanedCount).toBe(2)

      // Terms should be removed
      expect(termManager.getTerm(id1)).toBeNull()
      expect(termManager.getTerm(id2)).toBeNull()
    })

    it('should load and save terms', () => {
      // Create some terms
      termManager.getTermId('bra')
      termManager.getTermId('globo')
      termManager.getTermId('brasil')

      // Save terms
      const savedTerms = termManager.saveTerms()
      expect(savedTerms).toEqual({
        '1': 'bra',
        '2': 'globo',
        '3': 'brasil'
      })

      // Create new manager and load terms
      const newManager = new TermManager()
      newManager.loadTerms(savedTerms)

      expect(newManager.getTerm(1)).toBe('bra')
      expect(newManager.getTerm(2)).toBe('globo')
      expect(newManager.getTerm(3)).toBe('brasil')
    })

    it('should provide statistics', () => {
      termManager.getTermId('bra')
      termManager.getTermId('globo')

      const stats = termManager.getStats()
      expect(stats.totalTerms).toBe(2)
      expect(stats.nextId).toBe(3)
    })
  })

  describe('Term Mapping Concept', () => {
    it('should demonstrate term mapping benefits', () => {
      // Simulate a large dataset with repeated terms
      const terms = ['bra', 'globo', 'brasil', 'discovery', 'channel']
      const repeatedTerms = []

      // Create 1000 records with repeated terms
      for (let i = 0; i < 1000; i++) {
        const randomTerms = []
        for (let j = 0; j < 5; j++) {
          randomTerms.push(terms[Math.floor(Math.random() * terms.length)])
        }
        repeatedTerms.push(randomTerms)
      }

      // Map all terms to IDs
      const startTime = Date.now()
      const mappedData = repeatedTerms.map(record => 
        record.map(term => termManager.getTermId(term))
      )
      const mappingTime = Date.now() - startTime

      // Verify mapping worked
      expect(mappedData.length).toBe(1000)
      expect(mappedData[0].length).toBe(5)
      expect(termManager.getStats().totalTerms).toBe(5) // Only 5 unique terms

      console.log(`✅ Mapped 5000 terms to 5 unique IDs in ${mappingTime}ms`)
      console.log(`📊 Term mapping stats:`, termManager.getStats())
    })

    it('should show size reduction potential', () => {
      // Original data with repeated strings
      const originalData = [
        { id: 1, nameTerms: ['bra', 'globo', 'brasil'] },
        { id: 2, nameTerms: ['bra', 'discovery', 'channel'] },
        { id: 3, nameTerms: ['globo', 'brasil', 'discovery'] },
        { id: 4, nameTerms: ['bra', 'globo', 'channel'] },
        { id: 5, nameTerms: ['brasil', 'discovery', 'channel'] }
      ]

      // Calculate original size
      const originalSize = JSON.stringify(originalData).length

      // Map terms to IDs
      const mappedData = originalData.map(record => ({
        id: record.id,
        nameTerms: record.nameTerms.map(term => termManager.getTermId(term))
      }))

      // Create term mapping
      const termMapping = termManager.saveTerms()

      // Calculate new size
      const mappedSize = JSON.stringify(mappedData).length
      const termMappingSize = JSON.stringify(termMapping).length
      const totalNewSize = mappedSize + termMappingSize

      const reduction = ((originalSize - totalNewSize) / originalSize * 100).toFixed(1)

      console.log(`📊 Size comparison:`)
      console.log(`   Original: ${originalSize} bytes`)
      console.log(`   Mapped: ${mappedSize} bytes`)
      console.log(`   Terms: ${termMappingSize} bytes`)
      console.log(`   Total: ${totalNewSize} bytes`)
      console.log(`   Reduction: ${reduction}%`)

      expect(totalNewSize).toBeLessThan(originalSize)
    })
  })
})
