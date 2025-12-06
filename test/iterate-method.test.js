/**
 * Test suite for the new iterate() method
 * Tests bulk update capabilities with streaming performance
 */

import { Database } from '../src/Database.mjs'
import fs from 'fs'
import path from 'path'

describe('Database.iterate() Method', () => {
  let db
  const testFile = 'test-iterate.jdb'
  const testIdxFile = 'test-iterate.idx.jdb'

  beforeEach(async () => {
    // Clean up any existing test files
    if (fs.existsSync(testFile)) fs.unlinkSync(testFile)
    if (fs.existsSync(testIdxFile)) fs.unlinkSync(testIdxFile)
    
    db = new Database(testFile, {
      fields: { id: 'number', name: 'string', category: 'string', price: 'number' },
      debugMode: false,
      termMapping: true,
      indexes: { category: 'string', name: 'string', price: 'number' }
    })
    await db.init()
  })

  afterEach(async () => {
    if (db && !db.destroyed) {
      await db.close()
    }
    // Clean up test files
    if (fs.existsSync(testFile)) fs.unlinkSync(testFile)
    if (fs.existsSync(testIdxFile)) fs.unlinkSync(testIdxFile)
  })

  describe('Basic Functionality', () => {
    test('should iterate through records without modifications', async () => {
      // Insert test data
      await db.insert({ id: 1, name: 'Apple', category: 'fruits', price: 1.50 })
      await db.insert({ id: 2, name: 'Banana', category: 'fruits', price: 0.80 })
      await db.insert({ id: 3, name: 'Carrot', category: 'vegetables', price: 0.60 })
      
      const results = []
      for await (const entry of db.iterate({ category: 'fruits' })) {
        results.push(entry)
      }
      
      expect(results).toHaveLength(2)
      expect(results.map(r => r.name)).toEqual(['Apple', 'Banana'])
    })

    test('should detect and process modifications', async () => {
      // Insert test data
      await db.insert({ id: 1, name: 'Apple', category: 'fruits', price: 1.50 })
      await db.insert({ id: 2, name: 'Banana', category: 'fruits', price: 0.80 })
      
      // Iterate and modify records
      for await (const entry of db.iterate({ category: 'fruits' })) {
        if (entry.name === 'Apple') {
          entry.price = 2.00 // Modify price
        }
      }
      
      // Verify changes were applied
      const apple = await db.findOne({ name: 'Apple' })
      expect(apple.price).toBe(2.00)
      
      const banana = await db.findOne({ name: 'Banana' })
      expect(banana.price).toBe(0.80) // Unchanged
    })

    test('should handle deletions by setting entry to null', async () => {
      // Insert test data
      await db.insert({ id: 1, name: 'Apple', category: 'fruits', price: 1.50 })
      await db.insert({ id: 2, name: 'Banana', category: 'fruits', price: 0.80 })
      await db.insert({ id: 3, name: 'Carrot', category: 'vegetables', price: 0.60 })
      
      // Iterate and delete some records
      for await (const entry of db.iterate({ category: 'fruits' })) {
        if (entry.name === 'Apple') {
          // Delete the record using the delete method
          entry.delete()
        }
      }
      
      // Verify deletion
      const fruits = await db.find({ category: 'fruits' })
      expect(fruits).toHaveLength(1)
      expect(fruits[0].name).toBe('Banana')
      
      // Verify other categories unaffected
      const vegetables = await db.find({ category: 'vegetables' })
      expect(vegetables).toHaveLength(1)
    })
  })

  describe('Performance Features', () => {
    test('should process records in batches', async () => {
      // Insert many records
      const records = []
      for (let i = 1; i <= 2500; i++) {
        records.push({ 
          id: i, 
          name: `Item${i}`, 
          category: i % 2 === 0 ? 'even' : 'odd',
          price: i * 0.1
        })
      }
      
      // Insert all records
      for (const record of records) {
        await db.insert(record)
      }
      
      let processedCount = 0
      let modifiedCount = 0
      
      // Iterate with progress callback
      for await (const entry of db.iterate(
        { category: 'even' }, 
        { 
          chunkSize: 500,
          progressCallback: (progress) => {
            processedCount = progress.processed
            modifiedCount = progress.modified
          }
        }
      )) {
        // Modify every 20th record (to get exactly 125 from 1250)
        if (entry.id % 20 === 0) {
          entry.price = entry.price * 2
        }
      }
      
      expect(processedCount).toBe(1250) // Half of 2500
      expect(modifiedCount).toBe(125) // Every 20th of 1250 (20, 40, 60, ..., 2500)
    })

    test('should handle large datasets efficiently', async () => {
      // Insert test data
      const records = []
      for (let i = 1; i <= 1000; i++) {
        records.push({ 
          id: i, 
          name: `Product${i}`, 
          category: 'electronics',
          price: Math.random() * 100
        })
      }
      
      // Insert all records
      for (const record of records) {
        await db.insert(record)
      }
      
      const startTime = Date.now()
      let count = 0
      
      // Iterate through all records
      for await (const entry of db.iterate({ category: 'electronics' })) {
        count++
        // Simple modification
        entry.lastProcessed = Date.now()
      }
      
      const elapsed = Date.now() - startTime
      
      expect(count).toBe(1000)
      expect(elapsed).toBeLessThan(5000) // Should complete in under 5 seconds
      
      // Verify modifications were applied
      const sample = await db.findOne({ id: 1 })
      expect(sample.lastProcessed).toBeDefined()
    })
  })

  describe('Options and Configuration', () => {
    test('should respect chunkSize option', async () => {
      // Insert test data
      for (let i = 1; i <= 100; i++) {
        await db.insert({ id: i, name: `Item${i}`, category: 'test' })
      }
      
      let batchCount = 0
      const chunkSize = 25
      
      for await (const entry of db.iterate(
        { category: 'test' }, 
        { 
          chunkSize,
          progressCallback: (progress) => {
            if (progress.processed > 0 && progress.processed % chunkSize === 0) {
              batchCount++
            }
          }
        }
      )) {
        entry.processed = true
      }
      
      // Should have processed in 4-5 batches (100 / 25)
      expect(batchCount).toBeGreaterThanOrEqual(4)
      expect(batchCount).toBeLessThanOrEqual(5)
    })

    test('should work with manual change detection', async () => {
      await db.insert({ id: 1, name: 'Test', category: 'test', value: 1 })
      
      for await (const entry of db.iterate(
        { category: 'test' }, 
        { detectChanges: false }
      )) {
        entry.value = 2
        entry._modified = true // Manual flag
      }
      
      const result = await db.findOne({ id: 1 })
      expect(result.value).toBe(2)
    })

    test('should handle autoSave option', async () => {
      await db.insert({ id: 1, name: 'Test', category: 'test' })
      
      for await (const entry of db.iterate(
        { category: 'test' }, 
        { autoSave: true, chunkSize: 1 }
      )) {
        entry.name = 'Modified'
      }
      
      // Verify changes were saved
      const result = await db.findOne({ id: 1 })
      expect(result.name).toBe('Modified')
    })
  })

  describe('Error Handling', () => {
    test('should handle errors gracefully', async () => {
      await db.insert({ id: 1, name: 'Test', category: 'test' })
      
      let errorCaught = false
      try {
        for await (const entry of db.iterate({ category: 'test' })) {
          // Simulate an error
          throw new Error('Test error')
        }
      } catch (error) {
        errorCaught = true
        expect(error.message).toBe('Test error')
      }
      
      expect(errorCaught).toBe(true)
    })

    test('should validate state before iteration', async () => {
      await db.destroy()
      
      let errorCaught = false
      try {
        for await (const entry of db.iterate({})) {
          // This should not execute
        }
      } catch (error) {
        errorCaught = true
        expect(error.message).toContain('destroyed')
      }
      
      expect(errorCaught).toBe(true)
    })
  })
})
