/**
 * Close → Init Cycle Tests
 * 
 * Tests the ability to call init() after close() to reopen a database
 * This addresses the bug where databases couldn't be reopened after closing
 */

import { Database } from '../src/Database.mjs'
import fs from 'fs'

describe('Close → Init Cycle', () => {
  let db
  const testFile = 'test-close-init-cycle.jdb'

  beforeEach(() => {
    // Clean up any existing test files
    if (fs.existsSync(testFile)) fs.unlinkSync(testFile)
    if (fs.existsSync(testFile.replace('.jdb', '.idx.jdb'))) fs.unlinkSync(testFile.replace('.jdb', '.idx.jdb'))
    
    db = new Database(testFile, {
      fields: { name: 'string', value: 'number', data: 'string' },
      create: true,
      debugMode: false
    })
  })

  afterEach(async () => {
    if (db && !db.destroyed) {
      try {
        // Save any pending data before destroying
        if (!db.closed && db.writeBuffer && db.writeBuffer.length > 0) {
          await db.save()
        }
        await db.destroy()
      } catch (error) {
        // Ignore destroy errors for this test
        console.warn('Destroy error ignored:', error.message)
      }
    }
  })

  describe('Basic Close → Init Cycle', () => {
    it('should allow init() after close()', async () => {
      // Initialize database
      await db.init()
      expect(db.initialized).toBe(true)
      expect(db.closed).toBe(false)

      // Insert some data
      await db.insert({ name: 'Test1', value: 100 })
      await db.insert({ name: 'Test2', value: 200 })

      // Close database
      await db.close()
      expect(db.closed).toBe(true)
      expect(db.initialized).toBe(false)
      expect(db.destroyed).toBe(false) // Should not be destroyed

      // Reinitialize database
      await db.init()
      expect(db.initialized).toBe(true)
      expect(db.closed).toBe(false)

      // Verify data is still accessible
      const results = await db.find({})
      expect(results).toHaveLength(2)
      expect(results.some(r => r.name === 'Test1')).toBe(true)
      expect(results.some(r => r.name === 'Test2')).toBe(true)
    })

    it('should support multiple close → init cycles', async () => {
      await db.init()
      await db.insert({ name: 'Test', value: 123 })

      // First cycle
      await db.close()
      await db.init()
      let results = await db.find({})
      expect(results).toHaveLength(1)

      // Second cycle
      await db.close()
      await db.init()
      results = await db.find({})
      expect(results).toHaveLength(1)

      // Third cycle
      await db.close()
      await db.init()
      results = await db.find({})
      expect(results).toHaveLength(1)
    })

    it('should preserve data across close → init cycles', async () => {
      await db.init()
      
      // Insert initial data
      await db.insert({ name: 'Initial', value: 1 })
      await db.close()
      await db.init()

      // Add more data
      await db.insert({ name: 'AfterReopen', value: 2 })
      await db.close()
      await db.init()

      // Verify all data is preserved
      const results = await db.find({})
      expect(results).toHaveLength(2)
      expect(results.some(r => r.name === 'Initial')).toBe(true)
      expect(results.some(r => r.name === 'AfterReopen')).toBe(true)
    })
  })

  describe('Operations After Reinit', () => {
    it('should support basic operations after reinit', async () => {
      await db.init()
      await db.insert({ name: 'Test1', value: 100 })
      await db.save() // Ensure data is saved
      await db.close()
      await db.init()
      
      // Should be able to query existing data
      const results = await db.find({})
      expect(results).toHaveLength(1)
      expect(results[0].name).toBe('Test1')
    })

    it('should support insert operations after reinit', async () => {
      await db.init()
      await db.close()
      await db.init()
      
      await db.insert({ name: 'Test2', value: 200 })
      const results = await db.find({})
      expect(results).toHaveLength(1)
      expect(results[0].name).toBe('Test2')
    })
  })

  describe('Error Handling', () => {
    it('should throw error for operations on closed database', async () => {
      await db.init()
      await db.insert({ name: 'Test', value: 123 })
      await db.close()

      // Operations on closed database should throw error
      await expect(db.find({})).rejects.toThrow('Database is closed')
      await expect(db.insert({ name: 'Test2', value: 456 })).rejects.toThrow('Database is closed')
      await expect(db.update({ name: 'Test' }, { value: 999 })).rejects.toThrow('Database is closed')
      await expect(db.delete({ name: 'Test' })).rejects.toThrow('Database is closed')
    })

    it('should allow reinit after operations on closed database', async () => {
      await db.init()
      await db.insert({ name: 'Test', value: 123 })
      await db.close()

      // Try operation on closed database (should fail)
      await expect(db.find({})).rejects.toThrow('Database is closed')

      // Reinit should work
      await db.init()
      const results = await db.find({})
      expect(results).toHaveLength(1)
    })

    it('should not allow init() on destroyed database', async () => {
      await db.init()

      // Insert some data to ensure writeBuffer has content
      await db.insert({ name: 'test', value: 123 })

      // Ensure save() completes properly
      await db.save()

      // Now destroy should work without writeBuffer bug
      await db.destroy()

      // After destroy, init() should fail
      await expect(db.init()).rejects.toThrow('Cannot initialize destroyed database. Use a new instance instead.')
    })
  })

  describe('State Management', () => {
    it('should have correct state flags during close → init cycle', async () => {
      // Initial state
      expect(db.initialized).toBe(false)
      expect(db.closed).toBe(false)
      expect(db.destroyed).toBe(false)

      // After init
      await db.init()
      expect(db.initialized).toBe(true)
      expect(db.closed).toBe(false)
      expect(db.destroyed).toBe(false)

      // After close
      await db.close()
      expect(db.initialized).toBe(false)
      expect(db.closed).toBe(true)
      expect(db.destroyed).toBe(false)

      // After reinit
      await db.init()
      expect(db.initialized).toBe(true)
      expect(db.closed).toBe(false)
      expect(db.destroyed).toBe(false)
    })

    it('should reset write buffer state after close', async () => {
      await db.init()
      await db.insert({ name: 'Test', value: 123 })
      
      // Write buffer should have data
      expect(db.writeBuffer.length).toBeGreaterThan(0)
      
      await db.close()
      
      // Write buffer should be cleared after close
      expect(db.writeBuffer.length).toBe(0)
      expect(db.shouldSave).toBe(false)
      expect(db.isSaving).toBe(false)
    })
  })

  describe('Performance and Memory', () => {
    it('should not leak memory during multiple close → init cycles', async () => {
      const initialMemory = process.memoryUsage().heapUsed
      
      for (let i = 0; i < 10; i++) {
        await db.init()
        await db.insert({ name: `Test${i}`, value: i })
        await db.close()
      }
      
      const finalMemory = process.memoryUsage().heapUsed
      const memoryIncrease = finalMemory - initialMemory
      
      // Memory increase should be reasonable (less than 10MB)
      expect(memoryIncrease).toBeLessThan(10 * 1024 * 1024)
    })

    it('should handle large datasets across close → init cycles', async () => {
      await db.init()
      
      // Insert large dataset
      for (let i = 0; i < 1000; i++) {
        await db.insert({ name: `Record${i}`, value: i, data: 'x'.repeat(100) })
      }
      
      await db.close()
      await db.init()
      
      // Verify all data is accessible
      const results = await db.find({})
      expect(results).toHaveLength(1000)
      
      // Verify specific records
      const specific = await db.find({ name: 'Record500' })
      expect(specific).toHaveLength(1)
      expect(specific[0].value).toBe(500)
    })
  })
})
