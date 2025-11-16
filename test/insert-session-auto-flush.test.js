import { Database } from '../src/Database.mjs'
import fs from 'fs'
import path from 'path'

describe('InsertSession Auto-Flush', () => {
  let testDir
  let db

  beforeEach(() => {
    testDir = path.join(process.cwd(), 'test-files', 'insert-session-auto-flush')
    fs.mkdirSync(testDir, { recursive: true })
  })

  afterEach(async () => {
    if (db) {
      // Wait for all active insert sessions to complete
      if (db.activeInsertSessions && db.activeInsertSessions.size > 0) {
        const sessions = Array.from(db.activeInsertSessions)
        await Promise.all(sessions.map(session => {
          return session.waitForAutoFlushes().catch(() => {})
        }))
      }
      
      // Wait for any pending operations before closing
      await db.waitForOperations()
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

  describe('Auto-flush on batch size', () => {
    test('should auto-flush when batch size is reached', async () => {
      const dbPath = path.join(testDir, 'auto-flush-basic.jdb')
      db = new Database(dbPath, { clear: true, create: true })
      await db.init()

      const session = db.beginInsertSession({ batchSize: 10 })
      
      // Insert exactly 10 records (should trigger auto-flush)
      for (let i = 0; i < 10; i++) {
        await session.add({ name: `Record ${i}`, value: i })
      }

      // Wait a bit for auto-flush to complete
      await new Promise(resolve => setTimeout(resolve, 100))

      // Verify data was inserted (auto-flush should have processed it)
      expect(db.length).toBe(10)
      
      // Verify batches array is empty (auto-flush processed it)
      expect(session.batches.length).toBe(0)
      
      await session.commit()
    })

    test('should auto-flush multiple batches without accumulating in memory', async () => {
      const dbPath = path.join(testDir, 'auto-flush-multiple.jdb')
      db = new Database(dbPath, { clear: true, create: true })
      await db.init()

      const session = db.beginInsertSession({ batchSize: 100 })
      
      // Insert 5000 records (enough to test auto-flush without timeout)
      // This should create ~50 batches, but they should be auto-flushed
      const totalRecords = 5000
      
      for (let i = 0; i < totalRecords; i++) {
        await session.add({ 
          name: `Record ${i}`, 
          value: i,
          data: `Data for record ${i}`.repeat(10) // Add some data to make it realistic
        })
        
        // Periodically check that batches don't accumulate
        if (i % 1000 === 0 && i > 0) {
          // Wait a bit for auto-flushes to catch up
          await new Promise(resolve => setTimeout(resolve, 50))
          
          // Verify batches array doesn't grow unbounded
          // It should be small (only pending batches waiting to be flushed)
          expect(session.batches.length).toBeLessThan(10)
        }
      }

      // Wait for all auto-flushes to complete
      await session.waitForAutoFlushes()
      
      // Final commit should be fast (most data already flushed)
      await session.commit()

      // Verify all data was inserted
      expect(db.length).toBe(totalRecords)
      
      // Verify batches are empty after commit
      expect(session.batches.length).toBe(0)
      expect(session.currentBatch.length).toBe(0)
    })

    test('should handle concurrent inserts during flush', async () => {
      const dbPath = path.join(testDir, 'auto-flush-concurrent.jdb')
      db = new Database(dbPath, { clear: true, create: true })
      await db.init()

      const session = db.beginInsertSession({ batchSize: 50 })
      
      // Insert records concurrently while flush is happening
      const insertPromises = []
      for (let i = 0; i < 500; i++) {
        insertPromises.push(
          session.add({ name: `Record ${i}`, value: i })
        )
      }

      // All inserts should complete
      await Promise.all(insertPromises)

      // Wait for all auto-flushes
      await session.waitForAutoFlushes()
      
      // Commit should process any remaining data
      await session.commit()

      // Verify all data was inserted
      expect(db.length).toBe(500)
    })
  })

  describe('Commit waits for auto-flushes', () => {
    test('commit() should wait for all pending auto-flushes', async () => {
      const dbPath = path.join(testDir, 'commit-waits.jdb')
      db = new Database(dbPath, { clear: true, create: true })
      await db.init()

      const session = db.beginInsertSession({ batchSize: 10 })
      
      // Insert many records to trigger multiple auto-flushes
      for (let i = 0; i < 250; i++) {
        await session.add({ name: `Record ${i}`, value: i })
      }

      // Commit should wait for all auto-flushes to complete
      const insertedCount = await session.commit()

      // Verify all records were inserted
      expect(insertedCount).toBe(250)
      expect(db.length).toBe(250)
      
      // Verify no pending operations
      expect(session.hasPendingOperations()).toBe(false)
      
      // Verify all auto-flushes completed (pendingAutoFlushes should be empty)
      expect(session.pendingAutoFlushes.size).toBe(0)
    })

    test('commit() should flush remaining data after waiting for auto-flushes', async () => {
      const dbPath = path.join(testDir, 'commit-flush-remaining.jdb')
      db = new Database(dbPath, { clear: true, create: true })
      await db.init()

      const session = db.beginInsertSession({ batchSize: 100 })
      
      // Insert 83 records (less than batchSize, so no auto-flush)
      for (let i = 0; i < 83; i++) {
        await session.add({ name: `Record ${i}`, value: i })
      }

      // Verify data is in currentBatch but not flushed
      expect(session.currentBatch.length).toBe(83)
      expect(db.length).toBe(0) // Not yet inserted

      // Commit should flush the remaining data
      await session.commit()

      // Verify all data was inserted
      expect(db.length).toBe(83)
      expect(session.currentBatch.length).toBe(0)
    })
  })

  describe('_doFlush handles concurrent inserts', () => {
    test('_doFlush should process all data even if new data is added during flush', async () => {
      const dbPath = path.join(testDir, 'doflush-concurrent.jdb')
      db = new Database(dbPath, { clear: true, create: true })
      await db.init()

      const session = db.beginInsertSession({ batchSize: 10 })
      
      // Add initial batch
      for (let i = 0; i < 10; i++) {
        await session.add({ name: `Record ${i}`, value: i })
      }

      // Manually trigger flush, but add more data during flush
      const flushPromise = session._doFlush()
      
      // Add more data while flush is happening
      for (let i = 10; i < 25; i++) {
        await session.add({ name: `Record ${i}`, value: i })
      }

      // Wait for flush to complete
      await flushPromise

      // Verify all data was processed
      // The flush should have processed everything, including data added during flush
      expect(session.batches.length).toBe(0)
      
      // Final commit to ensure everything is inserted
      await session.commit()
      expect(db.length).toBe(25)
    })

    test('_doFlush should continue until queue is empty', async () => {
      const dbPath = path.join(testDir, 'doflush-empty-queue.jdb')
      db = new Database(dbPath, { clear: true, create: true })
      await db.init()

      const session = db.beginInsertSession({ batchSize: 5 })
      
      // Add multiple batches
      for (let i = 0; i < 23; i++) {
        await session.add({ name: `Record ${i}`, value: i })
      }

      // Wait for any auto-flushes to complete
      await session.waitForAutoFlushes()

      // Manually trigger flush (should process everything including currentBatch)
      await session._doFlush()

      // Verify all batches were processed
      // _doFlush processes everything, including partial currentBatch
      expect(session.batches.length).toBe(0)
      // After _doFlush, currentBatch should be empty (it was processed)
      expect(session.currentBatch.length).toBe(0)

      // Verify all data was inserted
      expect(db.length).toBe(23)
    })
  })

  describe('Memory management', () => {
    test('should not accumulate batches in memory', async () => {
      const dbPath = path.join(testDir, 'memory-management.jdb')
      db = new Database(dbPath, { clear: true, create: true })
      await db.init()

      const session = db.beginInsertSession({ batchSize: 100 })
      
      // Track memory usage
      const initialBatches = session.batches.length
      
      // Insert large amount of data
      for (let i = 0; i < 10000; i++) {
        await session.add({ name: `Record ${i}`, value: i })
        
        // Check periodically that batches don't accumulate
        if (i % 500 === 0 && i > 0) {
          await new Promise(resolve => setTimeout(resolve, 10))
          
          // Batches should be small (auto-flush is working)
          expect(session.batches.length).toBeLessThan(5)
        }
      }

      // Wait for all auto-flushes
      await session.waitForAutoFlushes()
      
      // Final commit
      await session.commit()

      // Verify all data was inserted
      expect(db.length).toBe(10000)
      
      // Verify batches are empty
      expect(session.batches.length).toBe(0)
      expect(session.currentBatch.length).toBe(0)
    })
  })

  describe('Edge cases', () => {
    test('should handle empty commit', async () => {
      const dbPath = path.join(testDir, 'empty-commit.jdb')
      db = new Database(dbPath, { clear: true, create: true })
      await db.init()

      const session = db.beginInsertSession()
      
      // Commit without adding anything
      const count = await session.commit()
      
      expect(count).toBe(0)
      expect(db.length).toBe(0)
    })

    test('should handle multiple commits on same session', async () => {
      const dbPath = path.join(testDir, 'multiple-commits.jdb')
      db = new Database(dbPath, { clear: true, create: true })
      await db.init()

      const session = db.beginInsertSession({ batchSize: 10 })
      
      // First commit
      for (let i = 0; i < 15; i++) {
        await session.add({ name: `Record ${i}`, value: i })
      }
      await session.commit()
      expect(db.length).toBe(15)

      // Second commit
      for (let i = 15; i < 30; i++) {
        await session.add({ name: `Record ${i}`, value: i })
      }
      await session.commit()
      expect(db.length).toBe(30)
    })

    test('should handle hasPendingOperations correctly', async () => {
      const dbPath = path.join(testDir, 'pending-operations.jdb')
      db = new Database(dbPath, { clear: true, create: true })
      await db.init()

      const session = db.beginInsertSession({ batchSize: 10 })
      
      // Initially no pending operations
      expect(session.hasPendingOperations()).toBe(false)
      
      // Add records (triggers auto-flush)
      for (let i = 0; i < 15; i++) {
        await session.add({ name: `Record ${i}`, value: i })
      }

      // Should have pending operations (auto-flush in progress)
      // Wait a bit and check
      await new Promise(resolve => setTimeout(resolve, 50))
      
      // After auto-flush completes, should have no pending (except currentBatch if < batchSize)
      await session.waitForAutoFlushes()
      
      // Commit to clear everything
      await session.commit()
      expect(session.hasPendingOperations()).toBe(false)
    })
  })
})

