/**
 * Auto-Save Test Suite
 * Tests the intelligent auto-save functionality of JexiDB
 */

const fs = require('fs');
const path = require('path');
const { default: Database } = require('../dist/index.js');

describe('Auto-Save Functionality', () => {
  const tmpDir = path.join(process.cwd(), `auto-save-test-${Date.now()}-${Math.random().toString(16).slice(2)}`);
  const dbPath = path.join(tmpDir, 'auto-save-test.jdb');

  beforeAll(async () => {
    fs.mkdirSync(tmpDir, { recursive: true });
  });

  afterAll(async () => {
    try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch (_) {}
  });

  beforeEach(async () => {
    // Clean up any existing database
    try { fs.unlinkSync(dbPath); } catch (_) {}
    try { fs.unlinkSync(dbPath.replace('.jdb', '.idx.jdb')); } catch (_) {}
  });

  test('should enable auto-save by default', async () => {
    const db = new Database(dbPath, {
      indexes: { category: 'string' },
      create: true,
      clear: true
    });

    await db.init();
    
    // Check default auto-save settings
    expect(db.options.autoSave).toBe(true);
    expect(db.options.autoSaveThreshold).toBe(50);
    expect(db.options.autoSaveInterval).toBe(5000);
    expect(db.options.forceSaveOnClose).toBe(true);
    expect(db.options.batchSize).toBe(50);

    await db.close();
  });

  test('should flush buffer when threshold is reached', async () => {
    const flushEvents = [];
    const bufferFullEvents = [];

    const db = new Database(dbPath, {
      indexes: { category: 'string' },
      create: true,
      clear: true,
      autoSaveThreshold: 5, // Low threshold for testing
      autoSaveInterval: 10000 // High interval to avoid timer interference
    });

    db.on('buffer-flush', (count) => {
      flushEvents.push(count);
    });

    db.on('buffer-full', () => {
      bufferFullEvents.push(true);
    });

    await db.init();

    // Insert 10 records (should trigger 2 flushes)
    for (let i = 0; i < 10; i++) {
      await db.insert({ id: i, category: 'test', name: `Item ${i}` });
    }

    // Wait a bit for async operations
    await new Promise(resolve => setTimeout(resolve, 100));

    expect(flushEvents.length).toBeGreaterThan(0);
    expect(bufferFullEvents.length).toBeGreaterThan(0);
    expect(db.getBufferStatus().pendingCount).toBeLessThan(5);

    await db.close();
  });

  test('should provide buffer status information', async () => {
    const db = new Database(dbPath, {
      indexes: { category: 'string' },
      create: true,
      clear: true,
      autoSaveThreshold: 10
    });

    await db.init();

    // Check initial status
    const initialStatus = db.getBufferStatus();
    expect(initialStatus.pendingCount).toBe(0);
    expect(initialStatus.bufferSize).toBe(50);
    expect(initialStatus.autoSaveEnabled).toBe(true);
    expect(initialStatus.shouldFlush).toBe(false);

    // Insert some records
    await db.insert({ id: 1, category: 'test', name: 'Item 1' });
    await db.insert({ id: 2, category: 'test', name: 'Item 2' });

    const statusAfterInsert = db.getBufferStatus();
    expect(statusAfterInsert.pendingCount).toBe(2);
    expect(statusAfterInsert.shouldFlush).toBe(false);

    await db.close();
  });

  test('should support manual flush', async () => {
    const flushEvents = [];
    const db = new Database(dbPath, {
      indexes: { category: 'string' },
      create: true,
      clear: true,
      autoSave: false // Disable auto-save for manual testing
    });

    db.on('buffer-flush', (count) => {
      flushEvents.push(count);
    });

    await db.init();

    // Insert records without auto-save
    for (let i = 0; i < 10; i++) {
      await db.insert({ id: i, category: 'test', name: `Item ${i}` });
    }

    expect(db.getBufferStatus().pendingCount).toBe(10);

    // Manual flush
    const flushedCount = await db.flush();
    expect(flushedCount).toBe(10);
    expect(flushEvents).toContain(10);
    expect(db.getBufferStatus().pendingCount).toBe(0);

    await db.close();
  });

  test('should support force save', async () => {
    const saveEvents = [];
    const db = new Database(dbPath, {
      indexes: { category: 'string' },
      create: true,
      clear: true,
      autoSave: false
    });

    db.on('save-complete', () => {
      saveEvents.push(true);
    });

    await db.init();

    // Insert just one record
    await db.insert({ id: 1, category: 'test', name: 'Item 1' });

    // Force save should work even with small buffer
    await db.forceSave();
    expect(saveEvents.length).toBe(1);

    await db.close();
  });

  test('should support performance configuration', async () => {
    const configEvents = [];
    const db = new Database(dbPath, {
      indexes: { category: 'string' },
      create: true,
      clear: true
    });

    db.on('performance-configured', (config) => {
      configEvents.push(config);
    });

    await db.init();

    // Get initial config
    const initialConfig = db.getPerformanceConfig();
    expect(initialConfig.batchSize).toBe(50);
    expect(initialConfig.autoSaveThreshold).toBe(50);

    // Reconfigure performance
    db.configurePerformance({
      batchSize: 25,
      autoSaveThreshold: 30,
      autoSaveInterval: 3000
    });

    expect(configEvents.length).toBe(1);

    const newConfig = db.getPerformanceConfig();
    expect(newConfig.batchSize).toBe(25);
    expect(newConfig.autoSaveThreshold).toBe(30);
    expect(newConfig.autoSaveInterval).toBe(3000);

    await db.close();
  });

  test('should respect batch size limits', async () => {
    const db = new Database(dbPath, {
      indexes: { category: 'string' },
      create: true,
      clear: true,
      minBatchSize: 5,
      maxBatchSize: 20
    });

    await db.init();

    // Try to set batch size below minimum
    db.configurePerformance({ batchSize: 1 });
    expect(db.getPerformanceConfig().batchSize).toBe(5);

    // Try to set batch size above maximum
    db.configurePerformance({ batchSize: 100 });
    expect(db.getPerformanceConfig().batchSize).toBe(20);

    // Valid batch size
    db.configurePerformance({ batchSize: 15 });
    expect(db.getPerformanceConfig().batchSize).toBe(15);

    await db.close();
  });

  test('should include auto-save info in stats', async () => {
    const db = new Database(dbPath, {
      indexes: { category: 'string' },
      create: true,
      clear: true
    });

    await db.init();

    const stats = db.stats;
    expect(stats.autoSave).toBeDefined();
    expect(stats.autoSave.enabled).toBe(true);
    expect(stats.autoSave.threshold).toBe(50);
    expect(stats.autoSave.interval).toBe(5000);
    expect(typeof stats.autoSave.timerActive).toBe('boolean');

    await db.close();
  });

  test('should handle close with force save', async () => {
    const closeEvents = [];
    const db = new Database(dbPath, {
      indexes: { category: 'string' },
      create: true,
      clear: true,
      forceSaveOnClose: true
    });

    db.on('close-save-complete', () => {
      closeEvents.push(true);
    });

    db.on('close', () => {
      closeEvents.push('close');
    });

    await db.init();

    // Insert some records
    await db.insert({ id: 1, category: 'test', name: 'Item 1' });
    await db.insert({ id: 2, category: 'test', name: 'Item 2' });

    await db.close();

    expect(closeEvents).toContain(true); // close-save-complete
    expect(closeEvents).toContain('close');
  });

  test('should clear auto-save timer on close', async () => {
    const db = new Database(dbPath, {
      indexes: { category: 'string' },
      create: true,
      clear: true,
      autoSaveInterval: 1000
    });

    await db.init();

    // Insert a record to start the timer
    await db.insert({ id: 1, category: 'test', name: 'Item 1' });
    
    expect(db.autoSaveTimer).toBeDefined();

    await db.close();

    expect(db.autoSaveTimer).toBeNull();
  });
});
