/**
 * Auto-Save Example - Demonstrates JexiDB's intelligent auto-save features
 * 
 * This example shows:
 * - Auto-save based on record count threshold
 * - Auto-save based on time interval
 * - Event handling for buffer operations
 * - Buffer status monitoring
 * - Performance configuration
 */

import Database from '../src/index.js';
import path from 'path';

async function autoSaveExample() {
  console.log('🚀 Starting Auto-Save Example...\n');
  
  // Create database with intelligent auto-save configuration
  const dbPath = path.join(process.cwd(), 'auto-save-example.jdb');
  const db = new Database(dbPath, {
    indexes: { category: 'string', priority: 'number' },
    create: true,
    clear: true,
    
    // Auto-save configuration
    autoSave: true,
    autoSaveThreshold: 10, // Flush every 10 records
    autoSaveInterval: 3000, // Flush every 3 seconds
    forceSaveOnClose: true,
    
    // Performance configuration
    batchSize: 20,
    adaptiveBatchSize: true,
    minBatchSize: 5,
    maxBatchSize: 50
  });

  // Set up event listeners
  db.on('buffer-flush', (count) => {
    console.log(`📤 Buffer flushed: ${count} records`);
  });

  db.on('buffer-full', () => {
    console.log('⚠️  Buffer reached threshold, flushing...');
  });

  db.on('auto-save-timer', () => {
    console.log('⏰ Auto-save timer triggered');
  });

  db.on('save-complete', () => {
    console.log('✅ Save completed');
  });

  db.on('close-save-complete', () => {
    console.log('🔒 Database closed with final save');
  });

  db.on('performance-configured', (config) => {
    console.log('⚙️  Performance reconfigured:', config);
  });

  await db.init();
  console.log('✅ Database initialized\n');

  // Show initial buffer status
  console.log('📊 Initial buffer status:');
  console.log(db.getBufferStatus());
  console.log('');

  // Insert records to demonstrate auto-save
  console.log('📝 Inserting records...');
  
  for (let i = 1; i <= 25; i++) {
    const record = {
      id: `item-${i}`,
      name: `Item ${i}`,
      category: i % 3 === 0 ? 'high' : i % 2 === 0 ? 'medium' : 'low',
      priority: i % 5 + 1,
      description: `This is item number ${i}`
    };

    await db.insert(record);
    console.log(`  Inserted: ${record.name} (${record.category} priority)`);
    
    // Show buffer status every 5 records
    if (i % 5 === 0) {
      const status = db.getBufferStatus();
      console.log(`  Buffer: ${status.pendingCount}/${status.bufferSize} records pending`);
    }
    
    // Small delay to demonstrate time-based auto-save
    await new Promise(resolve => setTimeout(resolve, 500));
  }

  console.log('\n📊 Final buffer status:');
  console.log(db.getBufferStatus());
  console.log('');

  // Demonstrate performance configuration
  console.log('⚙️  Reconfiguring performance...');
  db.configurePerformance({
    batchSize: 15,
    autoSaveThreshold: 8,
    autoSaveInterval: 2000
  });

  console.log('📊 New performance config:');
  console.log(db.getPerformanceConfig());
  console.log('');

  // Insert more records with new configuration
  console.log('📝 Inserting more records with new config...');
  
  for (let i = 26; i <= 35; i++) {
    const record = {
      id: `item-${i}`,
      name: `Item ${i}`,
      category: 'new',
      priority: 10,
      description: `New item ${i}`
    };

    await db.insert(record);
    console.log(`  Inserted: ${record.name}`);
    
    await new Promise(resolve => setTimeout(resolve, 300));
  }

  // Query the database
  console.log('\n🔍 Querying database...');
  const highPriority = await db.find({ priority: { '>=': 8 } });
  console.log(`Found ${highPriority.length} high priority items`);

  const categories = await db.find({ category: 'high' });
  console.log(`Found ${categories.length} high category items`);

  // Show final statistics
  console.log('\n📈 Final database statistics:');
  const stats = db.stats;
  console.log(`Total records: ${stats.recordCount}`);
  console.log(`Buffer size: ${stats.insertionBufferSize}`);
  console.log(`Auto-save enabled: ${stats.autoSave.enabled}`);
  console.log(`Last flush: ${stats.autoSave.lastFlush ? new Date(stats.autoSave.lastFlush).toLocaleTimeString() : 'Never'}`);

  // Close database (will trigger final save)
  console.log('\n🔒 Closing database...');
  await db.close();
  
  // Clean up
  await db.destroy();
  console.log('🧹 Cleanup completed');
  
  console.log('\n✅ Auto-Save Example completed successfully!');
}

// Run the example
autoSaveExample().catch(console.error);
