import Database from '../src/index.js';

async function demonstrateMemorySafeUsage() {
  console.log('🛡️  Memory-Safe JexiDB Configuration Example\n');

  // Configuration for memory-constrained environments
  const memorySafeConfig = {
    // Core options
    indexes: { id: 'number', category: 'string' },
    
    // Memory management
    memorySafeMode: true,        // Enable memory-safe operations
    chunkSize: 4 * 1024 * 1024, // 4MB chunks (reduced for low memory)
    gcInterval: 500,            // Force GC every 500 records
    maxFlushChunkBytes: 2 * 1024 * 1024, // 2MB max flush chunks
    
    // Auto-save with smaller thresholds
    autoSave: true,
    autoSaveThreshold: 25,      // Flush more frequently
    autoSaveInterval: 3000,     // Flush every 3 seconds
    
    // Performance with memory constraints
    batchSize: 25,              // Smaller batches
    minBatchSize: 5,
    maxBatchSize: 100,
    
    // Force save more frequently
    forceSaveOnClose: true
  };

  console.log('📋 Memory-Safe Configuration:');
  console.log(JSON.stringify(memorySafeConfig, null, 2));
  console.log('');

  const db = new Database('./memory-safe-test.jdb', memorySafeConfig);
  
  // Event listeners for monitoring
  db.on('buffer-flush', (count) => {
    console.log(`📤 Buffer flushed: ${count} records`);
  });
  
  db.on('buffer-full', () => {
    console.log('⚠️  Buffer reached threshold');
  });
  
  db.on('auto-save-timer', () => {
    console.log('⏰ Time-based auto-save triggered');
  });

  await db.init();
  console.log('✅ Database initialized with memory-safe configuration\n');

  // Insert many records to test memory management
  console.log('📝 Inserting records with memory-safe configuration...');
  
  const startTime = Date.now();
  const recordCount = 1000;
  
  for (let i = 0; i < recordCount; i++) {
    const record = {
      id: i + 1,
      name: `Record ${i + 1}`,
      category: i % 5 === 0 ? 'high' : i % 3 === 0 ? 'medium' : 'low',
      data: `Large data field ${i + 1} `.repeat(50), // Simulate large records
      timestamp: Date.now(),
      metadata: {
        source: 'memory-test',
        priority: i % 10,
        tags: [`tag${i % 20}`, `tag${i % 15}`]
      }
    };
    
    await db.insert(record);
    
    // Show progress
    if ((i + 1) % 100 === 0) {
      const elapsed = Date.now() - startTime;
      const rate = Math.round((i + 1) / (elapsed / 1000));
      console.log(`  Progress: ${i + 1}/${recordCount} records (${rate} records/sec)`);
      
      // Check buffer status
      const status = db.getBufferStatus();
      console.log(`  Buffer: ${status.pendingCount}/${status.bufferSize} pending`);
    }
  }

  console.log('\n📊 Final Statistics:');
  const stats = db.stats;
  console.log(`  Total records: ${stats.recordCount}`);
  console.log(`  Buffer size: ${stats.insertionBufferSize}`);
  console.log(`  Auto-save enabled: ${stats.autoSave.enabled}`);
  console.log(`  Last flush: ${stats.autoSave.lastFlush ? new Date(stats.autoSave.lastFlush).toLocaleTimeString() : 'Never'}`);

  // Test integrity validation with memory-safe mode
  console.log('\n🔍 Testing integrity validation...');
  const integrity = await db.validateIntegrity();
  console.log(`  Integrity check: ${integrity.isValid ? '✅ PASSED' : '❌ FAILED'}`);
  console.log(`  Message: ${integrity.message}`);

  // Test querying with memory constraints
  console.log('\n🔍 Testing queries...');
  const highPriorityRecords = await db.find({ category: 'high' });
  console.log(`  High priority records: ${highPriorityRecords.length}`);

  const recentRecords = await db.find({ 
    timestamp: { '>': Date.now() - 60000 } // Last minute
  });
  console.log(`  Recent records: ${recentRecords.length}`);

  // Test performance configuration
  console.log('\n⚙️  Testing performance configuration...');
  const currentConfig = db.getPerformanceConfig();
  console.log('  Current config:', currentConfig);

  // Reconfigure for even more memory safety
  db.configurePerformance({
    batchSize: 10,
    autoSaveThreshold: 10,
    autoSaveInterval: 2000
  });
  console.log('  Reconfigured for ultra memory safety');

  // Final save and close
  console.log('\n🔒 Closing database...');
  await db.close();
  console.log('✅ Database closed successfully');

  console.log('\n🎯 Memory-Safe Features Demonstrated:');
  console.log('  • Chunked file processing (4MB chunks)');
  console.log('  • Frequent garbage collection (every 500 records)');
  console.log('  • Smaller batch sizes (25 records)');
  console.log('  • Frequent auto-save (every 25 records or 3 seconds)');
  console.log('  • Memory-safe integrity validation');
  console.log('  • Dynamic performance configuration');
  console.log('  • Buffer status monitoring');
  
  console.log('\n💡 Tips for Memory-Constrained Environments:');
  console.log('  • Use smaller chunkSize (1-4MB) for low memory systems');
  console.log('  • Enable gcInterval for frequent garbage collection');
  console.log('  • Reduce batchSize and autoSaveThreshold');
  console.log('  • Monitor buffer status regularly');
  console.log('  • Use memorySafeMode: true (default)');
  console.log('  • Consider using --expose-gc flag for manual GC');
}

// Run the demonstration
demonstrateMemorySafeUsage().catch(console.error);
