/**
 * Performance Benchmarking Suite
 * Tests all aspects of JexiDB performance including CRUD operations, queries, and optimizations
 */

const { Database } = require('../dist/index.js');
const fs = require('fs').promises;
const path = require('path');

class PerformanceBenchmark {
  constructor() {
    this.results = {
      insert: { times: [], throughput: [] },
      find: { times: [], throughput: [] },
      update: { times: [], throughput: [] },
      delete: { times: [], throughput: [] },
      query: { times: [], throughput: [] },
      bulk: { times: [], throughput: [] }
    };
    this.testData = [];
    this.db = null;
  }

  /**
   * Generate test data
   */
  generateTestData(count) {
    const data = [];
    for (let i = 0; i < count; i++) {
      data.push({
        id: `user_${i}`,
        name: `User ${i}`,
        email: `user${i}@example.com`,
        age: Math.floor(Math.random() * 50) + 18,
        score: Math.random() * 100,
        active: Math.random() > 0.3,
        tags: Array.from({ length: Math.floor(Math.random() * 5) + 1 }, () => 
          ['tag1', 'tag2', 'tag3', 'tag4', 'tag5'][Math.floor(Math.random() * 5)]
        ),
        metadata: {
          created: new Date().toISOString(),
          lastLogin: new Date(Date.now() - Math.random() * 86400000).toISOString(),
          loginCount: Math.floor(Math.random() * 100),
          preferences: {
            theme: Math.random() > 0.5 ? 'dark' : 'light',
            language: ['en', 'es', 'fr', 'de'][Math.floor(Math.random() * 4)]
          }
        }
      });
    }
    return data;
  }

  /**
   * Initialize database for testing
   */
  async initDatabase() {
    const testDbPath = path.join(__dirname, '../test-performance-db.jdb');
    
    // Clean up existing test database
    try {
      await fs.unlink(testDbPath);
    } catch {}
    
    this.db = new Database(testDbPath, {
      indexes: { id: true, email: true, age: true, score: true },
      backgroundMaintenance: false, // Disable for benchmarking
      autoSave: false
    });
    
    await this.db.init();
  }

  /**
   * Clean up after testing
   */
  async cleanup() {
    if (this.db) {
      await this.db.destroy();
    }
    
    const testDbPath = path.join(__dirname, '../test-performance-db.jdb');
    try {
      await fs.unlink(testDbPath);
    } catch {}
  }

  /**
   * Benchmark single insert operations
   */
  async benchmarkInsert(count = 1000) {
    console.log(`📝 Benchmarking ${count} single inserts...`);
    
    const testData = this.generateTestData(count);
    const startTime = process.hrtime.bigint();
    
    for (let i = 0; i < count; i++) {
      const recordStart = process.hrtime.bigint();
      await this.db.insert(testData[i]);
      const recordEnd = process.hrtime.bigint();
      
      const duration = Number(recordEnd - recordStart) / 1000000; // ms
      this.results.insert.times.push(duration);
    }
    
    const endTime = process.hrtime.bigint();
    const totalDuration = Number(endTime - startTime) / 1000000; // ms
    const throughput = (count / totalDuration) * 1000; // ops/sec
    
    this.results.insert.throughput.push(throughput);
    
    console.log(`   ✅ Completed in ${totalDuration.toFixed(2)}ms (${throughput.toFixed(0)} ops/sec)`);
  }

  /**
   * Benchmark bulk insert operations
   */
  async benchmarkBulkInsert(count = 10000) {
    console.log(`📦 Benchmarking bulk insert of ${count} records...`);
    
    const testData = this.generateTestData(count);
    const startTime = process.hrtime.bigint();
    
    await this.db.insertMany(testData);
    
    const endTime = process.hrtime.bigint();
    const totalDuration = Number(endTime - startTime) / 1000000; // ms
    const throughput = (count / totalDuration) * 1000; // ops/sec
    
    this.results.bulk.times.push(totalDuration);
    this.results.bulk.throughput.push(throughput);
    
    console.log(`   ✅ Completed in ${totalDuration.toFixed(2)}ms (${throughput.toFixed(0)} ops/sec)`);
  }

  /**
   * Benchmark find operations
   */
  async benchmarkFind(count = 1000) {
    console.log(`🔍 Benchmarking ${count} find operations...`);
    
    const startTime = process.hrtime.bigint();
    
    for (let i = 0; i < count; i++) {
      const recordStart = process.hrtime.bigint();
      
      // Test different types of queries
      const queryType = i % 4;
      switch (queryType) {
        case 0:
          await this.db.findOne({ id: `user_${i % 100}` });
          break;
        case 1:
          await this.db.find({ age: { $gte: 25, $lte: 35 } });
          break;
        case 2:
          await this.db.find({ score: { $gt: 50 } });
          break;
        case 3:
          await this.db.find({ 'metadata.preferences.theme': 'dark' });
          break;
      }
      
      const recordEnd = process.hrtime.bigint();
      const duration = Number(recordEnd - recordStart) / 1000000; // ms
      this.results.find.times.push(duration);
    }
    
    const endTime = process.hrtime.bigint();
    const totalDuration = Number(endTime - startTime) / 1000000; // ms
    const throughput = (count / totalDuration) * 1000; // ops/sec
    
    this.results.find.throughput.push(throughput);
    
    console.log(`   ✅ Completed in ${totalDuration.toFixed(2)}ms (${throughput.toFixed(0)} ops/sec)`);
  }

  /**
   * Benchmark update operations
   */
  async benchmarkUpdate(count = 1000) {
    console.log(`✏️ Benchmarking ${count} update operations...`);
    
    const startTime = process.hrtime.bigint();
    
    for (let i = 0; i < count; i++) {
      const recordStart = process.hrtime.bigint();
      
      await this.db.update(
        { id: `user_${i % 100}` },
        { 
          score: Math.random() * 100,
          'metadata.lastLogin': new Date().toISOString()
        }
      );
      
      const recordEnd = process.hrtime.bigint();
      const duration = Number(recordEnd - recordStart) / 1000000; // ms
      this.results.update.times.push(duration);
    }
    
    const endTime = process.hrtime.bigint();
    const totalDuration = Number(endTime - startTime) / 1000000; // ms
    const throughput = (count / totalDuration) * 1000; // ops/sec
    
    this.results.update.throughput.push(throughput);
    
    console.log(`   ✅ Completed in ${totalDuration.toFixed(2)}ms (${throughput.toFixed(0)} ops/sec)`);
  }

  /**
   * Benchmark delete operations
   */
  async benchmarkDelete(count = 1000) {
    console.log(`🗑️ Benchmarking ${count} delete operations...`);
    
    const startTime = process.hrtime.bigint();
    
    for (let i = 0; i < count; i++) {
      const recordStart = process.hrtime.bigint();
      
      await this.db.delete({ id: `user_${i % 100}` });
      
      const recordEnd = process.hrtime.bigint();
      const duration = Number(recordEnd - recordStart) / 1000000; // ms
      this.results.delete.times.push(duration);
    }
    
    const endTime = process.hrtime.bigint();
    const totalDuration = Number(endTime - startTime) / 1000000; // ms
    const throughput = (count / totalDuration) * 1000; // ops/sec
    
    this.results.delete.throughput.push(throughput);
    
    console.log(`   ✅ Completed in ${totalDuration.toFixed(2)}ms (${throughput.toFixed(0)} ops/sec)`);
  }

  /**
   * Benchmark complex queries
   */
  async benchmarkComplexQueries(count = 100) {
    console.log(`🔬 Benchmarking ${count} complex queries...`);
    
    const startTime = process.hrtime.bigint();
    
    for (let i = 0; i < count; i++) {
      const recordStart = process.hrtime.bigint();
      
      // Complex query with multiple conditions
      await this.db.find({
        age: { $gte: 25, $lte: 40 },
        score: { $gt: 70 },
        active: true,
        'metadata.preferences.theme': 'dark'
      }, {
        limit: 50,
        sort: { score: -1 }
      });
      
      const recordEnd = process.hrtime.bigint();
      const duration = Number(recordEnd - recordStart) / 1000000; // ms
      this.results.query.times.push(duration);
    }
    
    const endTime = process.hrtime.bigint();
    const totalDuration = Number(endTime - startTime) / 1000000; // ms
    const throughput = (count / totalDuration) * 1000; // ops/sec
    
    this.results.query.throughput.push(throughput);
    
    console.log(`   ✅ Completed in ${totalDuration.toFixed(2)}ms (${throughput.toFixed(0)} ops/sec)`);
  }

  /**
   * Calculate statistics
   */
  calculateStats(values) {
    const sorted = values.sort((a, b) => a - b);
    const mean = values.reduce((a, b) => a + b, 0) / values.length;
    const median = sorted[Math.floor(sorted.length / 2)];
    const min = sorted[0];
    const max = sorted[sorted.length - 1];
    const p95 = sorted[Math.floor(sorted.length * 0.95)];
    const p99 = sorted[Math.floor(sorted.length * 0.99)];
    
    return { mean, median, min, max, p95, p99 };
  }

  /**
   * Print comprehensive results
   */
  printResults() {
    console.log('\n📊 PERFORMANCE BENCHMARK RESULTS\n');
    console.log('=' .repeat(100));
    
    for (const [operation, data] of Object.entries(this.results)) {
      if (data.times.length === 0) continue;
      
      const timeStats = this.calculateStats(data.times);
      const throughputStats = this.calculateStats(data.throughput);
      
      console.log(`\n🔧 ${operation.toUpperCase()} OPERATIONS:`);
      console.log(`   ⏱️  Time (ms): Mean=${timeStats.mean.toFixed(3)}, Median=${timeStats.median.toFixed(3)}, P95=${timeStats.p95.toFixed(3)}, P99=${timeStats.p99.toFixed(3)}`);
      console.log(`   🚀 Throughput (ops/sec): Mean=${throughputStats.mean.toFixed(0)}, Median=${throughputStats.median.toFixed(0)}, Max=${throughputStats.max.toFixed(0)}`);
    }
    
    // Performance summary
    console.log('\n🏆 PERFORMANCE SUMMARY:');
    const avgThroughput = {};
    for (const [operation, data] of Object.entries(this.results)) {
      if (data.throughput.length > 0) {
        avgThroughput[operation] = this.calculateStats(data.throughput).mean;
      }
    }
    
    const fastestOperation = Object.entries(avgThroughput).sort((a, b) => b[1] - a[1])[0];
    console.log(`   🚀 Fastest Operation: ${fastestOperation[0]} (${fastestOperation[1].toFixed(0)} ops/sec)`);
    
    // Database statistics
    if (this.db) {
      const stats = this.db.getStats();
      console.log(`\n📈 DATABASE STATISTICS:`);
      console.log(`   📊 Total Records: ${stats.recordCount || 0}`);
      console.log(`   📁 File Size: ${(stats.fileSize || 0).toFixed(2)} bytes`);
      console.log(`   🔍 Index Count: ${Object.keys(stats.indexes || {}).length}`);
    }
  }

  /**
   * Run complete benchmark suite
   */
  async runBenchmarks() {
    console.log('🚀 Starting Performance Benchmark Suite...\n');
    
    try {
      await this.initDatabase();
      
      // Run benchmarks
      await this.benchmarkBulkInsert(10000);
      await this.benchmarkInsert(1000);
      await this.benchmarkFind(1000);
      await this.benchmarkUpdate(1000);
      await this.benchmarkDelete(1000);
      await this.benchmarkComplexQueries(100);
      
      this.printResults();
      
    } catch (error) {
      console.error('❌ Benchmark failed:', error);
      throw error;
    } finally {
      await this.cleanup();
    }
  }

  /**
   * Run memory usage test
   */
  async runMemoryTest() {
    console.log('\n🧠 Testing memory usage...\n');
    
    const initialMemory = process.memoryUsage();
    console.log(`📊 Initial Memory: ${(initialMemory.heapUsed / 1024 / 1024).toFixed(2)} MB`);
    
    await this.initDatabase();
    
    // Insert large dataset
    const largeDataset = this.generateTestData(50000);
    await this.db.insertMany(largeDataset);
    
    const afterInsertMemory = process.memoryUsage();
    console.log(`📊 After Insert: ${(afterInsertMemory.heapUsed / 1024 / 1024).toFixed(2)} MB`);
    
    // Run queries
    for (let i = 0; i < 1000; i++) {
      await this.db.find({ age: { $gte: 25 } });
    }
    
    const afterQueryMemory = process.memoryUsage();
    console.log(`📊 After Queries: ${(afterQueryMemory.heapUsed / 1024 / 1024).toFixed(2)} MB`);
    
    // Memory growth
    const insertGrowth = afterInsertMemory.heapUsed - initialMemory.heapUsed;
    const queryGrowth = afterQueryMemory.heapUsed - afterInsertMemory.heapUsed;
    
    console.log(`📈 Memory Growth - Insert: ${(insertGrowth / 1024 / 1024).toFixed(2)} MB`);
    console.log(`📈 Memory Growth - Queries: ${(queryGrowth / 1024 / 1024).toFixed(2)} MB`);
    
    await this.cleanup();
  }
}

// Run benchmarks if this script is executed directly
if (require.main === module) {
  const benchmark = new PerformanceBenchmark();
  
  benchmark.runBenchmarks()
    .then(() => benchmark.runMemoryTest())
    .then(() => {
      console.log('\n✅ Performance benchmark suite completed successfully!');
      process.exit(0);
    })
    .catch(error => {
      console.error('❌ Performance benchmark failed:', error);
      process.exit(1);
    });
}

module.exports = PerformanceBenchmark; 