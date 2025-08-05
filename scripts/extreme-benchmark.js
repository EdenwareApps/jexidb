/**
 * Extreme Benchmark - Compare JexiDB 2.0.0 against JexiDB 1.x
 * Tests: JexiDB 2.0.0 (consolidated) vs JexiDB 1.x
 */

const fs = require('fs').promises;
const path = require('path');

// Import JexiDB (consolidated)
const { Database } = require('../src/index.js');

// Import JexiDB
const { Database: JexiDB } = require('jexidb');

class ExtremeBenchmark {
  constructor() {
    this.results = {};
    this.testData = [];
    this.databases = {};
  }

  generateTestData(count = 1000) {
    console.log(`Generating ${count} test records...`);
    
    for (let i = 0; i < count; i++) {
      this.testData.push({
        id: `user_${i}`,
        name: `User ${i}`,
        age: 18 + (i % 62),
        score: Math.floor(Math.random() * 1000),
        active: i % 3 === 0,
        email: `user${i}@example.com`,
        created: Date.now() - (i * 1000)
      });
    }
  }

  async initDatabases() {
    console.log('Initializing databases...');
    
    // Clean up previous test files
    const testDir = path.join(__dirname, '..', 'test-extreme');
    try {
      await fs.rm(testDir, { recursive: true, force: true });
      await fs.mkdir(testDir, { recursive: true });
    } catch (error) {
      // Ignore errors
    }

          // Initialize databases
      this.databases = {
        jexidb: new Database(path.join(testDir, 'jexidb.jsonl')),
        jexidb: new JexiDB(path.join(testDir, 'jexidb.jsonl'), {
          indexes: { id: 'string', age: 'number', score: 'number', active: 'boolean' }
        })
      };

    // Initialize all databases
    for (const [name, db] of Object.entries(this.databases)) {
      try {
        await db.init();
        console.log(`✓ ${name} initialized`);
      } catch (error) {
        console.error(`✗ Failed to initialize ${name}:`, error.message);
      }
    }
  }

  async cleanup() {
    console.log('Cleaning up...');
    
    for (const [name, db] of Object.entries(this.databases)) {
      try {
        if (db.close) {
          await db.close();
        }
      } catch (error) {
        // Ignore cleanup errors
      }
    }

    // Clean up test directory
    try {
      const testDir = path.join(__dirname, '..', 'test-extreme');
      await fs.rm(testDir, { recursive: true, force: true });
    } catch (error) {
      // Ignore cleanup errors
    }
  }

  measureTime(fn) {
    const start = process.hrtime.bigint();
    const result = fn();
    const end = process.hrtime.bigint();
    return {
      result,
      time: Number(end - start) / 1000000 // Convert to milliseconds
    };
  }

  async measureAsyncTime(fn) {
    const start = process.hrtime.bigint();
    const result = await fn();
    const end = process.hrtime.bigint();
    return {
      result,
      time: Number(end - start) / 1000000 // Convert to milliseconds
    };
  }

  calculateMedian(times) {
    const sorted = times.slice().sort((a, b) => a - b);
    const mid = Math.floor(sorted.length / 2);
    return sorted.length % 2 === 0
      ? (sorted[mid - 1] + sorted[mid]) / 2
      : sorted[mid];
  }

  async benchmarkSingleInsert() {
    console.log('\n🔍 Benchmarking Single Insert...');
    
    const results = {};
    const testRecord = this.testData[0];

    for (const [name, db] of Object.entries(this.databases)) {
      const times = [];
      
      for (let i = 0; i < 10; i++) {
        try {
          const { time } = await this.measureAsyncTime(async () => {
            if (name === 'jexidb') {
              return await db.insert(testRecord);
            } else {
              return await db.insert(testRecord);
            }
          });
          times.push(time);
        } catch (error) {
          console.error(`Error in ${name} single insert:`, error.message);
        }
      }
      
      results[name] = {
        median: this.calculateMedian(times),
        min: Math.min(...times),
        max: Math.max(...times),
        times
      };
    }

    this.results.singleInsert = results;
    return results;
  }

  async benchmarkBulkInsert() {
    console.log('\n📦 Benchmarking Bulk Insert...');
    
    const results = {};
    const bulkData = this.testData.slice(0, 100); // 100 records

    for (const [name, db] of Object.entries(this.databases)) {
      const times = [];
      
      for (let i = 0; i < 3; i++) {
        try {
          const { time } = await this.measureAsyncTime(async () => {
            const results = [];
            for (const record of bulkData) {
              if (name === 'jexidb') {
                results.push(await db.insert(record));
              } else {
                results.push(await db.insert(record));
              }
            }
            return results;
          });
          times.push(time);
        } catch (error) {
          console.error(`Error in ${name} bulk insert:`, error.message);
        }
      }
      
      results[name] = {
        median: this.calculateMedian(times),
        min: Math.min(...times),
        max: Math.max(...times),
        times
      };
    }

    this.results.bulkInsert = results;
    return results;
  }

  async benchmarkFind() {
    console.log('\n🔎 Benchmarking Find Operations...');
    
    const results = {};
    const queries = [
      { age: { '>=': 25 } },
      { score: { '>=': 500 } },
      { active: true },
      { id: 'user_50' }
    ];

    for (const [name, db] of Object.entries(this.databases)) {
      const times = [];
      
      for (let i = 0; i < 5; i++) {
        try {
          const { time } = await this.measureAsyncTime(async () => {
            const results = [];
            for (const query of queries) {
              if (name === 'jexidb') {
                results.push(await db.query(query));
              } else {
                results.push(await db.find(query));
              }
            }
            return results;
          });
          times.push(time);
        } catch (error) {
          console.error(`Error in ${name} find:`, error.message);
        }
      }
      
      results[name] = {
        median: this.calculateMedian(times),
        min: Math.min(...times),
        max: Math.max(...times),
        times
      };
    }

    this.results.find = results;
    return results;
  }

  async benchmarkUpdate() {
    console.log('\n✏️ Benchmarking Update Operations...');
    
    const results = {};
    const updates = { score: 999, updated: Date.now() };

    for (const [name, db] of Object.entries(this.databases)) {
      const times = [];
      
      for (let i = 0; i < 5; i++) {
        try {
          const { time } = await this.measureAsyncTime(async () => {
            if (name === 'jexidb') {
              return await db.update({ age: { '>=': 30 } }, updates);
            } else {
              return await db.update({ age: { '>=': 30 } }, updates);
            }
          });
          times.push(time);
        } catch (error) {
          console.error(`Error in ${name} update:`, error.message);
        }
      }
      
      results[name] = {
        median: this.calculateMedian(times),
        min: Math.min(...times),
        max: Math.max(...times),
        times
      };
    }

    this.results.update = results;
    return results;
  }

  async benchmarkMemoryUsage() {
    console.log('\n💾 Benchmarking Memory Usage...');
    
    const results = {};
    
    for (const [name, db] of Object.entries(this.databases)) {
      try {
        const memBefore = process.memoryUsage();
        
        // Perform some operations
        for (let i = 0; i < 50; i++) {
          const testRecord = this.testData[i % this.testData.length];
          if (name === 'jexidb') {
            await db.insert(testRecord);
          } else {
            await db.insert(testRecord);
          }
        }
        
        const memAfter = process.memoryUsage();
        
        results[name] = {
          heapUsed: memAfter.heapUsed - memBefore.heapUsed,
          heapTotal: memAfter.heapTotal - memBefore.heapTotal,
          external: memAfter.external - memBefore.external,
          rss: memAfter.rss - memBefore.rss
        };
      } catch (error) {
        console.error(`Error in ${name} memory test:`, error.message);
      }
    }

    this.results.memoryUsage = results;
    return results;
  }

  async runAllBenchmarks() {
    console.log('🚀 Starting Extreme Benchmark...');
    console.log('=' .repeat(60));
    
    try {
      this.generateTestData(1000);
      await this.initDatabases();
      
      await this.benchmarkSingleInsert();
      await this.benchmarkBulkInsert();
      await this.benchmarkFind();
      await this.benchmarkUpdate();
      await this.benchmarkMemoryUsage();
      
      this.generateReport();
      
    } catch (error) {
      console.error('Benchmark failed:', error);
    } finally {
      await this.cleanup();
    }
  }

  generateReport() {
    console.log('\n' + '='.repeat(60));
    console.log('📊 EXTREME BENCHMARK RESULTS');
    console.log('='.repeat(60));
    
    const operations = ['singleInsert', 'bulkInsert', 'find', 'update'];
    const dbNames = Object.keys(this.databases);
    
    // Create ranking table
    console.log('\n🏆 PERFORMANCE RANKING (Median Times)');
    console.log('-'.repeat(80));
    console.log('Operation'.padEnd(15) + dbNames.map(name => name.padEnd(12)).join(' | '));
    console.log('-'.repeat(80));
    
    for (const operation of operations) {
      const results = this.results[operation];
      const sorted = dbNames
        .map(name => ({ name, time: results[name]?.median || 0 }))
        .sort((a, b) => a.time - b.time);
      
      const row = operation.padEnd(15) + 
        dbNames.map(name => {
          const time = results[name]?.median || 0;
          const rank = sorted.findIndex(item => item.name === name) + 1;
          const medal = rank === 1 ? '🥇' : rank === 2 ? '🥈' : rank === 3 ? '🥉' : '  ';
          return `${medal}${time.toFixed(2)}ms`.padEnd(12);
        }).join(' | ');
      
      console.log(row);
    }
    
    // Memory usage
    console.log('\n💾 MEMORY USAGE (Heap Used)');
    console.log('-'.repeat(50));
    const memoryResults = this.results.memoryUsage;
    const sortedMemory = dbNames
      .map(name => ({ name, memory: memoryResults[name]?.heapUsed || 0 }))
      .sort((a, b) => a.memory - b.memory);
    
    for (const { name, memory } of sortedMemory) {
      const rank = sortedMemory.findIndex(item => item.name === name) + 1;
      const medal = rank === 1 ? '🥇' : rank === 2 ? '🥈' : rank === 3 ? '🥉' : '  ';
      console.log(`${medal} ${name.padEnd(12)}: ${(memory / 1024 / 1024).toFixed(2)} MB`);
    }
    
    // Detailed results
    console.log('\n📈 DETAILED RESULTS');
    console.log('-'.repeat(60));
    
    for (const operation of operations) {
      console.log(`\n${operation.toUpperCase()}:`);
      const results = this.results[operation];
      
      for (const [name, data] of Object.entries(results)) {
        console.log(`  ${name.padEnd(12)}: ${data.median.toFixed(2)}ms (min: ${data.min.toFixed(2)}ms, max: ${data.max.toFixed(2)}ms)`);
      }
    }
    
    // Winner analysis
    console.log('\n🏆 WINNER ANALYSIS');
    console.log('-'.repeat(40));
    
    const winners = {};
    for (const operation of operations) {
      const results = this.results[operation];
      const sorted = dbNames
        .map(name => ({ name, time: results[name]?.median || Infinity }))
        .sort((a, b) => a.time - b.time);
      
      winners[operation] = sorted[0];
      console.log(`${operation.padEnd(15)}: ${sorted[0].name} (${sorted[0].time.toFixed(2)}ms)`);
    }
    
    // Overall winner
    const overallScores = {};
    for (const name of dbNames) {
      overallScores[name] = operations.reduce((score, op) => {
        const rank = Object.values(this.results[op])
          .map(r => r.median)
          .sort((a, b) => a - b)
          .indexOf(this.results[op][name]?.median || Infinity) + 1;
        return score + rank;
      }, 0);
    }
    
    const overallWinner = Object.entries(overallScores)
      .sort(([,a], [,b]) => a - b)[0];
    
    console.log(`\n🏆 OVERALL WINNER: ${overallWinner[0]} (score: ${overallWinner[1]})`);
    
    // Save results
    const reportPath = path.join(__dirname, '..', 'extreme-benchmark-results.json');
    fs.writeFile(reportPath, JSON.stringify(this.results, null, 2))
      .then(() => console.log(`\n📄 Results saved to: ${reportPath}`))
      .catch(console.error);
  }
}

// Run benchmark
if (require.main === module) {
  const benchmark = new ExtremeBenchmark();
  benchmark.runAllBenchmarks().catch(console.error);
}

module.exports = ExtremeBenchmark; 