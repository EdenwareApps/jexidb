// Simple Benchmark: JexiDB Performance Test
// Focus on core operations performance

import Database from '../src/index.js';
import { promises as fs } from 'fs';

class SimpleBenchmark {
  constructor() {
    this.testSize = 1000; // Smaller dataset for faster testing
    this.testData = [];
  }

  generateTestData() {
    console.log('📊 Generating test data...');
    this.testData = [];
    
    for (let i = 0; i < this.testSize; i++) {
      this.testData.push({
        id: i + 1,
        name: `User ${i + 1}`,
        email: `user${i + 1}@example.com`,
        age: Math.floor(Math.random() * 50) + 18,
        score: Math.floor(Math.random() * 1000),
        active: Math.random() > 0.5
      });
    }
    console.log(`✅ Generated ${this.testData.length} test records`);
  }

  async benchmarkJexiDB2() {
    console.log('\n🚀 Benchmarking Database...');
    const dbPath = './benchmark-jexidb-2.jsonl';
    
    // Clean up
    try {
      await fs.unlink(dbPath);
    } catch (e) {}

    const db = new Database(dbPath, {
      indexes: { id: 'number', age: 'number', score: 'number' }
    });

    const results = {};

    // Initialize
    const initStart = performance.now();
    await db.init();
    results.init = performance.now() - initStart;

    // Bulk Insert
    const insertStart = performance.now();
    for (const record of this.testData) {
      await db.insert(record);
    }
    results.bulkInsert = performance.now() - insertStart;
    results.insertPerSecond = (this.testData.length / (results.bulkInsert / 1000)).toFixed(2);

    // Save
    const saveStart = performance.now();
    await db.save();
    results.save = performance.now() - saveStart;

    // Find by ID (indexed)
    const findStart = performance.now();
    const found = await db.find({ id: 1 });
    results.findById = performance.now() - findStart;

    // Find by age range (indexed)
    const findAgeStart = performance.now();
    const ageResults = await db.find({ age: { '>=': 25 } });
    results.findByAge = performance.now() - findAgeStart;
    results.ageResultsCount = ageResults.length;

    // Update
    const updateStart = performance.now();
    await db.update({ id: 1 }, { age: 35, score: 999 });
    results.update = performance.now() - updateStart;

    // Delete
    const deleteStart = performance.now();
    await db.delete({ id: 2 });
    results.delete = performance.now() - deleteStart;

    // Close
    const closeStart = performance.now();
    await db.close();
    results.close = performance.now() - closeStart;

    console.log('✅ Database benchmark completed');
    return results;
  }

  printResults(jexidb2Results) {
    console.log('\n' + '='.repeat(70));
    console.log('📊 BENCHMARK RESULTS: Database Performance Test');
    console.log('='.repeat(70));
    
    console.log('\n⏱️  Performance Comparison (Lower is Better):');
    console.log('─'.repeat(70));
          console.log('Operation'.padEnd(20) + 'Database (ms)'.padEnd(15) + 'Status');
    console.log('─'.repeat(70));
    
    const operations = ['init', 'bulkInsert', 'save', 'findById', 'findByAge', 'update', 'delete', 'close'];
    
    for (const op of operations) {
      if (jexidb2Results[op]) {
        const jexidb2Time = jexidb2Results[op];
        const status = '✅';
        
        console.log(
          op.padEnd(20) +
          jexidb2Time.toFixed(2).padEnd(15) +
          status
        );
      }
    }
    
    console.log('\n🚀 Insert Performance:');
    console.log('─'.repeat(70));
          console.log(`Database: ${jexidb2Results.insertPerSecond} records/second`);
    
    console.log('\n📊 Query Results:');
    console.log('─'.repeat(70));
          console.log(`Age >= 25 results: Database=${jexidb2Results.ageResultsCount}`);
    
    // Summary
    console.log('\n📈 Summary:');
    console.log('─'.repeat(70));
    
    const fasterOperations = operations.filter(op => 
      jexidb2Results[op]
    ).length;
    
          console.log(`✅ Completed ${fasterOperations}/${operations.length} operations`);
    
    if (fasterOperations > 0) {
      const avgImprovement = operations
        .filter(op => jexidb2Results[op])
        .reduce((sum, op) => {
          const improvement = 0; // No direct comparison to 1.x, so no improvement
          return sum + improvement;
        }, 0) / fasterOperations;
      
      console.log(`📊 Average improvement: ${avgImprovement.toFixed(2)}%`);
    }
    
          console.log('\n🎯 Key Advantages:');
    console.log('─'.repeat(70));
    console.log('• Hybrid architecture (indexed + on-demand reading)');
    console.log('• Batch insert optimization');
    console.log('• Memory-efficient for large datasets');
    console.log('• Better query performance with complex criteria');
    console.log('• Optimized file I/O operations');
    
    console.log('\n' + '='.repeat(70));
  }

  async run() {
    console.log('🏁 Starting Database Performance Benchmark');
    console.log(`📊 Test dataset: ${this.testSize.toLocaleString()} records`);
    console.log('⏳ This may take a minute...\n');
    
    this.generateTestData();
    
    const jexidb2Results = await this.benchmarkJexiDB2();
    
    this.printResults(jexidb2Results);
    
    // Cleanup
    try {
      await fs.unlink('./benchmark-jexidb-2.jsonl');
    } catch (e) {}
  }
}

// Run benchmark
const benchmark = new SimpleBenchmark();
benchmark.run().catch(console.error); 