#!/usr/bin/env node

/**
 * Test Database Runner
 * Simple script to run the test database generator
 */

const fs = require('fs').promises;
const path = require('path');

// Import JexiDB
const JexiDB = require('../src/index');

class TestDatabaseGenerator {
  constructor() {
    this.testDir = './test-data';
    this.ensureTestDir();
  }

  async ensureTestDir() {
    try {
      await fs.mkdir(this.testDir, { recursive: true });
    } catch (error) {
      // Directory already exists
    }
  }

  generateTestData(count = 1000) {
    const data = [];
    const categories = ['Electronics', 'Books', 'Clothing', 'Home', 'Sports'];
    const statuses = ['pending', 'processing', 'shipped', 'delivered', 'cancelled'];
    
    for (let i = 0; i < count; i++) {
      data.push({
        id: `item_${i + 1}`,
        name: `Test Item ${i + 1}`,
        category: categories[Math.floor(Math.random() * categories.length)],
        price: Math.round(Math.random() * 1000 * 100) / 100,
        status: statuses[Math.floor(Math.random() * statuses.length)],
        created: new Date(Date.now() - Math.random() * 365 * 24 * 60 * 60 * 1000).toISOString(),
        tags: ['test', 'sample', 'data'],
        metadata: {
          weight: Math.random() * 10,
          dimensions: {
            length: Math.random() * 100,
            width: Math.random() * 50,
            height: Math.random() * 30
          }
        }
      });
    }
    
    return data;
  }

  async generateDatabase(filename = null, recordCount = 1000) {
    const timestamp = Date.now();
    const randomId = Math.random().toString(36).substring(2, 8);
    const dbName = filename || `test-db-${timestamp}-${randomId}.jdb`;
    const dbPath = path.join(this.testDir, dbName);

    console.log(`🎯 JexiDB Test Database Generator`);
    console.log(`📁 Creating: ${dbPath}`);
    console.log(`📊 Records: ${recordCount.toLocaleString()}\n`);

    try {
      // Clean up existing file
      try {
        await fs.unlink(dbPath);
      } catch (e) {}

      // Create database
      const db = new JexiDB(dbPath, {
        indexes: {
          id: 'string',
          category: 'string',
          status: 'string',
          price: 'number'
        },
        autoSave: true
      });

      await db.init();
      console.log('✅ Database initialized');

      // Generate and insert data
      console.log('📝 Generating test data...');
      const testData = this.generateTestData(recordCount);
      
      console.log('💾 Inserting data...');
      const startTime = Date.now();
      
      // Insert in batches for better performance
      const batchSize = 100;
      for (let i = 0; i < testData.length; i += batchSize) {
        const batch = testData.slice(i, i + batchSize);
        await db.insertMany(batch);
        
        if (i % 1000 === 0) {
          console.log(`   Progress: ${i}/${testData.length} records`);
        }
      }
      
      const insertTime = Date.now() - startTime;
      console.log(`✅ Inserted ${testData.length.toLocaleString()} records in ${insertTime}ms`);

      // Get final stats
      const stats = await db.getStats();
      console.log(`📈 Final stats:`);
      console.log(`   Records: ${stats.summary.totalRecords.toLocaleString()}`);
      console.log(`   File size: ${(stats.file.size / 1024 / 1024).toFixed(2)} MB`);
      console.log(`   Indexes: ${stats.indexes.indexCount}`);

      // Validate integrity
      const integrity = await db.validateIntegrity();
      console.log(`✅ Integrity: ${integrity.isValid ? 'Valid' : 'Invalid'}`);

      await db.close();
      
      console.log(`\n🎉 Database created successfully!`);
      console.log(`📁 Path: ${dbPath}`);
      
      return {
        success: true,
        path: dbPath,
        recordCount: stats.summary.totalRecords,
        fileSize: stats.file.size,
        insertTime
      };

    } catch (error) {
      console.error('❌ Error creating database:', error.message);
      return {
        success: false,
        error: error.message
      };
    }
  }

  async generateMultipleDatabases(count = 5, recordCount = 1000) {
    console.log(`🎯 Generating ${count} test databases...\n`);
    
    const results = [];
    for (let i = 0; i < count; i++) {
      console.log(`📊 Database ${i + 1}/${count}:`);
      const result = await this.generateDatabase(null, recordCount);
      results.push(result);
      console.log('---\n');
    }
    
    const successCount = results.filter(r => r.success).length;
    console.log(`✅ Generated ${successCount}/${count} databases successfully`);
    
    return results;
  }
}

// Main execution
async function main() {
  const generator = new TestDatabaseGenerator();
  
  const args = process.argv.slice(2);
  const multiple = args.includes('--multiple');
  const count = parseInt(args.find(arg => arg.startsWith('--count='))?.split('=')[1]) || 1000;
  const dbCount = parseInt(args.find(arg => arg.startsWith('--databases='))?.split('=')[1]) || 5;
  
  if (multiple) {
    await generator.generateMultipleDatabases(dbCount, count);
  } else {
    await generator.generateDatabase(null, count);
  }
}

main().catch(console.error); 