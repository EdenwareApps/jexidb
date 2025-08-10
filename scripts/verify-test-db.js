#!/usr/bin/env node

/**
 * Test Database Verification Script
 * Verifies that the test database was created and shows its contents
 */

const fs = require('fs').promises;
const path = require('path');

// Import Database
const Database = require('../src/index');

class TestDatabaseVerifier {
  constructor() {
    this.testDir = './test-data';
  }

  async verifyDatabase(dbPath) {
    try {
      console.log(`🔍 Verifying database: ${dbPath}`);
      
      // Check if file exists
      const exists = await fs.access(dbPath).then(() => true).catch(() => false);
      if (!exists) {
        console.log('❌ Database file does not exist');
        return false;
      }

      // Get file stats
      const stats = await fs.stat(dbPath);
      console.log(`📊 File size: ${stats.size} bytes`);

      // Initialize database
      const db = new Database(dbPath);
      await db.init();

      // Get database stats
      const dbStats = await db.getStats();
      console.log(`📈 Records: ${dbStats.summary.totalRecords}`);
      console.log(`🔗 Indexes: ${dbStats.indexes.indexCount}`);

      // Validate integrity
      const integrity = await db.validateIntegrity();
      console.log(`✅ Integrity: ${integrity.isValid ? 'Valid' : 'Invalid'}`);

      // Close database
      await db.close();

      return integrity.isValid;
    } catch (error) {
      console.error(`❌ Error verifying database: ${error.message}`);
      return false;
    }
  }

  async verifyAllDatabases() {
    try {
      const files = await fs.readdir(this.testDir);
      const jsonlFiles = files.filter(file => file.endsWith('.jdb'));

      console.log(`🔍 Found ${jsonlFiles.length} database files`);

      let validCount = 0;
      for (const file of jsonlFiles) {
        const dbPath = path.join(this.testDir, file);
        const isValid = await this.verifyDatabase(dbPath);
        if (isValid) validCount++;
        console.log('---');
      }

      console.log(`✅ Verification complete: ${validCount}/${jsonlFiles.length} databases are valid`);
      return validCount === jsonlFiles.length;
    } catch (error) {
      console.error(`❌ Error during verification: ${error.message}`);
      return false;
    }
  }
}

// Run verification
async function main() {
  const verifier = new TestDatabaseVerifier();
  
  if (process.argv.includes('--all')) {
    await verifier.verifyAllDatabases();
  } else {
    const dbPath = process.argv[2] || './test-data/test-db.jdb';
    await verifier.verifyDatabase(dbPath);
  }
}

main().catch(console.error); 