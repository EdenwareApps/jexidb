#!/usr/bin/env node

/**
 * Cleanup After Tests Script
 * Removes all test files generated during test execution
 */

const fs = require('fs').promises;
const path = require('path');

async function cleanupTestFiles() {
  console.log('🧹 Cleaning up test files...');
  
  try {
    const files = await fs.readdir('.');
    let removedCount = 0;
    
    for (const file of files) {
      // Remove test database files
      if (file.startsWith('test-db-') && (file.endsWith('.jdb') || file.endsWith('.idx.jdb'))) {
        try {
          await fs.unlink(file);
          removedCount++;
        } catch (error) {
          console.log(`⚠️  Could not remove ${file}: ${error.message}`);
        }
      }
      
      // Remove debug files
      if (file.startsWith('debug-') && (file.endsWith('.jdb') || file.endsWith('.idx.jdb'))) {
        try {
          await fs.unlink(file);
          removedCount++;
        } catch (error) {
          console.log(`⚠️  Could not remove ${file}: ${error.message}`);
        }
      }
    }
    
    if (removedCount > 0) {
      console.log(`✅ Cleaned up ${removedCount} test files`);
    } else {
      console.log('✅ No test files to clean up');
    }
    
  } catch (error) {
    console.error('❌ Error during cleanup:', error.message);
    process.exit(1);
  }
}

// Run cleanup if called directly
if (require.main === module) {
  cleanupTestFiles().catch(error => {
    console.error('❌ Cleanup failed:', error.message);
    process.exit(1);
  });
}

module.exports = { cleanupTestFiles }; 