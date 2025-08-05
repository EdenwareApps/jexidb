/**
 * Cleanup Temporary Files Script
 * Removes temporary test files and directories created during development
 */

const fs = require('fs').promises;
const path = require('path');

async function cleanupTempFiles() {
  console.log('🧹 Cleaning up temporary files...');
  
  const rootDir = path.join(__dirname, '..');
  const files = await fs.readdir(rootDir);
  
  let removedCount = 0;
  
  for (const file of files) {
    const filePath = path.join(rootDir, file);
    const stat = await fs.stat(filePath);
    
    // Remove test database files
          if (file.startsWith('test-db-') && file.endsWith('.jdb')) {
      try {
        await fs.unlink(filePath);
        removedCount++;
      } catch (error) {
        console.log(`⚠️  Could not remove: ${file} - ${error.message}`);
      }
    }
    
    // Remove test utility directories
    if (file.startsWith('test-utils-') && stat.isDirectory()) {
      try {
        await fs.rm(filePath, { recursive: true, force: true });
        console.log(`🗑️  Removed directory: ${file}`);
        removedCount++;
      } catch (error) {
        console.log(`⚠️  Could not remove directory: ${file} - ${error.message}`);
      }
    }
    
    // Remove debug files
          if (file.startsWith('debug-') && file.endsWith('.jdb')) {
      try {
        await fs.unlink(filePath);
        removedCount++;
      } catch (error) {
        console.log(`⚠️  Could not remove: ${file} - ${error.message}`);
      }
    }
  }
  
  console.log(`\n✅ Cleanup completed! Removed ${removedCount} files/directories.`);
}

// Run cleanup
if (require.main === module) {
  cleanupTempFiles().catch(console.error);
}

module.exports = cleanupTempFiles; 