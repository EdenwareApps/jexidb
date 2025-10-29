#!/usr/bin/env node

import fs from 'fs';
import path from 'path';

function cleanTestFiles() {
  const startTime = Date.now();
  const currentDir = process.cwd();
  const files = fs.readdirSync(currentDir);
  
  const testFilePatterns = [
    /^test-db-.*\.jdb$/,
    /^test-db-.*\.offsets\.jdb$/,
    /^test-db-.*\.idx\.jdb$/,
    /^test-db-.*$/, // Files without extension
    /^test-normalize.*\.jdb$/,
    /^test-normalize.*\.offsets\.jdb$/,
    /^test-normalize.*\.idx\.jdb$/,
    /^test-normalize.*$/, // Files without extension
    /^test-confusion.*\.jdb$/,
    /^test-confusion.*\.offsets\.jdb$/,
    /^test-confusion.*\.idx\.jdb$/,
    /^test-confusion.*$/, // Files without extension
    /^debug-.*\.jdb$/,
    /^debug-.*\.offsets\.jdb$/,
    /^debug-.*\.idx\.jdb$/,
    /^debug-.*$/, // Files without extension
    /^test-simple-.*$/, // test-simple files
    /^test-count\.jdb$/, // test-count.jdb file
    /^test-index-persistence-.*\.jdb$/, // index persistence test files
    /^test-index-persistence-.*\.idx\.jdb$/, // index persistence idx files
    /^test-indexed-mode-.*\.jdb$/, // indexed mode test files
    /^test-indexed-mode-.*\.idx\.jdb$/, // indexed mode idx files
    /^test-term-mapping-.*\.jdb$/, // term mapping test files
    /^test-term-mapping-.*\.idx\.jdb$/, // term mapping idx files
    /^test-.*\.jdb$/, // Any test file with .jdb extension
    /^test-.*\.idx\.jdb$/ // Any test file with .idx.jdb extension
  ];
  
  let cleanedCount = 0;
  
  files.forEach(file => {
    const filePath = path.join(currentDir, file);
    const stats = fs.statSync(filePath);
    
    if (stats.isFile()) {
      const shouldDelete = testFilePatterns.some(pattern => pattern.test(file));
      
      if (shouldDelete) {
        try {
          fs.unlinkSync(filePath);
          cleanedCount++;
        } catch (error) {
          console.warn(`Warning: Could not delete ${file}: ${error.message}`);
        }
      }
    }
  });
  
  const endTime = Date.now();
  const duration = endTime - startTime;
  
  if (cleanedCount > 0) {
    console.log(`✅ Cleaned ${cleanedCount} test files.`);
  } else {
    console.log('No test files found to clean.');
  }
}

// Run cleanup if this script is executed directly
if (import.meta.url === `file://${process.argv[1]}` || process.argv[1]?.endsWith('clean-test-files.js')) {
  cleanTestFiles();
}

export default cleanTestFiles;
