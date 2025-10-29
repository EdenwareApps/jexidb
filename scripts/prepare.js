#!/usr/bin/env node

import { existsSync } from 'fs';
import { execSync } from 'child_process';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const rootDir = join(__dirname, '..');
const distFile = join(rootDir, 'dist', 'Database.cjs');

// Check if the bundle already exists
if (existsSync(distFile)) {
  console.log('✅ Bundle already exists, skipping build');
  process.exit(0);
}

console.log('📦 Bundle not found, building...');

try {
  execSync('npm run build', { 
    stdio: 'inherit',
    cwd: rootDir
  });
  console.log('✅ Build completed successfully');
} catch (error) {
  console.error('❌ Build failed:', error.message);
  process.exit(1);
}

