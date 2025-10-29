#!/usr/bin/env node

import { spawn } from 'child_process'
import { fileURLToPath } from 'url'
import { dirname, join } from 'path'

const __filename = fileURLToPath(import.meta.url)
const __dirname = dirname(__filename)

async function runTests() {
  const startTime = Date.now()
  
  console.log('🧪 Running JexiDB tests...')
  
  // Run Jest tests
  const jestProcess = spawn('npx', ['jest', ...process.argv.slice(2)], {
    stdio: 'inherit',
    shell: true
  })
  
  jestProcess.on('close', async (code) => {
    if (code !== 0) {
      console.error(`❌ Tests failed with exit code ${code}`)
      process.exit(code)
    }
    
    // Run cleanup
    const cleanupProcess = spawn('npm', ['run', 'clean:test-files'], {
      stdio: 'inherit',
      shell: true
    })
    
    cleanupProcess.on('close', (cleanupCode) => {
      const endTime = Date.now()
      const duration = Math.round((endTime - startTime) / 1000)
  
      // Display completion time
      const completionTime = new Date().toLocaleString('en-US', {
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: true
      });
      
      console.log(`✅ Tests completed at: ${completionTime}`);
      console.log(`📦 Total execution time: ${duration}s`)
      
      if (cleanupCode !== 0) {
        console.warn(`⚠️  Cleanup completed with warnings (exit code: ${cleanupCode})`)
      }
      
      process.exit(0)
    })
  })
  
  jestProcess.on('error', (error) => {
    console.error('❌ Failed to start Jest:', error.message)
    process.exit(1)
  })
}

// Handle process termination
process.on('SIGINT', () => {
  console.log('\n🛑 Test execution interrupted by user')
  process.exit(0)
})

process.on('SIGTERM', () => {
  console.log('\n🛑 Test execution terminated')
  process.exit(0)
})

// Run the tests
runTests().catch(error => {
  console.error('❌ Fatal error:', error.message)
  process.exit(1)
})
