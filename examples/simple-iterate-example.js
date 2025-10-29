/**
 * Simple example of the iterate() method
 * Demonstrates bulk updates with streaming performance
 */

import { Database } from '../src/Database.mjs'

async function simpleIterateExample() {
  console.log('🚀 JexiDB iterate() Method - Simple Example\n')
  
  // Create database
  const db = new Database('simple-iterate.jdb', {
    debugMode: false,
    termMapping: true,
    indexedFields: ['category', 'price']
  })
  
  await db.init()
  
  try {
    // 1. Insert sample data
    console.log('📝 Inserting sample data...')
    const products = [
      { id: 1, name: 'Apple', category: 'fruits', price: 1.50, inStock: true },
      { id: 2, name: 'Banana', category: 'fruits', price: 0.80, inStock: true },
      { id: 3, name: 'Orange', category: 'fruits', price: 1.20, inStock: false },
      { id: 4, name: 'Carrot', category: 'vegetables', price: 0.60, inStock: true },
      { id: 5, name: 'Broccoli', category: 'vegetables', price: 1.80, inStock: true }
    ]
    
    for (const product of products) {
      await db.insert(product)
    }
    
    console.log(`✅ Inserted ${products.length} products\n`)
    
    // 2. Basic iteration - list all fruits
    console.log('🍎 Listing all fruits:')
    for await (const entry of db.iterate({ category: 'fruits' })) {
      console.log(`  - ${entry.name}: $${entry.price} (${entry.inStock ? 'in stock' : 'out of stock'})`)
    }
    console.log()
    
    // 3. Bulk price update - 10% increase for in-stock items
    console.log('💰 Bulk price update - 10% increase for in-stock items:')
    let updatedCount = 0
    
    for await (const entry of db.iterate({ inStock: true })) {
      const oldPrice = entry.price
      entry.price = Math.round(entry.price * 1.1 * 100) / 100 // 10% increase
      entry.lastUpdated = new Date().toISOString()
      
      if (oldPrice !== entry.price) {
        console.log(`  💵 ${entry.name}: $${oldPrice} → $${entry.price}`)
        updatedCount++
      }
    }
    
    console.log(`✅ Updated ${updatedCount} products\n`)
    
    // 4. Bulk status update - mark all vegetables as organic
    console.log('🌱 Marking all vegetables as organic:')
    let organicCount = 0
    
    for await (const entry of db.iterate({ category: 'vegetables' })) {
      entry.organic = true
      entry.certifiedAt = new Date().toISOString()
      console.log(`  🌱 ${entry.name} is now organic`)
      organicCount++
    }
    
    console.log(`✅ Marked ${organicCount} vegetables as organic\n`)
    
    // 5. Performance test with progress tracking
    console.log('⚡ Performance test with progress tracking:')
    let processedCount = 0
    
    for await (const entry of db.iterate(
      {}, // All records
      { 
        chunkSize: 2,
        progressCallback: (progress) => {
          if (progress.completed) {
            console.log(`📊 Final stats: ${progress.processed} processed, ${progress.modified} modified in ${progress.elapsed}ms`)
          }
        }
      }
    )) {
      // Add processing timestamp
      entry.processedAt = Date.now()
      processedCount++
    }
    
    console.log(`⚡ Processed ${processedCount} records\n`)
    
    // 6. Verify all changes
    console.log('📋 Final results:')
    const allProducts = await db.find({})
    for (const product of allProducts) {
      console.log(`  - ${product.name} (${product.category}): $${product.price} ${product.organic ? '🌱' : ''} ${product.inStock ? '✅' : '❌'}`)
    }
    
  } finally {
    // Clean up
    await db.close()
    console.log('\n🧹 Database closed and cleaned up')
  }
}

// Run the example
if (import.meta.url === `file://${process.argv[1]}`) {
  simpleIterateExample().catch(console.error)
}

export { simpleIterateExample }
