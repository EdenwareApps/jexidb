/**
 * Example usage of the new iterate() method
 * Demonstrates bulk update capabilities with streaming performance
 */

import { Database } from '../src/Database.mjs'

async function demonstrateIterate() {
  console.log('🚀 JexiDB iterate() Method Demo\n')
  
  // Create database with indexing
  const db = new Database('iterate-demo.jdb', {
    debugMode: true,
    termMapping: true,
    indexedFields: ['category', 'price', 'status']
  })
  
  await db.init()
  
  try {
    // 1. Insert sample data
    console.log('📝 Inserting sample data...')
    const sampleData = [
      { id: 1, name: 'Apple', category: 'fruits', price: 1.50, status: 'active' },
      { id: 2, name: 'Banana', category: 'fruits', price: 0.80, status: 'active' },
      { id: 3, name: 'Orange', category: 'fruits', price: 1.20, status: 'inactive' },
      { id: 4, name: 'Carrot', category: 'vegetables', price: 0.60, status: 'active' },
      { id: 5, name: 'Broccoli', category: 'vegetables', price: 1.80, status: 'active' },
      { id: 6, name: 'Lettuce', category: 'vegetables', price: 0.90, status: 'inactive' },
      { id: 7, name: 'Milk', category: 'dairy', price: 2.50, status: 'active' },
      { id: 8, name: 'Cheese', category: 'dairy', price: 3.20, status: 'active' },
      { id: 9, name: 'Yogurt', category: 'dairy', price: 1.80, status: 'inactive' },
      { id: 10, name: 'Bread', category: 'bakery', price: 2.00, status: 'active' }
    ]
    
    for (const item of sampleData) {
      await db.insert(item)
    }
    
    console.log(`✅ Inserted ${sampleData.length} records\n`)
    
    // 2. Basic iteration without modifications
    console.log('🔍 Basic iteration - listing all fruits:')
    for await (const entry of db.iterate({ category: 'fruits' })) {
      console.log(`  - ${entry.name}: $${entry.price} (${entry.status})`)
    }
    console.log()
    
    // 3. Bulk price update with progress tracking
    console.log('💰 Bulk price update - 10% increase for all active items:')
    let processedCount = 0
    let modifiedCount = 0
    
    for await (const entry of db.iterate(
      { status: 'active' }, 
      { 
        chunkSize: 5,
        progressCallback: (progress) => {
          if (progress.processed % 5 === 0) {
            console.log(`  📊 Progress: ${progress.processed} processed, ${progress.modified} modified`)
          }
        }
      }
    )) {
      const oldPrice = entry.price
      entry.price = Math.round(entry.price * 1.1 * 100) / 100 // 10% increase, rounded to 2 decimals
      entry.lastUpdated = new Date().toISOString()
      
      if (oldPrice !== entry.price) {
        console.log(`  💵 ${entry.name}: $${oldPrice} → $${entry.price}`)
      }
    }
    
    console.log(`✅ Price update completed: ${processedCount} processed, ${modifiedCount} modified\n`)
    
    // 4. Bulk status change with deletions
    console.log('🗑️ Bulk operations - removing inactive items and updating status:')
    const inactiveItems = await db.find({ status: 'inactive' })
    for (const item of inactiveItems) {
      console.log(`  🗑️ Deleting inactive item: ${item.name}`)
      await db.delete({ id: item.id })
    }
    
    // Update remaining items
    for await (const entry of db.iterate({})) {
      if (entry.status === 'active') {
        entry.status = 'available'
        entry.updatedAt = new Date().toISOString()
      }
    }
    
    console.log('✅ Bulk operations completed\n')
    
    // 5. Verify results
    console.log('📋 Final results:')
    const allItems = await db.find({})
    console.log(`Total items remaining: ${allItems.length}`)
    
    for (const item of allItems) {
      console.log(`  - ${item.name} (${item.category}): $${item.price} [${item.status}]`)
    }
    
    // 6. Performance demonstration with larger dataset
    console.log('\n⚡ Performance test with larger dataset...')
    
    // Insert more data for performance test
    const startTime = Date.now()
    for (let i = 11; i <= 1000; i++) {
      await db.insert({
        id: i,
        name: `Product${i}`,
        category: ['electronics', 'clothing', 'books', 'home'][i % 4],
        price: Math.random() * 100,
        status: i % 3 === 0 ? 'inactive' : 'active'
      })
    }
    
    const insertTime = Date.now() - startTime
    console.log(`📝 Inserted 990 additional records in ${insertTime}ms`)
    
    // Bulk update with performance tracking
    const updateStartTime = Date.now()
    let totalProcessed = 0
    
    for await (const entry of db.iterate(
      { status: 'active' },
      { 
        chunkSize: 100,
        progressCallback: (progress) => {
          if (progress.completed) {
            totalProcessed = progress.processed
            console.log(`📊 Final stats: ${progress.processed} processed, ${progress.modified} modified in ${progress.elapsed}ms`)
          }
        }
      }
    )) {
      // Add a processing timestamp
      entry.processedAt = Date.now()
    }
    
    const updateTime = Date.now() - updateStartTime
    console.log(`⚡ Bulk update completed: ${totalProcessed} records processed in ${updateTime}ms`)
    console.log(`📈 Performance: ${Math.round(totalProcessed / (updateTime / 1000))} records/second`)
    
  } finally {
    // Clean up
    await db.close()
    console.log('\n🧹 Database closed and cleaned up')
  }
}

// Run the demonstration
if (import.meta.url === `file://${process.argv[1]}`) {
  demonstrateIterate().catch(console.error)
}

export { demonstrateIterate }
