// Simple test to demonstrate readColumnIndex method
const JexiDB = require('../dist/index.js').default;

async function testReadColumnIndex() {
  console.log('🧪 Testing readColumnIndex Method\n');

  const db = new JexiDB('./test-columns.jdb', { 
    indexes: { category: 'string', status: 'string' },
    create: true
  });
  
  await db.init();
  
  // Insert test data with duplicates
  await db.insertMany([
    { id: 1, category: 'Electronics', status: 'active', price: 100 },
    { id: 2, category: 'Books', status: 'inactive', price: 20 },
    { id: 3, category: 'Electronics', status: 'active', price: 150 }, // Duplicate category and status
    { id: 4, category: 'Clothing', status: 'active', price: 50 },
    { id: 5, category: 'Electronics', status: 'active', price: 200 }  // Another duplicate
  ]);
  await db.save();
  
  console.log('📊 Test Data Inserted:');
  console.log('  - 5 records total');
  console.log('  - 3 Electronics, 1 Books, 1 Clothing');
  console.log('  - 4 active, 1 inactive status');
  console.log('  - 5 different prices\n');
  
  // Test indexed columns
  console.log('=== Indexed Columns ===');
  
  const categories = db.readColumnIndex('category');
  console.log('📂 readColumnIndex("category"):', Array.from(categories));
  // Expected: ['Electronics', 'Books', 'Clothing']
  
  const statuses = db.readColumnIndex('status');
  console.log('📊 readColumnIndex("status"):', Array.from(statuses));
  // Expected: ['active', 'inactive']
  
  // Test non-indexed columns (should throw error)
  console.log('\n=== Non-Indexed Columns ===');
  
  try {
    const prices = db.readColumnIndex('price');
    console.log('❌ This should not appear');
  } catch (error) {
    console.log('❌ readColumnIndex("price") error:', error.message);
  }
  
  await db.destroy();
  console.log('\n✅ Test completed successfully!');
}

testReadColumnIndex().catch(console.error);
