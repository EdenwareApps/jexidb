#!/usr/bin/env node

const fs = require('fs').promises;
const path = require('path');

// Import JexiDB
const JexiDB = require('../dist/index.js').default;

async function simpleTest() {
  const testFile = './simple-test.jdb';
  
  // Clean up
  try {
    await fs.unlink(testFile);
  } catch (e) {}
  
  console.log('🧪 Simple JexiDB Test');
  console.log('=====================\n');
  
  try {
    // Create database
    console.log('1️⃣ Creating database...');
    const db = new JexiDB(testFile, {
      indexes: { id: 'number', email: 'string' },
      autoSave: true
    });
    
    await db.init();
    console.log('✅ Database initialized\n');
    
    // Insert data
    console.log('2️⃣ Inserting test data...');
    const users = [
      { id: 1, name: 'John Doe', email: 'john@example.com', age: 30 },
      { id: 2, name: 'Jane Smith', email: 'jane@example.com', age: 25 },
      { id: 3, name: 'Bob Johnson', email: 'bob@example.com', age: 35 }
    ];
    
    for (const user of users) {
      await db.insert(user);
      console.log(`   ✅ Inserted: ${user.name}`);
    }
    console.log('');
    
    // Query data
    console.log('3️⃣ Querying data...');
    
    const allUsers = await db.find({});
    console.log(`   📊 Total users: ${allUsers.length}`);
    
    const john = await db.findOne({ id: 1 });
    console.log(`   👤 Found John: ${john ? john.name : 'Not found'}`);
    
    const youngUsers = await db.find({ age: { '<': 30 } });
    console.log(`   🧒 Young users (< 30): ${youngUsers.length}`);
    
    const janeByEmail = await db.findOne({ email: 'jane@example.com' });
    console.log(`   📧 Found by email: ${janeByEmail ? janeByEmail.name : 'Not found'}`);
    console.log('');
    
    // Update data
    console.log('4️⃣ Updating data...');
    await db.update({ id: 1 }, { age: 31 });
    const updatedJohn = await db.findOne({ id: 1 });
    console.log(`   🔄 John's new age: ${updatedJohn.age}`);
    console.log('');
    
    // Count data
    console.log('5️⃣ Counting data...');
    const totalCount = await db.count();
    const youngCount = await db.count({ age: { '<': 30 } });
    console.log(`   📊 Total count: ${totalCount}`);
    console.log(`   🧒 Young count: ${youngCount}`);
    console.log('');
    
    // Get stats
    console.log('6️⃣ Getting statistics...');
    const stats = await db.getStats();
    console.log(`   📈 Records: ${stats.summary.totalRecords}`);
    console.log(`   📁 File size: ${stats.file.size} bytes`);
    console.log(`   🔗 Indexes: ${stats.indexes.indexCount}`);
    console.log('');
    
    // Validate integrity
    console.log('7️⃣ Validating integrity...');
    const integrity = await db.validateIntegrity();
    console.log(`   ✅ Integrity: ${integrity.isValid ? 'Valid' : 'Invalid'}`);
    console.log('');
    
    // Clean up
    console.log('8️⃣ Cleaning up...');
    await db.destroy();
    console.log('   🗑️ Database destroyed');
    
    console.log('\n🎉 All tests passed successfully!');
    
  } catch (error) {
    console.error('❌ Test failed:', error.message);
    process.exit(1);
  }
}

// Run test
simpleTest(); 