// Example: Using JexiDB with ESM (ES Modules)
// This file demonstrates how to use the library in modern ESM environments

import JexiDB from 'jexidb';

async function main() {
  console.log('🚀 JexiDB Examples\n');

  // Example 1: Basic usage
  console.log('=== Example 1: Basic Usage ===');
  
  const db = new JexiDB('./example-users.jdb', {
    indexes: { id: 'number', email: 'string', age: 'number' }
  });
  
  await db.init();
  
  // Insert records
  await db.insert({ id: 1, name: 'John Doe', email: 'john@example.com', age: 30 });
  await db.insert({ id: 2, name: 'Jane Smith', email: 'jane@example.com', age: 25 });
  
  console.log('✅ Records inserted: John Doe, Jane Smith');
  
  // Find all records
  const allUsers = await db.find({});
  console.log('📋 All users:', allUsers.length);
  
  // Find with criteria
  const youngUsers = await db.find({ age: { '<': 30 } });
  console.log('👶 Young users:', youngUsers.length);
  
  // Find one record
  const john = await db.findOne({ name: 'John Doe' });
  console.log('👤 John user:', john?.name);
  
  // Update records
  const updated = await db.update({ id: 1 }, { age: 31 });
  console.log('🔄 Updated records:', updated?.length);
  
  // Get statistics
  const stats = await db.getStats();
  console.log('📊 Database stats:', stats.summary);
  
  await db.save();
  console.log('💾 Database saved');
  
  await db.destroy();
  console.log('🔒 Database closed\n');

  // Example 2: New options (create, clear)
  console.log('=== Example 2: New Options (create, clear) ===');
  
  const db2 = new JexiDB('./example-options.jdb', {
    indexes: { category: 'string' },
    create: true,
    clear: false
  });
  
  await db2.init();
  console.log('✅ Database created with create: true');
  
  await db2.insert({ id: 1, category: 'Electronics' });
  await db2.insert({ id: 2, category: 'Books' });
  await db2.save();
  await db2.destroy();
  
  // Test clear option
  const db3 = new JexiDB('./example-options.jdb', {
    indexes: { category: 'string' },
    clear: true
  });
  
  await db3.init();
  console.log('✅ Database cleared with clear: true, length:', db3.length);
  await db3.destroy();

  // Example 3: readColumnIndex
  console.log('\n=== Example 3: readColumnIndex ===');
  
  const db4 = new JexiDB('./example-columns.jdb', {
    indexes: { category: 'string', status: 'string' },
    create: true
  });
  
  await db4.init();
  
  await db4.insertMany([
    { id: 1, category: 'Electronics', status: 'active' },
    { id: 2, category: 'Books', status: 'inactive' },
    { id: 3, category: 'Electronics', status: 'active' },
    { id: 4, category: 'Clothing', status: 'active' }
  ]);
  
  // Get unique values from indexed columns
  const categories = db4.readColumnIndex('category');
  console.log('📊 Unique categories:', Array.from(categories));
  
  const statuses = db4.readColumnIndex('status');
  console.log('📊 Unique statuses:', Array.from(statuses));
  
  // Test error for non-indexed column
  try {
    db4.readColumnIndex('name');
  } catch (error) {
    console.log('✅ Correctly threw error for non-indexed column:', error.message);
  }
  
  await db4.destroy();
  
  console.log('\n🎉 All examples completed successfully!');
}

main().catch(console.error); 