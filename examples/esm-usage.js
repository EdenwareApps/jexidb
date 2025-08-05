// Example: Using JexiDB with ESM (ES Modules)
// This file demonstrates how to use the library in modern ESM environments

import JexiDB, { utils, OPERATORS } from '../src/index.js';

async function exampleESM() {
  console.log('🚀 JexiDB ESM Example');
  console.log('=====================\n');

  // Create a new database
  const db = new JexiDB('./example-esm.jsonl', {
    indexes: {
      id: 'number',
      email: 'string',
      age: 'number'
    }
  });

  try {
    // Initialize the database
    await db.init();
    console.log('✅ Database initialized');

    // Insert some records
    const user1 = await db.insert({
      id: 1,
      name: 'John Doe',
      email: 'john@example.com',
      age: 30,
      city: 'New York'
    });

    const user2 = await db.insert({
      id: 2,
      name: 'Jane Smith',
      email: 'jane@example.com',
      age: 25,
      city: 'Los Angeles'
    });

    console.log('✅ Records inserted:', user1.name, user2.name);

    // Find records using different queries
    const allUsers = await db.find({});
    console.log('📋 All users:', allUsers.length);

    const youngUsers = await db.find({ age: { '<': 30 } });
    console.log('👶 Young users:', youngUsers.length);

    const johnUser = await db.find({ email: 'john@example.com' });
    console.log('👤 John user:', johnUser[0]?.name);

    // Update a record
    const updateResult = await db.update(
      { id: 1 },
      { age: 31, city: 'Boston' }
    );
    console.log('🔄 Updated records:', updateResult.updatedCount);

    // Use utility functions
    const stats = db.stats;
    console.log('📊 Database stats:', {
      recordCount: stats.recordCount,
      indexedFields: stats.indexedFields
    });

    // Save the database
    await db.save();
    console.log('💾 Database saved');

    // Close the database
    await db.close();
    console.log('🔒 Database closed');

  } catch (error) {
    console.error('❌ Error:', error.message);
  }
}

// Run the example
exampleESM().catch(console.error); 