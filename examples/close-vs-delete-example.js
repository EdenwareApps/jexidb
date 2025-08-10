import Database from '../src/index.js';

async function demonstrateCloseVsDelete() {
  console.log('🔍 Demonstrating close() vs destroy() vs deleteDatabase()\n');

  // Example 1: close() - Fecha instância, mantém arquivo
  console.log('📁 Example 1: close() - Closes instance, keeps file');
  const db1 = new Database('./example-close.jdb', { indexes: { id: 'number' } });
  await db1.init();
  await db1.insert({ id: 1, name: 'John' });
  await db1.insert({ id: 2, name: 'Jane' });
  
  console.log('   Records before close:', await db1.count());
  await db1.close();  // Fecha instância, mantém arquivo
  console.log('   ✅ Instance closed, file preserved\n');

  // Reopen the same file
  const db1Reopened = new Database('./example-close.jdb', { indexes: { id: 'number' } });
  await db1Reopened.init();
  console.log('   Records after reopening:', await db1Reopened.count());
  await db1Reopened.close();
  console.log('   ✅ Same data preserved\n');

  // Example 2: destroy() - Equivalente a close()
  console.log('📁 Example 2: destroy() - Equivalent to close()');
  const db2 = new Database('./example-destroy.jdb', { indexes: { id: 'number' } });
  await db2.init();
  await db2.insert({ id: 1, name: 'Alice' });
  await db2.insert({ id: 2, name: 'Bob' });
  
  console.log('   Records before destroy:', await db2.count());
  await db2.destroy();  // Equivalente a close()
  console.log('   ✅ Instance destroyed, file preserved\n');

  // Reopen the same file
  const db2Reopened = new Database('./example-destroy.jdb', { indexes: { id: 'number' } });
  await db2Reopened.init();
  console.log('   Records after reopening:', await db2Reopened.count());
  await db2Reopened.close();
  console.log('   ✅ Same data preserved\n');

  // Example 3: deleteDatabase() - Remove arquivo físico
  console.log('🗑️  Example 3: deleteDatabase() - Removes physical file');
  const db3 = new Database('./example-delete.jdb', { indexes: { id: 'number' } });
  await db3.init();
  await db3.insert({ id: 1, name: 'Charlie' });
  await db3.insert({ id: 2, name: 'Diana' });
  
  console.log('   Records before delete:', await db3.count());
  
  // Delete database file permanently
  await db3.deleteDatabase();
  console.log('   ✅ Database file deleted\n');

  // Try to reopen - should fail
  try {
    const db3Reopened = new Database('./example-delete.jdb', { indexes: { id: 'number' } });
    await db3Reopened.init();
  } catch (error) {
    console.log('   ❌ Cannot reopen (expected):', error.message);
  }

  console.log('\n📋 Summary:');
  console.log('   • close()     - Closes instance, keeps file');
  console.log('   • destroy()   - Equivalent to close()');
  console.log('   • deleteDatabase() - Permanently deletes database file');
  console.log('   • removeDatabase() - Alias for deleteDatabase()');
}

// Run the demonstration
demonstrateCloseVsDelete().catch(console.error);
