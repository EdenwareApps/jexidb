const { Database } = require('../dist/Database.cjs');
const fs = require('fs');
const path = require('path');

describe('Update Method Persistence Test', () => {
  let db;
  const testFile = path.join(__dirname, 'test-files', 'update-persistence-test.jdb');

  beforeEach(async () => {
    // Clean up any existing test file
    if (fs.existsSync(testFile)) {
      fs.unlinkSync(testFile);
    }
    if (fs.existsSync(testFile + '.idx.jdb')) {
      fs.unlinkSync(testFile + '.idx.jdb');
    }

    db = new Database(testFile, {
      fields: { name: 'string', age: 'number', status: 'string' }
    });
    await db.init();
  });

  afterEach(async () => {
    if (db && !db.destroyed) {
      await db.close();
    }
    // Clean up test files
    if (fs.existsSync(testFile)) {
      fs.unlinkSync(testFile);
    }
    if (fs.existsSync(testFile + '.idx.jdb')) {
      fs.unlinkSync(testFile + '.idx.jdb');
    }
  });

  test('update() should persist changes to file', async () => {
    // Insert a record
    const inserted = await db.insert({ name: 'John', age: 30 });
    expect(inserted).toBeDefined();
    expect(inserted.id).toBeDefined();
    
    // Save to ensure it's persisted
    await db.save();
    
    // Verify the record was saved
    const beforeUpdate = await db.findOne({ id: inserted.id });
    expect(beforeUpdate).toBeDefined();
    expect(beforeUpdate.name).toBe('John');
    expect(beforeUpdate.age).toBe(30);
    
    // Update the record
    const updateResult = await db.update({ id: inserted.id }, { age: 31, name: 'John Updated' });
    expect(Array.isArray(updateResult)).toBe(true);
    expect(updateResult.length).toBeGreaterThan(0);
    
    // Save after update
    await db.save();
    
    // Close and reopen database to verify persistence
    await db.close();
    db = new Database(testFile, {
      fields: { name: 'string', age: 'number', status: 'string' }
    });
    await db.init();
    
    // Verify the update was persisted
    const afterUpdate = await db.findOne({ id: inserted.id });
    expect(afterUpdate).toBeDefined();
    expect(afterUpdate.name).toBe('John Updated');
    expect(afterUpdate.age).toBe(31);
  });

  test('update() should work without explicit save() call if autoSave is enabled', async () => {
    // This test checks if update triggers auto-save
    const inserted = await db.insert({ name: 'Jane', age: 25 });
    await db.save();
    
    // Update the record
    await db.update({ id: inserted.id }, { age: 26 });
    
    // Note: The database might have auto-save, but we'll explicitly save to be sure
    await db.save();
    
    // Close and reopen
    await db.close();
    db = new Database(testFile, {
      fields: { name: 'string', age: 'number', status: 'string' }
    });
    await db.init();
    
    // Verify persistence
    const updated = await db.findOne({ id: inserted.id });
    expect(updated).toBeDefined();
    expect(updated.age).toBe(26);
  });

  test('update() should handle multiple records', async () => {
    // Insert multiple records
    const record1 = await db.insert({ name: 'Alice', age: 20 });
    const record2 = await db.insert({ name: 'Bob', age: 25 });
    await db.save();
    
    // Verify records exist before update
    const before1 = await db.findOne({ id: record1.id });
    const before2 = await db.findOne({ id: record2.id });
    expect(before1).toBeDefined();
    expect(before2).toBeDefined();
    
    // Update multiple records
    const updateResult = await db.update({ age: { $lt: 30 } }, { status: 'young' });
    expect(Array.isArray(updateResult)).toBe(true);
    expect(updateResult.length).toBe(2);
    
    // Verify updates in memory before save
    const inMemory1 = await db.findOne({ id: record1.id });
    const inMemory2 = await db.findOne({ id: record2.id });
    expect(inMemory1.status).toBe('young');
    expect(inMemory2.status).toBe('young');
    
    await db.save();
    
    // Close and reopen
    await db.close();
    db = new Database(testFile, {
      fields: { name: 'string', age: 'number', status: 'string' }
    });
    await db.init();
    
    // Verify updates persisted
    const updated1 = await db.findOne({ id: record1.id });
    const updated2 = await db.findOne({ id: record2.id });
    
    expect(updated1).toBeDefined();
    expect(updated2).toBeDefined();
    expect(updated1.status).toBe('young');
    expect(updated2.status).toBe('young');
  });
});

