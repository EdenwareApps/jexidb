const JexiDB = require('../dist/index.js').default;
const fs = require('fs').promises;
const path = require('path');

describe('JSONLDatabase', () => {
  let db;
  let testFile;

  beforeEach(async () => {
    // Create unique test file for each test to ensure isolation
    const timestamp = Date.now();
    const randomId = Math.random().toString(36).substring(7);
    testFile = `./test-db-${timestamp}-${randomId}.jsonl`;

    // Create new instance with background maintenance disabled
    db = new JexiDB(testFile, {
      indexes: {
        id: 'number',
        email: 'string',
        age: 'number'
      },
      autoSave: false, // Disable auto-save for tests
      backgroundMaintenance: false // Disable background maintenance for tests
    });
  });

  afterEach(async () => {
    if (db && !db.isDestroyed) {
      await db.destroy();
    }
    
    // Additional cleanup to ensure files are removed
    try {
      await fs.unlink(testFile);
      await fs.unlink(testFile.replace('.jsonl', '.idx.jdb'));
    } catch (error) {
      // Ignore if files don't exist
    }
  });

  describe('Initialization', () => {
    test('should initialize correctly', async () => {
      await db.init();
      expect(db.isInitialized).toBe(true);
    });

    test('should emit init event', async () => {
      const initPromise = new Promise(resolve => {
        db.once('init', resolve);
      });

      await db.init();
      await initPromise;
    });
  });

  describe('Insertion', () => {
    beforeEach(async () => {
      await db.init();
    });

    test('should insert a record', async () => {
      const record = { id: 1, name: 'John', email: 'john@test.com', age: 30 };
      const result = await db.insert(record);

      expect(result.id).toBe(1);
      expect(result.name).toBe('John');
      expect(result._created).toBeDefined();
      expect(result._updated).toBeDefined();
    });

    test('should insert multiple records', async () => {
      const records = [
        { id: 1, name: 'John', email: 'john@test.com', age: 30 },
        { id: 2, name: 'Jane', email: 'jane@test.com', age: 25 }
      ];

      const results = await db.insertMany(records);
      expect(results).toHaveLength(2);
      expect(results[0].id).toBe(1);
      expect(results[1].id).toBe(2);
    });

    test('should emit insert event', async () => {
      const insertPromise = new Promise(resolve => {
        db.once('insert', (...args) => resolve(args));
      });

      await db.insert({ id: 1, name: 'John' });
      const [record, index] = await insertPromise;
      expect(record.id).toBe(1);
      expect(typeof index).toBe('number');
    });
  });

  describe('Search', () => {
    beforeEach(async () => {
      await db.init();
      await db.insertMany([
        { id: 1, name: 'John', email: 'john@test.com', age: 30 },
        { id: 2, name: 'Jane', email: 'jane@test.com', age: 25 },
        { id: 3, name: 'Bob', email: 'bob@test.com', age: 35 }
      ]);
      await db.save();
    });

    test('should find all records', async () => {
      const results = await db.find();
      expect(results).toHaveLength(3);
    });

    test('should find by ID', async () => {
      const result = await db.findOne({ id: 1 });
      expect(result.name).toBe('John');
    });

    test('should find by email', async () => {
      const result = await db.findOne({ email: 'jane@test.com' });
      expect(result.name).toBe('Jane');
    });

    test('should find with operators', async () => {
      const results = await db.find({ age: { '<': 30 } });
      expect(results).toHaveLength(1);
      expect(results[0].name).toBe('Jane');
    });

    test('should find with in operator', async () => {
      const results = await db.find({ id: { in: [1, 3] } });
      expect(results).toHaveLength(2);
    });

    test('should apply limit', async () => {
      const results = await db.find({}, { limit: 2 });
      expect(results).toHaveLength(2);
    });

    test('should apply sorting', async () => {
      const results = await db.find({}, { sort: { age: 1 } });
      expect(results[0].name).toBe('Jane'); // Youngest
      expect(results[2].name).toBe('Bob');  // Oldest
    });
  });

  describe('Update', () => {
    beforeEach(async () => {
      await db.init();
      await db.insert({ id: 1, name: 'John', email: 'john@test.com', age: 30 });
      await db.save();
    });

    test('should update a record', async () => {
      const results = await db.update({ id: 1 }, { age: 31 });
      expect(results).toHaveLength(1);
      expect(results[0].age).toBe(31);
      expect(results[0]._updated).toBeDefined();
    });

    test('should emit update event', async () => {
      const updatePromise = new Promise(resolve => {
        db.once('update', (...args) => resolve(args));
      });

      await db.update({ id: 1 }, { age: 31 });
      const [record, index] = await updatePromise;
      expect(record.age).toBe(31);
    });
  });

  describe('Deletion', () => {
    beforeEach(async () => {
      await db.init();
      await db.insert({ id: 1, name: 'John', email: 'john@test.com', age: 30 });
      await db.save();
    });

    test('should mark as deleted', async () => {
      const deletedCount = await db.delete({ id: 1 });
      expect(deletedCount).toBe(1);

      const result = await db.findOne({ id: 1 });
      expect(result).toBeNull();
    });

    test('should physically remove', async () => {
      const deletedCount = await db.delete({ id: 1 }, { physical: true });
      expect(deletedCount).toBe(1);

      const result = await db.findOne({ id: 1 });
      expect(result).toBeNull();
    });

    test('should emit delete event', async () => {
      const deletePromise = new Promise(resolve => {
        db.once('delete', (...args) => resolve(args));
      });

      await db.delete({ id: 1 });
      const [record, index] = await deletePromise;
      expect(record.id).toBe(1);
    });
  });

  describe('Counting', () => {
    beforeEach(async () => {
      await db.init();
      await db.insertMany([
        { id: 1, name: 'John', age: 30 },
        { id: 2, name: 'Jane', age: 25 },
        { id: 3, name: 'Bob', age: 35 }
      ]);
      await db.save();
    });

    test('should count all records', async () => {
      const count = await db.count();
      expect(count).toBe(3);
    });

    test('should count with criteria', async () => {
      const count = await db.count({ age: { '>': 25 } });
      expect(count).toBe(2);
    });
  });

  describe('Walk Iterator', () => {
    beforeEach(async () => {
      await db.init();
      await db.insertMany([
        { id: 1, name: 'John' },
        { id: 2, name: 'Jane' },
        { id: 3, name: 'Bob' }
      ]);
      await db.save();
    });

    test('should iterate all records', async () => {
      const records = [];
      for await (const record of db.walk()) {
        records.push(record);
      }
      expect(records).toHaveLength(3);
    });

    test('should apply limit to walk', async () => {
      const records = [];
      for await (const record of db.walk({ limit: 2 })) {
        records.push(record);
      }
      expect(records).toHaveLength(2);
    });
  });

  describe('Integrity', () => {
    beforeEach(async () => {
      await db.init();
      await db.insert({ id: 1, name: 'John' });
      await db.save();
    });

    test('should validate integrity', async () => {
      const integrity = await db.validateIntegrity();
      expect(integrity.isValid).toBe(true);
    });

    test('should get statistics', async () => {
      const stats = await db.getStats();
      expect(stats.summary.totalRecords).toBe(1);
      expect(stats.file.size).toBeGreaterThan(0);
    });
  });

  describe('Properties', () => {
    beforeEach(async () => {
      await db.init();
      await db.insertMany([
        { id: 1, name: 'John' },
        { id: 2, name: 'Jane' }
      ]);
    });

    test('should return correct length', () => {
      expect(db.length).toBe(2);
    });

    test('should return index statistics', () => {
      const stats = db.indexStats;
      expect(stats.recordCount).toBe(2);
      expect(stats.indexCount).toBe(3); // id, email, age
    });
  });

  describe('Persistence', () => {
    test('should persist data between instances', async () => {
      // First instance
      const db1 = new JexiDB(testFile, { indexes: { id: 'number' } });
      await db1.init();
      await db1.insert({ id: 1, name: 'John' });
      await db1.save();
      // Don't destroy - just close the connection
      db1.isInitialized = false;

      // Second instance
      const db2 = new JexiDB(testFile, { indexes: { id: 'number' } });
      await db2.init();
      const result = await db2.findOne({ id: 1 });
      expect(result.name).toBe('John');
      await db2.destroy();
    });
  });

  describe('Database Options', () => {
    test('should create database when create is true (default)', async () => {
      const db = new JexiDB(testFile, { create: true });
      await db.init();
      expect(db.isInitialized).toBe(true);
      expect(db.length).toBe(0);
      await db.destroy();
    });

    test('should throw error when create is false and file does not exist', async () => {
      const db = new JexiDB(testFile, { create: false });
      await expect(db.init()).rejects.toThrow('Database file does not exist');
    });

    test('should clear database when clear is true', async () => {
      // First create a database with some data
      const db1 = new JexiDB(testFile, { create: true });
      await db1.init();
      await db1.insert({ id: 1, name: 'Test' });
      await db1.save();
      await db1.destroy();

      // Now clear it
      const db2 = new JexiDB(testFile, { clear: true });
      await db2.init();
      expect(db2.length).toBe(0);
      await db2.destroy();
    });

    test('should set create to true when clear is true', async () => {
      const db = new JexiDB(testFile, { clear: true });
      expect(db.options.create).toBe(true);
    });
  });

  describe('Column Values', () => {
    test('should get unique values from indexed column', async () => {
      const db = new JexiDB(testFile, { 
        indexes: { category: 'string', status: 'string' },
        create: true 
      });
      await db.init();
      
      await db.insert({ id: 1, category: 'A', status: 'active' });
      await db.insert({ id: 2, category: 'B', status: 'inactive' });
      await db.insert({ id: 3, category: 'A', status: 'active' }); // Duplicate category and status
      await db.save();

      const categories = db.readColumnIndex('category');
      expect(categories).toEqual(new Set(['A', 'B'])); // Only unique values

      const statuses = db.readColumnIndex('status');
      expect(statuses).toEqual(new Set(['active', 'inactive'])); // Only unique values

      await db.destroy();
    });

    test('should throw error for non-indexed columns', async () => {
      const db = new JexiDB(testFile, { create: true });
      await db.init();
      
      await db.insert({ id: 1, name: 'Alice' });
      await db.save();

      expect(() => db.readColumnIndex('name')).toThrow('Column \'name\' is not indexed');

      await db.destroy();
    });
  });
}); 