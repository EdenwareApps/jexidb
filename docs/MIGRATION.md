# JexiDB Migration Guide

## Migrating from Other Databases

### From JSON Files

```javascript
const fs = require('fs');
const { Database } = require('jexidb');

async function migrateFromJSON(jsonFilePath, dbPath) {
  // Read JSON file
  const jsonData = JSON.parse(fs.readFileSync(jsonFilePath, 'utf8'));
  
  // Create JexiDB database
  const db = new Database(dbPath);
  await db.init();
  
  // Insert data
  if (Array.isArray(jsonData)) {
    await db.insertMany(jsonData);
  } else {
    await db.insert(jsonData);
  }
  
  await db.save();
  await db.destroy();
  
  console.log('Migration completed successfully');
}

// Usage
migrateFromJSON('./old-data.json', './new-database.jsonl');
```

### From SQLite

```javascript
const sqlite3 = require('sqlite3');
const { Database } = require('jexidb');

async function migrateFromSQLite(sqlitePath, dbPath) {
  const db = new sqlite3.Database(sqlitePath);
  const jexidb = new Database(dbPath);
  await jexidb.init();
  
  return new Promise((resolve, reject) => {
    db.all('SELECT * FROM users', async (err, rows) => {
      if (err) {
        reject(err);
        return;
      }
      
      try {
              await jexidb.insertMany(rows);
      await jexidb.save();
      await jexidb.destroy();
        console.log('Migration completed successfully');
        resolve();
      } catch (error) {
        reject(error);
      }
    });
  });
}
```

### From MongoDB (Local)

```javascript
const { MongoClient } = require('mongodb');
const { Database } = require('jexidb');

async function migrateFromMongoDB(mongoUri, collectionName, dbPath) {
  const client = new MongoClient(mongoUri);
  await client.connect();
  
  const db = client.db();
  const collection = db.collection(collectionName);
  
  const jexidb = new Database(dbPath);
  await jexidb.init();
  
  const cursor = collection.find({});
  const batch = [];
  
  while (await cursor.hasNext()) {
    const doc = await cursor.next();
    batch.push(doc);
    
    if (batch.length >= 1000) {
      await jexidb.insertMany(batch);
      batch.length = 0;
    }
  }
  
  if (batch.length > 0) {
    await jexidb.insertMany(batch);
  }
  
  await jexidb.save();
  await jexidb.destroy();
  await client.close();
  
  console.log('Migration completed successfully');
}
```

## Data Transformation

### Converting Data Types

```javascript
function transformData(oldData) {
  return oldData.map(record => ({
    id: record._id || record.id,
    name: record.name || record.fullName,
    email: record.email?.toLowerCase(),
    age: parseInt(record.age) || 0,
    metadata: {
      created: record.createdAt || new Date().toISOString(),
      updated: record.updatedAt || new Date().toISOString()
    }
  }));
}

// Usage
const transformedData = transformData(oldData);
await db.insertMany(transformedData);
```

### Handling Different Schemas

```javascript
function normalizeSchema(records) {
  return records.map(record => {
    const normalized = {
      id: record.id || record._id || record.userId,
      name: record.name || record.fullName || record.userName,
      email: record.email || record.emailAddress,
      age: record.age || record.userAge || 0
    };
    
    // Add optional fields
    if (record.metadata) {
      normalized.metadata = record.metadata;
    }
    
    if (record.tags) {
      normalized.tags = Array.isArray(record.tags) ? record.tags : [record.tags];
    }
    
    return normalized;
  });
}
```

## Index Migration

### Creating Indexes for Existing Data

```javascript
async function createIndexesForExistingData(dbPath) {
  const db = new Database(dbPath, {
    indexes: {
      id: true,
      email: true,
      'metadata.created': true
    }
  });
  
  await db.init();
  
  // Rebuild indexes for existing data
  await db.rebuildIndexes({ verbose: true });
  
  await db.save();
  await db.destroy();
  
  console.log('Indexes created successfully');
}
```

## Validation and Verification

### Verify Migration Success

```javascript
async function verifyMigration(originalData, dbPath) {
  const db = new Database(dbPath);
  await db.init();
  
  // Check record count
  const count = await db.count();
  console.log(`Original: ${originalData.length}, Migrated: ${count}`);
  
  // Check data integrity
  const integrity = await db.validateIntegrity({ verbose: true });
  console.log('Integrity check:', integrity.isValid ? 'PASSED' : 'FAILED');
  
  // Sample verification
  const sample = await db.find({}, { limit: 5 });
  console.log('Sample records:', sample);
  
  await db.destroy();
}
```

## Performance Considerations

### Large Dataset Migration

```javascript
async function migrateLargeDataset(data, dbPath, batchSize = 1000) {
  const db = new Database(dbPath, {
    autoSave: false, // Disable auto-save for better performance
    backgroundMaintenance: false
  });
  
  await db.init();
  
  for (let i = 0; i < data.length; i += batchSize) {
    const batch = data.slice(i, i + batchSize);
    await db.insertMany(batch);
    
    // Progress indicator
    if (i % 10000 === 0) {
      console.log(`Migrated ${i} records...`);
    }
  }
  
  await db.save();
  await db.destroy();
  
  console.log('Large dataset migration completed');
}
```

## Rollback Strategy

### Backup Before Migration

```javascript
const fs = require('fs');

async function backupBeforeMigration(originalPath) {
  const backupPath = `${originalPath}.backup.${Date.now()}`;
  await fs.copyFile(originalPath, backupPath);
  console.log(`Backup created: ${backupPath}`);
  return backupPath;
}

async function rollbackMigration(backupPath, currentPath) {
  await fs.copyFile(backupPath, currentPath);
  console.log('Rollback completed');
}
```

## Migration Checklist

- [ ] **Backup original data**
- [ ] **Test migration with sample data**
- [ ] **Verify data integrity**
- [ ] **Check performance characteristics**
- [ ] **Update application code**
- [ ] **Test application functionality**
- [ ] **Monitor for issues**
- [ ] **Clean up old data** (after verification)

## Common Migration Issues

### 1. Data Type Mismatches

**Issue:** Different data types between source and target
**Solution:** Implement data transformation functions

### 2. Missing Required Fields

**Issue:** Target schema requires fields not in source
**Solution:** Provide default values or skip records

### 3. Duplicate Records

**Issue:** Multiple records with same ID
**Solution:** Use upsert operations or deduplication

### 4. Performance Issues

**Issue:** Migration taking too long
**Solution:** Use batch operations and disable auto-save

### 5. Memory Issues

**Issue:** Running out of memory during migration
**Solution:** Process data in smaller batches
