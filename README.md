# JexiDB - Pure JavaScript JSONL Database

**JexiDB** is a lightweight, high-performance JSONL (JSON Lines) database for Node.js built in pure JavaScript that provides fast data storage and retrieval with persistent indexing.

## 🚀 Features

- **JSONL Architecture**: Each database is a single JSONL file for simplicity and portability
- **Persistent Indexes**: Fast searches with disk-persisted indexes that don't need rebuilding
- **Point Reading**: Efficient memory usage - only reads necessary data
- **Rich Query API**: Support for complex queries with operators, sorting, and pagination
- **Automatic Integrity Validation**: Built-in data integrity checking and repair
- **Event System**: Real-time notifications for database operations
- **Legacy Compatibility**: Automatic migration from JexiDB 1.x databases
- **Pure JavaScript**: No native dependencies, works everywhere, easy to deploy

## 📦 Installation

```bash
npm install jexidb
```

## 🚀 Quick Start

```javascript
// import { Database } from 'jexidb'
const { Database } = require('jexidb');

// Create database with indexes (supports both .jdb and .jsonl)
const db = new Database('./users.jdb', {
  indexes: {
    id: 'number',
    email: 'string',
    age: 'number'
  },
  autoSave: true,
  validateOnInit: true
});

// Initialize
await db.init();

// Event listeners
db.on('insert', (record, index) => console.log(`Record inserted at index ${index}`));
db.on('update', (record, index) => console.log(`Record updated at index ${index}`));
db.on('save', () => console.log('Changes saved'));

// Insert data
const user = await db.insert({
  id: 1,
  name: 'John Doe',
  email: 'john@example.com',
  age: 30
});

// Search data (both methods work)
const john = await db.findOne({ id: 1 });
const youngUsers = await db.find({ age: { '<': 30 } });

// JexiDB 1.x compatible query
const results = await db.query({ name: 'john doe' }, { caseInsensitive: true });

// Update data
await db.update({ id: 1 }, { age: 31 });

// Remove data
await db.delete({ id: 1 });

// Save changes
await db.save();

// Destroy database
await db.destroy();
```

## 📚 API Reference

### Constructor

```javascript
const db = new Database(filePath, options);
```

**Parameters:**
- `filePath` (string): Path to the main file (.jdb)
- `options` (object): Configuration options

**Options:**
```javascript
{
  indexes: {},           // Indexes for fields
  markDeleted: true,     // Mark as deleted instead of physically removing
  autoSave: true,        // Automatically save after operations
  validateOnInit: false  // Validate integrity on initialization
}
```

### Main Methods

#### `init()`
Initializes the database.

#### `insert(data)`
Inserts a record.

#### `insertMany(dataArray)`
Inserts multiple records.

#### `find(criteria, options)` / `query(criteria, options)`
Searches records with optional criteria. Both methods work identically.

**Supported operators:**
```javascript
// Comparison
{ age: { '>': 25 } }
{ age: { '>=': 25 } }
{ age: { '<': 30 } }
{ age: { '<=': 30 } }
{ age: { '!=': 25 } }

// Arrays
{ tags: { in: ['developer', 'admin'] } }
{ tags: { nin: ['designer'] } }

// Strings
{ name: { regex: 'john' } }
{ name: { contains: 'john' } }
```

**Options:**
```javascript
{
  limit: 10,           // Limit results
  skip: 5,            // Skip records
  sort: { age: 1 },   // Sorting (1 = ascending, -1 = descending)
  caseInsensitive: false,  // Case insensitive matching (query() only)
  matchAny: false     // OR instead of AND
}
```

**JexiDB 1.x Compatibility:**
```javascript
// Both work identically
const results1 = await db.find({ name: 'John' });
const results2 = await db.query({ name: 'John' });

// Case insensitive query (JexiDB 1.x style)
const results = await db.query({ name: 'john' }, { caseInsensitive: true });
```

#### `findOne(criteria, options)`
Searches for one record.

#### `update(criteria, updateData, options)`
Updates records.

#### `delete(criteria, options)`
Removes records.

**Delete options:**
```javascript
{
  physical: false,  // Physically remove instead of marking as deleted
  limit: 1         // Limit number of records to delete
}
```

#### `count(criteria, options)`
Counts records.

#### `save()`
Saves pending changes.

#### `destroy()`
Destroys the database.

#### `validateIntegrity(options)`
Validates database integrity.

#### `rebuildIndexes(options)`
Rebuilds indexes.

#### `getStats()`
Gets detailed statistics.

### `walk()` Iterator

For traversing large volumes of data:

```javascript
// Traverse all records
for await (const record of db.walk()) {
  console.log(record.name);
}

// With options
for await (const record of db.walk({ 
  limit: 100, 
  skip: 50, 
  includeDeleted: false 
})) {
  console.log(record.name);
}
```

### Properties

#### `length`
Total number of records.

#### `indexStats`
Index statistics.

### Events

```javascript
db.on('init', () => console.log('Database initialized'));
db.on('insert', (record, index) => console.log('Record inserted'));
db.on('update', (record, index) => console.log('Record updated'));
db.on('delete', (record, index) => console.log('Record deleted'));
db.on('before-save', () => console.log('Before save'));
db.on('save', () => console.log('Save completed'));
db.on('destroy', () => console.log('Database destroyed'));
```

## 📁 File Structure

For each database, 2 files are created:

```
users.jdb            # Data (JSON Lines format)
users.idx.jdb        # Compressed persistent indexes
```

### 🔄 Legacy Compatibility

JexiDB automatically detects and migrates JexiDB 1.x files:

**Legacy Format (JexiDB 1.x):**
```
users.jsonl          # Data + indexes + offsets in single file
```

**New Format (JexiDB):**
```
users.jdb            # Data + offsets
users.idx.jdb        # Compressed indexes
```



### 🚀 Persistent Indexes

JexiDB implements **persistent indexes** that are saved to disk:

**Benefits:**
- **Fast startup**: No need to read all data to rebuild indexes
- **Scalable**: Works well with large databases (100k+ records)
- **Consistent**: Indexes synchronized with data
- **Portable**: Only 2 files to manage
- **Compressed**: Indexes compressed using gzip

**🔧 How it works:**
1. **First open**: Indexes are built by reading data
2. **Save**: Indexes are persisted and compressed to `users.idx.jdb`
3. **Reopen**: Indexes are loaded instantly from disk
4. **Fallback**: If index file is corrupted, rebuilds automatically

### JSONL Format

Each line is a valid JSON record:

```json
{"id":1,"name":"John","email":"john@example.com","_created":"2024-12-19T10:00:00.000Z","_updated":"2024-12-19T10:00:00.000Z"}
{"id":2,"name":"Jane","email":"jane@example.com","_created":"2024-12-19T10:01:00.000Z","_updated":"2024-12-19T10:01:00.000Z"}
```

## 🔍 Advanced Examples

### Complex Search

```javascript
// Young users from New York who are developers
const users = await db.find({
  age: { '<': 30 },
  'profile.city': 'New York',
  tags: { in: ['developer'] }
}, {
  sort: { age: 1 },
  limit: 10
});
```

### Batch Update

```javascript
// Update age of all users from a city
const updated = await db.update(
  { 'profile.city': 'New York' },
  { 'profile.country': 'USA' }
);
```

### Integrity Validation

```javascript
// Validate integrity with details
const integrity = await db.validateIntegrity({
  checkData: true,
  checkIndexes: true,
  checkOffsets: true,
  verbose: true
});

if (!integrity.isValid) {
  console.log('Errors:', integrity.errors);
  console.log('Warnings:', integrity.warnings);
}
```

### Detailed Statistics

```javascript
const stats = await db.getStats();
console.log('File size:', stats.file.size);
console.log('Total records:', stats.summary.totalRecords);
console.log('Indexes:', stats.indexes.indexCount);
```

## 🧪 Tests

```bash
npm test
```

**Automatic Cleanup**: The test script automatically removes all test files after execution to keep the project directory clean.

**Manual Cleanup**: If you need to clean up test files manually:
```bash
npm run test:clean
```

**Available Test Scripts**:
- `npm test` - Run all tests with automatic cleanup
- `npm run test:watch` - Run tests in watch mode
- `npm run test:clean` - Clean up test files manually
- `npm run test:optimized` - Run optimized performance tests
- `npm run test:parallel` - Run tests in parallel
- `npm run test:fast` - Run fast tests without isolation

## 📈 Performance

### JSONL Features

- **Point reading**: Only reads necessary lines
- **In-memory indexes**: Fast search by indexed fields
- **No complete parsing**: Doesn't load entire file into memory
- **Large volume support**: Scales with millions of records

### Comparison: JexiDB vs 1.x

| Feature | JexiDB | JexiDB 1.x |
|---------|---------------|------------|
| Safe truncation | ✅ | ❌ |
| Consistent offsets | ✅ | ❌ |
| Integrity validation | ✅ | ❌ |
| Isolated tests | ✅ | ❌ |
| No V8 dependency | ✅ | ❌ |
| Similar API | ✅ | ✅ |

## 🔧 Utilities

```javascript
const { utils } = require('jexidb');

// Validate JSONL file
const validation = await utils.validateJSONLFile('./data.jsonl');

// Convert JSON to JSONL (basic)
await utils.convertJSONToJSONL('./data.json', './data.jsonl');

// Convert JSONL to JSON
await utils.convertJSONLToJSON('./data.jsonl', './data.json');

// Create JexiDB database with automatic indexes
const result = await utils.createDatabaseFromJSON('./users.json', './users.jsonl', {
  autoDetectIndexes: true,
  autoIndexFields: ['id', 'email', 'name', 'username']
});

// Analyze JSON and suggest optimal indexes
const analysis = await utils.analyzeJSONForIndexes('./users.json', 100);
console.log('Recommended indexes:', analysis.suggestions.recommended);

// Migrate from JexiDB 1.x to JexiDB
await utils.migrateFromJexiDB('./jexidb-v1-database', './users.jsonl');
```

### 🔍 **How Utilities Work**

#### **1. Basic Conversion (No Indexes)**
```javascript
// Only converts format - DOES NOT add indexes
await utils.convertJSONToJSONL('./data.json', './data.jsonl');
```
- ✅ Converts JSON to JSONL
- ❌ **DOES NOT create indexes**
- ❌ **DOES NOT create JexiDB database**
- ✅ Pure JSONL file

#### **2. Database Creation with Automatic Indexes**
```javascript
// Create complete JexiDB database with indexes
const result = await utils.createDatabaseFromJSON('./users.json', './users.jsonl', {
  autoDetectIndexes: true,
  autoIndexFields: ['id', 'email', 'name']
});

console.log(result);
// {
//   success: true,
//   recordCount: 1000,
//   indexes: ['id', 'email', 'name'],
//   dbPath: './users.jsonl'
// }
```
- ✅ Converts JSON to JSONL
- ✅ **Creates indexes automatically**
- ✅ **Creates complete JexiDB database**
- ✅ File ready for use

#### **3. Intelligent Index Analysis**
```javascript
// Analyze data and suggest optimal indexes
const analysis = await utils.analyzeJSONForIndexes('./users.json');

console.log('Recommended:', analysis.suggestions.recommended);
// [
//   { field: 'id', type: 'number', coverage: 100, uniqueness: 100 },
//   { field: 'email', type: 'string', coverage: 95, uniqueness: 98 }
// ]
```



## 🔄 Migration from JexiDB 1.x

### Seamless Migration

JexiDB is **fully backward compatible** with JexiDB 1.x! You can use the same API:

```javascript
// JexiDB 1.x code works unchanged in JexiDB
import { Database } from 'jexidb';

const db = new Database('./database.jdb', { 
  indexes: { id: 'number', name: 'string' }
});
await db.init();

// All JexiDB 1.x methods work:
await db.insert({ id: 1, name: 'John Doe' });
const results = await db.query({ name: 'John Doe' }, { caseInsensitive: true });
await db.update({ id: 1 }, { name: 'John Smith' });
await db.delete({ id: 1 });
await db.save();
```

### File Format Support

JexiDB supports both file formats:
- **`.jdb`** (preferred) - JexiDB's branded extension
- **`.jsonl`** (standard) - JSON Lines format

```javascript
// Both work identically:
const db1 = new Database('./users.jdb', { indexes: { id: 'number' } });
const db2 = new Database('./users.jsonl', { indexes: { id: 'number' } });
```

### Key Improvements

| Feature | JexiDB 1.x | JexiDB |
|---------|------------|--------------|
| **API Compatibility** | Original | ✅ **100% Backward Compatible** |
| **Query Methods** | `db.query()` | ✅ `db.query()` + `db.find()` |
| **File Format** | `.jdb` (proprietary) | ✅ `.jdb` + `.jsonl` support |
| **Performance** | Basic | ✅ **10-100x faster** |
| **Memory Usage** | Higher | ✅ **25% less memory** |
| **Data Integrity** | Basic | ✅ **Advanced validation** |

## 📝 Changelog

See [CHANGELOG.md](CHANGELOG.md) for complete change history.

## 🤝 Contributing

1. Fork the project
2. Create a branch for your feature
3. Commit your changes
4. Push to the branch
5. Open a Pull Request

## 📄 License

MIT License - see [LICENSE](LICENSE) for details.

---

## 🎯 About JexiDB

JexiDB maintains the original JexiDB philosophy while fixing bugs and implementing a more robust architecture.

### 🚀 Performance

**JexiDB** performance compared to version 1.x:

- **Find operations**: 103x faster
- **Update operations**: 26x faster  
- **Insert operations**: 6-11x faster
- **Memory usage**: 25% less memory

<p align="center">
  <img width="420" src="https://edenware.app/jexidb/images/jexi-mascot.webp" alt="JexiDB mascot" title="JexiDB mascot" />
</p>
