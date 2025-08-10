# JexiDB API Reference

## Overview

JexiDB is a high-performance, local JSONL database with intelligent optimizations, real compression, and comprehensive error handling.

## Core Classes

### Database

The main database class that provides CRUD operations with intelligent optimizations.

#### Constructor

```javascript
new Database(filePath, options = {})
```

**Parameters:**
- `filePath` (string): Path to the database file
- `options` (object): Configuration options
  - `indexes` (object): Index configuration
  - `markDeleted` (boolean): Mark records as deleted instead of physical removal
  - `autoSave` (boolean): Automatically save after operations
  - `validateOnInit` (boolean): Validate integrity on initialization
  - `backgroundMaintenance` (boolean): Enable background maintenance
  - `create` (boolean): Create database if it doesn't exist (default: true)
  - `clear` (boolean): Clear database on load if not empty (default: false)
  - `batchSize` (number): Batch size for inserts (default: 100)

#### Methods

##### init()
Initializes the database and loads existing data.

```javascript
await db.init()
```

##### insert(data)
Inserts a single record with adaptive optimization.

```javascript
const record = await db.insert({ id: '1', name: 'John' })
```

##### insertMany(dataArray)
Inserts multiple records with bulk optimization.

```javascript
const records = await db.insertMany([
  { id: '1', name: 'John' },
  { id: '2', name: 'Jane' }
])
```

##### find(criteria, options)
Finds records matching criteria with query optimization.

```javascript
const results = await db.find(
  { age: { $gte: 25 } },
  { limit: 10, sort: { name: 1 } }
)
```

##### findOne(criteria, options)
Finds a single record matching criteria.

```javascript
const user = await db.findOne({ id: '1' })
```

##### update(criteria, updates, options)
Updates records matching criteria.

```javascript
const updated = await db.update(
  { id: '1' },
  { name: 'John Updated', age: 30 }
)
```

##### delete(criteria, options)
Deletes records matching criteria.

```javascript
const deleted = await db.delete({ id: '1' })
```

##### count(criteria)
Counts records matching criteria.

```javascript
const count = await db.count({ active: true })
```

##### save()
Saves pending changes to disk.

```javascript
await db.save()
```

##### validateIntegrity(options)
Validates database integrity.

```javascript
const integrity = await db.validateIntegrity({ verbose: true })
```

##### getStats()
Gets comprehensive database statistics.

```javascript
const stats = await db.getStats()
```

##### readColumnIndex(column)
Gets unique values from a specific column (indexed columns only).

```javascript
const categories = db.readColumnIndex('category')
// Returns: Set(['Electronics', 'Books', 'Clothing'])
// Throws error for non-indexed columns
```

##### destroy()
Destroys the database instance and cleans up resources.

```javascript
await db.destroy()
```

#### Properties

- `length`: Number of records in the database
- `indexStats`: Statistics about database indexes

#### Events

- `init`: Emitted when database is initialized
- `insert`: Emitted when a record is inserted
- `update`: Emitted when records are updated
- `delete`: Emitted when records are deleted
- `save`: Emitted when database is saved
- `before-save`: Emitted before database is saved

### Query Operators

The database supports MongoDB-style query operators:

- `$eq`: Equal to
- `$ne`: Not equal to
- `$gt`: Greater than
- `$gte`: Greater than or equal to
- `$lt`: Less than
- `$lte`: Less than or equal to
- `$in`: In array
- `$nin`: Not in array
- `$regex`: Regular expression match

### Nested Field Queries

You can query nested fields using dot notation:

```javascript
const results = await db.find({
  'metadata.preferences.theme': 'dark',
  'metadata.loginCount': { $gt: 10 }
})
```

### Query Options

- `limit`: Limit number of results
- `skip`: Skip number of results
- `sort`: Sort results by field
- `caseInsensitive`: Case-insensitive string matching

## Optimization Features

### Adaptive Mode Switching

JexiDB automatically switches between insertion and query optimization modes based on usage patterns.

### Real Compression

- **LZ4**: Fast compression for warm data
- **Gzip**: High compression for cold data
- **Automatic fallback**: Graceful degradation if compression fails

### Intelligent Caching

- **Query result caching**: Caches frequently accessed query results
- **Index caching**: Caches index data for faster lookups
- **Adaptive eviction**: Automatically manages cache size

### Background Maintenance

- **Automatic compression**: Compresses old data in the background
- **Index optimization**: Optimizes indexes during idle time
- **Integrity checks**: Performs periodic integrity validation

## Error Handling

Comprehensive error handling with automatic recovery:

- **File corruption recovery**: Repairs corrupted files
- **Index rebuilding**: Automatically rebuilds corrupted indexes
- **Memory pressure management**: Handles memory pressure gracefully
- **Compression fallback**: Falls back to no compression if needed

## Performance Characteristics

- **Insert**: ~10,000 ops/sec (bulk), ~1,000 ops/sec (single)
- **Query**: ~5,000 ops/sec (indexed), ~500 ops/sec (unindexed)
- **Update**: ~1,000 ops/sec
- **Delete**: ~1,000 ops/sec
- **Compression**: 20-80% size reduction depending on data type
