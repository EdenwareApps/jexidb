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
  - `create` (boolean): Create database if it doesn't exist (default: true)
  - `clear` (boolean): Clear database on load if not empty (default: false)
  
  **Auto-Save Configuration:**
  - `autoSave` (boolean): Enable intelligent auto-save (default: true)
  - `autoSaveThreshold` (number): Flush buffer when it reaches this many records (default: 50)
  - `autoSaveInterval` (number): Flush buffer every N milliseconds (default: 5000)
  - `forceSaveOnClose` (boolean): Always save when closing database (default: true)
  
  **Performance Configuration:**
  - `batchSize` (number): Batch size for inserts (default: 50)
  - `adaptiveBatchSize` (boolean): Adjust batch size based on usage (default: true)
  - `minBatchSize` (number): Minimum batch size for flush (default: 10)
  - `maxBatchSize` (number): Maximum batch size for performance (default: 200)
  
  **Memory Management:**
  - `memorySafeMode` (boolean): Enable memory-safe operations (default: true)
  - `chunkSize` (number): Chunk size for file operations (default: 8MB)
  - `gcInterval` (number): Force GC every N records (0 = disabled, default: 1000)
  - `maxMemoryUsage` (string|number): Memory limit ('auto' or bytes, default: 'auto')
  - `maxFlushChunkBytes` (number): Maximum chunk size for flush operations (default: 8MB)

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

##### flush()
Flushes the insertion buffer to disk immediately.

```javascript
const flushedCount = await db.flush()
// Returns: number of records flushed
```

##### forceSave()
Forces a save operation regardless of buffer size.

```javascript
await db.forceSave()
// Always saves, even with just 1 record in buffer
```

##### getBufferStatus()
Gets information about the current buffer state.

```javascript
const status = db.getBufferStatus()
// Returns: {
//   pendingCount: 15,
//   bufferSize: 50,
//   lastFlush: 1640995200000,
//   lastAutoSave: 1640995200000,
//   shouldFlush: false,
//   autoSaveEnabled: true,
//   autoSaveTimer: 'active'
// }
```

##### configurePerformance(settings)
Dynamically configures performance settings.

```javascript
db.configurePerformance({
  batchSize: 25,
  autoSaveThreshold: 30,
  autoSaveInterval: 4000
})
```

##### getPerformanceConfig()
Gets current performance configuration.

```javascript
const config = db.getPerformanceConfig()
// Returns: {
//   batchSize: 25,
//   autoSaveThreshold: 30,
//   autoSaveInterval: 4000,
//   adaptiveBatchSize: true,
//   minBatchSize: 10,
//   maxBatchSize: 200
// }
```

#### Memory Management

JexiDB includes advanced memory management features to prevent `RangeError: Array buffer allocation failed` errors in memory-constrained environments.

##### Memory-Safe Configuration

```javascript
const db = new Database('./data.jdb', {
  // Memory management
  memorySafeMode: true,        // Enable memory-safe operations
  chunkSize: 4 * 1024 * 1024, // 4MB chunks (reduced for low memory)
  gcInterval: 500,            // Force GC every 500 records
  maxFlushChunkBytes: 2 * 1024 * 1024, // 2MB max flush chunks
  
  // Auto-save with smaller thresholds
  autoSave: true,
  autoSaveThreshold: 25,      // Flush more frequently
  autoSaveInterval: 3000,     // Flush every 3 seconds
  
  // Performance with memory constraints
  batchSize: 25,              // Smaller batches
  minBatchSize: 5,
  maxBatchSize: 100
});
```

##### Memory-Safe Features

- **Chunked File Processing**: Files are processed in configurable chunks instead of loading entire files in memory
- **Garbage Collection**: Optional forced garbage collection at configurable intervals
- **Buffer Management**: Smaller, more frequent buffer flushes to reduce memory pressure
- **Fallback Strategies**: Graceful degradation when memory is insufficient
- **Memory Monitoring**: Real-time buffer status and memory usage tracking

##### Best Practices for Memory-Constrained Environments

1. **Use Smaller Chunks**: Set `chunkSize` to 1-4MB for low memory systems
2. **Enable Garbage Collection**: Set `gcInterval` to 500-1000 for frequent cleanup
3. **Reduce Batch Sizes**: Use smaller `batchSize` and `autoSaveThreshold`
4. **Monitor Buffer Status**: Use `getBufferStatus()` to track memory usage
5. **Enable Memory-Safe Mode**: Set `memorySafeMode: true` (default)
6. **Use Node.js GC Flag**: Run with `--expose-gc` for manual garbage collection

##### Example: Ultra Memory-Safe Configuration

```javascript
const ultraMemorySafeConfig = {
  memorySafeMode: true,
  chunkSize: 1 * 1024 * 1024,    // 1MB chunks
  gcInterval: 100,               // GC every 100 records
  maxFlushChunkBytes: 512 * 1024, // 512KB flush chunks
  autoSaveThreshold: 10,         // Flush every 10 records
  autoSaveInterval: 2000,        // Flush every 2 seconds
  batchSize: 10,                 // Very small batches
  minBatchSize: 2,
  maxBatchSize: 50
};
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

##### close()
Closes the database instance and saves pending changes.

```javascript
await db.close()
// Saves data and closes instance, but keeps the database file
```

##### destroy()
Closes the database instance and saves pending changes (equivalent to close()).

```javascript
await db.destroy()
// Same as: await db.close()
```

##### deleteDatabase()
**⚠️ WARNING: This permanently deletes the database file!**

Deletes the database file from disk and closes the instance.

```javascript
await db.deleteDatabase()  // Deletes the database file permanently
```

##### removeDatabase()
Removes the database file from disk (alias for deleteDatabase).

```javascript
await db.removeDatabase()  // Same as: await db.deleteDatabase()
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

**Auto-Save Events:**
- `buffer-flush`: Emitted when buffer is flushed (count parameter)
- `buffer-full`: Emitted when buffer reaches threshold
- `auto-save-timer`: Emitted when time-based auto-save triggers
- `save-complete`: Emitted when save operation completes
- `close-save-complete`: Emitted when database closes with final save
- `close`: Emitted when database is closed
- `performance-configured`: Emitted when performance settings are changed

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

## Auto-Save Intelligence

JexiDB features intelligent auto-save capabilities that automatically manage data persistence without manual intervention.

### Auto-Save Modes

**Intelligent Auto-Save (Default):**
- Automatically flushes buffer when it reaches the threshold (default: 50 records)
- Automatically flushes buffer every N milliseconds (default: 5000ms)
- Always saves when closing the database
- Provides real-time feedback through events

**Manual Mode:**
- Disable auto-save with `autoSave: false`
- Manually call `flush()` and `save()` when needed
- Useful for applications requiring precise control over persistence timing

### Auto-Save Configuration

```javascript
const db = new Database('data.jdb', {
  // Enable intelligent auto-save
  autoSave: true,
  autoSaveThreshold: 50,    // Flush every 50 records
  autoSaveInterval: 5000,   // Flush every 5 seconds
  forceSaveOnClose: true,   // Always save on close
  
  // Performance tuning
  batchSize: 50,            // Reduced for faster response
  adaptiveBatchSize: true,  // Adjust based on usage
  minBatchSize: 10,         // Minimum flush size
  maxBatchSize: 200         // Maximum flush size
});
```

### Event-Driven Monitoring

```javascript
// Monitor auto-save operations
db.on('buffer-flush', (count) => {
  console.log(`Flushed ${count} records`);
});

db.on('buffer-full', () => {
  console.log('Buffer reached threshold');
});

db.on('auto-save-timer', () => {
  console.log('Time-based auto-save triggered');
});

db.on('save-complete', () => {
  console.log('Database saved successfully');
});
```

### Buffer Status Monitoring

```javascript
// Check buffer status anytime
const status = db.getBufferStatus();
console.log(`Pending: ${status.pendingCount}/${status.bufferSize}`);
console.log(`Should flush: ${status.shouldFlush}`);
console.log(`Auto-save enabled: ${status.autoSaveEnabled}`);
```

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
