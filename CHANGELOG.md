# Changelog

All notable changes to JexiDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.3] - 2024-12-19

### 🚀 Added
- **Intelligent Auto-Save System**: Automatic data persistence with configurable thresholds and intervals
  - `autoSaveThreshold`: Flush buffer when it reaches N records (default: 50)
  - `autoSaveInterval`: Flush buffer every N milliseconds (default: 5000ms)
  - `forceSaveOnClose`: Always save when closing database (default: true)
- **Memory-Safe Operations**: Advanced memory management to prevent `RangeError: Array buffer allocation failed`
  - `memorySafeMode`: Enable memory-safe operations (default: true)
  - `chunkSize`: Chunk size for file operations (default: 8MB)
  - `gcInterval`: Force garbage collection every N records (default: 1000)
  - Chunked file processing instead of loading entire files in memory
  - Fallback strategies for memory-constrained environments
- **New Public Methods**:
  - `flush()`: Explicitly flush insertion buffer to disk
  - `forceSave()`: Force save operation regardless of buffer size
  - `getBufferStatus()`: Get current buffer state information
  - `configurePerformance(settings)`: Dynamic performance configuration
  - `getPerformanceConfig()`: Get current performance settings
  - `deleteDatabase()`: Permanently delete database file
  - `removeDatabase()`: Alias for deleteDatabase()
- **Event-Driven Monitoring**: New events for auto-save operations
  - `buffer-flush`: Emitted when buffer is flushed
  - `buffer-full`: Emitted when buffer reaches threshold
  - `auto-save-timer`: Emitted when time-based auto-save triggers
  - `save-complete`: Emitted when save operation completes
  - `close-save-complete`: Emitted when database closes with final save
  - `performance-configured`: Emitted when performance settings change
  - `delete-database`: Emitted when database file is deleted
- **Performance Configuration Options**:
  - `adaptiveBatchSize`: Adjust batch size based on usage (default: true)
  - `minBatchSize`: Minimum batch size for flush (default: 10)
  - `maxBatchSize`: Maximum batch size for performance (default: 200)
  - `maxMemoryUsage`: Memory usage limits
  - `maxFlushChunkBytes`: Maximum bytes per flush chunk (default: 8MB)

### 🔧 Changed
- **API Simplification**: Removed confirmation requirements from `deleteDatabase()` and `removeDatabase()`
  - Methods now work directly without `force` or `confirm` options
  - Names are self-explanatory and follow industry standards
- **Constructor Defaults**: Updated default options for better performance
  - `batchSize`: Reduced from 100 to 50 for faster response
  - `autoSave`: Enabled by default (true)
  - `autoSaveThreshold`: 50 records
  - `autoSaveInterval`: 5000ms (5 seconds)
  - `forceSaveOnClose`: Enabled by default (true)
- **Method Behavior**:
  - `destroy()`: Now equivalent to `close()` (closes instance, keeps file)
  - `deleteDatabase()`: Permanently deletes database file
  - `removeDatabase()`: Alias for `deleteDatabase()`

### 🐛 Fixed
- **Query Operators**: Fixed issues with `$gt`, `$gte`, `$lt`, `$lte`, `$ne`, `$nin` operators
- **Data Duplication**: Resolved duplicate results in `find()` operations
- **Type Preservation**: Fixed numeric values being stored as strings in persistent indexes
- **Persistence Issues**: Corrected data persistence between database instances
- **Performance Test Logic**: Fixed test expectations for large dataset operations

### 📚 Documentation
- **Updated README.md**: Added comprehensive auto-save documentation and examples
- **Updated API.md**: Added new methods, events, and configuration options
- **New Examples**: Created `auto-save-example.js` and `close-vs-delete-example.js`
- **Enhanced Tests**: Added comprehensive test suite for auto-save functionality

### 🧪 Testing
- **New Test Suite**: Added `tests/auto-save.test.js` with 10 comprehensive tests
- **All Tests Passing**: 86 tests passing (100% success rate)
- **Improved Coverage**: Better test coverage for new auto-save features

## [2.0.2] - 2024-12-18

### 🚀 Added
- **Persistent Indexes**: Indexes are now saved to disk and loaded on startup
- **Point Reading**: Efficient memory usage - only reads necessary data
- **Rich Query API**: Support for complex queries with operators, sorting, and pagination
- **Event-Driven Architecture**: Real-time notifications for all database operations
- **Automatic Integrity Validation**: Built-in data integrity checking and repair
- **Legacy Compatibility**: Automatic migration from JexiDB 1.x databases
- **Pure JavaScript**: No native dependencies, works everywhere

### 🔧 Changed
- **Performance**: 10-100x faster than JexiDB 1.x
- **Memory Usage**: 25% less memory consumption
- **File Format**: Improved JSONL architecture with separate index files
- **API**: Enhanced query methods with MongoDB-style operators

### 🐛 Fixed
- **Data Integrity**: Safe truncation and consistent offsets
- **Test Isolation**: Proper test isolation and cleanup
- **V8 Dependency**: Removed dependency on V8 engine

## [2.0.1] - 2024-12-17

### 🚀 Added
- **Initial Release**: First stable version of JexiDB 2.0
- **JSONL Architecture**: Pure JavaScript JSONL database implementation
- **Basic CRUD Operations**: Insert, find, update, delete operations
- **Index Support**: Basic indexing for fast queries
- **File Management**: Database file creation and management

### 🔧 Changed
- **Complete Rewrite**: New architecture from ground up
- **Performance Focus**: Optimized for speed and efficiency
- **Modern JavaScript**: ES6+ features and async/await support

### 🐛 Fixed
- **Stability**: Improved error handling and edge cases
- **Compatibility**: Better Node.js version support
