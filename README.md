# JexiDB

<p align="center">
  <img width="270" src="https://edenware.app/jexidb/images/jexidb-logo-icon.jpg" alt="JexiDB logo" title="JexiDB logo" />
</p>

## Overview

JexiDB is a high-performance, in-memory database with persistence capabilities. It provides advanced indexing, querying, and term mapping features for efficient data operations. Ideal for local Node.js projects as well as apps built with Electron or NW.js. Written in pure JavaScript, it requires no compilation and is compatible with both CommonJS and ESM modules.

**Key Features:**
- In-memory storage with optional persistence
- Advanced indexing and querying capabilities  
- Term mapping for efficient storage and querying
- Bulk update operations with `iterate()` method
- Manual save functionality (auto-save removed for better control)
- Transaction support with operation queuing
- Recovery mechanisms for data integrity

## Installation

To install JexiDB, you can use npm:

```bash
npm install EdenwareApps/jexidb
```

## Usage

### Creating a Database Instance

To create a new instance of the database, you need to provide a file path where the database will be stored and an optional configuration object for indexes.

```javascript
// const { Database } = require('jexidb'); // commonjs

import { Database } from 'jexidb'; // ESM

const db = new Database('path/to/database.jdb', {
  create: true,                    // Create file if doesn't exist (default: true)
  clear: false,                    // Clear existing files before loading (default: false)
  
  // REQUIRED - Define your schema structure
  fields: {
    id: 'number',
    name: 'string',
    email: 'string',
    tags: 'array:string'
  },
  
  // OPTIONAL - Performance optimization (only fields you query frequently)
  indexes: {
    name: 'string',               // ✅ Search by name
    tags: 'array:string'          // ✅ Search by tags
  }
  
  // termMapping is now auto-enabled for array:string fields
});
```
#### Constructor Options

- **create** (boolean, default: `true`): Controls whether the database file should be created if it doesn't exist.
  - `true`: Creates the file if it doesn't exist (default behavior)
  - `false`: Throws an error if the file doesn't exist

- **clear** (boolean, default: `false`): Controls whether to clear existing database files before loading.
  - `true`: Deletes both the main database file (.jdb) and index file (.idx.jdb) if they exist, then starts with a clean database
  - `false`: Loads existing data from files (default behavior)

You can [learn a bit more about these options at this link](https://github.com/EdenwareApps/jexidb/tree/main/test#readme).


### Initializing the Database

Before using the database, you need to initialize it. This will load the existing data and indexes from the file.

```javascript
await db.init();
```
Only the values ​​specified as indexes are kept in memory for faster queries. JexiDB will never load the entire file into memory.


### Inserting Data

You can insert data into the database by using the `insert` method. The data should be an object that contains the defined indexes. All object values will be saved into database.

```javascript
await db.insert({ id: 1, name: 'John Doe' });
await db.insert({ id: 2, name: 'Jane Doe', anyArbitraryField: '1' });
```

### Querying Data

The `query` method allows you to retrieve data based on specific criteria. You can specify criteria for multiple fields.

```javascript
const results = await db.query({ name: 'John Doe' }, { caseInsensitive: true });
console.log(results); // [{ id: 1, name: 'John Doe' }]
```

Note: For now the query should be limited to using the fields specified as 'indexes' when instantiating the class.

#### Querying with Conditions

You can use conditions to perform more complex queries:

```javascript
const results = await db.query({ id: { '>': 1 } });
console.log(results); // [{ id: 2, name: 'Jane Doe' }]
```

### Updating Data

To update existing records, use the `update` method with the criteria to find the records and the new data.

```javascript
await db.update({ id: 1 }, { name: 'John Smith' });
```

### Deleting Data

You can delete records that match certain criteria using the `delete` method.

```javascript
const deletedCount = await db.delete({ name: 'Jane Doe' });
console.log(`Deleted ${deletedCount} record(s).`);
```

### Iterating Through Records

You can iterate through records in the database using the `walk` method, which returns an async generator.

```javascript
for await (const record of db.walk()) {
  console.log(record);
}
```

### Saving Changes

After making any changes to the database, you need to save them using the `save` method. This will persist the changes to disk.

```javascript
await db.save();
```

## Testing

JexiDB includes a comprehensive test suite built with Jest. To run the tests:

```bash
# Run all tests
npm test

# Run tests in watch mode (for development)
npm test:watch

# Run tests with coverage report
npm test:coverage

# Run legacy test suite (original Mortal Kombat themed tests)
npm run test:legacy
```

The test suite includes:
- **Database Tests**: Complete CRUD operations, querying, indexing, and persistence
- **IndexManager Tests**: Index creation, querying with various operators, and data management
- **Serializer Tests**: JSON serialization/deserialization with various data types

All database operations ensure that the `_` property (position index) is always included in returned results.

## Bulk Operations with `iterate()`

The `iterate()` method provides high-performance bulk update capabilities with streaming support:

```javascript
// Basic bulk update
for await (const entry of db.iterate({ category: 'fruits' })) {
  entry.price = entry.price * 1.1 // 10% price increase
  entry.lastUpdated = new Date().toISOString()
}

// Advanced options with progress tracking
for await (const entry of db.iterate(
  { status: 'active' },
  {
    chunkSize: 1000,
    progressCallback: (progress) => {
      console.log(`Processed: ${progress.processed}, Modified: ${progress.modified}`)
    }
  }
)) {
  entry.processed = true
}
```

**Key Benefits:**
- **Streaming Performance**: Process large datasets without loading everything into memory
- **Bulk Updates**: Modify multiple records in a single operation
- **Automatic Change Detection**: Automatically detects which records were modified
- **Progress Tracking**: Optional progress callbacks for long-running operations

## 🔄 Migration Guide (1.x.x → 2.1.0)

### ⚠️ Important: Database Files Are NOT Compatible

**Existing `.jdb` files from version 1.x.x will NOT work with version 2.1.0.**

### Step 1: Export Data from 1.x.x

```javascript
// In your 1.x.x application
const oldDb = new Database('old-database.jdb', {
  indexes: { name: 'string', tags: 'array:string' }
})

await oldDb.init()
const allData = await oldDb.find({}) // Export all data
await oldDb.destroy()
```

### Step 2: Update Your Code

```javascript
// ❌ OLD (1.x.x)
const db = new Database('db.jdb', {
  indexes: { name: 'string', tags: 'array:string' }
})

// ✅ NEW (2.1.0)
const db = new Database('db.jdb', {
  fields: {                    // REQUIRED - Define schema
    id: 'number',
    name: 'string',
    tags: 'array:string'
  },
  indexes: {                   // OPTIONAL - Performance optimization
    name: 'string',            // ✅ Search by name
    tags: 'array:string'       // ✅ Search by tags
  }
})
```

### Step 3: Import Data to 2.1.0

```javascript
// In your 2.1.0 application
const newDb = new Database('new-database.jdb', {
  fields: { /* your schema */ },
  indexes: { /* your indexes */ }
})

await newDb.init()

// Import all data
for (const record of allData) {
  await newDb.insert(record)
}

await newDb.save()
```

### Key Changes Summary

| Feature | 1.x.x | 2.1.0 |
|---------|-------|-------|
| `fields` | Optional | **MANDATORY** |
| `termMapping` | `false` (default) | `true` (default) |
| `termMappingFields` | Manual config | Auto-detected |
| Database files | Compatible | **NOT compatible** |
| Performance | Basic | **77% size reduction** |

## 📚 Documentation

For comprehensive documentation and examples:

- **[📖 Full Documentation](docs/README.md)** - Complete documentation index
- **[🔧 API Reference](docs/API.md)** - Detailed API documentation
- **[💡 Examples](docs/EXAMPLES.md)** - Practical examples and use cases
- **[🚀 Getting Started](docs/README.md#quick-start)** - Quick start guide

### Key Features Documentation

- **[Bulk Operations](docs/API.md#bulk-operations)** - High-performance `iterate()` method
- **[Term Mapping](docs/API.md#term-mapping)** - Optimize storage for repetitive data
- **[Query Operators](docs/API.md#query-operators)** - Advanced querying capabilities
- **[Performance Tips](docs/API.md#best-practices)** - Optimization strategies

## Conclusion

JexiDB provides a simple yet powerful way to store and manage data in JavaScript applications. With its indexing and querying features, you can build efficient data-driven applications.

<p align="center">
  <img width="380" src="https://edenware.app/jexidb/images/jexidb-mascot3.jpg" alt="JexiDB mascot" title="JexiDB mascot" />
</p>

# Contributing

Please, feel free to contribute to the project by opening a discussion under Issues section or sending your PR.

If you find this library useful, please consider making a donation of any amount via [PayPal by clicking here](https://www.paypal.com/donate/?item_name=megacubo.tv&cmd=_donations&business=efox.web%40gmail.com) to help the developer continue to dedicate himself to the project. ❤
