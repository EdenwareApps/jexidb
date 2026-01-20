# JexiDB API Documentation

## The Only Pure JS Database with Smart Disk Persistence & Intelligent Memory Management

JexiDB offers a complete API for CRUD operations with advanced queries, optimized for desktop applications.

## Table of Contents

1. [Database Constructor](#database-constructor)
2. [Core Methods](#core-methods)
3. [Query Methods](#query-methods)
4. [Advanced Features](#advanced-features)
5. [Term Mapping](#term-mapping)
6. [Bulk Operations](#bulk-operations)
7. [Configuration Options](#configuration-options)

## Database Constructor

```javascript
import { Database } from 'jexidb'

const db = new Database('data.jdb', options)
```

### Constructor Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `create` | boolean | `true` | Create the file if it doesn't exist |
| `clear` | boolean | `false` | Clear existing files before loading |
| `fields` | object | **Required** | **MANDATORY** - Define the data structure schema |
| `indexes` | object | `{}` | **Optional** indexed fields for faster queries (not required) |
| `termMapping` | boolean | `true` | Enable term mapping for optimal performance (auto-detected) |
| `termMappingFields` | string[] | `[]` | Fields to apply term mapping to (when specified, overrides auto-detection from indexes) |
| `termMappingCleanup` | boolean | `true` | Automatically clean up orphaned terms |

### Fields vs Indexes - Important Distinction

**Fields** (Schema - **MANDATORY**):
- ✅ **Required** - Define the structure of your data
- ✅ **Schema enforcement** - Controls which fields are allowed
- ✅ **All fields** are queryable by default
- ✅ **No performance impact** on memory usage
- ✅ **Data validation** - Ensures data consistency

**Indexes** (Performance Optimization - **Optional**):
- ⚠️ **Optional** - Only for fields you query frequently
- ⚠️ **Memory overhead** - Each index uses additional memory
- ⚠️ **Use sparingly** - Only index fields you actually query
- ⚠️ **Query performance** - Only affects query speed, not functionality

### When to Use Indexes

```javascript
// ❌ DON'T: Index everything (wastes memory)
const db = new Database('users.jdb', {
  fields: {                 // REQUIRED - Define schema
    id: 'number',
    name: 'string',
    email: 'string',
    phone: 'string',
    address: 'string',
    city: 'string',
    country: 'string',
    status: 'string',
    createdAt: 'number',
    updatedAt: 'number'
  },
  indexes: {                // OPTIONAL - Only for performance
    id: 'number',           // Maybe needed
    name: 'string',         // Maybe needed
    email: 'string',        // Maybe needed
    phone: 'string',        // Maybe needed
    address: 'string',      // Maybe needed
    city: 'string',         // Maybe needed
    country: 'string',      // Maybe needed
    status: 'string',       // Maybe needed
    createdAt: 'number',    // Maybe needed
    updatedAt: 'number'    // Maybe needed
  }
})

// ✅ DO: Define schema + index only what you query frequently
const db = new Database('users.jdb', {
  fields: {                 // REQUIRED - Define schema
    id: 'number',
    name: 'string',
    email: 'string',
    phone: 'string',
    address: 'string',
    city: 'string',
    country: 'string',
    status: 'string',
    createdAt: 'number',
    updatedAt: 'number'
  },
  indexes: {                // OPTIONAL - Only for performance
    id: 'number',           // Primary key - always index
    email: 'string',        // Login queries - index this
    status: 'string'        // Filter queries - index this
  }
  // Other fields (name, phone, address, etc.) are still queryable
  // but will use slower sequential search
})
```

### Example

```javascript
// Example: E-commerce product database
const db = new Database('products.jdb', {
  create: true,
  clear: false,
  fields: {                   // REQUIRED - Define schema
    id: 'number',
    name: 'string',
    description: 'string',
    category: 'string',
    tags: 'array:string',
    price: 'number',
    imageUrl: 'string',
    inStock: 'boolean',
    createdAt: 'number'
  },
  indexes: {                  // OPTIONAL - Only for performance
    id: 'number',             // Primary key - always index
    category: 'string',       // Filter by category - index this
    tags: 'array:string',     // Search by tags - index this
    price: 'number'          // Price range queries - index this
  }
  // Fields like 'name', 'description', 'imageUrl' are still queryable
  // but will use slower sequential search (no index needed unless you query them frequently)
})

await db.init()

// All these queries work, but some are faster:
await db.find({ id: 1 })                    // ✅ Fast (indexed)
await db.find({ category: 'electronics' }) // ✅ Fast (indexed)  
await db.find({ tags: 'wireless' })        // ✅ Fast (indexed)
await db.find({ price: { '>': 100 } })     // ✅ Fast (indexed)
await db.find({ name: 'iPhone' })          // ⚠️ Slower (not indexed, but still works)
await db.find({ description: 'wireless' }) // ⚠️ Slower (not indexed, but still works)
```

## Core Methods

### `init()`

Initialize the database and load existing data.

```javascript
await db.init()
```

### `insert(data)`

Insert a new record into the database.

```javascript
await db.insert({ 
  id: 1, 
  name: 'John Doe', 
  email: 'john@example.com' 
})
```

### `update(criteria, updates)`

Update records matching the criteria.

```javascript
await db.update(
  { id: 1 }, 
  { email: 'newemail@example.com', updated: true }
)
```

### `delete(criteria)`

Delete records matching the criteria.

```javascript
await db.delete({ id: 1 })
```

### `save()`

Save all pending changes to disk.

```javascript
await db.save()
```

### `destroy()`

Close the database and clean up resources.

```javascript
await db.destroy()
```

## Query Methods

### `find(criteria, options)`

Find records matching the criteria.

```javascript
// Basic query
const results = await db.find({ name: 'John Doe' })

// Query with conditions
const results = await db.find({ 
  age: { '>': 18, '<': 65 },
  status: 'active'
})

// Case insensitive search
const results = await db.find(
  { name: 'john doe' }, 
  { caseInsensitive: true }
)
```

### `findOne(criteria, options)`

Find the first record matching the criteria.

```javascript
const user = await db.findOne({ id: 1 })
```

### `count(criteria)`

Count records matching the criteria.

```javascript
const userCount = await db.count({ status: 'active' })
```

### `score(fieldName, scores, options)`

Score and rank records based on weighted terms in an indexed `array:string` field. This method is optimized for in-memory operations and provides 10x+ performance improvement over equivalent `find()` queries.

```javascript
// Basic scoring
const results = await db.score('tags', {
  'javascript': 1.0,
  'node': 0.8,
  'typescript': 0.9
})

// With options
const results = await db.score('terms', {
  'action': 1.0,
  'comedy': 0.8
}, {
  limit: 10,
  sort: 'desc',
  includeScore: true
})
```

**Parameters:**

- `fieldName` (string, required): Name of the indexed `array:string` field to score
- `scores` (object, required): Map of terms to numeric weights (e.g., `{ 'action': 1.0, 'comedy': 0.8 }`)
- `options` (object, optional):
  - `limit` (number): Maximum results (default: 100)
  - `sort` (string): "desc" or "asc" (default: "desc")
  - `includeScore` (boolean): Include score in results (default: true)
  - `mode` (string): Score aggregation strategy: `"sum"` (default), `"max"`, `"avg"`, or `"first"`

**Returns:**

Array of records ordered by score, with optional score property:

```javascript
// With includeScore: true (default)
[
  { _: 123, score: 1.8, title: "Action Comedy", terms: ["action", "comedy"] },
  { _: 456, score: 1.0, title: "Action Movie", terms: ["action", "movie"] },
  { _: 789, score: 0.8, title: "Comedy Show", terms: ["comedy", "show"] }
]

// With includeScore: false
[
  { _: 123, title: "Action Comedy", terms: ["action", "comedy"] },
  { _: 456, title: "Action Movie", terms: ["action", "movie"] },
  { _: 789, title: "Comedy Show", terms: ["comedy", "show"] }
]
```

**Score Calculation Modes:**

- `sum` *(default)*: score is the sum of all weights for the matched keywords.
- `max`: score is the highest weight among the matched keywords.
- `avg`: score is the arithmetic average of the weights for the matched keywords.
- `first`: score uses the weight of the first keyword that appears in the `scores` object and matches the record (subsequent keywords are ignored).

```javascript
await db.score('terms', { a: 1.0, b: 0.5 }, { mode: 'sum'  })  // -> 1.5
await db.score('terms', { a: 1.0, b: 0.5 }, { mode: 'max'  })  // -> 1.0
await db.score('terms', { a: 1.0, b: 0.5 }, { mode: 'avg'  })  // -> 0.75
await db.score('terms', { a: 1.0, b: 0.5 }, { mode: 'first'})  // -> 1.0
```

**Example Use Cases:**

```javascript
// Content recommendation system
const recommendations = await db.score('categories', {
  'technology': 1.0,
  'programming': 0.9,
  'web': 0.8
}, { limit: 20 })

// Search with weighted keywords
const searchResults = await db.score('keywords', {
  'urgent': 2.0,
  'important': 1.5,
  'notable': 1.0
}, { sort: 'desc', limit: 50 })

// Product recommendations
const productMatches = await db.score('tags', {
  'wireless': 1.2,
  'bluetooth': 1.0,
  'rechargeable': 0.9,
  'compact': 0.8
}, { includeScore: false })
```

**Performance Notes:**

- ⚡ **Memory-only operations** - Uses in-memory indices exclusively
- ⚡ **No physical I/O for scoring** - Only final record fetch requires disk access
- ⚡ **10x+ faster** than equivalent `find()` + manual scoring
- ⚡ **O(T × N) complexity** - T = terms in scores, N = avg records per term
- ⚡ **Optimal for selective queries** - Best when scoring subset of total records

**Requirements:**

- Field must be indexed as `array:string` type
- Field must be present in `indexes` configuration
- Returns empty array if no terms match
- Records with zero scores are excluded

**Error Handling:**

```javascript
try {
  const results = await db.score('tags', { 'javascript': 1.0 })
} catch (error) {
  // Error: Field "tags" is not indexed
  // Error: Field "tags" must be of type "array:string"
  // Error: scores must be an object
  // Error: Score value for term "javascript" must be a number
}
```

### Query Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `>` | Greater than | `{ age: { '>': 18 } }` |
| `>=` | Greater than or equal | `{ score: { '>=': 80 } }` |
| `<` | Less than | `{ price: { '<': 100 } }` |
| `<=` | Less than or equal | `{ rating: { '<=': 5 } }` |
| `!=` | Not equal | `{ status: { '!=': 'deleted' } }` |
| `$in` | In array | `{ category: { '$in': ['A', 'B'] } }` |
| `$not` | Not | `{ name: { '$not': 'John' } }` |
| `$and` | Logical AND | `{ '$and': [{ age: { '>': 18 } }, { status: 'active' }] }` |
| `$or` | Logical OR | `{ '$or': [{ type: 'admin' }, { type: 'moderator' }] }` |

### Complex Queries

```javascript
// Multiple conditions (AND by default)
const results = await db.find({
  age: { '>': 18 },
  status: 'active',
  category: { '$in': ['premium', 'vip'] }
})

// Using logical operators
const results = await db.find({
  '$or': [
    { type: 'admin' },
    { '$and': [
      { type: 'user' },
      { verified: true }
    ]}
  ]
})

// Array field queries
const results = await db.find({
  tags: 'javascript',        // Contains 'javascript'
  tags: { '$all': ['js', 'node'] }  // Contains all specified tags
})
```

## Advanced Features

### Indexed Query Mode

Control whether queries are restricted to indexed fields only.

```javascript
// Strict mode - only indexed fields allowed in queries
const db = new Database('db.jdb', {
  fields: {                    // REQUIRED - Define schema
    id: 'number',
    name: 'string', 
    age: 'number',
    email: 'string'
  },
  indexes: {                   // OPTIONAL - Only fields you query frequently
    name: 'string',            // ✅ Search by name
    age: 'number'              // ✅ Filter by age
  },
  indexedQueryMode: 'strict'
})

// This will throw an error in strict mode
await db.find({ email: 'test@example.com' }) // Error: email is not indexed

// Permissive mode (default) - allows any field
const db = new Database('db.jdb', {
  fields: {                    // REQUIRED - Define schema
    id: 'number',
    name: 'string', 
    age: 'number',
    email: 'string'
  },
  indexes: {                   // OPTIONAL - Only fields you query frequently
    name: 'string',            // ✅ Search by name
    age: 'number'              // ✅ Filter by age
  },
  indexedQueryMode: 'permissive'
})

// This works in permissive mode, but will be slower than strict mode
await db.find({ email: 'test@example.com' }) // OK
```

### Query Performance

**Index Strategy Guidelines:**

1. **Always index primary keys** (usually `id`)
2. **Index frequently queried fields** (filters, searches)
3. **Don't index everything** - each index uses memory
4. **Use specific criteria** rather than broad queries

**Performance Examples:**

```javascript
// ✅ Good: Index only what you need
const db = new Database('users.jdb', {
  fields: {                    // REQUIRED - Define schema
    id: 'number',
    email: 'string',
    status: 'string',
    name: 'string',
    phone: 'string'
  },
  indexes: {                   // OPTIONAL - Only fields you query frequently
    email: 'string',         // ✅ Login queries
    status: 'string'          // ✅ Filter active/inactive users
  }
})

// ❌ Bad: Over-indexing wastes memory
const db = new Database('users.jdb', {
  fields: {                    // REQUIRED - Define schema
    id: 'number',
    name: 'string',
    email: 'string',
    phone: 'string',
    address: 'string',
    city: 'string',
    country: 'string',
    createdAt: 'number',
    updatedAt: 'number'
  },
  indexes: {                   // OPTIONAL - Performance optimization (too many!)
    name: 'string',          // ❌ Only if you search by name frequently
    email: 'string',         // ❌ Only if you search by email frequently
    phone: 'string',         // ❌ Only if you search by phone frequently  
    address: 'string',       // ❌ Only if you search by address frequently
    city: 'string',          // ❌ Only if you search by city frequently
    country: 'string',      // ❌ Only if you search by country frequently
    createdAt: 'number',     // ❌ Only if you filter by date frequently
    updatedAt: 'number'     // ❌ Only if you filter by date frequently
  }
})

// Query performance comparison:
await db.find({ id: 1 })           // ✅ Fast (indexed)
await db.find({ email: 'user@example.com' }) // ✅ Fast (indexed)
await db.find({ status: 'active' }) // ✅ Fast (indexed)
await db.find({ name: 'John' })    // ⚠️ Slower (not indexed, but works)
await db.find({ phone: '123-456-7890' }) // ⚠️ Slower (not indexed, but works)
```

**Memory Impact:**
- Each index uses additional memory
- String indexes use more memory than number indexes
- Array indexes use more memory than single-value indexes
- **Rule of thumb**: Only index fields you query in 80%+ of your queries

## Term Mapping

Term mapping is a powerful optimization that reduces database size by mapping repetitive string terms to numeric IDs. **It's now enabled by default** and automatically detects which fields benefit from term mapping.

### Benefits

- **77% size reduction** in typical scenarios
- **Faster queries** with numeric comparisons
- **Automatic cleanup** of unused terms
- **Transparent operation** - same API
- **Auto-detection** of optimal fields
- **Zero configuration** required

### Automatic Configuration

Term mapping is now **automatically enabled** and detects fields that benefit from optimization:

```javascript
const db = new Database('my-db.jdb', {
  fields: {                    // REQUIRED - Define schema
    id: 'number',
    name: 'string',
    tags: 'array:string',
    scores: 'array:number',
    price: 'number'
  },
  indexes: {                   // OPTIONAL - Only fields you query frequently
    name: 'string',           // ✅ Search by name (auto term mapping)
    tags: 'array:string'      // ✅ Search by tags (auto term mapping)
  }
})

// Term mapping is automatically enabled
console.log(db.opts.termMapping) // true
console.log(db.termManager.termMappingFields) // ['name', 'tags']
```

### Manual Configuration

For fine-grained control, you can manually specify which fields should use term mapping:

```javascript
const db = new Database('selective-mapping.jdb', {
  fields: {
    name: 'string',
    groups: 'array:string',        // Should remain as strings
    nameTerms: 'array:string',     // Should use term mapping
    groupTerms: 'array:string',    // Should use term mapping
  },
  indexes: {
    groups: 'array:string',        // Indexed but not term mapped
    nameTerms: 'array:string',     // Indexed and term mapped
    groupTerms: 'array:string',    // Indexed and term mapped
  },
  termMapping: true,
  termMappingFields: ['nameTerms', 'groupTerms'] // Only these fields
})

// Only specified fields use term mapping
console.log(db.termManager.termMappingFields) // ['nameTerms', 'groupTerms']
```

**When to use `termMappingFields`:**
- When you have `array:string` fields that should remain as actual strings
- When you want selective term mapping for performance reasons
- When auto-detection includes fields you don't want optimized

### How It Works

Term mapping automatically detects and optimizes fields:

1. **Auto-detection**: Fields with `'string'` or `'array:string'` types are automatically optimized
2. **Term ID mapping**: Each unique string term gets a numeric ID
3. **Efficient storage**: Term IDs are stored in optimized structures
4. **Transparent queries**: Queries automatically convert search terms to IDs
5. **Automatic cleanup**: Unused terms are automatically cleaned up

### Field Type Behavior

| Field Type | Term Mapping | Storage | Example |
|------------|--------------|---------|---------|
| `'string'` | ✅ Enabled | Term IDs | `"Brazil"` → `1` |
| `'array:string'` | ✅ Enabled | Term IDs | `["Brazil", "Argentina"]` → `[1, 2]` |
| `'number'` | ❌ Disabled | Direct values | `85` → `"85"` |
| `'array:number'` | ❌ Disabled | Direct values | `[85, 92]` → `["85", "92"]` |
| `'boolean'` | ❌ Disabled | Direct values | `true` → `"true"` |
| `'array:boolean'` | ❌ Disabled | Direct values | `[true, false]` → `["true", "false"]` |

### Example Usage

```javascript
// Create database with mixed field types
const db = new Database('products.jdb', {
  fields: {                    // REQUIRED - Define schema
    id: 'number',
    name: 'string',
    tags: 'array:string',
    scores: 'array:number',
    price: 'number'
  },
  indexes: {                   // OPTIONAL - Only fields you query frequently
    name: 'string',           // ✅ Search by name (auto term mapping)
    tags: 'array:string'      // ✅ Search by tags (auto term mapping)
  }
})

// Insert data with repetitive terms
await db.insert({
  name: 'Product A',
  tags: ['electronics', 'gadget', 'wireless'],
  scores: [85, 92, 78],
  price: 299.99
})

await db.insert({
  name: 'Product B', 
  tags: ['electronics', 'accessory', 'wireless'],
  scores: [90, 88, 95],
  price: 199.99
})

// Queries work normally - term mapping is transparent
const results = await db.find({ tags: 'electronics' })
const expensive = await db.find({ price: { '>': 250 } })

// Check term mapping status
console.log(db.opts.termMapping) // true
console.log(db.termManager.termMappingFields) // ['name', 'tags']
console.log(db.termManager.getStats()) // { totalTerms: 6, nextId: 7, orphanedTerms: 0 }
```

### Term Manager API

```javascript
// Get term ID (creates if doesn't exist)
const termId = db.termManager.getTermId('electronics')

// Get term by ID
const term = db.termManager.getTerm(1)

// Get statistics
const stats = db.termManager.getStats()

// Manual cleanup
const removedCount = await db.termManager.cleanupOrphanedTerms()
```

## Bulk Operations

### `iterate(criteria, options)`

High-performance bulk update method with streaming support.

```javascript
// Basic iteration
for await (const entry of db.iterate({ category: 'products' })) {
  console.log(`${entry.name}: $${entry.price}`)
}

// Bulk updates
for await (const entry of db.iterate({ inStock: true })) {
  entry.price = Math.round(entry.price * 1.1 * 100) / 100
  entry.lastUpdated = new Date().toISOString()
}

// Advanced options
for await (const entry of db.iterate(
  { category: 'electronics' },
  {
    chunkSize: 1000,           // Process in batches
    autoSave: false,           // Auto-save after each chunk
    detectChanges: true,       // Auto-detect modifications
    progressCallback: (progress) => {
      console.log(`Processed: ${progress.processed}, Modified: ${progress.modified}`)
    }
  }
)) {
  entry.processed = true
}
```

### Iterate Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `chunkSize` | number | 1000 | Batch size for processing |
| `strategy` | string | 'streaming' | Processing strategy |
| `autoSave` | boolean | false | Auto-save after each chunk |
| `detectChanges` | boolean | true | Auto-detect modifications |
| `progressCallback` | function | undefined | Progress callback function |

### Progress Callback

```javascript
{
  processed: 1500,      // Number of records processed
  modified: 120,        // Number of records modified
  deleted: 5,           // Number of records deleted
  elapsed: 2500,        // Time elapsed in milliseconds
  completed: false      // Whether the operation is complete
}
```

### `beginInsertSession(options)`

High-performance batch insertion method for inserting large amounts of data efficiently.

```javascript
// Example: EPG (Electronic Program Guide) data insertion
const session = db.beginInsertSession({
  batchSize: 500,            // Process in batches of 500
  enableAutoSave: true       // Auto-save after each batch
})

// Insert TV program data
const programmes = [
  {
    id: 1,
    title: 'Breaking News',
    channel: 'CNN',
    startTime: 1640995200000,
    endTime: 1640998800000,
    terms: ['news', 'breaking', 'politics'],
    category: 'news'
  },
  {
    id: 2,
    title: 'Sports Center',
    channel: 'ESPN',
    startTime: 1640998800000,
    endTime: 1641002400000,
    terms: ['sports', 'football', 'highlights'],
    category: 'sports'
  }
  // ... thousands more programmes
]

// Add all programmes to the session
for (const programme of programmes) {
  await session.add(programme)
}

// Commit all records at once (much faster than individual inserts)
await session.commit()
await db.save()

console.log(`Inserted ${session.totalInserted} programmes`)
```

### InsertSession Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `batchSize` | number | 100 | Number of records to process in each batch |
| `enableAutoSave` | boolean | false | Auto-save after each batch |

### InsertSession Methods

```javascript
const session = db.beginInsertSession({ batchSize: 500 })

// Add a single record
await session.add({ id: 1, name: 'John', email: 'john@example.com' })

// Add multiple records
for (const record of records) {
  await session.add(record)
}

// Commit all pending records
await session.commit()

// Get statistics
console.log(`Inserted ${session.totalInserted} records`)
```

### When to Use `beginInsertSession()`

**Use `beginInsertSession()` for:**
- ✅ **Bulk insertions** (1000+ new records)
- ✅ **Data migration** from other databases
- ✅ **Initial data loading** (EPG, product catalogs, etc.)
- ✅ **Batch processing** of external data sources

### Performance Comparison

```javascript
// ❌ SLOW: Individual inserts (1000 records = 1000 database operations)
for (let i = 0; i < 1000; i++) {
  await db.insert({ id: i, name: `Record ${i}` })
}

// ✅ FAST: Batch insertion (1000 records = 1 database operation)
const session = db.beginInsertSession({ batchSize: 1000 })
for (let i = 0; i < 1000; i++) {
  await session.add({ id: i, name: `Record ${i}` })
}
await session.commit()
```

### Performance Tips

1. **Use `beginInsertSession()`** for bulk insertions of 1000+ records
2. **Pre-initialize the schema** (via `fields` or `schema` options) before heavy ingestion to skip per-record auto-detection.
3. **Tune `batchSize` to your hardware**: start around 500–2000 records and watch memory/flush times; lower the value if auto-flush starts queuing.
4. **Enable autoSave** for critical operations to prevent data loss
5. **Use indexed fields** in your data for better query performance later
6. **Commit frequently** for very large datasets to avoid memory issues

## Configuration Options

### Database Options

```javascript
const db = new Database('database.jdb', {
  // Basic options
  create: true,                    // Create file if doesn't exist
  clear: false,                    // Clear existing files
  
  // Schema (REQUIRED - define data structure)
  fields: {                        // MANDATORY - Define all possible fields
    id: 'number',
    name: 'string',
    email: 'string',
    phone: 'string',
    address: 'string',
    city: 'string',
    country: 'string',
    status: 'string',
    createdAt: 'number',
    tags: 'array:string'
  },
  
  // Indexing (OPTIONAL - only for performance optimization)
  indexes: {                       // Only index fields you query frequently
    id: 'number',                  // Primary key - always index
    email: 'string',               // Login queries - index this
    status: 'string'               // Filter queries - index this
    // Don't index: name, phone, address, etc. unless you query them frequently
  },
  
  // Query mode
  indexedQueryMode: 'permissive',  // 'strict' or 'permissive'
  
  // Term mapping (enabled by default)
  termMapping: true,               // Enable term mapping (auto-detected)
  termMappingFields: [],           // Fields to map (auto-detected)
  termMappingCleanup: true,        // Auto cleanup
  
  // Performance
  chunkSize: 1000,                 // Default chunk size
  autoSave: false,                 // Auto-save changes
  
  // Debug
  debugMode: false                 // Enable debug logging
})
```

### Schema vs Indexes - Complete Guide

**Understanding the Difference:**

```javascript
// Your data structure (schema) - MUST be defined in fields option
const userRecord = {
  id: 1,
  name: 'John Doe',
  email: 'john@example.com',
  phone: '123-456-7890',
  address: '123 Main St',
  city: 'New York',
  country: 'USA',
  status: 'active',
  createdAt: 1640995200000,
  tags: ['premium', 'verified']
}

// Database configuration
const db = new Database('users.jdb', {
  fields: {                 // REQUIRED - Define schema structure
    id: 'number',
    name: 'string',
    email: 'string',
    phone: 'string',
    address: 'string',
    city: 'string',
    country: 'string',
    status: 'string',
    createdAt: 'number',
    tags: 'array:string'
  },
  indexes: {                // OPTIONAL - Only for performance optimization
    id: 'number',           // Primary key - always index
    email: 'string',        // Login queries - index this
    status: 'string',       // Filter queries - index this
    tags: 'array:string'    // Search by tags - index this
  }
  // Fields like name, phone, address, city, country, createdAt
  // are still queryable but will use slower sequential search
})

// All these queries work:
await db.find({ id: 1 })                    // ✅ Fast (indexed)
await db.find({ email: 'john@example.com' }) // ✅ Fast (indexed)
await db.find({ status: 'active' })        // ✅ Fast (indexed)
await db.find({ tags: 'premium' })         // ✅ Fast (indexed)
await db.find({ name: 'John Doe' })         // ⚠️ Slower (not indexed, but works)
await db.find({ phone: '123-456-7890' })   // ⚠️ Slower (not indexed, but works)
await db.find({ city: 'New York' })         // ⚠️ Slower (not indexed, but works)
await db.find({ createdAt: { '>': 1640000000000 } }) // ⚠️ Slower (not indexed, but works)
```

**Key Points:**
- ✅ **All fields are queryable** regardless of indexing
- ✅ **Schema is auto-detected** from your data
- ⚠️ **Indexes are optional** - only for performance
- ⚠️ **Each index uses memory** - use sparingly
- ⚠️ **Index only what you query frequently** (80%+ of queries)

### Field Types

Supported field types for indexing:

| Type | Term Mapping | Description | Example |
|------|--------------|-------------|---------|
| `'string'` | ✅ Auto-enabled | String values | `"Brazil"` |
| `'array:string'` | ✅ Auto-enabled | Array of strings | `["Brazil", "Argentina"]` |
| `'number'` | ❌ Disabled | Numeric values | `85` |
| `'array:number'` | ❌ Disabled | Array of numbers | `[85, 92, 78]` |
| `'boolean'` | ❌ Disabled | Boolean values | `true` |
| `'array:boolean'` | ❌ Disabled | Array of booleans | `[true, false]` |
| `'date'` | ❌ Disabled | Date objects (stored as timestamps) | `new Date()` |

## Error Handling

```javascript
try {
  await db.init()
  await db.insert({ id: 1, name: 'Test' })
  await db.save()
} catch (error) {
  console.error('Database error:', error.message)
} finally {
  await db.destroy()
}
```

## Best Practices

### Database Operations
1. **Always define `fields`** - This is mandatory for schema definition
2. **Always call `init()`** before using the database
3. **Use `save()`** to persist changes to disk
4. **Call `destroy()`** when done to clean up resources
5. **Handle errors** appropriately

### Performance Optimization
6. **Index strategically** - only fields you query frequently (80%+ of queries)
7. **Don't over-index** - each index uses additional memory
8. **Term mapping is automatically enabled** for optimal performance
9. **Use `iterate()`** for bulk operations on large datasets

### Index Strategy
10. **Always index primary keys** (usually `id`)
11. **Index frequently filtered fields** (status, category, type)
12. **Index search fields** (email, username, tags)
13. **Don't index descriptive fields** (name, description, comments) unless you search them frequently
14. **Monitor memory usage** - too many indexes can impact performance

## Migration Guide

### From Previous Versions

If you're upgrading from an older version:

1. **Backup your data** before upgrading
2. **Check breaking changes** in the changelog
3. **Update your code** to use new APIs
4. **Test thoroughly** with your data

### Common Migration Tasks

```javascript
// Old way
const results = db.query({ name: 'John' })

// New way  
const results = await db.find({ name: 'John' })

// Old way
db.insert({ id: 1, name: 'John' })

// New way
await db.insert({ id: 1, name: 'John' })
```


