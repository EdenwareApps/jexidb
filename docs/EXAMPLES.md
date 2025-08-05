# JexiDB Usage Examples

## Basic Usage

### Simple Database

```javascript
const { Database } = require('jexidb');

// Create database
const db = new Database('./data/users.jsonl');

// Initialize
await db.init();

// Insert data
await db.insert({ id: '1', name: 'John', age: 30 });
await db.insert({ id: '2', name: 'Jane', age: 25 });

// Find data
const users = await db.find({ age: { $gte: 25 } });
const user = await db.findOne({ id: '1' });

// Update data
await db.update({ id: '1' }, { age: 31 });

// Delete data
await db.delete({ id: '2' });

// Save changes
await db.save();

// Clean up
await db.destroy();
```

### With Indexes

```javascript
const db = new Database('./data/products.jsonl', {
  indexes: {
    id: true,
    category: true,
    price: true
  }
});

await db.init();

// Fast indexed queries
const expensiveProducts = await db.find({ price: { $gt: 100 } });
const electronics = await db.find({ category: 'electronics' });
```

### Complex Queries

```javascript
// Nested field queries
const activeUsers = await db.find({
  'metadata.active': true,
  'preferences.theme': 'dark',
  age: { $gte: 18, $lte: 65 }
}, {
  limit: 50,
  sort: { 'metadata.lastLogin': -1 }
});

// Array queries
const taggedPosts = await db.find({
  tags: { $in: ['javascript', 'database'] }
});

// Regular expressions
const emailUsers = await db.find({
  email: { $regex: /@gmail\.com$/ }
});
```

### Bulk Operations

```javascript
// Bulk insert
const users = Array.from({ length: 1000 }, (_, i) => ({
  id: `user_${i}`,
  name: `User ${i}`,
  email: `user${i}@example.com`,
  age: Math.floor(Math.random() * 50) + 18
}));

await db.insertMany(users);

// Bulk update
await db.update(
  { age: { $lt: 18 } },
  { status: 'minor' }
);

// Bulk delete
await db.delete({ status: 'inactive' });
```

### Event Handling

```javascript
db.on('insert', (record, index) => {
  console.log(`Inserted record at index ${index}`);
});

db.on('update', (records) => {
  console.log(`Updated ${records.length} records`);
});

db.on('delete', (records) => {
  console.log(`Deleted ${records.length} records`);
});

db.on('save', () => {
  console.log('Database saved');
});
```

### Error Handling

```javascript
try {
  await db.insert(invalidData);
} catch (error) {
  if (error.name === 'JexiDBError[JSON_PARSE_ERROR]') {
    console.log('Invalid JSON data');
  } else if (error.name === 'JexiDBError[FILE_CORRUPTION]') {
    console.log('File corruption detected, attempting recovery...');
  }
}
```

### Performance Monitoring

```javascript
// Get database statistics
const stats = await db.getStats();
console.log(`Records: ${stats.recordCount}`);
console.log(`File size: ${stats.fileSize} bytes`);
console.log(`Cache hit rate: ${stats.cacheStats.hitRate}%`);

// Get optimization recommendations
const recommendations = db.getOptimizationRecommendations();
console.log('Optimization tips:', recommendations);
```

### Advanced Configuration

```javascript
const db = new Database('./data/advanced.jsonl', {
  indexes: {
    id: true,
    email: true,
    'metadata.created': true
  },
  markDeleted: false, // Physical deletion
  autoSave: true,
  validateOnInit: true,
  backgroundMaintenance: true,
  cache: {
    maxSize: 1000,
    ttl: 300000 // 5 minutes
  },
  compression: {
    hot: { type: 'none', threshold: 7 },
    warm: { type: 'lz4', threshold: 30 },
    cold: { type: 'gzip', threshold: Infinity }
  },
  errorHandler: {
    logLevel: 'info',
    enableRecovery: true
  }
});
```
