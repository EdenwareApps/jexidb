# JexiDB Documentation

Welcome to the JexiDB documentation! This directory contains comprehensive guides and references for using JexiDB effectively.

## 📚 Documentation Structure

### 🚀 [API Reference](API.md)
Complete API documentation covering all methods, options, and features:
- Database constructor and configuration
- Core methods (insert, update, delete, find)
- Advanced features (term mapping, bulk operations)
- Query operators and complex queries
- Performance optimization tips

### 💡 [Examples](EXAMPLES.md)
Practical examples and real-world use cases:
- Basic usage patterns
- User management systems
- Product catalogs
- Blog systems
- Analytics dashboards
- Performance optimization techniques
- Error handling strategies

## 🎯 Quick Start

If you're new to JexiDB, start with these resources:

1. **Installation**: See the main [README.md](../README.md) for installation instructions
2. **Basic Usage**: Check out the [Basic Usage](EXAMPLES.md#basic-usage) section in Examples
3. **API Reference**: Browse the [API Reference](API.md) for detailed method documentation
4. **Advanced Features**: Explore [Term Mapping](API.md#term-mapping) and [Bulk Operations](API.md#bulk-operations)

## 🔍 What's New

### Recent Features
- **Term Mapping**: Reduce database size by up to 77% for repetitive data
- **Bulk Operations**: High-performance `iterate()` method for large datasets
- **Advanced Querying**: Support for complex queries with logical operators
- **Indexed Query Modes**: Strict and permissive query modes for performance control

### Performance Improvements
- Streaming operations for memory efficiency
- Automatic term cleanup
- Optimized indexing strategies
- Batch processing capabilities

## 📖 Key Concepts

### Indexes
Define which fields to keep in memory for fast queries:
```javascript
const db = new Database('app.jdb', {
  fields: {                    // REQUIRED - Define schema
    id: 'number',
    name: 'string',
    email: 'string'
  },
  indexes: {                    // OPTIONAL - Only fields you query frequently
    email: 'string'            // ✅ Login queries
  }
})
```

### Term Mapping
Optimize storage for repetitive string data:
```javascript
const db = new Database('app.jdb', {
  fields: {                    // REQUIRED - Define schema
    id: 'number',
    name: 'string',
    tags: 'array:string',
    categories: 'array:string'
  }
  // termMapping is now auto-enabled for array:string fields
})
```

### Bulk Operations
Process large datasets efficiently:
```javascript
for await (const record of db.iterate({ status: 'pending' })) {
  record.status = 'processed'
  record.updatedAt = Date.now()
}
```

## 🛠️ Common Patterns

### Database Lifecycle
```javascript
const db = new Database('my-app.jdb', options)
await db.init()        // Initialize
// ... use database ...
await db.save()        // Save changes
await db.destroy()     // Clean up
```

### Error Handling
```javascript
try {
  await db.init()
  await db.insert(data)
  await db.save()
} catch (error) {
  console.error('Database error:', error)
} finally {
  await db.destroy()
}
```

### Query Patterns
```javascript
// Simple queries
const users = await db.find({ status: 'active' })

// Complex queries
const results = await db.find({
  age: { '>': 18, '<': 65 },
  $or: [
    { role: 'admin' },
    { role: 'moderator' }
  ]
})

// Case-insensitive search
const results = await db.find(
  { name: 'john doe' },
  { caseInsensitive: true }
)
```

## 🔧 Configuration Options

### Basic Configuration
```javascript
const db = new Database('database.jdb', {
  create: true,           // Create file if doesn't exist
  clear: false,           // Clear existing data
  fields: {                // REQUIRED - Define schema
    id: 'number',
    name: 'string'
  },
  indexes: {              // OPTIONAL - Only fields you query frequently
    name: 'string'        // ✅ Search by name
  },
  indexedQueryMode: 'permissive'  // Query mode
})
```

### Advanced Configuration
```javascript
const db = new Database('database.jdb', {
  fields: {                        // REQUIRED - Define schema
    id: 'number',
    name: 'string',
    tags: 'array:string'
  },
  indexes: {                      // OPTIONAL - Only fields you query frequently
    name: 'string',              // ✅ Search by name
    tags: 'array:string'         // ✅ Search by tags
  }
  // termMapping is now auto-enabled for array:string fields
})
```

## 📊 Performance Tips

1. **Use indexed fields** in your queries for best performance
2. **Enable term mapping** for datasets with repetitive strings
3. **Use `iterate()`** for bulk operations on large datasets
4. **Specify fewer indexes** to reduce memory usage
5. **Use `save()`** strategically to persist changes

## 🆘 Getting Help

- **Issues**: Report bugs and request features on [GitHub Issues](https://github.com/EdenwareApps/jexidb/issues)
- **Documentation**: Browse this documentation for detailed guides
- **Examples**: Check out the [Examples](EXAMPLES.md) for practical use cases
- **API Reference**: Consult the [API Reference](API.md) for method details

## 📝 Contributing

Found an issue with the documentation? Want to add an example?

1. Fork the repository
2. Make your changes
3. Submit a pull request

We welcome contributions to improve the documentation!

---

**Happy coding with JexiDB! 🚀**
