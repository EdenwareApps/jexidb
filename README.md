# JexiDB - Intelligent JavaScript Database for Desktop Apps

**JEXIDB** = **J**avaScript **EX**tended **I**ntelligent **D**ata**B**ase

**The Only Pure JS Database with Smart Disk Persistence & Intelligent Memory Management** - Schema-Enforced, Streaming-Ready, No Native Dependencies

<p align="center">
  <img width="270" src="https://edenware.app/jexidb/images/jexidb-logo-icon.jpg" alt="JexiDB - JavaScript EXtended Intelligent DataBase for Node.js and Electron applications" title="JexiDB - JavaScript EXtended Intelligent DataBase logo" />
</p>

<div align="center">

[![npm version](https://img.shields.io/npm/v/jexidb.svg?style=flat-square)](https://www.npmjs.com/package/jexidb)
[![npm downloads](https://img.shields.io/npm/dm/jexidb.svg?style=flat-square)](https://www.npmjs.com/package/jexidb)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=flat-square)](https://opensource.org/licenses/MIT)
[![Node.js Version](https://img.shields.io/node/v/jexidb?style=flat-square)](https://nodejs.org/)
[![GitHub stars](https://img.shields.io/github/stars/EdenwareApps/jexidb?style=flat-square)](https://github.com/EdenwareApps/jexidb)
[![GitHub issues](https://img.shields.io/github/issues/EdenwareApps/jexidb?style=flat-square)](https://github.com/EdenwareApps/jexidb/issues)

**⚡ High-Performance • 💾 Persistent • 🧠 Memory Efficient • 🚀 Production Ready**

[📖 Documentation](docs/README.md) • [💡 Examples](docs/EXAMPLES.md) • [🔧 API Reference](docs/API.md) • [🚀 Quick Start](#quick-start---5-minutes-to-database)

</div>

## Table of Contents

- [Why Developers Choose JexiDB](#why-developers-choose-jexidb)
- [JexiDB vs Other JavaScript Databases](#jexidb-vs-other-javascript-databases)
- [Performance Benchmarks](#performance-benchmarks)
- [Frequently Asked Questions](#frequently-asked-questions)
- [Quick Start - 5 Minutes to Database](#quick-start---5-minutes-to-database)
- [Real-World Use Cases](#real-world-use-cases)
- [Migration Guide](#migration-guide)
- [What's Next - Roadmap](#whats-next---roadmap)

---

## What Makes JexiDB Unique

**The Only Pure JS Database with Smart Disk Persistence & Intelligent Memory Management**

JexiDB stands out among JavaScript databases by combining **pure JavaScript simplicity** with **enterprise-grade data management**. While other JS databases offer basic persistence, JexiDB provides **intelligent memory management** and **structured data enforcement** that traditional document databases lack.

### Unique Advantages Over Other JavaScript Databases

🚀 **Pure JavaScript, No Compromises** - 100% JavaScript, no native dependencies, WASM, or compilation required. Compatible with both CommonJS and ESM modules

💾 **Intelligent Disk Persistence** - JSONL files with compressed persistent indexes, not memory-only storage

📋 **Schema Enforcement** - Structured data model like SQL tables, ensuring data consistency in pure JavaScript

🧠 **Smart Memory Management** - Point reading and streaming operations, handles millions of records without loading everything into RAM

🔍 **Advanced Query Optimization** - MongoDB-like operators with automatic term mapping for 77% size reduction

⚡ **Production-Ready Performance** - Compressed indexes, streaming operations, and automatic optimization

🖥️ **Desktop-First Design** - Optimized for Electron, NW.js, and local Node.js applications from the ground up

### Technical Specifications

- **Storage Format**: JSONL (JSON Lines) with compressed persistent indexes
- **Memory Strategy**: Point reading + streaming, doesn't require loading entire database
- **Query Language**: Advanced operators ($in, $or, $and, ranges, regex, case-insensitive)
- **Data Types**: string, number, boolean, array (with automatic optimization)
- **Indexing**: Persistent compressed indexes with term mapping for optimal performance
- **Transactions**: Operation queuing with manual save control (auto-save removed for better control)
- **Persistence**: Manual save functionality ensures data integrity and performance
- **File Size**: Typically 25-75% smaller than alternatives for repetitive data

## JexiDB vs Other JavaScript Databases

| Database | Pure JS | Disk Persistence | Memory Usage | Data Structure | Best Use Case |
|----------|---------|------------------|--------------|----------------|---------------|
| **JexiDB** | ✅ 100% | ✅ JSONL + Compressed Indexes | 🧠 Smart (point reading) | 📋 Schema Required | Desktop apps, Node.js with structured data |
| **NeDB** | ✅ 100% | ✅ JSON files | 📈 High (loads all) | 📄 Document-free | Legacy projects (unmaintained) |
| **Lowdb** | ✅ 100% | ✅ JSON files | 📈 High (loads all) | 📄 Document-free | Small configs, simple apps |
| **LokiJS** | ✅ 100% | ⚠️ Adapters only | 📈 High (in-memory primary) | 📄 Document-free | In-memory applications |
| **PouchDB** | ✅ 100% | ✅ IndexedDB/WebSQL | 🧠 Moderate | 📄 Document-free | Offline-first web apps |
| **SQLite (WASM)** | ❌ WASM required | ✅ SQLite files | 🧠 Moderate | 📊 SQL Tables | Complex relational data |

### Why Developers Choose JexiDB

**Intelligent Memory Management**: Unlike other JS databases that load entire files into memory, JexiDB uses point reading and streaming operations for unlimited dataset sizes.

**Schema Enforcement**: Provides SQL-like data structure and consistency guarantees in pure JavaScript, without the complexity of traditional databases.

**Desktop-First Architecture**: Built specifically for Electron and NW.js applications, eliminating native dependency issues that complicate desktop deployment.

**Enterprise Performance**: Combines disk persistence with in-memory query speed through compressed indexes and automatic term mapping optimization.

## Performance Benchmarks

### Memory Usage Comparison (100,000 records)

| Database | Memory Usage | Query Speed | File Size |
|----------|--------------|-------------|-----------|
| **JexiDB** | ~25MB | ~5ms (indexed) | ~15MB (compressed) |
| NeDB | ~80MB | ~20ms | ~45MB |
| Lowdb | ~120MB | ~50ms | ~60MB |
| LokiJS | ~90MB | ~8ms | ~35MB (adapters) |

*Benchmarks based on typical e-commerce product catalog with repetitive category/tag data*

### Size Reduction with Term Mapping

- **Without term mapping**: 45MB file size
- **With JexiDB term mapping**: 15MB file size
- **Reduction**: Up to 77% for datasets with repetitive strings

## Frequently Asked Questions

### Is JexiDB suitable for production applications?

Yes, JexiDB is production-ready with features like:
- Automatic data integrity recovery
- Transaction support with operation queuing
- Comprehensive error handling
- Active maintenance and updates

### How does JexiDB handle large datasets?

JexiDB uses **point reading** - it only loads the specific records needed for your query, not the entire database. Combined with streaming operations via `iterate()` and `walk()`, it can handle millions of records efficiently.

### Can JexiDB replace SQLite in my Electron app?

For many use cases, yes! JexiDB offers:
- ✅ 100% JavaScript (no native SQLite compilation issues)
- ✅ Schema enforcement like SQL tables
- ✅ Advanced queries with operators
- ✅ Better memory efficiency for large datasets
- ✅ Simpler deployment (no native binaries)

### What's the difference between fields and indexes?

**Fields** define your data structure (schema) - they're required and enforced.
**Indexes** optimize query performance - they're optional and should only be created for fields you query frequently.

### How does JexiDB compare to traditional databases?

JexiDB provides SQL-like structure and data consistency in pure JavaScript, but without the complexity of server setup or native dependencies. It's perfect for applications that need structured data without the overhead of full database systems.

## Quick Start - 5 Minutes to Database

### Installation

```bash
npm install EdenwareApps/jexidb
```

### Create Your First Database

```javascript
import { Database } from 'jexidb';

const db = new Database('users.jdb', {
  fields: {
    id: 'number',
    name: 'string',
    email: 'string',
    role: 'string',
    tags: 'array:string'
  },
  indexes: {
    email: 'string',        // Fast email lookups
    role: 'string',         // Filter by role
    tags: 'array:string'    // Search by tags
  }
});

// Initialize and use
await db.init();

// Insert data
await db.insert({
  id: 1,
  name: 'John Doe',
  email: 'john@example.com',
  role: 'admin',
  tags: ['developer', 'team-lead']
});

// Query data
const users = await db.find({ role: 'admin' });
const devs = await db.find({ tags: 'developer' });

// Save changes
await db.save();
```

**That's it!** Your data is now persisted to `users.jdb` file.

## Real-World Use Cases

### ✅ Electron Desktop Applications
```javascript
// User management in Electron app
const userDb = new Database('users.jdb', {
  fields: { id: 'number', email: 'string', profile: 'object' },
  indexes: { email: 'string' }
});

// Works offline, no server required
const user = await userDb.findOne({ email: 'user@example.com' });
```

### ✅ Local Node.js Applications
```javascript
// Configuration storage
const configDb = new Database('config.jdb', {
  fields: { key: 'string', value: 'string' },
  indexes: { key: 'string' }
});

// Persist app settings locally
await configDb.insert({ key: 'theme', value: 'dark' });
```

### ✅ Data Processing Scripts
```javascript
// Process large CSV files
const dataDb = new Database('processed.jdb', {
  fields: { id: 'number', data: 'object' }
});

// Handle millions of records efficiently
for await (const record of dataDb.iterate({ processed: false })) {
  // Process record
  record.processed = true;
  record.timestamp = Date.now();
}
await dataDb.save();
```

### ✅ NW.js Applications
```javascript
// Product catalog for desktop POS system
const productsDb = new Database('products.jdb', {
  fields: {
    sku: 'string',
    name: 'string',
    price: 'number',
    category: 'string',
    tags: 'array:string'
  },
  indexes: {
    sku: 'string',
    category: 'string',
    tags: 'array:string'
  }
});
```

## Perfect Use Cases for JexiDB

### ✅ Ideal For:
- **Desktop Applications** (Electron, NW.js, Tauri) - No native dependency issues
- **Local Node.js Applications** - Simple deployment without database servers
- **Offline-First Apps** - Works completely offline with local persistence
- **Data Processing Scripts** - Handle large datasets with streaming operations
- **Configuration Storage** - Simple key-value storage with schema validation
- **Prototyping** - Quick setup with real persistence and advanced queries

### ❌ Less Ideal For:
- **Multi-user web applications** - Use database servers instead
- **Heavy concurrent writes** - JexiDB is single-writer optimized
- **Complex relational data** - Consider traditional SQL databases
- **Browser-only applications** - Use IndexedDB/PouchDB for web

## Bulk Operations with `iterate()`

The `iterate()` method provides high-performance bulk update capabilities with streaming support:

```javascript
// Basic bulk update
for await (const product of db.iterate({ category: 'electronics' })) {
  product.price = product.price * 1.1; // 10% increase
  product.updatedAt = new Date().toISOString();
}

// With progress tracking
for await (const item of db.iterate(
  { status: 'active' },
  {
    chunkSize: 1000,
    progressCallback: (progress) => {
      console.log(`Processed: ${progress.processed}, Modified: ${progress.modified}`);
    }
  }
)) {
  item.processed = true;
}
```

**Benefits:**
- **Streaming Performance**: Process large datasets without loading everything into memory
- **Bulk Updates**: Modify multiple records in a single operation
- **Automatic Change Detection**: Automatically detects modified records
- **Progress Tracking**: Optional progress callbacks for long-running operations

## Advanced Queries

```javascript
// Multiple conditions (automatic AND)
const results = await db.find({
  age: { '>': 18, '<': 65 },
  status: 'active',
  category: { '$in': ['premium', 'vip'] }
});

// Logical operators
const complex = await db.find({
  '$or': [
    { type: 'admin' },
    { '$and': [
      { type: 'user' },
      { verified: true }
    ]}
  ]
});

// Arrays
const withTags = await db.find({
  tags: 'javascript',              // Contains 'javascript'
  tags: { '$all': ['js', 'node'] }  // Contains all values
});

// Coverage analysis with filtering
const liveCoverage = await db.coverage('tags', [
  { terms: ['javascript'], excludes: [] },
  { terms: ['react', 'vue'], excludes: ['angular'] }
], { mediaType: 'live' })  // Only analyze live content

// Multi-value filtering (OR logic)
const multiTypeCoverage = await db.coverage('tags', [
  { terms: ['tutorial'] }
], { mediaType: ['live', 'vod'] })  // Live OR VOD content
```

### Fast Partial & Regex Searches

Raw `RegExp` conditions on indexed `string` / term-mapped fields are resolved against the
index keys instead of streaming the whole data file, so partial searches stay fast:

```javascript
const results = await db.find({ nameTerms: /^glo/i })   // Indexed regex (v2.2.5+)
```

In streaming queries, indexed conditions are pre-filtered first and residual conditions such
as `$ne` / `$nin` are applied last over the reduced candidate set:

```javascript
const liveItems = await db.find({
  $and: [{ nameTerms: /news/i }, { mediaType: { $ne: 'video' } }]
})
```

> Numeric indexes still use streaming for regex conditions (regex over numbers is not
> meaningful). With `indexedQueryMode: 'strict'`, residual operators such as `$ne` require
> `allowNonIndexed: true` to pass query validation.

## Read & Write Safety

The following constructor options control recovery, memory use, and reader/writer
coordination (also listed in the [API reference](docs/API.md)):

| Option | Default | Purpose |
|--------|---------|---------|
| `readOnly` | `false` | Open for reads only: never creates, writes, rebuilds or auto-flushes |
| `updatingSentinel` | `false` | Writer mode: exposes `<file>.updating.jdb` while saving so cross-process readers can skip in-place refresh/repair |
| `allowIndexRebuild` | `false` | Rebuild a missing/corrupt index on first query (default throws) |
| `indexIdleUnloadMs` | `30000` | Unload the in-memory index after inactivity (`0` disables) |
| `ioTimeoutMs` | `0` | I/O timeout for streaming reads / index rebuilds (`0` disables) |
| `maxRetries` | `3` | Retry count for timed-out reads / index rebuilds |

Data and index saves are atomic (unique temp file + rename, with a Windows-safe fallback),
and indexed readers detect a file swap and reload offsets + index from the current `.idx`.
Atomic saves prevent half-written files and temp collisions; they do not merge simultaneous
application writes, so keep a single writer (or coordinate via `updatingSentinel`) when
multiple instances can modify the same database.

## Testes

JexiDB inclui uma suíte completa de testes com Jest:

```bash
npm test              # Executa todos os testes
npm run test:watch    # Modo watch para desenvolvimento
npm run test:coverage # Relatório de cobertura
```

## 📚 Documentation

- **[📖 Full Documentation](docs/README.md)** - Complete documentation index
- **[🔧 API Reference](docs/API.md)** - Detailed API documentation
- **[💡 Examples](docs/EXAMPLES.md)** - Practical examples and use cases
- **[🚀 Getting Started](docs/README.md#quick-start)** - Quick start guide

## Migration Guide

### Coming from Other Databases

#### From NeDB/Lowdb
```javascript
// Before (NeDB)
const Datastore = require('nedb');
const db = new Datastore({ filename: 'data.db' });

// After (JexiDB)
import { Database } from 'jexidb';
const db = new Database('data.jdb', {
  fields: { /* define your schema */ },
  indexes: { /* define indexes */ }
});
await db.init(); // Required!
```

#### From SQLite
```javascript
// SQLite requires native compilation
const sqlite3 = require('sqlite3').verbose();
const db = new sqlite3.Database('data.db');

// JexiDB - pure JavaScript, no compilation
import { Database } from 'jexidb';
const db = new Database('data.jdb', { /* config */ });
```

#### From LocalStorage/IndexedDB
```javascript
// Browser storage limitations
localStorage.setItem('data', JSON.stringify(largeDataset));

// JexiDB - true file persistence
const db = new Database('data.jdb', { /* config */ });
await db.insert(largeDataset);
await db.save();
```

## Community & Support

### Get Help
- 📖 **[Full Documentation](docs/README.md)** - Complete guides and API reference
- 💬 **[GitHub Issues](https://github.com/EdenwareApps/jexidb/issues)** - Bug reports and feature requests
- 💡 **[Examples](docs/EXAMPLES.md)** - Real-world usage patterns

### Contributing
Found a bug or want to contribute? We welcome pull requests!

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### License
MIT License - see the [LICENSE](LICENSE) file for details.

## What's Next - Roadmap

### Planned Features
- 🚀 **SQL-like Query Builder** - More intuitive query construction
- 📊 **Built-in Aggregation Functions** - Count, sum, average operations
- 🔐 **Encryption Support** - Optional data encryption at rest
- 📱 **React Native Support** - Mobile database capabilities
- 🌐 **Multi-threading** - Better concurrent operation handling
- 📈 **Advanced Analytics** - Built-in data analysis tools

### Recent Updates
- ✅ **v2.2.5** - Faster indexed regex/partial search, smarter streaming pre-filtering, sampled debug logs, and race-free concurrent atomic saves (see [CHANGELOG](CHANGELOG.md))
- ✅ **v2.2.4** - Read/write safety hardening (see [CHANGELOG](CHANGELOG.md)):
  - Atomic data-file saves (temp + rename) - readers never see a half-written file
  - Reader-side file-swap detection with clean reload on indexed reads
  - Optional writer sentinel (`updatingSentinel`) for cross-process coordination
  - I/O timeouts/aborts surface as retriable errors (no more `uncaughtException`)
  - Idle-unload recovery via on-disk index reload (no forced rebuild/throw)
  - New `readOnly` mode; single-flight lazy index loading
- ✅ **v2.1.0** - Term mapping auto-detection, 77% size reduction
- ✅ **Schema Enforcement** - Mandatory fields for data consistency
- ✅ **Streaming Operations** - Memory-efficient bulk operations
- ✅ **Compressed Indexes** - Persistent indexing with compression

---

<p align="center">
  <img width="380" src="https://edenware.app/jexidb/images/jexidb-mascot3.jpg" alt="JexiDB mascot - JavaScript EXtended Intelligent DataBase for desktop applications" title="JexiDB - JavaScript EXtended Intelligent DataBase mascot" />
</p>

## Support the Project

If JexiDB helps your project, consider supporting its development:

- ⭐ **Star on GitHub** - Show your support
- 🐛 **Report Issues** - Help improve stability
- 💝 **Donate** - [PayPal](https://www.paypal.com/donate/?item_name=megacubo.tv&cmd=_donations&business=efox.web%40gmail.com)

**The Only Pure JS Database with Smart Disk Persistence & Intelligent Memory Management**

**Built with ❤️ for the JavaScript community**

---

---

## Links & Resources

- **📦 NPM Package**: [npmjs.com/package/jexidb](https://www.npmjs.com/package/jexidb)
- **📚 Documentation**: [docs/README.md](docs/README.md)
- **💬 Issues**: [github.com/EdenwareApps/jexidb/issues](https://github.com/EdenwareApps/jexidb/issues)
