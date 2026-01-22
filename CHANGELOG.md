# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.1.5] - 2026-01-20

### 🚀 **New Features**

#### **Coverage Method Filtering**

- Added optional `filterCriteria` parameter to `coverage()` method for performance-optimized filtering
- Filter criteria must use **only indexed fields** for maximum performance
- Supports single values and arrays for OR matching (e.g., `{mediaType: ['live', 'vod']}`)
- Multiple criteria are combined with AND logic
- Zero additional I/O - filtering works directly with indexes
- Maintains backward compatibility (parameter is optional)

### 🐛 **Bug Fixes**

#### **exists() Method Consistency**

- **Fixed critical bug** where `exists()` returned `false` but `find()` found records with same criteria
- Added support for full query criteria in `exists()` method (e.g., `{ field: { '!=': 'value' } }`)
- **Automatic operator detection** - complex operators now use `find()` internally for consistency
- **Performance optimization** - simple indexed queries still use ultra-fast index intersection
- **Backward compatibility** maintained for legacy syntax

### 📋 **Configuration**

#### **Node.js Engine Requirements**

- Added `engines.node: ">=16.0.0"` to package.json for clear Node.js compatibility requirements
- Ensures proper version checking by package managers and deployment tools
- Provides clear minimum version information for developers

## [2.1.0] - 2024-12-19

### 🚀 Major Features

#### **Term Mapping Auto-Detection**

- Term mapping enabled by default for optimal performance
- Automatic detection of `string` and `array:string` fields for mapping
- Zero-configuration term mapping for ideal performance

#### **Schema Requirements**

- `fields` option now mandatory for clear schema definition
- Clear distinction between `fields` (schema definition) and `indexes` (performance optimization)
- Enhanced schema validation with clear error messages

#### **Index Management**

- `array:string` fields use term IDs in indexes
- `array:number` fields use direct numeric values
- Improved performance for array fields
- Better memory usage for repetitive string data

### 🔧 Improvements

#### **Database Constructor**

- **BREAKING**: `fields` parameter is now required
- **NEW**: Auto-detection of term mapping fields
- **NEW**: Enhanced error messages for missing schema
- **NEW**: Better validation of field types

#### **Query Performance**

- **NEW**: Optimized query processing for term-mapped fields
- **NEW**: Improved `$in` operator handling for arrays
- **NEW**: Better support for mixed field types in queries

#### **File Reading Fixes**

- **FIXED**: `db.walk()` incomplete line reading causing JSON parsing errors
- **FIXED**: "Expected ',' or ']'" and "Unterminated string" errors in walk operations
- **FIXED**: `split('\n')` failing on JSON lines containing special characters or unescaped quotes
- **NEW**: Implemented `splitJsonLines()` method for proper JSON line parsing
- **NEW**: Range-based reading now handles complex JSON structures correctly
- **NEW**: `ensureCompleteLine()` method automatically expands reads to recover truncated JSON
- **IMPROVED**: No data loss when JSON lines are cut by range boundaries - automatic recovery

#### **Data Sanitization**

- **FIXED**: Control characters causing JSON parsing failures
- **FIXED**: Unescaped quotes breaking JSON structure
- **FIXED**: Newlines and carriage returns in strings causing NDJSON corruption
- **NEW**: Implemented `sanitizeDataForJSON()` method for automatic data cleaning
- **NEW**: Automatic removal of control characters (`\x00-\x1F`, `\x7F-\x9F`)
- **NEW**: Automatic escaping of quotes, backslashes, and formatting characters
- **NEW**: String length limits to prevent performance issues

#### **Documentation**

- **NEW**: Complete API documentation overhaul
- **NEW**: Practical examples with proper schema usage
- **NEW**: Performance optimization guidelines
- **NEW**: Migration guide for version 2.x

### 🐛 Bug Fixes

- Fixed `array:string` fields incorrectly using string values instead of term IDs
- Fixed `array:number` fields being incorrectly term-mapped
- Fixed term mapping not being enabled by default
- Fixed missing `termMappingFields` property on TermManager
- Fixed IndexManager not correctly identifying term mapping fields

### 📚 Documentation Updates

- **NEW**: Comprehensive API reference with examples
- **NEW**: Schema vs Indexes distinction clearly explained
- **NEW**: Performance tips and best practices
- **NEW**: Migration guide for existing users
- **NEW**: `beginInsertSession()` documentation

### 🔄 Migration Guide

#### **For Existing Users (1.x.x → 2.1.0)**

1. **Update your database initialization:**

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
       name: 'string',
       tags: 'array:string'
     }
   })
   ```
2. **Database files are NOT compatible:**

   - Existing `.jdb` files from 1.x.x will not work with 2.1.0
   - You need to export data from 1.x.x and re-import to 2.1.0
   - Consider this a fresh start for your database files
3. **Term mapping is now automatic:**

   - No need to manually configure `termMapping: true`
   - No need to specify `termMappingFields`
   - Fields are auto-detected from your `indexes` configuration

### 🎯 Performance Improvements

- **Up to 77% reduction** in database size for repetitive string data
- **Faster queries** on term-mapped fields
- **Better memory usage** for large datasets
- **Optimized indexing** for array fields

### 🧪 Testing

- Suíte abrangente de testes para mapeamento de termos
- Benchmarks de performance para grandes volumes de dados
- Testes de casos extremos para campos de array

