# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.1.0] - 2024-12-19

### ⚠️ BREAKING CHANGES

This version is **NOT backward compatible** with databases created with previous versions.

### 🚀 Major Features

#### **Term Mapping Auto-Detection**

- **BREAKING**: `termMapping` is now `true` by default (was `false`)
- **BREAKING**: `termMappingFields` is now auto-detected from `indexes` (was manual configuration)
- **NEW**: Automatic detection of `string` and `array:string` fields for term mapping
- **NEW**: Zero-configuration term mapping for optimal performance

#### **Schema Requirements**

- **BREAKING**: `fields` option is now **MANDATORY** (was optional)
- **NEW**: Clear distinction between `fields` (schema definition) and `indexes` (performance optimization)
- **NEW**: Enhanced schema validation and error messages

#### **Index Management**

- **BREAKING**: `array:string` fields now use term IDs in indexes (was string values)
- **BREAKING**: `array:number` fields use direct numeric values (was incorrectly term-mapped)
- **NEW**: Improved index performance for array fields
- **NEW**: Better memory usage for repetitive string data

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

- **NEW**: Comprehensive test suite for term mapping
- **NEW**: Performance benchmarks for large datasets
- **NEW**: Migration compatibility tests
- **NEW**: Edge case testing for array fields

---

## [1.1.0] - Previous Version

### Features

- Basic database functionality
- Manual term mapping configuration
- Optional schema definition
- Basic indexing support

### Limitations

- Term mapping required manual configuration
- Schema was optional, leading to confusion
- Array fields had indexing issues
- Performance not optimized for repetitive data
