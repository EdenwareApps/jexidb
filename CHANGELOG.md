# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.2.5] - 2026-09-05

### ⚡ Performance — partial/regex search resolved on the index (no full-file streaming)

Searching an indexed string/term field with a raw `RegExp` (e.g. Megacubo's partial live
search `{ nameTerms: /^glo/i }`) previously **forced the streaming strategy**, scanning the
whole data file record by record — seconds per database (observed ~12s; up to ~53s with
`debugMode` on, plus console flooding).

- `IndexManager.query()` now answers raw `RegExp` conditions on **string / term-mapped**
  indexes by scanning the **index keys** instead of the data file. For term-mapped fields the
  keys are numeric term IDs, so each key is translated back to its word via
  `termManager.getTerm()` before the regex is applied; only the matching line numbers are then
  read from disk.
- `QueryManager.shouldUseStreaming()` now routes raw `RegExp` on **non-numeric** indexed
  fields to the `indexed` strategy (both the root and the `$and` paths). Numeric indexes still
  go to streaming (regex over numbers is not meaningful).

**Do not revert this to "regex ⇒ streaming".** It is the change that makes partial search fast;
reverting reintroduces a full-file scan per database for every partial term.

### ⚡ Performance — non-indexed/complement fields are filtered LAST (stream the least)

In the streaming path, `QueryManager.findWithStreaming()` pre-filters with the index down to the
**minimal** candidate set, and only then applies the remaining conditions over those candidates:

- `_getIndexableFields()` / `_extractIndexableCriteria()` now include raw `RegExp` conditions on
  **string/term** indexes (they are answerable via the index keys).
- Complement operators (`$ne`, `$nin`) are **excluded** from the index pre-filter and left as a
  **residual** condition applied last, over the few pre-filtered records.
- Rationale (Megacubo live search): `$and [ { nameTerms: /term/i }, { mediaType: $ne 'video' } ]`
  used to force a full streaming scan of every record because of `$ne`. Now it pre-filters by
  `nameTerms` on the index (often 0 rows) and only streams those rows to apply `mediaType != video`.
- Note: in `indexedQueryMode: 'strict'`, `$ne` requires `{ allowNonIndexed: true }` to reach this
  residual path (validateStrictQuery rejects unsupported operators otherwise).

### 🐛 Bug fix — RegExp no longer corrupted by operator normalization

`normalizeCriteriaOperators()` treated a `RegExp` as an operator map (`Object.entries(regex)`
is empty) and returned `{}`, which made an indexed regex match **every record**.
`IndexManager.query()` now never normalizes a `RegExp` value (a RegExp is not an operator map).

### 🐛 Bug fix — atomic save no longer fails with `ENOENT` under concurrent writers

Two `Database` instances writing the same file (e.g. metadata updated in parallel by the
Megacubo updater) used a **fixed** `<file>.tmp` name; each writer removed the other's temp right
before its rename, producing `ENOENT: rename '<file>.idx.jdb.tmp' -> ...` on `save()`/`close()`.

- `FileHandler._atomicReplaceFile()` writes to a **unique** temp name per operation
  (`<file>.tmp.<pid>.<ts>.<rand>`), so concurrent writers never delete each other's temp.
- `FileHandler._safeRename()` tolerates `ENOENT`: if the temp is gone but the target exists, a
  concurrent writer already replaced it — the write was superseded, not an error.

**Do not revert to a fixed `.tmp` name** — reverting reintroduces the cross-instance race and
the `ENOENT` failures seen in the Megacubo updater.

> Atomic replacement prevents half-written files and temporary-file collisions, but it does
> not merge simultaneous application writes. When multiple instances can modify the same
> database, still coordinate write ownership (e.g. a single writer or `updatingSentinel`).

### 🛠️ Dev — debug logs sampled (no per-record spam)

`QueryManager.matchesFieldCondition()` logged `🔍 Checking field ...` / `Checking term mapping
field ...` **for every record** during streaming when `debugMode` was enabled, flooding the
console. Logs are now emitted **once per field per query** via `options._dbFieldLog` (each
`find` builds a fresh `options` object, so the sample naturally resets per query). This is
intentional: it keeps field-level debugging available without O(records) log lines.

## [2.2.4] - 2026-09-04

### 🐛 Bug Fixes

#### **I/O timeouts/aborts no longer crash the host process**

- Fixed `Database._rebuildIndexesWithRetry()` destroying the read stream with an `AbortError` payload right before closing `readline`, which surfaced an unhandled stream `'error'` as an `uncaughtException` and could crash the whole app
- Timeouts/aborts in index rebuilds and streaming reads are now normalized (new `src/utils/ioTimeout.mjs`) and delivered as **retriable errors**, never as an uncaught exception

#### **Idle-unload no longer breaks the next query**

- After an idle index unload, `find()`/`count()` now reload the index from the on-disk `.idx` file before deciding to rebuild - instead of throwing ("Index rebuild required but disabled") or forcing a full-file rebuild with the default `allowIndexRebuild: false`

#### **`find()` no longer writes to disk**

- Removed the in-place integrity-correction write (`_saveIndexDataToFile()`) from the `find()` hot path; `totalLines` is now reconciled in memory only, so reads never rewrite the `.idx` under a concurrent writer

### 🚀 New Features

#### **Read-only mode**

- New `readOnly` option: opens an existing database for reads only and never creates, writes, rebuilds or auto-flushes

#### **Atomic data-file saves**

- Data saves now write to a temp file and atomically rename it over the target (with a Windows-safe copy fallback), so concurrent readers never observe a half-written file

#### **Reader-side swap detection**

- On indexed reads, the reader cheaply detects (stat `size`/`mtime`) when the data file was atomically replaced by another process and cleanly reloads offsets + index from the current `.idx`

#### **Writer sentinel**

- New `updatingSentinel` option: the writer creates/removes `<file>.updating.jdb` around each save; readers detect it and skip any in-place refresh/repair while the writer is active

### 🛠️ Improvements

- Made the lazy index load single-flight robust: the shared promise is now cleared in a `finally`, and a failed load degrades gracefully to streaming instead of poisoning every later attempt

## [2.1.10] - 2026-05-15

### 🚀 **New Features**

#### **Index Auto-Unload**

- Added configurable idle index unload support via `indexIdleUnloadMs`
- Index data is now freed automatically after idle timeout, reducing memory pressure for infrequently used indexes

### 🛠️ **Improvements**

- Improved shutdown stability by cancelling pending idle unload timers during `Database.close()`
- Kept lazy index unload behavior intact while making cleanup safer

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

#### **Strict Mode Operator Support**

- Added `$exists` operator support in `indexedQueryMode: 'strict'`
- Fixed compatibility issues with complex query criteria in strict mode
- Ensures all valid query operators work consistently across query methods

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

