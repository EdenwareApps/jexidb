"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = exports.Database = void 0;
var _events = require("events");
var _IndexManager = _interopRequireDefault(require("./managers/IndexManager.mjs"));
var _Serializer = _interopRequireDefault(require("./Serializer.mjs"));
var _asyncMutex = require("async-mutex");
var _fs = _interopRequireDefault(require("fs"));
var _readline = _interopRequireDefault(require("readline"));
var _OperationQueue = require("./OperationQueue.mjs");
var _FileHandler = _interopRequireDefault(require("./FileHandler.mjs"));
var _QueryManager = require("./managers/QueryManager.mjs");
var _ConcurrencyManager = require("./managers/ConcurrencyManager.mjs");
var _StatisticsManager = require("./managers/StatisticsManager.mjs");
var _StreamingProcessor = _interopRequireDefault(require("./managers/StreamingProcessor.mjs"));
var _TermManager = _interopRequireDefault(require("./managers/TermManager.mjs"));
function _interopRequireDefault(e) { return e && e.__esModule ? e : { default: e }; }
function _interopRequireWildcard(e, t) { if ("function" == typeof WeakMap) var r = new WeakMap(), n = new WeakMap(); return (_interopRequireWildcard = function (e, t) { if (!t && e && e.__esModule) return e; var o, i, f = { __proto__: null, default: e }; if (null === e || "object" != typeof e && "function" != typeof e) return f; if (o = t ? n : r) { if (o.has(e)) return o.get(e); o.set(e, f); } for (const t in e) "default" !== t && {}.hasOwnProperty.call(e, t) && ((i = (o = Object.defineProperty) && Object.getOwnPropertyDescriptor(e, t)) && (i.get || i.set) ? o(f, t, i) : f[t] = e[t]); return f; })(e, t); }
function _awaitAsyncGenerator(e) { return new _OverloadYield(e, 0); }
function _wrapAsyncGenerator(e) { return function () { return new AsyncGenerator(e.apply(this, arguments)); }; }
function AsyncGenerator(e) { var r, t; function resume(r, t) { try { var n = e[r](t), o = n.value, u = o instanceof _OverloadYield; Promise.resolve(u ? o.v : o).then(function (t) { if (u) { var i = "return" === r ? "return" : "next"; if (!o.k || t.done) return resume(i, t); t = e[i](t).value; } settle(n.done ? "return" : "normal", t); }, function (e) { resume("throw", e); }); } catch (e) { settle("throw", e); } } function settle(e, n) { switch (e) { case "return": r.resolve({ value: n, done: !0 }); break; case "throw": r.reject(n); break; default: r.resolve({ value: n, done: !1 }); } (r = r.next) ? resume(r.key, r.arg) : t = null; } this._invoke = function (e, n) { return new Promise(function (o, u) { var i = { key: e, arg: n, resolve: o, reject: u, next: null }; t ? t = t.next = i : (r = t = i, resume(e, n)); }); }, "function" != typeof e.return && (this.return = void 0); }
AsyncGenerator.prototype["function" == typeof Symbol && Symbol.asyncIterator || "@@asyncIterator"] = function () { return this; }, AsyncGenerator.prototype.next = function (e) { return this._invoke("next", e); }, AsyncGenerator.prototype.throw = function (e) { return this._invoke("throw", e); }, AsyncGenerator.prototype.return = function (e) { return this._invoke("return", e); };
function _OverloadYield(e, d) { this.v = e, this.k = d; }
function _asyncIterator(r) { var n, t, o, e = 2; for ("undefined" != typeof Symbol && (t = Symbol.asyncIterator, o = Symbol.iterator); e--;) { if (t && null != (n = r[t])) return n.call(r); if (o && null != (n = r[o])) return new AsyncFromSyncIterator(n.call(r)); t = "@@asyncIterator", o = "@@iterator"; } throw new TypeError("Object is not async iterable"); }
function AsyncFromSyncIterator(r) { function AsyncFromSyncIteratorContinuation(r) { if (Object(r) !== r) return Promise.reject(new TypeError(r + " is not an object.")); var n = r.done; return Promise.resolve(r.value).then(function (r) { return { value: r, done: n }; }); } return AsyncFromSyncIterator = function (r) { this.s = r, this.n = r.next; }, AsyncFromSyncIterator.prototype = { s: null, n: null, next: function () { return AsyncFromSyncIteratorContinuation(this.n.apply(this.s, arguments)); }, return: function (r) { var n = this.s.return; return void 0 === n ? Promise.resolve({ value: r, done: !0 }) : AsyncFromSyncIteratorContinuation(n.apply(this.s, arguments)); }, throw: function (r) { var n = this.s.return; return void 0 === n ? Promise.reject(r) : AsyncFromSyncIteratorContinuation(n.apply(this.s, arguments)); } }, new AsyncFromSyncIterator(r); }
/**
 * IterateEntry class for intuitive API with automatic change detection
 * Uses native JavaScript setters for maximum performance
 */
class IterateEntry {
  constructor(entry, originalRecord) {
    this._entry = entry;
    this._originalRecord = originalRecord;
    this._modified = false;
    this._markedForDeletion = false;
  }

  // Generic getter that returns values from the original entry
  get(property) {
    return this._entry[property];
  }

  // Generic setter that sets values in the original entry
  set(property, value) {
    this._entry[property] = value;
    this._modified = true;
  }

  // Delete method for intuitive deletion
  delete() {
    this._markedForDeletion = true;
    return true;
  }

  // Getter for the underlying entry (for compatibility)
  get value() {
    return this._entry;
  }

  // Check if entry was modified
  get isModified() {
    return this._modified;
  }

  // Check if entry is marked for deletion
  get isMarkedForDeletion() {
    return this._markedForDeletion;
  }

  // Proxy all property access to the underlying entry
  get [Symbol.toPrimitive]() {
    return this._entry;
  }

  // Handle property access dynamically
  get [Symbol.toStringTag]() {
    return 'IterateEntry';
  }
}

// Import managers

/**
 * InsertSession - Simple batch insertion without memory duplication
 */
class InsertSession {
  constructor(database, sessionOptions = {}) {
    this.database = database;
    this.batchSize = sessionOptions.batchSize || 100;
    this.totalInserted = 0;
    this.flushing = false;
    this.batches = []; // Array of batches to avoid slice() in flush()
    this.currentBatch = []; // Current batch being filled
    this.sessionId = Math.random().toString(36).substr(2, 9);

    // Register this session as active
    this.database.activeInsertSessions.add(this);
  }
  async add(record) {
    // CRITICAL FIX: Remove the committed check to allow auto-reusability
    // The session should be able to handle multiple commits

    if (this.database.destroyed) {
      throw new Error('Database is destroyed');
    }

    // Process record
    const finalRecord = {
      ...record
    };
    const id = finalRecord.id || this.database.generateId();
    finalRecord.id = id;

    // Add to current batch
    this.currentBatch.push(finalRecord);
    this.totalInserted++;

    // If batch is full, move it to batches array
    if (this.currentBatch.length >= this.batchSize) {
      this.batches.push(this.currentBatch);
      this.currentBatch = [];
    }
    return finalRecord;
  }
  async flush() {
    // Check if there's anything to flush
    if (this.batches.length === 0 && this.currentBatch.length === 0) return;

    // Prevent concurrent flushes
    if (this.flushing) return;
    this.flushing = true;
    try {
      // Process all complete batches
      for (const batch of this.batches) {
        await this.database.insertBatch(batch);
      }

      // Process remaining records in current batch
      if (this.currentBatch.length > 0) {
        await this.database.insertBatch(this.currentBatch);
      }

      // Clear all batches
      this.batches = [];
      this.currentBatch = [];
    } finally {
      this.flushing = false;
    }
  }
  async commit() {
    // CRITICAL FIX: Make session auto-reusable by removing committed state
    // Allow multiple commits on the same session

    await this.flush();

    // Reset session state for next commit cycle
    const insertedCount = this.totalInserted;
    this.totalInserted = 0;
    return insertedCount;
  }

  /**
   * Wait for this session's operations to complete
   */
  async waitForOperations(maxWaitTime = null) {
    const startTime = Date.now();
    const hasTimeout = maxWaitTime !== null && maxWaitTime !== undefined;
    while (this.flushing || this.batches.length > 0 || this.currentBatch.length > 0) {
      // Check timeout only if we have one
      if (hasTimeout && Date.now() - startTime >= maxWaitTime) {
        return false;
      }
      await new Promise(resolve => setTimeout(resolve, 1));
    }
    return true;
  }

  /**
   * Check if this session has pending operations
   */
  hasPendingOperations() {
    return this.flushing || this.batches.length > 0 || this.currentBatch.length > 0;
  }

  /**
   * Destroy this session and unregister it
   */
  destroy() {
    // Unregister from database
    this.database.activeInsertSessions.delete(this);

    // Clear all data
    this.batches = [];
    this.currentBatch = [];
    this.totalInserted = 0;
    this.flushing = false;
  }
}

/**
 * JexiDB - A high-performance, in-memory database with persistence
 * 
 * Features:
 * - In-memory storage with optional persistence
 * - Advanced indexing and querying
 * - Transaction support
 * - Manual save functionality
 * - Recovery mechanisms
 * - Performance optimizations
 */
class Database extends _events.EventEmitter {
  constructor(file, opts = {}) {
    super();

    // Generate unique instance ID for debugging
    this.instanceId = Math.random().toString(36).substr(2, 9);

    // Initialize state flags
    this.managersInitialized = false;

    // Track active insert sessions
    this.activeInsertSessions = new Set();

    // Set default options
    this.opts = Object.assign({
      // Core options - auto-save removed, user must call save() manually
      // File creation options
      create: opts.create !== false,
      // Create file if it doesn't exist (default true)
      clear: opts.clear === true,
      // Clear existing files before loading (default false)
      // Timeout configurations for preventing hangs
      mutexTimeout: opts.mutexTimeout || 15000,
      // 15 seconds timeout for mutex operations
      maxFlushAttempts: opts.maxFlushAttempts || 50,
      // Maximum flush attempts before giving up
      // Term mapping options (always enabled and auto-detected from indexes)
      termMappingCleanup: opts.termMappingCleanup !== false,
      // Clean up orphaned terms on save (enabled by default)
      // Recovery options
      enableRecovery: opts.enableRecovery === true,
      // Recovery mechanisms disabled by default for large databases
      // Buffer size options for range merging
      maxBufferSize: opts.maxBufferSize || 4 * 1024 * 1024,
      // 4MB default maximum buffer size for grouped ranges
      // Memory management options (similar to published v1.1.0)
      maxMemoryUsage: opts.maxMemoryUsage || 64 * 1024,
      // 64KB limit like published version
      maxWriteBufferSize: opts.maxWriteBufferSize || 1000,
      // Maximum records in writeBuffer
      // Query strategy options
      streamingThreshold: opts.streamingThreshold || 0.8,
      // Use streaming when limit > 80% of total records
      // Serialization options
      enableArraySerialization: opts.enableArraySerialization !== false // Enable array serialization by default
    }, opts);

    // CRITICAL FIX: Initialize AbortController for lifecycle management
    this.abortController = new AbortController();
    this.pendingOperations = new Set();
    this.pendingPromises = new Set();
    this.destroyed = false;
    this.destroying = false;
    this.closed = false;
    this.operationCounter = 0;

    // CRITICAL FIX: Initialize OperationQueue to prevent race conditions
    this.operationQueue = new _OperationQueue.OperationQueue(false); // Disable debug mode for queue

    // Normalize file path to ensure it ends with .jdb
    this.normalizedFile = this.normalizeFilePath(file);

    // Initialize core properties
    this.offsets = []; // Array of byte offsets for each record
    this.indexOffset = 0; // Current position in file for new records
    this.deletedIds = new Set(); // Track deleted record IDs
    this.shouldSave = false;
    this.isLoading = false;
    this.isSaving = false;
    this.lastSaveTime = null;
    this.initialized = false;

    // Initialize managers
    this.initializeManagers();

    // Initialize file mutex for thread safety
    this.fileMutex = new _asyncMutex.Mutex();

    // Initialize performance tracking
    this.performanceStats = {
      operations: 0,
      saves: 0,
      loads: 0,
      queryTime: 0,
      saveTime: 0,
      loadTime: 0
    };

    // Initialize usage stats for QueryManager
    this.usageStats = {
      totalQueries: 0,
      indexedQueries: 0,
      streamingQueries: 0,
      indexedAverageTime: 0,
      streamingAverageTime: 0
    };

    // Note: Validation will be done after configuration conversion in initializeManagers()
  }

  /**
   * Validate field and index configuration
   */
  validateIndexConfiguration() {
    // Validate fields configuration
    if (this.opts.fields && typeof this.opts.fields === 'object') {
      this.validateFieldTypes(this.opts.fields, 'fields');
    }

    // Validate indexes configuration (legacy support)
    if (this.opts.indexes && typeof this.opts.indexes === 'object') {
      this.validateFieldTypes(this.opts.indexes, 'indexes');
    }

    // Validate indexes array (new format) - but only if we have fields
    if (this.opts.originalIndexes && Array.isArray(this.opts.originalIndexes)) {
      if (!this.opts.fields) {
        throw new Error('Index fields array requires fields configuration. Use: { fields: {...}, indexes: [...] }');
      }
      this.validateIndexFields(this.opts.originalIndexes);
    }
    if (this.opts.debugMode) {
      const fieldCount = this.opts.fields ? Object.keys(this.opts.fields).length : 0;
      const indexCount = Array.isArray(this.opts.indexes) ? this.opts.indexes.length : this.opts.indexes && typeof this.opts.indexes === 'object' ? Object.keys(this.opts.indexes).length : 0;
      if (fieldCount > 0 || indexCount > 0) {
        console.log(`✅ Configuration validated: ${fieldCount} fields, ${indexCount} indexes [${this.instanceId}]`);
      }
    }
  }

  /**
   * Validate field types
   */
  validateFieldTypes(fields, configType) {
    const supportedTypes = ['string', 'number', 'boolean', 'array:string', 'array:number', 'array:boolean', 'array', 'object'];
    const errors = [];
    for (const [fieldName, fieldType] of Object.entries(fields)) {
      // Check if type is supported
      if (!supportedTypes.includes(fieldType)) {
        errors.push(`Unsupported ${configType} type '${fieldType}' for field '${fieldName}'. Supported types: ${supportedTypes.join(', ')}`);
      }

      // Warn about legacy array type but don't error
      if (fieldType === 'array') {
        if (this.opts.debugMode) {
          console.log(`⚠️  Legacy array type '${fieldType}' for field '${fieldName}'. Consider using 'array:string' for better performance.`);
        }
      }

      // Check for common mistakes
      if (fieldType === 'array:') {
        errors.push(`Incomplete array type '${fieldType}' for field '${fieldName}'. Must specify element type after colon: array:string, array:number, or array:boolean`);
      }
    }
    if (errors.length > 0) {
      throw new Error(`${configType.charAt(0).toUpperCase() + configType.slice(1)} configuration errors:\n${errors.map(e => `  - ${e}`).join('\n')}`);
    }
  }

  /**
   * Validate index fields array
   */
  validateIndexFields(indexFields) {
    if (!this.opts.fields) {
      throw new Error('Index fields array requires fields configuration. Use: { fields: {...}, indexes: [...] }');
    }
    const availableFields = Object.keys(this.opts.fields);
    const errors = [];
    for (const fieldName of indexFields) {
      if (!availableFields.includes(fieldName)) {
        errors.push(`Index field '${fieldName}' not found in fields configuration. Available fields: ${availableFields.join(', ')}`);
      }
    }
    if (errors.length > 0) {
      throw new Error(`Index configuration errors:\n${errors.map(e => `  - ${e}`).join('\n')}`);
    }
  }

  /**
   * Prepare index configuration for IndexManager
   */
  prepareIndexConfiguration() {
    // Convert new fields/indexes format to legacy format for IndexManager
    if (this.opts.fields && Array.isArray(this.opts.indexes)) {
      // New format: { fields: {...}, indexes: [...] }
      const indexedFields = {};
      const originalIndexes = [...this.opts.indexes]; // Keep original for validation

      for (const fieldName of this.opts.indexes) {
        if (this.opts.fields[fieldName]) {
          indexedFields[fieldName] = this.opts.fields[fieldName];
        }
      }

      // Store original indexes for validation
      this.opts.originalIndexes = originalIndexes;

      // Replace indexes array with object for IndexManager
      this.opts.indexes = indexedFields;
      if (this.opts.debugMode) {
        console.log(`🔍 Converted fields/indexes format: ${Object.keys(indexedFields).join(', ')} [${this.instanceId}]`);
      }
    }
    // Legacy format (indexes as object) is already compatible
  }

  /**
   * Initialize all managers
   */
  initializeManagers() {
    // CRITICAL FIX: Prevent double initialization which corrupts term mappings
    if (this.managersInitialized) {
      if (this.opts.debugMode) {
        console.log(`⚠️  initializeManagers() called again - skipping to prevent corruption [${this.instanceId}]`);
      }
      return;
    }

    // CRITICAL FIX: Initialize serializer first - this was missing and causing crashes
    this.serializer = new _Serializer.default(this.opts);

    // Initialize schema for array-based serialization
    if (this.opts.enableArraySerialization !== false) {
      this.initializeSchema();
    }

    // Initialize TermManager - always enabled for optimal performance
    this.termManager = new _TermManager.default();

    // Auto-detect term mapping fields from indexes
    const termMappingFields = this.getTermMappingFields();
    this.termManager.termMappingFields = termMappingFields;
    this.opts.termMapping = true; // Always enable term mapping for optimal performance

    if (this.opts.debugMode) {
      if (termMappingFields.length > 0) {
        console.log(`🔍 TermManager initialized for fields: ${termMappingFields.join(', ')} [${this.instanceId}]`);
      } else {
        console.log(`🔍 TermManager initialized (no array:string fields detected) [${this.instanceId}]`);
      }
    }

    // Prepare index configuration for IndexManager
    this.prepareIndexConfiguration();

    // Validate configuration after conversion
    this.validateIndexConfiguration();

    // Initialize IndexManager with database reference for term mapping
    this.indexManager = new _IndexManager.default(this.opts, null, this);
    if (this.opts.debugMode) {
      console.log(`🔍 IndexManager initialized with fields: ${this.indexManager.indexedFields.join(', ')} [${this.instanceId}]`);
    }

    // Mark managers as initialized
    this.managersInitialized = true;
    this.indexOffset = 0;
    this.writeBuffer = [];
    this.writeBufferOffsets = []; // Track offsets for writeBuffer records
    this.writeBufferSizes = []; // Track sizes for writeBuffer records
    this.isInsideOperationQueue = false; // Flag to prevent deadlock in save() calls

    // Initialize other managers
    this.fileHandler = new _FileHandler.default(this.normalizedFile, this.fileMutex, this.opts);
    this.queryManager = new _QueryManager.QueryManager(this);
    this.concurrencyManager = new _ConcurrencyManager.ConcurrencyManager(this.opts);
    this.statisticsManager = new _StatisticsManager.StatisticsManager(this, this.opts);
    this.streamingProcessor = new _StreamingProcessor.default(this.opts);
  }

  /**
   * Get term mapping fields from indexes (auto-detected)
   * @returns {string[]} Array of field names that use term mapping
   */
  getTermMappingFields() {
    if (!this.opts.indexes) return [];

    // Auto-detect fields that benefit from term mapping
    const termMappingFields = [];
    for (const [field, type] of Object.entries(this.opts.indexes)) {
      // Fields that should use term mapping
      if (type === 'array:string' || type === 'string') {
        termMappingFields.push(field);
      }
    }
    return termMappingFields;
  }

  /**
   * CRITICAL FIX: Validate database state before critical operations
   * Prevents crashes from undefined methods and invalid states
   */
  validateState() {
    if (this.destroyed) {
      throw new Error('Database is destroyed');
    }
    if (this.closed) {
      throw new Error('Database is closed. Call init() to reopen it.');
    }

    // Allow operations during destroying phase for proper cleanup

    if (!this.serializer) {
      throw new Error('Database serializer not initialized - this indicates a critical bug');
    }
    if (!this.normalizedFile) {
      throw new Error('Database file path not set - this indicates file path management failure');
    }
    if (!this.fileHandler) {
      throw new Error('Database file handler not initialized');
    }
    if (!this.indexManager) {
      throw new Error('Database index manager not initialized');
    }
    return true;
  }

  /**
   * CRITICAL FIX: Ensure file path is valid and accessible
   * Prevents file path loss issues mentioned in crash report
   */
  ensureFilePath() {
    if (!this.normalizedFile) {
      throw new Error('Database file path is missing after initialization - this indicates a critical file path management failure');
    }
    return this.normalizedFile;
  }

  /**
   * Normalize file path to ensure it ends with .jdb
   */
  normalizeFilePath(file) {
    if (!file) return null;
    return file.endsWith('.jdb') ? file : `${file}.jdb`;
  }

  /**
   * Initialize the database
   */
  async initialize() {
    // Check if database is destroyed first (before checking initialized)
    if (this.destroyed) {
      throw new Error('Cannot initialize destroyed database. Use a new instance instead.');
    }
    if (this.initialized) return;

    // Prevent concurrent initialization - wait for ongoing init to complete
    if (this.isLoading) {
      if (this.opts.debugMode) {
        console.log('🔄 init() already in progress - waiting for completion');
      }
      // Wait for ongoing initialization to complete
      while (this.isLoading) {
        await new Promise(resolve => setTimeout(resolve, 10));
      }
      // Check if initialization completed successfully
      if (this.initialized) {
        if (this.opts.debugMode) {
          console.log('✅ Concurrent init() completed - database is now initialized');
        }
        return;
      }
      // If we get here, initialization failed - we can try again
    }
    try {
      this.isLoading = true;

      // Reset closed state when reinitializing
      this.closed = false;

      // Initialize managers (protected against double initialization)
      this.initializeManagers();

      // Handle clear option - delete existing files before loading
      if (this.opts.clear && this.normalizedFile) {
        await this.clearExistingFiles();
      }

      // Check file existence and handle create option
      if (this.normalizedFile) {
        const fileExists = await this.fileHandler.exists();
        if (!fileExists) {
          if (!this.opts.create) {
            throw new Error(`Database file '${this.normalizedFile}' does not exist and create option is disabled`);
          }
          // File will be created when first data is written
        } else {
          // Load existing data if file exists
          await this.load();
        }
      }

      // Manual save is now the default behavior

      this.initialized = true;
      this.emit('initialized');
      if (this.opts.debugMode) {
        console.log(`✅ Database initialized with ${this.writeBuffer.length} records`);
      }
    } catch (error) {
      console.error('Failed to initialize database:', error);
      throw error;
    } finally {
      this.isLoading = false;
    }
  }

  /**
   * Validate that the database is initialized before performing operations
   * @param {string} operation - The operation being attempted
   * @throws {Error} If database is not initialized
   */
  _validateInitialization(operation) {
    if (this.destroyed) {
      throw new Error(`❌ Cannot perform '${operation}' on a destroyed database. Create a new instance instead.`);
    }
    if (this.closed) {
      throw new Error(`❌ Database is closed. Call 'await db.init()' to reopen it before performing '${operation}' operations.`);
    }
    if (!this.initialized) {
      const errorMessage = `❌ Database not initialized. Call 'await db.init()' before performing '${operation}' operations.\n\n` + `Example:\n` + `  const db = new Database('./myfile.jdb')\n` + `  await db.init()  // ← Required before any operations\n` + `  await db.insert({ name: 'test' })  // ← Now you can use database operations\n\n` + `File: ${this.normalizedFile || 'unknown'}`;
      throw new Error(errorMessage);
    }
  }

  /**
   * Clear existing database files (.jdb and .idx.jdb)
   */
  async clearExistingFiles() {
    if (!this.normalizedFile) return;
    try {
      // Clear main database file
      if (await this.fileHandler.exists()) {
        await this.fileHandler.delete();
        if (this.opts.debugMode) {
          console.log(`🗑️ Cleared database file: ${this.normalizedFile}`);
        }
      }

      // Clear index file
      const idxPath = this.normalizedFile.replace('.jdb', '.idx.jdb');
      const idxFileHandler = new _FileHandler.default(idxPath, this.fileMutex, this.opts);
      if (await idxFileHandler.exists()) {
        await idxFileHandler.delete();
        if (this.opts.debugMode) {
          console.log(`🗑️ Cleared index file: ${idxPath}`);
        }
      }

      // Reset internal state
      this.offsets = [];
      this.indexOffset = 0;
      this.deletedIds.clear();
      this.shouldSave = false;

      // Create empty files to ensure they exist
      await this.fileHandler.writeAll('');
      await idxFileHandler.writeAll('');
      if (this.opts.debugMode) {
        console.log('🗑️ Database cleared successfully');
      }
    } catch (error) {
      console.error('Failed to clear existing files:', error);
      throw error;
    }
  }

  /**
   * Load data from file
   */
  async load() {
    if (!this.normalizedFile) return;
    try {
      const startTime = Date.now();
      this.isLoading = true;

      // Don't load the entire file - just initialize empty state
      // The actual record count will come from loaded offsets
      this.writeBuffer = []; // writeBuffer is only for new unsaved records

      // recordCount will be determined from loaded offsets
      // If no offsets were loaded, we'll count records only if needed

      // Load index data if available (always try to load offsets, even without indexed fields)
      if (this.indexManager) {
        const idxPath = this.normalizedFile.replace('.jdb', '.idx.jdb');
        try {
          const idxFileHandler = new _FileHandler.default(idxPath, this.fileMutex, this.opts);
          const idxData = await idxFileHandler.readAll();
          if (idxData && idxData.trim()) {
            const parsedIdxData = JSON.parse(idxData);

            // Always load offsets if available (even without indexed fields)
            if (parsedIdxData.offsets && Array.isArray(parsedIdxData.offsets)) {
              this.offsets = parsedIdxData.offsets;
              if (this.opts.debugMode) {
                console.log(`📂 Loaded ${this.offsets.length} offsets from ${idxPath}`);
              }
            }

            // Load indexOffset for proper range calculations
            if (parsedIdxData.indexOffset !== undefined) {
              this.indexOffset = parsedIdxData.indexOffset;
              if (this.opts.debugMode) {
                console.log(`📂 Loaded indexOffset: ${this.indexOffset} from ${idxPath}`);
              }
            }

            // Load index data only if available and we have indexed fields
            if (parsedIdxData && parsedIdxData.index && this.indexManager.indexedFields && this.indexManager.indexedFields.length > 0) {
              this.indexManager.load(parsedIdxData.index);

              // Load term mapping data from .idx file if it exists
              if (parsedIdxData.termMapping && this.termManager) {
                await this.termManager.loadTerms(parsedIdxData.termMapping);
                if (this.opts.debugMode) {
                  console.log(`📂 Loaded term mapping from ${idxPath}`);
                }
              }
              if (this.opts.debugMode) {
                console.log(`📂 Loaded index data from ${idxPath}`);
              }
            }

            // Load configuration from .idx file if database exists
            if (parsedIdxData.config) {
              const config = parsedIdxData.config;

              // Override constructor options with saved configuration
              if (config.fields) {
                this.opts.fields = config.fields;
                if (this.opts.debugMode) {
                  console.log(`📂 Loaded fields config from ${idxPath}:`, Object.keys(config.fields));
                }
              }
              if (config.indexes) {
                this.opts.indexes = config.indexes;
                if (this.opts.debugMode) {
                  console.log(`📂 Loaded indexes config from ${idxPath}:`, Object.keys(config.indexes));
                }
              }
              if (config.originalIndexes) {
                this.opts.originalIndexes = config.originalIndexes;
                if (this.opts.debugMode) {
                  console.log(`📂 Loaded originalIndexes config from ${idxPath}:`, config.originalIndexes.length, 'indexes');
                }
              }

              // Reinitialize schema from saved configuration
              if (config.schema && this.serializer) {
                this.serializer.initializeSchema(config.schema);
                if (this.opts.debugMode) {
                  console.log(`📂 Loaded schema from ${idxPath}:`, config.schema.join(', '));
                }
              }
            }
          }
        } catch (idxError) {
          // Index file doesn't exist or is corrupted, rebuild from data
          if (this.opts.debugMode) {
            console.log('📂 No index file found, rebuilding indexes from data');
          }
          // We can't rebuild index without violating no-memory-storage rule
          // Index will be rebuilt as needed during queries
        }
      } else {
        // No indexed fields, no need to rebuild indexes
      }
      this.performanceStats.loads++;
      this.performanceStats.loadTime += Date.now() - startTime;
      this.emit('loaded', this.writeBuffer.length);
    } catch (error) {
      console.error('Failed to load database:', error);
      throw error;
    } finally {
      this.isLoading = false;
    }
  }

  /**
   * Save data to file
   * @param {boolean} inQueue - Whether to execute within the operation queue (default: false)
   */
  async save(inQueue = false) {
    this._validateInitialization('save');
    if (this.opts.debugMode) {
      console.log(`💾 save() called: writeBuffer.length=${this.writeBuffer.length}, offsets.length=${this.offsets.length}`);
    }

    // Auto-save removed - no need to pause anything

    try {
      // CRITICAL FIX: Wait for any ongoing save operations to complete
      if (this.isSaving) {
        if (this.opts.debugMode) {
          console.log('💾 save(): waiting for previous save to complete');
        }
        // Wait for previous save to complete
        while (this.isSaving) {
          await new Promise(resolve => setTimeout(resolve, 10));
        }

        // Check if data changed since the previous save completed
        const hasDataToSave = this.writeBuffer.length > 0 || this.deletedIds.size > 0;
        const needsStructureCreation = this.indexManager && this.indexManager.indexedFields && this.indexManager.indexedFields.length > 0;
        if (!hasDataToSave && !needsStructureCreation) {
          if (this.opts.debugMode) {
            console.log('💾 Save: No new data to save since previous save completed');
          }
          return; // Nothing new to save
        }
      }

      // CRITICAL FIX: Check if there's actually data to save before proceeding
      // But allow save if we need to create database structure (index files, etc.)
      const hasDataToSave = this.writeBuffer.length > 0 || this.deletedIds.size > 0;
      const needsStructureCreation = this.indexManager && this.indexManager.indexedFields && this.indexManager.indexedFields.length > 0;
      if (!hasDataToSave && !needsStructureCreation) {
        if (this.opts.debugMode) {
          console.log('💾 Save: No data to save (writeBuffer empty and no deleted records)');
        }
        return; // Nothing to save
      }
      if (inQueue) {
        if (this.opts.debugMode) {
          console.log(`💾 save(): executing in queue`);
        }
        return this.operationQueue.enqueue(async () => {
          return this._doSave();
        });
      } else {
        if (this.opts.debugMode) {
          console.log(`💾 save(): calling _doSave() directly`);
        }
        return this._doSave();
      }
    } finally {
      // Auto-save removed - no need to resume anything
    }
  }

  /**
   * Internal save implementation (without queue)
   */
  async _doSave() {
    // CRITICAL FIX: Check if database is destroyed
    if (this.destroyed) return;

    // CRITICAL FIX: Use atomic check-and-set to prevent concurrent save operations
    if (this.isSaving) {
      if (this.opts.debugMode) {
        console.log('💾 _doSave: Save operation already in progress, skipping');
      }
      return;
    }

    // CRITICAL FIX: Check if there's actually data to save or structure to create
    const hasDataToSave = this.writeBuffer.length > 0 || this.deletedIds.size > 0;
    const needsStructureCreation = this.indexManager && this.indexManager.indexedFields && this.indexManager.indexedFields.length > 0;
    if (!hasDataToSave && !needsStructureCreation) {
      if (this.opts.debugMode) {
        console.log('💾 _doSave: No data to save (writeBuffer empty and no deleted records)');
      }
      return; // Nothing to save
    }

    // CRITICAL FIX: Set saving flag immediately to prevent race conditions
    this.isSaving = true;
    try {
      const startTime = Date.now();

      // CRITICAL FIX: Ensure file path is valid
      this.ensureFilePath();

      // CRITICAL FIX: Wait for ALL pending operations to complete before save
      await this._waitForPendingOperations();

      // CRITICAL FIX: Capture writeBuffer and deletedIds at the start to prevent race conditions
      const writeBufferSnapshot = [...this.writeBuffer];
      const deletedIdsSnapshot = new Set(this.deletedIds);

      // OPTIMIZATION: Process pending index updates in batch before save
      if (this.pendingIndexUpdates && this.pendingIndexUpdates.length > 0) {
        if (this.opts.debugMode) {
          console.log(`💾 Save: Processing ${this.pendingIndexUpdates.length} pending index updates in batch`);
        }

        // Extract records and line numbers for batch processing
        const records = this.pendingIndexUpdates.map(update => update.record);
        const startLineNumber = this.pendingIndexUpdates[0].lineNumber;

        // Process index updates in batch
        await this.indexManager.addBatch(records, startLineNumber);

        // Clear pending updates
        this.pendingIndexUpdates = [];
      }

      // CRITICAL FIX: Flush write buffer completely after capturing snapshot
      await this._flushWriteBufferCompletely();

      // CRITICAL FIX: Wait for all I/O operations to complete before clearing writeBuffer
      await this._waitForIOCompletion();

      // CRITICAL FIX: Verify write buffer is empty after I/O completion
      // But allow for ongoing insertions during high-volume scenarios
      if (this.writeBuffer.length > 0) {
        if (this.opts.debugMode) {
          console.log(`💾 Save: WriteBuffer still has ${this.writeBuffer.length} items after flush - this may indicate ongoing insertions`);
        }

        // If we have a reasonable number of items, continue processing
        if (this.writeBuffer.length < 10000) {
          // Reasonable threshold
          if (this.opts.debugMode) {
            console.log(`💾 Save: Continuing to process remaining ${this.writeBuffer.length} items`);
          }
          // Continue with the save process - the remaining items will be included in the final save
        } else {
          // Too many items remaining - likely a real problem
          throw new Error(`WriteBuffer has too many items after flush: ${this.writeBuffer.length} items remaining (threshold: 10000)`);
        }
      }

      // OPTIMIZATION: Parallel operations - cleanup and data preparation
      let allData = [];
      let orphanedCount = 0;

      // Check if there are new records to save (after flush, writeBuffer should be empty)
      if (this.opts.debugMode) {
        console.log(`💾 Save: writeBuffer.length=${this.writeBuffer.length}, writeBufferSnapshot.length=${writeBufferSnapshot.length}`);
      }
      if (this.writeBuffer.length > 0) {
        if (this.opts.debugMode) {
          console.log(`💾 Save: WriteBuffer has ${writeBufferSnapshot.length} records, using streaming approach`);
        }

        // Note: processTermMapping is already called during insert/update operations
        // No need to call it again here to avoid double processing

        // OPTIMIZATION: Check if we can skip reading existing records
        // Only use streaming if we have existing records AND we're not just appending new records
        const hasExistingRecords = this.indexOffset > 0 && this.offsets.length > 0 && writeBufferSnapshot.length > 0;
        if (!hasExistingRecords && deletedIdsSnapshot.size === 0) {
          // OPTIMIZATION: No existing records to read, just use writeBuffer
          allData = [...writeBufferSnapshot];
        } else {
          // OPTIMIZATION: Parallel operations - cleanup and streaming
          const parallelOperations = [];

          // Add term cleanup if enabled
          if (this.opts.termMappingCleanup && this.termManager) {
            parallelOperations.push(Promise.resolve().then(() => {
              orphanedCount = this.termManager.cleanupOrphanedTerms();
              if (this.opts.debugMode && orphanedCount > 0) {
                console.log(`🧹 Cleaned up ${orphanedCount} orphaned terms`);
              }
            }));
          }

          // Add streaming operation
          parallelOperations.push(this._streamExistingRecords(deletedIdsSnapshot, writeBufferSnapshot).then(existingRecords => {
            allData = [...existingRecords];

            // OPTIMIZATION: Use Map for faster lookups
            const existingRecordMap = new Map(existingRecords.filter(r => r && r.id).map(r => [r.id, r]));
            for (const record of writeBufferSnapshot) {
              if (!deletedIdsSnapshot.has(record.id)) {
                if (existingRecordMap.has(record.id)) {
                  // Replace existing record
                  const existingIndex = allData.findIndex(r => r.id === record.id);
                  allData[existingIndex] = record;
                } else {
                  // Add new record
                  allData.push(record);
                }
              }
            }
          }));

          // Execute parallel operations
          await Promise.all(parallelOperations);
        }
      } else {
        // CRITICAL FIX: When writeBuffer is empty, use streaming approach for existing records
        if (this.opts.debugMode) {
          console.log(`💾 Save: Checking streaming condition: indexOffset=${this.indexOffset}, deletedIds.size=${this.deletedIds.size}`);
          console.log(`💾 Save: writeBuffer.length=${this.writeBuffer.length}`);
        }
        if (this.indexOffset > 0 || this.deletedIds.size > 0) {
          try {
            if (this.opts.debugMode) {
              console.log(`💾 Save: Using streaming approach for existing records`);
              console.log(`💾 Save: indexOffset: ${this.indexOffset}, offsets.length: ${this.offsets.length}`);
              console.log(`💾 Save: deletedIds to filter:`, Array.from(deletedIdsSnapshot));
            }

            // OPTIMIZATION: Parallel operations - cleanup and streaming
            const parallelOperations = [];

            // Add term cleanup if enabled
            if (this.opts.termMappingCleanup && this.termManager) {
              parallelOperations.push(Promise.resolve().then(() => {
                orphanedCount = this.termManager.cleanupOrphanedTerms();
                if (this.opts.debugMode && orphanedCount > 0) {
                  console.log(`🧹 Cleaned up ${orphanedCount} orphaned terms`);
                }
              }));
            }

            // Add streaming operation
            parallelOperations.push(this._streamExistingRecords(deletedIdsSnapshot, writeBufferSnapshot).then(existingRecords => {
              if (this.opts.debugMode) {
                console.log(`💾 Save: _streamExistingRecords returned ${existingRecords.length} records`);
                console.log(`💾 Save: existingRecords:`, existingRecords);
              }
              // Combine existing records with new records from writeBuffer
              allData = [...existingRecords, ...writeBufferSnapshot.filter(record => !deletedIdsSnapshot.has(record.id))];
            }).catch(error => {
              if (this.opts.debugMode) {
                console.log(`💾 Save: _streamExistingRecords failed:`, error.message);
              }
              // CRITICAL FIX: Use safe fallback to preserve existing data instead of losing it
              return this._loadExistingRecordsFallback(deletedIdsSnapshot, writeBufferSnapshot).then(fallbackRecords => {
                allData = [...fallbackRecords, ...writeBufferSnapshot.filter(record => !deletedIdsSnapshot.has(record.id))];
                if (this.opts.debugMode) {
                  console.log(`💾 Save: Fallback preserved ${fallbackRecords.length} existing records, total: ${allData.length}`);
                }
              }).catch(fallbackError => {
                if (this.opts.debugMode) {
                  console.log(`💾 Save: All fallback methods failed:`, fallbackError.message);
                  console.log(`💾 Save: CRITICAL - Data loss may occur, only writeBuffer will be saved`);
                }
                // Last resort: at least save what we have in writeBuffer
                allData = writeBufferSnapshot.filter(record => !deletedIdsSnapshot.has(record.id));
              });
            }));

            // Execute parallel operations
            await Promise.all(parallelOperations);
          } catch (error) {
            if (this.opts.debugMode) {
              console.log(`💾 Save: Streaming approach failed, falling back to writeBuffer only: ${error.message}`);
            }
            // CRITICAL FIX: Use safe fallback to preserve existing data instead of losing it
            try {
              const fallbackRecords = await this._loadExistingRecordsFallback(deletedIdsSnapshot, writeBufferSnapshot);
              allData = [...fallbackRecords, ...writeBufferSnapshot.filter(record => !deletedIdsSnapshot.has(record.id))];
              if (this.opts.debugMode) {
                console.log(`💾 Save: Fallback preserved ${fallbackRecords.length} existing records, total: ${allData.length}`);
              }
            } catch (fallbackError) {
              if (this.opts.debugMode) {
                console.log(`💾 Save: All fallback methods failed:`, fallbackError.message);
                console.log(`💾 Save: CRITICAL - Data loss may occur, only writeBuffer will be saved`);
              }
              // Last resort: at least save what we have in writeBuffer
              allData = writeBufferSnapshot.filter(record => !deletedIdsSnapshot.has(record.id));
            }
          }
        } else {
          // No existing data, use only writeBuffer
          allData = writeBufferSnapshot.filter(record => !deletedIdsSnapshot.has(record.id));
        }
      }

      // CRITICAL FIX: Calculate offsets based on actual serialized data that will be written
      // This ensures consistency between offset calculation and file writing
      const jsonlData = allData.length > 0 ? this.serializer.serializeBatch(allData) : '';
      const jsonlString = jsonlData.toString('utf8');
      const lines = jsonlString.split('\n').filter(line => line.trim());
      this.offsets = [];
      let currentOffset = 0;
      for (let i = 0; i < lines.length; i++) {
        this.offsets.push(currentOffset);
        // CRITICAL FIX: Use actual line length including newline for accurate offset calculation
        // This accounts for UTF-8 encoding differences (e.g., 'ação' vs 'acao')
        const lineWithNewline = lines[i] + '\n';
        currentOffset += Buffer.byteLength(lineWithNewline, 'utf8');
      }

      // CRITICAL FIX: Ensure indexOffset matches actual file size
      this.indexOffset = currentOffset;
      if (this.opts.debugMode) {
        console.log(`💾 Save: Calculated indexOffset: ${this.indexOffset}, allData.length: ${allData.length}`);
      }

      // OPTIMIZATION: Parallel operations - file writing and index data preparation
      const parallelWriteOperations = [];

      // Add main file write operation
      parallelWriteOperations.push(this.fileHandler.writeBatch([jsonlData]));

      // Add index file operations - ALWAYS save offsets, even without indexed fields
      if (this.indexManager) {
        const idxPath = this.normalizedFile.replace('.jdb', '.idx.jdb');

        // OPTIMIZATION: Parallel data preparation
        const indexDataPromise = Promise.resolve({
          index: this.indexManager.indexedFields && this.indexManager.indexedFields.length > 0 ? this.indexManager.toJSON() : {},
          offsets: this.offsets,
          // Save actual offsets for efficient file operations
          indexOffset: this.indexOffset // Save file size for proper range calculations
        });

        // Add term mapping data if needed
        const termMappingFields = this.getTermMappingFields();
        if (termMappingFields.length > 0 && this.termManager) {
          const termDataPromise = this.termManager.saveTerms();

          // Combine index data and term data
          const combinedDataPromise = Promise.all([indexDataPromise, termDataPromise]).then(([indexData, termData]) => {
            indexData.termMapping = termData;
            return indexData;
          });

          // Add index file write operation
          parallelWriteOperations.push(combinedDataPromise.then(indexData => {
            const idxFileHandler = new _FileHandler.default(idxPath, this.fileMutex, this.opts);
            return idxFileHandler.writeAll(JSON.stringify(indexData, null, 2));
          }));
        } else {
          // Add index file write operation without term mapping
          parallelWriteOperations.push(indexDataPromise.then(indexData => {
            const idxFileHandler = new _FileHandler.default(idxPath, this.fileMutex, this.opts);
            return idxFileHandler.writeAll(JSON.stringify(indexData, null, 2));
          }));
        }
      }

      // Execute parallel write operations
      await Promise.all(parallelWriteOperations);
      if (this.opts.debugMode) {
        console.log(`💾 Saved ${allData.length} records to ${this.normalizedFile}`);
      }

      // CRITICAL FIX: Invalidate file size cache after save operation
      this._cachedFileStats = null;
      this.shouldSave = false;
      this.lastSaveTime = Date.now();

      // Clear writeBuffer and deletedIds after successful save only if we had data to save
      if (allData.length > 0) {
        // Rebuild index when records were deleted to maintain consistency
        const hadDeletedRecords = deletedIdsSnapshot.size > 0;
        if (this.indexManager && this.indexManager.indexedFields && this.indexManager.indexedFields.length > 0) {
          if (hadDeletedRecords) {
            // Clear the index and rebuild it from the remaining records
            this.indexManager.clear();
            if (this.opts.debugMode) {
              console.log(`🧹 Rebuilding index after removing ${deletedIdsSnapshot.size} deleted records`);
            }

            // Rebuild index from the saved records
            for (let i = 0; i < allData.length; i++) {
              const record = allData[i];
              await this.indexManager.add(record, i);
            }
          }
        }

        // CRITICAL FIX: Clear all records that were in the snapshot
        // Use a more robust comparison that handles different data types
        const originalLength = this.writeBuffer.length;
        this.writeBuffer = this.writeBuffer.filter(record => {
          // For objects with id, compare by id
          if (typeof record === 'object' && record !== null && record.id) {
            return !writeBufferSnapshot.some(snapshotRecord => typeof snapshotRecord === 'object' && snapshotRecord !== null && snapshotRecord.id && snapshotRecord.id === record.id);
          }
          // For other types (Buffers, primitives), use strict equality
          return !writeBufferSnapshot.some(snapshotRecord => snapshotRecord === record);
        });

        // Remove only the deleted IDs that were in the snapshot
        for (const deletedId of deletedIdsSnapshot) {
          this.deletedIds.delete(deletedId);
        }

        // CRITICAL FIX: Ensure writeBuffer is completely cleared after successful save
        if (this.writeBuffer.length > 0) {
          if (this.opts.debugMode) {
            console.log(`💾 Save: Force clearing remaining ${this.writeBuffer.length} items from writeBuffer`);
          }
          // If there are still items in writeBuffer after filtering, clear them
          // This prevents the "writeBuffer has records" bug in destroy()
          this.writeBuffer = [];
          this.writeBufferOffsets = [];
          this.writeBufferSizes = [];
        }

        // indexOffset already set correctly to currentOffset (total file size) above
        // No need to override it with record count
      }

      // CRITICAL FIX: Always save index data to file after saving records
      await this._saveIndexDataToFile();
      this.performanceStats.saves++;
      this.performanceStats.saveTime += Date.now() - startTime;
      this.emit('saved', this.writeBuffer.length);
    } catch (error) {
      console.error('Failed to save database:', error);
      throw error;
    } finally {
      this.isSaving = false;
    }
  }

  /**
   * Process term mapping for a record
   * @param {Object} record - Record to process
   * @param {boolean} isUpdate - Whether this is an update operation
   * @param {Object} oldRecord - Original record (for updates)
   */
  processTermMapping(record, isUpdate = false, oldRecord = null) {
    const termMappingFields = this.getTermMappingFields();
    if (!this.termManager || termMappingFields.length === 0) {
      return;
    }

    // CRITICAL FIX: Don't modify the original record object
    // The record should already be a copy created in insert/update methods
    // This prevents reference modification issues

    // Process each term mapping field
    for (const field of termMappingFields) {
      if (record[field] && Array.isArray(record[field])) {
        // Decrement old terms if this is an update
        if (isUpdate && oldRecord) {
          // Check if oldRecord has term IDs or terms
          const termIdsField = `${field}Ids`;
          if (oldRecord[termIdsField] && Array.isArray(oldRecord[termIdsField])) {
            // Use term IDs directly for decrementing
            for (const termId of oldRecord[termIdsField]) {
              this.termManager.decrementTermCount(termId);
            }
          } else if (oldRecord[field] && Array.isArray(oldRecord[field])) {
            // Use terms to decrement (fallback for backward compatibility)
            for (const term of oldRecord[field]) {
              const termId = this.termManager.termToId.get(term);
              if (termId) {
                this.termManager.decrementTermCount(termId);
              }
            }
          }
        }

        // Clear old term IDs if this is an update
        if (isUpdate) {
          delete record[`${field}Ids`];
        }

        // Process new terms - getTermId already increments the count
        const termIds = [];
        for (const term of record[field]) {
          const termId = this.termManager.getTermId(term);
          termIds.push(termId);
        }
        // Store term IDs in the record (for internal use)
        record[`${field}Ids`] = termIds;
      }
    }
  }

  /**
   * Convert terms to term IDs for serialization (SPACE OPTIMIZATION)
   * @param {Object} record - Record to process
   * @returns {Object} - Record with terms converted to term IDs
   */
  removeTermIdsForSerialization(record) {
    const termMappingFields = this.getTermMappingFields();
    if (termMappingFields.length === 0 || !this.termManager) {
      return record;
    }

    // Create a copy and convert terms to term IDs
    const optimizedRecord = {
      ...record
    };
    for (const field of termMappingFields) {
      if (optimizedRecord[field] && Array.isArray(optimizedRecord[field])) {
        // CRITICAL FIX: Only convert if values are strings (terms), skip if already numbers (term IDs)
        const firstValue = optimizedRecord[field][0];
        if (typeof firstValue === 'string') {
          // Convert terms to term IDs for storage
          optimizedRecord[field] = optimizedRecord[field].map(term => this.termManager.getTermIdWithoutIncrement(term));
        }
        // If already numbers (term IDs), leave as-is
      }
    }
    return optimizedRecord;
  }

  /**
   * Convert term IDs back to terms after deserialization (SPACE OPTIMIZATION)
   * @param {Object} record - Record with term IDs
   * @returns {Object} - Record with terms restored
   */
  restoreTermIdsAfterDeserialization(record) {
    const termMappingFields = this.getTermMappingFields();
    if (termMappingFields.length === 0 || !this.termManager) {
      return record;
    }

    // Create a copy and convert term IDs back to terms
    const restoredRecord = {
      ...record
    };
    for (const field of termMappingFields) {
      if (restoredRecord[field] && Array.isArray(restoredRecord[field])) {
        // Convert term IDs back to terms for user
        restoredRecord[field] = restoredRecord[field].map(termId => {
          const term = this.termManager.idToTerm.get(termId) || termId;
          return term;
        });
      }

      // Remove the *Ids field that was added during serialization
      const idsFieldName = field + 'Ids';
      if (restoredRecord[idsFieldName]) {
        delete restoredRecord[idsFieldName];
      }
    }
    return restoredRecord;
  }

  /**
   * Remove term mapping for a record
   * @param {Object} record - Record to process
   */
  removeTermMapping(record) {
    const termMappingFields = this.getTermMappingFields();
    if (!this.termManager || termMappingFields.length === 0) {
      return;
    }

    // Process each term mapping field
    for (const field of termMappingFields) {
      // Use terms to decrement (term IDs are not stored in records anymore)
      if (record[field] && Array.isArray(record[field])) {
        for (const term of record[field]) {
          const termId = this.termManager.termToId.get(term);
          if (termId) {
            this.termManager.decrementTermCount(termId);
          }
        }
      }
    }
  }

  /**
   * Process term mapping for multiple records in batch (OPTIMIZATION)
   * @param {Array} records - Records to process
   * @returns {Array} - Processed records with term mappings
   */
  processTermMappingBatch(records) {
    const termMappingFields = this.getTermMappingFields();
    if (!this.termManager || termMappingFields.length === 0 || !records.length) {
      return records;
    }

    // OPTIMIZATION: Pre-collect all unique terms to minimize Map operations
    const allTerms = new Set();
    const fieldTerms = new Map(); // field -> Set of terms

    for (const field of termMappingFields) {
      fieldTerms.set(field, new Set());
      for (const record of records) {
        if (record[field] && Array.isArray(record[field])) {
          for (const term of record[field]) {
            allTerms.add(term);
            fieldTerms.get(field).add(term);
          }
        }
      }
    }

    // OPTIMIZATION: Batch process all terms at once using bulk operations
    const termIdMap = new Map();
    if (this.termManager.bulkGetTermIds) {
      // Use bulk operation if available
      const allTermsArray = Array.from(allTerms);
      const termIds = this.termManager.bulkGetTermIds(allTermsArray);
      for (let i = 0; i < allTermsArray.length; i++) {
        termIdMap.set(allTermsArray[i], termIds[i]);
      }
    } else {
      // Fallback to individual operations
      for (const term of allTerms) {
        termIdMap.set(term, this.termManager.getTermId(term));
      }
    }

    // OPTIMIZATION: Process records using pre-computed term IDs
    return records.map(record => {
      const processedRecord = {
        ...record
      };
      for (const field of termMappingFields) {
        if (record[field] && Array.isArray(record[field])) {
          const termIds = record[field].map(term => termIdMap.get(term));
          processedRecord[`${field}Ids`] = termIds;
        }
      }
      return processedRecord;
    });
  }

  /**
   * Calculate total size of serialized records (OPTIMIZATION)
   * @param {Array} records - Records to calculate size for
   * @returns {number} - Total size in bytes
   */
  calculateBatchSize(records) {
    if (!records || !records.length) return 0;
    let totalSize = 0;
    for (const record of records) {
      // OPTIMIZATION: Calculate size without creating the actual string
      // SPACE OPTIMIZATION: Remove term IDs before size calculation
      const cleanRecord = this.removeTermIdsForSerialization(record);
      const jsonString = this.serializer.serialize(cleanRecord).toString('utf8');
      totalSize += Buffer.byteLength(jsonString, 'utf8') + 1; // +1 for newline
    }
    return totalSize;
  }

  /**
   * Begin an insert session for batch operations
   * @param {Object} sessionOptions - Options for the insert session
   * @returns {InsertSession} - The insert session instance
   */
  beginInsertSession(sessionOptions = {}) {
    if (this.destroyed) {
      throw new Error('Database is destroyed');
    }
    if (this.closed) {
      throw new Error('Database is closed. Call init() to reopen it.');
    }
    return new InsertSession(this, sessionOptions);
  }

  /**
   * Insert a new record
   */
  async insert(data) {
    this._validateInitialization('insert');
    return this.operationQueue.enqueue(async () => {
      this.isInsideOperationQueue = true;
      try {
        // CRITICAL FIX: Validate state before insert operation
        this.validateState();
        if (!data || typeof data !== 'object') {
          throw new Error('Data must be an object');
        }

        // CRITICAL FIX: Check abort signal before operation, but allow during destroy cleanup
        if (this.abortController.signal.aborted && !this.destroying) {
          throw new Error('Database is destroyed');
        }

        // Initialize schema if not already done (auto-detect from first record)
        if (this.serializer && !this.serializer.schemaManager.isInitialized) {
          this.serializer.initializeSchema(data, true);
          if (this.opts.debugMode) {
            console.log(`🔍 Schema auto-detected from first insert: ${this.serializer.getSchema().join(', ')} [${this.instanceId}]`);
          }
        }

        // OPTIMIZATION: Process single insert with deferred index updates
        // CRITICAL FIX: Clone the object to prevent reference modification
        const clonedData = {
          ...data
        };
        const id = clonedData.id || this.generateId();
        const record = {
          ...data,
          id
        };

        // OPTIMIZATION: Process term mapping
        this.processTermMapping(record);
        if (this.opts.debugMode) {
          // console.log(`💾 insert(): writeBuffer(before)=${this.writeBuffer.length}`)
        }

        // Apply schema enforcement - convert to array format and back to enforce schema
        const schemaEnforcedRecord = this.applySchemaEnforcement(record);

        // Don't store in this.data - only use writeBuffer and index
        this.writeBuffer.push(schemaEnforcedRecord);
        if (this.opts.debugMode) {
          console.log(`🔍 INSERT: Added record to writeBuffer, length now: ${this.writeBuffer.length}`);
        }

        // OPTIMIZATION: Calculate and store offset and size for writeBuffer record
        // SPACE OPTIMIZATION: Remove term IDs before serialization
        const cleanRecord = this.removeTermIdsForSerialization(record);
        const recordJson = this.serializer.serialize(cleanRecord).toString('utf8');
        const recordSize = Buffer.byteLength(recordJson, 'utf8');

        // Calculate offset based on end of file + previous writeBuffer sizes
        const previousWriteBufferSize = this.writeBufferSizes.reduce((sum, size) => sum + size, 0);
        const recordOffset = this.indexOffset + previousWriteBufferSize;
        this.writeBufferOffsets.push(recordOffset);
        this.writeBufferSizes.push(recordSize);

        // OPTIMIZATION: Use the current writeBuffer size as the line number (0-based index)
        const lineNumber = this.writeBuffer.length - 1;

        // OPTIMIZATION: Defer index updates to batch processing
        // Store the record for batch index processing
        if (!this.pendingIndexUpdates) {
          this.pendingIndexUpdates = [];
        }
        this.pendingIndexUpdates.push({
          record,
          lineNumber
        });

        // Manual save is now the responsibility of the application
        this.shouldSave = true;
        this.performanceStats.operations++;

        // Auto-save manager removed - manual save required

        this.emit('inserted', record);
        return record;
      } finally {
        this.isInsideOperationQueue = false;
      }
    });
  }

  /**
   * Insert multiple records in batch (OPTIMIZATION)
   */
  async insertBatch(dataArray) {
    this._validateInitialization('insertBatch');

    // If we're already inside the operation queue (e.g., from insert()), avoid re-enqueueing to prevent deadlocks
    if (this.isInsideOperationQueue) {
      if (this.opts.debugMode) {
        console.log(`💾 insertBatch inline: insideQueue=${this.isInsideOperationQueue}, size=${Array.isArray(dataArray) ? dataArray.length : 0}`);
      }
      return await this._insertBatchInternal(dataArray);
    }
    return this.operationQueue.enqueue(async () => {
      this.isInsideOperationQueue = true;
      try {
        if (this.opts.debugMode) {
          console.log(`💾 insertBatch enqueued: size=${Array.isArray(dataArray) ? dataArray.length : 0}`);
        }
        return await this._insertBatchInternal(dataArray);
      } finally {
        this.isInsideOperationQueue = false;
      }
    });
  }

  /**
   * Internal implementation for insertBatch to allow inline execution when already inside the queue
   */
  async _insertBatchInternal(dataArray) {
    // CRITICAL FIX: Validate state before insert operation
    this.validateState();
    if (!Array.isArray(dataArray) || dataArray.length === 0) {
      throw new Error('DataArray must be a non-empty array');
    }

    // CRITICAL FIX: Check abort signal before operation, but allow during destroy cleanup
    if (this.abortController.signal.aborted && !this.destroying) {
      throw new Error('Database is destroyed');
    }
    if (this.opts.debugMode) {
      console.log(`💾 _insertBatchInternal: processing size=${dataArray.length}, startWriteBuffer=${this.writeBuffer.length}`);
    }
    const records = [];
    const startLineNumber = this.writeBuffer.length;

    // Initialize schema if not already done (auto-detect from first record)
    if (this.serializer && !this.serializer.schemaManager.isInitialized && dataArray.length > 0) {
      this.serializer.initializeSchema(dataArray[0], true);
      if (this.opts.debugMode) {
        console.log(`🔍 Schema auto-detected from first batch insert: ${this.serializer.getSchema().join(', ')} [${this.instanceId}]`);
      }
    }

    // OPTIMIZATION: Process all records in batch
    for (let i = 0; i < dataArray.length; i++) {
      const data = dataArray[i];
      if (!data || typeof data !== 'object') {
        throw new Error(`Data at index ${i} must be an object`);
      }
      const id = data.id || this.generateId();
      const record = {
        ...data,
        id
      };
      records.push(record);
    }

    // OPTIMIZATION: Batch process term mapping
    const processedRecords = this.processTermMappingBatch(records);

    // Apply schema enforcement to all records
    const schemaEnforcedRecords = processedRecords.map(record => this.applySchemaEnforcement(record));

    // OPTIMIZATION: Add all records to writeBuffer at once
    this.writeBuffer.push(...schemaEnforcedRecords);

    // OPTIMIZATION: Calculate offsets and sizes in batch (O(n))
    let runningTotalSize = this.writeBufferSizes.reduce((sum, size) => sum + size, 0);
    for (let i = 0; i < processedRecords.length; i++) {
      const record = processedRecords[i];
      // SPACE OPTIMIZATION: Remove term IDs before serialization
      const cleanRecord = this.removeTermIdsForSerialization(record);
      const recordJson = this.serializer.serialize(cleanRecord).toString('utf8');
      const recordSize = Buffer.byteLength(recordJson, 'utf8');
      const recordOffset = this.indexOffset + runningTotalSize;
      runningTotalSize += recordSize;
      this.writeBufferOffsets.push(recordOffset);
      this.writeBufferSizes.push(recordSize);
    }

    // OPTIMIZATION: Batch process index updates
    if (!this.pendingIndexUpdates) {
      this.pendingIndexUpdates = [];
    }
    for (let i = 0; i < processedRecords.length; i++) {
      const lineNumber = startLineNumber + i;
      this.pendingIndexUpdates.push({
        record: processedRecords[i],
        lineNumber
      });
    }
    this.shouldSave = true;
    this.performanceStats.operations += processedRecords.length;

    // Emit events for all records
    if (this.listenerCount('inserted') > 0) {
      for (const record of processedRecords) {
        this.emit('inserted', record);
      }
    }
    if (this.opts.debugMode) {
      console.log(`💾 _insertBatchInternal: done. added=${processedRecords.length}, writeBuffer=${this.writeBuffer.length}`);
    }
    return processedRecords;
  }

  /**
   * Find records matching criteria
   */
  async find(criteria = {}, options = {}) {
    this._validateInitialization('find');

    // CRITICAL FIX: Validate state before find operation
    this.validateState();

    // OPTIMIZATION: Find searches writeBuffer directly

    const startTime = Date.now();
    if (this.opts.debugMode) {
      console.log(`🔍 FIND START: criteria=${JSON.stringify(criteria)}, writeBuffer=${this.writeBuffer.length}`);
    }
    try {
      // Validate indexed query mode if enabled
      if (this.opts.indexedQueryMode === 'strict') {
        this._validateIndexedQuery(criteria);
      }

      // Get results from file (QueryManager already handles term ID restoration)
      const fileResultsWithTerms = await this.queryManager.find(criteria, options);

      // Get results from writeBuffer
      const allPendingRecords = [...this.writeBuffer];
      const writeBufferResults = this.queryManager.matchesCriteria ? allPendingRecords.filter(record => this.queryManager.matchesCriteria(record, criteria, options)) : allPendingRecords;

      // SPACE OPTIMIZATION: Restore term IDs to terms for writeBuffer results (unless disabled)
      const writeBufferResultsWithTerms = options.restoreTerms !== false ? writeBufferResults.map(record => this.restoreTermIdsAfterDeserialization(record)) : writeBufferResults;

      // Combine results, removing duplicates (writeBuffer takes precedence)
      // OPTIMIZATION: Use parallel processing for better performance when writeBuffer has many records
      let allResults;
      if (writeBufferResults.length > 50) {
        // Parallel approach for large writeBuffer
        const [fileResultsSet, writeBufferSet] = await Promise.all([Promise.resolve(new Set(fileResultsWithTerms.map(r => r.id))), Promise.resolve(new Set(writeBufferResultsWithTerms.map(r => r.id)))]);

        // Merge efficiently: keep file results not in writeBuffer, then add all writeBuffer results
        const filteredFileResults = await Promise.resolve(fileResultsWithTerms.filter(r => !writeBufferSet.has(r.id)));
        allResults = [...filteredFileResults, ...writeBufferResultsWithTerms];
      } else {
        // Sequential approach for small writeBuffer (original logic)
        allResults = [...fileResultsWithTerms];

        // Replace file records with writeBuffer records and add new writeBuffer records
        for (const record of writeBufferResultsWithTerms) {
          const existingIndex = allResults.findIndex(r => r.id === record.id);
          if (existingIndex !== -1) {
            // Replace existing record with writeBuffer version
            allResults[existingIndex] = record;
          } else {
            // Add new record from writeBuffer
            allResults.push(record);
          }
        }
      }

      // Remove records that are marked as deleted
      const finalResults = allResults.filter(record => !this.deletedIds.has(record.id));
      if (this.opts.debugMode) {
        console.log(`🔍 Database.find returning: ${finalResults?.length || 0} records (${fileResultsWithTerms.length} from file, ${writeBufferResults.length} from writeBuffer, ${this.deletedIds.size} deleted), type: ${typeof finalResults}, isArray: ${Array.isArray(finalResults)}`);
      }
      this.performanceStats.queryTime += Date.now() - startTime;
      return finalResults;
    } catch (error) {
      // Don't log expected errors in strict mode or for array field validation
      if (this.opts.indexedQueryMode !== 'strict' || !error.message.includes('Strict indexed mode')) {
        // Don't log errors for array field validation as they are expected
        if (!error.message.includes('Invalid query for array field')) {
          console.error('Query failed:', error);
        }
      }
      throw error;
    }
  }

  /**
   * Validate indexed query mode for strict mode
   * @private
   */
  _validateIndexedQuery(criteria) {
    if (!criteria || typeof criteria !== 'object') {
      return; // Allow null/undefined criteria
    }
    const indexedFields = Object.keys(this.opts.indexes || {});
    if (indexedFields.length === 0) {
      return; // No indexed fields, allow all queries
    }
    const queryFields = this._extractQueryFields(criteria);
    const nonIndexedFields = queryFields.filter(field => !indexedFields.includes(field));
    if (nonIndexedFields.length > 0) {
      const availableFields = indexedFields.length > 0 ? indexedFields.join(', ') : 'none';
      if (nonIndexedFields.length === 1) {
        throw new Error(`Strict indexed mode: Field '${nonIndexedFields[0]}' is not indexed. Available indexed fields: ${availableFields}`);
      } else {
        throw new Error(`Strict indexed mode: Fields '${nonIndexedFields.join("', '")}' are not indexed. Available indexed fields: ${availableFields}`);
      }
    }
  }

  /**
   * Create a shallow copy of a record for change detection
   * Optimized for known field types: number, string, null, or single-level arrays
   * @private
   */
  _createShallowCopy(record) {
    const copy = {};
    // Use for...in loop for better performance
    for (const key in record) {
      const value = record[key];
      // Optimize for common types first
      if (value === null || typeof value === 'number' || typeof value === 'string' || typeof value === 'boolean') {
        copy[key] = value;
      } else if (Array.isArray(value)) {
        // Only copy if array has elements and is not empty
        copy[key] = value.length > 0 ? value.slice() : [];
      } else if (typeof value === 'object') {
        // For complex objects, use shallow copy
        copy[key] = {
          ...value
        };
      } else {
        copy[key] = value;
      }
    }
    return copy;
  }

  /**
   * Create an intuitive API wrapper using a class with Proxy
   * Combines the benefits of classes with the flexibility of Proxy
   * @private
   */
  _createEntryProxy(entry, originalRecord) {
    // Create a class instance that wraps the entry
    const iterateEntry = new IterateEntry(entry, originalRecord);

    // Create a lightweight proxy that only intercepts property access
    return new Proxy(iterateEntry, {
      get(target, property) {
        // Handle special methods
        if (property === 'delete') {
          return () => target.delete();
        }
        if (property === 'value') {
          return target.value;
        }
        if (property === 'isModified') {
          return target.isModified;
        }
        if (property === 'isMarkedForDeletion') {
          return target.isMarkedForDeletion;
        }

        // For all other properties, return from the underlying entry
        return target._entry[property];
      },
      set(target, property, value) {
        // Set the value in the underlying entry
        target._entry[property] = value;
        target._modified = true;
        return true;
      }
    });
  }

  /**
   * Create a high-performance wrapper for maximum speed
   * @private
   */
  _createHighPerformanceWrapper(entry, originalRecord) {
    // Create a simple wrapper object for high performance
    const wrapper = {
      value: entry,
      delete: () => {
        entry._markedForDeletion = true;
        return true;
      }
    };

    // Mark for change tracking
    entry._modified = false;
    entry._markedForDeletion = false;
    return wrapper;
  }

  /**
   * Check if a record has changed using optimized comparison
   * Optimized for known field types: number, string, null, or single-level arrays
   * @private
   */
  _hasRecordChanged(current, original) {
    // Quick reference check first
    if (current === original) return false;

    // Compare each field - optimized for common types
    for (const key in current) {
      const currentValue = current[key];
      const originalValue = original[key];

      // Quick reference check (most common case)
      if (currentValue === originalValue) continue;

      // Handle null values
      if (currentValue === null || originalValue === null) {
        if (currentValue !== originalValue) return true;
        continue;
      }

      // Handle primitive types (number, string, boolean) - most common
      const currentType = typeof currentValue;
      if (currentType === 'number' || currentType === 'string' || currentType === 'boolean') {
        if (currentType !== typeof originalValue || currentValue !== originalValue) return true;
        continue;
      }

      // Handle arrays (single-level) - second most common
      if (Array.isArray(currentValue)) {
        if (!Array.isArray(originalValue) || currentValue.length !== originalValue.length) return true;

        // Fast array comparison for primitive types
        for (let i = 0; i < currentValue.length; i++) {
          if (currentValue[i] !== originalValue[i]) return true;
        }
        continue;
      }

      // Handle objects (shallow comparison only) - least common
      if (currentType === 'object') {
        if (typeof originalValue !== 'object') return true;

        // Fast object comparison using for...in
        for (const objKey in currentValue) {
          if (currentValue[objKey] !== originalValue[objKey]) return true;
        }
        // Check if original has extra keys
        for (const objKey in originalValue) {
          if (!(objKey in currentValue)) return true;
        }
        continue;
      }

      // Fallback for other types
      if (currentValue !== originalValue) return true;
    }

    // Check if original has extra keys (only if we haven't found differences yet)
    for (const key in original) {
      if (!(key in current)) return true;
    }
    return false;
  }

  /**
   * Extract field names from query criteria
   * @private
   */
  _extractQueryFields(criteria) {
    const fields = new Set();
    const extractFromObject = obj => {
      for (const [key, value] of Object.entries(obj)) {
        if (key.startsWith('$')) {
          // Handle logical operators
          if (Array.isArray(value)) {
            value.forEach(item => {
              if (typeof item === 'object' && item !== null) {
                extractFromObject(item);
              }
            });
          } else if (typeof value === 'object' && value !== null) {
            extractFromObject(value);
          }
        } else {
          // Regular field
          fields.add(key);
        }
      }
    };
    extractFromObject(criteria);
    return Array.from(fields);
  }

  /**
   * Update records matching criteria
   */
  async update(criteria, updateData) {
    this._validateInitialization('update');
    return this.operationQueue.enqueue(async () => {
      this.isInsideOperationQueue = true;
      try {
        const startTime = Date.now();
        if (this.opts.debugMode) {
          console.log(`🔄 UPDATE START: criteria=${JSON.stringify(criteria)}, updateData=${JSON.stringify(updateData)}`);
        }

        // CRITICAL FIX: Validate state before update operation
        this.validateState();

        // CRITICAL FIX: If there's data to save, call save() to persist it
        // Only save if there are actual records in writeBuffer
        if (this.shouldSave && this.writeBuffer.length > 0) {
          if (this.opts.debugMode) {
            console.log(`🔄 UPDATE: Calling save() before update - writeBuffer.length=${this.writeBuffer.length}`);
          }
          const saveStart = Date.now();
          await this.save(false); // Use save(false) since we're already in queue
          if (this.opts.debugMode) {
            console.log(`🔄 UPDATE: Save completed in ${Date.now() - saveStart}ms`);
          }
        }
        if (this.opts.debugMode) {
          console.log(`🔄 UPDATE: Starting find() - writeBuffer=${this.writeBuffer.length}`);
        }
        const findStart = Date.now();
        // CRITICAL FIX: Get raw records without term restoration for update operations
        const records = await this.find(criteria, {
          restoreTerms: false
        });
        if (this.opts.debugMode) {
          console.log(`🔄 UPDATE: Find completed in ${Date.now() - findStart}ms, found ${records.length} records`);
        }
        const updatedRecords = [];
        for (const record of records) {
          const recordStart = Date.now();
          if (this.opts.debugMode) {
            console.log(`🔄 UPDATE: Processing record ${record.id}`);
          }
          const updated = {
            ...record,
            ...updateData
          };

          // Process term mapping for update
          const termMappingStart = Date.now();
          this.processTermMapping(updated, true, record);
          if (this.opts.debugMode) {
            console.log(`🔄 UPDATE: Term mapping completed in ${Date.now() - termMappingStart}ms`);
          }

          // CRITICAL FIX: Remove old terms from index before adding new ones
          if (this.indexManager) {
            await this.indexManager.remove(record);
            if (this.opts.debugMode) {
              console.log(`🔄 UPDATE: Removed old terms from index for record ${record.id}`);
            }
          }

          // Update record in writeBuffer or add to writeBuffer if not present
          const index = this.writeBuffer.findIndex(r => r.id === record.id);
          let lineNumber = null;
          if (index !== -1) {
            // Record is already in writeBuffer, update it
            this.writeBuffer[index] = updated;
            lineNumber = index;
            if (this.opts.debugMode) {
              console.log(`🔄 UPDATE: Updated existing writeBuffer record at index ${index}`);
            }
          } else {
            // Record is in file, add updated version to writeBuffer
            // This will ensure the updated record is saved and replaces the file version
            this.writeBuffer.push(updated);
            lineNumber = this.writeBuffer.length - 1;
            if (this.opts.debugMode) {
              console.log(`🔄 UPDATE: Added new record to writeBuffer at index ${lineNumber}`);
            }
          }
          const indexUpdateStart = Date.now();
          await this.indexManager.update(record, updated, lineNumber);
          if (this.opts.debugMode) {
            console.log(`🔄 UPDATE: Index update completed in ${Date.now() - indexUpdateStart}ms`);
          }
          updatedRecords.push(updated);
          if (this.opts.debugMode) {
            console.log(`🔄 UPDATE: Record ${record.id} completed in ${Date.now() - recordStart}ms`);
          }
        }
        this.shouldSave = true;
        this.performanceStats.operations++;
        if (this.opts.debugMode) {
          console.log(`🔄 UPDATE COMPLETED: ${updatedRecords.length} records updated in ${Date.now() - startTime}ms`);
        }
        this.emit('updated', updatedRecords);
        return updatedRecords;
      } finally {
        this.isInsideOperationQueue = false;
      }
    });
  }

  /**
   * Delete records matching criteria
   */
  async delete(criteria) {
    this._validateInitialization('delete');
    return this.operationQueue.enqueue(async () => {
      this.isInsideOperationQueue = true;
      try {
        // CRITICAL FIX: Validate state before delete operation
        this.validateState();
        const records = await this.find(criteria);
        const deletedIds = [];
        if (this.opts.debugMode) {
          console.log(`🗑️ Delete operation: found ${records.length} records to delete`);
          console.log(`🗑️ Records to delete:`, records.map(r => ({
            id: r.id,
            name: r.name
          })));
          console.log(`🗑️ Current writeBuffer length: ${this.writeBuffer.length}`);
          console.log(`🗑️ Current deletedIds:`, Array.from(this.deletedIds));
        }
        for (const record of records) {
          // Remove term mapping
          this.removeTermMapping(record);
          await this.indexManager.remove(record);

          // Remove record from writeBuffer or mark as deleted
          const index = this.writeBuffer.findIndex(r => r.id === record.id);
          if (index !== -1) {
            this.writeBuffer.splice(index, 1);
            if (this.opts.debugMode) {
              console.log(`🗑️ Removed record ${record.id} from writeBuffer`);
            }
          } else {
            // If record is not in writeBuffer (was saved), mark it as deleted
            this.deletedIds.add(record.id);
            if (this.opts.debugMode) {
              console.log(`🗑️ Marked record ${record.id} as deleted (not in writeBuffer)`);
            }
          }
          deletedIds.push(record.id);
        }
        if (this.opts.debugMode) {
          console.log(`🗑️ After delete: writeBuffer length: ${this.writeBuffer.length}, deletedIds:`, Array.from(this.deletedIds));
        }
        this.shouldSave = true;
        this.performanceStats.operations++;
        this.emit('deleted', deletedIds);
        return deletedIds;
      } finally {
        this.isInsideOperationQueue = false;
      }
    });
  }

  /**
   * Generate a unique ID
   */
  generateId() {
    return Date.now().toString(36) + Math.random().toString(36).substr(2);
  }

  /**
   * Apply schema enforcement to a record
   * Converts object to array and back to enforce schema (remove extra fields, add undefined for missing fields)
   */
  applySchemaEnforcement(record) {
    // Only apply schema enforcement if fields configuration is explicitly provided
    if (!this.opts.fields) {
      return record; // No schema enforcement without explicit fields configuration
    }
    if (!this.serializer || !this.serializer.schemaManager || !this.serializer.schemaManager.isInitialized) {
      return record; // No schema enforcement if schema not initialized
    }

    // Convert to array format (enforces schema)
    const arrayFormat = this.serializer.convertToArrayFormat(record);

    // Convert back to object (only schema fields will be present)
    const enforcedRecord = this.serializer.convertFromArrayFormat(arrayFormat);

    // Preserve the ID if it exists
    if (record.id) {
      enforcedRecord.id = record.id;
    }
    return enforcedRecord;
  }

  /**
   * Initialize schema for array-based serialization
   */
  initializeSchema() {
    if (!this.serializer || !this.serializer.schemaManager) {
      return;
    }

    // Try to get schema from options first
    if (this.opts.schema && Array.isArray(this.opts.schema)) {
      this.serializer.initializeSchema(this.opts.schema);
      if (this.opts.debugMode) {
        console.log(`🔍 Schema initialized from options: ${this.opts.schema.join(', ')} [${this.instanceId}]`);
      }
      return;
    }

    // Try to initialize from fields configuration (new format)
    if (this.opts.fields && typeof this.opts.fields === 'object') {
      const fieldNames = Object.keys(this.opts.fields);
      if (fieldNames.length > 0) {
        this.serializer.initializeSchema(fieldNames);
        if (this.opts.debugMode) {
          console.log(`🔍 Schema initialized from fields: ${fieldNames.join(', ')} [${this.instanceId}]`);
        }
        return;
      }
    }

    // Try to auto-detect schema from existing data
    if (this.data && this.data.length > 0) {
      this.serializer.initializeSchema(this.data, true); // autoDetect = true
      if (this.opts.debugMode) {
        console.log(`🔍 Schema auto-detected from data: ${this.serializer.getSchema().join(', ')} [${this.instanceId}]`);
      }
      return;
    }

    // CRITICAL FIX: Don't initialize schema from indexes
    // This was causing data loss because only indexed fields were preserved
    // Let schema be auto-detected from actual data instead

    if (this.opts.debugMode) {
      console.log(`🔍 No schema initialization possible - will auto-detect on first insert [${this.instanceId}]`);
    }
  }

  /**
   * Get database length (number of records)
   */
  get length() {
    // Return total records: writeBuffer + saved records
    // writeBuffer contains unsaved records
    // For saved records, use the length of offsets array (number of saved records)
    const savedRecords = this.offsets.length;
    const writeBufferRecords = this.writeBuffer.length;

    // CRITICAL FIX: Validate that offsets array is consistent with actual data
    // This prevents the bug where database reassignment causes desynchronization
    if (this.initialized && savedRecords > 0) {
      try {
        // Check if the offsets array is consistent with the actual file
        // If offsets exist but file is empty or corrupted, reset offsets
        if (this.fileHandler && this.fileHandler.file) {
          try {
            // Use synchronous file stats to check if file is empty
            const stats = _fs.default.statSync(this.fileHandler.file);
            if (stats && stats.size === 0 && savedRecords > 0) {
              // File is empty but offsets array has records - this is the bug condition
              if (this.opts.debugMode) {
                console.log(`🔧 LENGTH FIX: Detected desynchronized offsets (${savedRecords} records) with empty file, resetting offsets`);
              }
              this.offsets = [];
              return writeBufferRecords; // Return only writeBuffer records
            }
          } catch (fileError) {
            // File doesn't exist or can't be read - reset offsets
            if (savedRecords > 0) {
              if (this.opts.debugMode) {
                console.log(`🔧 LENGTH FIX: File doesn't exist but offsets array has ${savedRecords} records, resetting offsets`);
              }
              this.offsets = [];
              return writeBufferRecords;
            }
          }
        }
      } catch (error) {
        // If we can't validate, fall back to the original behavior
        if (this.opts.debugMode) {
          console.log(`🔧 LENGTH FIX: Could not validate offsets, using original calculation: ${error.message}`);
        }
      }
    }
    return writeBufferRecords + savedRecords;
  }

  /**
   * Calculate current writeBuffer size in bytes (similar to published v1.1.0)
   */
  currentWriteBufferSize() {
    if (!this.writeBuffer || this.writeBuffer.length === 0) {
      return 0;
    }

    // Calculate total size of all records in writeBuffer
    let totalSize = 0;
    for (const record of this.writeBuffer) {
      if (record) {
        // SPACE OPTIMIZATION: Remove term IDs before size calculation
        const cleanRecord = this.removeTermIdsForSerialization(record);
        const recordJson = JSON.stringify(cleanRecord) + '\n';
        totalSize += Buffer.byteLength(recordJson, 'utf8');
      }
    }
    return totalSize;
  }

  /**
   * Get database statistics
   */
  getStats() {
    const stats = {
      records: this.writeBuffer.length,
      writeBufferSize: this.currentWriteBufferSize(),
      maxMemoryUsage: this.opts.maxMemoryUsage,
      performance: this.performanceStats,
      lastSave: this.lastSaveTime,
      shouldSave: this.shouldSave,
      initialized: this.initialized
    };

    // Add term mapping stats if enabled
    if (this.opts.termMapping && this.termManager) {
      stats.termMapping = this.termManager.getStats();
    }
    return stats;
  }

  /**
   * Initialize database (alias for initialize for backward compatibility)
   */
  async init() {
    return this.initialize();
  }

  /**
   * Destroy database - DESTRUCTIVE MODE
   * Assumes save() has already been called by user
   * If anything is still active, it indicates a bug - log error and force cleanup
   */
  async destroy() {
    if (this.destroyed) return;

    // Mark as destroying immediately to prevent new operations
    this.destroying = true;

    // Wait for all active insert sessions to complete before destroying
    if (this.activeInsertSessions.size > 0) {
      if (this.opts.debugMode) {
        console.log(`⏳ destroy: Waiting for ${this.activeInsertSessions.size} active insert sessions`);
      }
      const sessionPromises = Array.from(this.activeInsertSessions).map(session => session.waitForOperations(null) // Wait indefinitely for sessions to complete
      );
      try {
        await Promise.all(sessionPromises);
      } catch (error) {
        if (this.opts.debugMode) {
          console.log(`⚠️ destroy: Error waiting for sessions: ${error.message}`);
        }
        // Continue with destruction even if sessions have issues
      }

      // Destroy all active sessions
      for (const session of this.activeInsertSessions) {
        session.destroy();
      }
      this.activeInsertSessions.clear();
    }

    // CRITICAL FIX: Add timeout protection to prevent destroy() from hanging
    const destroyPromise = this._performDestroy();
    let timeoutHandle = null;
    const timeoutPromise = new Promise((_, reject) => {
      timeoutHandle = setTimeout(() => {
        reject(new Error('Destroy operation timed out after 5 seconds'));
      }, 5000);
    });
    try {
      await Promise.race([destroyPromise, timeoutPromise]);
    } catch (error) {
      if (error.message === 'Destroy operation timed out after 5 seconds') {
        console.error('🚨 DESTROY TIMEOUT: Force destroying database after timeout');
        // Force mark as destroyed even if cleanup failed
        this.destroyed = true;
        this.destroying = false;
        return;
      }
      throw error;
    } finally {
      // Clear the timeout to prevent Jest open handle warning
      if (timeoutHandle) {
        clearTimeout(timeoutHandle);
      }
    }
  }

  /**
   * Internal destroy implementation
   */
  async _performDestroy() {
    try {
      // CRITICAL: Check for bugs - anything active indicates save() didn't work properly
      const bugs = [];

      // Check for pending data that should have been saved
      if (this.writeBuffer.length > 0) {
        const bug = `BUG: writeBuffer has ${this.writeBuffer.length} records - save() should have cleared this`;
        bugs.push(bug);
        console.error(`🚨 ${bug}`);
      }

      // Check for pending operations that should have completed
      if (this.pendingOperations.size > 0) {
        const bug = `BUG: ${this.pendingOperations.size} pending operations - save() should have completed these`;
        bugs.push(bug);
        console.error(`🚨 ${bug}`);
      }

      // Auto-save manager removed - no cleanup needed

      // Check for active save operation
      if (this.isSaving) {
        const bug = `BUG: save operation still active - previous save() should have completed`;
        bugs.push(bug);
        console.error(`🚨 ${bug}`);
      }

      // If bugs detected, throw error with details
      if (bugs.length > 0) {
        const errorMessage = `Database destroy() found ${bugs.length} bug(s) - save() did not complete properly:\n${bugs.join('\n')}`;
        console.error(`🚨 DESTROY ERROR: ${errorMessage}`);
        throw new Error(errorMessage);
      }

      // FORCE DESTRUCTIVE CLEANUP - no waiting, no graceful shutdown
      if (this.opts.debugMode) {
        console.log('💥 DESTRUCTIVE DESTROY: Force cleaning up all resources');
      }

      // Cancel all operations immediately
      this.abortController.abort();

      // Auto-save removed - no cleanup needed

      // Clear all buffers and data structures
      this.writeBuffer = [];
      this.writeBufferOffsets = [];
      this.writeBufferSizes = [];
      this.deletedIds.clear();
      this.pendingOperations.clear();
      this.pendingIndexUpdates = [];

      // Force close file handlers
      if (this.fileHandler) {
        try {
          // Force close any open file descriptors
          await this.fileHandler.close?.();
        } catch (error) {
          // Ignore file close errors during destructive cleanup
        }
      }

      // Clear all managers
      if (this.indexManager) {
        this.indexManager.clear?.();
      }
      if (this.termManager) {
        this.termManager.clear?.();
      }
      if (this.queryManager) {
        this.queryManager.clear?.();
      }

      // Clear operation queue
      if (this.operationQueue) {
        this.operationQueue.clear?.();
        this.operationQueue = null;
      }

      // Mark as destroyed
      this.destroyed = true;
      this.destroying = false;
      if (this.opts.debugMode) {
        console.log('💥 DESTRUCTIVE DESTROY: Database completely destroyed');
      }
    } catch (error) {
      // Even if cleanup fails, mark as destroyed
      this.destroyed = true;
      this.destroying = false;

      // Re-throw the error so user knows about the bug
      throw error;
    }
  }

  /**
   * Find one record
   */
  async findOne(criteria, options = {}) {
    this._validateInitialization('findOne');
    const results = await this.find(criteria, {
      ...options,
      limit: 1
    });
    return results.length > 0 ? results[0] : null;
  }

  /**
   * Count records
   */
  async count(criteria = {}, options = {}) {
    this._validateInitialization('count');
    const results = await this.find(criteria, options);
    return results.length;
  }

  /**
   * Wait for all pending operations to complete
   */
  async _waitForPendingOperations() {
    if (this.operationQueue && this.operationQueue.getQueueLength() > 0) {
      if (this.opts.debugMode) {
        console.log('💾 Save: Waiting for pending operations to complete');
      }
      // CRITICAL FIX: Wait without timeout to ensure all operations complete
      // This prevents race conditions and data loss
      await this.operationQueue.waitForCompletion(null);

      // Verify queue is actually empty
      if (this.operationQueue.getQueueLength() > 0) {
        throw new Error('Operation queue not empty after wait');
      }
    }
  }

  /**
   * Flush write buffer completely with smart detection of ongoing insertions
   */
  async _flushWriteBufferCompletely() {
    // Force complete flush of write buffer with intelligent detection
    let attempts = 0;
    const maxStuckAttempts = 5; // Maximum attempts with identical data (only protection against infinite loops)
    let stuckAttempts = 0;
    let lastBufferSample = null;

    // CRITICAL FIX: Remove maxAttempts limit - only stop when buffer is empty or truly stuck
    while (this.writeBuffer.length > 0) {
      const currentLength = this.writeBuffer.length;
      const currentSample = this._getBufferSample(); // Get lightweight sample

      // Process write buffer items
      await this._processWriteBuffer();

      // Check if buffer is actually stuck (same data) vs new data being added
      if (this.writeBuffer.length === currentLength) {
        // Check if the data is identical (stuck) or new data was added
        if (this._isBufferSampleIdentical(currentSample, lastBufferSample)) {
          stuckAttempts++;
          if (this.opts.debugMode) {
            console.log(`💾 Flush: Buffer appears stuck (identical data), attempt ${stuckAttempts}/${maxStuckAttempts}`);
          }
          if (stuckAttempts >= maxStuckAttempts) {
            throw new Error(`Write buffer flush stuck - identical data detected after ${maxStuckAttempts} attempts`);
          }
        } else {
          // New data was added, reset stuck counter
          stuckAttempts = 0;
          if (this.opts.debugMode) {
            console.log(`💾 Flush: New data detected, continuing flush (${this.writeBuffer.length} items remaining)`);
          }
        }
        lastBufferSample = currentSample;
      } else {
        // Progress was made, reset stuck counter
        stuckAttempts = 0;
        lastBufferSample = null;
        if (this.opts.debugMode) {
          console.log(`💾 Flush: Progress made, ${currentLength - this.writeBuffer.length} items processed, ${this.writeBuffer.length} remaining`);
        }
      }
      attempts++;

      // Small delay to allow ongoing operations to complete
      if (this.writeBuffer.length > 0) {
        await new Promise(resolve => setTimeout(resolve, 10));
      }
    }

    // CRITICAL FIX: Remove the artificial limit check - buffer should be empty by now
    // If we reach here, the buffer is guaranteed to be empty due to the while condition

    if (this.opts.debugMode) {
      console.log(`💾 Flush completed successfully after ${attempts} attempts`);
    }
  }

  /**
   * Get a lightweight sample of the write buffer for comparison
   * @returns {Object} - Sample data for comparison
   */
  _getBufferSample() {
    if (!this.writeBuffer || this.writeBuffer.length === 0) {
      return null;
    }

    // Create a lightweight sample using first few items and their IDs
    const sampleSize = Math.min(5, this.writeBuffer.length);
    const sample = {
      length: this.writeBuffer.length,
      firstIds: [],
      lastIds: [],
      checksum: 0
    };

    // Sample first few items
    for (let i = 0; i < sampleSize; i++) {
      const item = this.writeBuffer[i];
      if (item && item.id) {
        sample.firstIds.push(item.id);
        // Simple checksum using ID hash
        sample.checksum += item.id.toString().split('').reduce((a, b) => a + b.charCodeAt(0), 0);
      }
    }

    // Sample last few items if buffer is large
    if (this.writeBuffer.length > sampleSize) {
      for (let i = Math.max(0, this.writeBuffer.length - sampleSize); i < this.writeBuffer.length; i++) {
        const item = this.writeBuffer[i];
        if (item && item.id) {
          sample.lastIds.push(item.id);
          sample.checksum += item.id.toString().split('').reduce((a, b) => a + b.charCodeAt(0), 0);
        }
      }
    }
    return sample;
  }

  /**
   * Check if two buffer samples are identical (indicating stuck flush)
   * @param {Object} sample1 - First sample
   * @param {Object} sample2 - Second sample
   * @returns {boolean} - True if samples are identical
   */
  _isBufferSampleIdentical(sample1, sample2) {
    if (!sample1 || !sample2) {
      return false;
    }

    // Quick checks: different lengths or checksums mean different data
    if (sample1.length !== sample2.length || sample1.checksum !== sample2.checksum) {
      return false;
    }

    // Compare first IDs
    if (sample1.firstIds.length !== sample2.firstIds.length) {
      return false;
    }
    for (let i = 0; i < sample1.firstIds.length; i++) {
      if (sample1.firstIds[i] !== sample2.firstIds[i]) {
        return false;
      }
    }

    // Compare last IDs
    if (sample1.lastIds.length !== sample2.lastIds.length) {
      return false;
    }
    for (let i = 0; i < sample1.lastIds.length; i++) {
      if (sample1.lastIds[i] !== sample2.lastIds[i]) {
        return false;
      }
    }
    return true;
  }

  /**
   * Process write buffer items
   */
  async _processWriteBuffer() {
    // Process write buffer items without loading entire file
    // OPTIMIZATION: Use Set directly for both processing and lookup - single variable, better performance
    const itemsToProcess = new Set(this.writeBuffer);

    // CRITICAL FIX: Don't clear writeBuffer immediately - wait for processing to complete
    // This prevents race conditions where new operations arrive while old ones are still processing

    // OPTIMIZATION: Separate buffer items from object items for batch processing
    const bufferItems = [];
    const objectItems = [];
    for (const item of itemsToProcess) {
      if (Buffer.isBuffer(item)) {
        bufferItems.push(item);
      } else if (typeof item === 'object' && item !== null) {
        objectItems.push(item);
      }
    }

    // Process buffer items individually (they're already optimized)
    for (const buffer of bufferItems) {
      await this._processBufferItem(buffer);
    }

    // OPTIMIZATION: Process all object items in a single write operation
    if (objectItems.length > 0) {
      await this._processObjectItemsBatch(objectItems);
    }

    // CRITICAL FIX: Only remove processed items from writeBuffer after all async operations complete
    // OPTIMIZATION: Use Set.has() for O(1) lookup - same Set used for processing
    const beforeLength = this.writeBuffer.length;
    this.writeBuffer = this.writeBuffer.filter(item => !itemsToProcess.has(item));
    const afterLength = this.writeBuffer.length;
    if (this.opts.debugMode && beforeLength !== afterLength) {
      console.log(`💾 _processWriteBuffer: Removed ${beforeLength - afterLength} items from writeBuffer (${beforeLength} -> ${afterLength})`);
    }
  }

  /**
   * Process individual buffer item
   */
  async _processBufferItem(buffer) {
    // Process buffer item without loading entire file
    // This ensures we don't load the entire data file into memory
    if (this.fileHandler) {
      // Use writeDataAsync for non-blocking I/O
      await this.fileHandler.writeDataAsync(buffer);
    }
  }

  /**
   * Process individual object item
   */
  async _processObjectItem(obj) {
    // Process object item without loading entire file
    if (this.fileHandler) {
      // SPACE OPTIMIZATION: Remove term IDs before serialization
      const cleanRecord = this.removeTermIdsForSerialization(obj);
      const jsonString = this.serializer.serialize(cleanRecord).toString('utf8');
      // Use writeDataAsync for non-blocking I/O
      await this.fileHandler.writeDataAsync(Buffer.from(jsonString, 'utf8'));
    }
  }

  /**
   * Process multiple object items in a single batch write operation
   */
  async _processObjectItemsBatch(objects) {
    if (!this.fileHandler || objects.length === 0) return;

    // OPTIMIZATION: Combine all objects into a single buffer for one write operation
    // SPACE OPTIMIZATION: Remove term IDs before serialization
    const jsonStrings = objects.map(obj => this.serializer.serialize(this.removeTermIdsForSerialization(obj)).toString('utf8'));
    const combinedString = jsonStrings.join('');

    // CRITICAL FIX: Validate that the combined string ends with newline
    const validatedString = combinedString.endsWith('\n') ? combinedString : combinedString + '\n';
    const buffer = Buffer.from(validatedString, 'utf8');

    // Single write operation for all objects
    await this.fileHandler.writeDataAsync(buffer);
  }

  /**
   * Wait for all I/O operations to complete
   */
  async _waitForIOCompletion() {
    // Wait for all file operations to complete
    if (this.fileHandler && this.fileHandler.fileMutex) {
      await this.fileHandler.fileMutex.runExclusive(async () => {
        // Ensure all pending file operations complete
        await new Promise(resolve => setTimeout(resolve, 50));
      });
    }
  }

  /**
   * CRITICAL FIX: Safe fallback method to load existing records when _streamExistingRecords fails
   * This prevents data loss by attempting alternative methods to preserve existing data
   */
  async _loadExistingRecordsFallback(deletedIdsSnapshot, writeBufferSnapshot) {
    const existingRecords = [];
    try {
      if (this.opts.debugMode) {
        console.log(`💾 Save: Attempting fallback method to load existing records`);
      }

      // Method 1: Try to read the entire file and filter
      if (this.fileHandler.exists()) {
        const fs = await Promise.resolve().then(() => _interopRequireWildcard(require('fs')));
        const fileContent = await fs.promises.readFile(this.normalizedFile, 'utf8');
        const lines = fileContent.split('\n').filter(line => line.trim());
        for (let i = 0; i < lines.length && i < this.offsets.length; i++) {
          try {
            const record = this.serializer.deserialize(lines[i]);
            if (record && !deletedIdsSnapshot.has(record.id)) {
              // Check if this record is not being updated in writeBuffer
              const updatedRecord = writeBufferSnapshot.find(r => r.id === record.id);
              if (!updatedRecord) {
                existingRecords.push(record);
              }
            }
          } catch (error) {
            // Skip invalid lines
            if (this.opts.debugMode) {
              console.log(`💾 Save: Skipping invalid line ${i} in fallback:`, error.message);
            }
          }
        }
      }
      if (this.opts.debugMode) {
        console.log(`💾 Save: Fallback method loaded ${existingRecords.length} existing records`);
      }
      return existingRecords;
    } catch (error) {
      if (this.opts.debugMode) {
        console.log(`💾 Save: Fallback method failed:`, error.message);
      }
      // Return empty array as last resort - better than losing all data
      return [];
    }
  }

  /**
   * Stream existing records without loading entire file into memory
   * Optimized with group ranging and reduced JSON parsing
   */
  async _streamExistingRecords(deletedIdsSnapshot, writeBufferSnapshot) {
    const existingRecords = [];
    if (this.offsets.length === 0) {
      return existingRecords;
    }

    // OPTIMIZATION: Pre-allocate array with known size (but don't set length to avoid undefined slots)
    // existingRecords.length = this.offsets.length

    // Create a map of updated records for quick lookup
    const updatedRecordsMap = new Map();
    writeBufferSnapshot.forEach(record => {
      updatedRecordsMap.set(record.id, record);
    });

    // OPTIMIZATION: Cache file stats to avoid repeated stat() calls
    let fileSize = 0;
    if (this._cachedFileStats && this._cachedFileStats.timestamp > Date.now() - 1000) {
      // Use cached stats if less than 1 second old
      fileSize = this._cachedFileStats.size;
    } else {
      // Get fresh stats and cache them
      const fileStats = (await this.fileHandler.exists()) ? await _fs.default.promises.stat(this.normalizedFile) : null;
      fileSize = fileStats ? fileStats.size : 0;
      this._cachedFileStats = {
        size: fileSize,
        timestamp: Date.now()
      };
    }

    // CRITICAL FIX: Ensure indexOffset is consistent with actual file size
    if (this.indexOffset > fileSize) {
      if (this.opts.debugMode) {
        console.log(`💾 Save: Correcting indexOffset from ${this.indexOffset} to ${fileSize} (file size)`);
      }
      this.indexOffset = fileSize;
    }

    // Build ranges array for group reading
    const ranges = [];
    for (let i = 0; i < this.offsets.length; i++) {
      const offset = this.offsets[i];
      let nextOffset = i + 1 < this.offsets.length ? this.offsets[i + 1] : this.indexOffset;
      if (this.opts.debugMode) {
        console.log(`💾 Save: Building range for record ${i}: offset=${offset}, nextOffset=${nextOffset}`);
      }

      // CRITICAL FIX: Handle case where indexOffset is 0 (new database without index)
      if (nextOffset === 0 && i + 1 >= this.offsets.length) {
        // For the last record when there's no index yet, we need to find the actual end
        // Read a bit more data to find the newline character that ends the record
        const searchEnd = Math.min(offset + 1000, fileSize); // Search up to 1000 bytes ahead
        if (searchEnd > offset) {
          try {
            const searchBuffer = await this.fileHandler.readRange(offset, searchEnd);
            const searchText = searchBuffer.toString('utf8');

            // Look for the end of the JSON record (closing brace followed by newline or end of data)
            let recordEnd = -1;
            let braceCount = 0;
            let inString = false;
            let escapeNext = false;
            for (let j = 0; j < searchText.length; j++) {
              const char = searchText[j];
              if (escapeNext) {
                escapeNext = false;
                continue;
              }
              if (char === '\\') {
                escapeNext = true;
                continue;
              }
              if (char === '"' && !escapeNext) {
                inString = !inString;
                continue;
              }
              if (!inString) {
                if (char === '{') {
                  braceCount++;
                } else if (char === '}') {
                  braceCount--;
                  if (braceCount === 0) {
                    // Found the end of the JSON object
                    recordEnd = j + 1;
                    break;
                  }
                }
              }
            }
            if (recordEnd !== -1) {
              nextOffset = offset + recordEnd;
            } else {
              // If we can't find the end, read to end of file
              nextOffset = fileSize;
            }
          } catch (error) {
            // Fallback to end of file if search fails
            nextOffset = fileSize;
          }
        } else {
          nextOffset = fileSize;
        }
      }

      // Validate offset ranges
      if (offset < 0) {
        if (this.opts.debugMode) {
          console.log(`💾 Save: Skipped negative offset ${offset}`);
        }
        continue;
      }

      // CRITICAL FIX: Allow offsets that are at or beyond file size (for new records)
      if (fileSize > 0 && offset > fileSize) {
        if (this.opts.debugMode) {
          console.log(`💾 Save: Skipped offset ${offset} beyond file size ${fileSize}`);
        }
        continue;
      }
      if (nextOffset <= offset) {
        if (this.opts.debugMode) {
          console.log(`💾 Save: Skipped invalid range [${offset}, ${nextOffset}]`);
        }
        continue;
      }
      ranges.push({
        start: offset,
        end: nextOffset,
        index: i
      });
    }
    if (ranges.length === 0) {
      return existingRecords;
    }

    // Use group ranging for efficient reading
    const recordLines = await this.fileHandler.readRanges(ranges, async (lineString, range) => {
      if (!lineString || !lineString.trim()) {
        return null;
      }
      const trimmedLine = lineString.trim();

      // DEBUG: Log what we're reading (temporarily enabled for debugging)
      if (this.opts.debugMode) {
        console.log(`💾 Save: Reading range ${range.start}-${range.end}, length: ${trimmedLine.length}`);
        console.log(`💾 Save: First 100 chars: ${trimmedLine.substring(0, 100)}`);
        if (trimmedLine.length > 100) {
          console.log(`💾 Save: Last 100 chars: ${trimmedLine.substring(trimmedLine.length - 100)}`);
        }
      }

      // OPTIMIZATION: Try to extract ID without full JSON parsing
      let recordId = null;
      let needsFullParse = false;

      // For array format, try to extract ID from array position
      if (trimmedLine.startsWith('[') && trimmedLine.endsWith(']')) {
        // Array format: try to extract ID from the array
        try {
          const arrayData = JSON.parse(trimmedLine);
          if (Array.isArray(arrayData) && arrayData.length > 0) {
            // For arrays without explicit ID, use the first element as a fallback
            // or try to find the ID field if it exists
            if (arrayData.length > 2) {
              // ID is typically at position 2 in array format [age, city, id, name]
              recordId = arrayData[2];
            } else {
              // For arrays without ID field, use first element as fallback
              recordId = arrayData[0];
            }
            if (recordId !== undefined && recordId !== null) {
              recordId = String(recordId);
              // Check if this record needs full parsing (updated or deleted)
              needsFullParse = updatedRecordsMap.has(recordId) || deletedIdsSnapshot.has(recordId);
            } else {
              needsFullParse = true;
            }
          } else {
            needsFullParse = true;
          }
        } catch (e) {
          needsFullParse = true;
        }
      } else {
        // Object format: use regex for backward compatibility
        const idMatch = trimmedLine.match(/"id"\s*:\s*"([^"]+)"|"id"\s*:\s*(\d+)/);
        if (idMatch) {
          recordId = idMatch[1] || idMatch[2];
          needsFullParse = updatedRecordsMap.has(recordId) || deletedIdsSnapshot.has(recordId);
        } else {
          needsFullParse = true;
        }
      }
      if (!needsFullParse) {
        // Record is unchanged - we can avoid parsing entirely
        // Store the raw line and parse only when needed for the final result
        return {
          type: 'unchanged',
          line: trimmedLine,
          id: recordId,
          needsParse: false
        };
      }

      // Full parsing needed for updated/deleted records
      try {
        // Use serializer to properly deserialize array format
        const record = this.serializer ? this.serializer.deserialize(trimmedLine) : JSON.parse(trimmedLine);

        // Use record directly (no need to restore term IDs)
        const recordWithIds = record;
        if (updatedRecordsMap.has(recordWithIds.id)) {
          // Replace with updated version
          const updatedRecord = updatedRecordsMap.get(recordWithIds.id);
          if (this.opts.debugMode) {
            console.log(`💾 Save: Updated record ${recordWithIds.id} (${recordWithIds.name || 'Unnamed'})`);
          }
          return {
            type: 'updated',
            record: updatedRecord,
            id: recordWithIds.id,
            needsParse: false
          };
        } else if (!deletedIdsSnapshot.has(recordWithIds.id)) {
          // Keep existing record if not deleted
          if (this.opts.debugMode) {
            console.log(`💾 Save: Kept record ${recordWithIds.id} (${recordWithIds.name || 'Unnamed'})`);
          }
          return {
            type: 'kept',
            record: recordWithIds,
            id: recordWithIds.id,
            needsParse: false
          };
        } else {
          // Skip deleted record
          if (this.opts.debugMode) {
            console.log(`💾 Save: Skipped record ${recordWithIds.id} (${recordWithIds.name || 'Unnamed'}) - deleted`);
          }
          return {
            type: 'deleted',
            id: recordWithIds.id,
            needsParse: false
          };
        }
      } catch (jsonError) {
        // RACE CONDITION FIX: Skip records that can't be parsed due to incomplete writes
        if (this.opts.debugMode) {
          console.log(`💾 Save: Skipped corrupted record at range ${range.start}-${range.end} - ${jsonError.message}`);
          // console.log(`💾 Save: Problematic line: ${trimmedLine}`)
        }
        return null;
      }
    });

    // Process results and build final records array
    // OPTIMIZATION: Pre-allocate arrays with known size
    const unchangedLines = [];
    const parsedRecords = [];

    // OPTIMIZATION: Use for loop instead of Object.entries().sort() for better performance
    const sortedEntries = [];
    for (const key in recordLines) {
      if (recordLines.hasOwnProperty(key)) {
        sortedEntries.push([key, recordLines[key]]);
      }
    }

    // OPTIMIZATION: Sort by offset position using numeric comparison
    sortedEntries.sort(([keyA], [keyB]) => parseInt(keyA) - parseInt(keyB));

    // CRITICAL FIX: Maintain record order by processing in original offset order
    // and tracking which records are being kept vs deleted
    const keptRecords = [];
    const deletedOffsets = new Set();
    for (const [rangeKey, result] of sortedEntries) {
      if (!result) continue;
      const offset = parseInt(rangeKey);
      switch (result.type) {
        case 'unchanged':
          // Collect unchanged lines for batch processing
          unchangedLines.push(result.line);
          keptRecords.push({
            offset,
            type: 'unchanged',
            line: result.line
          });
          break;
        case 'updated':
        case 'kept':
          parsedRecords.push(result.record);
          keptRecords.push({
            offset,
            type: 'parsed',
            record: result.record
          });
          break;
        case 'deleted':
          // Track deleted records by their offset
          deletedOffsets.add(offset);
          break;
      }
    }

    // CRITICAL FIX: Build final records array in the correct order
    // and update offsets array to match the new record order
    const newOffsets = [];
    let currentOffset = 0;

    // OPTIMIZATION: Batch parse unchanged records for better performance
    if (unchangedLines.length > 0) {
      const batchParsedRecords = [];
      for (let i = 0; i < unchangedLines.length; i++) {
        try {
          // Use serializer to properly deserialize array format
          const record = this.serializer ? this.serializer.deserialize(unchangedLines[i]) : JSON.parse(unchangedLines[i]);
          batchParsedRecords.push(record);
        } catch (jsonError) {
          if (this.opts.debugMode) {
            console.log(`💾 Save: Failed to parse unchanged record: ${jsonError.message}`);
          }
          batchParsedRecords.push(null); // Mark as failed
        }
      }

      // Process kept records in their original offset order
      let batchIndex = 0;
      for (const keptRecord of keptRecords) {
        let record = null;
        if (keptRecord.type === 'unchanged') {
          record = batchParsedRecords[batchIndex++];
          if (!record) continue; // Skip failed parses
        } else if (keptRecord.type === 'parsed') {
          record = keptRecord.record;
        }
        if (record && typeof record === 'object') {
          existingRecords.push(record);
          newOffsets.push(currentOffset);
          // OPTIMIZATION: Use cached string length if available
          const recordSize = keptRecord.type === 'unchanged' ? keptRecord.line.length + 1 // Use actual line length
          : JSON.stringify(this.removeTermIdsForSerialization(record)).length + 1;
          currentOffset += recordSize;
        }
      }
    } else {
      // Process kept records in their original offset order (no unchanged records)
      for (const keptRecord of keptRecords) {
        if (keptRecord.type === 'parsed') {
          const record = keptRecord.record;
          if (record && typeof record === 'object' && record.id) {
            existingRecords.push(record);
            newOffsets.push(currentOffset);
            const recordSize = JSON.stringify(this.removeTermIdsForSerialization(record)).length + 1;
            currentOffset += recordSize;
          }
        }
      }
    }

    // Update the offsets array to reflect the new record order
    this.offsets = newOffsets;
    return existingRecords;
  }

  /**
   * Flush write buffer
   */
  async flush() {
    return this.operationQueue.enqueue(async () => {
      this.isInsideOperationQueue = true;
      try {
        // CRITICAL FIX: Actually flush the writeBuffer by saving data
        if (this.writeBuffer.length > 0 || this.shouldSave) {
          await this._doSave();
        }
        return Promise.resolve();
      } finally {
        this.isInsideOperationQueue = false;
      }
    });
  }

  /**
   * Flush insertion buffer (backward compatibility)
   */
  async flushInsertionBuffer() {
    // Flush insertion buffer implementation - save any pending data
    // Use the same robust flush logic as flush()
    return this.flush();
  }

  /**
   * Get memory usage
   */
  getMemoryUsage() {
    return {
      offsetsCount: this.offsets.length,
      writeBufferSize: this.writeBuffer ? this.writeBuffer.length : 0,
      used: this.writeBuffer.length,
      total: this.offsets.length + this.writeBuffer.length,
      percentage: 0
    };
  }
  _hasActualIndexData() {
    if (!this.indexManager) return false;
    const data = this.indexManager.index.data;
    for (const field in data) {
      const fieldData = data[field];
      for (const value in fieldData) {
        const hybridData = fieldData[value];
        if (hybridData.set && hybridData.set.size > 0) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Locate a record by line number and return its byte range
   * @param {number} n - Line number
   * @returns {Array} - [start, end] byte range or undefined
   */
  locate(n) {
    if (this.offsets[n] === undefined) {
      return undefined; // Record doesn't exist
    }

    // CRITICAL FIX: Calculate end offset correctly to prevent cross-line reading
    let end;
    if (n + 1 < this.offsets.length) {
      // Use next record's start minus 1 (to exclude newline) as this record's end
      end = this.offsets[n + 1] - 1;
    } else {
      // For the last record, use indexOffset (includes the record but not newline)
      end = this.indexOffset;
    }
    return [this.offsets[n], end];
  }

  /**
   * Get ranges for streaming based on line numbers
   * @param {Array|Set} map - Line numbers to get ranges for
   * @returns {Array} - Array of range objects {start, end, index}
   */
  getRanges(map) {
    return (map || Array.from(this.offsets.keys())).map(n => {
      const ret = this.locate(n);
      if (ret !== undefined) return {
        start: ret[0],
        end: ret[1],
        index: n
      };
    }).filter(n => n !== undefined);
  }

  /**
   * Walk through records using streaming (real implementation)
   */
  walk(_x) {
    var _this = this;
    return _wrapAsyncGenerator(function* (criteria, options = {}) {
      // CRITICAL FIX: Validate state before walk operation to prevent crashes
      _this.validateState();
      if (!_this.initialized) yield _awaitAsyncGenerator(_this.init());

      // If no data at all, return empty
      if (_this.indexOffset === 0 && _this.writeBuffer.length === 0) return;
      let map;
      if (!Array.isArray(criteria)) {
        if (criteria instanceof Set) {
          map = [...criteria];
        } else if (criteria && typeof criteria === 'object' && Object.keys(criteria).length > 0) {
          // Only use indexManager.query if criteria has actual filters
          map = [..._this.indexManager.query(criteria, options)];
        } else {
          // For empty criteria {} or null/undefined, get all records
          // Use writeBuffer length when indexOffset is 0 (data not saved yet)
          const totalRecords = _this.indexOffset > 0 ? _this.indexOffset : _this.writeBuffer.length;
          map = [...Array(totalRecords).keys()];
        }
      } else {
        map = criteria;
      }

      // Use writeBuffer when available (unsaved data)
      if (_this.writeBuffer.length > 0) {
        let count = 0;

        // If map is empty (no index results) but we have criteria, filter writeBuffer directly
        if (map.length === 0 && criteria && typeof criteria === 'object' && Object.keys(criteria).length > 0) {
          for (let i = 0; i < _this.writeBuffer.length; i++) {
            if (options.limit && count >= options.limit) {
              break;
            }
            const entry = _this.writeBuffer[i];
            if (entry && _this.queryManager.matchesCriteria(entry, criteria, options)) {
              count++;
              if (options.includeOffsets) {
                yield {
                  entry,
                  start: 0,
                  _: i
                };
              } else {
                if (_this.opts.includeLinePosition) {
                  entry._ = i;
                }
                yield entry;
              }
            }
          }
        } else {
          // Use map-based iteration (for all records or indexed results)
          for (const lineNumber of map) {
            if (options.limit && count >= options.limit) {
              break;
            }
            if (lineNumber < _this.writeBuffer.length) {
              const entry = _this.writeBuffer[lineNumber];
              if (entry) {
                count++;
                if (options.includeOffsets) {
                  yield {
                    entry,
                    start: 0,
                    _: lineNumber
                  };
                } else {
                  if (_this.opts.includeLinePosition) {
                    entry._ = lineNumber;
                  }
                  yield entry;
                }
              }
            }
          }
        }
        return;
      }

      // If writeBuffer is empty but we have saved data, we need to load it from file
      if (_this.writeBuffer.length === 0 && _this.indexOffset > 0) {
        // Load data from file for querying
        try {
          let data;
          let lines;

          // Smart threshold: decide between partial reads vs full read
          const resultPercentage = map ? map.length / _this.indexOffset * 100 : 100;
          const threshold = _this.opts.partialReadThreshold || 60; // Default 60% threshold

          // Use partial reads when:
          // 1. We have specific line numbers from index
          // 2. Results are below threshold percentage
          // 3. Database is large enough to benefit from partial reads
          const shouldUsePartialReads = map && map.length > 0 && resultPercentage < threshold && _this.indexOffset > 100; // Only for databases with >100 records

          if (shouldUsePartialReads) {
            if (_this.opts.debugMode) {
              console.log(`🔍 Using PARTIAL READS: ${map.length}/${_this.indexOffset} records (${resultPercentage.toFixed(1)}% < ${threshold}% threshold)`);
            }
            // Convert 0-based line numbers to 1-based for readSpecificLines
            const lineNumbers = map.map(num => num + 1);
            data = yield _awaitAsyncGenerator(_this.fileHandler.readSpecificLines(lineNumbers));
            lines = data ? data.split('\n') : [];
          } else {
            if (_this.opts.debugMode) {
              console.log(`🔍 Using STREAMING READ: ${map?.length || 0}/${_this.indexOffset} records (${resultPercentage.toFixed(1)}% >= ${threshold}% threshold or small DB)`);
            }
            // Use streaming instead of loading all data in memory
            // This prevents memory issues with large databases
            const streamingResults = yield _awaitAsyncGenerator(_this.fileHandler.readWithStreaming(criteria, {
              limit: options.limit,
              skip: options.skip
            }, matchesCriteria, _this.serializer));

            // Process streaming results directly without loading all lines
            for (const record of streamingResults) {
              if (options.limit && count >= options.limit) {
                break;
              }
              count++;

              // SPACE OPTIMIZATION: Restore term IDs to terms for user
              const recordWithTerms = _this.restoreTermIdsAfterDeserialization(record);
              if (options.includeOffsets) {
                yield {
                  entry: recordWithTerms,
                  start: 0,
                  _: 0
                };
              } else {
                if (_this.opts.includeLinePosition) {
                  recordWithTerms._ = 0;
                }
                yield recordWithTerms;
              }
            }
            return; // Exit early since we processed streaming results
          }
          if (lines.length > 0) {
            const records = [];
            for (const line of lines) {
              if (line.trim()) {
                try {
                  // CRITICAL FIX: Use serializer.deserialize instead of JSON.parse to handle array format
                  const record = _this.serializer.deserialize(line);
                  // SPACE OPTIMIZATION: Restore term IDs to terms for user
                  const recordWithTerms = _this.restoreTermIdsAfterDeserialization(record);
                  records.push(recordWithTerms);
                } catch (error) {
                  // Skip invalid lines
                }
              }
            }

            // Use loaded records for querying
            let count = 0;

            // When using partial reads, records correspond to the requested line numbers
            if (shouldUsePartialReads) {
              for (let i = 0; i < Math.min(records.length, map.length); i++) {
                if (options.limit && count >= options.limit) {
                  break;
                }
                const entry = records[i];
                const lineNumber = map[i];
                if (entry) {
                  count++;
                  if (options.includeOffsets) {
                    yield {
                      entry,
                      start: 0,
                      _: lineNumber
                    };
                  } else {
                    if (_this.opts.includeLinePosition) {
                      entry._ = lineNumber;
                    }
                    yield entry;
                  }
                }
              }
            } else {
              // Fallback to original logic when reading all data
              for (const lineNumber of map) {
                if (options.limit && count >= options.limit) {
                  break;
                }
                if (lineNumber < records.length) {
                  const entry = records[lineNumber];
                  if (entry) {
                    count++;
                    if (options.includeOffsets) {
                      yield {
                        entry,
                        start: 0,
                        _: lineNumber
                      };
                    } else {
                      if (_this.opts.includeLinePosition) {
                        entry._ = lineNumber;
                      }
                      yield entry;
                    }
                  }
                }
              }
            }
            return;
          }
        } catch (error) {
          // If file reading fails, continue to file-based streaming
        }
      }

      // Use file-based streaming for saved data
      const ranges = _this.getRanges(map);
      const groupedRanges = yield _awaitAsyncGenerator(_this.fileHandler.groupedRanges(ranges));
      const fd = yield _awaitAsyncGenerator(_fs.default.promises.open(_this.fileHandler.file, 'r'));
      try {
        let count = 0;
        for (const groupedRange of groupedRanges) {
          if (options.limit && count >= options.limit) {
            break;
          }
          var _iteratorAbruptCompletion = false;
          var _didIteratorError = false;
          var _iteratorError;
          try {
            for (var _iterator = _asyncIterator(_this.fileHandler.readGroupedRange(groupedRange, fd)), _step; _iteratorAbruptCompletion = !(_step = yield _awaitAsyncGenerator(_iterator.next())).done; _iteratorAbruptCompletion = false) {
              const row = _step.value;
              {
                if (options.limit && count >= options.limit) {
                  break;
                }
                const entry = yield _awaitAsyncGenerator(_this.serializer.deserialize(row.line, {
                  compress: _this.opts.compress,
                  v8: _this.opts.v8
                }));
                if (entry === null) continue;

                // SPACE OPTIMIZATION: Restore term IDs to terms for user
                const entryWithTerms = _this.restoreTermIdsAfterDeserialization(entry);
                count++;
                if (options.includeOffsets) {
                  yield {
                    entry: entryWithTerms,
                    start: row.start,
                    _: row._ || _this.offsets.findIndex(n => n === row.start)
                  };
                } else {
                  if (_this.opts.includeLinePosition) {
                    entryWithTerms._ = row._ || _this.offsets.findIndex(n => n === row.start);
                  }
                  yield entryWithTerms;
                }
              }
            }
          } catch (err) {
            _didIteratorError = true;
            _iteratorError = err;
          } finally {
            try {
              if (_iteratorAbruptCompletion && _iterator.return != null) {
                yield _awaitAsyncGenerator(_iterator.return());
              }
            } finally {
              if (_didIteratorError) {
                throw _iteratorError;
              }
            }
          }
        }
      } finally {
        yield _awaitAsyncGenerator(fd.close());
      }
    }).apply(this, arguments);
  }

  /**
   * Iterate through records with bulk update capabilities
   * Allows in-place modifications and deletions with optimized performance
   * 
   * @param {Object} criteria - Query criteria
   * @param {Object} options - Iteration options
   * @param {number} options.chunkSize - Batch size for processing (default: 1000)
   * @param {string} options.strategy - Processing strategy: 'streaming' (always uses walk() method)
   * @param {boolean} options.autoSave - Auto-save after each chunk (default: false)
   * @param {Function} options.progressCallback - Progress callback function
   * @param {boolean} options.detectChanges - Auto-detect changes (default: true)
   * @returns {AsyncGenerator} Generator yielding records for modification
   */
  iterate(_x2) {
    var _this2 = this;
    return _wrapAsyncGenerator(function* (criteria, options = {}) {
      // CRITICAL FIX: Validate state before iterate operation
      _this2.validateState();
      if (!_this2.initialized) yield _awaitAsyncGenerator(_this2.init());

      // Set default options
      const opts = {
        chunkSize: 1000,
        strategy: 'streaming',
        // Always use walk() method for optimal performance
        autoSave: false,
        detectChanges: true,
        ...options
      };

      // If no data, return empty
      if (_this2.indexOffset === 0 && _this2.writeBuffer.length === 0) return;
      const startTime = Date.now();
      let processedCount = 0;
      let modifiedCount = 0;
      let deletedCount = 0;

      // Buffers for batch processing
      const updateBuffer = [];
      const deleteBuffer = new Set();
      const originalRecords = new Map(); // Track original records for change detection

      try {
        // Always use walk() now that the bug is fixed - it works for both small and large datasets
        var _iteratorAbruptCompletion2 = false;
        var _didIteratorError2 = false;
        var _iteratorError2;
        try {
          for (var _iterator2 = _asyncIterator(_this2.walk(criteria, options)), _step2; _iteratorAbruptCompletion2 = !(_step2 = yield _awaitAsyncGenerator(_iterator2.next())).done; _iteratorAbruptCompletion2 = false) {
            const entry = _step2.value;
            {
              processedCount++;

              // Store original record for change detection BEFORE yielding
              let originalRecord = null;
              if (opts.detectChanges) {
                originalRecord = _this2._createShallowCopy(entry);
                originalRecords.set(entry.id, originalRecord);
              }

              // Create wrapper based on performance preference
              const entryWrapper = opts.highPerformance ? _this2._createHighPerformanceWrapper(entry, originalRecord) : _this2._createEntryProxy(entry, originalRecord);

              // Yield the wrapper for user modification
              yield entryWrapper;

              // Check if entry was modified or deleted AFTER yielding
              if (entryWrapper.isMarkedForDeletion) {
                // Entry was marked for deletion
                if (originalRecord) {
                  deleteBuffer.add(originalRecord.id);
                  deletedCount++;
                }
              } else if (opts.detectChanges && originalRecord) {
                // Check if entry was modified by comparing with original (optimized comparison)
                if (_this2._hasRecordChanged(entry, originalRecord)) {
                  updateBuffer.push(entry);
                  modifiedCount++;
                }
              } else if (entryWrapper.isModified) {
                // Manual change detection
                updateBuffer.push(entry);
                modifiedCount++;
              }

              // Process batch when chunk size is reached
              if (updateBuffer.length >= opts.chunkSize || deleteBuffer.size >= opts.chunkSize) {
                yield _awaitAsyncGenerator(_this2._processIterateBatch(updateBuffer, deleteBuffer, opts));

                // Clear buffers
                updateBuffer.length = 0;
                deleteBuffer.clear();
                originalRecords.clear();

                // Progress callback
                if (opts.progressCallback) {
                  opts.progressCallback({
                    processed: processedCount,
                    modified: modifiedCount,
                    deleted: deletedCount,
                    elapsed: Date.now() - startTime
                  });
                }
              }
            }
          }

          // Process remaining records in buffers
        } catch (err) {
          _didIteratorError2 = true;
          _iteratorError2 = err;
        } finally {
          try {
            if (_iteratorAbruptCompletion2 && _iterator2.return != null) {
              yield _awaitAsyncGenerator(_iterator2.return());
            }
          } finally {
            if (_didIteratorError2) {
              throw _iteratorError2;
            }
          }
        }
        if (updateBuffer.length > 0 || deleteBuffer.size > 0) {
          yield _awaitAsyncGenerator(_this2._processIterateBatch(updateBuffer, deleteBuffer, opts));
        }

        // Final progress callback (always called)
        if (opts.progressCallback) {
          opts.progressCallback({
            processed: processedCount,
            modified: modifiedCount,
            deleted: deletedCount,
            elapsed: Date.now() - startTime,
            completed: true
          });
        }
        if (_this2.opts.debugMode) {
          console.log(`🔄 ITERATE COMPLETED: ${processedCount} processed, ${modifiedCount} modified, ${deletedCount} deleted in ${Date.now() - startTime}ms`);
        }
      } catch (error) {
        console.error('Iterate operation failed:', error);
        throw error;
      }
    }).apply(this, arguments);
  }

  /**
   * Process a batch of updates and deletes from iterate operation
   * @private
   */
  async _processIterateBatch(updateBuffer, deleteBuffer, options) {
    if (updateBuffer.length === 0 && deleteBuffer.size === 0) return;
    const startTime = Date.now();
    try {
      // Process updates
      if (updateBuffer.length > 0) {
        for (const record of updateBuffer) {
          // Remove the _modified flag if it exists
          delete record._modified;

          // Update record in writeBuffer or add to writeBuffer
          const index = this.writeBuffer.findIndex(r => r.id === record.id);
          if (index !== -1) {
            // Record is already in writeBuffer, update it
            this.writeBuffer[index] = record;
          } else {
            // Record is in file, add updated version to writeBuffer
            this.writeBuffer.push(record);
          }

          // Update index
          await this.indexManager.update(record, record, this.writeBuffer.length - 1);
        }
        if (this.opts.debugMode) {
          console.log(`🔄 ITERATE: Updated ${updateBuffer.length} records in ${Date.now() - startTime}ms`);
        }
      }

      // Process deletes
      if (deleteBuffer.size > 0) {
        for (const recordId of deleteBuffer) {
          // Find the record to get its data for term mapping removal
          const record = this.writeBuffer.find(r => r.id === recordId) || (await this.findOne({
            id: recordId
          }));
          if (record) {
            // Remove term mapping
            this.removeTermMapping(record);

            // Remove from index
            await this.indexManager.remove(record);

            // Remove from writeBuffer or mark as deleted
            const index = this.writeBuffer.findIndex(r => r.id === recordId);
            if (index !== -1) {
              this.writeBuffer.splice(index, 1);
            } else {
              // Mark as deleted if not in writeBuffer
              this.deletedIds.add(recordId);
            }
          }
        }
        if (this.opts.debugMode) {
          console.log(`🗑️ ITERATE: Deleted ${deleteBuffer.size} records in ${Date.now() - startTime}ms`);
        }
      }

      // Auto-save if enabled
      if (options.autoSave) {
        await this.save();
      }
      this.shouldSave = true;
      this.performanceStats.operations++;
    } catch (error) {
      console.error('Batch processing failed:', error);
      throw error;
    }
  }

  /**
   * Close the database
   */
  async close() {
    if (this.destroyed || this.closed) return;
    try {
      if (this.opts.debugMode) {
        console.log(`💾 close(): Saving and closing database (reopenable)`);
      }

      // 1. Save all pending data and index data to files
      if (this.writeBuffer.length > 0 || this.shouldSave) {
        await this.save();
        // Ensure writeBuffer is cleared after save
        if (this.writeBuffer.length > 0) {
          console.warn('⚠️ WriteBuffer not cleared after save() - forcing clear');
          this.writeBuffer = [];
          this.writeBufferOffsets = [];
          this.writeBufferSizes = [];
        }
      } else {
        // Even if no data to save, ensure index data is persisted
        await this._saveIndexDataToFile();
      }

      // 2. Mark as closed (but not destroyed) to allow reopening
      this.closed = true;
      this.initialized = false;

      // 3. Clear any remaining state for clean reopening
      this.writeBuffer = [];
      this.writeBufferOffsets = [];
      this.writeBufferSizes = [];
      this.shouldSave = false;
      this.isSaving = false;
      this.lastSaveTime = null;
      if (this.opts.debugMode) {
        console.log(`💾 Database closed (can be reopened with init())`);
      }
    } catch (error) {
      console.error('Failed to close database:', error);
      // Mark as closed even if save failed
      this.closed = true;
      this.initialized = false;
      throw error;
    }
  }

  /**
   * Save index data to .idx.jdb file
   * @private
   */
  async _saveIndexDataToFile() {
    if (this.indexManager) {
      try {
        const idxPath = this.normalizedFile.replace('.jdb', '.idx.jdb');
        const indexData = {
          index: this.indexManager.indexedFields && this.indexManager.indexedFields.length > 0 ? this.indexManager.toJSON() : {},
          offsets: this.offsets,
          // Save actual offsets for efficient file operations
          indexOffset: this.indexOffset,
          // Save file size for proper range calculations
          // Save configuration for reuse when database exists
          config: {
            fields: this.opts.fields,
            indexes: this.opts.indexes,
            originalIndexes: this.opts.originalIndexes,
            schema: this.serializer?.getSchema?.() || null
          }
        };

        // Include term mapping data in .idx file if term mapping fields exist
        const termMappingFields = this.getTermMappingFields();
        if (termMappingFields.length > 0 && this.termManager) {
          const termData = await this.termManager.saveTerms();
          indexData.termMapping = termData;
        }

        // Always create .idx file for databases with indexes, even if empty
        // This ensures the database structure is complete
        const originalFile = this.fileHandler.file;
        this.fileHandler.file = idxPath;
        await this.fileHandler.writeAll(JSON.stringify(indexData, null, 2));
        this.fileHandler.file = originalFile;
        if (this.opts.debugMode) {
          console.log(`💾 Index data saved to ${idxPath}`);
        }
      } catch (error) {
        console.warn('Failed to save index data:', error.message);
        throw error; // Re-throw to let caller handle
      }
    }
  }

  /**
   * Get operation queue statistics
   */
  getQueueStats() {
    if (!this.operationQueue) {
      return {
        queueLength: 0,
        isProcessing: false,
        totalOperations: 0,
        completedOperations: 0,
        failedOperations: 0,
        successRate: 0,
        averageProcessingTime: 0,
        maxProcessingTime: 0
      };
    }
    return this.operationQueue.getStats();
  }

  /**
   * Wait for all pending operations to complete
   * This includes operation queue AND active insert sessions
   * If called with no arguments, interpret as waitForOperations(null).
   * If argument provided (maxWaitTime), pass that on.
   */
  async waitForOperations(maxWaitTime = null) {
    // Accept any falsy/undefined/empty call as "wait for all"
    const actualWaitTime = arguments.length === 0 ? null : maxWaitTime;
    const startTime = Date.now();
    const hasTimeout = actualWaitTime !== null && actualWaitTime !== undefined;

    // Wait for operation queue
    if (this.operationQueue) {
      const queueCompleted = await this.operationQueue.waitForCompletion(actualWaitTime);
      if (!queueCompleted && hasTimeout) {
        return false;
      }
    }

    // Wait for active insert sessions
    if (this.activeInsertSessions.size > 0) {
      if (this.opts.debugMode) {
        console.log(`⏳ waitForOperations: Waiting for ${this.activeInsertSessions.size} active insert sessions`);
      }

      // Wait for all active sessions to complete
      const sessionPromises = Array.from(this.activeInsertSessions).map(session => session.waitForOperations(actualWaitTime));
      try {
        const sessionResults = await Promise.all(sessionPromises);

        // Check if any session timed out
        if (hasTimeout && sessionResults.some(result => !result)) {
          return false;
        }
      } catch (error) {
        if (this.opts.debugMode) {
          console.log(`⚠️ waitForOperations: Error waiting for sessions: ${error.message}`);
        }
        // Continue anyway - don't fail the entire operation
      }
    }
    return true;
  }
}
exports.Database = Database;
var _default = exports.default = Database;