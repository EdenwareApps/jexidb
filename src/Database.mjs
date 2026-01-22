import { EventEmitter } from 'events'
import IndexManager from './managers/IndexManager.mjs'
import Serializer from './Serializer.mjs'
import { Mutex } from 'async-mutex'
import fs from 'fs'
import readline from 'readline'
import { OperationQueue } from './OperationQueue.mjs'

/**
 * IterateEntry class for intuitive API with automatic change detection
 * Uses native JavaScript setters for maximum performance
 */
class IterateEntry {
  constructor(entry, originalRecord) {
    this._entry = entry
    this._originalRecord = originalRecord
    this._modified = false
    this._markedForDeletion = false
  }
  
  // Generic getter that returns values from the original entry
  get(property) {
    return this._entry[property]
  }
  
  // Generic setter that sets values in the original entry
  set(property, value) {
    this._entry[property] = value
    this._modified = true
  }
  
  // Delete method for intuitive deletion
  delete() {
    this._markedForDeletion = true
    return true
  }
  
  // Getter for the underlying entry (for compatibility)
  get value() {
    return this._entry
  }
  
  // Check if entry was modified
  get isModified() {
    return this._modified
  }
  
  // Check if entry is marked for deletion
  get isMarkedForDeletion() {
    return this._markedForDeletion
  }
  
  // Proxy all property access to the underlying entry
  get [Symbol.toPrimitive]() {
    return this._entry
  }
  
  // Handle property access dynamically
  get [Symbol.toStringTag]() {
    return 'IterateEntry'
  }
}

// Import managers
import FileHandler from './FileHandler.mjs'
import { QueryManager } from './managers/QueryManager.mjs'
import { ConcurrencyManager } from './managers/ConcurrencyManager.mjs'
import { StatisticsManager } from './managers/StatisticsManager.mjs'
import StreamingProcessor from './managers/StreamingProcessor.mjs'
import TermManager from './managers/TermManager.mjs'

/**
 * InsertSession - Simple batch insertion without memory duplication
 */
class InsertSession {
  constructor(database, sessionOptions = {}) {
    this.database = database
    this.batchSize = sessionOptions.batchSize || 100
    this.enableAutoSave = sessionOptions.enableAutoSave !== undefined ? sessionOptions.enableAutoSave : true
    this.totalInserted = 0
    this.flushing = false
    this.batches = [] // Array of batches to avoid slice() in flush()
    this.currentBatch = [] // Current batch being filled
    this.sessionId = Math.random().toString(36).substr(2, 9)
    
    // Track pending auto-flush operations
    this.pendingAutoFlushes = new Set()
    
    // Register this session as active
    this.database.activeInsertSessions.add(this)
  }

  async add(record) {
    // CRITICAL FIX: Remove the committed check to allow auto-reusability
    // The session should be able to handle multiple commits
    
    if (this.database.destroyed) {
      throw new Error('Database is destroyed')
    }
    
    // Process record
    const finalRecord = { ...record }
    const id = finalRecord.id || this.database.generateId()
    finalRecord.id = id
    
    // Add to current batch
    this.currentBatch.push(finalRecord)
    this.totalInserted++
    
    // If batch is full, move it to batches array and trigger auto-flush
    if (this.currentBatch.length >= this.batchSize) {
      this.batches.push(this.currentBatch)
      this.currentBatch = []
      
      // Auto-flush in background (non-blocking)
      // This ensures batches are flushed automatically without blocking add()
      this.autoFlush().catch(err => {
        // Log error but don't throw - we don't want to break the add() flow
        console.error('Auto-flush error in InsertSession:', err)
      })
    }
    
    return finalRecord
  }

  async autoFlush() {
    // Only flush if not already flushing
    // This method will process all pending batches
    if (this.flushing) return
    
    // Create a promise for this auto-flush operation
    const flushPromise = this._doFlush()
    this.pendingAutoFlushes.add(flushPromise)
    
    // Remove from pending set when complete (success or error)
    flushPromise
      .then(() => {
        this.pendingAutoFlushes.delete(flushPromise)
      })
      .catch((err) => {
        this.pendingAutoFlushes.delete(flushPromise)
        throw err
      })
    
    return flushPromise
  }

  async _doFlush() {
    // Check if database is destroyed or closed before starting
    if (this.database.destroyed || this.database.closed) {
      // Clear batches if database is closed/destroyed
      this.batches = []
      this.currentBatch = []
      return
    }

    // Prevent concurrent flushes - if already flushing, wait for it
    if (this.flushing) {
      // Wait for the current flush to complete
      while (this.flushing) {
        await new Promise(resolve => setTimeout(resolve, 1))
      }
      // After waiting, check if there's anything left to flush
      // If another flush completed everything, we're done
      if (this.batches.length === 0 && this.currentBatch.length === 0) return
      
      // Check again if database was closed during wait
      if (this.database.destroyed || this.database.closed) {
        this.batches = []
        this.currentBatch = []
        return
      }
    }
    
    this.flushing = true

    try {
      // Process continuously until queue is completely empty
      // This handles the case where new data is added during the flush
      while (this.batches.length > 0 || this.currentBatch.length > 0) {
        // Check if database was closed during processing
        if (this.database.destroyed || this.database.closed) {
          // Clear remaining batches
          this.batches = []
          this.currentBatch = []
          return
        }

        // Process all complete batches that exist at this moment
        // Note: new batches may be added to this.batches during this loop
        const batchesToProcess = this.batches.length
        for (let i = 0; i < batchesToProcess; i++) {
          // Check again before each batch
          if (this.database.destroyed || this.database.closed) {
            this.batches = []
            this.currentBatch = []
            return
          }
          
          const batch = this.batches.shift() // Remove from front
          await this.database.insertBatch(batch)
        }

        // Process current batch if it has data
        // Note: new records may be added to currentBatch during processing
        if (this.currentBatch.length > 0) {
          // Check if database was closed
          if (this.database.destroyed || this.database.closed) {
            this.batches = []
            this.currentBatch = []
            return
          }

          // Check if currentBatch reached batchSize during processing
          if (this.currentBatch.length >= this.batchSize) {
            // Move it to batches array and process in next iteration
            this.batches.push(this.currentBatch)
            this.currentBatch = []
            continue
          }
          
          // Process the current batch
          const batchToProcess = this.currentBatch
          this.currentBatch = [] // Clear before processing to allow new adds
          await this.database.insertBatch(batchToProcess)
        }
      }
    } finally {
      this.flushing = false
    }
  }

  async flush() {
    // Wait for any pending auto-flushes to complete first
    await this.waitForAutoFlushes()
    
    // Then do a final flush to ensure everything is processed
    await this._doFlush()
  }

  async waitForAutoFlushes() {
    // Wait for all pending auto-flush operations to complete
    if (this.pendingAutoFlushes.size > 0) {
      await Promise.all(Array.from(this.pendingAutoFlushes))
    }
  }

  async commit() {
    // CRITICAL FIX: Make session auto-reusable by removing committed state
    // Allow multiple commits on the same session
    
    // First, wait for all pending auto-flushes to complete
    await this.waitForAutoFlushes()
    
    // Then flush any remaining data (including currentBatch)
    // This ensures everything is inserted before commit returns
    await this.flush()
    
    // Reset session state for next commit cycle
    const insertedCount = this.totalInserted
    this.totalInserted = 0
    return insertedCount
  }
  
  /**
   * Wait for this session's operations to complete
   */
  async waitForOperations(maxWaitTime = null) {
    const startTime = Date.now()
    const hasTimeout = maxWaitTime !== null && maxWaitTime !== undefined
    
    // Wait for auto-flushes first
    await this.waitForAutoFlushes()
    
    while (this.flushing || this.batches.length > 0 || this.currentBatch.length > 0) {
      // Check timeout only if we have one
      if (hasTimeout && (Date.now() - startTime) >= maxWaitTime) {
        return false
      }
      
      await new Promise(resolve => setTimeout(resolve, 1))
    }
    
    return true
  }
  
  /**
   * Check if this session has pending operations
   */
  hasPendingOperations() {
    return this.pendingAutoFlushes.size > 0 || 
           this.flushing || 
           this.batches.length > 0 || 
           this.currentBatch.length > 0
  }
  
  /**
   * Destroy this session and unregister it
   */
  destroy() {
    // Unregister from database
    this.database.activeInsertSessions.delete(this)
    
    // Clear all data
    this.batches = []
    this.currentBatch = []
    this.totalInserted = 0
    this.flushing = false
    this.pendingAutoFlushes.clear()
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
class Database extends EventEmitter {
  constructor(file, opts = {}) {
    super()
    
    // Generate unique instance ID for debugging
    this.instanceId = Math.random().toString(36).substr(2, 9)
    
    // Initialize state flags
    this.managersInitialized = false
    
    // Track active insert sessions
    this.activeInsertSessions = new Set()
    
    // Set default options
    this.opts = Object.assign({
      // Core options - auto-save removed, user must call save() manually
      // File creation options
      create: opts.create !== false, // Create file if it doesn't exist (default true)
      clear: opts.clear === true, // Clear existing files before loading (default false)
      // Timeout configurations for preventing hangs
      mutexTimeout: opts.mutexTimeout || 15000, // 15 seconds timeout for mutex operations
      maxFlushAttempts: opts.maxFlushAttempts || 50, // Maximum flush attempts before giving up
      // Term mapping options (always enabled and auto-detected from indexes)
      termMappingCleanup: opts.termMappingCleanup !== false, // Clean up orphaned terms on save (enabled by default)
      // Recovery options
      enableRecovery: opts.enableRecovery === true, // Recovery mechanisms disabled by default for large databases
      // Buffer size options for range merging
      maxBufferSize: opts.maxBufferSize || 4 * 1024 * 1024, // 4MB default maximum buffer size for grouped ranges
      // Memory management options (similar to published v1.1.0)
      maxMemoryUsage: opts.maxMemoryUsage || 64 * 1024, // 64KB limit like published version
      maxWriteBufferSize: opts.maxWriteBufferSize || 1000, // Maximum records in writeBuffer
      // Query strategy options
      streamingThreshold: opts.streamingThreshold || 0.8, // Use streaming when limit > 80% of total records
      // Serialization options
      enableArraySerialization: opts.enableArraySerialization !== false, // Enable array serialization by default
      // Index rebuild options
      allowIndexRebuild: opts.allowIndexRebuild === true, // Allow automatic index rebuild when corrupted (default false - throws error)
    }, opts)
    
    // CRITICAL FIX: Initialize AbortController for lifecycle management
    this.abortController = new AbortController()
    this.pendingOperations = new Set()
    this.pendingPromises = new Set()
    this.destroyed = false
    this.destroying = false
    this.closed = false
    this.operationCounter = 0
    
    // CRITICAL FIX: Initialize OperationQueue to prevent race conditions
    this.operationQueue = new OperationQueue(false) // Disable debug mode for queue
    
    // Normalize file path to ensure it ends with .jdb
    this.normalizedFile = this.normalizeFilePath(file)
    
    // Initialize core properties
    this.offsets = [] // Array of byte offsets for each record
    this.indexOffset = 0 // Current position in file for new records
    this.deletedIds = new Set() // Track deleted record IDs
    this.shouldSave = false
    this.isLoading = false
    this.isSaving = false
    this.lastSaveTime = null
    this.initialized = false
    this._offsetRecoveryInProgress = false
    this.writeBufferTotalSize = 0
    
    
    // Initialize managers
    this.initializeManagers()
    
    // Initialize file mutex for thread safety
    this.fileMutex = new Mutex()
    
    // Initialize performance tracking
    this.performanceStats = {
      operations: 0,
      saves: 0,
      loads: 0,
      queryTime: 0,
      saveTime: 0,
      loadTime: 0
    }

    // Initialize integrity correction tracking
    this.integrityCorrections = {
      indexSync: 0,        // index.totalLines vs offsets.length corrections
      indexInconsistency: 0, // Index record count vs offsets mismatch
      writeBufferForced: 0, // WriteBuffer not cleared after save
      indexSaveFailures: 0, // Failed to save index data
      dataIntegrity: 0,    // General data integrity issues
      utf8Recovery: 0,     // UTF-8 decoding failures recovered
      jsonRecovery: 0      // JSON parsing failures recovered
    }


    // Initialize usage stats for QueryManager
    this.usageStats = {
      totalQueries: 0,
      indexedQueries: 0,
      streamingQueries: 0,
      indexedAverageTime: 0,
      streamingAverageTime: 0
    }

    // Note: Validation will be done after configuration conversion in initializeManagers()
  }

  /**
   * Validate field and index configuration
   */
  validateIndexConfiguration() {
    // Validate fields configuration
    if (this.opts.fields && typeof this.opts.fields === 'object') {
      this.validateFieldTypes(this.opts.fields, 'fields')
    }

    // Validate indexes configuration (legacy support)
    if (this.opts.indexes && typeof this.opts.indexes === 'object') {
      this.validateFieldTypes(this.opts.indexes, 'indexes')
    }

    // Validate indexes array (new format) - but only if we have fields
    if (this.opts.originalIndexes && Array.isArray(this.opts.originalIndexes)) {
      if (this.opts.fields) {
        this.validateIndexFields(this.opts.originalIndexes)
      } else if (this.opts.debugMode) {
        console.log('⚠️  Skipping index field validation because no fields configuration was provided')
      }
    }

    if (this.opts.debugMode) {
      const fieldCount = this.opts.fields ? Object.keys(this.opts.fields).length : 0
      const indexCount = Array.isArray(this.opts.indexes) ? this.opts.indexes.length : 
                        (this.opts.indexes && typeof this.opts.indexes === 'object' ? Object.keys(this.opts.indexes).length : 0)
      if (fieldCount > 0 || indexCount > 0) {
        console.log(`✅ Configuration validated: ${fieldCount} fields, ${indexCount} indexes [${this.instanceId}]`)
      }
    }
  }

  /**
   * Validate field types
   */
  validateFieldTypes(fields, configType) {
    const supportedTypes = ['string', 'number', 'boolean', 'array:string', 'array:number', 'array:boolean', 'array', 'object', 'auto']
    const errors = []

    for (const [fieldName, fieldType] of Object.entries(fields)) {
      if (fieldType === 'auto') {
        continue
      }

      // Check if type is supported
      if (!supportedTypes.includes(fieldType)) {
        errors.push(`Unsupported ${configType} type '${fieldType}' for field '${fieldName}'. Supported types: ${supportedTypes.join(', ')}`)
      }

      // Warn about legacy array type but don't error
      if (fieldType === 'array') {
        if (this.opts.debugMode) {
          console.log(`⚠️  Legacy array type '${fieldType}' for field '${fieldName}'. Consider using 'array:string' for better performance.`)
        }
      }

      // Check for common mistakes
      if (fieldType === 'array:') {
        errors.push(`Incomplete array type '${fieldType}' for field '${fieldName}'. Must specify element type after colon: array:string, array:number, or array:boolean`)
      }
    }

    if (errors.length > 0) {
      throw new Error(`${configType.charAt(0).toUpperCase() + configType.slice(1)} configuration errors:\n${errors.map(e => `  - ${e}`).join('\n')}`)
    }
  }

  /**
   * Validate index fields array
   */
  validateIndexFields(indexFields) {
    if (!this.opts.fields) {
      throw new Error('Index fields array requires fields configuration. Use: { fields: {...}, indexes: [...] }')
    }

    const availableFields = Object.keys(this.opts.fields)
    const errors = []

    for (const fieldName of indexFields) {
      if (!availableFields.includes(fieldName)) {
        errors.push(`Index field '${fieldName}' not found in fields configuration. Available fields: ${availableFields.join(', ')}`)
      }
    }

    if (errors.length > 0) {
      throw new Error(`Index configuration errors:\n${errors.map(e => `  - ${e}`).join('\n')}`)
    }
  }

  /**
   * Prepare index configuration for IndexManager
   */
  prepareIndexConfiguration() {
    if (Array.isArray(this.opts.indexes)) {
      const indexedFields = {}
      const originalIndexes = [...this.opts.indexes]
      const hasFieldConfig = this.opts.fields && typeof this.opts.fields === 'object'

      for (const fieldName of this.opts.indexes) {
        if (hasFieldConfig && this.opts.fields[fieldName]) {
          indexedFields[fieldName] = this.opts.fields[fieldName]
        } else {
          indexedFields[fieldName] = 'auto'
        }
      }

      this.opts.originalIndexes = originalIndexes
      this.opts.indexes = indexedFields

      if (this.opts.debugMode) {
        console.log(`🔍 Normalized indexes array to object: ${Object.keys(indexedFields).join(', ')} [${this.instanceId}]`)
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
        console.log(`⚠️  initializeManagers() called again - skipping to prevent corruption [${this.instanceId}]`)
      }
      return
    }
    
    // Handle legacy 'schema' option migration
    if (this.opts.schema) {
      // If fields is already provided and valid, ignore schema
      if (this.opts.fields && typeof this.opts.fields === 'object' && Object.keys(this.opts.fields).length > 0) {
        if (this.opts.debugMode) {
          console.log(`⚠️  Both 'schema' and 'fields' options provided. Ignoring 'schema' and using 'fields'. [${this.instanceId}]`)
        }
      } else if (Array.isArray(this.opts.schema)) {
        // Schema as array is no longer supported
        throw new Error('The "schema" option as an array is no longer supported. Please use "fields" as an object instead. Example: { fields: { id: "number", name: "string" } }')
      } else if (typeof this.opts.schema === 'object' && this.opts.schema !== null) {
        // Schema as object - migrate to fields
        this.opts.fields = { ...this.opts.schema }
        if (this.opts.debugMode) {
          console.log(`⚠️  Migrated 'schema' option to 'fields'. Please update your code to use 'fields' instead of 'schema'. [${this.instanceId}]`)
        }
      } else {
        throw new Error('The "schema" option must be an object. Example: { schema: { id: "number", name: "string" } }')
      }
    }
    
    // Validate that fields is provided (mandatory)
    if (!this.opts.fields || typeof this.opts.fields !== 'object' || Object.keys(this.opts.fields).length === 0) {
      throw new Error('The "fields" option is mandatory and must be an object with at least one field definition. Example: { fields: { id: "number", name: "string" } }')
    }
    
    // CRITICAL FIX: Initialize serializer first - this was missing and causing crashes
    this.serializer = new Serializer(this.opts)
    
    // Initialize schema for array-based serialization
    if (this.opts.enableArraySerialization !== false) {
      this.initializeSchema()
    }
    
    // Initialize TermManager - always enabled for optimal performance
    this.termManager = new TermManager()
    
    // Auto-detect term mapping fields from indexes
    const termMappingFields = this.getTermMappingFields()
    this.termManager.termMappingFields = termMappingFields
    this.opts.termMapping = true // Always enable term mapping for optimal performance
    
    // Validation: Ensure all array:string indexed fields are in term mapping fields
    if (this.opts.indexes) {
      const arrayStringFields = []
      for (const [field, type] of Object.entries(this.opts.indexes)) {
        if (type === 'array:string' && !termMappingFields.includes(field)) {
          arrayStringFields.push(field)
        }
      }
      if (arrayStringFields.length > 0) {
        if (this.opts.debugMode) {
          console.warn(`⚠️  Warning: The following array:string indexed fields were not added to term mapping: ${arrayStringFields.join(', ')}. This may impact performance.`)
        }
      }
    }
    
    if (this.opts.debugMode) {
      if (termMappingFields.length > 0) {
        console.log(`🔍 TermManager initialized for fields: ${termMappingFields.join(', ')} [${this.instanceId}]`)
      } else {
        console.log(`🔍 TermManager initialized (no array:string fields detected) [${this.instanceId}]`)
      }
    }
    
    // Prepare index configuration for IndexManager
    this.prepareIndexConfiguration()

    // Validate configuration after conversion
    this.validateIndexConfiguration()

    // Initialize IndexManager with database reference for term mapping
    this.indexManager = new IndexManager(this.opts, null, this)
    if (this.opts.debugMode) {
        console.log(`🔍 IndexManager initialized with fields: ${this.indexManager.indexedFields.join(', ')} [${this.instanceId}]`)
    }
    
    // Mark managers as initialized
    this.managersInitialized = true
    this.indexOffset = 0
    this.writeBuffer = []
    this.writeBufferOffsets = [] // Track offsets for writeBuffer records
    this.writeBufferSizes = []   // Track sizes for writeBuffer records
    this.writeBufferTotalSize = 0
    this.isInsideOperationQueue = false // Flag to prevent deadlock in save() calls
    
    // Initialize other managers
    this.fileHandler = new FileHandler(this.normalizedFile, this.fileMutex, this.opts)
    this.queryManager = new QueryManager(this)
    this.concurrencyManager = new ConcurrencyManager(this.opts)
    this.statisticsManager = new StatisticsManager(this, this.opts)
    this.streamingProcessor = new StreamingProcessor(this.opts)
  }

  /**
   * Get term mapping fields from configuration or indexes (auto-detected)
   * @returns {string[]} Array of field names that use term mapping
   */
  getTermMappingFields() {
    // If termMappingFields is explicitly configured, use it
    if (this.opts.termMappingFields && Array.isArray(this.opts.termMappingFields)) {
      return [...this.opts.termMappingFields]
    }

    // Auto-detect fields that benefit from term mapping from indexes
    if (!this.opts.indexes) return []

    const termMappingFields = []

    for (const [field, type] of Object.entries(this.opts.indexes)) {
      // Fields that should use term mapping (only array fields)
      if (type === 'array:string') {
        termMappingFields.push(field)
      }
    }

    return termMappingFields
  }

  /**
   * CRITICAL FIX: Validate database state before critical operations
   * Prevents crashes from undefined methods and invalid states
   */
  validateState() {
    if (this.destroyed) {
      throw new Error('Database is destroyed')
    }
    
    if (this.closed) {
      throw new Error('Database is closed. Call init() to reopen it.')
    }
    
    // Allow operations during destroying phase for proper cleanup
    
    if (!this.serializer) {
      throw new Error('Database serializer not initialized - this indicates a critical bug')
    }
    
    if (!this.normalizedFile) {
      throw new Error('Database file path not set - this indicates file path management failure')
    }
    
    if (!this.fileHandler) {
      throw new Error('Database file handler not initialized')
    }
    
    if (!this.indexManager) {
      throw new Error('Database index manager not initialized')
    }
    
    return true
  }

  /**
   * CRITICAL FIX: Ensure file path is valid and accessible
   * Prevents file path loss issues mentioned in crash report
   */
  ensureFilePath() {
    if (!this.normalizedFile) {
      throw new Error('Database file path is missing after initialization - this indicates a critical file path management failure')
    }
    return this.normalizedFile
  }

  /**
   * Normalize file path to ensure it ends with .jdb
   */
  normalizeFilePath(file) {
    if (!file) return null
    return file.endsWith('.jdb') ? file : `${file}.jdb`
  }

  /**
   * Initialize the database
   */
  async initialize() {
    // Check if database is destroyed first (before checking initialized)
    if (this.destroyed) {
      throw new Error('Cannot initialize destroyed database. Use a new instance instead.')
    }
    
    if (this.initialized) return
    
    // Prevent concurrent initialization - wait for ongoing init to complete
    if (this.isLoading) {
      if (this.opts.debugMode) {
        console.log('🔄 init() already in progress - waiting for completion')
      }
      // Wait for ongoing initialization to complete
      while (this.isLoading) {
        await new Promise(resolve => setTimeout(resolve, 10))
      }
      // Check if initialization completed successfully
      if (this.initialized) {
        if (this.opts.debugMode) {
          console.log('✅ Concurrent init() completed - database is now initialized')
        }
        return
      }
      // If we get here, initialization failed - we can try again
    }
    
    try {
      this.isLoading = true
      
      // Reset closed state when reinitializing
      this.closed = false

      // Initialize managers (protected against double initialization)
      this.initializeManagers()
      
      // Handle clear option - delete existing files before loading
      if (this.opts.clear && this.normalizedFile) {
        await this.clearExistingFiles()
      }
      
      // Check file existence and handle create option
      if (this.normalizedFile) {
        const fileExists = await this.fileHandler.exists()
        
        if (!fileExists) {
          if (!this.opts.create) {
            throw new Error(`Database file '${this.normalizedFile}' does not exist and create option is disabled`)
          }
          // File will be created when first data is written
        } else {
          // Load existing data if file exists
          await this.load()
        }
      }

      // CRITICAL INTEGRITY CHECK: Ensure IndexManager is consistent with loaded offsets
      // This must happen immediately after load() to prevent any subsequent operations from seeing inconsistent state
      if (this.indexManager && this.offsets && this.offsets.length > 0) {
        const currentTotalLines = this.indexManager.totalLines || 0
        if (currentTotalLines !== this.offsets.length) {
          this.indexManager.setTotalLines(this.offsets.length)
          if (this.opts.debugMode) {
            console.log(`🔧 Post-load integrity sync: IndexManager totalLines ${currentTotalLines} → ${this.offsets.length}`)
          }
        }
      }

      // Manual save is now the default behavior

      // CRITICAL FIX: Ensure IndexManager totalLines is consistent with offsets
      // This prevents data integrity issues when database is initialized without existing data
      if (this.indexManager && this.offsets) {
        this.indexManager.setTotalLines(this.offsets.length)
        if (this.opts.debugMode) {
          console.log(`🔧 Initialized index totalLines to ${this.offsets.length}`)
        }
      }

      this.initialized = true
      this.emit('initialized')

      if (this.opts.debugMode) {
        console.log(`✅ Database initialized with ${this.writeBuffer.length} records`)
      }
    } catch (error) {
      console.error('Failed to initialize database:', error)
      throw error
    } finally {
      this.isLoading = false
    }
  }

  /**
   * Validate that the database is initialized before performing operations
   * @param {string} operation - The operation being attempted
   * @throws {Error} If database is not initialized
   */
  _validateInitialization(operation) {
    if (this.destroyed) {
      throw new Error(`❌ Cannot perform '${operation}' on a destroyed database. Create a new instance instead.`)
    }
    
    if (this.closed) {
      throw new Error(`❌ Database is closed. Call 'await db.init()' to reopen it before performing '${operation}' operations.`)
    }
    
    if (!this.initialized) {
      const errorMessage = `❌ Database not initialized. Call 'await db.init()' before performing '${operation}' operations.\n\n` +
                          `Example:\n` +
                          `  const db = new Database('./myfile.jdb')\n` +
                          `  await db.init()  // ← Required before any operations\n` +
                          `  await db.insert({ name: 'test' })  // ← Now you can use database operations\n\n` +
                          `File: ${this.normalizedFile || 'unknown'}`
      
      throw new Error(errorMessage)
    }
  }

  /**
   * Clear existing database files (.jdb and .idx.jdb)
   */
  async clearExistingFiles() {
    if (!this.normalizedFile) return
    
    try {
      // Clear main database file
      if (await this.fileHandler.exists()) {
        await this.fileHandler.delete()
        if (this.opts.debugMode) {
          console.log(`🗑️ Cleared database file: ${this.normalizedFile}`)
        }
      }
      
      // Clear index file
      const idxPath = this.normalizedFile.replace('.jdb', '.idx.jdb')
      const idxFileHandler = new FileHandler(idxPath, this.fileMutex, this.opts)
      if (await idxFileHandler.exists()) {
        await idxFileHandler.delete()
        if (this.opts.debugMode) {
          console.log(`🗑️ Cleared index file: ${idxPath}`)
        }
      }
      
      // Reset internal state
      this.offsets = []
      this.indexOffset = 0
      this.deletedIds.clear()
      this.shouldSave = false
      
      // Create empty files to ensure they exist
      await this.fileHandler.writeAll('')
      await idxFileHandler.writeAll('')
      
      if (this.opts.debugMode) {
        console.log('🗑️ Database cleared successfully')
      }
    } catch (error) {
      console.error('Failed to clear existing files:', error)
      throw error
    }
  }

  /**
   * Load data from file
   */
  async load() {
    if (!this.normalizedFile) return
    
    
    try {
      const startTime = Date.now()
      this.isLoading = true
      
      // Don't load the entire file - just initialize empty state
      // The actual record count will come from loaded offsets
      this.writeBuffer = [] // writeBuffer is only for new unsaved records
      this.writeBufferOffsets = []
      this.writeBufferSizes = []
      this.writeBufferTotalSize = 0
      
      // recordCount will be determined from loaded offsets
      // If no offsets were loaded, we'll count records only if needed
      
      // Load index data if available (always try to load offsets, even without indexed fields)
      if (this.indexManager) {
        const idxPath = this.normalizedFile.replace('.jdb', '.idx.jdb')
        try {
          const idxFileHandler = new FileHandler(idxPath, this.fileMutex, this.opts)
          
          // Check if file exists BEFORE trying to read it
          const fileExists = await idxFileHandler.exists()
          if (!fileExists) {
            // File doesn't exist - this will be handled by catch block
            throw new Error('Index file does not exist')
          }
          
          const idxData = await idxFileHandler.readAll()
          
          // If file exists but is empty or has no content, treat as corrupted
          if (!idxData || !idxData.trim()) {
            // File exists but is empty - treat as corrupted
            const fileExists = await this.fileHandler.exists()
            if (fileExists) {
              const stats = await this.fileHandler.getFileStats()
              if (stats && stats.size > 0) {
                // Data file has content but index is empty - corrupted
                if (!this.opts.allowIndexRebuild) {
                  throw new Error(
                    `Index file is corrupted: ${idxPath} exists but contains no index data, ` +
                    `while the data file has ${stats.size} bytes. ` +
                    `Set allowIndexRebuild: true to automatically rebuild the index, ` +
                    `or manually fix/delete the corrupted index file.`
                  )
                }
                // Schedule rebuild if allowed
                if (this.opts.debugMode) {
                  console.log(`⚠️ Index file exists but is empty while data file has ${stats.size} bytes - scheduling rebuild`)
                }
                this._scheduleIndexRebuild()
                // Continue execution - rebuild will happen on first query
                // Don't return - let the code continue to load other things if needed
              }
            }
            // If data file is also empty, just continue (no error needed)
            // Don't return - let the code continue to load other things if needed
          } else {
            // File has content - parse and load it
            const parsedIdxData = JSON.parse(idxData)
            
            // Always load offsets if available (even without indexed fields)
            if (parsedIdxData.offsets && Array.isArray(parsedIdxData.offsets)) {
              this.offsets = parsedIdxData.offsets
              // CRITICAL FIX: Update IndexManager totalLines to match offsets length
              // This ensures queries and length property work correctly even if offsets are reset later
              if (this.indexManager) {
                this.indexManager.setTotalLines(this.offsets.length)
                if (this.opts.debugMode) {
                  console.log(`📂 Loaded ${this.offsets.length} offsets from ${idxPath}, synced IndexManager totalLines`)
                }
              }
            }
            
            // Load indexOffset for proper range calculations
            if (parsedIdxData.indexOffset !== undefined) {
              this.indexOffset = parsedIdxData.indexOffset
              if (this.opts.debugMode) {
                console.log(`📂 Loaded indexOffset: ${this.indexOffset} from ${idxPath}`)
              }
            }
            
            // Load configuration from .idx file if database exists
            // CRITICAL: Load config FIRST so indexes are available for term mapping detection
            if (parsedIdxData.config) {
              const config = parsedIdxData.config
              
              // Override constructor options with saved configuration
              if (config.fields) {
                this.opts.fields = config.fields
                if (this.opts.debugMode) {
                  console.log(`📂 Loaded fields config from ${idxPath}:`, Object.keys(config.fields))
                }
              }
              
              if (config.indexes) {
                this.opts.indexes = config.indexes
                if (this.indexManager) {
                  this.indexManager.setIndexesConfig(config.indexes)
                }
                if (this.opts.debugMode) {
                  console.log(`📂 Loaded indexes config from ${idxPath}:`, Object.keys(config.indexes))
                }
              }
              
              // CRITICAL FIX: Update term mapping fields AFTER loading indexes from config
              // This ensures termManager knows which fields use term mapping
              // (getTermMappingFields() was called during init() before indexes were loaded)
              if (this.termManager && config.indexes) {
                const termMappingFields = this.getTermMappingFields()
                this.termManager.termMappingFields = termMappingFields
                if (this.opts.debugMode && termMappingFields.length > 0) {
                  console.log(`🔍 Updated term mapping fields after loading indexes: ${termMappingFields.join(', ')}`)
                }
              }
            }
            
            // Load term mapping data from .idx file if it exists
            // CRITICAL: Load termMapping even if index is empty (terms are needed for queries)
            // NOTE: termMappingFields should already be set above from config.indexes
            if (parsedIdxData.termMapping && this.termManager && this.termManager.termMappingFields && this.termManager.termMappingFields.length > 0) {
              await this.termManager.loadTerms(parsedIdxData.termMapping)
              if (this.opts.debugMode) {
                console.log(`📂 Loaded term mapping from ${idxPath} (${Object.keys(parsedIdxData.termMapping).length} terms)`)
              }
            }
            
            // Load index data only if available and we have indexed fields
            if (parsedIdxData && parsedIdxData.index && this.indexManager.indexedFields && this.indexManager.indexedFields.length > 0) {
              this.indexManager.load(parsedIdxData.index)
              
              if (this.opts.debugMode) {
                console.log(`📂 Loaded index data from ${idxPath}`)
              }
              
              // Check if loaded index is actually empty (corrupted)
              let hasAnyIndexData = false
              for (const field of this.indexManager.indexedFields) {
                if (this.indexManager.hasUsableIndexData(field)) {
                  hasAnyIndexData = true
                  break
                }
              }
              
              if (this.opts.debugMode) {
                console.log(`📊 Index check: hasAnyIndexData=${hasAnyIndexData}, indexedFields=${this.indexManager.indexedFields.join(',')}`)
              }
              
              // Schedule rebuild if index is empty AND file exists with data
              if (!hasAnyIndexData) {
                // Check if the actual .jdb file has data
                const fileExists = await this.fileHandler.exists()
                if (this.opts.debugMode) {
                  console.log(`📊 File check: exists=${fileExists}`)
                }
                if (fileExists) {
                  const stats = await this.fileHandler.getFileStats()
                  if (this.opts.debugMode) {
                    console.log(`📊 File stats: size=${stats?.size}`)
                  }
                  if (stats && stats.size > 0) {
                    // File has data but index is empty - corrupted index detected
                    if (!this.opts.allowIndexRebuild) {
                      const idxPath = this.normalizedFile.replace('.jdb', '.idx.jdb')
                      throw new Error(
                        `Index file is corrupted: ${idxPath} exists but contains no index data, ` +
                        `while the data file has ${stats.size} bytes. ` +
                        `Set allowIndexRebuild: true to automatically rebuild the index, ` +
                        `or manually fix/delete the corrupted index file.`
                      )
                    }
                    // Schedule rebuild if allowed
                    if (this.opts.debugMode) {
                      console.log(`⚠️ Index loaded but empty while file has ${stats.size} bytes - scheduling rebuild`)
                    }
                    this._scheduleIndexRebuild()
                  }
                }
              }
            }
            
            // Continue with remaining config loading
            if (parsedIdxData.config) {
              const config = parsedIdxData.config
              
              if (config.originalIndexes) {
                this.opts.originalIndexes = config.originalIndexes
                if (this.opts.debugMode) {
                  console.log(`📂 Loaded originalIndexes config from ${idxPath}:`, config.originalIndexes.length, 'indexes')
                }
              }
              
              // Reinitialize schema from saved configuration (only if fields not provided)
              // Note: fields option takes precedence over saved schema
              if (!this.opts.fields && config.schema && this.serializer) {
                this.serializer.initializeSchema(config.schema)
                if (this.opts.debugMode) {
                  console.log(`📂 Loaded schema from ${idxPath}:`, config.schema.join(', '))
                }
              } else if (this.opts.fields && this.serializer) {
                // Use fields option instead of saved schema
                const fieldNames = Object.keys(this.opts.fields)
                if (fieldNames.length > 0) {
                  this.serializer.initializeSchema(fieldNames)
                  if (this.opts.debugMode) {
                    console.log(`📂 Schema initialized from fields option:`, fieldNames.join(', '))
                  }
                }
              }
            }
          }
        } catch (idxError) {
          // Index file doesn't exist or is corrupted, rebuild from data
          // BUT: if error is about rebuild being disabled, re-throw it immediately
          if (idxError.message && (idxError.message.includes('allowIndexRebuild') || idxError.message.includes('corrupted'))) {
            throw idxError
          }
          
          // If error is "Index file does not exist", check if we should throw or rebuild
          if (idxError.message && idxError.message.includes('does not exist')) {
            // Check if the actual .jdb file has data that needs indexing
            try {
              const fileExists = await this.fileHandler.exists()
              if (fileExists) {
                const stats = await this.fileHandler.getFileStats()
                if (stats && stats.size > 0) {
                  // File has data but index is missing
                  if (!this.opts.allowIndexRebuild) {
                    const idxPath = this.normalizedFile.replace('.jdb', '.idx.jdb')
                    throw new Error(
                      `Index file is missing or corrupted: ${idxPath} does not exist or is invalid, ` +
                      `while the data file has ${stats.size} bytes. ` +
                      `Set allowIndexRebuild: true to automatically rebuild the index, ` +
                      `or manually create/fix the index file.`
                    )
                  }
                  // Schedule rebuild if allowed
                  if (this.opts.debugMode) {
                    console.log(`⚠️ .jdb file has ${stats.size} bytes but index missing - scheduling rebuild`)
                  }
                  this._scheduleIndexRebuild()
                  return // Exit early
                }
              }
            } catch (statsError) {
              if (this.opts.debugMode) {
                console.log('⚠️ Could not check file stats:', statsError.message)
              }
              // Re-throw if it's our error about rebuild being disabled
              if (statsError.message && statsError.message.includes('allowIndexRebuild')) {
                throw statsError
              }
            }
            // If no data file or empty, just continue (no error needed)
            return
          }
          
          if (this.opts.debugMode) {
            console.log('📂 No index file found or corrupted, checking if rebuild is needed...')
          }
          
          // Check if the actual .jdb file has data that needs indexing
          try {
            const fileExists = await this.fileHandler.exists()
            if (fileExists) {
              const stats = await this.fileHandler.getFileStats()
              if (stats && stats.size > 0) {
                // File has data but index is missing or corrupted
                if (!this.opts.allowIndexRebuild) {
                  const idxPath = this.normalizedFile.replace('.jdb', '.idx.jdb')
                  throw new Error(
                    `Index file is missing or corrupted: ${idxPath} does not exist or is invalid, ` +
                    `while the data file has ${stats.size} bytes. ` +
                    `Set allowIndexRebuild: true to automatically rebuild the index, ` +
                    `or manually create/fix the index file.`
                  )
                }
                // Schedule rebuild if allowed
                if (this.opts.debugMode) {
                  console.log(`⚠️ .jdb file has ${stats.size} bytes but index missing - scheduling rebuild`)
                }
                this._scheduleIndexRebuild()
              }
            }
          } catch (statsError) {
            if (this.opts.debugMode) {
              console.log('⚠️ Could not check file stats:', statsError.message)
            }
            // Re-throw if it's our error about rebuild being disabled
            if (statsError.message && statsError.message.includes('allowIndexRebuild')) {
              throw statsError
            }
          }
        }
      } else {
        // No indexed fields, no need to rebuild indexes
      }
      
      this.performanceStats.loads++
      this.performanceStats.loadTime += Date.now() - startTime
      this.emit('loaded', this.writeBuffer.length)
    } catch (error) {
      console.error('Failed to load database:', error)
      throw error
    } finally {
      this.isLoading = false
    }
  }


  /**
   * Save data to file
   * @param {boolean} inQueue - Whether to execute within the operation queue (default: false)
   */
  async save(inQueue = false) {
    this._validateInitialization('save')
    
    if (this.opts.debugMode) {
      console.log(`💾 save() called: writeBuffer.length=${this.writeBuffer.length}, offsets.length=${this.offsets.length}`)
    }
    
    // CRITICAL FIX: Wait for all active insert sessions to complete their auto-flushes
    // This prevents race conditions where save() writes data while auto-flushes are still adding to writeBuffer
    if (this.activeInsertSessions && this.activeInsertSessions.size > 0) {
      if (this.opts.debugMode) {
        console.log(`⏳ save(): Waiting for ${this.activeInsertSessions.size} active insert sessions to complete auto-flushes`)
      }
      
      const sessionPromises = Array.from(this.activeInsertSessions).map(session => 
        session.waitForAutoFlushes().catch(err => {
          if (this.opts.debugMode) {
            console.warn(`⚠️ save(): Error waiting for insert session: ${err.message}`)
          }
        })
      )
      
      await Promise.all(sessionPromises)
      
      if (this.opts.debugMode) {
        console.log(`✅ save(): All insert sessions completed auto-flushes`)
      }
    }
    
    // Auto-save removed - no need to pause anything
    
    try {
      // CRITICAL FIX: Wait for any ongoing save operations to complete
      if (this.isSaving) {
        if (this.opts.debugMode) {
          console.log('💾 save(): waiting for previous save to complete')
        }
        // Wait for previous save to complete
        while (this.isSaving) {
          await new Promise(resolve => setTimeout(resolve, 10))
        }
        
        // Check if data changed since the previous save completed
        const hasDataToSave = this.writeBuffer.length > 0 || this.deletedIds.size > 0
        const needsStructureCreation = this.indexManager && this.indexManager.indexedFields && this.indexManager.indexedFields.length > 0
        
        if (!hasDataToSave && !needsStructureCreation) {
          if (this.opts.debugMode) {
            console.log('💾 Save: No new data to save since previous save completed')
          }
          return // Nothing new to save
        }
      }
      
      // CRITICAL FIX: Check if there's actually data to save before proceeding
      // But allow save if we need to create database structure (index files, etc.)
      const hasDataToSave = this.writeBuffer.length > 0 || this.deletedIds.size > 0
      const needsStructureCreation = this.indexManager && this.indexManager.indexedFields && this.indexManager.indexedFields.length > 0
      
      if (!hasDataToSave && !needsStructureCreation) {
        if (this.opts.debugMode) {
          console.log('💾 Save: No data to save (writeBuffer empty and no deleted records)')
        }
        return // Nothing to save
      }
      
      if (inQueue) {
        if (this.opts.debugMode) {
          console.log(`💾 save(): executing in queue`)
        }
        return this.operationQueue.enqueue(async () => {
          return this._doSave()
        })
      } else {
        if (this.opts.debugMode) {
          console.log(`💾 save(): calling _doSave() directly`)
        }
        return this._doSave()
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
    if (this.destroyed) return
    
    // CRITICAL FIX: Use atomic check-and-set to prevent concurrent save operations
    if (this.isSaving) {
      if (this.opts.debugMode) {
        console.log('💾 _doSave: Save operation already in progress, skipping')
      }
      return
    }
    
    // CRITICAL FIX: Check if there's actually data to save or structure to create
    const hasDataToSave = this.writeBuffer.length > 0 || this.deletedIds.size > 0
    const needsStructureCreation = this.indexManager && this.indexManager.indexedFields && this.indexManager.indexedFields.length > 0
    
    if (!hasDataToSave && !needsStructureCreation) {
      if (this.opts.debugMode) {
        console.log('💾 _doSave: No data to save (writeBuffer empty and no deleted records)')
      }
      return // Nothing to save
    }
    
    // CRITICAL FIX: Set saving flag immediately to prevent race conditions
    this.isSaving = true
    
    try {
      const startTime = Date.now()
      
      // CRITICAL FIX: Ensure file path is valid
      this.ensureFilePath()
      
      // CRITICAL FIX: Wait for ALL pending operations to complete before save
      await this._waitForPendingOperations()
            
      // CRITICAL FIX: Capture writeBuffer and deletedIds at the start to prevent race conditions
      const writeBufferSnapshot = [...this.writeBuffer]
      // CRITICAL FIX: Normalize deleted IDs to strings for consistent comparison
      const deletedIdsSnapshot = new Set(Array.from(this.deletedIds).map(id => String(id)))
      
      // OPTIMIZATION: Process pending index updates in batch before save
      if (this.pendingIndexUpdates && this.pendingIndexUpdates.length > 0) {
        if (this.opts.debugMode) {
          console.log(`💾 Save: Processing ${this.pendingIndexUpdates.length} pending index updates in batch`)
        }
        
        // Extract records and line numbers for batch processing
        const records = this.pendingIndexUpdates.map(update => update.record)
        const startLineNumber = this.pendingIndexUpdates[0].lineNumber
        
        // Process index updates in batch
        await this.indexManager.addBatch(records, startLineNumber)
        
        // Clear pending updates
        this.pendingIndexUpdates = []
      }
      
      // CRITICAL FIX: DO NOT flush writeBuffer before processing existing records
      // This prevents duplicating updated records in the file.
      // The _streamExistingRecords() will handle replacing old records with updated ones from writeBufferSnapshot.
      // After processing, all records (existing + updated + new) will be written to file in one operation.
      if (this.opts.debugMode) {
        console.log(`💾 Save: writeBufferSnapshot captured with ${writeBufferSnapshot.length} records (will be processed with existing records)`)
      }
      
      // OPTIMIZATION: Parallel operations - cleanup and data preparation
      let allData = []
      let orphanedCount = 0
      
      // Check if there are records to save from writeBufferSnapshot
      // CRITICAL FIX: Process writeBufferSnapshot records (both new and updated) with existing records
      // Updated records will replace old ones via _streamExistingRecords, new records will be added
      if (this.opts.debugMode) {
        console.log(`💾 Save: writeBuffer.length=${this.writeBuffer.length}, writeBufferSnapshot.length=${writeBufferSnapshot.length}`)
      }
      if (this.writeBuffer.length > 0 || writeBufferSnapshot.length > 0) {
        if (this.opts.debugMode) {
          console.log(`💾 Save: WriteBuffer has ${writeBufferSnapshot.length} records, using streaming approach`)
        }
        
        // Note: processTermMapping is already called during insert/update operations
        // No need to call it again here to avoid double processing
        
        // OPTIMIZATION: Check if we can skip reading existing records
        // Only use streaming if we have existing records AND we're not just appending new records
        const hasExistingRecords = this.indexOffset > 0 && this.offsets.length > 0 && writeBufferSnapshot.length > 0
        
        if (!hasExistingRecords && deletedIdsSnapshot.size === 0) {
          // OPTIMIZATION: No existing records to read, just use writeBuffer
          allData = [...writeBufferSnapshot]
        } else {
          // OPTIMIZATION: Parallel operations - cleanup and streaming
          const parallelOperations = []
          
          // Add term cleanup if enabled
          if (this.opts.termMappingCleanup && this.termManager) {
            parallelOperations.push(
              Promise.resolve().then(() => {
                orphanedCount = this.termManager.cleanupOrphanedTerms()
                if (this.opts.debugMode && orphanedCount > 0) {
                  console.log(`🧹 Cleaned up ${orphanedCount} orphaned terms`)
                }
              })
            )
          }
          
          // Add streaming operation
          parallelOperations.push(
            this._streamExistingRecords(deletedIdsSnapshot, writeBufferSnapshot).then(existingRecords => {
              // CRITICAL FIX: _streamExistingRecords already handles updates via updatedRecordsMap
              // So existingRecords already contains updated records from writeBufferSnapshot
              // We only need to add records from writeBufferSnapshot that are NEW (not updates)
              allData = [...existingRecords]
              
              // OPTIMIZATION: Use Set for faster lookups of existing record IDs
              // CRITICAL FIX: Normalize IDs to strings for consistent comparison
              const existingRecordIds = new Set(existingRecords.filter(r => r && r.id).map(r => String(r.id)))
              
              // CRITICAL FIX: Create a map of records in existingRecords by ID for comparison
              const existingRecordsById = new Map()
              existingRecords.forEach(r => {
                if (r && r.id) {
                  existingRecordsById.set(String(r.id), r)
                }
              })
              
              // Add only NEW records from writeBufferSnapshot (not updates, as those are already in existingRecords)
              // CRITICAL FIX: Also ensure that if an updated record wasn't properly replaced, we replace it now
              for (const record of writeBufferSnapshot) {
                if (!record || !record.id) continue
                if (deletedIdsSnapshot.has(String(record.id))) continue
                
                const recordIdStr = String(record.id)
                const existingRecord = existingRecordsById.get(recordIdStr)
                
                if (!existingRecord) {
                  // This is a new record, not an update
                  allData.push(record)
                  if (this.opts.debugMode) {
                    console.log(`💾 Save: Adding NEW record to allData:`, { id: recordIdStr, price: record.price, app_id: record.app_id, currency: record.currency })
                  }
                } else {
                  // This is an update - verify that existingRecords contains the updated version
                  // If not, replace it (this handles edge cases where substitution might have failed)
                  const existingIndex = allData.findIndex(r => r && r.id && String(r.id) === recordIdStr)
                  if (existingIndex !== -1) {
                    // Verify if the existing record is actually the updated one
                    // Compare key fields to detect if replacement is needed
                    const needsReplacement = JSON.stringify(allData[existingIndex]) !== JSON.stringify(record)
                    if (needsReplacement) {
                      if (this.opts.debugMode) {
                        console.log(`💾 Save: REPLACING existing record with updated version in allData:`, {
                          old: { id: String(allData[existingIndex].id), price: allData[existingIndex].price },
                          new: { id: recordIdStr, price: record.price }
                        })
                      }
                      allData[existingIndex] = record
                    } else if (this.opts.debugMode) {
                      console.log(`💾 Save: Record already correctly updated in allData:`, { id: recordIdStr })
                    }
                  }
                }
              }
            })
          )
          
          // Execute parallel operations
          await Promise.all(parallelOperations)
        }
      } else {
        // CRITICAL FIX: When writeBuffer is empty, use streaming approach for existing records
        if (this.opts.debugMode) {
          console.log(`💾 Save: Checking streaming condition: indexOffset=${this.indexOffset}, deletedIds.size=${this.deletedIds.size}`)
          console.log(`💾 Save: writeBuffer.length=${this.writeBuffer.length}`)
        }
        if (this.indexOffset > 0 || this.deletedIds.size > 0) {
          try {
            if (this.opts.debugMode) {
              console.log(`💾 Save: Using streaming approach for existing records`)
              console.log(`💾 Save: indexOffset: ${this.indexOffset}, offsets.length: ${this.offsets.length}`)
              console.log(`💾 Save: deletedIds to filter:`, Array.from(deletedIdsSnapshot))
            }
            
            // OPTIMIZATION: Parallel operations - cleanup and streaming
            const parallelOperations = []
            
            // Add term cleanup if enabled
            if (this.opts.termMappingCleanup && this.termManager) {
              parallelOperations.push(
                Promise.resolve().then(() => {
                  orphanedCount = this.termManager.cleanupOrphanedTerms()
                  if (this.opts.debugMode && orphanedCount > 0) {
                    console.log(`🧹 Cleaned up ${orphanedCount} orphaned terms`)
                  }
                })
              )
            }
            
            // Add streaming operation
            parallelOperations.push(
              this._streamExistingRecords(deletedIdsSnapshot, writeBufferSnapshot).then(existingRecords => {
                if (this.opts.debugMode) {
                  console.log(`💾 Save: _streamExistingRecords returned ${existingRecords.length} records`)
                  console.log(`💾 Save: existingRecords:`, existingRecords)
                }
                // CRITICAL FIX: _streamExistingRecords already handles updates via updatedRecordsMap
                // So existingRecords already contains updated records from writeBufferSnapshot
                // We only need to add records from writeBufferSnapshot that are NEW (not updates)
                allData = [...existingRecords]
                
                // OPTIMIZATION: Use Set for faster lookups of existing record IDs
                // CRITICAL FIX: Normalize IDs to strings for consistent comparison
                const existingRecordIds = new Set(existingRecords.filter(r => r && r.id).map(r => String(r.id)))
                
                // CRITICAL FIX: Create a map of records in existingRecords by ID for comparison
                const existingRecordsById = new Map()
                existingRecords.forEach(r => {
                  if (r && r.id) {
                    existingRecordsById.set(String(r.id), r)
                  }
                })
                
                // Add only NEW records from writeBufferSnapshot (not updates, as those are already in existingRecords)
                // CRITICAL FIX: Also ensure that if an updated record wasn't properly replaced, we replace it now
                for (const record of writeBufferSnapshot) {
                  if (!record || !record.id) continue
                  if (deletedIdsSnapshot.has(String(record.id))) continue
                  
                  const recordIdStr = String(record.id)
                  const existingRecord = existingRecordsById.get(recordIdStr)
                  
                  if (!existingRecord) {
                    // This is a new record, not an update
                    allData.push(record)
                    if (this.opts.debugMode) {
                      console.log(`💾 Save: Adding NEW record to allData:`, { id: recordIdStr, price: record.price, app_id: record.app_id, currency: record.currency })
                    }
                  } else {
                    // This is an update - verify that existingRecords contains the updated version
                    // If not, replace it (this handles edge cases where substitution might have failed)
                    const existingIndex = allData.findIndex(r => r && r.id && String(r.id) === recordIdStr)
                    if (existingIndex !== -1) {
                      // Verify if the existing record is actually the updated one
                      // Compare key fields to detect if replacement is needed
                      const needsReplacement = JSON.stringify(allData[existingIndex]) !== JSON.stringify(record)
                      if (needsReplacement) {
                        if (this.opts.debugMode) {
                          console.log(`💾 Save: REPLACING existing record with updated version in allData:`, {
                            old: { id: String(allData[existingIndex].id), price: allData[existingIndex].price },
                            new: { id: recordIdStr, price: record.price }
                          })
                        }
                        allData[existingIndex] = record
                      } else if (this.opts.debugMode) {
                        console.log(`💾 Save: Record already correctly updated in allData:`, { id: recordIdStr })
                      }
                    }
                  }
                }
                
                if (this.opts.debugMode) {
                  const updatedCount = writeBufferSnapshot.filter(r => r && r.id && existingRecordIds.has(String(r.id))).length
                  const newCount = writeBufferSnapshot.filter(r => r && r.id && !existingRecordIds.has(String(r.id))).length
                  console.log(`💾 Save: Combined data - existingRecords: ${existingRecords.length}, updatedFromBuffer: ${updatedCount}, newFromBuffer: ${newCount}, total: ${allData.length}`)
                  console.log(`💾 Save: WriteBuffer record IDs:`, writeBufferSnapshot.map(r => r && r.id ? String(r.id) : 'no-id'))
                  console.log(`💾 Save: Existing record IDs:`, Array.from(existingRecordIds))
                  console.log(`💾 Save: All records in allData:`, allData.map(r => r && r.id ? { id: String(r.id), price: r.price, app_id: r.app_id, currency: r.currency } : 'no-id'))
                  console.log(`💾 Save: Sample existing record:`, existingRecords[0] ? { id: String(existingRecords[0].id), price: existingRecords[0].price, app_id: existingRecords[0].app_id, currency: existingRecords[0].currency } : 'null')
                  console.log(`💾 Save: Sample writeBuffer record:`, writeBufferSnapshot[0] ? { id: String(writeBufferSnapshot[0].id), price: writeBufferSnapshot[0].price, app_id: writeBufferSnapshot[0].app_id, currency: writeBufferSnapshot[0].currency } : 'null')
                }
              }).catch(error => {
                if (this.opts.debugMode) {
                  console.log(`💾 Save: _streamExistingRecords failed:`, error.message)
                }
                // CRITICAL FIX: Use safe fallback to preserve existing data instead of losing it
                return this._loadExistingRecordsFallback(deletedIdsSnapshot, writeBufferSnapshot).then(fallbackRecords => {
                  // CRITICAL FIX: Avoid duplicating updated records
                  const fallbackRecordIds = new Set(fallbackRecords.map(r => r.id))
                  const newRecordsFromBuffer = writeBufferSnapshot.filter(record => 
                    !deletedIdsSnapshot.has(String(record.id)) && !fallbackRecordIds.has(record.id)
                  )
                  allData = [...fallbackRecords, ...newRecordsFromBuffer]
                  if (this.opts.debugMode) {
                    console.log(`💾 Save: Fallback preserved ${fallbackRecords.length} existing records, total: ${allData.length}`)
                  }
                }).catch(fallbackError => {
                  if (this.opts.debugMode) {
                    console.log(`💾 Save: All fallback methods failed:`, fallbackError.message)
                    console.log(`💾 Save: CRITICAL - Data loss may occur, only writeBuffer will be saved`)
                  }
                  // Last resort: at least save what we have in writeBuffer
                  allData = writeBufferSnapshot.filter(record => !deletedIdsSnapshot.has(String(record.id)))
                })
              })
            )
            
            // Execute parallel operations
            await Promise.all(parallelOperations)
          } catch (error) {
            if (this.opts.debugMode) {
              console.log(`💾 Save: Streaming approach failed, falling back to writeBuffer only: ${error.message}`)
            }
            // CRITICAL FIX: Use safe fallback to preserve existing data instead of losing it
            try {
              const fallbackRecords = await this._loadExistingRecordsFallback(deletedIdsSnapshot, writeBufferSnapshot)
              // CRITICAL FIX: Avoid duplicating updated records
              const fallbackRecordIds = new Set(fallbackRecords.map(r => r.id))
              const newRecordsFromBuffer = writeBufferSnapshot.filter(record => 
                !deletedIdsSnapshot.has(String(record.id)) && !fallbackRecordIds.has(record.id)
              )
              allData = [...fallbackRecords, ...newRecordsFromBuffer]
              if (this.opts.debugMode) {
                console.log(`💾 Save: Fallback preserved ${fallbackRecords.length} existing records, total: ${allData.length}`)
              }
            } catch (fallbackError) {
              if (this.opts.debugMode) {
                console.log(`💾 Save: All fallback methods failed:`, fallbackError.message)
                console.log(`💾 Save: CRITICAL - Data loss may occur, only writeBuffer will be saved`)
              }
              // Last resort: at least save what we have in writeBuffer
              allData = writeBufferSnapshot.filter(record => !deletedIdsSnapshot.has(String(record.id)))
            }
          }
        } else {
          // No existing data, use only writeBuffer
          allData = writeBufferSnapshot.filter(record => !deletedIdsSnapshot.has(String(record.id)))
        }
      }
      
      // CRITICAL FIX: Calculate offsets based on actual serialized data that will be written
      // This ensures consistency between offset calculation and file writing
      // CRITICAL FIX: Remove term IDs before serialization to ensure proper serialization
      const cleanedData = allData.map(record => {
        if (!record || typeof record !== 'object') {
          if (this.opts.debugMode) {
            console.log(`💾 Save: WARNING - Invalid record in allData:`, record)
          }
          return record
        }
        return this.removeTermIdsForSerialization(record)
      })
      
      if (this.opts.debugMode) {
        console.log(`💾 Save: allData.length=${allData.length}, cleanedData.length=${cleanedData.length}`)
        console.log(`💾 Save: Current offsets.length before recalculation: ${this.offsets.length}`)
        console.log(`💾 Save: All records in allData before serialization:`, allData.map(r => r && r.id ? { id: String(r.id), price: r.price, app_id: r.app_id, currency: r.currency } : 'no-id'))
        console.log(`💾 Save: Sample cleaned record:`, cleanedData[0] ? Object.keys(cleanedData[0]) : 'null')
      }
      
      const jsonlData = cleanedData.length > 0 
        ? this.serializer.serializeBatch(cleanedData)
        : ''
      const jsonlString = jsonlData.toString('utf8')
      const lines = jsonlString.split('\n').filter(line => line.trim())
      
      if (this.opts.debugMode) {
        console.log(`💾 Save: Serialized ${lines.length} lines`)
        console.log(`💾 Save: All records in allData after serialization check:`, allData.map(r => r && r.id ? { id: String(r.id), price: r.price, app_id: r.app_id, currency: r.currency } : 'no-id'))
        if (lines.length > 0) {
          console.log(`💾 Save: First line (first 200 chars):`, lines[0].substring(0, 200))
        }
      }
      
      // CRITICAL FIX: Always recalculate offsets from serialized data to ensure consistency
      // Even if _streamExistingRecords updated offsets, we need to recalculate based on actual serialized data
      this.offsets = []
      let currentOffset = 0
      for (let i = 0; i < lines.length; i++) {
        this.offsets.push(currentOffset)
        // CRITICAL FIX: Use actual line length including newline for accurate offset calculation
        // This accounts for UTF-8 encoding differences (e.g., 'ação' vs 'acao')
        const lineWithNewline = lines[i] + '\n'
        currentOffset += Buffer.byteLength(lineWithNewline, 'utf8')
      }
      
      if (this.opts.debugMode) {
        console.log(`💾 Save: Recalculated offsets.length=${this.offsets.length}, should match lines.length=${lines.length}`)
      }
      
      // CRITICAL FIX: Ensure indexOffset matches actual file size
      this.indexOffset = currentOffset
      
      if (this.opts.debugMode) {
        console.log(`💾 Save: Calculated indexOffset: ${this.indexOffset}, allData.length: ${allData.length}`)
      }
      
      // CRITICAL FIX: Write main data file first
      // Index will be saved AFTER reconstruction to ensure it contains correct data
      await this.fileHandler.writeBatch([jsonlData])
      
      if (this.opts.debugMode) {
        console.log(`💾 Saved ${allData.length} records to ${this.normalizedFile}`)
      }
      
      // CRITICAL FIX: Invalidate file size cache after save operation
      this._cachedFileStats = null
      
      this.shouldSave = false
      this.lastSaveTime = Date.now()
      
      // CRITICAL FIX: Always clear deletedIds and rebuild index if there were deletions,
      // even if allData.length === 0 (all records were deleted)
        const hadDeletedRecords = deletedIdsSnapshot.size > 0
        const hadUpdatedRecords = writeBufferSnapshot.length > 0
      
      // Clear writeBuffer and deletedIds after successful save
      // Also rebuild index if records were deleted or updated, even if allData is empty
      if (allData.length > 0 || hadDeletedRecords || hadUpdatedRecords) {
        // Rebuild index when records were deleted or updated to maintain consistency
        if (this.indexManager && this.indexManager.indexedFields && this.indexManager.indexedFields.length > 0) {
          if (hadDeletedRecords || hadUpdatedRecords) {
            // Clear the index and rebuild it from the saved records
            // This ensures that lineNumbers point to the correct positions in the file
            this.indexManager.clear()
            if (this.opts.debugMode) {
              if (hadDeletedRecords && hadUpdatedRecords) {
                console.log(`🧹 Rebuilding index after removing ${deletedIdsSnapshot.size} deleted records and updating ${writeBufferSnapshot.length} records`)
              } else if (hadDeletedRecords) {
                console.log(`🧹 Rebuilding index after removing ${deletedIdsSnapshot.size} deleted records`)
              } else {
                console.log(`🧹 Rebuilding index after updating ${writeBufferSnapshot.length} records`)
              }
            }
            
            // Rebuild index from the saved records
            // CRITICAL: Process term mapping for records loaded from file to ensure ${field}Ids are available
            if (this.opts.debugMode) {
              console.log(`💾 Save: Rebuilding index from ${allData.length} records in allData`)
            }
            for (let i = 0; i < allData.length; i++) {
              let record = allData[i]
              
              if (this.opts.debugMode && i < 3) {
                console.log(`💾 Save: Rebuilding index record[${i}]:`, { id: String(record.id), price: record.price, app_id: record.app_id, currency: record.currency })
              }
              
              // CRITICAL FIX: Ensure records have ${field}Ids for term mapping fields
              // Records from writeBuffer already have ${field}Ids from processTermMapping
              // Records from file need to be processed to restore ${field}Ids
              const termMappingFields = this.getTermMappingFields()
              if (termMappingFields.length > 0 && this.termManager) {
                for (const field of termMappingFields) {
                  if (record[field] && Array.isArray(record[field])) {
                    // Check if field contains term IDs (numbers) or terms (strings)
                    const firstValue = record[field][0]
                    if (typeof firstValue === 'number') {
                      // Already term IDs, create ${field}Ids
                      record[`${field}Ids`] = record[field]
                    } else if (typeof firstValue === 'string') {
                      // Terms, need to convert to term IDs
                      const termIds = record[field].map(term => {
                        const termId = this.termManager.getTermIdWithoutIncrement(term)
                        return termId !== undefined ? termId : this.termManager.getTermId(term)
                      })
                      record[`${field}Ids`] = termIds
                    }
                  }
                }
              }
              
              await this.indexManager.add(record, i)
            }

            // VALIDATION: Ensure index consistency after rebuild
            // Check that all indexed records have valid line numbers
            const indexedRecordCount = this.indexManager.getIndexedRecordCount?.() || allData.length
            if (indexedRecordCount !== this.offsets.length) {
              this.integrityCorrections.indexInconsistency++
              console.log(`🔧 Auto-corrected index consistency: ${indexedRecordCount} indexed → ${this.offsets.length} offsets`)

              if (this.integrityCorrections.indexInconsistency > 5) {
                console.warn(`⚠️ Frequent index inconsistencies detected (${this.integrityCorrections.indexInconsistency} times)`)
              }

              // Force consistency by setting totalLines to match offsets
              this.indexManager.setTotalLines(this.offsets.length)
            } else {
              this.indexManager.setTotalLines(this.offsets.length)
            }

            if (this.opts.debugMode) {
              console.log(`💾 Save: Index rebuilt with ${allData.length} records, totalLines set to ${this.offsets.length}`)
            }
          }
        }
        
        // CRITICAL FIX: Clear all records that were in the snapshot
        // Use a more robust comparison that handles different data types
        const originalLength = this.writeBuffer.length
        this.writeBuffer = this.writeBuffer.filter(record => {
          // For objects with id, compare by id
          if (typeof record === 'object' && record !== null && record.id) {
            return !writeBufferSnapshot.some(snapshotRecord => 
              typeof snapshotRecord === 'object' && snapshotRecord !== null && 
              snapshotRecord.id && snapshotRecord.id === record.id
            )
          }
          // For other types (Buffers, primitives), use strict equality
          return !writeBufferSnapshot.some(snapshotRecord => snapshotRecord === record)
        })
        
        // Remove only the deleted IDs that were in the snapshot
        for (const deletedId of deletedIdsSnapshot) {
          this.deletedIds.delete(deletedId)
        }
      } else if (hadDeletedRecords) {
        // CRITICAL FIX: Even if allData is empty, clear deletedIds and rebuild index
        // when records were deleted to ensure consistency
        if (this.indexManager && this.indexManager.indexedFields && this.indexManager.indexedFields.length > 0) {
          // Clear the index since all records were deleted
          this.indexManager.clear()
          this.indexManager.setTotalLines(0)
          if (this.opts.debugMode) {
            console.log(`🧹 Cleared index after removing all ${deletedIdsSnapshot.size} deleted records`)
          }
        }
        
        // Clear deletedIds even when allData is empty
        for (const deletedId of deletedIdsSnapshot) {
          this.deletedIds.delete(deletedId)
        }
        
        // CRITICAL FIX: Ensure writeBuffer is completely cleared after successful save
        if (this.writeBuffer.length > 0) {
          if (this.opts.debugMode) {
            console.log(`💾 Save: Force clearing remaining ${this.writeBuffer.length} items from writeBuffer`)
          }
          // If there are still items in writeBuffer after filtering, clear them
          // This prevents the "writeBuffer has records" bug in destroy()
          this.writeBuffer = []
          this.writeBufferOffsets = []
          this.writeBufferSizes = []
          this.writeBufferTotalSize = 0
          this.writeBufferTotalSize = 0
        }
        
        // indexOffset already set correctly to currentOffset (total file size) above
        // No need to override it with record count
      }
      
      // CRITICAL FIX: Always save index data to file after saving records
      await this._saveIndexDataToFile()
      
      this.performanceStats.saves++
      this.performanceStats.saveTime += Date.now() - startTime
      this.emit('saved', this.writeBuffer.length)
      
    } catch (error) {
      console.error('Failed to save database:', error)
      throw error
    } finally {
      this.isSaving = false
    }
  }

  /**
   * Process term mapping for a record
   * @param {Object} record - Record to process
   * @param {boolean} isUpdate - Whether this is an update operation
   * @param {Object} oldRecord - Original record (for updates)
   */
  processTermMapping(record, isUpdate = false, oldRecord = null) {
    const termMappingFields = this.getTermMappingFields()
    if (!this.termManager || termMappingFields.length === 0) {
      return
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
          const termIdsField = `${field}Ids`
          if (oldRecord[termIdsField] && Array.isArray(oldRecord[termIdsField])) {
            // Use term IDs directly for decrementing
            for (const termId of oldRecord[termIdsField]) {
              this.termManager.decrementTermCount(termId)
            }
          } else if (oldRecord[field] && Array.isArray(oldRecord[field])) {
            // Check if field contains term IDs (numbers) or terms (strings)
            const firstValue = oldRecord[field][0]
            if (typeof firstValue === 'number') {
              // Field contains term IDs (from find with restoreTerms: false)
              for (const termId of oldRecord[field]) {
                this.termManager.decrementTermCount(termId)
              }
            } else if (typeof firstValue === 'string') {
              // Field contains terms (strings) - convert to term IDs
              for (const term of oldRecord[field]) {
                const termId = this.termManager.termToId.get(term)
                if (termId) {
                  this.termManager.decrementTermCount(termId)
                }
              }
            }
          }
        }

        // Clear old term IDs if this is an update
        if (isUpdate) {
          delete record[`${field}Ids`]
        }

        // Process new terms - getTermId already increments the count
        const termIds = []
        for (const term of record[field]) {
          const termId = this.termManager.getTermId(term)
          termIds.push(termId)
        }
        // Store term IDs in the record (for internal use)
        record[`${field}Ids`] = termIds
        
      }
    }
  }

  /**
   * Convert terms to term IDs for serialization (SPACE OPTIMIZATION)
   * @param {Object} record - Record to process
   * @returns {Object} - Record with terms converted to term IDs
   */
  removeTermIdsForSerialization(record) {
    const termMappingFields = this.getTermMappingFields()
    if (termMappingFields.length === 0 || !this.termManager) {
      return record
    }

    // Create a copy and convert terms to term IDs
    const optimizedRecord = { ...record }
    
    for (const field of termMappingFields) {
      if (optimizedRecord[field] && Array.isArray(optimizedRecord[field])) {
        // CRITICAL FIX: Only convert if values are strings (terms), skip if already numbers (term IDs)
        const firstValue = optimizedRecord[field][0]
        if (typeof firstValue === 'string') {
          // Convert terms to term IDs for storage
          optimizedRecord[field] = optimizedRecord[field].map(term => 
            this.termManager.getTermIdWithoutIncrement(term)
          )
        }
        // If already numbers (term IDs), leave as-is
      }
    }
    
    return optimizedRecord
  }

  /**
   * Convert term IDs back to terms after deserialization (SPACE OPTIMIZATION)
   * @param {Object} record - Record with term IDs
   * @returns {Object} - Record with terms restored
   */
  restoreTermIdsAfterDeserialization(record) {
    const termMappingFields = this.getTermMappingFields()
    if (termMappingFields.length === 0 || !this.termManager) {
      return record
    }


    // Create a copy and convert term IDs back to terms
    const restoredRecord = { ...record }
    
    for (const field of termMappingFields) {
      if (restoredRecord[field] && Array.isArray(restoredRecord[field])) {
        
        // Convert term IDs back to terms for user
        restoredRecord[field] = restoredRecord[field].map(termId => {
          const term = this.termManager.idToTerm.get(termId) || termId
          
          
          return term
        })
      }
      
      // Remove the *Ids field that was added during serialization
      const idsFieldName = field + 'Ids'
      if (restoredRecord[idsFieldName]) {
        delete restoredRecord[idsFieldName]
      }
    }
    
    
    return restoredRecord
  }


  /**
   * Remove term mapping for a record
   * @param {Object} record - Record to process
   */
  removeTermMapping(record) {
    const termMappingFields = this.getTermMappingFields()
    if (!this.termManager || termMappingFields.length === 0) {
      return
    }

    // Process each term mapping field
    for (const field of termMappingFields) {
      // Use terms to decrement (term IDs are not stored in records anymore)
      if (record[field] && Array.isArray(record[field])) {
        for (const term of record[field]) {
          const termId = this.termManager.termToId.get(term)
          if (termId) {
            this.termManager.decrementTermCount(termId)
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
    const termMappingFields = this.getTermMappingFields()
    if (!this.termManager || termMappingFields.length === 0 || !records.length) {
      return records
    }

    // OPTIMIZATION: Pre-collect all unique terms to minimize Map operations
    const allTerms = new Set()
    const fieldTerms = new Map() // field -> Set of terms
    
    for (const field of termMappingFields) {
      fieldTerms.set(field, new Set())
      for (const record of records) {
        if (record[field] && Array.isArray(record[field])) {
          for (const term of record[field]) {
            allTerms.add(term)
            fieldTerms.get(field).add(term)
          }
        }
      }
    }

    // OPTIMIZATION: Batch process all terms at once using bulk operations
    const termIdMap = new Map()
    if (this.termManager.bulkGetTermIds) {
      // Use bulk operation if available
      const allTermsArray = Array.from(allTerms)
      const termIds = this.termManager.bulkGetTermIds(allTermsArray)
      for (let i = 0; i < allTermsArray.length; i++) {
        termIdMap.set(allTermsArray[i], termIds[i])
      }
    } else {
      // Fallback to individual operations
      for (const term of allTerms) {
        termIdMap.set(term, this.termManager.getTermId(term))
      }
    }

    // OPTIMIZATION: Process records using pre-computed term IDs
    for (const record of records) {
      for (const field of termMappingFields) {
        if (record[field] && Array.isArray(record[field])) {
          const termIds = record[field].map(term => termIdMap.get(term))
          record[`${field}Ids`] = termIds
        }
      }
    }
    
    return records
  }


  /**
   * Calculate total size of serialized records (OPTIMIZATION)
   * @param {Array} records - Records to calculate size for
   * @returns {number} - Total size in bytes
   */
  calculateBatchSize(records) {
    if (!records || !records.length) return 0
    
    let totalSize = 0
    for (const record of records) {
      // OPTIMIZATION: Calculate size without creating the actual string
      // SPACE OPTIMIZATION: Remove term IDs before size calculation
      const cleanRecord = this.removeTermIdsForSerialization(record)
      const jsonString = this.serializer.serialize(cleanRecord).toString('utf8')
      totalSize += Buffer.byteLength(jsonString, 'utf8') + 1 // +1 for newline
    }
    
    return totalSize
  }

  /**
   * Begin an insert session for batch operations
   * @param {Object} sessionOptions - Options for the insert session
   * @returns {InsertSession} - The insert session instance
   */
  beginInsertSession(sessionOptions = {}) {
    if (this.destroyed) {
      throw new Error('Database is destroyed')
    }
    
    if (this.closed) {
      throw new Error('Database is closed. Call init() to reopen it.')
    }
    
    return new InsertSession(this, sessionOptions)
  }

  /**
   * Insert a new record
   */
  async insert(data) {
    this._validateInitialization('insert')
    
    return this.operationQueue.enqueue(async () => {
      this.isInsideOperationQueue = true
      try {
        // CRITICAL FIX: Validate state before insert operation
        this.validateState()
      
      if (!data || typeof data !== 'object') {
        throw new Error('Data must be an object')
      }
      
      // CRITICAL FIX: Check abort signal before operation, but allow during destroy cleanup
      if (this.abortController.signal.aborted && !this.destroying) {
        throw new Error('Database is destroyed')
      }
      
      // Initialize schema if not already done (auto-detect from first record)
      if (this.serializer && !this.serializer.schemaManager.isInitialized) {
        this.serializer.initializeSchema(data, true)
        if (this.opts.debugMode) {
          console.log(`🔍 Schema auto-detected from first insert: ${this.serializer.getSchema().join(', ')} [${this.instanceId}]`)
        }
      }

      // OPTIMIZATION: Process single insert with deferred index updates
      // CRITICAL FIX: Clone the object to prevent reference modification
      const clonedData = {...data}
      const id = clonedData.id || this.generateId()
      const record = { ...data, id }
      
      // OPTIMIZATION: Process term mapping
      this.processTermMapping(record)
      if (this.opts.debugMode) {
        // console.log(`💾 insert(): writeBuffer(before)=${this.writeBuffer.length}`)
      }
      
      // Apply schema enforcement - convert to array format and back to enforce schema
      // This will discard any fields not in the schema
      const schemaEnforcedRecord = this.applySchemaEnforcement(record)
      
      // Don't store in this.data - only use writeBuffer and index
      this.writeBuffer.push(schemaEnforcedRecord)
      if (this.opts.debugMode) {
        console.log(`🔍 INSERT: Added record to writeBuffer, length now: ${this.writeBuffer.length}`)
      }
      
      // OPTIMIZATION: Calculate and store offset and size for writeBuffer record
      // SPACE OPTIMIZATION: Remove term IDs before serialization
      const cleanRecord = this.removeTermIdsForSerialization(record)
      const recordBuffer = this.serializer.serialize(cleanRecord)
      const recordSize = recordBuffer.length
      
      // Calculate offset based on end of file + previous writeBuffer sizes
      const previousWriteBufferSize = this.writeBufferTotalSize
      const recordOffset = this.indexOffset + previousWriteBufferSize
      
      this.writeBufferOffsets.push(recordOffset)
      this.writeBufferSizes.push(recordSize)
      this.writeBufferTotalSize += recordSize
      
      // OPTIMIZATION: Use the absolute line number (persisted records + writeBuffer index)
      const lineNumber = this._getAbsoluteLineNumber(this.writeBuffer.length - 1)
      
      // OPTIMIZATION: Defer index updates to batch processing
      // Store the record for batch index processing
      if (!this.pendingIndexUpdates) {
        this.pendingIndexUpdates = []
      }
      this.pendingIndexUpdates.push({ record, lineNumber })
      
      // Manual save is now the responsibility of the application
      this.shouldSave = true
      
      this.performanceStats.operations++

      // Auto-save manager removed - manual save required

      this.emit('inserted', record)
      return record
      } finally {
        this.isInsideOperationQueue = false
      }
    })
  }

  /**
   * Insert multiple records in batch (OPTIMIZATION)
   */
  async insertBatch(dataArray) {
    this._validateInitialization('insertBatch')
    
    // If we're already inside the operation queue (e.g., from insert()), avoid re-enqueueing to prevent deadlocks
    if (this.isInsideOperationQueue) {
      if (this.opts.debugMode) {
        console.log(`💾 insertBatch inline: insideQueue=${this.isInsideOperationQueue}, size=${Array.isArray(dataArray) ? dataArray.length : 0}`)
      }
      return await this._insertBatchInternal(dataArray)
    }

    return this.operationQueue.enqueue(async () => {
      this.isInsideOperationQueue = true
      try {
        if (this.opts.debugMode) {
          console.log(`💾 insertBatch enqueued: size=${Array.isArray(dataArray) ? dataArray.length : 0}`)
        }
        return await this._insertBatchInternal(dataArray)
      } finally {
        this.isInsideOperationQueue = false
      }
    })
  }

  /**
   * Internal implementation for insertBatch to allow inline execution when already inside the queue
   */
  async _insertBatchInternal(dataArray) {
    // CRITICAL FIX: Validate state before insert operation
    this.validateState()
    
    if (!Array.isArray(dataArray) || dataArray.length === 0) {
      throw new Error('DataArray must be a non-empty array')
    }
    
    // CRITICAL FIX: Check abort signal before operation, but allow during destroy cleanup
    if (this.abortController.signal.aborted && !this.destroying) {
      throw new Error('Database is destroyed')
    }
    
    if (this.opts.debugMode) {
      console.log(`💾 _insertBatchInternal: processing size=${dataArray.length}, startWriteBuffer=${this.writeBuffer.length}`)
    }
    const records = []
    const existingWriteBufferLength = this.writeBuffer.length
    
    // Initialize schema if not already done (auto-detect from first record)
    if (this.serializer && !this.serializer.schemaManager.isInitialized && dataArray.length > 0) {
      this.serializer.initializeSchema(dataArray[0], true)
      if (this.opts.debugMode) {
        console.log(`🔍 Schema auto-detected from first batch insert: ${this.serializer.getSchema().join(', ')} [${this.instanceId}]`)
      }
    }

    // OPTIMIZATION: Process all records in batch
    for (let i = 0; i < dataArray.length; i++) {
      const data = dataArray[i]
      if (!data || typeof data !== 'object') {
        throw new Error(`Data at index ${i} must be an object`)
      }
      
      const id = data.id || this.generateId()
      const record = { ...data, id }
      records.push(record)
    }
    
    // OPTIMIZATION: Batch process term mapping
    const processedRecords = this.processTermMappingBatch(records)
    
    // Apply schema enforcement to all records
    const schemaEnforcedRecords = processedRecords.map(record => this.applySchemaEnforcement(record))
    
    // OPTIMIZATION: Add all records to writeBuffer at once
    this.writeBuffer.push(...schemaEnforcedRecords)
    
    // OPTIMIZATION: Calculate offsets and sizes in batch (O(n))
    let runningTotalSize = this.writeBufferTotalSize
    for (let i = 0; i < processedRecords.length; i++) {
      const record = processedRecords[i]
      // SPACE OPTIMIZATION: Remove term IDs before serialization
      const cleanRecord = this.removeTermIdsForSerialization(record)
      const recordBuffer = this.serializer.serialize(cleanRecord)
      const recordSize = recordBuffer.length
      
      const recordOffset = this.indexOffset + runningTotalSize
      runningTotalSize += recordSize
      
      this.writeBufferOffsets.push(recordOffset)
      this.writeBufferSizes.push(recordSize)
    }
    this.writeBufferTotalSize = runningTotalSize
    
    // OPTIMIZATION: Batch process index updates
    if (!this.pendingIndexUpdates) {
      this.pendingIndexUpdates = []
    }
    
    for (let i = 0; i < processedRecords.length; i++) {
      const lineNumber = this._getAbsoluteLineNumber(existingWriteBufferLength + i)
      this.pendingIndexUpdates.push({ record: processedRecords[i], lineNumber })
    }
    
    this.shouldSave = true
    this.performanceStats.operations += processedRecords.length
    
    // Emit events for all records
    if (this.listenerCount('inserted') > 0) {
      for (const record of processedRecords) {
        this.emit('inserted', record)
      }
    }
    
    if (this.opts.debugMode) {
      console.log(`💾 _insertBatchInternal: done. added=${processedRecords.length}, writeBuffer=${this.writeBuffer.length}`)
    }
    return processedRecords
  }

  /**
   * Find records matching criteria
   */
  async find(criteria = {}, options = {}) {
    this._validateInitialization('find')

    // CRITICAL FIX: Validate state before find operation
    this.validateState()
    
    // OPTIMIZATION: Find searches writeBuffer directly
    
    const startTime = Date.now()
    
    if (this.opts.debugMode) {
      console.log(`🔍 FIND START: criteria=${JSON.stringify(criteria)}, writeBuffer=${this.writeBuffer.length}`)
    }
    
    try {
      // INTEGRITY CHECK: Validate data consistency before querying
      // This is a safety net for unexpected inconsistencies - should rarely trigger
      if (this.indexManager && this.offsets && this.offsets.length > 0) {
        const indexTotalLines = this.indexManager.totalLines || 0
        const offsetsLength = this.offsets.length

        if (indexTotalLines !== offsetsLength) {
          // This should be extremely rare - indicates a real bug if it happens frequently
          this.integrityCorrections.dataIntegrity++

          // Only show in debug mode - these corrections indicate real issues
          if (this.opts.debugMode) {
            console.log(`🔧 Integrity correction needed: index.totalLines ${indexTotalLines} → ${offsetsLength} (${this.integrityCorrections.dataIntegrity} total)`)
          }

          // Warn if corrections are becoming frequent (indicates a real problem)
          if (this.integrityCorrections.dataIntegrity > 5) {
            console.warn(`⚠️ Frequent integrity corrections (${this.integrityCorrections.dataIntegrity} times) - this indicates a systemic issue`)
          }

          this.indexManager.setTotalLines(offsetsLength)

          // Try to persist the fix, but don't fail the operation if it doesn't work
          try {
            await this._saveIndexDataToFile()
          } catch (error) {
            // Just track the failure - don't throw since this is a safety net
            this.integrityCorrections.indexSaveFailures++
          }
        }
      }

      // Validate indexed query mode if enabled
      if (this.opts.indexedQueryMode === 'strict') {
        this._validateIndexedQuery(criteria, options)
      }
      
      // Get results from file (QueryManager already handles term ID restoration)
      const fileResultsWithTerms = await this.queryManager.find(criteria, options)
      
      // Get results from writeBuffer
      const allPendingRecords = [...this.writeBuffer]
      
      const writeBufferResults = this.queryManager.matchesCriteria ? 
        allPendingRecords.filter(record => this.queryManager.matchesCriteria(record, criteria, options)) :
        allPendingRecords
      
      // SPACE OPTIMIZATION: Restore term IDs to terms for writeBuffer results (unless disabled)
      const writeBufferResultsWithTerms = options.restoreTerms !== false ? 
        writeBufferResults.map(record => this.restoreTermIdsAfterDeserialization(record)) :
        writeBufferResults
      
      
      // Combine results, removing duplicates (writeBuffer takes precedence)
      // OPTIMIZATION: Unified efficient approach with consistent precedence rules
      let allResults

      // Create efficient lookup map for writeBuffer records
      const writeBufferMap = new Map()
      writeBufferResultsWithTerms.forEach(record => {
        if (record && record.id) {
          writeBufferMap.set(record.id, record)
        }
      })
        
      // Filter file results to exclude any records that exist in writeBuffer
      // This ensures writeBuffer always takes precedence
      const filteredFileResults = fileResultsWithTerms.filter(record =>
        record && record.id && !writeBufferMap.has(record.id)
      )

      // Combine results: file results (filtered) + all writeBuffer results
        allResults = [...filteredFileResults, ...writeBufferResultsWithTerms]
      
      // Remove records that are marked as deleted
      const finalResults = allResults.filter(record => !this.deletedIds.has(record.id))
      
      if (this.opts.debugMode) {
        console.log(`🔍 Database.find returning: ${finalResults?.length || 0} records (${fileResultsWithTerms.length} from file, ${writeBufferResults.length} from writeBuffer, ${this.deletedIds.size} deleted), type: ${typeof finalResults}, isArray: ${Array.isArray(finalResults)}`)
      }
      
      this.performanceStats.queryTime += Date.now() - startTime
      return finalResults
    } catch (error) {
      // Don't log expected errors in strict mode or for array field validation
      if (this.opts.indexedQueryMode !== 'strict' || !error.message.includes('Strict indexed mode')) {
        // Don't log errors for array field validation as they are expected
        if (!error.message.includes('Invalid query for array field')) {
          console.error('Query failed:', error)
        }
      }
      throw error
    }
  }

  /**
   * Validate indexed query mode for strict mode
   * @private
   * @param {Object} criteria - Query criteria
   * @param {Object} options - Query options
   */
  _validateIndexedQuery(criteria, options = {}) {
    // Allow bypassing strict mode validation with allowNonIndexed option
    if (options.allowNonIndexed === true) {
      return; // Skip validation for this query
    }

    if (!criteria || typeof criteria !== 'object') {
      return // Allow null/undefined criteria
    }
    
    const indexedFields = Object.keys(this.opts.indexes || {})
    if (indexedFields.length === 0) {
      return // No indexed fields, allow all queries
    }
    
    const queryFields = this._extractQueryFields(criteria)
    const nonIndexedFields = queryFields.filter(field => !indexedFields.includes(field))
    
    if (nonIndexedFields.length > 0) {
      const availableFields = indexedFields.length > 0 ? indexedFields.join(', ') : 'none'
      if (nonIndexedFields.length === 1) {
        throw new Error(`Strict indexed mode: Field '${nonIndexedFields[0]}' is not indexed. Available indexed fields: ${availableFields}`)
      } else {
        throw new Error(`Strict indexed mode: Fields '${nonIndexedFields.join("', '")}' are not indexed. Available indexed fields: ${availableFields}`)
      }
    }
  }
  
  /**
   * Create a shallow copy of a record for change detection
   * Optimized for known field types: number, string, null, or single-level arrays
   * @private
   */
  _createShallowCopy(record) {
    const copy = {}
    // Use for...in loop for better performance
    for (const key in record) {
      const value = record[key]
      // Optimize for common types first
      if (value === null || typeof value === 'number' || typeof value === 'string' || typeof value === 'boolean') {
        copy[key] = value
      } else if (Array.isArray(value)) {
        // Only copy if array has elements and is not empty
        copy[key] = value.length > 0 ? value.slice() : []
      } else if (typeof value === 'object') {
        // For complex objects, use shallow copy
        copy[key] = { ...value }
      } else {
        copy[key] = value
      }
    }
    return copy
  }

  /**
   * Create an intuitive API wrapper using a class with Proxy
   * Combines the benefits of classes with the flexibility of Proxy
   * @private
   */
  _createEntryProxy(entry, originalRecord) {
    // Create a class instance that wraps the entry
    const iterateEntry = new IterateEntry(entry, originalRecord)
    
    // Create a lightweight proxy that only intercepts property access
    return new Proxy(iterateEntry, {
      get(target, property) {
        // Handle special methods
        if (property === 'delete') {
          return () => target.delete()
        }
        if (property === 'value') {
          return target.value
        }
        if (property === 'isModified') {
          return target.isModified
        }
        if (property === 'isMarkedForDeletion') {
          return target.isMarkedForDeletion
        }
        
        // For all other properties, return from the underlying entry
        return target._entry[property]
      },
      
      set(target, property, value) {
        // Set the value in the underlying entry
        target._entry[property] = value
        target._modified = true
        return true
      }
    })
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
        entry._markedForDeletion = true
        return true
      }
    }
    
    // Mark for change tracking
    entry._modified = false
    entry._markedForDeletion = false
    
    return wrapper
  }

  /**
   * Check if a record has changed using optimized comparison
   * Optimized for known field types: number, string, null, or single-level arrays
   * @private
   */
  _hasRecordChanged(current, original) {
    // Quick reference check first
    if (current === original) return false
    
    // Compare each field - optimized for common types
    for (const key in current) {
      const currentValue = current[key]
      const originalValue = original[key]
      
      // Quick reference check (most common case)
      if (currentValue === originalValue) continue
      
      // Handle null values
      if (currentValue === null || originalValue === null) {
        if (currentValue !== originalValue) return true
        continue
      }
      
      // Handle primitive types (number, string, boolean) - most common
      const currentType = typeof currentValue
      if (currentType === 'number' || currentType === 'string' || currentType === 'boolean') {
        if (currentType !== typeof originalValue || currentValue !== originalValue) return true
        continue
      }
      
      // Handle arrays (single-level) - second most common
      if (Array.isArray(currentValue)) {
        if (!Array.isArray(originalValue) || currentValue.length !== originalValue.length) return true
        
        // Fast array comparison for primitive types
        for (let i = 0; i < currentValue.length; i++) {
          if (currentValue[i] !== originalValue[i]) return true
        }
        continue
      }
      
      // Handle objects (shallow comparison only) - least common
      if (currentType === 'object') {
        if (typeof originalValue !== 'object') return true
        
        // Fast object comparison using for...in
        for (const objKey in currentValue) {
          if (currentValue[objKey] !== originalValue[objKey]) return true
        }
        // Check if original has extra keys
        for (const objKey in originalValue) {
          if (!(objKey in currentValue)) return true
        }
        continue
      }
      
      // Fallback for other types
      if (currentValue !== originalValue) return true
    }
    
    // Check if original has extra keys (only if we haven't found differences yet)
    for (const key in original) {
      if (!(key in current)) return true
    }
    
    return false
  }

  /**
   * Extract field names from query criteria
   * @private
   */
  _extractQueryFields(criteria) {
    const fields = new Set()
    
    const extractFromObject = (obj) => {
      for (const [key, value] of Object.entries(obj)) {
        if (key.startsWith('$')) {
          // Handle logical operators
          if (Array.isArray(value)) {
            value.forEach(item => {
              if (typeof item === 'object' && item !== null) {
                extractFromObject(item)
              }
            })
          } else if (typeof value === 'object' && value !== null) {
            extractFromObject(value)
          }
        } else {
          // Regular field
          fields.add(key)
        }
      }
    }
    
    extractFromObject(criteria)
    return Array.from(fields)
  }



  /**
   * Update records matching criteria
   */
  async update(criteria, updateData) {
    this._validateInitialization('update')
    
    return this.operationQueue.enqueue(async () => {
      this.isInsideOperationQueue = true
      try {
        const startTime = Date.now()
        if (this.opts.debugMode) {
          console.log(`🔄 UPDATE START: criteria=${JSON.stringify(criteria)}, updateData=${JSON.stringify(updateData)}`)
        }
        
        // CRITICAL FIX: Validate state before update operation
        this.validateState()
      
      if (this.opts.debugMode) {
        console.log(`🔄 UPDATE: Starting find() - writeBuffer=${this.writeBuffer.length}`)
      }
      const findStart = Date.now()
      // CRITICAL FIX: Get raw records without term restoration for update operations
      const records = await this.find(criteria, { restoreTerms: false })
      if (this.opts.debugMode) {
        console.log(`🔄 UPDATE: Find completed in ${Date.now() - findStart}ms, found ${records.length} records`)
      }
      
      const updatedRecords = []

      if (this.opts.debugMode) {
        console.log(`🔄 UPDATE: About to process ${records.length} records`)
        console.log(`🔄 UPDATE: Records:`, records.map(r => ({ id: r.id, value: r.value })))
      }

      for (const record of records) {
        const recordStart = Date.now()
        if (this.opts.debugMode) {
          console.log(`🔄 UPDATE: Processing record ${record.id}`)
        }
        
        const updated = { ...record, ...updateData }
        
        // DEBUG: Log the update operation details
        if (this.opts.debugMode) {
          console.log(`🔄 UPDATE: Original record ID: ${record.id}, type: ${typeof record.id}`)
          console.log(`🔄 UPDATE: Updated record ID: ${updated.id}, type: ${typeof updated.id}`)
          console.log(`🔄 UPDATE: Update data keys:`, Object.keys(updateData))
          console.log(`🔄 UPDATE: Updated record keys:`, Object.keys(updated))
        }
        
        // Process term mapping for update
        const termMappingStart = Date.now()
        this.processTermMapping(updated, true, record)
        if (this.opts.debugMode) {
          console.log(`🔄 UPDATE: Term mapping completed in ${Date.now() - termMappingStart}ms`)
          console.log(`🔄 UPDATE: After term mapping - ID: ${updated.id}, type: ${typeof updated.id}`)
        }
        
        // CRITICAL FIX: Remove old terms from index before adding new ones
        if (this.indexManager) {
          await this.indexManager.remove(record)
          if (this.opts.debugMode) {
            console.log(`🔄 UPDATE: Removed old terms from index for record ${record.id}`)
          }
        }
        
        // CRITICAL FIX: Update record in writeBuffer or add to writeBuffer if not present
        // For records in the file, we need to ensure they are properly marked for replacement
        const index = this.writeBuffer.findIndex(r => r.id === record.id)
        let lineNumber = null

        if (this.opts.debugMode) {
          console.log(`🔄 UPDATE: writeBuffer.findIndex for ${record.id} returned ${index}`)
          console.log(`🔄 UPDATE: writeBuffer length: ${this.writeBuffer.length}`)
          console.log(`🔄 UPDATE: writeBuffer IDs:`, this.writeBuffer.map(r => r.id))
        }

        if (index !== -1) {
          // Record is already in writeBuffer, update it
          this.writeBuffer[index] = updated
          lineNumber = this._getAbsoluteLineNumber(index)
          if (this.opts.debugMode) {
            console.log(`🔄 UPDATE: Updated existing writeBuffer record at index ${index}`)
            console.log(`🔄 UPDATE: writeBuffer now has ${this.writeBuffer.length} records`)
          }
        } else {
          // Record is in file, add updated version to writeBuffer
          // CRITICAL FIX: Ensure the old record in file will be replaced by checking if it exists in offsets
          // The save() method will handle replacement via _streamExistingRecords which checks updatedRecordsMap
          this.writeBuffer.push(updated)
          lineNumber = this._getAbsoluteLineNumber(this.writeBuffer.length - 1)
          if (this.opts.debugMode) {
            console.log(`🔄 UPDATE: Added updated record to writeBuffer (will replace file record ${record.id})`)
            console.log(`🔄 UPDATE: writeBuffer now has ${this.writeBuffer.length} records`)
          }
        }
        
        const indexUpdateStart = Date.now()
        await this.indexManager.update(record, updated, lineNumber)
        if (this.opts.debugMode) {
          console.log(`🔄 UPDATE: Index update completed in ${Date.now() - indexUpdateStart}ms`)
        }
        
        updatedRecords.push(updated)
        if (this.opts.debugMode) {
          console.log(`🔄 UPDATE: Record ${record.id} completed in ${Date.now() - recordStart}ms`)
        }
      }
      
      this.shouldSave = true
      this.performanceStats.operations++
      
      if (this.opts.debugMode) {
        console.log(`🔄 UPDATE COMPLETED: ${updatedRecords.length} records updated in ${Date.now() - startTime}ms`)
      }
      
      this.emit('updated', updatedRecords)
      return updatedRecords
      } finally {
        this.isInsideOperationQueue = false
      }
    })
  }

  /**
   * Delete records matching criteria
   */
  async delete(criteria) {
    this._validateInitialization('delete')

    return this.operationQueue.enqueue(async () => {
      this.isInsideOperationQueue = true
      try {
        // CRITICAL FIX: Validate state before delete operation
        this.validateState()

        // 🔧 NEW: Validate indexed query mode for delete operations
        if (this.opts.indexedQueryMode === 'strict') {
          this._validateIndexedQuery(criteria, { operation: 'delete' })
        }

        // ⚠️ NEW: Warn about non-indexed fields in permissive mode
        if (this.opts.indexedQueryMode !== 'strict') {
          const indexedFields = Object.keys(this.opts.indexes || {})
          const queryFields = this._extractQueryFields(criteria)
          const nonIndexedFields = queryFields.filter(field => !indexedFields.includes(field))

          if (nonIndexedFields.length > 0) {
            if (this.opts.debugMode) {
              console.warn(`⚠️ Delete operation using non-indexed fields: ${nonIndexedFields.join(', ')}`)
              console.warn(`   This may be slow or fail silently. Consider indexing these fields.`)
            }
          }
        }

      const records = await this.find(criteria)
      const deletedIds = []
      
      if (this.opts.debugMode) {
        console.log(`🗑️ Delete operation: found ${records.length} records to delete`)
        console.log(`🗑️ Records to delete:`, records.map(r => ({ id: r.id, name: r.name })))
        console.log(`🗑️ Current writeBuffer length: ${this.writeBuffer.length}`)
        console.log(`🗑️ Current deletedIds:`, Array.from(this.deletedIds))
      }
      
      for (const record of records) {
        // Remove term mapping
        this.removeTermMapping(record)
        
        await this.indexManager.remove(record)
        
        // Remove record from writeBuffer or mark as deleted
        const index = this.writeBuffer.findIndex(r => r.id === record.id)
        if (index !== -1) {
          this.writeBuffer.splice(index, 1)
          if (this.opts.debugMode) {
            console.log(`🗑️ Removed record ${record.id} from writeBuffer`)
          }
        } else {
          // If record is not in writeBuffer (was saved), mark it as deleted
          this.deletedIds.add(record.id)
          if (this.opts.debugMode) {
            console.log(`🗑️ Marked record ${record.id} as deleted (not in writeBuffer)`)
          }
        }
        deletedIds.push(record.id)
      }
      
      if (this.opts.debugMode) {
        console.log(`🗑️ After delete: writeBuffer length: ${this.writeBuffer.length}, deletedIds:`, Array.from(this.deletedIds))
      }
      
      this.shouldSave = true
      this.performanceStats.operations++
      
      this.emit('deleted', deletedIds)
      return deletedIds
      } finally {
        this.isInsideOperationQueue = false
      }
    })
  }


  /**
   * Generate a unique ID
   */
  generateId() {
    return Date.now().toString(36) + Math.random().toString(36).substr(2)
  }

  /**
   * Apply schema enforcement to a record
   * Converts object to array and back to enforce schema (remove extra fields, add undefined for missing fields)
   */
  applySchemaEnforcement(record) {
    // Only apply schema enforcement if fields configuration is explicitly provided
    if (!this.opts.fields) {
      return record // No schema enforcement without explicit fields configuration
    }

    if (!this.serializer || !this.serializer.schemaManager || !this.serializer.schemaManager.isInitialized) {
      return record // No schema enforcement if schema not initialized
    }

    // Convert to array format (enforces schema)
    const arrayFormat = this.serializer.convertToArrayFormat(record)
    
    // Convert back to object (only schema fields will be present)
    const enforcedRecord = this.serializer.convertFromArrayFormat(arrayFormat)
    
    // Preserve the ID if it exists
    if (record.id) {
      enforcedRecord.id = record.id
    }
    
    return enforcedRecord
  }

  /**
   * Initialize schema for array-based serialization
   */
  initializeSchema() {
    if (!this.serializer || !this.serializer.schemaManager) {
      return
    }

    // Initialize from fields configuration (mandatory)
    if (this.opts.fields && typeof this.opts.fields === 'object') {
      const fieldNames = Object.keys(this.opts.fields)
      if (fieldNames.length > 0) {
        this.serializer.initializeSchema(fieldNames)
        if (this.opts.debugMode) {
          console.log(`🔍 Schema initialized from fields: ${fieldNames.join(', ')} [${this.instanceId}]`)
        }
        return
      }
    }

    // Try to auto-detect schema from existing data (fallback for migration scenarios)
    if (this.data && this.data.length > 0) {
      this.serializer.initializeSchema(this.data, true) // autoDetect = true
      if (this.opts.debugMode) {
        console.log(`🔍 Schema auto-detected from data: ${this.serializer.getSchema().join(', ')} [${this.instanceId}]`)
      }
      return
    }

    if (this.opts.debugMode) {
      console.log(`🔍 No schema initialization possible - will auto-detect on first insert [${this.instanceId}]`)
    }
  }

  /**
   * Get database length (number of records)
   */
  get length() {
    // Return total records: writeBuffer + saved records
    // writeBuffer contains unsaved records
    // For saved records, use the length of offsets array (number of saved records)
    const savedRecords = this.offsets.length
    const writeBufferRecords = this.writeBuffer.length
    
    // CRITICAL FIX: If offsets are empty but indexOffset exists, use fallback calculation
    // This handles cases where offsets weren't loaded or were reset
    if (savedRecords === 0 && this.indexOffset > 0 && this.initialized) {
      // Try to use IndexManager totalLines if available
      if (this.indexManager && this.indexManager.totalLines > 0) {
        return this.indexManager.totalLines + writeBufferRecords
      }
      
      // Fallback: estimate from indexOffset (less accurate but better than 0)
      // This is a defensive fix for cases where offsets are missing but file has data
      if (this.opts.debugMode) {
        console.log(`⚠️  LENGTH: offsets array is empty but indexOffset=${this.indexOffset}, using IndexManager.totalLines or estimation`)
      }
    }
    
    // CRITICAL FIX: Validate that offsets array is consistent with actual data
    // This prevents the bug where database reassignment causes desynchronization
    if (this.initialized && savedRecords > 0) {
      try {
        // Check if the offsets array is consistent with the actual file
        // If offsets exist but file is empty or corrupted, reset offsets
        if (this.fileHandler && this.fileHandler.file) {
          try {
            // Use synchronous file stats to check if file is empty
            const stats = fs.statSync(this.fileHandler.file)
            if (stats && stats.size === 0 && savedRecords > 0) {
              // File is empty but offsets array has records - this is the bug condition
              if (this.opts.debugMode) {
                console.log(`🔧 LENGTH FIX: Detected desynchronized offsets (${savedRecords} records) with empty file, resetting offsets`)
              }
              this.offsets = []
              return writeBufferRecords // Return only writeBuffer records
            }
          } catch (fileError) {
            // File doesn't exist or can't be read - reset offsets
            if (savedRecords > 0) {
              if (this.opts.debugMode) {
                console.log(`🔧 LENGTH FIX: File doesn't exist but offsets array has ${savedRecords} records, resetting offsets`)
              }
              this.offsets = []
              return writeBufferRecords
            }
          }
        }
      } catch (error) {
        // If we can't validate, fall back to the original behavior
        if (this.opts.debugMode) {
          console.log(`🔧 LENGTH FIX: Could not validate offsets, using original calculation: ${error.message}`)
        }
      }
    }
    
    return writeBufferRecords + savedRecords
  }


  /**
   * Calculate current writeBuffer size in bytes (similar to published v1.1.0)
   */
  currentWriteBufferSize() {
    return this.writeBufferTotalSize || 0
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
    }
    
    // Add term mapping stats if enabled
    if (this.opts.termMapping && this.termManager) {
      stats.termMapping = this.termManager.getStats()
    }
    
    return stats
  }

  /**
   * Initialize database (alias for initialize for backward compatibility)
   */
  async init() {
    return this.initialize()
  }

  /**
   * Schedule index rebuild when index data is missing or corrupted
   * @private
   */
  _scheduleIndexRebuild() {
    // Mark that rebuild is needed
    this._indexRebuildNeeded = true
    
    // Rebuild will happen lazily on first query if index is empty
    // This avoids blocking init() but ensures index is available when needed
  }

  /**
   * Rebuild indexes from data file if needed
   * @private
   */
  async _rebuildIndexesIfNeeded() {
    if (this.opts.debugMode) {
      console.log(`🔍 _rebuildIndexesIfNeeded called: _indexRebuildNeeded=${this._indexRebuildNeeded}`)
    }
    if (!this._indexRebuildNeeded) return
    if (!this.indexManager || !this.indexManager.indexedFields || this.indexManager.indexedFields.length === 0) return
    
    // Check if index actually needs rebuilding
    let needsRebuild = false
    for (const field of this.indexManager.indexedFields) {
      if (!this.indexManager.hasUsableIndexData(field)) {
        needsRebuild = true
        break
      }
    }
    
    if (!needsRebuild) {
      this._indexRebuildNeeded = false
      return
    }
    
    // Check if rebuild is allowed
    if (!this.opts.allowIndexRebuild) {
      const idxPath = this.normalizedFile.replace('.jdb', '.idx.jdb')
      throw new Error(
        `Index rebuild required but disabled: Index file ${idxPath} is corrupted or missing, ` +
        `and allowIndexRebuild is set to false. ` +
        `Set allowIndexRebuild: true to automatically rebuild the index, ` +
        `or manually fix/delete the corrupted index file.`
      )
    }
    
    if (this.opts.debugMode) {
      console.log('🔨 Rebuilding indexes from data file...')
    }
    
    try {
      // Read all records and rebuild index
      let count = 0
      const startTime = Date.now()
      
      // Auto-detect schema from first line if not initialized
      if (!this.serializer.schemaManager.isInitialized) {
        const fs = await import('fs')
        const readline = await import('readline')
        const stream = fs.createReadStream(this.fileHandler.file, {
          highWaterMark: 64 * 1024,
          encoding: 'utf8'
        })
        const rl = readline.createInterface({
          input: stream,
          crlfDelay: Infinity
        })
        
        for await (const line of rl) {
          if (line && line.trim()) {
            try {
              const firstRecord = JSON.parse(line)
              if (Array.isArray(firstRecord)) {
                // Try to infer schema from opts.fields if available
                if (this.opts.fields && typeof this.opts.fields === 'object') {
                  const fieldNames = Object.keys(this.opts.fields)
                  if (fieldNames.length >= firstRecord.length) {
                    // Use first N fields from opts.fields to match array length
                    const schema = fieldNames.slice(0, firstRecord.length)
                    this.serializer.initializeSchema(schema)
                    if (this.opts.debugMode) {
                      console.log(`🔍 Inferred schema from opts.fields: ${schema.join(', ')}`)
                    }
                  } else {
                    throw new Error(`Cannot rebuild index: array has ${firstRecord.length} elements but opts.fields only defines ${fieldNames.length} fields. Schema must be explicitly provided.`)
                  }
                } else {
                  throw new Error('Cannot rebuild index: schema missing, file uses array format, and opts.fields not provided. The .idx.jdb file is corrupted.')
                }
              } else {
                // Object format, initialize from object keys
                this.serializer.initializeSchema(firstRecord, true)
                if (this.opts.debugMode) {
                  console.log(`🔍 Auto-detected schema from object: ${Object.keys(firstRecord).join(', ')}`)
                }
              }
              break
            } catch (error) {
              if (this.opts.debugMode) {
                console.error('❌ Failed to auto-detect schema:', error.message)
              }
              throw error
            }
          }
        }
        stream.destroy()
      }
      
      // Use streaming to read records without loading everything into memory
      // Also rebuild offsets while we're at it
      const fs = await import('fs')
      const readline = await import('readline')
      
      this.offsets = []
      let currentOffset = 0
      
      const stream = fs.createReadStream(this.fileHandler.file, {
        highWaterMark: 64 * 1024,
        encoding: 'utf8'
      })
      
      const rl = readline.createInterface({
        input: stream,
        crlfDelay: Infinity
      })
      
      try {
        for await (const line of rl) {
          if (line && line.trim()) {
            try {
              // Record the offset for this line
              this.offsets.push(currentOffset)
              
              const record = this.serializer.deserialize(line)
              const recordWithTerms = this.restoreTermIdsAfterDeserialization(record)
              await this.indexManager.add(recordWithTerms, count)
              count++
            } catch (error) {
              // Skip invalid lines
              if (this.opts.debugMode) {
                console.log(`⚠️ Rebuild: Failed to deserialize line ${count}:`, error.message)
              }
            }
          }
          // Update offset for next line (including newline character)
          currentOffset += Buffer.byteLength(line, 'utf8') + 1
        }
      } finally {
        stream.destroy()
      }
      
      // Update indexManager totalLines
      if (this.indexManager) {
        this.indexManager.setTotalLines(this.offsets.length)
      }
      
      this._indexRebuildNeeded = false
      
      if (this.opts.debugMode) {
        console.log(`✅ Index rebuilt from ${count} records in ${Date.now() - startTime}ms`)
      }
      
      // Save the rebuilt index
      await this._saveIndexDataToFile()
    } catch (error) {
      if (this.opts.debugMode) {
        console.error('❌ Failed to rebuild indexes:', error.message)
      }
      // Don't throw - queries will fall back to streaming
    }
  }

  /**
   * Destroy database - DESTRUCTIVE MODE
   * Assumes save() has already been called by user
   * If anything is still active, it indicates a bug - log error and force cleanup
   */
  async destroy() {
    if (this.destroyed) return
    
    // Mark as destroying immediately to prevent new operations
    this.destroying = true
    
    // Wait for all active insert sessions to complete before destroying
    if (this.activeInsertSessions.size > 0) {
      if (this.opts.debugMode) {
        console.log(`⏳ destroy: Waiting for ${this.activeInsertSessions.size} active insert sessions`)
      }
      
      const sessionPromises = Array.from(this.activeInsertSessions).map(session => 
        session.waitForOperations(null) // Wait indefinitely for sessions to complete
      )
      
      try {
        await Promise.all(sessionPromises)
      } catch (error) {
        if (this.opts.debugMode) {
          console.log(`⚠️ destroy: Error waiting for sessions: ${error.message}`)
        }
        // Continue with destruction even if sessions have issues
      }
      
      // Destroy all active sessions
      for (const session of this.activeInsertSessions) {
        session.destroy()
      }
      this.activeInsertSessions.clear()
    }
    
    // CRITICAL FIX: Add timeout protection to prevent destroy() from hanging
    const destroyPromise = this._performDestroy()
    let timeoutHandle = null
    const timeoutPromise = new Promise((_, reject) => {
      timeoutHandle = setTimeout(() => {
        reject(new Error('Destroy operation timed out after 5 seconds'))
      }, 5000)
    })
    
    try {
      await Promise.race([destroyPromise, timeoutPromise])
    } catch (error) {
      if (error.message === 'Destroy operation timed out after 5 seconds') {
        console.error('🚨 DESTROY TIMEOUT: Force destroying database after timeout')
        // Force mark as destroyed even if cleanup failed
        this.destroyed = true
        this.destroying = false
        return
      }
      throw error
    } finally {
      // Clear the timeout to prevent Jest open handle warning
      if (timeoutHandle) {
        clearTimeout(timeoutHandle)
      }
    }
  }

  /**
   * Internal destroy implementation
   */
  async _performDestroy() {
    try {
      // CRITICAL: Check for bugs - anything active indicates save() didn't work properly
      const bugs = []
      
      // Check for pending data that should have been saved
      if (this.writeBuffer.length > 0) {
        const bug = `BUG: writeBuffer has ${this.writeBuffer.length} records - save() should have cleared this`
        bugs.push(bug)
        console.error(`🚨 ${bug}`)
      }
      
      // Check for pending operations that should have completed
      if (this.pendingOperations.size > 0) {
        const bug = `BUG: ${this.pendingOperations.size} pending operations - save() should have completed these`
        bugs.push(bug)
        console.error(`🚨 ${bug}`)
      }
      
      // Auto-save manager removed - no cleanup needed
      
      // Check for active save operation
      if (this.isSaving) {
        const bug = `BUG: save operation still active - previous save() should have completed`
        bugs.push(bug)
        console.error(`🚨 ${bug}`)
      }
      
      // If bugs detected, throw error with details
      if (bugs.length > 0) {
        const errorMessage = `Database destroy() found ${bugs.length} bug(s) - save() did not complete properly:\n${bugs.join('\n')}`
        console.error(`🚨 DESTROY ERROR: ${errorMessage}`)
        throw new Error(errorMessage)
      }
      
      // FORCE DESTRUCTIVE CLEANUP - no waiting, no graceful shutdown
      if (this.opts.debugMode) {
        console.log('💥 DESTRUCTIVE DESTROY: Force cleaning up all resources')
      }
      
      // Cancel all operations immediately
      this.abortController.abort()
      
      // Auto-save removed - no cleanup needed
      
      // Clear all buffers and data structures
      this.writeBuffer = []
      this.writeBufferOffsets = []
      this.writeBufferSizes = []
      this.writeBufferTotalSize = 0
      this.writeBufferTotalSize = 0
      this.deletedIds.clear()
      this.pendingOperations.clear()
      this.pendingIndexUpdates = []
      
      // Force close file handlers
      if (this.fileHandler) {
        try {
          // Force close any open file descriptors
          await this.fileHandler.close?.()
        } catch (error) {
          // Ignore file close errors during destructive cleanup
        }
      }
      
      // Clear all managers
      if (this.indexManager) {
        this.indexManager.clear?.()
      }
      
      if (this.termManager) {
        this.termManager.clear?.()
      }
      
      if (this.queryManager) {
        this.queryManager.clear?.()
      }
      
      // Clear operation queue
      if (this.operationQueue) {
        this.operationQueue.clear?.()
        this.operationQueue = null
      }
      
      // Mark as destroyed
      this.destroyed = true
      this.destroying = false
      
      if (this.opts.debugMode) {
        console.log('💥 DESTRUCTIVE DESTROY: Database completely destroyed')
      }
      
    } catch (error) {
      // Even if cleanup fails, mark as destroyed
      this.destroyed = true
      this.destroying = false
      
      // Re-throw the error so user knows about the bug
      throw error
    }
  }

  /**
   * Find one record
   */
  async findOne(criteria, options = {}) {
    this._validateInitialization('findOne')
    
    const results = await this.find(criteria, { ...options, limit: 1 })
    return results.length > 0 ? results[0] : null
  }

  /**
   * Count records
   */
  async count(criteria = {}, options = {}) {
    this._validateInitialization('count')
    
    // OPTIMIZATION: Use queryManager.count() instead of find() for better performance
    // This is especially faster for indexed queries which can use indexManager.query().size
    const fileCount = await this.queryManager.count(criteria, options)
    
    // Count matching records in writeBuffer
    const writeBufferCount = this.writeBuffer.filter(record => 
      this.queryManager.matchesCriteria(record, criteria, options)
    ).length
    
    return fileCount + writeBufferCount
  }

  /**
   * Check if any records exist matching the given criteria (ultra-fast when using indexed fields)
   *
   * @param {string|object} fieldName - Indexed field name (legacy) OR query criteria object (new)
   * @param {string|Array<string>} terms - Single term or array of terms (when using legacy syntax)
   * @param {Object} options - Options: { $all: true/false, caseInsensitive: true/false, excludes: Array<string> }
   * @returns {Promise<boolean>} - True if at least one match exists
   *
   * @example
   * // Legacy syntax - ultra-fast index-only check
   * const exists = await db.exists('nameTerms', 'tv');
   * const existsAll = await db.exists('nameTerms', ['tv', 'globo'], { $all: true });
   *
   * @example
   * // New syntax - full query criteria support
   * const exists = await db.exists({ mediaType: 'live', status: 'active' });
   * const existsOr = await db.exists({ mediaType: ['live', 'vod'] });
   */
  async exists(fieldNameOrCriteria, terms, options = {}) {
    this._validateInitialization('exists')

    // Detect syntax: new criteria object vs legacy field/terms
    if (typeof fieldNameOrCriteria === 'object' && fieldNameOrCriteria !== null && !Array.isArray(fieldNameOrCriteria)) {
      // New syntax: exists(criteria)
      const criteria = fieldNameOrCriteria
      return this._existsWithCriteria(criteria)
    } else if (typeof fieldNameOrCriteria === 'string' || fieldNameOrCriteria === null || Array.isArray(fieldNameOrCriteria)) {
      // Legacy syntax: exists(fieldName, terms, options)
      // Also handle invalid inputs (null, array) for backward compatibility
      const fieldName = fieldNameOrCriteria
      return this.indexManager.exists(fieldName, terms, options)
    } else {
      // Invalid input type
      throw new Error('First parameter must be a string (fieldName) or object (criteria)')
    }
  }

  /**
   * Check if any records exist using full query criteria
   * Uses index intersection when possible for maximum performance
   * @private
   * @param {object} criteria - Query criteria object
   * @returns {Promise<boolean>} - True if at least one match exists
   */

  async _existsWithCriteria(criteria) {
    if (criteria === null || criteria === undefined || typeof criteria !== 'object' || Array.isArray(criteria)) {
      throw new Error('Criteria must be a non-null object')
    }

    // Check if criteria is empty (should match all records)
    const criteriaFields = Object.keys(criteria)
    if (criteriaFields.length === 0) {
      // Empty criteria matches all records - check if any exist
      try {
        const result = await this.find({}, { limit: 1 })
        return result.length > 0
      } catch (error) {
        return false
      }
    }

    // 🎯 ELEGANT SOLUTION: Use the same find() logic for perfect consistency
    // This ensures exists() uses identical logic to find() for all criteria processing
    try {
      const result = await this.find(criteria, { limit: 1 })
      return result.length > 0
    } catch (error) {
      // If find() fails (e.g., strict mode violations), no records exist
      return false
    }
  }

  /**
   * Calculate coverage for grouped include/exclude term sets
   * @param {string} fieldName - Name of the indexed field
   * @param {Array<object>} groups - Array of { terms, excludes } objects
   * @param {object} filterCriteria - Optional filter criteria (indexed fields only)
   * @param {object} options - Optional settings
   * @returns {Promise<number>} Coverage percentage between 0 and 100
   */
  async coverage(fieldName, groups, filterCriteria = null, options = {}) {
    this._validateInitialization('coverage')

    if (typeof fieldName !== 'string' || !fieldName.trim()) {
      throw new Error('fieldName must be a non-empty string')
    }

    if (!Array.isArray(groups)) {
      throw new Error('groups must be an array')
    }

    if (groups.length === 0) {
      return 0
    }

    if (!this.opts.indexes || !this.opts.indexes[fieldName]) {
      throw new Error(`Field "${fieldName}" is not indexed`)
    }

    const fieldType = this.opts.indexes[fieldName]
    const supportedTypes = ['array:string', 'string']
    if (!supportedTypes.includes(fieldType)) {
      throw new Error(`coverage() only supports fields of type ${supportedTypes.join(', ')} (found: ${fieldType})`)
    }

    const fieldIndex = this.indexManager?.index?.data?.[fieldName]
    if (!fieldIndex) {
      return 0
    }

    // Validate filter criteria - only indexed fields allowed for performance
    let filteredLines = null
    if (filterCriteria && typeof filterCriteria === 'object') {
      if (Array.isArray(filterCriteria)) {
        throw new Error('filterCriteria must be an object, not an array')
      }

      // Check that all filter fields are indexed
      for (const field of Object.keys(filterCriteria)) {
        if (!this.opts.indexes || !this.opts.indexes[field]) {
          throw new Error(`Filter field "${field}" must be indexed for coverage() performance. Add it to indexes in database options.`)
        }
      }

      // Get filtered records using QueryManager for consistency
      try {
        const filteredRecords = await this.queryManager.find(filterCriteria, {
          limit: null, // Get all matching records for coverage calculation
          indexedQueryMode: this.opts.indexedQueryMode,
          allowNonIndexed: false
        })
        filteredLines = new Set(filteredRecords.map(record => record._))
        if (filteredLines.size === 0) {
          return 0 // No records match the filter
        }
      } catch (error) {
        // If filtering fails, return 0 (no coverage possible)
        return 0
      }
    }

    const isTermMapped = this.termManager &&
      this.termManager.termMappingFields &&
      this.termManager.termMappingFields.includes(fieldName)

    const normalizeTerm = (term) => {
      if (term === undefined || term === null) {
        return ''
      }
      return String(term).trim()
    }

    const resolveKey = (term) => {
      if (isTermMapped) {
        const termId = this.termManager.getTermIdWithoutIncrement(term)
        if (termId === null || termId === undefined) {
          return null
        }
        return String(termId)
      }
      return String(term)
    }

    let matchedGroups = 0

    for (const group of groups) {
      if (!group || typeof group !== 'object') {
        throw new Error('Each coverage group must be an object')
      }

      const includeTermsRaw = Array.isArray(group.terms) ? group.terms : []
      const excludeTermsRaw = Array.isArray(group.excludes) ? group.excludes : []

      const includeTerms = Array.from(new Set(
        includeTermsRaw
          .map(normalizeTerm)
          .filter(term => term.length > 0)
      ))

      if (includeTerms.length === 0) {
        throw new Error('Each coverage group must define at least one term')
      }

      const excludeTerms = Array.from(new Set(
        excludeTermsRaw
          .map(normalizeTerm)
          .filter(term => term.length > 0)
      ))

      let candidateLines = null
      let groupMatched = true

      for (const term of includeTerms) {
        const key = resolveKey(term)
        if (key === null) {
          groupMatched = false
          break
        }

        const termData = fieldIndex[key]
        if (!termData) {
          groupMatched = false
          break
        }

        const lineNumbers = this.indexManager._getAllLineNumbers(termData)
        if (!lineNumbers || lineNumbers.length === 0) {
          groupMatched = false
          break
        }

        // Apply filter if specified
        let validLineNumbers = lineNumbers
        if (filteredLines) {
          validLineNumbers = lineNumbers.filter(line => filteredLines.has(line))
          if (validLineNumbers.length === 0) {
            groupMatched = false
            break
          }
        }

        if (candidateLines === null) {
          candidateLines = new Set(validLineNumbers)
        } else {
          const termSet = new Set(validLineNumbers)
          for (const line of Array.from(candidateLines)) {
            if (!termSet.has(line)) {
              candidateLines.delete(line)
            }
          }
        }

        if (!candidateLines || candidateLines.size === 0) {
          groupMatched = false
          break
        }
      }

      if (!groupMatched || !candidateLines || candidateLines.size === 0) {
        continue
      }

      for (const term of excludeTerms) {
        const key = resolveKey(term)
        if (key === null) {
          continue
        }

        const termData = fieldIndex[key]
        if (!termData) {
          continue
        }

        const excludeLines = this.indexManager._getAllLineNumbers(termData)
        if (!excludeLines || excludeLines.length === 0) {
          continue
        }

        // Apply filter to exclude lines if specified
        let validExcludeLines = excludeLines
        if (filteredLines) {
          validExcludeLines = excludeLines.filter(line => filteredLines.has(line))
        }

        for (const line of validExcludeLines) {
          if (!candidateLines.size) {
            break
          }
          candidateLines.delete(line)
        }

        if (!candidateLines.size) {
          break
        }
      }

      if (candidateLines && candidateLines.size > 0) {
        matchedGroups++
      }
    }

    if (matchedGroups === 0) {
      return 0
    }

    const precision = typeof options.precision === 'number' && options.precision >= 0
      ? options.precision
      : 2

    const coverageValue = (matchedGroups / groups.length) * 100
    return Number(coverageValue.toFixed(precision))
  }

  /**
   * Score records based on weighted terms in an indexed array:string field
   * @param {string} fieldName - Name of indexed array:string field
   * @param {object} scores - Map of terms to numeric weights
   * @param {object} options - Query options
   * @returns {Promise<Array>} Records with scores, sorted by score
   */
  async score(fieldName, scores, options = {}) {
    // Validate initialization
    this._validateInitialization('score')
    
    // Set default options
    const opts = {
      limit: options.limit ?? 100,
      sort: options.sort ?? 'desc',
      includeScore: options.includeScore !== false,
      mode: options.mode ?? 'sum'
    }
    
    // Validate fieldName
    if (typeof fieldName !== 'string' || !fieldName) {
      throw new Error('fieldName must be a non-empty string')
    }
    
    // Validate scores object
    if (!scores || typeof scores !== 'object' || Array.isArray(scores)) {
      throw new Error('scores must be an object')
    }
    
    // Handle empty scores - return empty array as specified
    if (Object.keys(scores).length === 0) {
      return []
    }
    
    // Validate scores values are numeric
    for (const [term, weight] of Object.entries(scores)) {
      if (typeof weight !== 'number' || isNaN(weight)) {
        throw new Error(`Score value for term "${term}" must be a number`)
      }
    }

    // Validate mode
    const allowedModes = new Set(['sum', 'max', 'avg', 'first'])
    if (!allowedModes.has(opts.mode)) {
      throw new Error(`Invalid score mode "${opts.mode}". Must be one of: ${Array.from(allowedModes).join(', ')}`)
    }
    
    // Check if field is indexed and is array:string type
    if (!this.opts.indexes || !this.opts.indexes[fieldName]) {
      throw new Error(`Field "${fieldName}" is not indexed`)
    }
    
    const fieldType = this.opts.indexes[fieldName]
    if (fieldType !== 'array:string') {
      throw new Error(`Field "${fieldName}" must be of type "array:string" (found: ${fieldType})`)
    }
    
    // Check if this is a term-mapped field
    const isTermMapped = this.termManager && 
      this.termManager.termMappingFields && 
      this.termManager.termMappingFields.includes(fieldName)
    
    // Access the index for this field
    const fieldIndex = this.indexManager.index.data[fieldName]
    if (!fieldIndex) {
      return []
    }
    
    // Accumulate scores for each line number
    const scoreMap = new Map()
    const countMap = opts.mode === 'avg' ? new Map() : null
    
    // Iterate through each term in the scores object
    for (const [term, weight] of Object.entries(scores)) {
      // Get term ID if this is a term-mapped field
      let termKey
      if (isTermMapped) {
        // For term-mapped fields, convert term to term ID
        const termId = this.termManager.getTermIdWithoutIncrement(term)
        if (termId === null || termId === undefined) {
          // Term doesn't exist, skip it
          continue
        }
        termKey = String(termId)
      } else {
        termKey = String(term)
      }
      
      // Look up line numbers for this term
      const termData = fieldIndex[termKey]
      if (!termData) {
        // Term doesn't exist in index, skip
        continue
      }
      
      // Get all line numbers for this term
      const lineNumbers = this.indexManager._getAllLineNumbers(termData)
      
      // Add weight to score for each line number
      for (const lineNumber of lineNumbers) {
        const currentScore = scoreMap.get(lineNumber)
        
        switch (opts.mode) {
          case 'sum': {
            const nextScore = (currentScore || 0) + weight
            scoreMap.set(lineNumber, nextScore)
            break
          }
          case 'max': {
            if (currentScore === undefined) {
              scoreMap.set(lineNumber, weight)
            } else {
              scoreMap.set(lineNumber, Math.max(currentScore, weight))
            }
            break
          }
          case 'avg': {
            const previous = currentScore || 0
            scoreMap.set(lineNumber, previous + weight)
            const count = (countMap.get(lineNumber) || 0) + 1
            countMap.set(lineNumber, count)
            break
          }
          case 'first': {
            if (currentScore === undefined) {
              scoreMap.set(lineNumber, weight)
            }
            break
          }
        }
      }
    }

    // For average mode, divide total by count
    if (opts.mode === 'avg') {
      for (const [lineNumber, totalScore] of scoreMap.entries()) {
        const count = countMap.get(lineNumber) || 1
        scoreMap.set(lineNumber, totalScore / count)
      }
    }
    
    // Filter out zero scores and sort by score
    const scoredEntries = Array.from(scoreMap.entries())
      .filter(([, score]) => score > 0)
    
    // Sort by score
    scoredEntries.sort((a, b) => {
      return opts.sort === 'asc' ? a[1] - b[1] : b[1] - a[1]
    })
    
    // Apply limit
    const limitedEntries = opts.limit > 0 
      ? scoredEntries.slice(0, opts.limit)
      : scoredEntries
    
    if (limitedEntries.length === 0) {
      return []
    }
    
    // Fetch actual records
    const lineNumbers = limitedEntries.map(([lineNumber]) => lineNumber)
    const scoresByLineNumber = new Map(limitedEntries)
    
    const persistedCount = Array.isArray(this.offsets) ? this.offsets.length : 0
    
    // Separate lineNumbers into file records and writeBuffer records
    const fileLineNumbers = []
    const writeBufferLineNumbers = []
    
    for (const lineNumber of lineNumbers) {
      if (lineNumber >= persistedCount) {
        // This lineNumber points to writeBuffer
        writeBufferLineNumbers.push(lineNumber)
      } else {
        // This lineNumber points to file
        fileLineNumbers.push(lineNumber)
      }
    }
    
    const results = []
    
    // Read records from file
    if (fileLineNumbers.length > 0) {
      const ranges = this.getRanges(fileLineNumbers)
      if (ranges.length > 0) {
        // Create a map from start offset to lineNumber for accurate mapping
        const startToLineNumber = new Map()
        for (const range of ranges) {
          if (range.index !== undefined) {
            startToLineNumber.set(range.start, range.index)
          }
        }
        
        const groupedRanges = await this.fileHandler.groupedRanges(ranges)
        
        const fs = await import('fs')
        const fd = await fs.promises.open(this.fileHandler.file, 'r')
        
        try {
          for (const groupedRange of groupedRanges) {
            for await (const row of this.fileHandler.readGroupedRange(groupedRange, fd)) {
              try {
                const record = this.serializer.deserialize(row.line)
                
                // Get line number from the row, fallback to start offset mapping
                let lineNumber = row._ !== null && row._ !== undefined ? row._ : (startToLineNumber.get(row.start) ?? 0)
                
                // Restore term IDs to terms
                const recordWithTerms = this.restoreTermIdsAfterDeserialization(record)
                
                // Add line number
                recordWithTerms._ = lineNumber
                
                // Add score if includeScore is true (default is true)
                if (opts.includeScore !== false) {
                  recordWithTerms.score = scoresByLineNumber.get(lineNumber) || 0
                }
                
                results.push(recordWithTerms)
              } catch (error) {
                // Skip invalid lines
                if (this.opts.debugMode) {
                  console.error('Error deserializing record in score():', error)
                }
              }
            }
          }
        } finally {
          await fd.close()
        }
      }
    }
    
    // Read records from writeBuffer
    if (writeBufferLineNumbers.length > 0 && this.writeBuffer) {
      for (const lineNumber of writeBufferLineNumbers) {
        const writeBufferIndex = lineNumber - persistedCount
        if (writeBufferIndex >= 0 && writeBufferIndex < this.writeBuffer.length) {
          const record = this.writeBuffer[writeBufferIndex]
          if (record) {
            // Restore term IDs to terms
            const recordWithTerms = this.restoreTermIdsAfterDeserialization(record)
            
            // Add line number
            recordWithTerms._ = lineNumber
            
            // Add score if includeScore is true
            if (opts.includeScore) {
              recordWithTerms.score = scoresByLineNumber.get(lineNumber) || 0
            }
            
            results.push(recordWithTerms)
          }
        }
      }
    }
    
    // Re-sort results to maintain score order (since reads might be out of order)
    results.sort((a, b) => {
      const scoreA = scoresByLineNumber.get(a._) || 0
      const scoreB = scoresByLineNumber.get(b._) || 0
      return opts.sort === 'asc' ? scoreA - scoreB : scoreB - scoreA
    })
    
    return results
  }

  /**
   * Wait for all pending operations to complete
   */
  async _waitForPendingOperations() {
    if (this.operationQueue && this.operationQueue.getQueueLength() > 0) {
      if (this.opts.debugMode) {
        console.log('💾 Save: Waiting for pending operations to complete')
      }
      // CRITICAL FIX: Wait without timeout to ensure all operations complete
      // This prevents race conditions and data loss
      await this.operationQueue.waitForCompletion(null)
      
      // Verify queue is actually empty
      if (this.operationQueue.getQueueLength() > 0) {
        throw new Error('Operation queue not empty after wait')
      }
    }
  }

  /**
   * Flush write buffer completely with smart detection of ongoing insertions
   */
  async _flushWriteBufferCompletely() {
    // Force complete flush of write buffer with intelligent detection
    let attempts = 0
    const maxStuckAttempts = 5 // Maximum attempts with identical data (only protection against infinite loops)
    let stuckAttempts = 0
    let lastBufferSample = null
    
    // CRITICAL FIX: Remove maxAttempts limit - only stop when buffer is empty or truly stuck
    while (this.writeBuffer.length > 0) {
      const currentLength = this.writeBuffer.length
      const currentSample = this._getBufferSample() // Get lightweight sample
      
      // Process write buffer items
      await this._processWriteBuffer()
      
      // Check if buffer is actually stuck (same data) vs new data being added
      if (this.writeBuffer.length === currentLength) {
        // Check if the data is identical (stuck) or new data was added
        if (this._isBufferSampleIdentical(currentSample, lastBufferSample)) {
          stuckAttempts++
          if (this.opts.debugMode) {
            console.log(`💾 Flush: Buffer appears stuck (identical data), attempt ${stuckAttempts}/${maxStuckAttempts}`)
          }
          
          if (stuckAttempts >= maxStuckAttempts) {
            throw new Error(`Write buffer flush stuck - identical data detected after ${maxStuckAttempts} attempts`)
          }
        } else {
          // New data was added, reset stuck counter
          stuckAttempts = 0
          if (this.opts.debugMode) {
            console.log(`💾 Flush: New data detected, continuing flush (${this.writeBuffer.length} items remaining)`)
          }
        }
        lastBufferSample = currentSample
      } else {
        // Progress was made, reset stuck counter
        stuckAttempts = 0
        lastBufferSample = null
        if (this.opts.debugMode) {
          console.log(`💾 Flush: Progress made, ${currentLength - this.writeBuffer.length} items processed, ${this.writeBuffer.length} remaining`)
        }
      }
      
      attempts++
      
      // Small delay to allow ongoing operations to complete
      if (this.writeBuffer.length > 0) {
        await new Promise(resolve => setTimeout(resolve, 10))
      }
    }
    
    // CRITICAL FIX: Remove the artificial limit check - buffer should be empty by now
    // If we reach here, the buffer is guaranteed to be empty due to the while condition
    
    if (this.opts.debugMode) {
      console.log(`💾 Flush completed successfully after ${attempts} attempts`)
    }
  }

  /**
   * Get a lightweight sample of the write buffer for comparison
   * @returns {Object} - Sample data for comparison
   */
  _getBufferSample() {
    if (!this.writeBuffer || this.writeBuffer.length === 0) {
      return null
    }
    
    // Create a lightweight sample using first few items and their IDs
    const sampleSize = Math.min(5, this.writeBuffer.length)
    const sample = {
      length: this.writeBuffer.length,
      firstIds: [],
      lastIds: [],
      checksum: 0
    }
    
    // Sample first few items
    for (let i = 0; i < sampleSize; i++) {
      const item = this.writeBuffer[i]
      if (item && item.id) {
        sample.firstIds.push(item.id)
        // Simple checksum using ID hash
        sample.checksum += item.id.toString().split('').reduce((a, b) => a + b.charCodeAt(0), 0)
      }
    }
    
    // Sample last few items if buffer is large
    if (this.writeBuffer.length > sampleSize) {
      for (let i = Math.max(0, this.writeBuffer.length - sampleSize); i < this.writeBuffer.length; i++) {
        const item = this.writeBuffer[i]
        if (item && item.id) {
          sample.lastIds.push(item.id)
          sample.checksum += item.id.toString().split('').reduce((a, b) => a + b.charCodeAt(0), 0)
        }
      }
    }
    
    return sample
  }
  
  /**
   * Check if two buffer samples are identical (indicating stuck flush)
   * @param {Object} sample1 - First sample
   * @param {Object} sample2 - Second sample
   * @returns {boolean} - True if samples are identical
   */
  _isBufferSampleIdentical(sample1, sample2) {
    if (!sample1 || !sample2) {
      return false
    }
    
    // Quick checks: different lengths or checksums mean different data
    if (sample1.length !== sample2.length || sample1.checksum !== sample2.checksum) {
      return false
    }
    
    // Compare first IDs
    if (sample1.firstIds.length !== sample2.firstIds.length) {
      return false
    }
    
    for (let i = 0; i < sample1.firstIds.length; i++) {
      if (sample1.firstIds[i] !== sample2.firstIds[i]) {
        return false
      }
    }
    
    // Compare last IDs
    if (sample1.lastIds.length !== sample2.lastIds.length) {
      return false
    }
    
    for (let i = 0; i < sample1.lastIds.length; i++) {
      if (sample1.lastIds[i] !== sample2.lastIds[i]) {
        return false
      }
    }
    
    return true
  }

  /**
   * Process write buffer items
   */
  async _processWriteBuffer() {
    // Process write buffer items without loading entire file
    // OPTIMIZATION: Use Set directly for both processing and lookup - single variable, better performance
    const itemsToProcess = new Set(this.writeBuffer)
    
    // CRITICAL FIX: Don't clear writeBuffer immediately - wait for processing to complete
    // This prevents race conditions where new operations arrive while old ones are still processing
    
    // OPTIMIZATION: Separate buffer items from object items for batch processing
    const bufferItems = []
    const objectItems = []
    
    for (const item of itemsToProcess) {
      if (Buffer.isBuffer(item)) {
        bufferItems.push(item)
      } else if (typeof item === 'object' && item !== null) {
        objectItems.push(item)
      }
    }
    
    // Process buffer items individually (they're already optimized)
    for (const buffer of bufferItems) {
      await this._processBufferItem(buffer)
    }
    
    // OPTIMIZATION: Process all object items in a single write operation
    if (objectItems.length > 0) {
      await this._processObjectItemsBatch(objectItems)
    }
    
    // CRITICAL FIX: Only remove processed items from writeBuffer after all async operations complete
    const beforeLength = this.writeBuffer.length
    if (beforeLength > 0) {
      const originalRecords = this.writeBuffer
      const originalOffsets = this.writeBufferOffsets
      const originalSizes = this.writeBufferSizes
      const retainedRecords = []
      const retainedOffsets = []
      const retainedSizes = []
      let retainedTotal = 0
      let removedCount = 0
      
      for (let i = 0; i < originalRecords.length; i++) {
        const record = originalRecords[i]
        if (itemsToProcess.has(record)) {
          removedCount++
          continue
        }
        
        retainedRecords.push(record)
        if (originalOffsets && i < originalOffsets.length) {
          retainedOffsets.push(originalOffsets[i])
        }
        if (originalSizes && i < originalSizes.length) {
          const size = originalSizes[i]
          if (size !== undefined) {
            retainedSizes.push(size)
            retainedTotal += size
          }
        }
      }
      
      if (removedCount > 0) {
        this.writeBuffer = retainedRecords
        this.writeBufferOffsets = retainedOffsets
        this.writeBufferSizes = retainedSizes
        this.writeBufferTotalSize = retainedTotal
      }
    }
    const afterLength = this.writeBuffer.length
    
    if (afterLength === 0) {
      this.writeBufferOffsets = []
      this.writeBufferSizes = []
      this.writeBufferTotalSize = 0
    }
    
    if (this.opts.debugMode && beforeLength !== afterLength) {
      console.log(`💾 _processWriteBuffer: Removed ${beforeLength - afterLength} items from writeBuffer (${beforeLength} -> ${afterLength})`)
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
      await this.fileHandler.writeDataAsync(buffer)
    }
  }

  /**
   * Process individual object item
   */
  async _processObjectItem(obj) {
    // Process object item without loading entire file
    if (this.fileHandler) {
      // SPACE OPTIMIZATION: Remove term IDs before serialization
      const cleanRecord = this.removeTermIdsForSerialization(obj)
      const jsonString = this.serializer.serialize(cleanRecord).toString('utf8')
      // Use writeDataAsync for non-blocking I/O
      await this.fileHandler.writeDataAsync(Buffer.from(jsonString, 'utf8'))
    }
  }

  /**
   * Process multiple object items in a single batch write operation
   */
  async _processObjectItemsBatch(objects) {
    if (!this.fileHandler || objects.length === 0) return
    
    // OPTIMIZATION: Combine all objects into a single buffer for one write operation
    // SPACE OPTIMIZATION: Remove term IDs before serialization
    const jsonStrings = objects.map(obj => this.serializer.serialize(this.removeTermIdsForSerialization(obj)).toString('utf8'))
    const combinedString = jsonStrings.join('')
    
    // CRITICAL FIX: Validate that the combined string ends with newline
    const validatedString = combinedString.endsWith('\n') ? combinedString : combinedString + '\n'
    const buffer = Buffer.from(validatedString, 'utf8')
    
    // Single write operation for all objects
    await this.fileHandler.writeDataAsync(buffer)
  }

  /**
   * Wait for all I/O operations to complete
   */
  async _waitForIOCompletion() {
    // Wait for all file operations to complete
    if (this.fileHandler && this.fileHandler.fileMutex) {
      await this.fileHandler.fileMutex.runExclusive(async () => {
        // Ensure all pending file operations complete
        await new Promise(resolve => setTimeout(resolve, 50))
      })
    }
  }

  /**
   * CRITICAL FIX: Safe fallback method to load existing records when _streamExistingRecords fails
   * This prevents data loss by attempting alternative methods to preserve existing data
   */
  async _loadExistingRecordsFallback(deletedIdsSnapshot, writeBufferSnapshot) {
    const existingRecords = []
    
    try {
      if (this.opts.debugMode) {
        console.log(`💾 Save: Attempting fallback method to load existing records`)
      }
      
      // Method 1: Try to read the entire file and filter
      if (this.fileHandler.exists()) {
        const fs = await import('fs')
        const fileContent = await fs.promises.readFile(this.normalizedFile, 'utf8')
        const lines = fileContent.split('\n').filter(line => line.trim())
        
        for (let i = 0; i < lines.length && i < this.offsets.length; i++) {
          try {
            const record = this.serializer.deserialize(lines[i])
            if (record && !deletedIdsSnapshot.has(String(record.id))) {
              // Check if this record is not being updated in writeBuffer
              // CRITICAL FIX: Normalize IDs to strings for consistent comparison
              const normalizedRecordId = String(record.id)
              const updatedRecord = writeBufferSnapshot.find(r => r && r.id && String(r.id) === normalizedRecordId)
              if (!updatedRecord) {
                existingRecords.push(record)
              }
            }
          } catch (error) {
            // Skip invalid lines
            if (this.opts.debugMode) {
              console.log(`💾 Save: Skipping invalid line ${i} in fallback:`, error.message)
            }
          }
        }
      }
      
      if (this.opts.debugMode) {
        console.log(`💾 Save: Fallback method loaded ${existingRecords.length} existing records`)
      }
      
      return existingRecords
      
    } catch (error) {
      if (this.opts.debugMode) {
        console.log(`💾 Save: Fallback method failed:`, error.message)
      }
      // Return empty array as last resort - better than losing all data
      return []
    }
  }

  /**
   * Stream existing records without loading entire file into memory
   * Optimized with group ranging and reduced JSON parsing
   */
  async _streamExistingRecords(deletedIdsSnapshot, writeBufferSnapshot) {
    const existingRecords = []
    
    if (this.offsets.length === 0) {
      return existingRecords
    }
    
    // OPTIMIZATION: Pre-allocate array with known size (but don't set length to avoid undefined slots)
    // existingRecords.length = this.offsets.length
    
    // Create a map of updated records for quick lookup
    // CRITICAL FIX: Normalize IDs to strings for consistent comparison
    const updatedRecordsMap = new Map()
    writeBufferSnapshot.forEach((record, index) => {
      if (record && record.id !== undefined && record.id !== null) {
        // Normalize ID to string for consistent comparison
        const normalizedId = String(record.id)
        updatedRecordsMap.set(normalizedId, record)
        if (this.opts.debugMode) {
          console.log(`💾 Save: Added to updatedRecordsMap: ID=${normalizedId} (original: ${record.id}, type: ${typeof record.id}), index=${index}`)
        }
      } else if (this.opts.debugMode) {
        console.log(`⚠️ Save: Skipped record in writeBufferSnapshot[${index}] - missing or invalid ID:`, record ? { id: record.id, keys: Object.keys(record) } : 'null')
      }
    })
    
    if (this.opts.debugMode) {
      console.log(`💾 Save: updatedRecordsMap size: ${updatedRecordsMap.size}, keys:`, Array.from(updatedRecordsMap.keys()))
    }
    
    // OPTIMIZATION: Cache file stats to avoid repeated stat() calls
    let fileSize = 0
    if (this._cachedFileStats && this._cachedFileStats.timestamp > Date.now() - 1000) {
      // Use cached stats if less than 1 second old
      fileSize = this._cachedFileStats.size
    } else {
      // Get fresh stats and cache them
      const fileStats = await this.fileHandler.exists() ? await fs.promises.stat(this.normalizedFile) : null
      fileSize = fileStats ? fileStats.size : 0
      this._cachedFileStats = {
        size: fileSize,
        timestamp: Date.now()
      }
    }
    
    // CRITICAL FIX: Ensure indexOffset is consistent with actual file size
    if (this.indexOffset > fileSize) {
      if (this.opts.debugMode) {
        console.log(`💾 Save: Correcting indexOffset from ${this.indexOffset} to ${fileSize} (file size)`)
      }
      this.indexOffset = fileSize
    }
    
    // Build ranges array for group reading
    const ranges = []
    for (let i = 0; i < this.offsets.length; i++) {
      const offset = this.offsets[i]
      let nextOffset = i + 1 < this.offsets.length ? this.offsets[i + 1] : this.indexOffset
      
      if (this.opts.debugMode) {
        console.log(`💾 Save: Building range for record ${i}: offset=${offset}, nextOffset=${nextOffset}`)
      }
      
      // CRITICAL FIX: Handle case where indexOffset is 0 (new database without index)
      if (nextOffset === 0 && i + 1 >= this.offsets.length) {
        // For the last record when there's no index yet, we need to find the actual end
        // Read a bit more data to find the newline character that ends the record
        const searchEnd = Math.min(offset + 1000, fileSize) // Search up to 1000 bytes ahead
        if (searchEnd > offset) {
          try {
            const searchBuffer = await this.fileHandler.readRange(offset, searchEnd)
            const searchText = searchBuffer.toString('utf8')
            
            // Look for the end of the JSON record (closing brace followed by newline or end of data)
            let recordEnd = -1
            let braceCount = 0
            let inString = false
            let escapeNext = false
            
            for (let j = 0; j < searchText.length; j++) {
              const char = searchText[j]
              
              if (escapeNext) {
                escapeNext = false
                continue
              }
              
              if (char === '\\') {
                escapeNext = true
                continue
              }
              
              if (char === '"' && !escapeNext) {
                inString = !inString
                continue
              }
              
              if (!inString) {
                if (char === '{') {
                  braceCount++
                } else if (char === '}') {
                  braceCount--
                  if (braceCount === 0) {
                    // Found the end of the JSON object
                    recordEnd = j + 1
                    break
                  }
                }
              }
            }
            
            if (recordEnd !== -1) {
              nextOffset = offset + recordEnd
            } else {
              // If we can't find the end, read to end of file
              nextOffset = fileSize
            }
          } catch (error) {
            // Fallback to end of file if search fails
            nextOffset = fileSize
          }
        } else {
          nextOffset = fileSize
        }
      }
      
      // Validate offset ranges
      if (offset < 0) {
        if (this.opts.debugMode) {
          console.log(`💾 Save: Skipped negative offset ${offset}`)
        }
        continue
      }
      
      // CRITICAL FIX: Allow offsets that are at or beyond file size (for new records)
      if (fileSize > 0 && offset > fileSize) {
        if (this.opts.debugMode) {
          console.log(`💾 Save: Skipped offset ${offset} beyond file size ${fileSize}`)
        }
        continue
      }
      
      if (nextOffset <= offset) {
        if (this.opts.debugMode) {
          console.log(`💾 Save: Skipped invalid range [${offset}, ${nextOffset}]`)
        }
        continue
      }
      
      ranges.push({ start: offset, end: nextOffset, index: i })
    }
    
    if (ranges.length === 0) {
      return existingRecords
    }
    
    // Use group ranging for efficient reading
    const recordLines = await this.fileHandler.readRanges(ranges, async (lineString, range) => {
      if (!lineString || !lineString.trim()) {
        return null
      }
      
      const trimmedLine = lineString.trim()
      
      // DEBUG: Log what we're reading (temporarily enabled for debugging)
      if (this.opts.debugMode) {
        console.log(`💾 Save: Reading range ${range.start}-${range.end}, length: ${trimmedLine.length}`)
        console.log(`💾 Save: First 100 chars: ${trimmedLine.substring(0, 100)}`)
        if (trimmedLine.length > 100) {
          console.log(`💾 Save: Last 100 chars: ${trimmedLine.substring(trimmedLine.length - 100)}`)
        }
      }
      
      // OPTIMIZATION: Try to extract ID without full JSON parsing
      let recordId = null
      let needsFullParse = false
      
      // For array format, try to extract ID from array position
      if (trimmedLine.startsWith('[') && trimmedLine.endsWith(']')) {
        // Array format: try to extract ID from the array
        try {
          const arrayData = JSON.parse(trimmedLine)
          if (Array.isArray(arrayData) && arrayData.length > 0) {
            // CRITICAL FIX: Use schema to find ID position, not hardcoded position
            // The schema defines the order of fields in the array
            if (this.serializer && this.serializer.schemaManager && this.serializer.schemaManager.isInitialized) {
              const schema = this.serializer.schemaManager.getSchema()
              const idIndex = schema.indexOf('id')
              if (idIndex !== -1 && arrayData.length > idIndex) {
                // ID is at the position defined by schema
                recordId = arrayData[idIndex]
              } else if (arrayData.length > schema.length) {
                // ID might be appended after schema fields (for backward compatibility)
                recordId = arrayData[schema.length]
              } else {
                // Fallback: use first element
                recordId = arrayData[0]
              }
            } else {
              // No schema available, try common positions
            if (arrayData.length > 2) {
                // Try position 2 (common in older formats)
              recordId = arrayData[2]
            } else {
                // Fallback: use first element
              recordId = arrayData[0]
              }
            }
            if (recordId !== undefined && recordId !== null) {
              recordId = String(recordId)
              // Check if this record needs full parsing (updated or deleted)
              // CRITICAL FIX: Normalize ID to string for consistent comparison
              needsFullParse = updatedRecordsMap.has(recordId) || deletedIdsSnapshot.has(String(recordId))
            } else {
              needsFullParse = true
            }
          } else {
            needsFullParse = true
          }
        } catch (e) {
          needsFullParse = true
        }
      } else {
        // Object format: use regex for backward compatibility
        const idMatch = trimmedLine.match(/"id"\s*:\s*"([^"]+)"|"id"\s*:\s*(\d+)/)
        if (idMatch) {
          recordId = idMatch[1] || idMatch[2]
          // CRITICAL FIX: Normalize ID to string for consistent comparison
          needsFullParse = updatedRecordsMap.has(String(recordId)) || deletedIdsSnapshot.has(String(recordId))
        } else {
          needsFullParse = true
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
        }
      }
      
      // Full parsing needed for updated/deleted records
      try {
        // Use serializer to properly deserialize array format
        const record = this.serializer ? this.serializer.deserialize(trimmedLine) : JSON.parse(trimmedLine)
        
        // Use record directly (no need to restore term IDs)
        const recordWithIds = record
        
        // CRITICAL FIX: Normalize ID to string for consistent comparison
        const normalizedId = String(recordWithIds.id)
        if (this.opts.debugMode) {
          console.log(`💾 Save: Checking record ID=${normalizedId} (original: ${recordWithIds.id}, type: ${typeof recordWithIds.id}) in updatedRecordsMap`)
          console.log(`💾 Save: updatedRecordsMap.has(${normalizedId}): ${updatedRecordsMap.has(normalizedId)}`)
          if (!updatedRecordsMap.has(normalizedId)) {
            console.log(`💾 Save: Record ${normalizedId} NOT found in updatedRecordsMap. Available keys:`, Array.from(updatedRecordsMap.keys()))
          }
        }
        if (updatedRecordsMap.has(normalizedId)) {
          // Replace with updated version
          const updatedRecord = updatedRecordsMap.get(normalizedId)
          if (this.opts.debugMode) {
            console.log(`💾 Save: ✅ REPLACING record ${recordWithIds.id} with updated version`)
            console.log(`💾 Save: Old record:`, { id: recordWithIds.id, price: recordWithIds.price, app_id: recordWithIds.app_id, currency: recordWithIds.currency })
            console.log(`💾 Save: New record:`, { id: updatedRecord.id, price: updatedRecord.price, app_id: updatedRecord.app_id, currency: updatedRecord.currency })
          }
          return { 
            type: 'updated', 
            record: updatedRecord,
            id: recordWithIds.id,
            needsParse: false
          }
        } else if (!deletedIdsSnapshot.has(String(recordWithIds.id))) {
          // Keep existing record if not deleted
          if (this.opts.debugMode) {
            console.log(`💾 Save: Kept record ${recordWithIds.id} (${recordWithIds.name || 'Unnamed'}) - not in deletedIdsSnapshot`)
          }
          return { 
            type: 'kept', 
            record: recordWithIds,
            id: recordWithIds.id,
            needsParse: false
          }
        } else {
          // Skip deleted record
          if (this.opts.debugMode) {
            console.log(`💾 Save: Skipped record ${recordWithIds.id} (${recordWithIds.name || 'Unnamed'}) - deleted (found in deletedIdsSnapshot)`)
            console.log(`💾 Save: deletedIdsSnapshot contains:`, Array.from(deletedIdsSnapshot))
            console.log(`💾 Save: Record ID check: String(${recordWithIds.id}) = "${String(recordWithIds.id)}", has() = ${deletedIdsSnapshot.has(String(recordWithIds.id))}`)
          }
          return { 
            type: 'deleted', 
            id: recordWithIds.id,
            needsParse: false
          }
        }
      } catch (jsonError) {
        // RACE CONDITION FIX: Skip records that can't be parsed due to incomplete writes
        if (this.opts.debugMode) {
          console.log(`💾 Save: Skipped corrupted record at range ${range.start}-${range.end} - ${jsonError.message}`)
          // console.log(`💾 Save: Problematic line: ${trimmedLine}`)
        }
        return null
      }
    })
    
    // Process results and build final records array
    // OPTIMIZATION: Pre-allocate arrays with known size
    const unchangedLines = []
    const parsedRecords = []
    
    // OPTIMIZATION: Use for loop instead of Object.entries().sort() for better performance
    const sortedEntries = []
    for (const key in recordLines) {
      if (recordLines.hasOwnProperty(key)) {
        sortedEntries.push([key, recordLines[key]])
      }
    }
    
    // OPTIMIZATION: Sort by offset position using numeric comparison
    sortedEntries.sort(([keyA], [keyB]) => parseInt(keyA) - parseInt(keyB))
    
    // CRITICAL FIX: Maintain record order by processing in original offset order
    // and tracking which records are being kept vs deleted
    const keptRecords = []
    const deletedOffsets = new Set()
    
    for (const [rangeKey, result] of sortedEntries) {
      if (!result) continue
      
      const offset = parseInt(rangeKey)
      
      switch (result.type) {
        case 'unchanged':
          // CRITICAL FIX: Verify that unchanged records are not deleted
          // Extract ID from the line to check against deletedIdsSnapshot
          let unchangedRecordId = null
          try {
            if (result.line.startsWith('[') && result.line.endsWith(']')) {
              const arrayData = JSON.parse(result.line)
              if (Array.isArray(arrayData) && arrayData.length > 0) {
                // CRITICAL FIX: Use schema to find ID position, not hardcoded position
                if (this.serializer && this.serializer.schemaManager && this.serializer.schemaManager.isInitialized) {
                  const schema = this.serializer.schemaManager.getSchema()
                  const idIndex = schema.indexOf('id')
                  if (idIndex !== -1 && arrayData.length > idIndex) {
                    unchangedRecordId = String(arrayData[idIndex])
                  } else if (arrayData.length > schema.length) {
                    unchangedRecordId = String(arrayData[schema.length])
                  } else {
                    unchangedRecordId = String(arrayData[0])
                  }
                } else {
                  // No schema, try common positions
                  if (arrayData.length > 2) {
                    unchangedRecordId = String(arrayData[2])
                  } else {
                    unchangedRecordId = String(arrayData[0])
                  }
                }
              }
            } else {
              const obj = JSON.parse(result.line)
              unchangedRecordId = obj.id ? String(obj.id) : null
            }
          } catch (e) {
            // If we can't parse, skip this record to be safe
            if (this.opts.debugMode) {
              console.log(`💾 Save: Could not parse unchanged record to check deletion: ${e.message}`)
            }
            continue
          }
          
          // Skip if this record is deleted
          if (unchangedRecordId && deletedIdsSnapshot.has(unchangedRecordId)) {
            if (this.opts.debugMode) {
              console.log(`💾 Save: Skipping unchanged record ${unchangedRecordId} - deleted`)
            }
            deletedOffsets.add(offset)
            break
          }
          
          // Collect unchanged lines for batch processing
          unchangedLines.push(result.line)
          keptRecords.push({ offset, type: 'unchanged', line: result.line })
          break
          
        case 'updated':
        case 'kept':
          parsedRecords.push(result.record)
          keptRecords.push({ offset, type: 'parsed', record: result.record })
          break
          
        case 'deleted':
          // Track deleted records by their offset
          deletedOffsets.add(offset)
          break
      }
    }
    
    // CRITICAL FIX: Build final records array in the correct order
    // and update offsets array to match the new record order
    const newOffsets = []
    let currentOffset = 0
    
    // OPTIMIZATION: Batch parse unchanged records for better performance
    if (unchangedLines.length > 0) {
      const batchParsedRecords = []
      for (let i = 0; i < unchangedLines.length; i++) {
        try {
          // Use serializer to properly deserialize array format
          const record = this.serializer ? this.serializer.deserialize(unchangedLines[i]) : JSON.parse(unchangedLines[i])
          batchParsedRecords.push(record)
        } catch (jsonError) {
          if (this.opts.debugMode) {
            console.log(`💾 Save: Failed to parse unchanged record: ${jsonError.message}`)
          }
          batchParsedRecords.push(null) // Mark as failed
        }
      }
      
      // Process kept records in their original offset order
      let batchIndex = 0
      for (const keptRecord of keptRecords) {
        let record = null
        
        if (keptRecord.type === 'unchanged') {
          record = batchParsedRecords[batchIndex++]
          if (!record) continue // Skip failed parses
        } else if (keptRecord.type === 'parsed') {
          record = keptRecord.record
        }
        
        if (record && typeof record === 'object') {
          existingRecords.push(record)
          newOffsets.push(currentOffset)
          // OPTIMIZATION: Use cached string length if available
            const recordSize = keptRecord.type === 'unchanged' 
              ? keptRecord.line.length + 1  // Use actual line length
              : JSON.stringify(this.removeTermIdsForSerialization(record)).length + 1
          currentOffset += recordSize
        }
      }
    } else {
      // Process kept records in their original offset order (no unchanged records)
      for (const keptRecord of keptRecords) {
        if (keptRecord.type === 'parsed') {
          const record = keptRecord.record
          if (record && typeof record === 'object' && record.id) {
            existingRecords.push(record)
            newOffsets.push(currentOffset)
            const recordSize = JSON.stringify(this.removeTermIdsForSerialization(record)).length + 1
            currentOffset += recordSize
          }
        }
      }
    }
    
    // Update the offsets array to reflect the new record order
    this.offsets = newOffsets
    
    return existingRecords
  }

  /**
   * Flush write buffer
   */
  async flush() {
    return this.operationQueue.enqueue(async () => {
      this.isInsideOperationQueue = true
      try {
        // CRITICAL FIX: Actually flush the writeBuffer by saving data
        if (this.writeBuffer.length > 0 || this.shouldSave) {
          await this._doSave()
        }
        return Promise.resolve()
      } finally {
        this.isInsideOperationQueue = false
      }
    })
  }

  /**
   * Flush insertion buffer (backward compatibility)
   */
  async flushInsertionBuffer() {
    // Flush insertion buffer implementation - save any pending data
    // Use the same robust flush logic as flush()
    return this.flush()
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
    }
  }

  _hasActualIndexData() {
    if (!this.indexManager) return false
    
    const data = this.indexManager.index.data
    for (const field in data) {
      const fieldData = data[field]
      for (const value in fieldData) {
        const hybridData = fieldData[value]
        if (hybridData.set && hybridData.set.size > 0) {
          return true
        }
      }
    }
    return false
  }

  /**
   * Locate a record by line number and return its byte range
   * @param {number} n - Line number
   * @returns {Array} - [start, end] byte range or undefined
   */
  locate(n) {
    if (this.offsets[n] === undefined) {
      return undefined // Record doesn't exist
    }
    
    // CRITICAL FIX: Calculate end offset correctly to prevent cross-line reading
    let end
    if (n + 1 < this.offsets.length) {
      // Use next record's start minus 1 (to exclude newline) as this record's end
      end = this.offsets[n + 1] - 1
    } else {
      // For the last record, use indexOffset (includes the record but not newline)
      end = this.indexOffset
    }
    
    return [this.offsets[n], end]
  }

  /**
   * Get ranges for streaming based on line numbers
   * @param {Array|Set} map - Line numbers to get ranges for
   * @returns {Array} - Array of range objects {start, end, index}
   */
  getRanges(map) {
    return (map || Array.from(this.offsets.keys())).map(n => {
      const ret = this.locate(n)
      if (ret !== undefined) return { start: ret[0], end: ret[1], index: n }
    }).filter(n => n !== undefined)
  }

  /**
   * Get the base line number for writeBuffer entries (number of persisted records)
   * @private
   */
  _getWriteBufferBaseLineNumber() {
    return Array.isArray(this.offsets) ? this.offsets.length : 0
  }

  /**
   * Convert a writeBuffer index into an absolute line number
   * @param {number} writeBufferIndex - Index inside writeBuffer (0-based)
   * @returns {number} Absolute line number (0-based)
   * @private
   */
  _getAbsoluteLineNumber(writeBufferIndex) {
    if (typeof writeBufferIndex !== 'number' || writeBufferIndex < 0) {
      throw new Error('Invalid writeBuffer index')
    }
    return this._getWriteBufferBaseLineNumber() + writeBufferIndex
  }


  /**
   * Attempts to recover a corrupted line by cleaning invalid characters and fixing common JSON issues
   * @param {string} line - The corrupted line to recover
   * @returns {string|null} - The recovered line or null if recovery is not possible
   */
  _tryRecoverLine(line) {
    if (!line || typeof line !== 'string') {
      return null
    }

    try {
      // Try parsing as-is first
      JSON.parse(line)
      return line // Line is already valid
    } catch (e) {
      // Line is corrupted, attempt recovery
    }

    let recovered = line.trim()

    // Remove invalid control characters (except \n, \r, \t)
    recovered = recovered.replace(/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/g, '')

    // Try to close unclosed strings
    // Count quotes and ensure they're balanced
    const quoteCount = (recovered.match(/"/g) || []).length
    if (quoteCount % 2 !== 0) {
      // Odd number of quotes - try to close the string
      const lastQuoteIndex = recovered.lastIndexOf('"')
      if (lastQuoteIndex > 0) {
        // Check if we're inside a string (not escaped)
        const beforeLastQuote = recovered.substring(0, lastQuoteIndex)
        const escapedQuotes = (beforeLastQuote.match(/\\"/g) || []).length
        const unescapedQuotes = (beforeLastQuote.match(/"/g) || []).length - escapedQuotes
        
        if (unescapedQuotes % 2 !== 0) {
          // We're inside an unclosed string - try to close it
          recovered = recovered + '"'
        }
      }
    }

    // Try to close unclosed arrays/objects
    const openBraces = (recovered.match(/\{/g) || []).length
    const closeBraces = (recovered.match(/\}/g) || []).length
    const openBrackets = (recovered.match(/\[/g) || []).length
    const closeBrackets = (recovered.match(/\]/g) || []).length

    // Remove trailing commas before closing braces/brackets
    recovered = recovered.replace(/,\s*([}\]])/g, '$1')

    // Try to close arrays
    if (openBrackets > closeBrackets) {
      recovered = recovered + ']'.repeat(openBrackets - closeBrackets)
    }

    // Try to close objects
    if (openBraces > closeBraces) {
      recovered = recovered + '}'.repeat(openBraces - closeBraces)
    }

    // Final validation - try to parse
    try {
      JSON.parse(recovered)
      return recovered
    } catch (e) {
      // Recovery failed
      return null
    }
  }

  async *_streamingRecoveryGenerator(criteria, options, alreadyYielded = 0, map = null, remainingSkipValue = 0) {
    if (this._offsetRecoveryInProgress) {
      return
    }

    if (!this.fileHandler || !this.fileHandler.file) {
      return
    }

    this._offsetRecoveryInProgress = true

    const fsModule = this._fsModule || (this._fsModule = await import('fs'))
    let fd

    try {
      fd = await fsModule.promises.open(this.fileHandler.file, 'r')
    } catch (error) {
      this._offsetRecoveryInProgress = false
      if (this.opts.debugMode) {
        console.warn(`⚠️ Offset recovery skipped: ${error.message}`)
      }
      return
    }

    const chunkSize = this.opts.offsetRecoveryChunkSize || 64 * 1024
    let buffer = Buffer.alloc(0)
    let readOffset = 0
    const originalOffsets = Array.isArray(this.offsets) ? [...this.offsets] : []
    const newOffsets = []
    let offsetAdjusted = false
    let limitReached = false
    let lineIndex = 0
    let lastLineEnd = 0
    let producedTotal = alreadyYielded || 0
    let remainingSkip = remainingSkipValue || 0
    let remainingAlreadyYielded = alreadyYielded || 0
    const limit = typeof options?.limit === 'number' ? options.limit : null
    const includeOffsets = options?.includeOffsets === true
    const includeLinePosition = this.opts.includeLinePosition
    const mapSet = map instanceof Set ? new Set(map) : (Array.isArray(map) ? new Set(map) : null)
    const criteriaIsObject = criteria && typeof criteria === 'object' && !Array.isArray(criteria) && !(criteria instanceof Set)
    const hasCriteria = criteriaIsObject && Object.keys(criteria).length > 0

    const decodeLineBuffer = (lineBuffer) => {
      let trimmed = lineBuffer
      if (trimmed.length > 0 && trimmed[trimmed.length - 1] === 0x0A) {
        trimmed = trimmed.subarray(0, trimmed.length - 1)
      }
      if (trimmed.length > 0 && trimmed[trimmed.length - 1] === 0x0D) {
        trimmed = trimmed.subarray(0, trimmed.length - 1)
      }
      return trimmed
    }

    const processLine = async (lineBuffer, lineStart) => {
      const lineLength = lineBuffer.length
      newOffsets[lineIndex] = lineStart
      const expected = originalOffsets[lineIndex]
      if (expected !== undefined && expected !== lineStart) {
        offsetAdjusted = true
        if (this.opts.debugMode) {
          console.warn(`⚠️ Offset mismatch detected at line ${lineIndex}: expected ${expected}, actual ${lineStart}`)
        }
      } else if (expected === undefined) {
        offsetAdjusted = true
      }

      lastLineEnd = Math.max(lastLineEnd, lineStart + lineLength)

      let entryWithTerms = null
      let shouldYield = false

      const decodedBuffer = decodeLineBuffer(lineBuffer)
      if (decodedBuffer.length > 0) {
        let lineString
        try {
          lineString = decodedBuffer.toString('utf8')
        } catch (error) {
          lineString = decodedBuffer.toString('utf8', { replacement: '?' })
        }

        try {
          const record = await this.serializer.deserialize(lineString)
          if (record && typeof record === 'object') {
            entryWithTerms = this.restoreTermIdsAfterDeserialization(record)
            if (includeLinePosition) {
              entryWithTerms._ = lineIndex
            }

            if (mapSet) {
              shouldYield = mapSet.has(lineIndex)
              if (shouldYield) {
                mapSet.delete(lineIndex)
              }
            } else if (hasCriteria) {
              shouldYield = this.queryManager.matchesCriteria(entryWithTerms, criteria, options)
            } else {
              shouldYield = true
            }
          }
        } catch (error) {
          if (this.opts.debugMode) {
            console.warn(`⚠️ Offset recovery failed to deserialize line ${lineIndex} at ${lineStart}: ${error.message}`)
          }
        }
      }

      let yieldedEntry = null

      if (shouldYield && entryWithTerms) {
        if (remainingSkip > 0) {
          remainingSkip--
        } else if (remainingAlreadyYielded > 0) {
          remainingAlreadyYielded--
        } else if (!limit || producedTotal < limit) {
          producedTotal++
          yieldedEntry = includeOffsets
            ? { entry: entryWithTerms, start: lineStart, _: lineIndex }
            : entryWithTerms
        } else {
          limitReached = true
        }
      }

      lineIndex++

      if (yieldedEntry) {
        return yieldedEntry
      }
      return null
    }

    let recoveryFailed = false

    try {
      while (true) {
        const tempBuffer = Buffer.allocUnsafe(chunkSize)
        const { bytesRead } = await fd.read(tempBuffer, 0, chunkSize, readOffset)

        if (bytesRead === 0) {
          if (buffer.length > 0) {
            const lineStart = readOffset - buffer.length
            const yieldedEntry = await processLine(buffer, lineStart)
            if (yieldedEntry) {
              yield yieldedEntry
            }
          }
          break
        }

        readOffset += bytesRead
        let chunk = buffer.length > 0
          ? Buffer.concat([buffer, tempBuffer.subarray(0, bytesRead)])
          : tempBuffer.subarray(0, bytesRead)

        let processedUpTo = 0
        const chunkBaseOffset = readOffset - chunk.length

        while (true) {
          const newlineIndex = chunk.indexOf(0x0A, processedUpTo)
          if (newlineIndex === -1) {
            break
          }

          const lineBuffer = chunk.subarray(processedUpTo, newlineIndex + 1)
          const lineStart = chunkBaseOffset + processedUpTo
          const yieldedEntry = await processLine(lineBuffer, lineStart)
          processedUpTo = newlineIndex + 1

          if (yieldedEntry) {
            yield yieldedEntry
          }
        }

        buffer = chunk.subarray(processedUpTo)
      }
    } catch (error) {
      recoveryFailed = true
      if (this.opts.debugMode) {
        console.warn(`⚠️ Offset recovery aborted: ${error.message}`)
      }
    } finally {
      await fd.close().catch(() => {})
      this._offsetRecoveryInProgress = false

      if (recoveryFailed) {
        return
      }

      this.offsets = newOffsets
      if (lineIndex < this.offsets.length) {
        this.offsets.length = lineIndex
      }

      if (originalOffsets.length !== newOffsets.length) {
        offsetAdjusted = true
      }

      this.indexOffset = lastLineEnd

      if (offsetAdjusted) {
        this.shouldSave = true
        try {
          await this._saveIndexDataToFile()
        } catch (error) {
          if (this.opts.debugMode) {
            console.warn(`⚠️ Failed to persist recovered offsets: ${error.message}`)
          }
        }
      }
    }
  }

  /**
   * Walk through records using streaming (real implementation)
   */
  async *walk(criteria, options = {}) {
    // CRITICAL FIX: Validate state before walk operation to prevent crashes
    this.validateState()
    
    if (!this.initialized) await this.init()
    
    // If no data at all, return empty
    if (this.indexOffset === 0 && this.writeBuffer.length === 0) return
    
    // CRITICAL FIX: Wait for any ongoing save operations to complete
    // This prevents reading partially written data
    if (this.isSaving) {
      if (this.opts.debugMode) {
        console.log('🔍 walk(): waiting for save operation to complete')
      }
      while (this.isSaving) {
        await new Promise(resolve => setTimeout(resolve, 10))
      }
    }
    
    let count = 0
    let remainingSkip = options.skip || 0
    
    let map
    if (!Array.isArray(criteria)) {
      if (criteria instanceof Set) {
        map = [...criteria]
      } else if (criteria && typeof criteria === 'object' && Object.keys(criteria).length > 0) {
        // Only use indexManager.query if criteria has actual filters
        map = [...this.indexManager.query(criteria, options)]
      } else {
        // For empty criteria {} or null/undefined, get all records
        const totalRecords = this.offsets && this.offsets.length > 0
          ? this.offsets.length
          : this.writeBuffer.length
        map = [...Array(totalRecords).keys()]
      }
    } else {
      map = criteria
    }
    
    // Use writeBuffer when available (unsaved data)
    if (this.writeBuffer.length > 0) {
      let count = 0
      
      // If map is empty (no index results) but we have criteria, filter writeBuffer directly
      if (map.length === 0 && criteria && typeof criteria === 'object' && Object.keys(criteria).length > 0) {
        for (let i = 0; i < this.writeBuffer.length; i++) {
          if (options.limit && count >= options.limit) {
            break
          }
          const entry = this.writeBuffer[i]
          if (entry && this.queryManager.matchesCriteria(entry, criteria, options)) {
            if (remainingSkip > 0) {
              remainingSkip--
              continue
            }
            count++
            if (options.includeOffsets) {
              yield { entry, start: 0, _: i }
            } else {
              if (this.opts.includeLinePosition) {
                entry._ = i
              }
              yield entry
            }
          }
        }
      } else {
        // Use map-based iteration (for all records or indexed results)
        for (const lineNumber of map) {
          if (options.limit && count >= options.limit) {
            break
          }
          if (lineNumber < this.writeBuffer.length) {
            const entry = this.writeBuffer[lineNumber]
            if (entry) {
              if (remainingSkip > 0) {
                remainingSkip--
                continue
              }
              count++
              if (options.includeOffsets) {
                yield { entry, start: 0, _: lineNumber }
              } else {
                if (this.opts.includeLinePosition) {
                  entry._ = lineNumber
                }
                yield entry
              }
            }
          }
        }
      }
      
      return
    }
    
    // If writeBuffer is empty but we have saved data, we need to load it from file
    if (this.writeBuffer.length === 0 && this.indexOffset > 0) {
      // Load data from file for querying
      try {
        let data
        let lines
        
        // Smart threshold: decide between partial reads vs full read
        const resultPercentage = map ? (map.length / this.indexOffset) * 100 : 100
        const threshold = this.opts.partialReadThreshold || 60 // Default 60% threshold
        
        // Use partial reads when:
        // 1. We have specific line numbers from index
        // 2. Results are below threshold percentage
        // 3. Database is large enough to benefit from partial reads
        const shouldUsePartialReads = map && map.length > 0 && 
          resultPercentage < threshold && 
          this.indexOffset > 100 // Only for databases with >100 records
        
        if (shouldUsePartialReads) {
          if (this.opts.debugMode) {
            console.log(`🔍 Using PARTIAL READS: ${map.length}/${this.indexOffset} records (${resultPercentage.toFixed(1)}% < ${threshold}% threshold)`)
          }
          // OPTIMIZATION: Use ranges instead of reading entire file
          const ranges = this.getRanges(map)
          const groupedRanges = await this.fileHandler.groupedRanges(ranges)
          
          const fs = await import('fs')
          const fd = await fs.promises.open(this.fileHandler.file, 'r')
          
          try {
            for (const groupedRange of groupedRanges) {
              for await (const row of this.fileHandler.readGroupedRange(groupedRange, fd)) {
                if (options.limit && count >= options.limit) {
                  break
                }
                
                try {
                  // CRITICAL FIX: Use serializer.deserialize instead of JSON.parse to handle array format
                  const record = this.serializer.deserialize(row.line)
                  // SPACE OPTIMIZATION: Restore term IDs to terms for user
                  const recordWithTerms = this.restoreTermIdsAfterDeserialization(record)
                  
                  if (remainingSkip > 0) {
                    remainingSkip--
                    continue
                  }

                  count++
                  if (options.includeOffsets) {
                    yield { entry: recordWithTerms, start: row.start, _: row._ || 0 }
                  } else {
                    if (this.opts.includeLinePosition) {
                      recordWithTerms._ = row._ || 0
                    }
                    yield recordWithTerms
                  }
                } catch (error) {
                  // CRITICAL FIX: Log deserialization errors instead of silently ignoring them
                  // This helps identify data corruption issues
                  if (this.opts.debugMode) {
                    console.warn(`⚠️ walk(): Failed to deserialize record at offset ${row.start}: ${error.message}`)
                    console.warn(`⚠️ walk(): Problematic line (first 200 chars): ${row.line.substring(0, 200)}`)
                  }
                  
                  // CRITICAL FIX: Attempt to recover corrupted line before giving up
                  const recoveredLine = this._tryRecoverLine(row.line)
                  if (recoveredLine) {
                    try {
                      const record = this.serializer.deserialize(recoveredLine)
                      if (record !== null) {
                        this.integrityCorrections.jsonRecovery++
                        console.log(`🔧 Recovered corrupted JSON line (${this.integrityCorrections.jsonRecovery} recoveries)`)

                        if (this.integrityCorrections.jsonRecovery > 20) {
                          console.warn(`⚠️ Frequent JSON recovery detected (${this.integrityCorrections.jsonRecovery} times) - may indicate data corruption`)
                        }

                        const recordWithTerms = this.restoreTermIdsAfterDeserialization(record)

                        if (remainingSkip > 0) {
                          remainingSkip--
                          continue
                        }

                        count++
                        if (options.includeOffsets) {
                          yield { entry: recordWithTerms, start: row.start, _: row._ || 0 }
                        } else {
                          if (this.opts.includeLinePosition) {
                            recordWithTerms._ = row._ || 0
                          }
                          yield recordWithTerms
                        }
                        continue // Successfully recovered and yielded
                      }
                    } catch (recoveryError) {
                      // Recovery attempt failed, continue with normal error handling
                      if (this.opts.debugMode) {
                        console.warn(`⚠️ walk(): Line recovery failed: ${recoveryError.message}`)
                      }
                    }
                  }
                  
                  if (!this._offsetRecoveryInProgress) {
                    for await (const recoveredEntry of this._streamingRecoveryGenerator(criteria, options, count, map, remainingSkip)) {
                      yield recoveredEntry
                      count++
                    }
                    return
                  }
                  // Skip invalid lines but continue processing
                  // This prevents one corrupted record from stopping the entire walk operation
                }
              }
              if (options.limit && count >= options.limit) {
                break
              }
            }
          } finally {
            await fd.close()
          }
          return // Exit early since we processed partial reads
        } else {
          if (this.opts.debugMode) {
            console.log(`🔍 Using STREAMING READ: ${map?.length || 0}/${this.indexOffset} records (${resultPercentage.toFixed(1)}% >= ${threshold}% threshold or small DB)`)
          }
          // Use streaming instead of loading all data in memory
          // This prevents memory issues with large databases
          const streamingResults = await this.fileHandler.readWithStreaming(
            criteria, 
            { limit: options.limit, skip: options.skip }, 
            matchesCriteria,
            this.serializer
          )
          
          // Process streaming results directly without loading all lines
          for (const record of streamingResults) {
            if (options.limit && count >= options.limit) {
              break
            }

            if (remainingSkip > 0) {
              remainingSkip--
              continue
            }

            count++
            
            // SPACE OPTIMIZATION: Restore term IDs to terms for user
            const recordWithTerms = this.restoreTermIdsAfterDeserialization(record)
            
            if (options.includeOffsets) {
              yield { entry: recordWithTerms, start: 0, _: 0 }
            } else {
              if (this.opts.includeLinePosition) {
                recordWithTerms._ = 0
              }
              yield recordWithTerms
            }
          }
          return // Exit early since we processed streaming results
        }
      } catch (error) {
        // If file reading fails, continue to file-based streaming
      }
    }
    
    // Use file-based streaming for saved data
    const ranges = this.getRanges(map)
    const groupedRanges = await this.fileHandler.groupedRanges(ranges)
    const fd = await fs.promises.open(this.fileHandler.file, 'r')
    
    try {
      let count = 0
      for (const groupedRange of groupedRanges) {
        if (options.limit && count >= options.limit) {
          break
        }
        for await (const row of this.fileHandler.readGroupedRange(groupedRange, fd)) {
          if (options.limit && count >= options.limit) {
            break
          }
          
          try {
            const entry = await this.serializer.deserialize(row.line, { compress: this.opts.compress, v8: this.opts.v8 })
            if (entry === null) continue
            
            // SPACE OPTIMIZATION: Restore term IDs to terms for user
            const entryWithTerms = this.restoreTermIdsAfterDeserialization(entry)
          
            if (remainingSkip > 0) {
              remainingSkip--
              continue
            }

            count++
            if (options.includeOffsets) {
              yield { entry: entryWithTerms, start: row.start, _: row._ || this.offsets.findIndex(n => n === row.start) }
            } else {
              if (this.opts.includeLinePosition) {
                entryWithTerms._ = row._ || this.offsets.findIndex(n => n === row.start)
              }
              yield entryWithTerms
            }
          } catch (error) {
            // CRITICAL FIX: Log deserialization errors instead of silently ignoring them
            // This helps identify data corruption issues
            if (this.opts.debugMode) {
              console.warn(`⚠️ walk(): Failed to deserialize record at offset ${row.start}: ${error.message}`)
              console.warn(`⚠️ walk(): Problematic line (first 200 chars): ${row.line.substring(0, 200)}`)
            }
            
            // CRITICAL FIX: Attempt to recover corrupted line before giving up
            const recoveredLine = this._tryRecoverLine(row.line)
            if (recoveredLine) {
              try {
                const entry = await this.serializer.deserialize(recoveredLine, { compress: this.opts.compress, v8: this.opts.v8 })
                if (entry !== null) {
                  this.integrityCorrections.jsonRecovery++
                  console.log(`🔧 Recovered corrupted JSON line (${this.integrityCorrections.jsonRecovery} recoveries)`)

                  if (this.integrityCorrections.jsonRecovery > 20) {
                    console.warn(`⚠️ Frequent JSON recovery detected (${this.integrityCorrections.jsonRecovery} times) - may indicate data corruption`)
                  }

                  const entryWithTerms = this.restoreTermIdsAfterDeserialization(entry)

                  if (remainingSkip > 0) {
                    remainingSkip--
                    continue
                  }

                  count++
                  if (options.includeOffsets) {
                    yield { entry: entryWithTerms, start: row.start, _: row._ || this.offsets.findIndex(n => n === row.start) }
                  } else {
                    if (this.opts.includeLinePosition) {
                      entryWithTerms._ = row._ || this.offsets.findIndex(n => n === row.start)
                    }
                    yield entryWithTerms
                  }
                  continue // Successfully recovered and yielded
                }
              } catch (recoveryError) {
                // Recovery attempt failed, continue with normal error handling
                if (this.opts.debugMode) {
                  console.warn(`⚠️ walk(): Line recovery failed: ${recoveryError.message}`)
                }
              }
            }
            
            if (!this._offsetRecoveryInProgress) {
              for await (const recoveredEntry of this._streamingRecoveryGenerator(criteria, options, count, map, remainingSkip)) {
                yield recoveredEntry
                count++
              }
              return
            }
            // Skip invalid lines but continue processing
            // This prevents one corrupted record from stopping the entire walk operation
          }
        }
      }
    } finally {
      await fd.close()
    }
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
  async *iterate(criteria, options = {}) {
    // CRITICAL FIX: Validate state before iterate operation
    this.validateState()
    
    if (!this.initialized) await this.init()
    
    // Set default options
    const opts = {
      chunkSize: 1000,
      strategy: 'streaming', // Always use walk() method for optimal performance
      autoSave: false,
      detectChanges: true,
      ...options
    }
    
    // If no data, return empty
    if (this.indexOffset === 0 && this.writeBuffer.length === 0) return
    
    const startTime = Date.now()
    let processedCount = 0
    let modifiedCount = 0
    let deletedCount = 0
    
    // Buffers for batch processing
    const updateBuffer = []
    const deleteBuffer = new Set()
    const originalRecords = new Map() // Track original records for change detection
    
    try {
      // Always use walk() now that the bug is fixed - it works for both small and large datasets
      for await (const entry of this.walk(criteria, options)) {
        processedCount++
        
        // Store original record for change detection BEFORE yielding
        let originalRecord = null
        if (opts.detectChanges) {
          originalRecord = this._createShallowCopy(entry)
          originalRecords.set(entry.id, originalRecord)
        }
        
        // Create wrapper based on performance preference
        const entryWrapper = opts.highPerformance 
          ? this._createHighPerformanceWrapper(entry, originalRecord)
          : this._createEntryProxy(entry, originalRecord)
        
        // Yield the wrapper for user modification
        yield entryWrapper
        
        // Check if entry was modified or deleted AFTER yielding
        if (entryWrapper.isMarkedForDeletion) {
          // Entry was marked for deletion
          if (originalRecord) {
            deleteBuffer.add(originalRecord.id)
            deletedCount++
          }
        } else if (opts.detectChanges && originalRecord) {
          // Check if entry was modified by comparing with original (optimized comparison)
          if (this._hasRecordChanged(entry, originalRecord)) {
            updateBuffer.push(entry)
            modifiedCount++
          }
        } else if (entryWrapper.isModified) {
          // Manual change detection
          updateBuffer.push(entry)
          modifiedCount++
        }
        
        // Process batch when chunk size is reached
        if (updateBuffer.length >= opts.chunkSize || deleteBuffer.size >= opts.chunkSize) {
          await this._processIterateBatch(updateBuffer, deleteBuffer, opts)
          
          // Clear buffers
          updateBuffer.length = 0
          deleteBuffer.clear()
          originalRecords.clear()
          
          // Progress callback
          if (opts.progressCallback) {
            opts.progressCallback({
              processed: processedCount,
              modified: modifiedCount,
              deleted: deletedCount,
              elapsed: Date.now() - startTime
            })
          }
        }
      }
      
      // Process remaining records in buffers
      if (updateBuffer.length > 0 || deleteBuffer.size > 0) {
        await this._processIterateBatch(updateBuffer, deleteBuffer, opts)
      }
      
      // Final progress callback (always called)
      if (opts.progressCallback) {
        opts.progressCallback({
          processed: processedCount,
          modified: modifiedCount,
          deleted: deletedCount,
          elapsed: Date.now() - startTime,
          completed: true
        })
      }
      
      if (this.opts.debugMode) {
        console.log(`🔄 ITERATE COMPLETED: ${processedCount} processed, ${modifiedCount} modified, ${deletedCount} deleted in ${Date.now() - startTime}ms`)
      }
      
    } catch (error) {
      console.error('Iterate operation failed:', error)
      throw error
    }
  }

  /**
   * Process a batch of updates and deletes from iterate operation
   * @private
   */
  async _processIterateBatch(updateBuffer, deleteBuffer, options) {
    if (updateBuffer.length === 0 && deleteBuffer.size === 0) return
    
    const startTime = Date.now()
    
    try {
      // Process updates
      if (updateBuffer.length > 0) {
        for (const record of updateBuffer) {
          // Remove the _modified flag if it exists
          delete record._modified
          
          // Update record in writeBuffer or add to writeBuffer
          const index = this.writeBuffer.findIndex(r => r.id === record.id)
          let targetIndex
          if (index !== -1) {
            // Record is already in writeBuffer, update it
            this.writeBuffer[index] = record
            targetIndex = index
          } else {
            // Record is in file, add updated version to writeBuffer
            this.writeBuffer.push(record)
            targetIndex = this.writeBuffer.length - 1
          }
          
          // Update index
          const absoluteLineNumber = this._getAbsoluteLineNumber(targetIndex)
          await this.indexManager.update(record, record, absoluteLineNumber)
        }
        
        if (this.opts.debugMode) {
          console.log(`🔄 ITERATE: Updated ${updateBuffer.length} records in ${Date.now() - startTime}ms`)
        }
      }
      
      // Process deletes
      if (deleteBuffer.size > 0) {
        for (const recordId of deleteBuffer) {
          // Find the record to get its data for term mapping removal
          const record = this.writeBuffer.find(r => r.id === recordId) || 
                        await this.findOne({ id: recordId })
          
          if (record) {
            // Remove term mapping
            this.removeTermMapping(record)
            
            // Remove from index
            await this.indexManager.remove(record)
            
            // Remove from writeBuffer or mark as deleted
            const index = this.writeBuffer.findIndex(r => r.id === recordId)
            if (index !== -1) {
              this.writeBuffer.splice(index, 1)
            } else {
              // Mark as deleted if not in writeBuffer
              this.deletedIds.add(recordId)
            }
          }
        }
        
        if (this.opts.debugMode) {
          console.log(`🗑️ ITERATE: Deleted ${deleteBuffer.size} records in ${Date.now() - startTime}ms`)
        }
      }
      
      // Auto-save if enabled
      if (options.autoSave) {
        await this.save()
      }
      
      this.shouldSave = true
      this.performanceStats.operations++
      
    } catch (error) {
      console.error('Batch processing failed:', error)
      throw error
    }
  }

  /**
   * Close the database
   */
  async close() {
    if (this.destroyed || this.closed) return
    
    try {
      if (this.opts.debugMode) {
        console.log(`💾 close(): Saving and closing database (reopenable)`)
      }
      
      // 1. Save all pending data and index data to files
      if (this.writeBuffer.length > 0 || this.shouldSave) {
        await this.save()
        // Ensure writeBuffer is cleared after save
        if (this.writeBuffer.length > 0) {
          this.integrityCorrections.writeBufferForced++
          console.log(`🔧 Forced WriteBuffer clear after save (${this.writeBuffer.length} items remaining)`)

          if (this.integrityCorrections.writeBufferForced > 3) {
            console.warn(`⚠️ Frequent WriteBuffer clear issues detected (${this.integrityCorrections.writeBufferForced} times)`)
          }

          this.writeBuffer = []
          this.writeBufferOffsets = []
          this.writeBufferSizes = []
        }
      } else {
        // Only save index data if it actually has content
        // Don't overwrite a valid index with an empty one
        if (this.indexManager && this.indexManager.indexedFields && this.indexManager.indexedFields.length > 0) {
          let hasIndexData = false
          for (const field of this.indexManager.indexedFields) {
            if (this.indexManager.hasUsableIndexData(field)) {
              hasIndexData = true
              break
            }
          }
          // Only save if we have actual index data OR if offsets are populated
          // (offsets being populated means we've processed data)
          if (hasIndexData || (this.offsets && this.offsets.length > 0)) {
            await this._saveIndexDataToFile()
          } else if (this.opts.debugMode) {
            console.log('⚠️ close(): Skipping index save - index is empty and no offsets')
          }
        }
      }
      
      // 2. Mark as closed (but not destroyed) to allow reopening
      this.closed = true
      this.initialized = false
      
      // 3. Clear any remaining state for clean reopening
      this.writeBuffer = []
      this.writeBufferOffsets = []
      this.writeBufferSizes = []
      this.shouldSave = false
      this.isSaving = false
      this.lastSaveTime = null
      
      if (this.opts.debugMode) {
        console.log(`💾 Database closed (can be reopened with init())`)
      }
      
    } catch (error) {
      console.error('Failed to close database:', error)
      // Mark as closed even if save failed
      this.closed = true
      this.initialized = false
      throw error
    }
  }

  /**
   * Save index data to .idx.jdb file
   * @private
   */
  async _saveIndexDataToFile() {
    if (this.indexManager) {
      try {
        const idxPath = this.normalizedFile.replace('.jdb', '.idx.jdb')
        const indexJSON = this.indexManager.indexedFields && this.indexManager.indexedFields.length > 0 ? this.indexManager.toJSON() : {}
        
        // Check if index is empty
        const isEmpty = !indexJSON || Object.keys(indexJSON).length === 0 || 
          (this.indexManager.indexedFields && this.indexManager.indexedFields.every(field => {
            const fieldIndex = indexJSON[field]
            return !fieldIndex || (typeof fieldIndex === 'object' && Object.keys(fieldIndex).length === 0)
          }))
        
        // PROTECTION: Don't overwrite a valid index file with empty data
        // If the .idx.jdb file exists and has data, and we're trying to save empty index,
        // skip the save to prevent corruption
        if (isEmpty && !this.offsets?.length) {
          const fs = await import('fs')
          if (fs.existsSync(idxPath)) {
            try {
              const existingData = JSON.parse(await fs.promises.readFile(idxPath, 'utf8'))
              const existingHasData = existingData.index && Object.keys(existingData.index).length > 0
              const existingHasOffsets = existingData.offsets && existingData.offsets.length > 0
              
              if (existingHasData || existingHasOffsets) {
                if (this.opts.debugMode) {
                  console.log(`⚠️ _saveIndexDataToFile: Skipping save - would overwrite valid index with empty data`)
                }
                return // Don't overwrite valid index with empty one
              }
            } catch (error) {
              // If we can't read existing file, proceed with save (might be corrupted)
              if (this.opts.debugMode) {
                console.log(`⚠️ _saveIndexDataToFile: Could not read existing index file, proceeding with save`)
              }
            }
          }
        }
        
        const indexData = {
          index: indexJSON,
          offsets: this.offsets, // Save actual offsets for efficient file operations
          indexOffset: this.indexOffset, // Save file size for proper range calculations
          // Save configuration for reuse when database exists
          config: {
            fields: this.opts.fields,
            indexes: this.opts.indexes,
            originalIndexes: this.opts.originalIndexes,
            schema: this.serializer?.getSchema?.() || null
          }
        }
        
        // Include term mapping data in .idx file if term mapping fields exist
        const termMappingFields = this.getTermMappingFields()
        if (termMappingFields.length > 0 && this.termManager) {
          const termData = await this.termManager.saveTerms()
          indexData.termMapping = termData
        }
        
        // Always create .idx file for databases with indexes, even if empty
        // This ensures the database structure is complete
        const originalFile = this.fileHandler.file
        this.fileHandler.file = idxPath
        await this.fileHandler.writeAll(JSON.stringify(indexData, null, 2))
        this.fileHandler.file = originalFile
        
        if (this.opts.debugMode) {
          console.log(`💾 Index data saved to ${idxPath}`)
        }
      } catch (error) {
        this.integrityCorrections.indexSaveFailures++
        console.warn(`⚠️ Index save failure (${this.integrityCorrections.indexSaveFailures} times): ${error.message}`)
        throw error // Re-throw to let caller handle
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
      }
    }
    return this.operationQueue.getStats()
  }

  /**
   * Wait for all pending operations to complete
   * This includes operation queue AND active insert sessions
   * If called with no arguments, interpret as waitForOperations(null).
   * If argument provided (maxWaitTime), pass that on.
   */
  async waitForOperations(maxWaitTime = null) {
    // Accept any falsy/undefined/empty call as "wait for all"
    const actualWaitTime = arguments.length === 0 ? null : maxWaitTime
    const startTime = Date.now()
    const hasTimeout = actualWaitTime !== null && actualWaitTime !== undefined
    
    // Wait for operation queue
    if (this.operationQueue) {
      const queueCompleted = await this.operationQueue.waitForCompletion(actualWaitTime)
      if (!queueCompleted && hasTimeout) {
        return false
      }
    }
    
    // Wait for active insert sessions
    if (this.activeInsertSessions.size > 0) {
      if (this.opts.debugMode) {
        console.log(`⏳ waitForOperations: Waiting for ${this.activeInsertSessions.size} active insert sessions`)
      }
      
      // Wait for all active sessions to complete
      const sessionPromises = Array.from(this.activeInsertSessions).map(session => 
        session.waitForOperations(actualWaitTime)
      )
      
      try {
        const sessionResults = await Promise.all(sessionPromises)
        
        // Check if any session timed out
        if (hasTimeout && sessionResults.some(result => !result)) {
          return false
        }
      } catch (error) {
        if (this.opts.debugMode) {
          console.log(`⚠️ waitForOperations: Error waiting for sessions: ${error.message}`)
        }
        // Continue anyway - don't fail the entire operation
      }
    }
    
    return true
  }
}

export { Database }

