/**
 * SchemaManager - Manages field schemas for optimized array-based serialization
 * This replaces the need for repeating field names in JSON objects
 */
export default class SchemaManager {
  constructor(opts = {}) {
    this.opts = Object.assign({
      enableArraySerialization: true,
      strictSchema: true,
      debugMode: false
    }, opts)
    
    // Schema definition: array of field names in order
    this.schema = []
    this.fieldToIndex = new Map() // field name -> index
    this.indexToField = new Map() // index -> field name
    this.schemaVersion = 1
    this.isInitialized = false
  }

  /**
   * Initialize schema from options or auto-detect from data
   */
  initializeSchema(schemaOrData, autoDetect = false) {
    if (this.isInitialized && this.opts.strictSchema) {
      if (this.opts.debugMode) {
        console.log('SchemaManager: Schema already initialized, skipping')
      }
      return
    }

    if (Array.isArray(schemaOrData)) {
      // Explicit schema provided
      this.setSchema(schemaOrData)
    } else if (autoDetect && typeof schemaOrData === 'object') {
      // Auto-detect schema from data
      this.autoDetectSchema(schemaOrData)
    } else if (schemaOrData && typeof schemaOrData === 'object') {
      // Initialize from database options
      this.initializeFromOptions(schemaOrData)
    }

    this.isInitialized = true
    if (this.opts.debugMode) {
      console.log('SchemaManager: Schema initialized:', this.schema)
    }
  }

  /**
   * Set explicit schema
   */
  setSchema(fieldNames) {
    this.schema = [...fieldNames] // Create copy
    this.fieldToIndex.clear()
    this.indexToField.clear()
    
    this.schema.forEach((field, index) => {
      this.fieldToIndex.set(field, index)
      this.indexToField.set(index, field)
    })

    if (this.opts.debugMode) {
      console.log('SchemaManager: Schema set:', this.schema)
    }
  }

  /**
   * Auto-detect schema from sample data
   */
  autoDetectSchema(sampleData) {
    if (Array.isArray(sampleData)) {
      // Use first record as template
      if (sampleData.length > 0) {
        this.autoDetectSchema(sampleData[0])
      }
      return
    }

    if (typeof sampleData === 'object' && sampleData !== null) {
      const fields = Object.keys(sampleData).sort() // Sort for consistency
      
      // CRITICAL FIX: Always include 'id' field in schema for proper array format
      if (!fields.includes('id')) {
        fields.push('id')
      }
      
      this.setSchema(fields)
    }
  }

  /**
   * Initialize schema from database options
   * Note: schema option is no longer supported, use fields instead
   */
  initializeFromOptions(opts) {
    // Schema option is no longer supported - fields should be used instead
    // This method is kept for compatibility but does nothing
    // Schema initialization is handled by Database.initializeSchema() using fields
  }

  /**
   * Add new field to schema (for schema evolution)
   */
  addField(fieldName) {
    if (this.fieldToIndex.has(fieldName)) {
      return this.fieldToIndex.get(fieldName)
    }

    const newIndex = this.schema.length
    this.schema.push(fieldName)
    this.fieldToIndex.set(fieldName, newIndex)
    this.indexToField.set(newIndex, fieldName)

    if (this.opts.debugMode) {
      console.log('SchemaManager: Added field:', fieldName, 'at index:', newIndex)
    }

    return newIndex
  }

  /**
   * Convert object to array using schema with strict field enforcement
   */
  objectToArray(obj) {
    if (!this.isInitialized || !this.opts.enableArraySerialization) {
      return obj // Fallback to object format
    }

    if (typeof obj !== 'object' || obj === null || Array.isArray(obj)) {
      return obj // Don't convert non-objects or arrays
    }

    const result = new Array(this.schema.length)
    
    // Fill array with values in schema order
    // Missing fields become undefined, extra fields are ignored
    for (let i = 0; i < this.schema.length; i++) {
      const fieldName = this.schema[i]
      result[i] = obj[fieldName] !== undefined ? obj[fieldName] : undefined
    }
    
    // CRITICAL FIX: Always append 'id' field if it exists and is not in schema
    // The 'id' field must be preserved even if not in the schema
    if (obj.id !== undefined && obj.id !== null && this.schema.indexOf('id') === -1) {
      result.push(obj.id)
    }

    return result
  }

  /**
   * Convert array back to object using schema
   */
  arrayToObject(arr) {
    if (!this.isInitialized || !this.opts.enableArraySerialization) {
      return arr // Fallback to array format
    }

    if (!Array.isArray(arr)) {
      return arr // Don't convert non-arrays
    }

    const obj = {}
    const idIndex = this.schema.indexOf('id')
    
    // DISABLED: Schema migration detection was causing field mapping corruption
    // The logic was incorrectly assuming ID was in first position when it's appended at the end
    // This caused fields to be shifted incorrectly during object-to-array-to-object conversion
    let arrayOffset = 0
    
    // Map array values to object properties
    // Only include fields that are in the schema
    for (let i = 0; i < Math.min(arr.length - arrayOffset, this.schema.length); i++) {
      const fieldName = this.schema[i]
      const arrayIndex = i + arrayOffset
      // Only include non-undefined values to avoid cluttering the object
      if (arr[arrayIndex] !== undefined) {
        obj[fieldName] = arr[arrayIndex]
      }
    }
    
    // CRITICAL FIX: Always preserve 'id' field if it exists in the original object
    // The 'id' field may not be in the schema but must be preserved
    if (idIndex !== -1 && arr[idIndex] !== undefined) {
      // 'id' is in schema and has a value
      obj.id = arr[idIndex]
    } else if (!obj.id && arr.length > this.schema.length + arrayOffset) {
      // 'id' is not in schema but array has extra element(s) - check if last element could be ID
      // This handles cases where ID was added after schema initialization
      for (let i = this.schema.length + arrayOffset; i < arr.length; i++) {
        // Try to infer if this is an ID (string that looks like an ID)
        const potentialId = arr[i]
        if (potentialId !== undefined && potentialId !== null && typeof potentialId === 'string' && potentialId.length > 0 && potentialId.length < 100) {
          obj.id = potentialId
          break // Use first potential ID found
        }
      }
    }

    return obj
  }

  /**
   * Get field index by name
   */
  getFieldIndex(fieldName) {
    return this.fieldToIndex.get(fieldName)
  }

  /**
   * Get field name by index
   */
  getFieldName(index) {
    return this.indexToField.get(index)
  }

  /**
   * Check if field exists in schema
   */
  hasField(fieldName) {
    return this.fieldToIndex.has(fieldName)
  }

  /**
   * Get schema as array of field names
   */
  getSchema() {
    return [...this.schema] // Return copy
  }

  /**
   * Get schema size
   */
  getSchemaSize() {
    return this.schema.length
  }

  /**
   * Validate that object conforms to schema
   */
  validateObject(obj) {
    if (!this.isInitialized || !this.opts.strictSchema) {
      return true
    }

    if (typeof obj !== 'object' || obj === null || Array.isArray(obj)) {
      return false
    }

    // Check if object has all required fields
    for (const field of this.schema) {
      if (!(field in obj)) {
        if (this.opts.debugMode) {
          console.warn('SchemaManager: Missing required field:', field)
        }
        return false
      }
    }

    return true
  }

  /**
   * Get schema metadata for serialization
   */
  getSchemaMetadata() {
    return {
      version: this.schemaVersion,
      fields: [...this.schema],
      fieldCount: this.schema.length,
      isInitialized: this.isInitialized
    }
  }

  /**
   * Reset schema
   */
  reset() {
    this.schema = []
    this.fieldToIndex.clear()
    this.indexToField.clear()
    this.isInitialized = false
    this.schemaVersion++
  }

  /**
   * Get performance statistics
   */
  getStats() {
    return {
      schemaSize: this.schema.length,
      isInitialized: this.isInitialized,
      version: this.schemaVersion,
      enableArraySerialization: this.opts.enableArraySerialization
    }
  }
}
