// NOTE: Buffer pool was removed due to complexity with low performance gain
// It was causing serialization issues and data corruption in batch operations
// If reintroducing buffer pooling in the future, ensure proper buffer management
// and avoid reusing buffers that may contain stale data

import SchemaManager from './SchemaManager.mjs'

export default class Serializer {
  constructor(opts = {}) {
    this.opts = Object.assign({
      enableAdvancedSerialization: true,
      enableArraySerialization: true
      // NOTE: bufferPoolSize, adaptivePooling, memoryPressureThreshold removed
      // Buffer pool was causing more problems than benefits
    }, opts)

    // Initialize schema manager for array-based serialization
    this.schemaManager = new SchemaManager({
      enableArraySerialization: this.opts.enableArraySerialization,
      strictSchema: true,
      debugMode: this.opts.debugMode || false
    })



    // Advanced serialization settings
    this.serializationStats = {
      totalSerializations: 0,
      totalDeserializations: 0,
      jsonSerializations: 0,
      arraySerializations: 0,
      objectSerializations: 0
    }
  }

  /**
   * Initialize schema for array-based serialization
   */
  initializeSchema(schemaOrData, autoDetect = false) {
    this.schemaManager.initializeSchema(schemaOrData, autoDetect)
  }

  /**
   * Get current schema
   */
  getSchema() {
    return this.schemaManager.getSchema()
  }

  /**
   * Convert object to array format for optimized serialization
   */
  convertToArrayFormat(obj) {
    if (!this.opts.enableArraySerialization) {
      return obj
    }
    return this.schemaManager.objectToArray(obj)
  }

  /**
   * Convert array format back to object
   */
  convertFromArrayFormat(arr) {
    if (!this.opts.enableArraySerialization) {
      return arr
    }
    const obj = this.schemaManager.arrayToObject(arr)
    
    // CRITICAL FIX: Always preserve 'id' field if it exists in the original array
    // The 'id' field may not be in the schema but must be preserved
    // Check if array has more elements than schema fields - the extra element(s) might be the ID
    if (!obj.id && Array.isArray(arr) && this.schemaManager.isInitialized) {
      const schemaLength = this.schemaManager.schema ? this.schemaManager.schema.length : 0
      if (arr.length > schemaLength) {
        // Check if any extra element looks like an ID (string)
        for (let i = schemaLength; i < arr.length; i++) {
          const potentialId = arr[i]
          if (potentialId !== undefined && potentialId !== null && typeof potentialId === 'string' && potentialId.length > 0) {
            obj.id = potentialId
            break
          }
        }
      }
    }
    
    return obj
  }

  /**
   * Advanced serialization with optimized JSON and buffer pooling
   */
  serialize(data, opts = {}) {
    this.serializationStats.totalSerializations++
    const addLinebreak = opts.linebreak !== false

    // Convert to array format if enabled
    const serializationData = this.convertToArrayFormat(data)
    
    // Track conversion statistics
    if (Array.isArray(serializationData) && typeof data === 'object' && data !== null) {
      this.serializationStats.arraySerializations++
    } else {
      this.serializationStats.objectSerializations++
    }

    // Use advanced JSON serialization
    if (this.opts.enableAdvancedSerialization) {
      this.serializationStats.jsonSerializations++
      return this.serializeAdvanced(serializationData, addLinebreak)
    }

    // Fallback to standard serialization
    this.serializationStats.jsonSerializations++
    return this.serializeStandard(serializationData, addLinebreak)
  }



  /**
   * Advanced serialization with optimized JSON.stringify and buffer pooling
   */
  serializeAdvanced(data, addLinebreak) {
    // CRITICAL FIX: Sanitize data to remove problematic characters before serialization
    const sanitizedData = this.sanitizeDataForJSON(data)

    // Validate encoding before serialization
    this.validateEncodingBeforeSerialization(sanitizedData)

    // Use optimized JSON.stringify without buffer pooling
    // NOTE: Buffer pool removed - using direct Buffer creation for simplicity and reliability
    const json = this.optimizedStringify(sanitizedData)
    
    // CRITICAL FIX: Normalize encoding before creating buffer
    const normalizedJson = this.normalizeEncoding(json)
    const jsonBuffer = Buffer.from(normalizedJson, 'utf8')

    const totalLength = jsonBuffer.length + (addLinebreak ? 1 : 0)
    const result = Buffer.allocUnsafe(totalLength)

    jsonBuffer.copy(result, 0, 0, jsonBuffer.length)
    if (addLinebreak) {
      result[jsonBuffer.length] = 0x0A
    }

    return result
  }

  /**
   * Proper encoding normalization with UTF-8 validation
   * Fixed to prevent double-encoding and data corruption
   */
  normalizeEncoding(str) {
    if (typeof str !== 'string') return str
    
    // Skip if already valid UTF-8 (99% of cases)
    if (this.isValidUTF8(str)) return str
    
    // Try to detect and convert encoding safely
    return this.safeConvertToUTF8(str)
  }

  /**
   * Check if string is valid UTF-8
   */
  isValidUTF8(str) {
    try {
      // Test if string can be encoded and decoded as UTF-8 without loss
      const encoded = Buffer.from(str, 'utf8')
      const decoded = encoded.toString('utf8')
      return decoded === str
    } catch (error) {
      return false
    }
  }

  /**
   * Safe conversion to UTF-8 with proper encoding detection
   */
  safeConvertToUTF8(str) {
    // Try common encodings in order of likelihood
    const encodings = ['utf8', 'latin1', 'utf16le', 'ascii']
    
    for (const encoding of encodings) {
      try {
        const converted = Buffer.from(str, encoding).toString('utf8')
        
        // Validate the conversion didn't lose information
        if (this.isValidUTF8(converted)) {
          return converted
        }
      } catch (error) {
        // Try next encoding
        continue
      }
    }
    
    // Fallback: return original string (preserve data)
    console.warn('JexiDB: Could not normalize encoding, preserving original string')
    return str
  }

  /**
   * Enhanced deep encoding normalization with UTF-8 validation
   * Fixed to prevent double-encoding and data corruption
   */
  deepNormalizeEncoding(obj) {
    if (obj === null || obj === undefined) return obj
    
    if (typeof obj === 'string') {
      return this.normalizeEncoding(obj)
    }
    
    if (Array.isArray(obj)) {
      // Check if normalization is needed (performance optimization)
      const needsNormalization = obj.some(item => 
        typeof item === 'string' && !this.isValidUTF8(item)
      )
      
      if (!needsNormalization) return obj
      
      return obj.map(item => this.deepNormalizeEncoding(item))
    }
    
    if (typeof obj === 'object') {
      // Check if normalization is needed (performance optimization)
      const needsNormalization = Object.values(obj).some(value => 
        typeof value === 'string' && !this.isValidUTF8(value)
      )
      
      if (!needsNormalization) return obj
      
      const normalized = {}
      for (const [key, value] of Object.entries(obj)) {
        normalized[key] = this.deepNormalizeEncoding(value)
      }
      return normalized
    }
    
    return obj
  }

  /**
   * Validate encoding before serialization
   */
  /**
   * Sanitize data to remove problematic characters that break JSON parsing
   * CRITICAL FIX: Prevents "Expected ',' or ']'" and "Unterminated string" errors
   * by removing control characters that cannot be safely represented in JSON
   */
  sanitizeDataForJSON(data) {
    const sanitizeString = (str) => {
      if (typeof str !== 'string') return str

      return str
        // Remove control characters that break JSON parsing (but keep \n, \r, \t as they can be escaped)
        // Remove: NUL, SOH, STX, ETX, EOT, ENQ, ACK, BEL, VT, FF, SO, SI, DLE, DC1-DC4, NAK, SYN, ETB, CAN, EM, SUB, ESC, FS, GS, RS, US, DEL, C1 controls
        .replace(/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]/g, '')
        // Limit string length to prevent performance issues
        .substring(0, 10000)
    }

    const sanitizeArray = (arr) => {
      if (!Array.isArray(arr)) return arr

      return arr
        .map(item => this.sanitizeDataForJSON(item))
        .filter(item => item !== null && item !== undefined && item !== '')
    }

    if (typeof data === 'string') {
      return sanitizeString(data)
    }

    if (Array.isArray(data)) {
      return sanitizeArray(data)
    }

    if (data && typeof data === 'object') {
      const sanitized = {}
      for (const [key, value] of Object.entries(data)) {
        const sanitizedValue = this.sanitizeDataForJSON(value)
        // Only include non-null, non-undefined values
        if (sanitizedValue !== null && sanitizedValue !== undefined) {
          sanitized[key] = sanitizedValue
        }
      }
      return sanitized
    }

    return data
  }

  validateEncodingBeforeSerialization(data) {
    const issues = []
    
    const checkString = (str, path = '') => {
      if (typeof str === 'string' && !this.isValidUTF8(str)) {
        issues.push(`Invalid encoding at ${path}: "${str.substring(0, 50)}..."`)
      }
    }
    
    const traverse = (obj, path = '') => {
      if (typeof obj === 'string') {
        checkString(obj, path)
      } else if (Array.isArray(obj)) {
        obj.forEach((item, index) => {
          traverse(item, `${path}[${index}]`)
        })
      } else if (obj && typeof obj === 'object') {
        Object.entries(obj).forEach(([key, value]) => {
          traverse(value, path ? `${path}.${key}` : key)
        })
      }
    }
    
    traverse(data)
    
    if (issues.length > 0) {
      console.warn('JexiDB: Encoding issues detected:', issues)
    }
    
    return issues.length === 0
  }

  /**
   * Optimized JSON.stringify with fast paths for common data structures
   * Now includes deep encoding normalization for all string fields
   */
  optimizedStringify(obj) {
    // CRITICAL: Normalize encoding for all string fields before stringify
    const normalizedObj = this.deepNormalizeEncoding(obj)
    return this._stringifyNormalizedValue(normalizedObj)
  }

  _stringifyNormalizedValue(value) {
    // Fast path for null and undefined
    if (value === null || value === undefined) {
      return 'null'
    }

    const type = typeof value

    // Fast path for primitives
    if (type === 'boolean') {
      return value ? 'true' : 'false'
    }

    if (type === 'number') {
      return Number.isFinite(value) ? value.toString() : 'null'
    }

    if (type === 'string') {
      // Fast path for simple strings (no escaping needed)
      if (!/[\\"\u0000-\u001f]/.test(value)) {
        return '"' + value + '"'
      }
      // Fall back to JSON.stringify for complex strings
      return JSON.stringify(value)
    }

    if (Array.isArray(value)) {
      return this._stringifyNormalizedArray(value)
    }

    if (type === 'object') {
      const keys = Object.keys(value)
      if (keys.length === 0) return '{}'
      // Use native stringify for object to leverage stable handling of undefined, Dates, etc.
      return JSON.stringify(value)
    }

    // Fallback to JSON.stringify for unknown types (BigInt, symbols, etc.)
    return JSON.stringify(value)
  }

  _stringifyNormalizedArray(arr) {
    const length = arr.length
    if (length === 0) return '[]'

    let result = '['
    for (let i = 0; i < length; i++) {
      if (i > 0) result += ','
      const element = arr[i]

      // JSON spec: undefined, functions, and symbols are serialized as null within arrays
      if (element === undefined || typeof element === 'function' || typeof element === 'symbol') {
        result += 'null'
        continue
      }

      result += this._stringifyNormalizedValue(element)
    }
    result += ']'
    return result
  }

  /**
   * Standard serialization (fallback)
   */
  serializeStandard(data, addLinebreak) {
    // CRITICAL FIX: Sanitize data to remove problematic characters before serialization
    const sanitizedData = this.sanitizeDataForJSON(data)

    // Validate encoding before serialization
    this.validateEncodingBeforeSerialization(sanitizedData)
    
    // NOTE: Buffer pool removed - using direct Buffer creation for simplicity and reliability
    // CRITICAL: Normalize encoding for all string fields before stringify
    const normalizedData = this.deepNormalizeEncoding(sanitizedData)
    const json = JSON.stringify(normalizedData)
    
    // CRITICAL FIX: Normalize encoding before creating buffer
    const normalizedJson = this.normalizeEncoding(json)
    const jsonBuffer = Buffer.from(normalizedJson, 'utf8')

    const totalLength = jsonBuffer.length + (addLinebreak ? 1 : 0)
    const result = Buffer.allocUnsafe(totalLength)

    jsonBuffer.copy(result, 0, 0, jsonBuffer.length)
    if (addLinebreak) {
      result[jsonBuffer.length] = 0x0A
    }

    return result
  }

  /**
   * Advanced deserialization with fast paths
   */
  deserialize(data) {
    this.serializationStats.totalDeserializations++
    
    if (data.length === 0) return null

    try {
      // Handle both Buffer and string inputs
      let str
      if (Buffer.isBuffer(data)) {
        // Fast path: avoid toString() for empty data
        if (data.length === 1 && data[0] === 0x0A) return null // Just newline
        str = data.toString('utf8').trim()
      } else if (typeof data === 'string') {
        str = data.trim()
      } else {
        throw new Error('Invalid data type for deserialization')
      }
      
      const strLength = str.length

      // Fast path for empty strings
      if (strLength === 0) return null

      // CRITICAL FIX: Detect and handle multiple JSON objects in the same line
      // This can happen if data was corrupted during concurrent writes or offset calculation errors
      const firstBrace = str.indexOf('{')
      const firstBracket = str.indexOf('[')
      
      // Helper function to extract first complete JSON object/array from a string
      // CRITICAL FIX: Must handle strings and escaped characters correctly
      // to avoid counting braces/brackets that are inside string values
      const extractFirstJson = (jsonStr, startChar) => {
        if (startChar === '{') {
          let braceCount = 0
          let endPos = -1
          let inString = false
          let escapeNext = false
          
          for (let i = 0; i < jsonStr.length; i++) {
            const char = jsonStr[i]
            
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
              if (char === '{') braceCount++
              if (char === '}') {
                braceCount--
                if (braceCount === 0) {
                  endPos = i + 1
                  break
                }
              }
            }
          }
          return endPos > 0 ? jsonStr.substring(0, endPos) : null
        } else if (startChar === '[') {
          let bracketCount = 0
          let endPos = -1
          let inString = false
          let escapeNext = false
          
          for (let i = 0; i < jsonStr.length; i++) {
            const char = jsonStr[i]
            
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
              if (char === '[') bracketCount++
              if (char === ']') {
                bracketCount--
                if (bracketCount === 0) {
                  endPos = i + 1
                  break
                }
              }
            }
          }
          return endPos > 0 ? jsonStr.substring(0, endPos) : null
        }
        return null
      }
      
      // Check if JSON starts at the beginning of the string
      const jsonStartsAtZero = (firstBrace === 0) || (firstBracket === 0)
      let hasValidJson = false
      
      if (jsonStartsAtZero) {
        // JSON starts at beginning - check for multiple JSON objects/arrays
        if (firstBrace === 0) {
          const secondBrace = str.indexOf('{', 1)
          if (secondBrace !== -1) {
            // Multiple objects detected - extract first
            const extracted = extractFirstJson(str, '{')
            if (extracted) {
              str = extracted
              hasValidJson = true
              if (this.opts && this.opts.debugMode) {
                console.warn(`⚠️ Deserialize: Multiple JSON objects detected, using first object only`)
              }
            }
          } else {
            hasValidJson = true // Single valid object starting at 0
          }
        } else if (firstBracket === 0) {
          const secondBracket = str.indexOf('[', 1)
          if (secondBracket !== -1) {
            // Multiple arrays detected - extract first
            const extracted = extractFirstJson(str, '[')
            if (extracted) {
              str = extracted
              hasValidJson = true
              if (this.opts && this.opts.debugMode) {
                console.warn(`⚠️ Deserialize: Multiple JSON arrays detected, using first array only`)
              }
            }
          } else {
            hasValidJson = true // Single valid array starting at 0
          }
        }
      } else {
        // JSON doesn't start at beginning - try to find and extract first valid JSON
        const jsonStart = firstBrace !== -1 ? (firstBracket !== -1 ? Math.min(firstBrace, firstBracket) : firstBrace) : firstBracket
        
        if (jsonStart !== -1 && jsonStart > 0) {
          // Found JSON but not at start - extract from that position
          const jsonStr = str.substring(jsonStart)
          const startChar = jsonStr[0]
          const extracted = extractFirstJson(jsonStr, startChar)
          
          if (extracted) {
            str = extracted
            hasValidJson = true
            if (this.opts && this.opts.debugMode) {
              console.warn(`⚠️ Deserialize: Found JSON after ${jsonStart} chars of invalid text, extracted first ${startChar === '{' ? 'object' : 'array'}`)
            }
          }
        }
      }

      // CRITICAL FIX: If no valid JSON structure found, throw error before attempting parse
      // This allows walk() and other callers to catch and skip invalid lines
      if (!hasValidJson && firstBrace === -1 && firstBracket === -1) {
        const errorStr = Buffer.isBuffer(data) ? data.toString('utf8').trim() : data.trim()
        const error = new Error(`Failed to deserialize JSON data: No valid JSON structure found in "${errorStr.substring(0, 100)}..."`)
        // Mark this as a "no valid JSON" error so it can be handled appropriately
        error.noValidJson = true
        throw error
      }

      // If we tried to extract but got nothing valid, also throw error
      if (hasValidJson && (!str || str.trim().length === 0)) {
        const error = new Error(`Failed to deserialize JSON data: Extracted JSON is empty`)
        error.noValidJson = true
        throw error
      }

      // Parse JSON data
      const parsedData = JSON.parse(str)
      
      // Convert from array format back to object if needed
      return this.convertFromArrayFormat(parsedData)
    } catch (e) {
      // If error was already formatted with noValidJson flag, re-throw as-is
      if (e.noValidJson) {
        throw e
      }
      // Otherwise, format the error message
      const str = Buffer.isBuffer(data) ? data.toString('utf8').trim() : data.trim()
      throw new Error(`Failed to deserialize JSON data: "${str.substring(0, 100)}..." - ${e.message}`)
    }
  }

  /**
   * Batch serialization for multiple records
   */
  serializeBatch(dataArray, opts = {}) {
    // CRITICAL FIX: Sanitize data to remove problematic characters before serialization
    const sanitizedDataArray = dataArray.map(data => this.sanitizeDataForJSON(data))

    // Validate encoding before serialization
    this.validateEncodingBeforeSerialization(sanitizedDataArray)

    // Convert all objects to array format for optimization
    const convertedData = sanitizedDataArray.map(data => this.convertToArrayFormat(data))
    
    // Track conversion statistics
    this.serializationStats.arraySerializations += convertedData.filter((item, index) => 
      Array.isArray(item) && typeof dataArray[index] === 'object' && dataArray[index] !== null
    ).length
    this.serializationStats.objectSerializations += dataArray.length - this.serializationStats.arraySerializations
    
    // JSONL format: serialize each array as a separate line
    try {
      const lines = []
      for (const arrayData of convertedData) {
        const json = this.optimizedStringify(arrayData)
        const normalizedJson = this.normalizeEncoding(json)
        lines.push(normalizedJson)
      }
      
      // Join all lines with newlines
      const jsonlContent = lines.join('\n')
      const jsonlBuffer = Buffer.from(jsonlContent, 'utf8')
      
      // Add final linebreak if requested
      const addLinebreak = opts.linebreak !== false
      const totalLength = jsonlBuffer.length + (addLinebreak ? 1 : 0)
      const result = Buffer.allocUnsafe(totalLength)
      
      jsonlBuffer.copy(result, 0, 0, jsonlBuffer.length)
      if (addLinebreak) {
        result[jsonlBuffer.length] = 0x0A
      }
      
      return result
    } catch (e) {
      // Fallback to individual serialization if batch serialization fails
      const results = []
      const batchSize = opts.batchSize || 100

      for (let i = 0; i < convertedData.length; i += batchSize) {
        const batch = convertedData.slice(i, i + batchSize)
        const batchResults = batch.map(data => this.serialize(data, opts))
        results.push(...batchResults)
      }

      return results
    }
  }

  /**
   * Batch deserialization for multiple records
   */
  deserializeBatch(dataArray) {
    // Optimization: try to parse all entries as a single JSON array first
    // This is much faster than parsing each entry individually
    try {
      // Convert all entries to strings and join them as a single JSON array
      const entriesJson = '[' + dataArray.map(data => {
        if (Buffer.isBuffer(data)) {
          return data.toString('utf8').trim()
        } else if (typeof data === 'string') {
          return data.trim()
        } else {
          throw new Error('Invalid data type for batch deserialization')
        }
      }).join(',') + ']'
      const parsedResults = JSON.parse(entriesJson)
      
      // Convert arrays back to objects if needed
      const results = parsedResults.map(data => this.convertFromArrayFormat(data))
      
      // Validate that all results are objects (JexiDB requirement)
      if (Array.isArray(results) && results.every(item => item && typeof item === 'object')) {
        return results
      }
      
      // If validation fails, fall back to individual parsing
      throw new Error('Validation failed - not all entries are objects')
    } catch (e) {
      // Fallback to individual deserialization if batch parsing fails
      const results = []
      const batchSize = 100 // Process in batches to avoid blocking

      for (let i = 0; i < dataArray.length; i += batchSize) {
        const batch = dataArray.slice(i, i + batchSize)
        const batchResults = batch.map(data => this.deserialize(data))
        results.push(...batchResults)
      }

      return results
    }
  }

  /**
   * Check if data appears to be binary (always false since we only use JSON now)
   */
  isBinaryData(data) {
    // All data is now JSON format
    return false
  }

  /**
   * Get comprehensive performance statistics
   */
  getStats() {
    // NOTE: Buffer pool stats removed - buffer pool was causing more problems than benefits
    return {
      // Serialization stats
      totalSerializations: this.serializationStats.totalSerializations,
      totalDeserializations: this.serializationStats.totalDeserializations,
      jsonSerializations: this.serializationStats.jsonSerializations,
      arraySerializations: this.serializationStats.arraySerializations,
      objectSerializations: this.serializationStats.objectSerializations,

      // Configuration
      enableAdvancedSerialization: this.opts.enableAdvancedSerialization,
      enableArraySerialization: this.opts.enableArraySerialization,
      
      // Schema stats
      schemaStats: this.schemaManager.getStats()
    }
  }

  /**
   * Cleanup resources
   */
  cleanup() {
    // NOTE: Buffer pool cleanup removed - buffer pool was causing more problems than benefits
    this.serializationStats = {
      totalSerializations: 0,
      totalDeserializations: 0,
      jsonSerializations: 0,
      arraySerializations: 0,
      objectSerializations: 0
    }
    
    // Reset schema manager
    if (this.schemaManager) {
      this.schemaManager.reset()
    }
  }
}