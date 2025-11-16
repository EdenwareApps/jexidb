import { Mutex } from 'async-mutex'
import { normalizeCriteriaOperators } from '../utils/operatorNormalizer.mjs'

export default class IndexManager {
  constructor(opts, databaseMutex = null, database = null) {
    this.opts = Object.assign({}, opts)
    this.index = Object.assign({data: {}}, this.opts.index)
    this.totalLines = 0
    this.rangeThreshold = 10 // Sensible threshold: 10+ consecutive numbers justify ranges
    this.binarySearchThreshold = 32 // Much higher for better performance
    this.database = database // Reference to database for term manager access
    
    // CRITICAL: Use database mutex to prevent deadlocks
    // If no database mutex provided, create a local one (for backward compatibility)
    this.mutex = databaseMutex || new Mutex()
    
    this.indexedFields = []
    this.setIndexesConfig(this.opts.indexes)
  }

  setTotalLines(total) {
    this.totalLines = total
  }

  /**
   * Update indexes configuration and ensure internal structures stay in sync
   * @param {Object|Array<string>} indexes
   */
  setIndexesConfig(indexes) {
    if (!indexes) {
      this.opts.indexes = undefined
      this.indexedFields = []
      return
    }

    if (Array.isArray(indexes)) {
      const fields = indexes.map(field => String(field))
      this.indexedFields = fields

      const normalizedConfig = {}
      for (const field of fields) {
        const existingConfig = (!Array.isArray(this.opts.indexes) && typeof this.opts.indexes === 'object') ? this.opts.indexes[field] : undefined
        normalizedConfig[field] = existingConfig ?? 'auto'
        if (!this.index.data[field]) {
          this.index.data[field] = {}
        }
      }
      this.opts.indexes = normalizedConfig
      return
    }

    if (typeof indexes === 'object') {
      this.opts.indexes = Object.assign({}, indexes)
      this.indexedFields = Object.keys(this.opts.indexes)

      for (const field of this.indexedFields) {
        if (!this.index.data[field]) {
          this.index.data[field] = {}
        }
      }
    }
  }

  /**
   * Check if a field is configured as an index
   * @param {string} field - Field name
   * @returns {boolean}
   */
  isFieldIndexed(field) {
    if (!field) return false
    if (!Array.isArray(this.indexedFields)) {
      return false
    }
    return this.indexedFields.includes(field)
  }

  /**
   * Determine whether the index has usable data for a given field
   * @param {string} field - Field name
   * @returns {boolean}
   */
  hasUsableIndexData(field) {
    if (!field) return false
    const fieldData = this.index?.data?.[field]
    if (!fieldData || typeof fieldData !== 'object') {
      return false
    }

    for (const key in fieldData) {
      if (!Object.prototype.hasOwnProperty.call(fieldData, key)) continue
      const entry = fieldData[key]
      if (!entry) continue

      if (entry.set && typeof entry.set.size === 'number' && entry.set.size > 0) {
        return true
      }

      if (Array.isArray(entry.ranges) && entry.ranges.length > 0) {
        const hasRangeData = entry.ranges.some(range => {
          if (range === null || typeof range === 'undefined') {
            return false
          }
          if (typeof range === 'object') {
            const count = typeof range.count === 'number' ? range.count : 0
            return count > 0
          }
          // When ranges are stored as individual numbers
          return true
        })

        if (hasRangeData) {
          return true
        }
      }
    }

    return false
  }

  // Ultra-fast range conversion - only for very large datasets
  _toRanges(numbers) {
    if (numbers.length === 0) return []
    if (numbers.length < this.rangeThreshold) return numbers // Keep as-is for small arrays
    
    const sorted = numbers.sort((a, b) => a - b) // Sort in-place
    const ranges = []
    let start = sorted[0]
    let count = 1
    
    for (let i = 1; i < sorted.length; i++) {
      if (sorted[i] === sorted[i-1] + 1) {
        count++
      } else {
        // End of consecutive sequence
        if (count >= this.rangeThreshold) {
          ranges.push({start, count})
        } else {
          // Add individual numbers for small sequences
          for (let j = start; j < start + count; j++) {
            ranges.push(j)
          }
        }
        start = sorted[i]
        count = 1
      }
    }
    
    // Handle last sequence
    if (count >= this.rangeThreshold) {
      ranges.push({start, count})
    } else {
      for (let j = start; j < start + count; j++) {
        ranges.push(j)
      }
    }
    
    return ranges
  }

  // Ultra-fast range expansion
  _fromRanges(ranges) {
    if (!ranges || ranges.length === 0) return []
    
    const numbers = []
    for (const item of ranges) {
      if (typeof item === 'object' && item.start !== undefined) {
        // It's a range - use direct loop for maximum speed
        const end = item.start + item.count
        for (let i = item.start; i < end; i++) {
          numbers.push(i)
        }
      } else {
        // It's an individual number
        numbers.push(item)
      }
    }
    return numbers
  }

  // Ultra-fast lookup - optimized for Set operations
  _hasLineNumber(hybridData, lineNumber) {
    if (!hybridData) return false
    
    // Check in Set first (O(1)) - most common case
    if (hybridData.set && hybridData.set.has(lineNumber)) {
      return true
    }
    
    // Check in ranges only if necessary
    if (hybridData.ranges && hybridData.ranges.length > 0) {
      return this._searchInRanges(hybridData.ranges, lineNumber)
    }
    
    return false
  }

  // Optimized search strategy
  _searchInRanges(ranges, lineNumber) {
    if (ranges.length < this.binarySearchThreshold) {
      // Linear search for small ranges
      return this._linearSearchRanges(ranges, lineNumber)
    } else {
      // Binary search for large ranges
      return this._binarySearchRanges(ranges, lineNumber)
    }
  }

  // Ultra-fast linear search
  _linearSearchRanges(ranges, lineNumber) {
    for (const item of ranges) {
      if (typeof item === 'object' && item.start !== undefined) {
        // It's a range
        if (lineNumber >= item.start && lineNumber < item.start + item.count) {
          return true
        }
      } else if (item === lineNumber) {
        // It's an individual number
        return true
      }
    }
    return false
  }

  // Optimized binary search
  _binarySearchRanges(ranges, lineNumber) {
    let left = 0
    let right = ranges.length - 1
    
    while (left <= right) {
      const mid = Math.floor((left + right) / 2)
      const range = ranges[mid]
      
      if (typeof range === 'object' && range.start !== undefined) {
        // It's a range
        if (lineNumber >= range.start && lineNumber < range.start + range.count) {
          return true
        } else if (lineNumber < range.start) {
          right = mid - 1
        } else {
          left = mid + 1
        }
      } else {
        // It's an individual number
        if (range === lineNumber) {
          return true
        } else if (range < lineNumber) {
          left = mid + 1
        } else {
          right = mid - 1
        }
      }
    }
    
    return false
  }

  // Ultra-fast add operation - minimal overhead
  _addLineNumber(hybridData, lineNumber) {
    // Initialize structure if needed
    if (!hybridData) {
      hybridData = { set: new Set(), ranges: [] }
    }
    
    // Add to Set directly (fastest path)
    if (!hybridData.set) {
      hybridData.set = new Set()
    }
    hybridData.set.add(lineNumber)
    
    // Optimize to ranges when Set gets reasonably large
    if (hybridData.set.size >= this.rangeThreshold * 2) { // 20 elements
      if (this.opts.debugMode) {
        console.log(`🔧 Triggering range optimization: Set size ${hybridData.set.size} >= threshold ${this.rangeThreshold * 2}`)
      }
      this._optimizeToRanges(hybridData)
    }
    
    return hybridData
  }

  // Ultra-fast remove operation
  _removeLineNumber(hybridData, lineNumber) {
    if (!hybridData) {
      return hybridData
    }
    
    // Remove from Set (fast path)
    if (hybridData.set) {
      hybridData.set.delete(lineNumber)
    }
    
    // Remove from ranges (less common)
    if (hybridData.ranges) {
      hybridData.ranges = this._removeFromRanges(hybridData.ranges, lineNumber)
    }
    
    return hybridData
  }

  // Optimized range removal
  _removeFromRanges(ranges, lineNumber) {
    if (!ranges || ranges.length === 0) return ranges
    
    const newRanges = []
    
    for (const item of ranges) {
      if (typeof item === 'object' && item.start !== undefined) {
        // It's a range
        if (lineNumber >= item.start && lineNumber < item.start + item.count) {
          // Split range if needed
          if (lineNumber === item.start) {
            // Remove first element
            if (item.count > 1) {
              newRanges.push({ start: item.start + 1, count: item.count - 1 })
            }
          } else if (lineNumber === item.start + item.count - 1) {
            // Remove last element
            if (item.count > 1) {
              newRanges.push({ start: item.start, count: item.count - 1 })
            }
          } else {
            // Remove from middle - split into two ranges
            const beforeCount = lineNumber - item.start
            const afterCount = item.count - beforeCount - 1
            
            if (beforeCount >= this.rangeThreshold) {
              newRanges.push({ start: item.start, count: beforeCount })
            } else {
              // Add individual numbers for small sequences
              for (let i = item.start; i < lineNumber; i++) {
                newRanges.push(i)
              }
            }
            
            if (afterCount >= this.rangeThreshold) {
              newRanges.push({ start: lineNumber + 1, count: afterCount })
            } else {
              // Add individual numbers for small sequences
              for (let i = lineNumber + 1; i < item.start + item.count; i++) {
                newRanges.push(i)
              }
            }
          }
        } else {
          newRanges.push(item)
        }
      } else if (item !== lineNumber) {
        // It's an individual number
        newRanges.push(item)
      }
    }
    
    return newRanges
  }

  // Ultra-lazy range conversion - only when absolutely necessary
  _optimizeToRanges(hybridData) {
    if (!hybridData.set || hybridData.set.size === 0) {
      return
    }
    
    if (this.opts.debugMode) {
      console.log(`🔧 Starting range optimization for Set with ${hybridData.set.size} elements`)
    }
    
    // Only convert if we have enough data to make it worthwhile
    if (hybridData.set.size < this.rangeThreshold) {
      return
    }
    
    // Convert Set to array and find consecutive sequences
    const numbers = Array.from(hybridData.set).sort((a, b) => a - b)
    const ranges = []
    
    let start = numbers[0]
    let count = 1
    
    for (let i = 1; i < numbers.length; i++) {
      if (numbers[i] === numbers[i-1] + 1) {
        count++
      } else {
        // End of consecutive sequence
        if (count >= this.rangeThreshold) {
          ranges.push({start, count})
          // Remove these numbers from Set
          for (let j = start; j < start + count; j++) {
            hybridData.set.delete(j)
          }
        }
        start = numbers[i]
        count = 1
      }
    }
    
    // Handle last sequence
    if (count >= this.rangeThreshold) {
      ranges.push({start, count})
      for (let j = start; j < start + count; j++) {
        hybridData.set.delete(j)
      }
    }
    
    // Add new ranges to existing ranges
    if (ranges.length > 0) {
      if (!hybridData.ranges) {
        hybridData.ranges = []
      }
      hybridData.ranges.push(...ranges)
      // Keep ranges sorted for efficient binary search
      hybridData.ranges.sort((a, b) => {
        const aStart = typeof a === 'object' ? a.start : a
        const bStart = typeof b === 'object' ? b.start : b
        return aStart - bStart
      })
    }
  }

  // Ultra-fast get all line numbers
  _getAllLineNumbers(hybridData) {
    if (!hybridData) return []
    
    // Use generator for lazy evaluation and better memory efficiency
    return Array.from(this._getAllLineNumbersGenerator(hybridData))
  }

  // OPTIMIZATION: Generator-based approach for better memory efficiency
  *_getAllLineNumbersGenerator(hybridData) {
    const normalizeLineNumber = (value) => {
      if (typeof value === 'number') {
        return value
      }
      if (typeof value === 'string') {
        const parsed = Number(value)
        return Number.isNaN(parsed) ? value : parsed
      }
      if (typeof value === 'bigint') {
        const maxSafe = BigInt(Number.MAX_SAFE_INTEGER)
        return value <= maxSafe ? Number(value) : value
      }
      return value
    }

    // Yield from Set (fastest path)
    if (hybridData.set) {
      for (const num of hybridData.set) {
        yield normalizeLineNumber(num)
      }
    }
    
    // Yield from ranges (optimized)
    if (hybridData.ranges) {
      for (const item of hybridData.ranges) {
        if (typeof item === 'object' && item.start !== undefined) {
          // It's a range - use direct loop for better performance
          const end = item.start + item.count
          for (let i = item.start; i < end; i++) {
            yield normalizeLineNumber(i)
          }
        } else {
          // It's an individual number
          yield normalizeLineNumber(item)
        }
      }
    }
  }

  // OPTIMIZATION 6: Ultra-fast add operation with incremental index updates
  async add(row, lineNumber) {
    if (typeof row !== 'object' || !row) {
      throw new Error('Invalid \'row\' parameter, it must be an object')
    }
    if (typeof lineNumber !== 'number') {
      throw new Error('Invalid line number')
    }
    
    // OPTIMIZATION 6: Use direct field access with minimal operations
    const data = this.index.data
    
    // OPTIMIZATION 6: Pre-allocate field structures for better performance
    const fields = Object.keys(this.opts.indexes || {})
    for (const field of fields) {
      // PERFORMANCE: Check if this is a term mapping field once
      const isTermMappingField = this.database?.termManager && 
        this.database.termManager.termMappingFields && 
        this.database.termManager.termMappingFields.includes(field)
      
      // CRITICAL FIX: For term mapping fields, prefer ${field}Ids if available
      // Records processed by processTermMapping have term IDs in ${field}Ids
      // Records loaded from file have term IDs directly in ${field} (after restoreTermIdsAfterDeserialization)
      let value
      if (isTermMappingField) {
        const termIdsField = `${field}Ids`
        const termIds = row[termIdsField]
        if (termIds && Array.isArray(termIds) && termIds.length > 0) {
          // Use term IDs from ${field}Ids (preferred - from processTermMapping)
          value = termIds
        } else {
          // Fallback: use field directly (for records loaded from file that have term IDs in field)
          value = row[field]
        }
      } else {
        value = row[field]
      }
      
      if (value !== undefined && value !== null) {
        // OPTIMIZATION 6: Initialize field structure if it doesn't exist
        if (!data[field]) {
          data[field] = {}
        }
        
        const values = Array.isArray(value) ? value : [value]
        for (const val of values) {
          let key
          
          if (isTermMappingField && typeof val === 'number') {
            // For term mapping fields, values are already term IDs
            key = String(val)
          } else if (isTermMappingField && typeof val === 'string') {
            // Fallback: convert string to term ID
            // CRITICAL: During indexing (add), we should use getTermId() to create IDs if needed
            // This is different from queries where we use getTermIdWithoutIncrement() to avoid creating new IDs
            const termId = this.database.termManager.getTermId(val)
            key = String(termId)
          } else {
            // For non-term-mapping fields (including array:number), use values directly
            key = String(val)
          }
          
          // OPTIMIZATION 6: Use direct assignment for better performance
          if (!data[field][key]) {
            data[field][key] = { set: new Set(), ranges: [] }
          }
          
          // OPTIMIZATION 6: Direct Set operation - fastest possible
          data[field][key].set.add(lineNumber)
          
          // OPTIMIZATION 6: Lazy range optimization - only when beneficial
          if (data[field][key].set.size >= this.rangeThreshold * 3) {
            this._optimizeToRanges(data[field][key])
          }
        }
      }
    }
  }

  /**
   * OPTIMIZATION 6: Add multiple records to the index in batch with optimized operations
   * @param {Array} records - Records to add
   * @param {number} startLineNumber - Starting line number
   */
  async addBatch(records, startLineNumber) {
    if (!records || !records.length) return
    
    // OPTIMIZATION 6: Pre-allocate index structures for better performance
    const data = this.index.data
    const fields = Object.keys(this.opts.indexes || {})
    
    for (const field of fields) {
      if (!data[field]) {
        data[field] = {}
      }
    }

    // OPTIMIZATION 6: Use Map for batch processing to reduce lookups
    const fieldUpdates = new Map()
    
    // OPTIMIZATION 6: Process all records in batch with optimized data structures
    for (let i = 0; i < records.length; i++) {
      const row = records[i]
      const lineNumber = startLineNumber + i

      for (const field of fields) {
        // PERFORMANCE: Check if this is a term mapping field once
        const isTermMappingField = this.database?.termManager && 
          this.database.termManager.termMappingFields && 
          this.database.termManager.termMappingFields.includes(field)
        
        // CRITICAL FIX: For term mapping fields, prefer ${field}Ids if available
        // Records processed by processTermMapping have term IDs in ${field}Ids
        // Records loaded from file have term IDs directly in ${field} (after restoreTermIdsAfterDeserialization)
        let value
        if (isTermMappingField) {
          const termIdsField = `${field}Ids`
          const termIds = row[termIdsField]
          if (termIds && Array.isArray(termIds) && termIds.length > 0) {
            // Use term IDs from ${field}Ids (preferred - from processTermMapping)
            value = termIds
          } else {
            // Fallback: use field directly (for records loaded from file that have term IDs in field)
            value = row[field]
          }
        } else {
          value = row[field]
        }
        
        if (value !== undefined && value !== null) {
          const values = Array.isArray(value) ? value : [value]
          for (const val of values) {
            let key
            
            if (isTermMappingField && typeof val === 'number') {
              // For term mapping fields, values are already term IDs
              key = String(val)
            } else if (isTermMappingField && typeof val === 'string') {
              // Fallback: convert string to term ID
              // CRITICAL: During indexing (addBatch), we should use getTermId() to create IDs if needed
              // This is different from queries where we use getTermIdWithoutIncrement() to avoid creating new IDs
              const termId = this.database.termManager.getTermId(val)
              key = String(termId)
            } else {
              // For non-term-mapping fields (including array:number), use values directly
              key = String(val)
            }
            
            // OPTIMIZATION 6: Use Map for efficient batch updates
            if (!fieldUpdates.has(field)) {
              fieldUpdates.set(field, new Map())
            }
            
            const fieldMap = fieldUpdates.get(field)
            if (!fieldMap.has(key)) {
              fieldMap.set(key, new Set())
            }
            
            fieldMap.get(key).add(lineNumber)
          }
        }
      }
    }
    
    // OPTIMIZATION 6: Apply all updates in batch for better performance
    for (const [field, fieldMap] of fieldUpdates) {
      for (const [key, lineNumbers] of fieldMap) {
        if (!data[field][key]) {
          data[field][key] = { set: new Set(), ranges: [] }
        }
        
        // OPTIMIZATION 6: Add all line numbers at once
        for (const lineNumber of lineNumbers) {
          data[field][key].set.add(lineNumber)
        }
        
        // OPTIMIZATION 6: Lazy range optimization - only when beneficial
        if (data[field][key].set.size >= this.rangeThreshold * 3) {
          this._optimizeToRanges(data[field][key])
        }
      }
    }
  }

  // Ultra-fast dry remove
  dryRemove(ln) {
    const data = this.index.data
    for (const field in data) {
      for (const value in data[field]) {
        // Direct Set operation - fastest possible
        if (data[field][value].set) {
          data[field][value].set.delete(ln)
        }
        if (data[field][value].ranges) {
          data[field][value].ranges = this._removeFromRanges(data[field][value].ranges, ln)
        }
        // Remove empty entries
        if ((!data[field][value].set || data[field][value].set.size === 0) &&
            (!data[field][value].ranges || data[field][value].ranges.length === 0)) {
          delete data[field][value]
        }
      }
    }
  }


  // Cleanup method to free memory
  cleanup() {
    const data = this.index.data
    for (const field in data) {
      for (const value in data[field]) {
        if (data[field][value].set) {
          if (typeof data[field][value].set.clearAll === 'function') {
            data[field][value].set.clearAll()
          } else if (typeof data[field][value].set.clear === 'function') {
            data[field][value].set.clear()
          }
        }
        if (data[field][value].ranges) {
          data[field][value].ranges.length = 0
        }
      }
      // Clear the entire field
      data[field] = {}
    }
    // Clear all data
    this.index.data = {}
    this.totalLines = 0
  }

  // Clear all indexes
  clear() {
    this.index.data = {}
    this.totalLines = 0
  }

  

  // Update a record in the index
  async update(oldRecord, newRecord, lineNumber = null) {
    if (!oldRecord || !newRecord) return
    
    // Remove old record by ID
    await this.remove(oldRecord)
    
    // Add new record with provided line number or use hash of the ID
    const actualLineNumber = lineNumber !== null ? lineNumber : this._getIdAsNumber(newRecord.id)
    await this.add(newRecord, actualLineNumber)
  }

  // Convert string ID to number for line number
  _getIdAsNumber(id) {
    if (typeof id === 'number') return id
    if (typeof id === 'string') {
      // Simple hash function to convert string to number
      let hash = 0
      for (let i = 0; i < id.length; i++) {
        const char = id.charCodeAt(i)
        hash = ((hash << 5) - hash) + char
        hash = hash & hash // Convert to 32-bit integer
      }
      return Math.abs(hash)
    }
    return 0
  }

  // Remove a record from the index
  async remove(record) {
    if (!record) return
    
    // If record is an array of line numbers, use the original method
    if (Array.isArray(record)) {
      return this._removeLineNumbers(record)
    }
    
    // If record is an object, remove by record data
    if (typeof record === 'object' && record.id) {
      return await this._removeRecord(record)
    }
  }

  // Remove a specific record from the index
  async _removeRecord(record) {
    if (!record) return
    
    const data = this.index.data
    const database = this.database
    const persistedCount = Array.isArray(database?.offsets) ? database.offsets.length : 0
    const lineMatchCache = new Map()

    const doesLineNumberBelongToRecord = async (lineNumber) => {
      if (lineMatchCache.has(lineNumber)) {
        return lineMatchCache.get(lineNumber)
      }

      let belongs = false

      try {
        if (lineNumber >= persistedCount) {
          const writeBufferIndex = lineNumber - persistedCount
          const candidate = database?.writeBuffer?.[writeBufferIndex]
          belongs = !!candidate && candidate.id === record.id
        } else if (lineNumber >= 0) {
          const range = database?.locate?.(lineNumber)
          if (range && database.fileHandler && database.serializer) {
            const [start, end] = range
            const buffer = await database.fileHandler.readRange(start, end)
            if (buffer && buffer.length > 0) {
              let line = buffer.toString('utf8')
              if (line) {
                line = line.trim()
                if (line.length > 0) {
                  const storedRecord = database.serializer.deserialize(line)
                  belongs = storedRecord && storedRecord.id === record.id
                }
              }
            }
          }
        }
      } catch (error) {
        belongs = false
      }

      lineMatchCache.set(lineNumber, belongs)
      return belongs
    }

    for (const field in data) {
      if (record[field] !== undefined && record[field] !== null) {
        const values = Array.isArray(record[field]) ? record[field] : [record[field]]
        for (const val of values) {
          let key
          
          // Check if this is a term mapping field (array:string fields only)
          const isTermMappingField = this.database?.termManager && 
            this.database.termManager.termMappingFields && 
            this.database.termManager.termMappingFields.includes(field)
          
          if (isTermMappingField && typeof val === 'number') {
            // For term mapping fields (array:string), the values are already term IDs
            key = String(val)
            if (this.database.opts.debugMode) {
              console.log(`🔍 IndexManager._removeRecord: Using term ID ${val} directly for field "${field}"`)
            }
          } else if (isTermMappingField && typeof val === 'string') {
            // For term mapping fields (array:string), convert string to term ID
            const termId = this.database.termManager.getTermIdWithoutIncrement(val)
            key = String(termId)
            if (this.database.opts.debugMode) {
              console.log(`🔍 IndexManager._removeRecord: Using term ID ${termId} for term "${val}"`)
            }
          } else {
            // For non-term-mapping fields (including array:number), use values directly
            key = String(val)
            if (this.database?.opts?.debugMode) {
              console.log(`🔍 IndexManager._removeRecord: Using value "${val}" directly for field "${field}"`)
            }
          }
          
          // Note: TermManager notification is handled by Database.mjs
          // to avoid double decrementation during updates
          
          const indexEntry = data[field][key]
          if (indexEntry) {
            const lineNumbers = this._getAllLineNumbers(indexEntry)
            const filteredLineNumbers = []

            for (const lineNumber of lineNumbers) {
              if (!(await doesLineNumberBelongToRecord(lineNumber))) {
                filteredLineNumbers.push(lineNumber)
              }
            }
            
            if (filteredLineNumbers.length === 0) {
              delete data[field][key]
            } else {
              // Rebuild the index value with filtered line numbers
              data[field][key].set = new Set(filteredLineNumbers)
              data[field][key].ranges = []
            }
          }
        }
      }
    }
  }

  // Ultra-fast remove with batch processing (renamed from remove)
  _removeLineNumbers(lineNumbers) {
    if (!lineNumbers || lineNumbers.length === 0) return
    
    lineNumbers.sort((a, b) => a - b) // Sort ascending for efficient processing
    
    const data = this.index.data
    for (const field in data) {
      for (const value in data[field]) {
        const numbers = this._getAllLineNumbers(data[field][value])
        const newNumbers = []
        
        for (const ln of numbers) {
          let offset = 0
          for (const lineNumber of lineNumbers) {
            if (lineNumber < ln) {
              offset++
            } else if (lineNumber === ln) {
              offset = -1 // Mark for removal
              break
            }
          }  
          if (offset >= 0) {
            newNumbers.push(ln - offset) // Update the value
          }
        }  
        
        if (newNumbers.length > 0) {
          // Rebuild hybrid structure with new numbers
          data[field][value] = { set: new Set(), ranges: [] }
          for (const num of newNumbers) {
            data[field][value] = this._addLineNumber(data[field][value], num)
          }
        } else {
          delete data[field][value]
        }
      }
    }
  } 

  // Ultra-fast replace with batch processing
  replace(map) {
    if (!map || map.size === 0) return
    
    const data = this.index.data
    for (const field in data) {
      for (const value in data[field]) {
        const numbers = this._getAllLineNumbers(data[field][value])
        const newNumbers = []
        
        for (const lineNumber of numbers) {
          if (map.has(lineNumber)) {
            newNumbers.push(map.get(lineNumber))
          } else {
            newNumbers.push(lineNumber)
          }
        }
        
        // Rebuild hybrid structure with new numbers
        data[field][value] = { set: new Set(), ranges: [] }
        for (const num of newNumbers) {
          data[field][value] = this._addLineNumber(data[field][value], num)
        }
      }
    }
  }

  // Ultra-fast query with early exit and smart processing
  query(criteria, options = {}) {
    if (typeof options === 'boolean') {
      options = { matchAny: options };
    }
    const { matchAny = false, caseInsensitive = false } = options;
    
    if (!criteria) {
        // Return all line numbers when no criteria provided
        return new Set(Array.from({ length: this.totalLines || 0 }, (_, i) => i));
    }
    
    // Handle $not operator
    if (criteria.$not && typeof criteria.$not === 'object') {
      // Get all possible line numbers from database offsets or totalLines
      const totalRecords = this.database?.offsets?.length || this.totalLines || 0;
      const allLines = new Set(Array.from({ length: totalRecords }, (_, i) => i));
      
      // Get line numbers matching the $not condition
      const notLines = this.query(criteria.$not, options);
      
      // Return complement (all lines except those matching $not condition)
      const result = new Set([...allLines].filter(x => !notLines.has(x)));
      
      // If there are other conditions besides $not, we need to intersect with them
      const otherCriteria = { ...criteria };
      delete otherCriteria.$not;
      
      if (Object.keys(otherCriteria).length > 0) {
        const otherResults = this.query(otherCriteria, options);
        return new Set([...result].filter(x => otherResults.has(x)));
      }
      
      return result;
    }
    
    // Handle $and queries with parallel processing optimization
    if (criteria.$and && Array.isArray(criteria.$and)) {
      // OPTIMIZATION: Process conditions in parallel for better performance
      if (criteria.$and.length > 1) {
        // Process all conditions in parallel (synchronous since query is not async)
        const conditionResults = criteria.$and.map(andCondition => 
          this.query(andCondition, options)
        );
        
        // Intersect all results for AND logic
        let result = conditionResults[0];
        for (let i = 1; i < conditionResults.length; i++) {
          result = new Set([...result].filter(x => conditionResults[i].has(x)));
        }
        
        // IMPORTANT: Check if there are other fields besides $and at the root level
        // If so, we need to intersect with them too
        const otherCriteria = { ...criteria };
        delete otherCriteria.$and;
        
        if (Object.keys(otherCriteria).length > 0) {
          const otherResults = this.query(otherCriteria, options);
          result = new Set([...result].filter(x => otherResults.has(x)));
        }
        
        return result || new Set();
      } else {
        // Single condition - check for other criteria at root level
        const andResult = this.query(criteria.$and[0], options);
        
        const otherCriteria = { ...criteria };
        delete otherCriteria.$and;
        
        if (Object.keys(otherCriteria).length > 0) {
          const otherResults = this.query(otherCriteria, options);
          return new Set([...andResult].filter(x => otherResults.has(x)));
        }
        
        return andResult;
      }
    }
    
    const fields = Object.keys(criteria);
    if (!fields.length) {
        // Return all line numbers when criteria is empty object
        return new Set(Array.from({ length: this.totalLines || 0 }, (_, i) => i));
    }
    
    let matchingLines = matchAny ? new Set() : null;
    const data = this.index.data
  
    for (const field of fields) {
      // Skip logical operators - they are handled separately
      if (field.startsWith('$')) continue;
      
      if (typeof data[field] === 'undefined') continue;
      
      const originalCriteriaValue = criteria[field];
      const criteriaValue = normalizeCriteriaOperators(originalCriteriaValue, { target: 'legacy', preserveOriginal: true });
      let lineNumbersForField = new Set();
      const isNumericField = this.opts.indexes[field] === 'number';
  
      // Handle RegExp values directly (MUST check before object check since RegExp is an object)
      if (criteriaValue instanceof RegExp) {
        // RegExp cannot be efficiently queried using indices - fall back to streaming
        // This will be handled by the QueryManager's streaming strategy
        continue;
      }

      if (typeof criteriaValue === 'object' && !Array.isArray(criteriaValue)) {
        const fieldIndex = data[field];
        
        // Handle $in operator for array queries
        if (criteriaValue.$in !== undefined) {
          const inValues = Array.isArray(criteriaValue.$in) ? criteriaValue.$in : [criteriaValue.$in];
          
          // PERFORMANCE: Cache term mapping field check once
          const isTermMappingField = this.database?.termManager && 
            this.database.termManager.termMappingFields && 
            this.database.termManager.termMappingFields.includes(field)
          
          // PERFORMANCE: Track if any term was found and matched
          let foundAnyMatch = false
          
          for (const inValue of inValues) {
            // SPACE OPTIMIZATION: Convert search term to term ID for lookup
            let searchTermId
            
            if (isTermMappingField && typeof inValue === 'number') {
              // For term mapping fields (array:string), the search value is already a term ID
              searchTermId = String(inValue)
            } else if (isTermMappingField && typeof inValue === 'string') {
              // For term mapping fields (array:string), convert string to term ID
              const termId = this.database?.termManager?.getTermIdWithoutIncrement(String(inValue))
              if (termId === undefined) {
                // Term not found in termManager - skip this search value
                // This means the term was never saved to the database
                if (this.opts?.debugMode) {
                  console.log(`⚠️  Term "${inValue}" not found in termManager for field "${field}" - skipping`)
                }
                continue // Skip this value, no matches possible
              }
              searchTermId = String(termId)
            } else {
              // For non-term-mapping fields (including array:number), use values directly
              searchTermId = String(inValue)
            }
            
            // PERFORMANCE: Direct lookup instead of iteration
            let matched = false
            if (caseInsensitive && typeof inValue === 'string') {
              const searchLower = searchTermId.toLowerCase()
              for (const value in fieldIndex) {
                if (value.toLowerCase() === searchLower) {
                  const numbers = this._getAllLineNumbers(fieldIndex[value]);
                  for (const lineNumber of numbers) {
                    lineNumbersForField.add(lineNumber);
                  }
                  matched = true
                  foundAnyMatch = true
                }
              }
            } else {
              const indexData = fieldIndex[searchTermId]
              if (indexData) {
                const numbers = this._getAllLineNumbers(indexData);
                for (const lineNumber of numbers) {
                  lineNumbersForField.add(lineNumber);
                }
                matched = true
                foundAnyMatch = true
              }
            }
          }
          
          // CRITICAL FIX: If no matches found at all (all terms were unknown or not in index),
          // lineNumbersForField remains empty which is correct (no results for $in)
          // This is handled correctly by the caller - empty Set means no matches
        }
        // Handle $nin operator (not in) - returns complement of $in
        else if (criteriaValue.$nin !== undefined) {
          const ninValues = Array.isArray(criteriaValue.$nin) ? criteriaValue.$nin : [criteriaValue.$nin];
          
          // Get all possible line numbers
          const totalRecords = this.database?.offsets?.length || this.totalLines || 0;
          const allLines = new Set(Array.from({ length: totalRecords }, (_, i) => i));
          
          // Get line numbers that match any of the $nin values
          const matchingLines = new Set();
          
          // PERFORMANCE: Cache term mapping field check once
          const isTermMappingField = this.database?.termManager && 
            this.database.termManager.termMappingFields && 
            this.database.termManager.termMappingFields.includes(field)
          
          for (const ninValue of ninValues) {
            // SPACE OPTIMIZATION: Convert search term to term ID for lookup
            let searchTermId
            
            if (isTermMappingField && typeof ninValue === 'number') {
              // For term mapping fields (array:string), the search value is already a term ID
              searchTermId = String(ninValue)
            } else if (isTermMappingField && typeof ninValue === 'string') {
              // For term mapping fields (array:string), convert string to term ID
              const termId = this.database?.termManager?.getTermIdWithoutIncrement(String(ninValue))
              if (termId === undefined) {
                // Term not found - skip this value (can't exclude what doesn't exist)
                if (this.opts?.debugMode) {
                  console.log(`⚠️  Term "${ninValue}" not found in termManager for field "${field}" - skipping`)
                }
                continue
              }
              searchTermId = String(termId)
            } else {
              // For non-term-mapping fields (including array:number), use values directly
              searchTermId = String(ninValue)
            }
            
            // PERFORMANCE: Direct lookup instead of iteration
            if (caseInsensitive && typeof ninValue === 'string') {
              const searchLower = searchTermId.toLowerCase()
              for (const value in fieldIndex) {
                if (value.toLowerCase() === searchLower) {
                  const numbers = this._getAllLineNumbers(fieldIndex[value]);
                  for (const lineNumber of numbers) {
                    matchingLines.add(lineNumber);
                  }
                }
              }
            } else {
              const indexData = fieldIndex[searchTermId]
              if (indexData) {
                const numbers = this._getAllLineNumbers(indexData);
                for (const lineNumber of numbers) {
                  matchingLines.add(lineNumber);
                }
              }
            }
          }
          
          // Return complement: all lines EXCEPT those matching $nin values
          lineNumbersForField = new Set([...allLines].filter(x => !matchingLines.has(x)));
        }
        // Handle $contains operator for array queries
        else if (criteriaValue.$contains !== undefined) {
          const containsValue = criteriaValue.$contains;
          // Handle case-insensitive for $contains
          if (caseInsensitive && typeof containsValue === 'string') {
            for (const value in fieldIndex) {
              if (value.toLowerCase() === containsValue.toLowerCase()) {
                const numbers = this._getAllLineNumbers(fieldIndex[value]);
                for (const lineNumber of numbers) {
                  lineNumbersForField.add(lineNumber);
                }
              }
            }
          } else {
            if (fieldIndex[containsValue]) {
              const numbers = this._getAllLineNumbers(fieldIndex[containsValue]);
              for (const lineNumber of numbers) {
                lineNumbersForField.add(lineNumber);
              }
            }
          }
        }
        // Handle $all operator for array queries - FIXED FOR TERM MAPPING
        else if (criteriaValue.$all !== undefined) {
          const allValues = Array.isArray(criteriaValue.$all) ? criteriaValue.$all : [criteriaValue.$all];

          const isTermMappingField = this.database?.termManager && 
            this.database.termManager.termMappingFields && 
            this.database.termManager.termMappingFields.includes(field)

          const normalizeValue = (value) => {
            if (isTermMappingField) {
              if (typeof value === 'number') {
                return String(value)
              }
              if (typeof value === 'string') {
                const termId = this.database?.termManager?.getTermIdWithoutIncrement(value)
                if (termId !== undefined) {
                  return String(termId)
                }
                return null
              }
              return null
            }
            return String(value)
          }

          const normalizedValues = []
          for (const value of allValues) {
            const normalized = normalizeValue(value)
            if (normalized === null) {
              // Term not found in term manager, no matches possible
              return lineNumbersForField
            }
            normalizedValues.push(normalized)
          }
          
          // Early exit optimization
          if (normalizedValues.length === 0) {
            // Empty $all matches everything
            for (const value in fieldIndex) {
              const numbers = this._getAllLineNumbers(fieldIndex[value]);
              for (const lineNumber of numbers) {
                lineNumbersForField.add(lineNumber);
              }
            }
          } else {
            // For term mapping, we need to find records that contain ALL specified terms
            // This requires a different approach than simple field matching
            
            // First, get all line numbers that contain each individual term
            const termLineNumbers = new Map();
            for (const term of normalizedValues) {
              if (fieldIndex[term]) {
                termLineNumbers.set(term, new Set(this._getAllLineNumbers(fieldIndex[term])));
              } else {
                // If any term doesn't exist, no records can match $all
                termLineNumbers.set(term, new Set());
              }
            }
            
            // Find intersection of all term line numbers
            if (termLineNumbers.size > 0) {
              const allTermSets = Array.from(termLineNumbers.values());
              let intersection = allTermSets[0];
              
              for (let i = 1; i < allTermSets.length; i++) {
                intersection = new Set([...intersection].filter(x => allTermSets[i].has(x)));
              }
              
              // Add all line numbers from intersection
              for (const lineNumber of intersection) {
                lineNumbersForField.add(lineNumber);
              }
            }
          }
        }
        // Handle other operators
        else {
          for (const value in fieldIndex) {
            let includeValue = true;
            if (isNumericField) {
              const numericValue = parseFloat(value);
              if (!isNaN(numericValue)) {
                if (criteriaValue['>'] !== undefined && numericValue <= criteriaValue['>']) {
                  includeValue = false;
                }
                if (criteriaValue['>='] !== undefined && numericValue < criteriaValue['>=']) {
                  includeValue = false;
                }
                if (criteriaValue['<'] !== undefined && numericValue >= criteriaValue['<']) {
                  includeValue = false;
                }
                if (criteriaValue['<='] !== undefined && numericValue > criteriaValue['<=']) {
                  includeValue = false;
                }
                if (criteriaValue['!='] !== undefined) {
                  const excludeValues = Array.isArray(criteriaValue['!='])
                    ? criteriaValue['!=']
                    : [criteriaValue['!=']];
                  if (excludeValues.includes(numericValue)) {
                    includeValue = false;
                  }
                }
              }
            } else {
              if (criteriaValue['contains'] !== undefined && typeof value === 'string') {
                const term = String(criteriaValue['contains']);
                if (caseInsensitive) {
                  if (!value.toLowerCase().includes(term.toLowerCase())) {
                    includeValue = false;
                  }
                } else {
                  if (!value.includes(term)) {
                    includeValue = false;
                  }
                }
              }
              if (criteriaValue['regex'] !== undefined) {
                let regex;
                if (typeof criteriaValue['regex'] === 'string') {
                  regex = new RegExp(criteriaValue['regex'], caseInsensitive ? 'i' : '');
                } else if (criteriaValue['regex'] instanceof RegExp) {
                  if (caseInsensitive && !criteriaValue['regex'].ignoreCase) {
                    const flags = criteriaValue['regex'].flags.includes('i')
                      ? criteriaValue['regex'].flags
                      : criteriaValue['regex'].flags + 'i';
                    regex = new RegExp(criteriaValue['regex'].source, flags);
                  } else {
                    regex = criteriaValue['regex'];
                  }
                }
                if (regex) {
                  // For array fields, test regex against each element
                  if (Array.isArray(value)) {
                    if (!value.some(element => regex.test(String(element)))) {
                      includeValue = false;
                    }
                  } else {
                    // For non-array fields, test regex against the value directly
                    if (!regex.test(String(value))) {
                      includeValue = false;
                    }
                  }
                }
              }
              if (criteriaValue['!='] !== undefined) {
                const excludeValues = Array.isArray(criteriaValue['!='])
                  ? criteriaValue['!=']
                  : [criteriaValue['!=']];
                if (excludeValues.includes(value)) {
                  includeValue = false;
                }
              }
            }

            if (includeValue) {
              const numbers = this._getAllLineNumbers(fieldIndex[value]);
              for (const lineNumber of numbers) {
                lineNumbersForField.add(lineNumber);
              }
            }
          }
        }
      } else {
        // Simple equality comparison - handle array queries
        const values = Array.isArray(criteriaValue) ? criteriaValue : [criteriaValue];
        const fieldData = data[field];
        for (const searchValue of values) {
          // SPACE OPTIMIZATION: Convert search term to term ID for lookup
          let searchTermId
          
          // PERFORMANCE: Cache term mapping field check once per field
          const isTermMappingField = this.database?.termManager && 
            this.database.termManager.termMappingFields && 
            this.database.termManager.termMappingFields.includes(field)
          
          if (isTermMappingField && typeof searchValue === 'number') {
            // For term mapping fields (array:string), the search value is already a term ID
            searchTermId = String(searchValue)
          } else if (isTermMappingField && typeof searchValue === 'string') {
            // For term mapping fields (array:string), convert string to term ID
            const termId = this.database?.termManager?.getTermIdWithoutIncrement(String(searchValue))
            if (termId === undefined) {
              // Term not found - skip this value
              if (this.opts?.debugMode) {
                console.log(`⚠️  Term "${searchValue}" not found in termManager for field "${field}" - skipping`)
              }
              continue // Skip this value, no matches possible
            }
            searchTermId = String(termId)
          } else {
            // For non-term-mapping fields (including array:number), use values directly
            searchTermId = String(searchValue)
          }
          
          for (const key in fieldData) {
            let match = false;
            if (isNumericField) {
              // Convert both parts to number
              match = Number(key) === Number(searchValue);
            } else {
              // SPACE OPTIMIZATION: Compare term IDs instead of full terms
              if (caseInsensitive) {
                // For case-insensitive, we need to check if the search term ID matches any key
                match = key === String(searchTermId);
              } else {
                match = key === String(searchTermId);
              }
            }
            if (match) {
              const numbers = this._getAllLineNumbers(fieldData[key]);
              for (const lineNumber of numbers) {
                lineNumbersForField.add(lineNumber);
              }
            }
          }
        }
      }
  
      // Consolidate results from each field
      if (matchAny) {
        matchingLines = new Set([...matchingLines, ...lineNumbersForField]);
      } else {
        if (matchingLines === null) {
          matchingLines = lineNumbersForField;
        } else {
          matchingLines = new Set([...matchingLines].filter(n => lineNumbersForField.has(n)));
        }
        if (!matchingLines.size) {
          return new Set();
        }
      }
    }
    return matchingLines || new Set();
  } 

  /**
   * Check if any records exist for given field and terms (index-only, ultra-fast)
   * Stops at first match for maximum performance - no disk I/O required
   * 
   * @param {string} fieldName - Indexed field name (e.g., 'nameTerms', 'groupTerms')
   * @param {string|Array<string>} terms - Single term or array of terms to check
   * @param {Object} options - Options: { $all: true/false, caseInsensitive: true/false, excludes: Array<string> }
   * @returns {boolean} - True if at least one match exists
   * 
   * @example
   * // Check if any record has 'channel' in nameTerms
   * indexManager.exists('nameTerms', 'channel')
   * 
   * @example
   * // Check if any record has ALL terms ['a', 'e'] in nameTerms ($all)
   * indexManager.exists('nameTerms', ['a', 'e'], { $all: true })
   * 
   * @example
   * // Check if any record has ANY of the terms ['channel', 'tv'] in nameTerms
   * indexManager.exists('nameTerms', ['channel', 'tv'], { $all: false })
   * 
   * @example
   * // Check if any record has 'tv' but NOT 'globo' in nameTerms
   * indexManager.exists('nameTerms', 'tv', { excludes: ['globo'] })
   * 
   * @example
   * // Check if any record has ['tv', 'news'] but NOT 'sports' in nameTerms
   * indexManager.exists('nameTerms', ['tv', 'news'], { $all: true, excludes: ['sports'] })
   */
  exists(fieldName, terms, options = {}) {
    // Early exit: validate fieldName
    if (!fieldName || typeof fieldName !== 'string') {
      return false;
    }
    
    // Early exit: check if field is indexed
    if (!this.isFieldIndexed(fieldName)) {
      return false;
    }
    
    const fieldIndex = this.index.data[fieldName];
    if (!fieldIndex || typeof fieldIndex !== 'object') {
      return false;
    }
    
    // Normalize terms to array
    const termsArray = Array.isArray(terms) ? terms : [terms];
    if (termsArray.length === 0) {
      return false;
    }
    
    const { $all = false, caseInsensitive = false, excludes = [] } = options;
    const hasExcludes = Array.isArray(excludes) && excludes.length > 0;
    const isTermMappingField = this.database?.termManager && 
      this.database.termManager.termMappingFields && 
      this.database.termManager.termMappingFields.includes(fieldName);
    
    // Helper: check if termData has any line numbers (ULTRA LIGHT - no expansion)
    const hasData = (termData) => {
      if (!termData) return false;
      // Check Set size (O(1))
      if (termData.set && termData.set.size > 0) {
        return true;
      }
      // Check ranges length (O(1))
      if (termData.ranges && termData.ranges.length > 0) {
        return true;
      }
      return false;
    };
    
    // Helper: get term key with term mapping and case-insensitive support
    const getTermKey = (term, useCaseInsensitive = false) => {
      if (isTermMappingField && typeof term === 'string') {
        let termId;
        if (useCaseInsensitive) {
          // For case-insensitive, search termManager for case-insensitive match
          const searchLower = String(term).toLowerCase();
          termId = null;
          if (this.database?.termManager?.termToId) {
            for (const [termStr, id] of this.database.termManager.termToId.entries()) {
              if (termStr.toLowerCase() === searchLower) {
                termId = id;
                break;
              }
            }
          }
        } else {
          termId = this.database?.termManager?.getTermIdWithoutIncrement(String(term));
        }
        
        if (termId === undefined || termId === null) {
          return null;
        }
        return String(termId);
      }
      
      // For non-term-mapping fields
      if (useCaseInsensitive && typeof term === 'string') {
        const searchLower = String(term).toLowerCase();
        for (const key in fieldIndex) {
          if (key.toLowerCase() === searchLower) {
            return key;
          }
        }
        return null;
      }
      
      return String(term);
    };
    
    // Handle $all (all terms must exist and have intersection)
    if ($all) {
      // Collect term data for all terms first (with early exit)
      const termDataArray = [];
      
      for (const term of termsArray) {
        // Get term key (with term mapping if applicable)
        let termKey;
        if (isTermMappingField && typeof term === 'string') {
          let termId;
          if (caseInsensitive) {
            // For case-insensitive, search termManager for case-insensitive match
            const searchLower = String(term).toLowerCase();
            termId = null;
            for (const [termStr, id] of this.database.termManager.termToId.entries()) {
              if (termStr.toLowerCase() === searchLower) {
                termId = id;
                break;
              }
            }
          } else {
            termId = this.database?.termManager?.getTermIdWithoutIncrement(String(term));
          }
          
          if (termId === undefined || termId === null) {
            return false; // Early exit: term doesn't exist in mapping
          }
          termKey = String(termId);
        } else {
          termKey = String(term);
          // For non-term-mapping fields with case-insensitive, search index keys
          if (caseInsensitive && typeof term === 'string') {
            const searchLower = termKey.toLowerCase();
            let foundKey = null;
            for (const key in fieldIndex) {
              if (key.toLowerCase() === searchLower) {
                foundKey = key;
                break;
              }
            }
            if (foundKey === null) {
              return false; // Early exit: term doesn't exist
            }
            termKey = foundKey;
          }
        }
        
        // Check if term exists in index
        const termData = fieldIndex[termKey];
        if (!termData || !hasData(termData)) {
          return false; // Early exit: term doesn't exist or has no data
        }
        
        termDataArray.push(termData);
      }
      
      // If we got here, all terms exist and have data
      // Now check if there's intersection (only if more than one term)
      if (termDataArray.length === 1) {
        // Single term - check excludes if any
        if (!hasExcludes) {
          return true; // Single term, already verified it has data, no excludes
        }
        // Need to check excludes - expand line numbers
        const lineNumbers = this._getAllLineNumbers(termDataArray[0]);
        const candidateLines = new Set(lineNumbers);
        
        // Remove lines that have exclude terms
        for (const excludeTerm of excludes) {
          const excludeKey = getTermKey(excludeTerm, caseInsensitive);
          if (excludeKey === null) continue;
          
          const excludeData = fieldIndex[excludeKey];
          if (!excludeData) continue;
          
          const excludeLines = this._getAllLineNumbers(excludeData);
          for (const line of excludeLines) {
            candidateLines.delete(line);
          }
          
          // Early exit if all candidates excluded
          if (candidateLines.size === 0) {
            return false;
          }
        }
        
        return candidateLines.size > 0;
      }
      
      // For multiple terms, we need to check intersection
      // But we want to do this as lightly as possible
      // Get line numbers only for intersection check (unavoidable for $all)
      const termLineNumberSets = [];
      for (const termData of termDataArray) {
        const lineNumbers = this._getAllLineNumbers(termData);
        if (lineNumbers.length === 0) {
          return false; // Early exit: no line numbers (shouldn't happen, but safety check)
        }
        termLineNumberSets.push(new Set(lineNumbers));
      }
      
      // Calculate intersection incrementally with early exit
      let intersection = termLineNumberSets[0];
      for (let i = 1; i < termLineNumberSets.length; i++) {
        // Filter intersection to only include items in current set
        intersection = new Set([...intersection].filter(x => termLineNumberSets[i].has(x)));
        if (intersection.size === 0) {
          return false; // Early exit: intersection is empty
        }
      }
      
      // Apply excludes if any
      if (hasExcludes) {
        for (const excludeTerm of excludes) {
          const excludeKey = getTermKey(excludeTerm, caseInsensitive);
          if (excludeKey === null) continue;
          
          const excludeData = fieldIndex[excludeKey];
          if (!excludeData) continue;
          
          const excludeLines = this._getAllLineNumbers(excludeData);
          for (const line of excludeLines) {
            intersection.delete(line);
          }
          
          // Early exit if all candidates excluded
          if (intersection.size === 0) {
            return false;
          }
        }
      }
      
      return intersection.size > 0;
    }
    
    // Handle $in behavior (any term exists) - default - ULTRA LIGHT
    // If no excludes, use ultra-fast path (no expansion needed)
    if (!hasExcludes) {
      for (const term of termsArray) {
        // Handle case-insensitive FIRST (before normal conversion)
        if (caseInsensitive && typeof term === 'string') {
          if (isTermMappingField && this.database?.termManager?.termToId) {
            // For term mapping fields, we need to find the term in termManager first
            // (case-insensitive), then convert to ID
            const searchLower = String(term).toLowerCase();
            let foundTermId = null;
            
            // Search termManager for case-insensitive match
            for (const [termStr, termId] of this.database.termManager.termToId.entries()) {
              if (termStr.toLowerCase() === searchLower) {
                foundTermId = termId;
                break;
              }
            }
            
            if (foundTermId !== null) {
              const termData = fieldIndex[String(foundTermId)];
              if (hasData(termData)) {
                return true; // Early exit: found a match
              }
            }
            // If not found, continue to next term
            continue;
          } else {
            // For non-term-mapping fields, search index keys directly
            const searchLower = String(term).toLowerCase();
            for (const key in fieldIndex) {
              if (key.toLowerCase() === searchLower) {
                const termData = fieldIndex[key];
                if (hasData(termData)) {
                  return true; // Early exit: found a match
                }
              }
            }
            // If not found, continue to next term
            continue;
          }
        }
        
        // Normal (case-sensitive) lookup
        const termKey = getTermKey(term, false);
        if (termKey === null) {
          continue; // Term not in mapping, try next
        }
        
        // Direct lookup (fastest path) - O(1) hash lookup
        const termData = fieldIndex[termKey];
        if (hasData(termData)) {
          return true; // Early exit: found a match
        }
      }
      
      return false;
    }
    
    // With excludes, we need to collect candidates and filter
    const candidateLines = new Set();
    
    for (const term of termsArray) {
      // Handle case-insensitive FIRST (before normal conversion)
      if (caseInsensitive && typeof term === 'string') {
        if (isTermMappingField && this.database?.termManager?.termToId) {
          // For term mapping fields, we need to find the term in termManager first
          // (case-insensitive), then convert to ID
          const searchLower = String(term).toLowerCase();
          let foundTermId = null;
          
          // Search termManager for case-insensitive match
          for (const [termStr, termId] of this.database.termManager.termToId.entries()) {
            if (termStr.toLowerCase() === searchLower) {
              foundTermId = termId;
              break;
            }
          }
          
          if (foundTermId !== null) {
            const termData = fieldIndex[String(foundTermId)];
            if (hasData(termData)) {
              // Add line numbers to candidates (need to expand for excludes check)
              const lineNumbers = this._getAllLineNumbers(termData);
              for (const line of lineNumbers) {
                candidateLines.add(line);
              }
            }
          }
          continue;
        } else {
          // For non-term-mapping fields, search index keys directly
          const searchLower = String(term).toLowerCase();
          for (const key in fieldIndex) {
            if (key.toLowerCase() === searchLower) {
              const termData = fieldIndex[key];
              if (hasData(termData)) {
                // Add line numbers to candidates
                const lineNumbers = this._getAllLineNumbers(termData);
                for (const line of lineNumbers) {
                  candidateLines.add(line);
                }
              }
            }
          }
          continue;
        }
      }
      
      // Normal (case-sensitive) lookup
      const termKey = getTermKey(term, false);
      if (termKey === null) {
        continue; // Term not in mapping, try next
      }
      
      // Direct lookup
      const termData = fieldIndex[termKey];
      if (hasData(termData)) {
        // Add line numbers to candidates (need to expand for excludes check)
        const lineNumbers = this._getAllLineNumbers(termData);
        for (const line of lineNumbers) {
          candidateLines.add(line);
        }
      }
    }
    
    // If no candidates found, return false
    if (candidateLines.size === 0) {
      return false;
    }
    
    // Apply excludes
    for (const excludeTerm of excludes) {
      const excludeKey = getTermKey(excludeTerm, caseInsensitive);
      if (excludeKey === null) continue;
      
      const excludeData = fieldIndex[excludeKey];
      if (!excludeData) continue;
      
      const excludeLines = this._getAllLineNumbers(excludeData);
      for (const line of excludeLines) {
        candidateLines.delete(line);
      }
      
      // Early exit if all candidates excluded
      if (candidateLines.size === 0) {
        return false;
      }
    }
    
    return candidateLines.size > 0;
  }
 
  // Ultra-fast load with minimal conversions
  load(index) {
    // CRITICAL FIX: Check if index is already loaded by looking for actual data, not just empty field structures
    if (this.index && this.index.data) {
      let hasActualData = false
      for (const field in this.index.data) {
        const fieldData = this.index.data[field]
        if (fieldData && Object.keys(fieldData).length > 0) {
          // Check if any field has actual index entries with data
          for (const key in fieldData) {
            const entry = fieldData[key]
            if (entry && ((entry.set && entry.set.size > 0) || (entry.ranges && entry.ranges.length > 0))) {
              hasActualData = true
              break
            }
          }
          if (hasActualData) break
        }
      }
      
      if (hasActualData) {
        if (this.opts.debugMode) {
          console.log('🔍 IndexManager.load: Index already loaded with actual data, skipping')
        }
        return
      }
    }
    
    // CRITICAL FIX: Add comprehensive null/undefined validation
    if (!index || typeof index !== 'object') {
      if (this.opts.debugMode) {
        console.log(`🔍 IndexManager.load: Invalid index data provided (${typeof index}), using defaults`)
      }
      return this._initializeDefaults()
    }

    if (!index.data || typeof index.data !== 'object') {
      if (this.opts.debugMode) {
        console.log(`🔍 IndexManager.load: Invalid index.data provided (${typeof index.data}), using defaults`)
      }
      return this._initializeDefaults()
    }
    
    // CRITICAL FIX: Only log if there are actual fields to load
    if (this.opts.debugMode && Object.keys(index.data).length > 0) {
      console.log(`🔍 IndexManager.load: Loading index with fields: ${Object.keys(index.data).join(', ')}`)
    }
    
    // Create a deep copy to avoid reference issues
    const processedIndex = {
      data: {}
    }
    
    // CRITICAL FIX: Add null/undefined checks for field iteration
    const fields = Object.keys(index.data)
    for(const field of fields) {
      if (!field || typeof field !== 'string') {
        continue // Skip invalid field names
      }

      const fieldData = index.data[field]
      if (!fieldData || typeof fieldData !== 'object') {
        continue // Skip invalid field data
      }

      processedIndex.data[field] = {}
      
      // CRITICAL FIX: Check if this is a term mapping field for conversion
      const isTermMappingField = this.database?.termManager && 
        this.database.termManager.termMappingFields && 
        this.database.termManager.termMappingFields.includes(field)
      
      const terms = Object.keys(fieldData)
      for(const term of terms) {
        if (!term || typeof term !== 'string') {
          continue // Skip invalid term names
        }

        const termData = fieldData[term]
        
        // CRITICAL FIX: Convert term strings to term IDs for term mapping fields
        // If the key is a string term (not a numeric ID), convert it to term ID
        let termKey = term
        if (isTermMappingField && typeof term === 'string' && !/^\d+$/.test(term)) {
          // Key is a term string, convert to term ID
          const termId = this.database?.termManager?.getTermIdWithoutIncrement(term)
          if (termId !== undefined) {
            termKey = String(termId)
          } else {
            // Term not found in termManager - skip this key (orphaned term from old index)
            // This can happen if termMapping wasn't loaded yet or term was removed
            if (this.opts?.debugMode) {
              console.log(`⚠️  IndexManager.load: Term "${term}" not found in termManager for field "${field}" - skipping (orphaned from old index)`)
            }
            continue
          }
        }
        
        // Convert various formats to new hybrid format
        if (Array.isArray(termData)) {
              // Check if it's the new compact format [setArray, rangesArray]
              if (termData.length === 2 && Array.isArray(termData[0]) && Array.isArray(termData[1])) {
                // New compact format: [setArray, rangesArray]
                // Convert ultra-compact ranges [start, count] back to {start, count}
                const ranges = termData[1].map(range => {
                  if (Array.isArray(range) && range.length === 2) {
                    // Ultra-compact format: [start, count]
                    return { start: range[0], count: range[1] }
                  } else {
                    // Legacy format: {start, count}
                    return range
                  }
                })
                processedIndex.data[field][termKey] = {
                  set: new Set(termData[0]),
                  ranges: ranges
                }
              } else {
                // Legacy array format (just set data)
                processedIndex.data[field][termKey] = { set: new Set(termData), ranges: [] }
              }
        } else if (termData && typeof termData === 'object') {
          if (termData.set || termData.ranges) {
            // Legacy hybrid format - convert set array back to Set
            const hybridData = termData
            let setObject
            if (Array.isArray(hybridData.set)) {
              // Convert array back to Set
              setObject = new Set(hybridData.set)
            } else {
              // Fallback to empty Set
              setObject = new Set()
            }
            processedIndex.data[field][termKey] = { 
              set: setObject, 
              ranges: hybridData.ranges || [] 
            }
          } else {
            // Convert from Set format to hybrid
            const numbers = Array.from(termData || [])
            processedIndex.data[field][termKey] = { set: new Set(numbers), ranges: [] }
          }
        }
      }
    }
    
    // Preserve initialized fields if no data was loaded
    if (!processedIndex.data || Object.keys(processedIndex.data).length === 0) {
      // CRITICAL FIX: Only log if debug mode is enabled and there are actual fields
      if (this.opts.debugMode && this.index.data && Object.keys(this.index.data).length > 0) {
        console.log(`🔍 IndexManager.load: No data loaded, preserving initialized fields: ${Object.keys(this.index.data).join(', ')}`)
      }
      // Keep the current index with initialized fields
      return
    }
    
    this.index = processedIndex
  }

  /**
   * CRITICAL FIX: Initialize default index structure when invalid data is provided
   * This prevents TypeError when Object.keys() is called on null/undefined
   */
  _initializeDefaults() {
    if (this.opts.debugMode) {
      console.log(`🔍 IndexManager._initializeDefaults: Initializing default index structure`)
    }
    
    // Initialize empty index structure
    this.index = { data: {} }
    
    // Initialize fields from options if available
    if (this.opts.indexes && typeof this.opts.indexes === 'object') {
      const fields = Object.keys(this.opts.indexes)
      for (const field of fields) {
        if (field && typeof field === 'string') {
          this.index.data[field] = {}
        }
      }
    }
    
    if (this.opts.debugMode) {
      console.log(`🔍 IndexManager._initializeDefaults: Initialized with fields: ${Object.keys(this.index.data).join(', ')}`)
    }
  }
  
  readColumnIndex(column) {
    return new Set((this.index.data && this.index.data[column]) ? Object.keys(this.index.data[column]) : [])
  }

  /**
   * Convert index to JSON-serializable format for debugging and export
   * This resolves the issue where Sets appear as empty objects in JSON.stringify
   */
  toJSON() {
    const serializable = { data: {} }
    
    // Check if this is a term mapping field for conversion
    const isTermMappingField = (field) => {
      return this.database?.termManager && 
        this.database.termManager.termMappingFields && 
        this.database.termManager.termMappingFields.includes(field)
    }
    
    for (const field in this.index.data) {
      serializable.data[field] = {}
      const isTermField = isTermMappingField(field)
      
      for (const term in this.index.data[field]) {
        const hybridData = this.index.data[field][term]
        
        // CRITICAL FIX: Convert term strings to term IDs for term mapping fields
        // If the key is a string term (not a numeric ID), convert it to term ID
        let termKey = term
        if (isTermField && typeof term === 'string' && !/^\d+$/.test(term)) {
          // Key is a term string, convert to term ID
          const termId = this.database?.termManager?.getTermIdWithoutIncrement(term)
          if (termId !== undefined) {
            termKey = String(termId)
          } else {
            // Term not found in termManager, keep original key
            // This prevents data loss when term mapping is incomplete
            termKey = term
            if (this.opts?.debugMode) {
              console.log(`⚠️  IndexManager.toJSON: Term "${term}" not found in termManager for field "${field}" - using original key`)
            }
          }
        }
        
        // OPTIMIZATION: Create ranges before serialization if beneficial
        if (hybridData.set && hybridData.set.size >= this.rangeThreshold) {
          this._optimizeToRanges(hybridData)
        }
        
        // Convert hybrid structure to serializable format
        let setArray = []
        if (hybridData.set) {
          if (typeof hybridData.set.size !== 'undefined') {
            // Regular Set
            setArray = Array.from(hybridData.set)
          }
        }
        
        // Use ultra-compact format: [setArray, rangesArray] to save space
        const ranges = hybridData.ranges || []
        if (ranges.length > 0) {
          // Convert ranges to ultra-compact format: [start, count] instead of {start, count}
          const compactRanges = ranges.map(range => [range.start, range.count])
          serializable.data[field][termKey] = [setArray, compactRanges]
        } else {
          // CRITICAL FIX: Always use the [setArray, []] format for consistency
          // This ensures the load() method can properly deserialize the data
          serializable.data[field][termKey] = [setArray, []]
        }
      }
    }
    
    return serializable
  }

  /**
   * Get a JSON string representation of the index
   * This properly handles Sets unlike the default JSON.stringify
   */
  toString() {
    return JSON.stringify(this.toJSON(), null, 2)
  }

  // Simplified term mapping methods - just basic functionality
  
  /**
   * Rebuild index (stub for compatibility)
   */
  async rebuild() {
    // Stub implementation for compatibility
    return Promise.resolve()
  }
}

