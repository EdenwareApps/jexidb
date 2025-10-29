/**
 * QueryManager - Handles all query operations and strategies
 * 
 * Responsibilities:
 * - find(), findOne(), count(), query()
 * - findWithStreaming(), findWithIndexed()
 * - matchesCriteria(), extractQueryFields()
 * - Query strategies (INDEXED vs STREAMING)
 * - Result estimation
 */

export class QueryManager {
  constructor(database) {
    this.database = database
    this.opts = database.opts
    this.indexManager = database.indexManager
    this.fileHandler = database.fileHandler
    this.serializer = database.serializer
    this.usageStats = database.usageStats || {
      totalQueries: 0,
      indexedQueries: 0,
      streamingQueries: 0,
      indexedAverageTime: 0,
      streamingAverageTime: 0
    }
  }

  /**
   * Main find method with strategy selection
   * @param {Object} criteria - Query criteria
   * @param {Object} options - Query options
   * @returns {Promise<Array>} - Query results
   */
  async find(criteria, options = {}) {
    if (this.database.destroyed) throw new Error('Database is destroyed')
    if (!this.database.initialized) await this.database.init()
    
    // Manual save is now the responsibility of the application
    
    // Preprocess query to handle array field syntax automatically
    const processedCriteria = this.preprocessQuery(criteria)
    
    const finalCriteria = processedCriteria
    
    // Validate strict indexed mode before processing
    if (this.opts.indexedQueryMode === 'strict') {
      this.validateStrictQuery(finalCriteria);
    }    
    
    const startTime = Date.now();
    this.usageStats.totalQueries++;

    try {
      // Decide which strategy to use
      const strategy = this.shouldUseStreaming(finalCriteria, options);
      
      let results = [];
      
      if (strategy === 'streaming') {
        results = await this.findWithStreaming(finalCriteria, options);
        this.usageStats.streamingQueries++;
        this.updateAverageTime('streaming', Date.now() - startTime);
      } else {
        results = await this.findWithIndexed(finalCriteria, options);
        this.usageStats.indexedQueries++;
        this.updateAverageTime('indexed', Date.now() - startTime);
      }

      if (this.opts.debugMode) {
        const time = Date.now() - startTime;
        console.log(`⏱️ Query completed in ${time}ms using ${strategy} strategy`);
        console.log(`📊 Results: ${results.length} records`);
        console.log(`📊 Results type: ${typeof results}, isArray: ${Array.isArray(results)}`);
      }

      return results;

    } catch (error) {
      if (this.opts.debugMode) {
        console.error('❌ Query failed:', error);
      }
      throw error;
    }
  }

  /**
   * Find one record
   * @param {Object} criteria - Query criteria
   * @param {Object} options - Query options
   * @returns {Promise<Object|null>} - First matching record or null
   */
  async findOne(criteria, options = {}) {
    if (this.database.destroyed) throw new Error('Database is destroyed')
    if (!this.database.initialized) await this.database.init()
    // Manual save is now the responsibility of the application
    
    // Preprocess query to handle array field syntax automatically
    const processedCriteria = this.preprocessQuery(criteria)
    
    // Validate strict indexed mode before processing
    if (this.opts.indexedQueryMode === 'strict') {
      this.validateStrictQuery(processedCriteria);
    }
    
    const startTime = Date.now();
    this.usageStats.totalQueries++;

    try {
      // Decide which strategy to use
      const strategy = this.shouldUseStreaming(processedCriteria, options);
      
      let results = [];
      
      if (strategy === 'streaming') {
        results = await this.findWithStreaming(processedCriteria, { ...options, limit: 1 });
        this.usageStats.streamingQueries++;
        this.updateAverageTime('streaming', Date.now() - startTime);
      } else {
        results = await this.findWithIndexed(processedCriteria, { ...options, limit: 1 });
        this.usageStats.indexedQueries++;
        this.updateAverageTime('indexed', Date.now() - startTime);
      }

      if (this.opts.debugMode) {
        const time = Date.now() - startTime;
        console.log(`⏱️ findOne completed in ${time}ms using ${strategy} strategy`);
        console.log(`📊 Results: ${results.length} record(s)`);
      }

      // Return the first result or null if no results found
      return results.length > 0 ? results[0] : null;

    } catch (error) {
      if (this.opts.debugMode) {
        console.error('❌ findOne failed:', error);
      }
      throw error;
    }
  }

  /**
   * Count records matching criteria
   * @param {Object} criteria - Query criteria
   * @param {Object} options - Query options
   * @returns {Promise<number>} - Count of matching records
   */
  async count(criteria, options = {}) {
    if (this.database.destroyed) throw new Error('Database is destroyed')
    if (!this.database.initialized) await this.database.init()
    // Manual save is now the responsibility of the application
    
    // Validate strict indexed mode before processing
    if (this.opts.indexedQueryMode === 'strict') {
      this.validateStrictQuery(criteria);
    }
    
    // Use the same strategy as find method
    const strategy = this.shouldUseStreaming(criteria, options);
    
    let count = 0;
    
    if (strategy === 'streaming') {
      // Use streaming approach for non-indexed fields or large result sets
      const results = await this.findWithStreaming(criteria, options);
      count = results.length;
    } else {
      // Use indexed approach for indexed fields
      const results = await this.findWithIndexed(criteria, options);
      count = results.length;
    }
    
    return count;
  }

  /**
   * Compatibility method that redirects to find
   * @param {Object} criteria - Query criteria
   * @param {Object} options - Query options
   * @returns {Promise<Array>} - Query results
   */
  async query(criteria, options = {}) {
    return this.find(criteria, options)
  }

  /**
   * Find using streaming strategy with pre-filtering optimization
   * @param {Object} criteria - Query criteria
   * @param {Object} options - Query options
   * @returns {Promise<Array>} - Query results
   */
  async findWithStreaming(criteria, options = {}) {
    if (this.opts.debugMode) {
      console.log('🌊 Using streaming strategy');
    }
    
    // OPTIMIZATION: Try to use indices for pre-filtering when possible
    const indexableFields = this._getIndexableFields(criteria);
    if (indexableFields.length > 0) {
      if (this.opts.debugMode) {
        console.log(`🌊 Using pre-filtered streaming with ${indexableFields.length} indexable fields`);
      }
      
      // Use indices to pre-filter and reduce streaming scope
      const preFilteredLines = this.indexManager.query(
        this._extractIndexableCriteria(criteria), 
        options
      );
      
      // Stream only the pre-filtered records
      return this._streamPreFilteredRecords(preFilteredLines, criteria, options);
    }
    
    // Fallback to full streaming
    if (this.opts.debugMode) {
      console.log('🌊 Using full streaming (no indexable fields found)');
    }
    
    return this._streamAllRecords(criteria, options);
  }

  /**
   * Get indexable fields from criteria
   * @param {Object} criteria - Query criteria
   * @returns {Array} - Array of indexable field names
   */
  _getIndexableFields(criteria) {
    const indexableFields = [];
    
    if (!criteria || typeof criteria !== 'object') {
      return indexableFields;
    }
    
    // Handle $and conditions
    if (criteria.$and && Array.isArray(criteria.$and)) {
      for (const andCondition of criteria.$and) {
        indexableFields.push(...this._getIndexableFields(andCondition));
      }
    }
    
    // Handle regular field conditions
    for (const [field, condition] of Object.entries(criteria)) {
      if (field.startsWith('$')) continue; // Skip logical operators
      
      // RegExp conditions cannot be pre-filtered using indices
      if (condition instanceof RegExp) {
        continue;
      }
      
      if (this.indexManager.opts.indexes && this.indexManager.opts.indexes[field]) {
        indexableFields.push(field);
      }
    }
    
    return [...new Set(indexableFields)]; // Remove duplicates
  }

  /**
   * Extract indexable criteria for pre-filtering
   * @param {Object} criteria - Full query criteria
   * @returns {Object} - Criteria with only indexable fields
   */
  _extractIndexableCriteria(criteria) {
    if (!criteria || typeof criteria !== 'object') {
      return {};
    }
    
    const indexableCriteria = {};
    
    // Handle $and conditions
    if (criteria.$and && Array.isArray(criteria.$and)) {
      const indexableAndConditions = criteria.$and
        .map(andCondition => this._extractIndexableCriteria(andCondition))
        .filter(condition => Object.keys(condition).length > 0);
      
      if (indexableAndConditions.length > 0) {
        indexableCriteria.$and = indexableAndConditions;
      }
    }
    
    // Handle regular field conditions
    for (const [field, condition] of Object.entries(criteria)) {
      if (field.startsWith('$')) continue; // Skip logical operators
      
      // RegExp conditions cannot be pre-filtered using indices
      if (condition instanceof RegExp) {
        continue;
      }
      
      if (this.indexManager.opts.indexes && this.indexManager.opts.indexes[field]) {
        indexableCriteria[field] = condition;
      }
    }
    
    return indexableCriteria;
  }

  /**
   * OPTIMIZATION 4: Stream pre-filtered records using line numbers from indices with partial index optimization
   * @param {Set} preFilteredLines - Line numbers from index query
   * @param {Object} criteria - Full query criteria
   * @param {Object} options - Query options
   * @returns {Promise<Array>} - Query results
   */
  async _streamPreFilteredRecords(preFilteredLines, criteria, options = {}) {
    if (preFilteredLines.size === 0) {
      return [];
    }
    
    const results = [];
    const lineNumbers = Array.from(preFilteredLines);
    
    // OPTIMIZATION 4: Sort line numbers for efficient file reading
    lineNumbers.sort((a, b) => a - b);
    
    // OPTIMIZATION 4: Use batch reading for better performance
    const batchSize = Math.min(1000, lineNumbers.length); // Read in batches of 1000
    const batches = [];
    
    for (let i = 0; i < lineNumbers.length; i += batchSize) {
      batches.push(lineNumbers.slice(i, i + batchSize));
    }
    
    for (const batch of batches) {
      // OPTIMIZATION: Use ranges instead of reading entire file
      const ranges = this.database.getRanges(batch);
      const groupedRanges = await this.fileHandler.groupedRanges(ranges);
      
      const fs = await import('fs');
      const fd = await fs.promises.open(this.fileHandler.file, 'r');
      
      try {
        for (const groupedRange of groupedRanges) {
          for await (const row of this.fileHandler.readGroupedRange(groupedRange, fd)) {
            if (row.line && row.line.trim()) {
              try {
                // CRITICAL FIX: Use serializer.deserialize instead of JSON.parse to handle array format
                const record = this.database.serializer.deserialize(row.line);
                
                // OPTIMIZATION 4: Use optimized criteria matching for pre-filtered records
                if (this._matchesCriteriaOptimized(record, criteria, options)) {
                  // SPACE OPTIMIZATION: Restore term IDs to terms for user (unless disabled)
                  const recordWithTerms = options.restoreTerms !== false ? 
                    this.database.restoreTermIdsAfterDeserialization(record) : 
                    record
                  results.push(recordWithTerms);
                  
                  // Check limit
                  if (options.limit && results.length >= options.limit) {
                    return this._applyOrdering(results, options);
                  }
                }
              } catch (error) {
                // Skip invalid lines
                continue;
              }
            }
          }
        }
      } finally {
        await fd.close();
      }
    }
    
    return this._applyOrdering(results, options);
  }

  /**
   * OPTIMIZATION 4: Optimized criteria matching for pre-filtered records
   * @param {Object} record - Record to check
   * @param {Object} criteria - Filter criteria
   * @param {Object} options - Query options
   * @returns {boolean} - True if matches
   */
  _matchesCriteriaOptimized(record, criteria, options = {}) {
    if (!criteria || Object.keys(criteria).length === 0) {
      return true;
    }

    // Handle $not operator at the top level
    if (criteria.$not && typeof criteria.$not === 'object') {
      // For $not conditions, we need to negate the result
      // IMPORTANT: For $not conditions, we should NOT skip pre-filtered fields
      // because we need to evaluate the actual field values to determine exclusion
      
      // Use the regular matchesCriteria method for $not conditions to ensure proper field evaluation
      const notResult = this.matchesCriteria(record, criteria.$not, options);
      return !notResult;
    }

    // OPTIMIZATION 4: Skip indexable fields since they were already pre-filtered
    const indexableFields = this._getIndexableFields(criteria);
    
    // Handle explicit logical operators at the top level
    if (criteria.$or && Array.isArray(criteria.$or)) {
      let orMatches = false;
      for (const orCondition of criteria.$or) {
        if (this._matchesCriteriaOptimized(record, orCondition, options)) {
          orMatches = true;
          break;
        }
      }
      
      if (!orMatches) {
        return false;
      }
    } else if (criteria.$and && Array.isArray(criteria.$and)) {
      for (const andCondition of criteria.$and) {
        if (!this._matchesCriteriaOptimized(record, andCondition, options)) {
          return false;
        }
      }
    }

    // Handle individual field conditions (exclude logical operators and pre-filtered fields)
    for (const [field, condition] of Object.entries(criteria)) {
      if (field.startsWith('$')) continue;
      
      // OPTIMIZATION 4: Skip indexable fields that were already pre-filtered
      if (indexableFields.includes(field)) {
        continue;
      }
      
      if (!this.matchesFieldCondition(record, field, condition, options)) {
        return false;
      }
    }

    if (criteria.$or && Array.isArray(criteria.$or)) {
      return true;
    }

    return true;
  }

  /**
   * OPTIMIZATION 4: Apply ordering to results
   * @param {Array} results - Results to order
   * @param {Object} options - Query options
   * @returns {Array} - Ordered results
   */
  _applyOrdering(results, options) {
    if (options.orderBy) {
      const [field, direction = 'asc'] = options.orderBy.split(' ');
      results.sort((a, b) => {
        if (a[field] > b[field]) return direction === 'asc' ? 1 : -1;
        if (a[field] < b[field]) return direction === 'asc' ? -1 : 1;
        return 0;
      });
    }
    
    return results;
  }

  /**
   * Stream all records (fallback method)
   * @param {Object} criteria - Query criteria
   * @param {Object} options - Query options
   * @returns {Promise<Array>} - Query results
   */
  async _streamAllRecords(criteria, options = {}) {
    const memoryLimit = options.limit || undefined;
    const streamingOptions = { ...options, limit: memoryLimit };
    
    const results = await this.fileHandler.readWithStreaming(criteria, streamingOptions, (record, criteria) => {
      return this.matchesCriteria(record, criteria, options);
    }, this.serializer || null);
    
    // Apply ordering if specified
    if (options.orderBy) {
      const [field, direction = 'asc'] = options.orderBy.split(' ');
      results.sort((a, b) => {
        if (a[field] > b[field]) return direction === 'asc' ? 1 : -1;
        if (a[field] < b[field]) return direction === 'asc' ? -1 : 1;
        return 0;
      });
    }
    
    return results;
  }

  /**
   * Find using indexed search strategy with real streaming
   * @param {Object} criteria - Query criteria
   * @param {Object} options - Query options
   * @returns {Promise<Array>} - Query results
   */
  async findWithIndexed(criteria, options = {}) {
    if (this.opts.debugMode) {
      console.log('📊 Using indexed strategy with real streaming');
    }
    
    let results = []
    const limit = options.limit // No default limit - return all results unless explicitly limited

    // Use IndexManager to get line numbers, then read specific records
    const lineNumbers = this.indexManager.query(criteria, options)
    if (this.opts.debugMode) {
      console.log(`🔍 IndexManager returned ${lineNumbers.size} line numbers:`, Array.from(lineNumbers))
    }
    
    // Read specific records using the line numbers
    if (lineNumbers.size > 0) {
      const lineNumbersArray = Array.from(lineNumbers)
      const ranges = this.database.getRanges(lineNumbersArray)
      const groupedRanges = await this.database.fileHandler.groupedRanges(ranges)
      
      const fs = await import('fs')
      const fd = await fs.promises.open(this.database.fileHandler.file, 'r')
      
      try {
        for (const groupedRange of groupedRanges) {
          for await (const row of this.database.fileHandler.readGroupedRange(groupedRange, fd)) {
            try {
              const record = this.database.serializer.deserialize(row.line)
              const recordWithTerms = options.restoreTerms !== false ? 
                this.database.restoreTermIdsAfterDeserialization(record) : 
                record
              results.push(recordWithTerms)
              if (limit && results.length >= limit) break
            } catch (error) {
              // Skip invalid lines
            }
          }
          if (limit && results.length >= limit) break
        }
      } finally {
        await fd.close()
      }
    }
    
    if (options.orderBy) {
      const [field, direction = 'asc'] = options.orderBy.split(' ')
      results.sort((a, b) => {
        if (a[field] > b[field]) return direction === 'asc' ? 1 : -1
        if (a[field] < b[field]) return direction === 'asc' ? -1 : 1
        return 0;
      })
    }
    return results
  }

  /**
   * Check if a record matches criteria
   * @param {Object} record - Record to check
   * @param {Object} criteria - Filter criteria
   * @param {Object} options - Query options (for caseInsensitive, etc.)
   * @returns {boolean} - True if matches
   */
  matchesCriteria(record, criteria, options = {}) {
    
    if (!criteria || Object.keys(criteria).length === 0) {
      return true;
    }

    // Handle explicit logical operators at the top level
    if (criteria.$or && Array.isArray(criteria.$or)) {
      let orMatches = false;
      for (const orCondition of criteria.$or) {
        if (this.matchesCriteria(record, orCondition, options)) {
          orMatches = true;
          break;
        }
      }
      
      // If $or doesn't match, return false immediately
      if (!orMatches) {
        return false;
      }
      
      // If $or matches, continue to check other conditions if they exist
      // Don't return true yet - we need to check other conditions
    } else if (criteria.$and && Array.isArray(criteria.$and)) {
      for (const andCondition of criteria.$and) {
        if (!this.matchesCriteria(record, andCondition, options)) {
          return false;
        }
      }
      // $and matches, continue to check other conditions if they exist
    }

    // Handle individual field conditions and $not operator
    for (const [field, condition] of Object.entries(criteria)) {
      // Skip logical operators that are handled above
      if (field.startsWith('$') && field !== '$not') {
        continue;
      }
      
      if (field === '$not') {
        // Handle $not operator - it should negate the result of its condition
        if (typeof condition === 'object' && condition !== null) {
          // Empty $not condition should not exclude anything
          if (Object.keys(condition).length === 0) {
            continue; // Don't exclude anything
          }
          
          // Check if the $not condition matches - if it does, this record should be excluded
          if (this.matchesCriteria(record, condition, options)) {
            return false; // Exclude this record
          }
        }
      } else {
        // Handle regular field conditions
        if (!this.matchesFieldCondition(record, field, condition, options)) {
          return false;
        }
      }
    }

    // If we have $or conditions and they matched, return true
    if (criteria.$or && Array.isArray(criteria.$or)) {
      return true;
    }

    // For other cases (no $or, or $and, or just field conditions), return true if we got this far
    return true;
  }

  /**
   * Check if a field matches a condition
   * @param {Object} record - Record to check
   * @param {string} field - Field name
   * @param {*} condition - Condition to match
   * @param {Object} options - Query options
   * @returns {boolean} - True if matches
   */
  matchesFieldCondition(record, field, condition, options = {}) {
    const value = record[field];

    // Debug logging for all field conditions
    if (this.database.opts.debugMode) {
      console.log(`🔍 Checking field '${field}':`, { value, condition, record: record.name || record.id });
    }

    // Debug logging for term mapping fields
    if (this.database.opts.termMapping && Object.keys(this.database.opts.indexes || {}).includes(field)) {
      if (this.database.opts.debugMode) {
        console.log(`🔍 Checking term mapping field '${field}':`, { value, condition, record: record.name || record.id });
      }
    }

    // Handle null/undefined values
    if (value === null || value === undefined) {
      return condition === null || condition === undefined;
    }

    // Handle regex conditions (MUST come before object check since RegExp is an object)
    if (condition instanceof RegExp) {
      // For array fields, test regex against each element
      if (Array.isArray(value)) {
        return value.some(element => condition.test(String(element)));
      }
      // For non-array fields, test regex against the value directly
      return condition.test(String(value));
    }

    // Handle array conditions
    if (Array.isArray(condition)) {
      // For array fields, check if any element in the field matches any element in the condition
      if (Array.isArray(value)) {
        return condition.some(condVal => value.includes(condVal));
      }
      // For non-array fields, check if value is in condition
      return condition.includes(value);
    }

    // Handle object conditions (operators)
    if (typeof condition === 'object' && !Array.isArray(condition)) {
      for (const [operator, operatorValue] of Object.entries(condition)) {
        if (!this.matchesOperator(value, operator, operatorValue, options)) {
          return false;
        }
      }
      return true;
    }

    // Handle case-insensitive string comparison
    if (options.caseInsensitive && typeof value === 'string' && typeof condition === 'string') {
      return value.toLowerCase() === condition.toLowerCase();
    }

    // Handle direct array field search (e.g., { nameTerms: 'channel' })
    if (Array.isArray(value) && typeof condition === 'string') {
      return value.includes(condition);
    }

    // Simple equality
    return value === condition;
  }

  /**
   * Check if a value matches an operator condition
   * @param {*} value - Value to check
   * @param {string} operator - Operator
   * @param {*} operatorValue - Operator value
   * @param {Object} options - Query options
   * @returns {boolean} - True if matches
   */
  matchesOperator(value, operator, operatorValue, options = {}) {
    switch (operator) {
      case '$gt':
        return value > operatorValue;
      case '$gte':
        return value >= operatorValue;
      case '$lt':
        return value < operatorValue;
      case '$lte':
        return value <= operatorValue;
      case '$ne':
        return value !== operatorValue;
      case '$not':
        // $not operator should be handled at the criteria level, not field level
        // This is a fallback for backward compatibility
        return value !== operatorValue;
      case '$in':
        if (Array.isArray(value)) {
          // For array fields, check if any element in the array matches any value in operatorValue
          return Array.isArray(operatorValue) && operatorValue.some(opVal => value.includes(opVal));
        } else {
          // For non-array fields, check if value is in operatorValue
          return Array.isArray(operatorValue) && operatorValue.includes(value);
        }
      case '$nin':
        if (Array.isArray(value)) {
          // For array fields, check if NO elements in the array match any value in operatorValue
          return Array.isArray(operatorValue) && !operatorValue.some(opVal => value.includes(opVal));
        } else {
          // For non-array fields, check if value is not in operatorValue
          return Array.isArray(operatorValue) && !operatorValue.includes(value);
        }
      case '$regex':
        const regex = new RegExp(operatorValue, options.caseInsensitive ? 'i' : '');
        // For array fields, test regex against each element
        if (Array.isArray(value)) {
          return value.some(element => regex.test(String(element)));
        }
        // For non-array fields, test regex against the value directly
        return regex.test(String(value));
      case '$contains':
        if (Array.isArray(value)) {
          return value.includes(operatorValue);
        }
        return String(value).includes(String(operatorValue));
      case '$all':
        if (!Array.isArray(value) || !Array.isArray(operatorValue)) {
          return false;
        }
        return operatorValue.every(item => value.includes(item));
      case '$exists':
        return operatorValue ? (value !== undefined && value !== null) : (value === undefined || value === null);
      case '$size':
        if (Array.isArray(value)) {
          return value.length === operatorValue;
        }
        return false;
      default:
        return false;
    }
  }

  /**
   * Preprocess query to handle array field syntax automatically
   * @param {Object} criteria - Query criteria
   * @returns {Object} - Processed criteria
   */
  preprocessQuery(criteria) {
    if (!criteria || typeof criteria !== 'object') {
      return criteria
    }

    const processed = {}
    
    for (const [field, value] of Object.entries(criteria)) {
      // Check if this is a term mapping field
      const isTermMappingField = this.database.opts.termMapping && 
        this.database.termManager && 
        this.database.termManager.termMappingFields && 
        this.database.termManager.termMappingFields.includes(field)
      
      if (isTermMappingField) {
        // Handle term mapping field queries
        if (typeof value === 'string') {
          // Convert term to $in query for term mapping fields
          processed[field] = { $in: [value] }
        } else if (Array.isArray(value)) {
          // Convert array to $in query
          processed[field] = { $in: value }
        } else if (value && typeof value === 'object') {
          // Handle special query operators for term mapping
          if (value.$in) {
            processed[field] = { $in: value.$in }
          } else if (value.$all) {
            processed[field] = { $all: value.$all }
          } else {
            processed[field] = value
          }
        } else {
          // Invalid value for term mapping field
          throw new Error(`Invalid query for array field '${field}'. Use { $in: [value] } syntax or direct value.`)
        }
        
        if (this.database.opts.debugMode) {
          console.log(`🔍 Processed term mapping query for field '${field}':`, processed[field])
        }
      } else {
        // Check if this field is defined as an array in the schema
        const indexes = this.opts.indexes || {}
        const fieldConfig = indexes[field]
        const isArrayField = fieldConfig && 
          (Array.isArray(fieldConfig) && fieldConfig.includes('array') || 
           fieldConfig === 'array:string' ||
           fieldConfig === 'array:number' ||
           fieldConfig === 'array:boolean')
        
        if (isArrayField) {
          // Handle array field queries
          if (typeof value === 'string' || typeof value === 'number') {
            // Convert direct value to $in query for array fields
            processed[field] = { $in: [value] }
          } else if (Array.isArray(value)) {
            // Convert array to $in query
            processed[field] = { $in: value }
          } else if (value && typeof value === 'object') {
            // Already properly formatted query object
            processed[field] = value
          } else {
            // Invalid value for array field
            throw new Error(`Invalid query for array field '${field}'. Use { $in: [value] } syntax or direct value.`)
          }
        } else {
          // Non-array field, keep as is
          processed[field] = value
        }
      }
    }
    
    return processed
  }

  /**
   * Determine which query strategy to use
   * @param {Object} criteria - Query criteria
   * @param {Object} options - Query options
   * @returns {string} - 'streaming' or 'indexed'
   */
  shouldUseStreaming(criteria, options = {}) {
    const { limit } = options; // No default limit
    const totalRecords = this.database.length || 0;

    // Strategy 1: Always streaming for queries without criteria
    if (!criteria || Object.keys(criteria).length === 0) {
      if (this.opts.debugMode) {
        console.log('📊 QueryStrategy: STREAMING - No criteria provided');
      }
      return 'streaming';
    }

    // Strategy 2: Check if all fields are indexed and support the operators used
    // First, check if $not is present at root level - if so, we need to use streaming for proper $not handling
    if (criteria.$not && !this.opts.termMapping) {
      if (this.opts.debugMode) {
        console.log('📊 QueryStrategy: STREAMING - $not operator requires streaming mode');
      }
      return 'streaming';
    }
    
    // OPTIMIZATION: For term mapping, we can process $not using indices
    if (criteria.$not && this.opts.termMapping) {
      // Check if all $not fields are indexed
      const notFields = Object.keys(criteria.$not)
      const allNotFieldsIndexed = notFields.every(field => 
        this.indexManager.opts.indexes && this.indexManager.opts.indexes[field]
      )
      
      if (allNotFieldsIndexed) {
        if (this.opts.debugMode) {
          console.log('📊 QueryStrategy: INDEXED - $not with term mapping can use indexed strategy');
        }
        // Continue to check other conditions instead of forcing streaming
      } else {
        if (this.opts.debugMode) {
          console.log('📊 QueryStrategy: STREAMING - $not fields not all indexed');
        }
        return 'streaming';
      }
    }
    
    // Handle $and queries - check if all conditions in $and are indexable
    if (criteria.$and && Array.isArray(criteria.$and)) {
      const allAndConditionsIndexed = criteria.$and.every(andCondition => {
        // Handle $not conditions within $and
        if (andCondition.$not) {
          const notFields = Object.keys(andCondition.$not);
          return notFields.every(field => {
            if (!this.indexManager.opts.indexes || !this.indexManager.opts.indexes[field]) {
              return false;
            }
            // For term mapping, $not can be processed with indices
            return this.opts.termMapping && Object.keys(this.opts.indexes || {}).includes(field);
          });
        }
        
        // Handle regular field conditions
        return Object.keys(andCondition).every(field => {
          if (!this.indexManager.opts.indexes || !this.indexManager.opts.indexes[field]) {
            return false;
          }
          
          const condition = andCondition[field];
          
          // RegExp cannot be efficiently queried using indices - must use streaming
          if (condition instanceof RegExp) {
            return false;
          }
          
          if (typeof condition === 'object' && !Array.isArray(condition)) {
            const operators = Object.keys(condition);
            
            if (this.opts.termMapping && Object.keys(this.opts.indexes || {}).includes(field)) {
              return operators.every(op => {
                return !['$gt', '$gte', '$lt', '$lte', '$ne', '$regex', '$contains', '$exists', '$size'].includes(op);
              });
            } else {
              return operators.every(op => {
                return !['$all', '$in', '$gt', '$gte', '$lt', '$lte', '$ne', '$not', '$regex', '$contains', '$exists', '$size'].includes(op);
              });
            }
          }
          return true;
        });
      });
      
      if (!allAndConditionsIndexed) {
        if (this.opts.debugMode) {
          console.log('📊 QueryStrategy: STREAMING - Some $and conditions not indexed or operators not supported');
        }
        return 'streaming';
      }
    }
    
    const allFieldsIndexed = Object.keys(criteria).every(field => {
      // Skip $and as it's handled separately above
      if (field === '$and') return true;
      
      if (!this.opts.indexes || !this.opts.indexes[field]) {
        if (this.opts.debugMode) {
          console.log(`🔍 Field '${field}' not indexed. Available indexes:`, Object.keys(this.opts.indexes || {}))
        }
        return false;
      }
      
      // Check if the field uses operators that are supported by IndexManager
      const condition = criteria[field];
      
      // RegExp cannot be efficiently queried using indices - must use streaming
      if (condition instanceof RegExp) {
        if (this.opts.debugMode) {
          console.log(`🔍 Field '${field}' uses RegExp - requires streaming strategy`)
        }
        return false;
      }
      
      if (typeof condition === 'object' && !Array.isArray(condition)) {
        const operators = Object.keys(condition);
        if (this.opts.debugMode) {
          console.log(`🔍 Field '${field}' has operators:`, operators)
        }
        
        // With term mapping enabled, we can support complex operators via partial reads
        if (this.opts.termMapping && Object.keys(this.opts.indexes || {}).includes(field)) {
          // Term mapping fields can use complex operators with partial reads
          return operators.every(op => {
            // Support $in, $nin, $all, $not for term mapping fields (converted to multiple equality checks)
            return !['$gt', '$gte', '$lt', '$lte', '$ne', '$regex', '$contains', '$exists', '$size'].includes(op);
          });
        } else {
          // Non-term-mapping fields only support simple equality operations
          return operators.every(op => {
            return !['$all', '$in', '$gt', '$gte', '$lt', '$lte', '$ne', '$not', '$regex', '$contains', '$exists', '$size'].includes(op);
          });
        }
      }
      return true;
    });

    if (!allFieldsIndexed) {
      if (this.opts.debugMode) {
        console.log('📊 QueryStrategy: STREAMING - Some fields not indexed or operators not supported');
      }
      return 'streaming';
    }

    // OPTIMIZATION 2: Hybrid strategy - use pre-filtered streaming when index is empty
    const indexData = this.indexManager.index.data || {};
    const hasIndexData = Object.keys(indexData).length > 0;
    if (!hasIndexData) {
      // Check if we can use pre-filtered streaming with term mapping
      if (this.opts.termMapping && this._canUsePreFilteredStreaming(criteria)) {
        if (this.opts.debugMode) {
          console.log('📊 QueryStrategy: HYBRID - Using pre-filtered streaming with term mapping');
        }
        return 'streaming'; // Will use pre-filtered streaming in findWithStreaming
      }
      
      if (this.opts.debugMode) {
        console.log('📊 QueryStrategy: STREAMING - Index is empty and no pre-filtering available');
      }
      return 'streaming';
    }

    // Strategy 3: Streaming if limit is very high (only if database has records)
    if (totalRecords > 0 && limit > totalRecords * this.opts.streamingThreshold) {
      if (this.opts.debugMode) {
        console.log(`📊 QueryStrategy: STREAMING - High limit (${limit} > ${Math.round(totalRecords * this.opts.streamingThreshold)})`);
      }
      return 'streaming';
    }

    // Strategy 4: Use indexed strategy when all fields are indexed and streamingThreshold is respected
    if (this.opts.debugMode) {
      console.log(`📊 QueryStrategy: INDEXED - All fields indexed, using indexed strategy`);
    }
    return 'indexed';
  }

  /**
   * Estimate number of results for a query
   * @param {Object} criteria - Query criteria
   * @param {number} totalRecords - Total records in database
   * @returns {number} - Estimated results
   */
  estimateQueryResults(criteria, totalRecords) {
    // If database is empty, return 0
    if (totalRecords === 0) {
      if (this.opts.debugMode) {
        console.log(`📊 Estimation: Database empty → 0 results`);
      }
      return 0;
    }

    let minResults = Infinity;

    for (const [field, condition] of Object.entries(criteria)) {
      // Check if field is indexed
      if (!this.indexManager.opts.indexes || !this.indexManager.opts.indexes[field]) {
        // Non-indexed field - assume it could match any record
        if (this.opts.debugMode) {
          console.log(`📊 Estimation: ${field} = non-indexed → ~${totalRecords} results`);
        }
        return totalRecords;
      }

      const fieldIndex = this.indexManager.index.data[field];
      
      if (!fieldIndex) {
        // Non-indexed field - assume it could match any record
        if (this.opts.debugMode) {
          console.log(`📊 Estimation: ${field} = non-indexed → ~${totalRecords} results`);
        }
        return totalRecords;
      }

      let fieldEstimate = 0;

      if (typeof condition === 'object' && !Array.isArray(condition)) {
        // Handle different types of operators
        for (const [operator, value] of Object.entries(condition)) {
          if (operator === '$all') {
            // Special handling for $all operator
            fieldEstimate = this.estimateAllOperator(fieldIndex, value);
          } else if (['$gt', '$gte', '$lt', '$lte', '$in', '$regex'].includes(operator)) {
            // Numeric and other operators
            fieldEstimate = this.estimateOperatorResults(fieldIndex, operator, value, totalRecords);
          } else {
            // Unknown operator, assume it could match any record
            fieldEstimate = totalRecords;
          }
        }
      } else {
        // Simple equality
        const recordIds = fieldIndex[condition];
        fieldEstimate = recordIds ? recordIds.length : 0;
      }

      if (this.opts.debugMode) {
        console.log(`📊 Estimation: ${field} = ${fieldEstimate} results`);
      }

      minResults = Math.min(minResults, fieldEstimate);
    }

    return minResults === Infinity ? 0 : minResults;
  }

  /**
   * Estimate results for $all operator
   * @param {Object} fieldIndex - Field index
   * @param {Array} values - Values to match
   * @returns {number} - Estimated results
   */
  estimateAllOperator(fieldIndex, values) {
    if (!Array.isArray(values) || values.length === 0) {
      return 0;
    }

    let minCount = Infinity;
    for (const value of values) {
      const recordIds = fieldIndex[value];
      const count = recordIds ? recordIds.length : 0;
      minCount = Math.min(minCount, count);
    }

    return minCount === Infinity ? 0 : minCount;
  }

  /**
   * Estimate results for operators
   * @param {Object} fieldIndex - Field index
   * @param {string} operator - Operator
   * @param {*} value - Value
   * @param {number} totalRecords - Total records
   * @returns {number} - Estimated results
   */
  estimateOperatorResults(fieldIndex, operator, value, totalRecords) {
    // This is a simplified estimation - in practice, you might want more sophisticated logic
    switch (operator) {
      case '$in':
        if (Array.isArray(value)) {
          let total = 0;
          for (const v of value) {
            const recordIds = fieldIndex[v];
            if (recordIds) total += recordIds.length;
          }
          return total;
        }
        break;
      case '$gt':
      case '$gte':
      case '$lt':
      case '$lte':
        // For range queries, estimate based on data distribution
        // This is a simplified approach - real implementation would be more sophisticated
        return Math.floor(totalRecords * 0.1); // Assume 10% of records match
      case '$regex':
        // Regex is hard to estimate without scanning
        return Math.floor(totalRecords * 0.05); // Assume 5% of records match
    }
    return 0;
  }

  /**
   * Validate strict query mode
   * @param {Object} criteria - Query criteria
   */
  validateStrictQuery(criteria) {
    if (!criteria || Object.keys(criteria).length === 0) {
      return; // Empty criteria are always allowed
    }

    // Handle logical operators at the top level
    if (criteria.$not) {
      this.validateStrictQuery(criteria.$not);
      return;
    }

    if (criteria.$or && Array.isArray(criteria.$or)) {
      for (const orCondition of criteria.$or) {
        this.validateStrictQuery(orCondition);
      }
      return;
    }

    if (criteria.$and && Array.isArray(criteria.$and)) {
      for (const andCondition of criteria.$and) {
        this.validateStrictQuery(andCondition);
      }
      return;
    }

    // Get available indexed fields
    const indexedFields = Object.keys(this.indexManager.opts.indexes || {});
    const availableFields = indexedFields.length > 0 ? indexedFields.join(', ') : 'none';

    // Check each field
    const nonIndexedFields = [];
    for (const [field, condition] of Object.entries(criteria)) {
      // Skip logical operators
      if (field.startsWith('$')) {
        continue;
      }

      // Check if field is indexed
      if (!this.indexManager.opts.indexes || !this.indexManager.opts.indexes[field]) {
        nonIndexedFields.push(field);
      }

      // Check if condition uses supported operators
      if (typeof condition === 'object' && !Array.isArray(condition)) {
        const operators = Object.keys(condition);
        for (const op of operators) {
          if (!['$in', '$nin', '$contains', '$all', '>', '>=', '<', '<=', '!=', 'contains', 'regex'].includes(op)) {
            throw new Error(`Operator '${op}' is not supported in strict mode for field '${field}'.`);
          }
        }
      }
    }

    // Generate appropriate error message
    if (nonIndexedFields.length > 0) {
      if (nonIndexedFields.length === 1) {
        throw new Error(`Strict indexed mode: Field '${nonIndexedFields[0]}' is not indexed. Available indexed fields: ${availableFields}`);
      } else {
        throw new Error(`Strict indexed mode: Fields '${nonIndexedFields.join("', '")}' are not indexed. Available indexed fields: ${availableFields}`);
      }
    }
  }

  /**
   * Update average time for performance tracking
   * @param {string} type - Type of operation ('streaming' or 'indexed')
   * @param {number} time - Time taken
   */
  updateAverageTime(type, time) {
    if (!this.usageStats[`${type}AverageTime`]) {
      this.usageStats[`${type}AverageTime`] = 0;
    }
    
    const currentAverage = this.usageStats[`${type}AverageTime`];
    const count = this.usageStats[`${type}Queries`] || 1;
    
    // Calculate running average
    this.usageStats[`${type}AverageTime`] = (currentAverage * (count - 1) + time) / count;
  }

  /**
   * OPTIMIZATION 2: Check if we can use pre-filtered streaming with term mapping
   * @param {Object} criteria - Query criteria
   * @returns {boolean} - True if pre-filtered streaming can be used
   */
  _canUsePreFilteredStreaming(criteria) {
    if (!criteria || typeof criteria !== 'object') {
      return false;
    }

    // Check if we have term mapping fields in the query
    const termMappingFields = Object.keys(this.opts.indexes || {});
    const queryFields = Object.keys(criteria).filter(field => !field.startsWith('$'));
    
    // Check if any query field is a term mapping field
    const hasTermMappingFields = queryFields.some(field => termMappingFields.includes(field));
    
    if (!hasTermMappingFields) {
      return false;
    }

    // Check if the query is simple enough for pre-filtering
    // Simple equality queries on term mapping fields work well with pre-filtering
    for (const [field, condition] of Object.entries(criteria)) {
      if (field.startsWith('$')) continue;
      
      if (termMappingFields.includes(field)) {
        // For term mapping fields, simple equality or $in queries work well
        if (typeof condition === 'string' || 
            (typeof condition === 'object' && condition.$in && Array.isArray(condition.$in))) {
          return true;
        }
      }
    }

    return false;
  }

  // Simplified term mapping - handled in TermManager
}
