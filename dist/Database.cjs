'use strict';

Object.defineProperty(exports, '__esModule', { value: true });

var events = require('events');
var asyncMutex = require('async-mutex');
var fs = require('fs');
var readline = require('readline');
var path = require('path');

function _OverloadYield(e, d) {
  this.v = e, this.k = d;
}
function _asyncIterator(r) {
  var n,
    t,
    o,
    e = 2;
  for ("undefined" != typeof Symbol && (t = Symbol.asyncIterator, o = Symbol.iterator); e--;) {
    if (t && null != (n = r[t])) return n.call(r);
    if (o && null != (n = r[o])) return new AsyncFromSyncIterator(n.call(r));
    t = "@@asyncIterator", o = "@@iterator";
  }
  throw new TypeError("Object is not async iterable");
}
function AsyncFromSyncIterator(r) {
  function AsyncFromSyncIteratorContinuation(r) {
    if (Object(r) !== r) return Promise.reject(new TypeError(r + " is not an object."));
    var n = r.done;
    return Promise.resolve(r.value).then(function (r) {
      return {
        value: r,
        done: n
      };
    });
  }
  return AsyncFromSyncIterator = function (r) {
    this.s = r, this.n = r.next;
  }, AsyncFromSyncIterator.prototype = {
    s: null,
    n: null,
    next: function () {
      return AsyncFromSyncIteratorContinuation(this.n.apply(this.s, arguments));
    },
    return: function (r) {
      var n = this.s.return;
      return void 0 === n ? Promise.resolve({
        value: r,
        done: true
      }) : AsyncFromSyncIteratorContinuation(n.apply(this.s, arguments));
    },
    throw: function (r) {
      var n = this.s.return;
      return void 0 === n ? Promise.reject(r) : AsyncFromSyncIteratorContinuation(n.apply(this.s, arguments));
    }
  }, new AsyncFromSyncIterator(r);
}
function _awaitAsyncGenerator(e) {
  return new _OverloadYield(e, 0);
}
function _wrapAsyncGenerator(e) {
  return function () {
    return new AsyncGenerator(e.apply(this, arguments));
  };
}
function AsyncGenerator(e) {
  var r, t;
  function resume(r, t) {
    try {
      var n = e[r](t),
        o = n.value,
        u = o instanceof _OverloadYield;
      Promise.resolve(u ? o.v : o).then(function (t) {
        if (u) {
          var i = "return" === r ? "return" : "next";
          if (!o.k || t.done) return resume(i, t);
          t = e[i](t).value;
        }
        settle(n.done ? "return" : "normal", t);
      }, function (e) {
        resume("throw", e);
      });
    } catch (e) {
      settle("throw", e);
    }
  }
  function settle(e, n) {
    switch (e) {
      case "return":
        r.resolve({
          value: n,
          done: true
        });
        break;
      case "throw":
        r.reject(n);
        break;
      default:
        r.resolve({
          value: n,
          done: false
        });
    }
    (r = r.next) ? resume(r.key, r.arg) : t = null;
  }
  this._invoke = function (e, n) {
    return new Promise(function (o, u) {
      var i = {
        key: e,
        arg: n,
        resolve: o,
        reject: u,
        next: null
      };
      t ? t = t.next = i : (r = t = i, resume(e, n));
    });
  }, "function" != typeof e.return && (this.return = void 0);
}
AsyncGenerator.prototype["function" == typeof Symbol && Symbol.asyncIterator || "@@asyncIterator"] = function () {
  return this;
}, AsyncGenerator.prototype.next = function (e) {
  return this._invoke("next", e);
}, AsyncGenerator.prototype.throw = function (e) {
  return this._invoke("throw", e);
}, AsyncGenerator.prototype.return = function (e) {
  return this._invoke("return", e);
};

const aliasToCanonical = {
  '>': '$gt',
  '>=': '$gte',
  '<': '$lt',
  '<=': '$lte',
  '!=': '$ne',
  '=': '$eq',
  '==': '$eq',
  eq: '$eq',
  equals: '$eq',
  in: '$in',
  nin: '$nin',
  regex: '$regex',
  contains: '$contains',
  all: '$all',
  exists: '$exists',
  size: '$size',
  not: '$not'
};
const canonicalToLegacy = {
  '$gt': '>',
  '$gte': '>=',
  '$lt': '<',
  '$lte': '<=',
  '$ne': '!=',
  '$eq': '=',
  '$contains': 'contains',
  '$regex': 'regex'
};

/**
 * Normalize an operator to its canonical Mongo-style representation (prefixed with $)
 * @param {string} operator
 * @returns {string}
 */
function normalizeOperator(operator) {
  if (typeof operator !== 'string') {
    return operator;
  }
  if (operator.startsWith('$')) {
    return operator;
  }
  if (aliasToCanonical[operator] !== undefined) {
    return aliasToCanonical[operator];
  }
  const lowerCase = operator.toLowerCase();
  if (aliasToCanonical[lowerCase] !== undefined) {
    return aliasToCanonical[lowerCase];
  }
  return operator;
}

/**
 * Convert an operator to its legacy (non-prefixed) alias when available
 * @param {string} operator
 * @returns {string}
 */
function operatorToLegacy(operator) {
  if (typeof operator !== 'string') {
    return operator;
  }
  const canonical = normalizeOperator(operator);
  if (canonicalToLegacy[canonical]) {
    return canonicalToLegacy[canonical];
  }
  return operator;
}

/**
 * Normalize operator keys in a criteria object
 * @param {Object} criteriaValue
 * @param {Object} options
 * @param {'canonical'|'legacy'} options.target - Preferred operator style
 * @param {boolean} [options.preserveOriginal=false] - Whether to keep the original keys alongside normalized ones
 * @returns {Object}
 */
function normalizeCriteriaOperators(criteriaValue, {
  target = 'canonical',
  preserveOriginal = false
} = {}) {
  if (!criteriaValue || typeof criteriaValue !== 'object' || Array.isArray(criteriaValue)) {
    return criteriaValue;
  }
  const normalized = preserveOriginal ? {
    ...criteriaValue
  } : {};
  for (const [operator, value] of Object.entries(criteriaValue)) {
    const canonical = normalizeOperator(operator);
    if (target === 'canonical') {
      normalized[canonical] = value;
      if (preserveOriginal && canonical !== operator) {
        normalized[operator] = value;
      }
    } else if (target === 'legacy') {
      const legacy = operatorToLegacy(operator);
      normalized[legacy] = value;
      if (preserveOriginal) {
        if (legacy !== canonical) {
          normalized[canonical] = value;
        }
        if (operator !== legacy && operator !== canonical) {
          normalized[operator] = value;
        }
      }
    }
  }
  return normalized;
}

class IndexManager {
  constructor(opts, databaseMutex = null, database = null) {
    this.opts = Object.assign({}, opts);
    this.index = Object.assign({
      data: {}
    }, this.opts.index);
    this.totalLines = 0;
    this.rangeThreshold = 10; // Sensible threshold: 10+ consecutive numbers justify ranges
    this.binarySearchThreshold = 32; // Much higher for better performance
    this.database = database; // Reference to database for term manager access

    // CRITICAL: Use database mutex to prevent deadlocks
    // If no database mutex provided, create a local one (for backward compatibility)
    this.mutex = databaseMutex || new asyncMutex.Mutex();
    this.indexedFields = [];
    this.setIndexesConfig(this.opts.indexes);
  }
  setTotalLines(total) {
    this.totalLines = total;
  }

  /**
   * Update indexes configuration and ensure internal structures stay in sync
   * @param {Object|Array<string>} indexes
   */
  setIndexesConfig(indexes) {
    if (!indexes) {
      this.opts.indexes = undefined;
      this.indexedFields = [];
      return;
    }
    if (Array.isArray(indexes)) {
      const fields = indexes.map(field => String(field));
      this.indexedFields = fields;
      const normalizedConfig = {};
      for (const field of fields) {
        const existingConfig = !Array.isArray(this.opts.indexes) && typeof this.opts.indexes === 'object' ? this.opts.indexes[field] : undefined;
        normalizedConfig[field] = existingConfig ?? 'auto';
        if (!this.index.data[field]) {
          this.index.data[field] = {};
        }
      }
      this.opts.indexes = normalizedConfig;
      return;
    }
    if (typeof indexes === 'object') {
      this.opts.indexes = Object.assign({}, indexes);
      this.indexedFields = Object.keys(this.opts.indexes);
      for (const field of this.indexedFields) {
        if (!this.index.data[field]) {
          this.index.data[field] = {};
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
    if (!field) return false;
    if (!Array.isArray(this.indexedFields)) {
      return false;
    }
    return this.indexedFields.includes(field);
  }

  /**
   * Determine whether the index has usable data for a given field
   * @param {string} field - Field name
   * @returns {boolean}
   */
  hasUsableIndexData(field) {
    if (!field) return false;
    const fieldData = this.index?.data?.[field];
    if (!fieldData || typeof fieldData !== 'object') {
      return false;
    }
    for (const key in fieldData) {
      if (!Object.prototype.hasOwnProperty.call(fieldData, key)) continue;
      const entry = fieldData[key];
      if (!entry) continue;
      if (entry.set && typeof entry.set.size === 'number' && entry.set.size > 0) {
        return true;
      }
      if (Array.isArray(entry.ranges) && entry.ranges.length > 0) {
        const hasRangeData = entry.ranges.some(range => {
          if (range === null || typeof range === 'undefined') {
            return false;
          }
          if (typeof range === 'object') {
            const count = typeof range.count === 'number' ? range.count : 0;
            return count > 0;
          }
          // When ranges are stored as individual numbers
          return true;
        });
        if (hasRangeData) {
          return true;
        }
      }
    }
    return false;
  }

  // Ultra-fast range conversion - only for very large datasets
  _toRanges(numbers) {
    if (numbers.length === 0) return [];
    if (numbers.length < this.rangeThreshold) return numbers; // Keep as-is for small arrays

    const sorted = numbers.sort((a, b) => a - b); // Sort in-place
    const ranges = [];
    let start = sorted[0];
    let count = 1;
    for (let i = 1; i < sorted.length; i++) {
      if (sorted[i] === sorted[i - 1] + 1) {
        count++;
      } else {
        // End of consecutive sequence
        if (count >= this.rangeThreshold) {
          ranges.push({
            start,
            count
          });
        } else {
          // Add individual numbers for small sequences
          for (let j = start; j < start + count; j++) {
            ranges.push(j);
          }
        }
        start = sorted[i];
        count = 1;
      }
    }

    // Handle last sequence
    if (count >= this.rangeThreshold) {
      ranges.push({
        start,
        count
      });
    } else {
      for (let j = start; j < start + count; j++) {
        ranges.push(j);
      }
    }
    return ranges;
  }

  // Ultra-fast range expansion
  _fromRanges(ranges) {
    if (!ranges || ranges.length === 0) return [];
    const numbers = [];
    for (const item of ranges) {
      if (typeof item === 'object' && item.start !== undefined) {
        // It's a range - use direct loop for maximum speed
        const end = item.start + item.count;
        for (let i = item.start; i < end; i++) {
          numbers.push(i);
        }
      } else {
        // It's an individual number
        numbers.push(item);
      }
    }
    return numbers;
  }

  // Ultra-fast lookup - optimized for Set operations
  _hasLineNumber(hybridData, lineNumber) {
    if (!hybridData) return false;

    // Check in Set first (O(1)) - most common case
    if (hybridData.set && hybridData.set.has(lineNumber)) {
      return true;
    }

    // Check in ranges only if necessary
    if (hybridData.ranges && hybridData.ranges.length > 0) {
      return this._searchInRanges(hybridData.ranges, lineNumber);
    }
    return false;
  }

  // Optimized search strategy
  _searchInRanges(ranges, lineNumber) {
    if (ranges.length < this.binarySearchThreshold) {
      // Linear search for small ranges
      return this._linearSearchRanges(ranges, lineNumber);
    } else {
      // Binary search for large ranges
      return this._binarySearchRanges(ranges, lineNumber);
    }
  }

  // Ultra-fast linear search
  _linearSearchRanges(ranges, lineNumber) {
    for (const item of ranges) {
      if (typeof item === 'object' && item.start !== undefined) {
        // It's a range
        if (lineNumber >= item.start && lineNumber < item.start + item.count) {
          return true;
        }
      } else if (item === lineNumber) {
        // It's an individual number
        return true;
      }
    }
    return false;
  }

  // Optimized binary search
  _binarySearchRanges(ranges, lineNumber) {
    let left = 0;
    let right = ranges.length - 1;
    while (left <= right) {
      const mid = Math.floor((left + right) / 2);
      const range = ranges[mid];
      if (typeof range === 'object' && range.start !== undefined) {
        // It's a range
        if (lineNumber >= range.start && lineNumber < range.start + range.count) {
          return true;
        } else if (lineNumber < range.start) {
          right = mid - 1;
        } else {
          left = mid + 1;
        }
      } else {
        // It's an individual number
        if (range === lineNumber) {
          return true;
        } else if (range < lineNumber) {
          left = mid + 1;
        } else {
          right = mid - 1;
        }
      }
    }
    return false;
  }

  // Ultra-fast add operation - minimal overhead
  _addLineNumber(hybridData, lineNumber) {
    // Initialize structure if needed
    if (!hybridData) {
      hybridData = {
        set: new Set(),
        ranges: []
      };
    }

    // Add to Set directly (fastest path)
    if (!hybridData.set) {
      hybridData.set = new Set();
    }
    hybridData.set.add(lineNumber);

    // Optimize to ranges when Set gets reasonably large
    if (hybridData.set.size >= this.rangeThreshold * 2) {
      // 20 elements
      if (this.opts.debugMode) {
        console.log(`🔧 Triggering range optimization: Set size ${hybridData.set.size} >= threshold ${this.rangeThreshold * 2}`);
      }
      this._optimizeToRanges(hybridData);
    }
    return hybridData;
  }

  // Ultra-fast remove operation
  _removeLineNumber(hybridData, lineNumber) {
    if (!hybridData) {
      return hybridData;
    }

    // Remove from Set (fast path)
    if (hybridData.set) {
      hybridData.set.delete(lineNumber);
    }

    // Remove from ranges (less common)
    if (hybridData.ranges) {
      hybridData.ranges = this._removeFromRanges(hybridData.ranges, lineNumber);
    }
    return hybridData;
  }

  // Optimized range removal
  _removeFromRanges(ranges, lineNumber) {
    if (!ranges || ranges.length === 0) return ranges;
    const newRanges = [];
    for (const item of ranges) {
      if (typeof item === 'object' && item.start !== undefined) {
        // It's a range
        if (lineNumber >= item.start && lineNumber < item.start + item.count) {
          // Split range if needed
          if (lineNumber === item.start) {
            // Remove first element
            if (item.count > 1) {
              newRanges.push({
                start: item.start + 1,
                count: item.count - 1
              });
            }
          } else if (lineNumber === item.start + item.count - 1) {
            // Remove last element
            if (item.count > 1) {
              newRanges.push({
                start: item.start,
                count: item.count - 1
              });
            }
          } else {
            // Remove from middle - split into two ranges
            const beforeCount = lineNumber - item.start;
            const afterCount = item.count - beforeCount - 1;
            if (beforeCount >= this.rangeThreshold) {
              newRanges.push({
                start: item.start,
                count: beforeCount
              });
            } else {
              // Add individual numbers for small sequences
              for (let i = item.start; i < lineNumber; i++) {
                newRanges.push(i);
              }
            }
            if (afterCount >= this.rangeThreshold) {
              newRanges.push({
                start: lineNumber + 1,
                count: afterCount
              });
            } else {
              // Add individual numbers for small sequences
              for (let i = lineNumber + 1; i < item.start + item.count; i++) {
                newRanges.push(i);
              }
            }
          }
        } else {
          newRanges.push(item);
        }
      } else if (item !== lineNumber) {
        // It's an individual number
        newRanges.push(item);
      }
    }
    return newRanges;
  }

  // Ultra-lazy range conversion - only when absolutely necessary
  _optimizeToRanges(hybridData) {
    if (!hybridData.set || hybridData.set.size === 0) {
      return;
    }
    if (this.opts.debugMode) {
      console.log(`🔧 Starting range optimization for Set with ${hybridData.set.size} elements`);
    }

    // Only convert if we have enough data to make it worthwhile
    if (hybridData.set.size < this.rangeThreshold) {
      return;
    }

    // Convert Set to array and find consecutive sequences
    const numbers = Array.from(hybridData.set).sort((a, b) => a - b);
    const ranges = [];
    let start = numbers[0];
    let count = 1;
    for (let i = 1; i < numbers.length; i++) {
      if (numbers[i] === numbers[i - 1] + 1) {
        count++;
      } else {
        // End of consecutive sequence
        if (count >= this.rangeThreshold) {
          ranges.push({
            start,
            count
          });
          // Remove these numbers from Set
          for (let j = start; j < start + count; j++) {
            hybridData.set.delete(j);
          }
        }
        start = numbers[i];
        count = 1;
      }
    }

    // Handle last sequence
    if (count >= this.rangeThreshold) {
      ranges.push({
        start,
        count
      });
      for (let j = start; j < start + count; j++) {
        hybridData.set.delete(j);
      }
    }

    // Add new ranges to existing ranges
    if (ranges.length > 0) {
      if (!hybridData.ranges) {
        hybridData.ranges = [];
      }
      hybridData.ranges.push(...ranges);
      // Keep ranges sorted for efficient binary search
      hybridData.ranges.sort((a, b) => {
        const aStart = typeof a === 'object' ? a.start : a;
        const bStart = typeof b === 'object' ? b.start : b;
        return aStart - bStart;
      });
    }
  }

  // Ultra-fast get all line numbers
  _getAllLineNumbers(hybridData) {
    if (!hybridData) return [];

    // Use generator for lazy evaluation and better memory efficiency
    return Array.from(this._getAllLineNumbersGenerator(hybridData));
  }

  // OPTIMIZATION: Generator-based approach for better memory efficiency
  *_getAllLineNumbersGenerator(hybridData) {
    const normalizeLineNumber = value => {
      if (typeof value === 'number') {
        return value;
      }
      if (typeof value === 'string') {
        const parsed = Number(value);
        return Number.isNaN(parsed) ? value : parsed;
      }
      if (typeof value === 'bigint') {
        const maxSafe = BigInt(Number.MAX_SAFE_INTEGER);
        return value <= maxSafe ? Number(value) : value;
      }
      return value;
    };

    // Yield from Set (fastest path)
    if (hybridData.set) {
      for (const num of hybridData.set) {
        yield normalizeLineNumber(num);
      }
    }

    // Yield from ranges (optimized)
    if (hybridData.ranges) {
      for (const item of hybridData.ranges) {
        if (typeof item === 'object' && item.start !== undefined) {
          // It's a range - use direct loop for better performance
          const end = item.start + item.count;
          for (let i = item.start; i < end; i++) {
            yield normalizeLineNumber(i);
          }
        } else {
          // It's an individual number
          yield normalizeLineNumber(item);
        }
      }
    }
  }

  // OPTIMIZATION 6: Ultra-fast add operation with incremental index updates
  async add(row, lineNumber) {
    if (typeof row !== 'object' || !row) {
      throw new Error('Invalid \'row\' parameter, it must be an object');
    }
    if (typeof lineNumber !== 'number') {
      throw new Error('Invalid line number');
    }

    // OPTIMIZATION 6: Use direct field access with minimal operations
    const data = this.index.data;

    // OPTIMIZATION 6: Pre-allocate field structures for better performance
    const fields = Object.keys(this.opts.indexes || {});
    for (const field of fields) {
      // PERFORMANCE: Check if this is a term mapping field once
      const isTermMappingField = this.database?.termManager && this.database.termManager.termMappingFields && this.database.termManager.termMappingFields.includes(field);

      // CRITICAL FIX: For term mapping fields, prefer ${field}Ids if available
      // Records processed by processTermMapping have term IDs in ${field}Ids
      // Records loaded from file have term IDs directly in ${field} (after restoreTermIdsAfterDeserialization)
      let value;
      if (isTermMappingField) {
        const termIdsField = `${field}Ids`;
        const termIds = row[termIdsField];
        if (termIds && Array.isArray(termIds) && termIds.length > 0) {
          // Use term IDs from ${field}Ids (preferred - from processTermMapping)
          value = termIds;
        } else {
          // Fallback: use field directly (for records loaded from file that have term IDs in field)
          value = row[field];
        }
      } else {
        value = row[field];
      }
      if (value !== undefined && value !== null) {
        // OPTIMIZATION 6: Initialize field structure if it doesn't exist
        if (!data[field]) {
          data[field] = {};
        }
        const values = Array.isArray(value) ? value : [value];
        for (const val of values) {
          let key;
          if (isTermMappingField && typeof val === 'number') {
            // For term mapping fields, values are already term IDs
            key = String(val);
          } else if (isTermMappingField && typeof val === 'string') {
            // Fallback: convert string to term ID
            // CRITICAL: During indexing (add), we should use getTermId() to create IDs if needed
            // This is different from queries where we use getTermIdWithoutIncrement() to avoid creating new IDs
            const termId = this.database.termManager.getTermId(val);
            key = String(termId);
          } else {
            // For non-term-mapping fields (including array:number), use values directly
            key = String(val);
          }

          // OPTIMIZATION 6: Use direct assignment for better performance
          if (!data[field][key]) {
            data[field][key] = {
              set: new Set(),
              ranges: []
            };
          }

          // OPTIMIZATION 6: Direct Set operation - fastest possible
          data[field][key].set.add(lineNumber);

          // OPTIMIZATION 6: Lazy range optimization - only when beneficial
          if (data[field][key].set.size >= this.rangeThreshold * 3) {
            this._optimizeToRanges(data[field][key]);
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
    if (!records || !records.length) return;

    // OPTIMIZATION 6: Pre-allocate index structures for better performance
    const data = this.index.data;
    const fields = Object.keys(this.opts.indexes || {});
    for (const field of fields) {
      if (!data[field]) {
        data[field] = {};
      }
    }

    // OPTIMIZATION 6: Use Map for batch processing to reduce lookups
    const fieldUpdates = new Map();

    // OPTIMIZATION 6: Process all records in batch with optimized data structures
    for (let i = 0; i < records.length; i++) {
      const row = records[i];
      const lineNumber = startLineNumber + i;
      for (const field of fields) {
        // PERFORMANCE: Check if this is a term mapping field once
        const isTermMappingField = this.database?.termManager && this.database.termManager.termMappingFields && this.database.termManager.termMappingFields.includes(field);

        // CRITICAL FIX: For term mapping fields, prefer ${field}Ids if available
        // Records processed by processTermMapping have term IDs in ${field}Ids
        // Records loaded from file have term IDs directly in ${field} (after restoreTermIdsAfterDeserialization)
        let value;
        if (isTermMappingField) {
          const termIdsField = `${field}Ids`;
          const termIds = row[termIdsField];
          if (termIds && Array.isArray(termIds) && termIds.length > 0) {
            // Use term IDs from ${field}Ids (preferred - from processTermMapping)
            value = termIds;
          } else {
            // Fallback: use field directly (for records loaded from file that have term IDs in field)
            value = row[field];
          }
        } else {
          value = row[field];
        }
        if (value !== undefined && value !== null) {
          const values = Array.isArray(value) ? value : [value];
          for (const val of values) {
            let key;
            if (isTermMappingField && typeof val === 'number') {
              // For term mapping fields, values are already term IDs
              key = String(val);
            } else if (isTermMappingField && typeof val === 'string') {
              // Fallback: convert string to term ID
              // CRITICAL: During indexing (addBatch), we should use getTermId() to create IDs if needed
              // This is different from queries where we use getTermIdWithoutIncrement() to avoid creating new IDs
              const termId = this.database.termManager.getTermId(val);
              key = String(termId);
            } else {
              // For non-term-mapping fields (including array:number), use values directly
              key = String(val);
            }

            // OPTIMIZATION 6: Use Map for efficient batch updates
            if (!fieldUpdates.has(field)) {
              fieldUpdates.set(field, new Map());
            }
            const fieldMap = fieldUpdates.get(field);
            if (!fieldMap.has(key)) {
              fieldMap.set(key, new Set());
            }
            fieldMap.get(key).add(lineNumber);
          }
        }
      }
    }

    // OPTIMIZATION 6: Apply all updates in batch for better performance
    for (const [field, fieldMap] of fieldUpdates) {
      for (const [key, lineNumbers] of fieldMap) {
        if (!data[field][key]) {
          data[field][key] = {
            set: new Set(),
            ranges: []
          };
        }

        // OPTIMIZATION 6: Add all line numbers at once
        for (const lineNumber of lineNumbers) {
          data[field][key].set.add(lineNumber);
        }

        // OPTIMIZATION 6: Lazy range optimization - only when beneficial
        if (data[field][key].set.size >= this.rangeThreshold * 3) {
          this._optimizeToRanges(data[field][key]);
        }
      }
    }
  }

  // Ultra-fast dry remove
  dryRemove(ln) {
    const data = this.index.data;
    for (const field in data) {
      for (const value in data[field]) {
        // Direct Set operation - fastest possible
        if (data[field][value].set) {
          data[field][value].set.delete(ln);
        }
        if (data[field][value].ranges) {
          data[field][value].ranges = this._removeFromRanges(data[field][value].ranges, ln);
        }
        // Remove empty entries
        if ((!data[field][value].set || data[field][value].set.size === 0) && (!data[field][value].ranges || data[field][value].ranges.length === 0)) {
          delete data[field][value];
        }
      }
    }
  }

  // Cleanup method to free memory
  cleanup() {
    const data = this.index.data;
    for (const field in data) {
      for (const value in data[field]) {
        if (data[field][value].set) {
          if (typeof data[field][value].set.clearAll === 'function') {
            data[field][value].set.clearAll();
          } else if (typeof data[field][value].set.clear === 'function') {
            data[field][value].set.clear();
          }
        }
        if (data[field][value].ranges) {
          data[field][value].ranges.length = 0;
        }
      }
      // Clear the entire field
      data[field] = {};
    }
    // Clear all data
    this.index.data = {};
    this.totalLines = 0;
  }

  // Clear all indexes
  clear() {
    this.index.data = {};
    this.totalLines = 0;
  }

  // Update a record in the index
  async update(oldRecord, newRecord, lineNumber = null) {
    if (!oldRecord || !newRecord) return;

    // Remove old record by ID
    await this.remove(oldRecord);

    // Add new record with provided line number or use hash of the ID
    const actualLineNumber = lineNumber !== null ? lineNumber : this._getIdAsNumber(newRecord.id);
    await this.add(newRecord, actualLineNumber);
  }

  // Convert string ID to number for line number
  _getIdAsNumber(id) {
    if (typeof id === 'number') return id;
    if (typeof id === 'string') {
      // Simple hash function to convert string to number
      let hash = 0;
      for (let i = 0; i < id.length; i++) {
        const char = id.charCodeAt(i);
        hash = (hash << 5) - hash + char;
        hash = hash & hash; // Convert to 32-bit integer
      }
      return Math.abs(hash);
    }
    return 0;
  }

  // Remove a record from the index
  async remove(record) {
    if (!record) return;

    // If record is an array of line numbers, use the original method
    if (Array.isArray(record)) {
      return this._removeLineNumbers(record);
    }

    // If record is an object, remove by record data
    if (typeof record === 'object' && record.id) {
      return await this._removeRecord(record);
    }
  }

  // Remove a specific record from the index
  async _removeRecord(record) {
    if (!record) return;
    const data = this.index.data;
    const database = this.database;
    const persistedCount = Array.isArray(database?.offsets) ? database.offsets.length : 0;
    const lineMatchCache = new Map();
    const doesLineNumberBelongToRecord = async lineNumber => {
      if (lineMatchCache.has(lineNumber)) {
        return lineMatchCache.get(lineNumber);
      }
      let belongs = false;
      try {
        if (lineNumber >= persistedCount) {
          const writeBufferIndex = lineNumber - persistedCount;
          const candidate = database?.writeBuffer?.[writeBufferIndex];
          belongs = !!candidate && candidate.id === record.id;
        } else if (lineNumber >= 0) {
          const range = database?.locate?.(lineNumber);
          if (range && database.fileHandler && database.serializer) {
            const [start, end] = range;
            const buffer = await database.fileHandler.readRange(start, end);
            if (buffer && buffer.length > 0) {
              let line = buffer.toString('utf8');
              if (line) {
                line = line.trim();
                if (line.length > 0) {
                  const storedRecord = database.serializer.deserialize(line);
                  belongs = storedRecord && storedRecord.id === record.id;
                }
              }
            }
          }
        }
      } catch (error) {
        belongs = false;
      }
      lineMatchCache.set(lineNumber, belongs);
      return belongs;
    };
    for (const field in data) {
      if (record[field] !== undefined && record[field] !== null) {
        const values = Array.isArray(record[field]) ? record[field] : [record[field]];
        for (const val of values) {
          let key;

          // Check if this is a term mapping field (array:string fields only)
          const isTermMappingField = this.database?.termManager && this.database.termManager.termMappingFields && this.database.termManager.termMappingFields.includes(field);
          if (isTermMappingField && typeof val === 'number') {
            // For term mapping fields (array:string), the values are already term IDs
            key = String(val);
            if (this.database.opts.debugMode) {
              console.log(`🔍 IndexManager._removeRecord: Using term ID ${val} directly for field "${field}"`);
            }
          } else if (isTermMappingField && typeof val === 'string') {
            // For term mapping fields (array:string), convert string to term ID
            const termId = this.database.termManager.getTermIdWithoutIncrement(val);
            key = String(termId);
            if (this.database.opts.debugMode) {
              console.log(`🔍 IndexManager._removeRecord: Using term ID ${termId} for term "${val}"`);
            }
          } else {
            // For non-term-mapping fields (including array:number), use values directly
            key = String(val);
            if (this.database?.opts?.debugMode) {
              console.log(`🔍 IndexManager._removeRecord: Using value "${val}" directly for field "${field}"`);
            }
          }

          // Note: TermManager notification is handled by Database.mjs
          // to avoid double decrementation during updates

          const indexEntry = data[field][key];
          if (indexEntry) {
            const lineNumbers = this._getAllLineNumbers(indexEntry);
            const filteredLineNumbers = [];
            for (const lineNumber of lineNumbers) {
              if (!(await doesLineNumberBelongToRecord(lineNumber))) {
                filteredLineNumbers.push(lineNumber);
              }
            }
            if (filteredLineNumbers.length === 0) {
              delete data[field][key];
            } else {
              // Rebuild the index value with filtered line numbers
              data[field][key].set = new Set(filteredLineNumbers);
              data[field][key].ranges = [];
            }
          }
        }
      }
    }
  }

  // Ultra-fast remove with batch processing (renamed from remove)
  _removeLineNumbers(lineNumbers) {
    if (!lineNumbers || lineNumbers.length === 0) return;
    lineNumbers.sort((a, b) => a - b); // Sort ascending for efficient processing

    const data = this.index.data;
    for (const field in data) {
      for (const value in data[field]) {
        const numbers = this._getAllLineNumbers(data[field][value]);
        const newNumbers = [];
        for (const ln of numbers) {
          let offset = 0;
          for (const lineNumber of lineNumbers) {
            if (lineNumber < ln) {
              offset++;
            } else if (lineNumber === ln) {
              offset = -1; // Mark for removal
              break;
            }
          }
          if (offset >= 0) {
            newNumbers.push(ln - offset); // Update the value
          }
        }
        if (newNumbers.length > 0) {
          // Rebuild hybrid structure with new numbers
          data[field][value] = {
            set: new Set(),
            ranges: []
          };
          for (const num of newNumbers) {
            data[field][value] = this._addLineNumber(data[field][value], num);
          }
        } else {
          delete data[field][value];
        }
      }
    }
  }

  // Ultra-fast replace with batch processing
  replace(map) {
    if (!map || map.size === 0) return;
    const data = this.index.data;
    for (const field in data) {
      for (const value in data[field]) {
        const numbers = this._getAllLineNumbers(data[field][value]);
        const newNumbers = [];
        for (const lineNumber of numbers) {
          if (map.has(lineNumber)) {
            newNumbers.push(map.get(lineNumber));
          } else {
            newNumbers.push(lineNumber);
          }
        }

        // Rebuild hybrid structure with new numbers
        data[field][value] = {
          set: new Set(),
          ranges: []
        };
        for (const num of newNumbers) {
          data[field][value] = this._addLineNumber(data[field][value], num);
        }
      }
    }
  }

  // Ultra-fast query with early exit and smart processing
  query(criteria, options = {}) {
    if (typeof options === 'boolean') {
      options = {
        matchAny: options
      };
    }
    const {
      matchAny = false,
      caseInsensitive = false
    } = options;
    if (!criteria) {
      // Return all line numbers when no criteria provided
      return new Set(Array.from({
        length: this.totalLines || 0
      }, (_, i) => i));
    }

    // Handle $not operator
    if (criteria.$not && typeof criteria.$not === 'object') {
      // Get all possible line numbers from database offsets or totalLines
      const totalRecords = this.database?.offsets?.length || this.totalLines || 0;
      const allLines = new Set(Array.from({
        length: totalRecords
      }, (_, i) => i));

      // Get line numbers matching the $not condition
      const notLines = this.query(criteria.$not, options);

      // Return complement (all lines except those matching $not condition)
      const result = new Set([...allLines].filter(x => !notLines.has(x)));

      // If there are other conditions besides $not, we need to intersect with them
      const otherCriteria = {
        ...criteria
      };
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
        const conditionResults = criteria.$and.map(andCondition => this.query(andCondition, options));

        // Intersect all results for AND logic
        let result = conditionResults[0];
        for (let i = 1; i < conditionResults.length; i++) {
          result = new Set([...result].filter(x => conditionResults[i].has(x)));
        }

        // IMPORTANT: Check if there are other fields besides $and at the root level
        // If so, we need to intersect with them too
        const otherCriteria = {
          ...criteria
        };
        delete otherCriteria.$and;
        if (Object.keys(otherCriteria).length > 0) {
          const otherResults = this.query(otherCriteria, options);
          result = new Set([...result].filter(x => otherResults.has(x)));
        }
        return result || new Set();
      } else {
        // Single condition - check for other criteria at root level
        const andResult = this.query(criteria.$and[0], options);
        const otherCriteria = {
          ...criteria
        };
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
      return new Set(Array.from({
        length: this.totalLines || 0
      }, (_, i) => i));
    }
    let matchingLines = matchAny ? new Set() : null;
    const data = this.index.data;
    for (const field of fields) {
      // Skip logical operators - they are handled separately
      if (field.startsWith('$')) continue;
      if (typeof data[field] === 'undefined') continue;
      const originalCriteriaValue = criteria[field];
      const criteriaValue = normalizeCriteriaOperators(originalCriteriaValue, {
        target: 'legacy',
        preserveOriginal: true
      });
      let lineNumbersForField = new Set();
      const isNumericField = this.opts.indexes[field] === 'number';

      // Handle RegExp values directly (MUST check before object check since RegExp is an object)
      if (criteriaValue instanceof RegExp) {
        // RegExp cannot be efficiently queried using indices - fall back to streaming
        // This will be handled by the QueryManager's streaming strategy
        continue;
      }
      if (typeof criteriaValue === 'object' && !Array.isArray(criteriaValue) && criteriaValue !== null) {
        const fieldIndex = data[field];

        // Handle $in operator for array queries
        if (criteriaValue.$in !== undefined && criteriaValue.$in !== null) {
          const inValues = Array.isArray(criteriaValue.$in) ? criteriaValue.$in : [criteriaValue.$in];

          // PERFORMANCE: Cache term mapping field check once
          const isTermMappingField = this.database?.termManager && this.database.termManager.termMappingFields && this.database.termManager.termMappingFields.includes(field);
          for (const inValue of inValues) {
            // SPACE OPTIMIZATION: Convert search term to term ID for lookup
            let searchTermId;
            if (isTermMappingField && typeof inValue === 'number') {
              // For term mapping fields (array:string), the search value is already a term ID
              searchTermId = String(inValue);
            } else if (isTermMappingField && typeof inValue === 'string') {
              // For term mapping fields (array:string), convert string to term ID
              const termId = this.database?.termManager?.getTermIdWithoutIncrement(String(inValue));
              if (termId === undefined) {
                // Term not found in termManager - skip this search value
                // This means the term was never saved to the database
                if (this.opts?.debugMode) {
                  console.log(`⚠️  Term "${inValue}" not found in termManager for field "${field}" - skipping`);
                }
                continue; // Skip this value, no matches possible
              }
              searchTermId = String(termId);
            } else {
              // For non-term-mapping fields (including array:number), use values directly
              searchTermId = String(inValue);
            }
            if (caseInsensitive && typeof inValue === 'string') {
              const searchLower = searchTermId.toLowerCase();
              for (const value in fieldIndex) {
                if (value.toLowerCase() === searchLower) {
                  const numbers = this._getAllLineNumbers(fieldIndex[value]);
                  for (const lineNumber of numbers) {
                    lineNumbersForField.add(lineNumber);
                  }
                }
              }
            } else {
              const indexData = fieldIndex[searchTermId];
              if (indexData) {
                const numbers = this._getAllLineNumbers(indexData);
                for (const lineNumber of numbers) {
                  lineNumbersForField.add(lineNumber);
                }
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
          const allLines = new Set(Array.from({
            length: totalRecords
          }, (_, i) => i));

          // Get line numbers that match any of the $nin values
          const matchingLines = new Set();

          // PERFORMANCE: Cache term mapping field check once
          const isTermMappingField = this.database?.termManager && this.database.termManager.termMappingFields && this.database.termManager.termMappingFields.includes(field);
          for (const ninValue of ninValues) {
            // SPACE OPTIMIZATION: Convert search term to term ID for lookup
            let searchTermId;
            if (isTermMappingField && typeof ninValue === 'number') {
              // For term mapping fields (array:string), the search value is already a term ID
              searchTermId = String(ninValue);
            } else if (isTermMappingField && typeof ninValue === 'string') {
              // For term mapping fields (array:string), convert string to term ID
              const termId = this.database?.termManager?.getTermIdWithoutIncrement(String(ninValue));
              if (termId === undefined) {
                // Term not found - skip this value (can't exclude what doesn't exist)
                if (this.opts?.debugMode) {
                  console.log(`⚠️  Term "${ninValue}" not found in termManager for field "${field}" - skipping`);
                }
                continue;
              }
              searchTermId = String(termId);
            } else {
              // For non-term-mapping fields (including array:number), use values directly
              searchTermId = String(ninValue);
            }

            // PERFORMANCE: Direct lookup instead of iteration
            if (caseInsensitive && typeof ninValue === 'string') {
              const searchLower = searchTermId.toLowerCase();
              for (const value in fieldIndex) {
                if (value.toLowerCase() === searchLower) {
                  const numbers = this._getAllLineNumbers(fieldIndex[value]);
                  for (const lineNumber of numbers) {
                    matchingLines.add(lineNumber);
                  }
                }
              }
            } else {
              const indexData = fieldIndex[searchTermId];
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
          const isTermMappingField = this.database?.termManager && this.database.termManager.termMappingFields && this.database.termManager.termMappingFields.includes(field);
          const normalizeValue = value => {
            if (isTermMappingField) {
              if (typeof value === 'number') {
                return String(value);
              }
              if (typeof value === 'string') {
                const termId = this.database?.termManager?.getTermIdWithoutIncrement(value);
                if (termId !== undefined) {
                  return String(termId);
                }
                return null;
              }
              return null;
            }
            return String(value);
          };
          const normalizedValues = [];
          for (const value of allValues) {
            const normalized = normalizeValue(value);
            if (normalized === null) {
              // Term not found in term manager, no matches possible
              return lineNumbersForField;
            }
            normalizedValues.push(normalized);
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
                  const excludeValues = Array.isArray(criteriaValue['!=']) ? criteriaValue['!='] : [criteriaValue['!=']];
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
                    const flags = criteriaValue['regex'].flags.includes('i') ? criteriaValue['regex'].flags : criteriaValue['regex'].flags + 'i';
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
                const excludeValues = Array.isArray(criteriaValue['!=']) ? criteriaValue['!='] : [criteriaValue['!=']];
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
          let searchTermId;

          // PERFORMANCE: Cache term mapping field check once per field
          const isTermMappingField = this.database?.termManager && this.database.termManager.termMappingFields && this.database.termManager.termMappingFields.includes(field);
          if (isTermMappingField && typeof searchValue === 'number') {
            // For term mapping fields (array:string), the search value is already a term ID
            searchTermId = String(searchValue);
          } else if (isTermMappingField && typeof searchValue === 'string') {
            // For term mapping fields (array:string), convert string to term ID
            const termId = this.database?.termManager?.getTermIdWithoutIncrement(String(searchValue));
            if (termId === undefined) {
              // Term not found - skip this value
              if (this.opts?.debugMode) {
                console.log(`⚠️  Term "${searchValue}" not found in termManager for field "${field}" - skipping`);
              }
              continue; // Skip this value, no matches possible
            }
            searchTermId = String(termId);
          } else {
            // For non-term-mapping fields (including array:number), use values directly
            searchTermId = String(searchValue);
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
    const {
      $all = false,
      caseInsensitive = false,
      excludes = []
    } = options;
    const hasExcludes = Array.isArray(excludes) && excludes.length > 0;
    const isTermMappingField = this.database?.termManager && this.database.termManager.termMappingFields && this.database.termManager.termMappingFields.includes(fieldName);

    // Helper: check if termData has any line numbers (ULTRA LIGHT - no expansion)
    const hasData = termData => {
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
      let hasActualData = false;
      for (const field in this.index.data) {
        const fieldData = this.index.data[field];
        if (fieldData && Object.keys(fieldData).length > 0) {
          // Check if any field has actual index entries with data
          for (const key in fieldData) {
            const entry = fieldData[key];
            if (entry && (entry.set && entry.set.size > 0 || entry.ranges && entry.ranges.length > 0)) {
              hasActualData = true;
              break;
            }
          }
          if (hasActualData) break;
        }
      }
      if (hasActualData) {
        if (this.opts.debugMode) {
          console.log('🔍 IndexManager.load: Index already loaded with actual data, skipping');
        }
        return;
      }
    }

    // CRITICAL FIX: Add comprehensive null/undefined validation
    if (!index || typeof index !== 'object') {
      if (this.opts.debugMode) {
        console.log(`🔍 IndexManager.load: Invalid index data provided (${typeof index}), using defaults`);
      }
      return this._initializeDefaults();
    }
    if (!index.data || typeof index.data !== 'object') {
      if (this.opts.debugMode) {
        console.log(`🔍 IndexManager.load: Invalid index.data provided (${typeof index.data}), using defaults`);
      }
      return this._initializeDefaults();
    }

    // CRITICAL FIX: Only log if there are actual fields to load
    if (this.opts.debugMode && Object.keys(index.data).length > 0) {
      console.log(`🔍 IndexManager.load: Loading index with fields: ${Object.keys(index.data).join(', ')}`);
    }

    // Create a deep copy to avoid reference issues
    const processedIndex = {
      data: {}
    };

    // CRITICAL FIX: Add null/undefined checks for field iteration
    const fields = Object.keys(index.data);
    for (const field of fields) {
      if (!field || typeof field !== 'string') {
        continue; // Skip invalid field names
      }
      const fieldData = index.data[field];
      if (!fieldData || typeof fieldData !== 'object') {
        continue; // Skip invalid field data
      }
      processedIndex.data[field] = {};

      // CRITICAL FIX: Check if this is a term mapping field for conversion
      const isTermMappingField = this.database?.termManager && this.database.termManager.termMappingFields && this.database.termManager.termMappingFields.includes(field);
      const terms = Object.keys(fieldData);
      for (const term of terms) {
        if (!term || typeof term !== 'string') {
          continue; // Skip invalid term names
        }
        const termData = fieldData[term];

        // CRITICAL FIX: Convert term strings to term IDs for term mapping fields
        // If the key is a string term (not a numeric ID), convert it to term ID
        let termKey = term;
        if (isTermMappingField && typeof term === 'string' && !/^\d+$/.test(term)) {
          // Key is a term string, convert to term ID
          const termId = this.database?.termManager?.getTermIdWithoutIncrement(term);
          if (termId !== undefined) {
            termKey = String(termId);
          } else {
            // Term not found in termManager - skip this key (orphaned term from old index)
            // This can happen if termMapping wasn't loaded yet or term was removed
            if (this.opts?.debugMode) {
              console.log(`⚠️  IndexManager.load: Term "${term}" not found in termManager for field "${field}" - skipping (orphaned from old index)`);
            }
            continue;
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
                return {
                  start: range[0],
                  count: range[1]
                };
              } else {
                // Legacy format: {start, count}
                return range;
              }
            });
            processedIndex.data[field][termKey] = {
              set: new Set(termData[0]),
              ranges: ranges
            };
          } else {
            // Legacy array format (just set data)
            processedIndex.data[field][termKey] = {
              set: new Set(termData),
              ranges: []
            };
          }
        } else if (termData && typeof termData === 'object') {
          if (termData.set || termData.ranges) {
            // Legacy hybrid format - convert set array back to Set
            const hybridData = termData;
            let setObject;
            if (Array.isArray(hybridData.set)) {
              // Convert array back to Set
              setObject = new Set(hybridData.set);
            } else {
              // Fallback to empty Set
              setObject = new Set();
            }
            processedIndex.data[field][termKey] = {
              set: setObject,
              ranges: hybridData.ranges || []
            };
          } else {
            // Convert from Set format to hybrid
            const numbers = Array.from(termData || []);
            processedIndex.data[field][termKey] = {
              set: new Set(numbers),
              ranges: []
            };
          }
        }
      }
    }

    // Preserve initialized fields if no data was loaded
    if (!processedIndex.data || Object.keys(processedIndex.data).length === 0) {
      // CRITICAL FIX: Only log if debug mode is enabled and there are actual fields
      if (this.opts.debugMode && this.index.data && Object.keys(this.index.data).length > 0) {
        console.log(`🔍 IndexManager.load: No data loaded, preserving initialized fields: ${Object.keys(this.index.data).join(', ')}`);
      }
      // Keep the current index with initialized fields
      return;
    }

    // Restore totalLines from saved data
    if (index.totalLines !== undefined) {
      this.totalLines = index.totalLines;
      if (this.opts.debugMode) {
        console.log(`🔍 IndexManager.load: Restored totalLines=${this.totalLines}`);
      }
    }
    this.index = processedIndex;
  }

  /**
   * CRITICAL FIX: Initialize default index structure when invalid data is provided
   * This prevents TypeError when Object.keys() is called on null/undefined
   */
  _initializeDefaults() {
    if (this.opts.debugMode) {
      console.log(`🔍 IndexManager._initializeDefaults: Initializing default index structure`);
    }

    // Initialize empty index structure
    this.index = {
      data: {}
    };

    // Initialize fields from options if available
    if (this.opts.indexes && typeof this.opts.indexes === 'object') {
      const fields = Object.keys(this.opts.indexes);
      for (const field of fields) {
        if (field && typeof field === 'string') {
          this.index.data[field] = {};
        }
      }
    }
    if (this.opts.debugMode) {
      console.log(`🔍 IndexManager._initializeDefaults: Initialized with fields: ${Object.keys(this.index.data).join(', ')}`);
    }
  }
  readColumnIndex(column) {
    return new Set(this.index.data && this.index.data[column] ? Object.keys(this.index.data[column]) : []);
  }

  /**
   * Convert index to JSON-serializable format for debugging and export
   * This resolves the issue where Sets appear as empty objects in JSON.stringify
   */
  toJSON() {
    const serializable = {
      data: {},
      totalLines: this.totalLines
    };

    // Check if this is a term mapping field for conversion
    const isTermMappingField = field => {
      return this.database?.termManager && this.database.termManager.termMappingFields && this.database.termManager.termMappingFields.includes(field);
    };
    for (const field in this.index.data) {
      serializable.data[field] = {};
      const isTermField = isTermMappingField(field);
      for (const term in this.index.data[field]) {
        const hybridData = this.index.data[field][term];

        // CRITICAL FIX: Convert term strings to term IDs for term mapping fields
        // If the key is a string term (not a numeric ID), convert it to term ID
        let termKey = term;
        if (isTermField && typeof term === 'string' && !/^\d+$/.test(term)) {
          // Key is a term string, convert to term ID
          const termId = this.database?.termManager?.getTermIdWithoutIncrement(term);
          if (termId !== undefined) {
            termKey = String(termId);
          } else {
            // Term not found in termManager, keep original key
            // This prevents data loss when term mapping is incomplete
            termKey = term;
            if (this.opts?.debugMode) {
              console.log(`⚠️  IndexManager.toJSON: Term "${term}" not found in termManager for field "${field}" - using original key`);
            }
          }
        }

        // OPTIMIZATION: Create ranges before serialization if beneficial
        if (hybridData.set && hybridData.set.size >= this.rangeThreshold) {
          this._optimizeToRanges(hybridData);
        }

        // Convert hybrid structure to serializable format
        let setArray = [];
        if (hybridData.set) {
          if (typeof hybridData.set.size !== 'undefined') {
            // Regular Set
            setArray = Array.from(hybridData.set);
          }
        }

        // Use ultra-compact format: [setArray, rangesArray] to save space
        const ranges = hybridData.ranges || [];
        if (ranges.length > 0) {
          // Convert ranges to ultra-compact format: [start, count] instead of {start, count}
          const compactRanges = ranges.map(range => [range.start, range.count]);
          serializable.data[field][termKey] = [setArray, compactRanges];
        } else {
          // CRITICAL FIX: Always use the [setArray, []] format for consistency
          // This ensures the load() method can properly deserialize the data
          serializable.data[field][termKey] = [setArray, []];
        }
      }
    }
    return serializable;
  }

  /**
   * Get a JSON string representation of the index
   * This properly handles Sets unlike the default JSON.stringify
   */
  toString() {
    return JSON.stringify(this.toJSON(), null, 2);
  }

  // Simplified term mapping methods - just basic functionality

  /**
   * Rebuild index (stub for compatibility)
   */
  async rebuild() {
    // Stub implementation for compatibility
    return Promise.resolve();
  }
}

/**
 * SchemaManager - Manages field schemas for optimized array-based serialization
 * This replaces the need for repeating field names in JSON objects
 */
class SchemaManager {
  constructor(opts = {}) {
    this.opts = Object.assign({
      enableArraySerialization: true,
      strictSchema: true,
      debugMode: false
    }, opts);

    // Schema definition: array of field names in order
    this.schema = [];
    this.fieldToIndex = new Map(); // field name -> index
    this.indexToField = new Map(); // index -> field name
    this.schemaVersion = 1;
    this.isInitialized = false;
  }

  /**
   * Initialize schema from options or auto-detect from data
   */
  initializeSchema(schemaOrData, autoDetect = false) {
    if (this.isInitialized && this.opts.strictSchema) {
      if (this.opts.debugMode) {
        console.log('SchemaManager: Schema already initialized, skipping');
      }
      return;
    }
    if (Array.isArray(schemaOrData)) {
      // Explicit schema provided
      this.setSchema(schemaOrData);
    } else if (autoDetect && typeof schemaOrData === 'object') {
      // Auto-detect schema from data
      this.autoDetectSchema(schemaOrData);
    } else if (schemaOrData && typeof schemaOrData === 'object') {
      // Initialize from database options
      this.initializeFromOptions(schemaOrData);
    }
    this.isInitialized = true;
    if (this.opts.debugMode) {
      console.log('SchemaManager: Schema initialized:', this.schema);
    }
  }

  /**
   * Set explicit schema
   */
  setSchema(fieldNames) {
    this.schema = [...fieldNames]; // Create copy
    this.fieldToIndex.clear();
    this.indexToField.clear();
    this.schema.forEach((field, index) => {
      this.fieldToIndex.set(field, index);
      this.indexToField.set(index, field);
    });
    if (this.opts.debugMode) {
      console.log('SchemaManager: Schema set:', this.schema);
    }
  }

  /**
   * Auto-detect schema from sample data
   */
  autoDetectSchema(sampleData) {
    if (Array.isArray(sampleData)) {
      // Use first record as template
      if (sampleData.length > 0) {
        this.autoDetectSchema(sampleData[0]);
      }
      return;
    }
    if (typeof sampleData === 'object' && sampleData !== null) {
      const fields = Object.keys(sampleData).sort(); // Sort for consistency

      // CRITICAL FIX: Always include 'id' field in schema for proper array format
      if (!fields.includes('id')) {
        fields.push('id');
      }
      this.setSchema(fields);
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
      return this.fieldToIndex.get(fieldName);
    }
    const newIndex = this.schema.length;
    this.schema.push(fieldName);
    this.fieldToIndex.set(fieldName, newIndex);
    this.indexToField.set(newIndex, fieldName);
    if (this.opts.debugMode) {
      console.log('SchemaManager: Added field:', fieldName, 'at index:', newIndex);
    }
    return newIndex;
  }

  /**
   * Convert object to array using schema with strict field enforcement
   */
  objectToArray(obj) {
    if (!this.isInitialized || !this.opts.enableArraySerialization) {
      return obj; // Fallback to object format
    }
    if (typeof obj !== 'object' || obj === null || Array.isArray(obj)) {
      return obj; // Don't convert non-objects or arrays
    }
    const result = new Array(this.schema.length);

    // Fill array with values in schema order
    // Missing fields become undefined, extra fields are ignored
    for (let i = 0; i < this.schema.length; i++) {
      const fieldName = this.schema[i];
      result[i] = obj[fieldName] !== undefined ? obj[fieldName] : undefined;
    }

    // CRITICAL FIX: Always append 'id' field if it exists and is not in schema
    // The 'id' field must be preserved even if not in the schema
    if (obj.id !== undefined && obj.id !== null && this.schema.indexOf('id') === -1) {
      result.push(obj.id);
    }
    return result;
  }

  /**
   * Convert array back to object using schema
   */
  arrayToObject(arr) {
    if (!this.isInitialized || !this.opts.enableArraySerialization) {
      return arr; // Fallback to array format
    }
    if (!Array.isArray(arr)) {
      return arr; // Don't convert non-arrays
    }
    const obj = {};
    const idIndex = this.schema.indexOf('id');

    // DISABLED: Schema migration detection was causing field mapping corruption
    // The logic was incorrectly assuming ID was in first position when it's appended at the end
    // This caused fields to be shifted incorrectly during object-to-array-to-object conversion
    let arrayOffset = 0;

    // Map array values to object properties
    // Only include fields that are in the schema
    for (let i = 0; i < Math.min(arr.length - arrayOffset, this.schema.length); i++) {
      const fieldName = this.schema[i];
      const arrayIndex = i + arrayOffset;
      // Only include non-undefined values to avoid cluttering the object
      if (arr[arrayIndex] !== undefined) {
        obj[fieldName] = arr[arrayIndex];
      }
    }

    // CRITICAL FIX: Always preserve 'id' field if it exists in the original object
    // The 'id' field may not be in the schema but must be preserved
    if (idIndex !== -1 && arr[idIndex] !== undefined) {
      // 'id' is in schema and has a value
      obj.id = arr[idIndex];
    } else if (!obj.id && arr.length > this.schema.length + arrayOffset) {
      // 'id' is not in schema but array has extra element(s) - check if last element could be ID
      // This handles cases where ID was added after schema initialization
      for (let i = this.schema.length + arrayOffset; i < arr.length; i++) {
        // Try to infer if this is an ID (string that looks like an ID)
        const potentialId = arr[i];
        if (potentialId !== undefined && potentialId !== null && typeof potentialId === 'string' && potentialId.length > 0 && potentialId.length < 100) {
          obj.id = potentialId;
          break; // Use first potential ID found
        }
      }
    }
    return obj;
  }

  /**
   * Get field index by name
   */
  getFieldIndex(fieldName) {
    return this.fieldToIndex.get(fieldName);
  }

  /**
   * Get field name by index
   */
  getFieldName(index) {
    return this.indexToField.get(index);
  }

  /**
   * Check if field exists in schema
   */
  hasField(fieldName) {
    return this.fieldToIndex.has(fieldName);
  }

  /**
   * Get schema as array of field names
   */
  getSchema() {
    return [...this.schema]; // Return copy
  }

  /**
   * Get schema size
   */
  getSchemaSize() {
    return this.schema.length;
  }

  /**
   * Validate that object conforms to schema
   */
  validateObject(obj) {
    if (!this.isInitialized || !this.opts.strictSchema) {
      return true;
    }
    if (typeof obj !== 'object' || obj === null || Array.isArray(obj)) {
      return false;
    }

    // Check if object has all required fields
    for (const field of this.schema) {
      if (!(field in obj)) {
        if (this.opts.debugMode) {
          console.warn('SchemaManager: Missing required field:', field);
        }
        return false;
      }
    }
    return true;
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
    };
  }

  /**
   * Reset schema
   */
  reset() {
    this.schema = [];
    this.fieldToIndex.clear();
    this.indexToField.clear();
    this.isInitialized = false;
    this.schemaVersion++;
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
    };
  }
}

// NOTE: Buffer pool was removed due to complexity with low performance gain
// It was causing serialization issues and data corruption in batch operations
// If reintroducing buffer pooling in the future, ensure proper buffer management
// and avoid reusing buffers that may contain stale data

class Serializer {
  constructor(opts = {}) {
    this.opts = Object.assign({
      enableAdvancedSerialization: true,
      enableArraySerialization: true
      // NOTE: bufferPoolSize, adaptivePooling, memoryPressureThreshold removed
      // Buffer pool was causing more problems than benefits
    }, opts);

    // Initialize schema manager for array-based serialization
    this.schemaManager = new SchemaManager({
      enableArraySerialization: this.opts.enableArraySerialization,
      strictSchema: true,
      debugMode: this.opts.debugMode || false
    });

    // Advanced serialization settings
    this.serializationStats = {
      totalSerializations: 0,
      totalDeserializations: 0,
      jsonSerializations: 0,
      arraySerializations: 0,
      objectSerializations: 0
    };
  }

  /**
   * Initialize schema for array-based serialization
   */
  initializeSchema(schemaOrData, autoDetect = false) {
    this.schemaManager.initializeSchema(schemaOrData, autoDetect);
  }

  /**
   * Get current schema
   */
  getSchema() {
    return this.schemaManager.getSchema();
  }

  /**
   * Convert object to array format for optimized serialization
   */
  convertToArrayFormat(obj) {
    if (!this.opts.enableArraySerialization) {
      return obj;
    }
    return this.schemaManager.objectToArray(obj);
  }

  /**
   * Convert array format back to object
   */
  convertFromArrayFormat(arr) {
    if (!this.opts.enableArraySerialization) {
      return arr;
    }
    const obj = this.schemaManager.arrayToObject(arr);

    // CRITICAL FIX: Always preserve 'id' field if it exists in the original array
    // The 'id' field may not be in the schema but must be preserved
    // Check if array has more elements than schema fields - the extra element(s) might be the ID
    if (!obj.id && Array.isArray(arr) && this.schemaManager.isInitialized) {
      const schemaLength = this.schemaManager.schema ? this.schemaManager.schema.length : 0;
      if (arr.length > schemaLength) {
        // Check if any extra element looks like an ID (string)
        for (let i = schemaLength; i < arr.length; i++) {
          const potentialId = arr[i];
          if (potentialId !== undefined && potentialId !== null && typeof potentialId === 'string' && potentialId.length > 0) {
            obj.id = potentialId;
            break;
          }
        }
      }
    }
    return obj;
  }

  /**
   * Advanced serialization with optimized JSON and buffer pooling
   */
  serialize(data, opts = {}) {
    this.serializationStats.totalSerializations++;
    const addLinebreak = opts.linebreak !== false;

    // Convert to array format if enabled
    const serializationData = this.convertToArrayFormat(data);

    // Track conversion statistics
    if (Array.isArray(serializationData) && typeof data === 'object' && data !== null) {
      this.serializationStats.arraySerializations++;
    } else {
      this.serializationStats.objectSerializations++;
    }

    // Use advanced JSON serialization
    if (this.opts.enableAdvancedSerialization) {
      this.serializationStats.jsonSerializations++;
      return this.serializeAdvanced(serializationData, addLinebreak);
    }

    // Fallback to standard serialization
    this.serializationStats.jsonSerializations++;
    return this.serializeStandard(serializationData, addLinebreak);
  }

  /**
   * Advanced serialization with optimized JSON.stringify and buffer pooling
   */
  serializeAdvanced(data, addLinebreak) {
    // Validate encoding before serialization
    this.validateEncodingBeforeSerialization(data);

    // Use optimized JSON.stringify without buffer pooling
    // NOTE: Buffer pool removed - using direct Buffer creation for simplicity and reliability
    const json = this.optimizedStringify(data);

    // CRITICAL FIX: Normalize encoding before creating buffer
    const normalizedJson = this.normalizeEncoding(json);
    const jsonBuffer = Buffer.from(normalizedJson, 'utf8');
    const totalLength = jsonBuffer.length + (addLinebreak ? 1 : 0);
    const result = Buffer.allocUnsafe(totalLength);
    jsonBuffer.copy(result, 0, 0, jsonBuffer.length);
    if (addLinebreak) {
      result[jsonBuffer.length] = 0x0A;
    }
    return result;
  }

  /**
   * Proper encoding normalization with UTF-8 validation
   * Fixed to prevent double-encoding and data corruption
   */
  normalizeEncoding(str) {
    if (typeof str !== 'string') return str;

    // Skip if already valid UTF-8 (99% of cases)
    if (this.isValidUTF8(str)) return str;

    // Try to detect and convert encoding safely
    return this.safeConvertToUTF8(str);
  }

  /**
   * Check if string is valid UTF-8
   */
  isValidUTF8(str) {
    try {
      // Test if string can be encoded and decoded as UTF-8 without loss
      const encoded = Buffer.from(str, 'utf8');
      const decoded = encoded.toString('utf8');
      return decoded === str;
    } catch (error) {
      return false;
    }
  }

  /**
   * Safe conversion to UTF-8 with proper encoding detection
   */
  safeConvertToUTF8(str) {
    // Try common encodings in order of likelihood
    const encodings = ['utf8', 'latin1', 'utf16le', 'ascii'];
    for (const encoding of encodings) {
      try {
        const converted = Buffer.from(str, encoding).toString('utf8');

        // Validate the conversion didn't lose information
        if (this.isValidUTF8(converted)) {
          return converted;
        }
      } catch (error) {
        // Try next encoding
        continue;
      }
    }

    // Fallback: return original string (preserve data)
    console.warn('JexiDB: Could not normalize encoding, preserving original string');
    return str;
  }

  /**
   * Enhanced deep encoding normalization with UTF-8 validation
   * Fixed to prevent double-encoding and data corruption
   */
  deepNormalizeEncoding(obj) {
    if (obj === null || obj === undefined) return obj;
    if (typeof obj === 'string') {
      return this.normalizeEncoding(obj);
    }
    if (Array.isArray(obj)) {
      // Check if normalization is needed (performance optimization)
      const needsNormalization = obj.some(item => typeof item === 'string' && !this.isValidUTF8(item));
      if (!needsNormalization) return obj;
      return obj.map(item => this.deepNormalizeEncoding(item));
    }
    if (typeof obj === 'object') {
      // Check if normalization is needed (performance optimization)
      const needsNormalization = Object.values(obj).some(value => typeof value === 'string' && !this.isValidUTF8(value));
      if (!needsNormalization) return obj;
      const normalized = {};
      for (const [key, value] of Object.entries(obj)) {
        normalized[key] = this.deepNormalizeEncoding(value);
      }
      return normalized;
    }
    return obj;
  }

  /**
   * Validate encoding before serialization
   */
  validateEncodingBeforeSerialization(data) {
    const issues = [];
    const checkString = (str, path = '') => {
      if (typeof str === 'string' && !this.isValidUTF8(str)) {
        issues.push(`Invalid encoding at ${path}: "${str.substring(0, 50)}..."`);
      }
    };
    const traverse = (obj, path = '') => {
      if (typeof obj === 'string') {
        checkString(obj, path);
      } else if (Array.isArray(obj)) {
        obj.forEach((item, index) => {
          traverse(item, `${path}[${index}]`);
        });
      } else if (obj && typeof obj === 'object') {
        Object.entries(obj).forEach(([key, value]) => {
          traverse(value, path ? `${path}.${key}` : key);
        });
      }
    };
    traverse(data);
    if (issues.length > 0) {
      console.warn('JexiDB: Encoding issues detected:', issues);
    }
    return issues.length === 0;
  }

  /**
   * Optimized JSON.stringify with fast paths for common data structures
   * Now includes deep encoding normalization for all string fields
   */
  optimizedStringify(obj) {
    // CRITICAL: Normalize encoding for all string fields before stringify
    const normalizedObj = this.deepNormalizeEncoding(obj);
    return this._stringifyNormalizedValue(normalizedObj);
  }
  _stringifyNormalizedValue(value) {
    // Fast path for null and undefined
    if (value === null || value === undefined) {
      return 'null';
    }
    const type = typeof value;

    // Fast path for primitives
    if (type === 'boolean') {
      return value ? 'true' : 'false';
    }
    if (type === 'number') {
      return Number.isFinite(value) ? value.toString() : 'null';
    }
    if (type === 'string') {
      // Fast path for simple strings (no escaping needed)
      if (!/[\\"\u0000-\u001f]/.test(value)) {
        return '"' + value + '"';
      }
      // Fall back to JSON.stringify for complex strings
      return JSON.stringify(value);
    }
    if (Array.isArray(value)) {
      return this._stringifyNormalizedArray(value);
    }
    if (type === 'object') {
      const keys = Object.keys(value);
      if (keys.length === 0) return '{}';
      // Use native stringify for object to leverage stable handling of undefined, Dates, etc.
      return JSON.stringify(value);
    }

    // Fallback to JSON.stringify for unknown types (BigInt, symbols, etc.)
    return JSON.stringify(value);
  }
  _stringifyNormalizedArray(arr) {
    const length = arr.length;
    if (length === 0) return '[]';
    let result = '[';
    for (let i = 0; i < length; i++) {
      if (i > 0) result += ',';
      const element = arr[i];

      // JSON spec: undefined, functions, and symbols are serialized as null within arrays
      if (element === undefined || typeof element === 'function' || typeof element === 'symbol') {
        result += 'null';
        continue;
      }
      result += this._stringifyNormalizedValue(element);
    }
    result += ']';
    return result;
  }

  /**
   * Standard serialization (fallback)
   */
  serializeStandard(data, addLinebreak) {
    // Validate encoding before serialization
    this.validateEncodingBeforeSerialization(data);

    // NOTE: Buffer pool removed - using direct Buffer creation for simplicity and reliability
    // CRITICAL: Normalize encoding for all string fields before stringify
    const normalizedData = this.deepNormalizeEncoding(data);
    const json = JSON.stringify(normalizedData);

    // CRITICAL FIX: Normalize encoding before creating buffer
    const normalizedJson = this.normalizeEncoding(json);
    const jsonBuffer = Buffer.from(normalizedJson, 'utf8');
    const totalLength = jsonBuffer.length + (addLinebreak ? 1 : 0);
    const result = Buffer.allocUnsafe(totalLength);
    jsonBuffer.copy(result, 0, 0, jsonBuffer.length);
    if (addLinebreak) {
      result[jsonBuffer.length] = 0x0A;
    }
    return result;
  }

  /**
   * Advanced deserialization with fast paths
   */
  deserialize(data) {
    this.serializationStats.totalDeserializations++;
    if (data.length === 0) return null;
    try {
      // Handle both Buffer and string inputs
      let str;
      if (Buffer.isBuffer(data)) {
        // Fast path: avoid toString() for empty data
        if (data.length === 1 && data[0] === 0x0A) return null; // Just newline
        str = data.toString('utf8').trim();
      } else if (typeof data === 'string') {
        str = data.trim();
      } else {
        throw new Error('Invalid data type for deserialization');
      }
      const strLength = str.length;

      // Fast path for empty strings
      if (strLength === 0) return null;

      // CRITICAL FIX: Detect and handle multiple JSON objects in the same line
      // This can happen if data was corrupted during concurrent writes or offset calculation errors
      const firstBrace = str.indexOf('{');
      const firstBracket = str.indexOf('[');

      // Helper function to extract first complete JSON object/array from a string
      // CRITICAL FIX: Must handle strings and escaped characters correctly
      // to avoid counting braces/brackets that are inside string values
      const extractFirstJson = (jsonStr, startChar) => {
        if (startChar === '{') {
          let braceCount = 0;
          let endPos = -1;
          let inString = false;
          let escapeNext = false;
          for (let i = 0; i < jsonStr.length; i++) {
            const char = jsonStr[i];
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
              if (char === '{') braceCount++;
              if (char === '}') {
                braceCount--;
                if (braceCount === 0) {
                  endPos = i + 1;
                  break;
                }
              }
            }
          }
          return endPos > 0 ? jsonStr.substring(0, endPos) : null;
        } else if (startChar === '[') {
          let bracketCount = 0;
          let endPos = -1;
          let inString = false;
          let escapeNext = false;
          for (let i = 0; i < jsonStr.length; i++) {
            const char = jsonStr[i];
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
              if (char === '[') bracketCount++;
              if (char === ']') {
                bracketCount--;
                if (bracketCount === 0) {
                  endPos = i + 1;
                  break;
                }
              }
            }
          }
          return endPos > 0 ? jsonStr.substring(0, endPos) : null;
        }
        return null;
      };

      // Check if JSON starts at the beginning of the string
      const jsonStartsAtZero = firstBrace === 0 || firstBracket === 0;
      let hasValidJson = false;
      if (jsonStartsAtZero) {
        // JSON starts at beginning - check for multiple JSON objects/arrays
        if (firstBrace === 0) {
          const secondBrace = str.indexOf('{', 1);
          if (secondBrace !== -1) {
            // Multiple objects detected - extract first
            const extracted = extractFirstJson(str, '{');
            if (extracted) {
              str = extracted;
              hasValidJson = true;
              if (this.opts && this.opts.debugMode) {
                console.warn(`⚠️ Deserialize: Multiple JSON objects detected, using first object only`);
              }
            }
          } else {
            hasValidJson = true; // Single valid object starting at 0
          }
        } else if (firstBracket === 0) {
          const secondBracket = str.indexOf('[', 1);
          if (secondBracket !== -1) {
            // Multiple arrays detected - extract first
            const extracted = extractFirstJson(str, '[');
            if (extracted) {
              str = extracted;
              hasValidJson = true;
              if (this.opts && this.opts.debugMode) {
                console.warn(`⚠️ Deserialize: Multiple JSON arrays detected, using first array only`);
              }
            }
          } else {
            hasValidJson = true; // Single valid array starting at 0
          }
        }
      } else {
        // JSON doesn't start at beginning - try to find and extract first valid JSON
        const jsonStart = firstBrace !== -1 ? firstBracket !== -1 ? Math.min(firstBrace, firstBracket) : firstBrace : firstBracket;
        if (jsonStart !== -1 && jsonStart > 0) {
          // Found JSON but not at start - extract from that position
          const jsonStr = str.substring(jsonStart);
          const startChar = jsonStr[0];
          const extracted = extractFirstJson(jsonStr, startChar);
          if (extracted) {
            str = extracted;
            hasValidJson = true;
            if (this.opts && this.opts.debugMode) {
              console.warn(`⚠️ Deserialize: Found JSON after ${jsonStart} chars of invalid text, extracted first ${startChar === '{' ? 'object' : 'array'}`);
            }
          }
        }
      }

      // CRITICAL FIX: If no valid JSON structure found, throw error before attempting parse
      // This allows walk() and other callers to catch and skip invalid lines
      if (!hasValidJson && firstBrace === -1 && firstBracket === -1) {
        const errorStr = Buffer.isBuffer(data) ? data.toString('utf8').trim() : data.trim();
        const error = new Error(`Failed to deserialize JSON data: No valid JSON structure found in "${errorStr.substring(0, 100)}..."`);
        // Mark this as a "no valid JSON" error so it can be handled appropriately
        error.noValidJson = true;
        throw error;
      }

      // If we tried to extract but got nothing valid, also throw error
      if (hasValidJson && (!str || str.trim().length === 0)) {
        const error = new Error(`Failed to deserialize JSON data: Extracted JSON is empty`);
        error.noValidJson = true;
        throw error;
      }

      // Parse JSON data
      const parsedData = JSON.parse(str);

      // Convert from array format back to object if needed
      return this.convertFromArrayFormat(parsedData);
    } catch (e) {
      // If error was already formatted with noValidJson flag, re-throw as-is
      if (e.noValidJson) {
        throw e;
      }
      // Otherwise, format the error message
      const str = Buffer.isBuffer(data) ? data.toString('utf8').trim() : data.trim();
      throw new Error(`Failed to deserialize JSON data: "${str.substring(0, 100)}..." - ${e.message}`);
    }
  }

  /**
   * Batch serialization for multiple records
   */
  serializeBatch(dataArray, opts = {}) {
    // Validate encoding before serialization
    this.validateEncodingBeforeSerialization(dataArray);

    // Convert all objects to array format for optimization
    const convertedData = dataArray.map(data => this.convertToArrayFormat(data));

    // Track conversion statistics
    this.serializationStats.arraySerializations += convertedData.filter((item, index) => Array.isArray(item) && typeof dataArray[index] === 'object' && dataArray[index] !== null).length;
    this.serializationStats.objectSerializations += dataArray.length - this.serializationStats.arraySerializations;

    // JSONL format: serialize each array as a separate line
    try {
      const lines = [];
      for (const arrayData of convertedData) {
        const json = this.optimizedStringify(arrayData);
        const normalizedJson = this.normalizeEncoding(json);
        lines.push(normalizedJson);
      }

      // Join all lines with newlines
      const jsonlContent = lines.join('\n');
      const jsonlBuffer = Buffer.from(jsonlContent, 'utf8');

      // Add final linebreak if requested
      const addLinebreak = opts.linebreak !== false;
      const totalLength = jsonlBuffer.length + (addLinebreak ? 1 : 0);
      const result = Buffer.allocUnsafe(totalLength);
      jsonlBuffer.copy(result, 0, 0, jsonlBuffer.length);
      if (addLinebreak) {
        result[jsonlBuffer.length] = 0x0A;
      }
      return result;
    } catch (e) {
      // Fallback to individual serialization if batch serialization fails
      const results = [];
      const batchSize = opts.batchSize || 100;
      for (let i = 0; i < convertedData.length; i += batchSize) {
        const batch = convertedData.slice(i, i + batchSize);
        const batchResults = batch.map(data => this.serialize(data, opts));
        results.push(...batchResults);
      }
      return results;
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
          return data.toString('utf8').trim();
        } else if (typeof data === 'string') {
          return data.trim();
        } else {
          throw new Error('Invalid data type for batch deserialization');
        }
      }).join(',') + ']';
      const parsedResults = JSON.parse(entriesJson);

      // Convert arrays back to objects if needed
      const results = parsedResults.map(data => this.convertFromArrayFormat(data));

      // Validate that all results are objects (JexiDB requirement)
      if (Array.isArray(results) && results.every(item => item && typeof item === 'object')) {
        return results;
      }

      // If validation fails, fall back to individual parsing
      throw new Error('Validation failed - not all entries are objects');
    } catch (e) {
      // Fallback to individual deserialization if batch parsing fails
      const results = [];
      const batchSize = 100; // Process in batches to avoid blocking

      for (let i = 0; i < dataArray.length; i += batchSize) {
        const batch = dataArray.slice(i, i + batchSize);
        const batchResults = batch.map(data => this.deserialize(data));
        results.push(...batchResults);
      }
      return results;
    }
  }

  /**
   * Check if data appears to be binary (always false since we only use JSON now)
   */
  isBinaryData(data) {
    // All data is now JSON format
    return false;
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
    };
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
    };

    // Reset schema manager
    if (this.schemaManager) {
      this.schemaManager.reset();
    }
  }
}

/**
 * OperationQueue - Queue system for database operations
 * Resolves race conditions between concurrent operations
 */

class OperationQueue {
  constructor(debugMode = false) {
    this.queue = [];
    this.processing = false;
    this.operationId = 0;
    this.debugMode = debugMode;
    this.stats = {
      totalOperations: 0,
      completedOperations: 0,
      failedOperations: 0,
      averageProcessingTime: 0,
      maxProcessingTime: 0,
      totalProcessingTime: 0
    };
  }

  /**
   * Adds an operation to the queue
   * @param {Function} operation - Asynchronous function to be executed
   * @returns {Promise} - Promise that resolves when the operation is completed
   */
  async enqueue(operation) {
    const id = ++this.operationId;
    const startTime = Date.now();
    if (this.debugMode) {
      console.log(`🔄 Queue: Enqueuing operation ${id}, queue length: ${this.queue.length}`);
    }
    this.stats.totalOperations++;
    return new Promise((resolve, reject) => {
      // Capture stack trace for debugging stuck operations
      const stackTrace = new Error().stack;
      this.queue.push({
        id,
        operation,
        resolve,
        reject,
        timestamp: startTime,
        stackTrace: stackTrace,
        startTime: Date.now()
      });

      // Process immediately if not already processing
      this.process().catch(reject);
    });
  }

  /**
   * Processes all operations in the queue sequentially
   */
  async process() {
    if (this.processing || this.queue.length === 0) {
      return;
    }
    this.processing = true;
    if (this.debugMode) {
      console.log(`🔄 Queue: Starting to process ${this.queue.length} operations`);
    }
    try {
      while (this.queue.length > 0) {
        const {
          id,
          operation,
          resolve,
          reject,
          timestamp
        } = this.queue.shift();
        if (this.debugMode) {
          console.log(`🔄 Queue: Processing operation ${id}`);
        }
        try {
          const result = await operation();
          const processingTime = Date.now() - timestamp;

          // Atualizar estatísticas
          this.stats.completedOperations++;
          this.stats.totalProcessingTime += processingTime;
          this.stats.averageProcessingTime = this.stats.totalProcessingTime / this.stats.completedOperations;
          this.stats.maxProcessingTime = Math.max(this.stats.maxProcessingTime, processingTime);
          resolve(result);
          if (this.debugMode) {
            console.log(`✅ Queue: Operation ${id} completed in ${processingTime}ms`);
          }
        } catch (error) {
          const processingTime = Date.now() - timestamp;

          // Atualizar estatísticas
          this.stats.failedOperations++;
          this.stats.totalProcessingTime += processingTime;
          this.stats.averageProcessingTime = this.stats.totalProcessingTime / (this.stats.completedOperations + this.stats.failedOperations);
          this.stats.maxProcessingTime = Math.max(this.stats.maxProcessingTime, processingTime);
          reject(error);
          if (this.debugMode) {
            console.error(`❌ Queue: Operation ${id} failed in ${processingTime}ms:`, error);
          }
        }
      }
    } finally {
      this.processing = false;
      if (this.debugMode) {
        console.log(`🔄 Queue: Finished processing, remaining: ${this.queue.length}`);
      }
    }
  }

  /**
   * Waits for all pending operations to be processed
   * @param {number|null} maxWaitTime - Maximum wait time in ms (null = wait indefinitely)
   * @returns {Promise<boolean>} - true if all operations were processed, false if a timeout occurred
   */
  async waitForCompletion(maxWaitTime = 5000) {
    const startTime = Date.now();

    // CRITICAL FIX: Support infinite wait when maxWaitTime is null
    const hasTimeout = maxWaitTime !== null && maxWaitTime !== undefined;
    while (this.queue.length > 0) {
      // Check timeout only if we have one
      if (hasTimeout && Date.now() - startTime >= maxWaitTime) {
        break;
      }
      await new Promise(resolve => setTimeout(resolve, 1));
    }
    const completed = this.queue.length === 0;
    if (!completed && hasTimeout) {
      // CRITICAL: Don't leave operations hanging - fail fast with detailed error
      const pendingOperations = this.queue.map(op => ({
        id: op.id,
        stackTrace: op.stackTrace,
        startTime: op.startTime,
        waitTime: Date.now() - op.startTime
      }));

      // Clear the queue to prevent memory leaks
      this.queue = [];
      const error = new Error(`OperationQueue: Operations timed out after ${maxWaitTime}ms. ${pendingOperations.length} operations were stuck and have been cleared.`);
      error.pendingOperations = pendingOperations;
      error.queueStats = this.getStats();
      if (this.debugMode) {
        console.error(`❌ Queue: Operations timed out, clearing ${pendingOperations.length} stuck operations:`);
        pendingOperations.forEach(op => {
          console.error(`  - Operation ${op.id} (waiting ${op.waitTime}ms):`);
          console.error(`    Stack: ${op.stackTrace}`);
        });
      }
      throw error;
    }
    return completed;
  }

  /**
   * Returns the current queue length
   */
  getQueueLength() {
    return this.queue.length;
  }

  /**
   * Checks whether operations are currently being processed
   */
  isProcessing() {
    return this.processing;
  }

  /**
   * Returns queue statistics
   */
  getStats() {
    return {
      ...this.stats,
      queueLength: this.queue.length,
      isProcessing: this.processing,
      successRate: this.stats.totalOperations > 0 ? this.stats.completedOperations / this.stats.totalOperations * 100 : 0
    };
  }

  /**
   * Clears the queue (for emergency situations)
   */
  clear() {
    const clearedCount = this.queue.length;
    this.queue = [];
    if (this.debugMode) {
      console.log(`🧹 Queue: Cleared ${clearedCount} pending operations`);
    }
    return clearedCount;
  }

  /**
   * Detects stuck operations and returns detailed information
   * @param {number} stuckThreshold - Time in ms to consider an operation stuck
   * @returns {Array} - List of stuck operations with stack traces
   */
  detectStuckOperations(stuckThreshold = 10000) {
    const now = Date.now();
    const stuckOperations = this.queue.filter(op => now - op.startTime > stuckThreshold);
    return stuckOperations.map(op => ({
      id: op.id,
      waitTime: now - op.startTime,
      stackTrace: op.stackTrace,
      timestamp: op.timestamp
    }));
  }

  /**
   * Force-cleans stuck operations (last resort)
   * @param {number} stuckThreshold - Time in ms to consider an operation stuck
   * @returns {number} - Number of operations removed
   */
  forceCleanupStuckOperations(stuckThreshold = 10000) {
    const stuckOps = this.detectStuckOperations(stuckThreshold);
    if (stuckOps.length > 0) {
      // Reject all stuck operations
      stuckOps.forEach(stuckOp => {
        const opIndex = this.queue.findIndex(op => op.id === stuckOp.id);
        if (opIndex !== -1) {
          const op = this.queue[opIndex];
          op.reject(new Error(`Operation ${op.id} was stuck for ${stuckOp.waitTime}ms and has been force-cleaned. Stack: ${stuckOp.stackTrace}`));
          this.queue.splice(opIndex, 1);
        }
      });
      if (this.debugMode) {
        console.error(`🧹 Queue: Force-cleaned ${stuckOps.length} stuck operations`);
        stuckOps.forEach(op => {
          console.error(`  - Operation ${op.id} (stuck for ${op.waitTime}ms)`);
        });
      }
    }
    return stuckOps.length;
  }

  /**
   * Checks whether the queue is empty
   */
  isEmpty() {
    return this.queue.length === 0;
  }

  /**
   * Returns information about the next operation in the queue
   */
  peekNext() {
    if (this.queue.length === 0) {
      return null;
    }
    const next = this.queue[0];
    return {
      id: next.id,
      timestamp: next.timestamp,
      waitTime: Date.now() - next.timestamp
    };
  }
}

/*
How it works:
`this.#head` is an instance of `Node` which keeps track of its current value and nests another instance of `Node` that keeps the value that comes after it. When a value is provided to `.enqueue()`, the code needs to iterate through `this.#head`, going deeper and deeper to find the last value. However, iterating through every single item is slow. This problem is solved by saving a reference to the last value as `this.#tail` so that it can reference it to add a new value.
*/

class Node {
	value;
	next;

	constructor(value) {
		this.value = value;
	}
}

class Queue {
	#head;
	#tail;
	#size;

	constructor() {
		this.clear();
	}

	enqueue(value) {
		const node = new Node(value);

		if (this.#head) {
			this.#tail.next = node;
			this.#tail = node;
		} else {
			this.#head = node;
			this.#tail = node;
		}

		this.#size++;
	}

	dequeue() {
		const current = this.#head;
		if (!current) {
			return;
		}

		this.#head = this.#head.next;
		this.#size--;
		return current.value;
	}

	peek() {
		if (!this.#head) {
			return;
		}

		return this.#head.value;

		// TODO: Node.js 18.
		// return this.#head?.value;
	}

	clear() {
		this.#head = undefined;
		this.#tail = undefined;
		this.#size = 0;
	}

	get size() {
		return this.#size;
	}

	* [Symbol.iterator]() {
		let current = this.#head;

		while (current) {
			yield current.value;
			current = current.next;
		}
	}
}

function pLimit(concurrency) {
	validateConcurrency(concurrency);

	const queue = new Queue();
	let activeCount = 0;

	const resumeNext = () => {
		if (activeCount < concurrency && queue.size > 0) {
			queue.dequeue()();
			// Since `pendingCount` has been decreased by one, increase `activeCount` by one.
			activeCount++;
		}
	};

	const next = () => {
		activeCount--;

		resumeNext();
	};

	const run = async (function_, resolve, arguments_) => {
		const result = (async () => function_(...arguments_))();

		resolve(result);

		try {
			await result;
		} catch {}

		next();
	};

	const enqueue = (function_, resolve, arguments_) => {
		// Queue `internalResolve` instead of the `run` function
		// to preserve asynchronous context.
		new Promise(internalResolve => {
			queue.enqueue(internalResolve);
		}).then(
			run.bind(undefined, function_, resolve, arguments_),
		);

		(async () => {
			// This function needs to wait until the next microtask before comparing
			// `activeCount` to `concurrency`, because `activeCount` is updated asynchronously
			// after the `internalResolve` function is dequeued and called. The comparison in the if-statement
			// needs to happen asynchronously as well to get an up-to-date value for `activeCount`.
			await Promise.resolve();

			if (activeCount < concurrency) {
				resumeNext();
			}
		})();
	};

	const generator = (function_, ...arguments_) => new Promise(resolve => {
		enqueue(function_, resolve, arguments_);
	});

	Object.defineProperties(generator, {
		activeCount: {
			get: () => activeCount,
		},
		pendingCount: {
			get: () => queue.size,
		},
		clearQueue: {
			value() {
				queue.clear();
			},
		},
		concurrency: {
			get: () => concurrency,

			set(newConcurrency) {
				validateConcurrency(newConcurrency);
				concurrency = newConcurrency;

				queueMicrotask(() => {
					// eslint-disable-next-line no-unmodified-loop-condition
					while (activeCount < concurrency && queue.size > 0) {
						resumeNext();
					}
				});
			},
		},
	});

	return generator;
}

function validateConcurrency(concurrency) {
	if (!((Number.isInteger(concurrency) || concurrency === Number.POSITIVE_INFINITY) && concurrency > 0)) {
		throw new TypeError('Expected `concurrency` to be a number from 1 and up');
	}
}

class FileHandler {
  constructor(file, fileMutex = null, opts = {}) {
    this.file = file;
    this.indexFile = file ? file.replace(/\.jdb$/, '.idx.jdb') : null;
    this.fileMutex = fileMutex;
    this.opts = opts;
    this.maxBufferSize = opts.maxBufferSize || 4 * 1024 * 1024; // 4MB default
    // Global I/O limiter to prevent file descriptor exhaustion in concurrent operations
    this.readLimiter = pLimit(opts.maxConcurrentReads || 4);
  }
  async truncate(offset) {
    try {
      await fs.promises.access(this.file, fs.constants.F_OK);
      await fs.promises.truncate(this.file, offset);
    } catch (err) {
      await fs.promises.writeFile(this.file, '');
    }
  }
  async writeOffsets(data) {
    // Write offsets to the index file (will be combined with index data)
    await fs.promises.writeFile(this.indexFile, data);
  }
  async readOffsets() {
    try {
      return await fs.promises.readFile(this.indexFile);
    } catch (err) {
      return null;
    }
  }
  async writeIndex(data) {
    // Write index data to the index file (will be combined with offsets)
    // Use Windows-specific retry logic for file operations
    await this._writeFileWithRetry(this.indexFile, data);
  }
  async readIndex() {
    try {
      return await fs.promises.readFile(this.indexFile);
    } catch (err) {
      return null;
    }
  }
  async exists() {
    try {
      await fs.promises.access(this.file, fs.constants.F_OK);
      return true;
    } catch (err) {
      return false;
    }
  }
  async indexExists() {
    try {
      await fs.promises.access(this.indexFile, fs.constants.F_OK);
      return true;
    } catch (err) {
      return false;
    }
  }
  async isLegacyFormat() {
    if (!(await this.exists())) return false;
    if (await this.indexExists()) return false;

    // Check if main file contains offsets at the end (legacy format)
    try {
      const lastLine = await this.readLastLine();
      if (!lastLine || !lastLine.length) return false;

      // Try to parse as offsets array
      const content = lastLine.toString('utf-8').trim();
      const parsed = JSON.parse(content);
      return Array.isArray(parsed);
    } catch (err) {
      return false;
    }
  }
  async migrateLegacyFormat(serializer) {
    if (!(await this.isLegacyFormat())) return false;
    console.log('Migrating from legacy format to new 3-file format...');

    // Read the legacy file
    const lastLine = await this.readLastLine();
    const offsets = JSON.parse(lastLine.toString('utf-8').trim());

    // Get index offset and truncate offsets array
    const indexOffset = offsets[offsets.length - 2];
    const dataOffsets = offsets.slice(0, -2);

    // Read index data
    const indexStart = indexOffset;
    const indexEnd = offsets[offsets.length - 1];
    const indexBuffer = await this.readRange(indexStart, indexEnd);
    const indexData = await serializer.deserialize(indexBuffer);

    // Write offsets to separate file
    const offsetsString = await serializer.serialize(dataOffsets, {
      linebreak: false
    });
    await this.writeOffsets(offsetsString);

    // Write index to separate file
    const indexString = await serializer.serialize(indexData, {
      linebreak: false
    });
    await this.writeIndex(indexString);

    // Truncate main file to remove index and offsets
    await this.truncate(indexOffset);
    console.log('Migration completed successfully!');
    return true;
  }
  async readRange(start, end) {
    // Check if file exists before trying to read it
    if (!(await this.exists())) {
      return Buffer.alloc(0); // Return empty buffer if file doesn't exist
    }
    let fd = await fs.promises.open(this.file, 'r');
    try {
      // CRITICAL FIX: Check file size before attempting to read
      const stats = await fd.stat();
      const fileSize = stats.size;

      // If start position is beyond file size, return empty buffer
      if (start >= fileSize) {
        await fd.close();
        return Buffer.alloc(0);
      }

      // Adjust end position if it's beyond file size
      const actualEnd = Math.min(end, fileSize);
      const length = actualEnd - start;

      // If length is 0 or negative, return empty buffer
      if (length <= 0) {
        await fd.close();
        return Buffer.alloc(0);
      }
      let buffer = Buffer.alloc(length);
      const {
        bytesRead
      } = await fd.read(buffer, 0, length, start);
      await fd.close();

      // CRITICAL FIX: Ensure we read the expected amount of data
      if (bytesRead !== length) {
        const errorMsg = `CRITICAL: Expected to read ${length} bytes, but read ${bytesRead} bytes at position ${start}`;
        console.error(`⚠️ ${errorMsg}`);

        // This indicates a race condition or file corruption
        // Don't retry - the caller should handle synchronization properly
        if (bytesRead === 0) {
          throw new Error(`File corruption detected: ${errorMsg}`);
        }

        // Return partial data with warning - caller should handle this
        return buffer.subarray(0, bytesRead);
      }
      return buffer;
    } catch (error) {
      await fd.close().catch(() => {});
      throw error;
    }
  }
  async readRanges(ranges, mapper) {
    const lines = {};

    // Check if file exists before trying to read it
    if (!(await this.exists())) {
      return lines; // Return empty object if file doesn't exist
    }
    const fd = await fs.promises.open(this.file, 'r');
    const groupedRanges = await this.groupedRanges(ranges);
    try {
      await Promise.allSettled(groupedRanges.map(async groupedRange => {
        await this.readLimiter(async () => {
          var _iteratorAbruptCompletion = false;
          var _didIteratorError = false;
          var _iteratorError;
          try {
            for (var _iterator = _asyncIterator(this.readGroupedRange(groupedRange, fd)), _step; _iteratorAbruptCompletion = !(_step = await _iterator.next()).done; _iteratorAbruptCompletion = false) {
              const row = _step.value;
              {
                lines[row.start] = mapper ? await mapper(row.line, {
                  start: row.start,
                  end: row.start + row.line.length
                }) : row.line;
              }
            }
          } catch (err) {
            _didIteratorError = true;
            _iteratorError = err;
          } finally {
            try {
              if (_iteratorAbruptCompletion && _iterator.return != null) {
                await _iterator.return();
              }
            } finally {
              if (_didIteratorError) {
                throw _iteratorError;
              }
            }
          }
        });
      }));
    } catch (e) {
      console.error('Error reading ranges:', e);
    } finally {
      await fd.close();
    }
    return lines;
  }
  async groupedRanges(ranges) {
    // expects ordered ranges from Database.getRanges()
    const readSize = 512 * 1024; // 512KB  
    const groupedRanges = [];
    let currentGroup = [];
    let currentSize = 0;

    // each range is a {start: number, end: number} object
    for (let i = 0; i < ranges.length; i++) {
      const range = ranges[i];
      const rangeSize = range.end - range.start;
      if (currentGroup.length > 0) {
        const lastRange = currentGroup[currentGroup.length - 1];
        if (lastRange.end !== range.start || currentSize + rangeSize > readSize) {
          groupedRanges.push(currentGroup);
          currentGroup = [];
          currentSize = 0;
        }
      }
      currentGroup.push(range);
      currentSize += rangeSize;
    }
    if (currentGroup.length > 0) {
      groupedRanges.push(currentGroup);
    }
    return groupedRanges;
  }
  readGroupedRange(groupedRange, fd) {
    var _this = this;
    return _wrapAsyncGenerator(function* () {
      if (groupedRange.length === 0) return;

      // OPTIMIZATION: For single range, use direct approach
      if (groupedRange.length === 1) {
        const range = groupedRange[0];
        const bufferSize = range.end - range.start;
        if (bufferSize <= 0 || bufferSize > _this.maxBufferSize) {
          throw new Error(`Invalid buffer size: ${bufferSize}. Start: ${range.start}, End: ${range.end}. Max allowed: ${_this.maxBufferSize}`);
        }
        const buffer = Buffer.allocUnsafe(bufferSize);
        const {
          bytesRead
        } = yield _awaitAsyncGenerator(fd.read(buffer, 0, bufferSize, range.start));
        const actualBuffer = bytesRead < bufferSize ? buffer.subarray(0, bytesRead) : buffer;
        if (actualBuffer.length === 0) return;
        let lineString;
        try {
          lineString = actualBuffer.toString('utf8');
        } catch (error) {
          lineString = actualBuffer.toString('utf8', {
            replacement: '?'
          });
        }

        // CRITICAL FIX: Remove trailing newlines and whitespace for single range too
        // Optimized: Use trimEnd() which efficiently removes all trailing whitespace (faster than manual checks)
        lineString = lineString.trimEnd();
        yield {
          line: lineString,
          start: range.start,
          _: range.index !== undefined ? range.index : range._ || null
        };
        return;
      }

      // OPTIMIZATION: For multiple ranges, read as single buffer and split by offsets
      const firstRange = groupedRange[0];
      const lastRange = groupedRange[groupedRange.length - 1];
      const totalSize = lastRange.end - firstRange.start;
      if (totalSize <= 0 || totalSize > _this.maxBufferSize) {
        throw new Error(`Invalid total buffer size: ${totalSize}. Start: ${firstRange.start}, End: ${lastRange.end}. Max allowed: ${_this.maxBufferSize}`);
      }

      // Read entire grouped range as single buffer
      const buffer = Buffer.allocUnsafe(totalSize);
      const {
        bytesRead
      } = yield _awaitAsyncGenerator(fd.read(buffer, 0, totalSize, firstRange.start));
      const actualBuffer = bytesRead < totalSize ? buffer.subarray(0, bytesRead) : buffer;
      if (actualBuffer.length === 0) return;

      // Convert to string once
      let content;
      try {
        content = actualBuffer.toString('utf8');
      } catch (error) {
        content = actualBuffer.toString('utf8', {
          replacement: '?'
        });
      }

      // CRITICAL FIX: Handle ranges more carefully to prevent corruption
      if (groupedRange.length === 2 && groupedRange[0].end === groupedRange[1].start) {
        // Special case: Adjacent ranges - split by newlines to prevent corruption
        const lines = content.split('\n').filter(line => line.trim().length > 0);
        for (let i = 0; i < Math.min(lines.length, groupedRange.length); i++) {
          const range = groupedRange[i];
          yield {
            line: lines[i],
            start: range.start,
            _: range.index !== undefined ? range.index : range._ || null
          };
        }
      } else {
        // CRITICAL FIX: For non-adjacent ranges, use the range.end directly
        // because range.end already excludes the newline (calculated as offsets[n+1] - 1)
        // We just need to find the line start (beginning of the line in the buffer)
        for (let i = 0; i < groupedRange.length; i++) {
          const range = groupedRange[i];
          const relativeStart = range.start - firstRange.start;
          const relativeEnd = range.end - firstRange.start;

          // OPTIMIZATION 2: Find line start only if necessary
          // Check if we're already at a line boundary to avoid unnecessary backwards search
          let lineStart = relativeStart;
          if (relativeStart > 0 && content[relativeStart - 1] !== '\n') {
            // Only search backwards if we're not already at a line boundary
            while (lineStart > 0 && content[lineStart - 1] !== '\n') {
              lineStart--;
            }
          }

          // OPTIMIZATION 3: Use slice() instead of substring() for better performance
          // CRITICAL FIX: range.end = offsets[n+1] - 1 points to the newline character
          // slice(start, end) includes characters from start to end-1 (end is exclusive)
          // So if relativeEnd points to the newline, slice will include it
          let rangeContent = content.slice(lineStart, relativeEnd);

          // OPTIMIZATION 4: Direct character check instead of regex/trimEnd
          // Remove trailing newlines and whitespace efficiently
          // trimEnd() is actually optimized in V8, but we can check if there's anything to trim first
          const len = rangeContent.length;
          if (len > 0) {
            // Quick check: if last char is not whitespace, skip trimEnd
            const lastChar = rangeContent[len - 1];
            if (lastChar === '\n' || lastChar === '\r' || lastChar === ' ' || lastChar === '\t') {
              // Only call trimEnd if we detected trailing whitespace
              rangeContent = rangeContent.trimEnd();
            }
          }
          if (rangeContent.length === 0) continue;
          yield {
            line: rangeContent,
            start: range.start,
            _: range.index !== undefined ? range.index : range._ || null
          };
        }
      }
    })();
  }
  walk(ranges) {
    var _this2 = this;
    return _wrapAsyncGenerator(function* () {
      // Check if file exists before trying to read it
      if (!(yield _awaitAsyncGenerator(_this2.exists()))) {
        return; // Return empty generator if file doesn't exist
      }
      const fd = yield _awaitAsyncGenerator(fs.promises.open(_this2.file, 'r'));
      try {
        const groupedRanges = yield _awaitAsyncGenerator(_this2.groupedRanges(ranges));
        for (const groupedRange of groupedRanges) {
          var _iteratorAbruptCompletion2 = false;
          var _didIteratorError2 = false;
          var _iteratorError2;
          try {
            for (var _iterator2 = _asyncIterator(_this2.readGroupedRange(groupedRange, fd)), _step2; _iteratorAbruptCompletion2 = !(_step2 = yield _awaitAsyncGenerator(_iterator2.next())).done; _iteratorAbruptCompletion2 = false) {
              const row = _step2.value;
              {
                yield row;
              }
            }
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
        }
      } finally {
        yield _awaitAsyncGenerator(fd.close());
      }
    })();
  }
  async replaceLines(ranges, lines) {
    // CRITICAL: Always use file mutex to prevent concurrent file operations
    if (this.fileMutex) {
      return this.fileMutex.runExclusive(async () => {
        // Add a small delay to ensure any pending operations complete
        await new Promise(resolve => setTimeout(resolve, 10));
        return this._replaceLinesInternal(ranges, lines);
      });
    } else {
      return this._replaceLinesInternal(ranges, lines);
    }
  }
  async _replaceLinesInternal(ranges, lines) {
    const tmpFile = this.file + '.tmp';
    let writer, reader;
    try {
      writer = await fs.promises.open(tmpFile, 'w+');

      // Check if the main file exists before trying to read it
      if (await this.exists()) {
        reader = await fs.promises.open(this.file, 'r');
      } else {
        // If file doesn't exist, we'll just write the new lines
        reader = null;
      }

      // Sort ranges by start position to ensure correct order
      const sortedRanges = [...ranges].sort((a, b) => a.start - b.start);
      let position = 0;
      let lineIndex = 0;
      for (const range of sortedRanges) {
        // Write existing content before the range (only if file exists)
        if (reader && position < range.start) {
          const buffer = await this.readRange(position, range.start);
          await writer.write(buffer);
        }

        // Write new line if provided, otherwise skip the range (for delete operations)
        if (lineIndex < lines.length && lines[lineIndex]) {
          const line = lines[lineIndex];
          // Ensure line ends with newline
          let formattedBuffer;
          if (Buffer.isBuffer(line)) {
            const needsNewline = line.length === 0 || line[line.length - 1] !== 0x0A;
            formattedBuffer = needsNewline ? Buffer.concat([line, Buffer.from('\n')]) : line;
          } else {
            const withNewline = line.endsWith('\n') ? line : line + '\n';
            formattedBuffer = Buffer.from(withNewline, 'utf8');
          }
          await writer.write(formattedBuffer);
        }

        // Update position to range.end to avoid overlapping writes
        position = range.end;
        lineIndex++;
      }

      // Write remaining content after the last range (only if file exists)
      if (reader) {
        const {
          size
        } = await reader.stat();
        if (position < size) {
          const buffer = await this.readRange(position, size);
          await writer.write(buffer);
        }
      }

      // Ensure all data is written to disk
      await writer.sync();
      if (reader) await reader.close();
      await writer.close();

      // Validate the temp file before renaming
      await this._validateTempFile(tmpFile);

      // CRITICAL: Retry logic for Windows EPERM errors
      await this._safeRename(tmpFile, this.file);
    } catch (e) {
      console.error('Erro ao substituir linhas:', e);
      throw e;
    } finally {
      if (reader) await reader.close().catch(() => {});
      if (writer) await writer.close().catch(() => {});
      await fs.promises.unlink(tmpFile).catch(() => {});
    }
  }
  async _safeRename(tmpFile, targetFile, maxRetries = 3) {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        await fs.promises.rename(tmpFile, targetFile);
        return; // Success
      } catch (error) {
        if (error.code === 'EPERM' && attempt < maxRetries) {
          // Quick delay: 50ms, 100ms, 200ms
          const delay = 50 * attempt;
          console.log(`🔄 EPERM retry ${attempt}/${maxRetries}, waiting ${delay}ms...`);
          await new Promise(resolve => setTimeout(resolve, delay));
          continue;
        }

        // If all retries failed, try Windows fallback approach
        if (error.code === 'EPERM' && attempt === maxRetries) {
          console.log(`⚠️ All EPERM retries failed, trying Windows fallback...`);
          return this._windowsFallbackRename(tmpFile, targetFile);
        }
        throw error; // Re-throw if not EPERM or max retries reached
      }
    }
  }
  async _validateTempFile(tmpFile) {
    try {
      // Read the temp file and validate JSON structure
      const content = await fs.promises.readFile(tmpFile, 'utf8');
      const lines = content.split('\n').filter(line => line.trim());
      let hasInvalidJson = false;
      const validLines = [];
      for (let i = 0; i < lines.length; i++) {
        try {
          JSON.parse(lines[i]);
          validLines.push(lines[i]);
        } catch (error) {
          console.warn(`⚠️ Invalid JSON in temp file at line ${i + 1}, skipping:`, lines[i].substring(0, 100));
          hasInvalidJson = true;
        }
      }

      // If we found invalid JSON, rewrite the file with only valid lines
      if (hasInvalidJson && validLines.length > 0) {
        console.log(`🔧 Rewriting temp file with ${validLines.length} valid lines`);
        const correctedContent = validLines.join('\n') + '\n';
        await fs.promises.writeFile(tmpFile, correctedContent, 'utf8');
      }
      console.log(`✅ Temp file validation passed: ${validLines.length} valid JSON lines`);
    } catch (error) {
      console.error(`❌ Temp file validation failed:`, error.message);
      throw error;
    }
  }
  async _windowsFallbackRename(tmpFile, targetFile) {
    try {
      // Windows fallback: copy content instead of rename
      console.log(`🔄 Using Windows fallback: copy + delete approach`);

      // Validate temp file before copying
      await this._validateTempFile(tmpFile);

      // Read the temp file content
      const content = await fs.promises.readFile(tmpFile, 'utf8');

      // Write to target file directly
      await fs.promises.writeFile(targetFile, content, 'utf8');

      // Delete temp file
      await fs.promises.unlink(tmpFile);
      console.log(`✅ Windows fallback successful`);
      return;
    } catch (fallbackError) {
      console.error(`❌ Windows fallback also failed:`, fallbackError);
      throw fallbackError;
    }
  }
  async writeData(data, immediate, fd) {
    await fd.write(data);
  }
  async writeDataAsync(data) {
    // CRITICAL FIX: Ensure directory exists before writing
    const dir = path.dirname(this.file);
    await fs.promises.mkdir(dir, {
      recursive: true
    });
    await fs.promises.appendFile(this.file, data);
  }

  /**
   * Check if data appears to be binary (always false since we only use JSON now)
   */
  isBinaryData(data) {
    // All data is now JSON format
    return false;
  }

  /**
   * Check if file is binary (always false since we only use JSON now)
   */
  async isBinaryFile() {
    // All files are now JSON format
    return false;
  }
  async readLastLine() {
    // Use global read limiter to prevent file descriptor exhaustion
    return this.readLimiter(async () => {
      // Check if file exists before trying to read it
      if (!(await this.exists())) {
        return null; // Return null if file doesn't exist
      }
      const reader = await fs.promises.open(this.file, 'r');
      try {
        const {
          size
        } = await reader.stat();
        if (size < 1) throw 'empty file';
        this.size = size;
        const bufferSize = 16384;
        let buffer,
          isFirstRead = true,
          lastReadSize,
          readPosition = Math.max(size - bufferSize, 0);
        while (readPosition >= 0) {
          const readSize = Math.min(bufferSize, size - readPosition);
          if (readSize !== lastReadSize) {
            lastReadSize = readSize;
            buffer = Buffer.alloc(readSize);
          }
          const {
            bytesRead
          } = await reader.read(buffer, 0, isFirstRead ? readSize - 1 : readSize, readPosition);
          if (isFirstRead) isFirstRead = false;
          if (bytesRead === 0) break;
          const newlineIndex = buffer.lastIndexOf(10);
          const start = readPosition + newlineIndex + 1;
          if (newlineIndex !== -1) {
            const lastLine = Buffer.alloc(size - start);
            await reader.read(lastLine, 0, size - start, start);
            if (!lastLine || !lastLine.length) {
              throw 'no metadata or empty file';
            }
            return lastLine;
          } else {
            readPosition -= bufferSize;
          }
        }
      } catch (e) {
        String(e).includes('empty file') || console.error('Error reading last line:', e);
      } finally {
        reader.close();
      }
    });
  }

  /**
   * Read records with streaming using readline
   * @param {Object} criteria - Filter criteria
   * @param {Object} options - Options (limit, skip)
   * @param {Function} matchesCriteria - Function to check if record matches criteria
   * @returns {Promise<Array>} - Array of records
   */
  async readWithStreaming(criteria, options = {}, matchesCriteria, serializer = null) {
    // CRITICAL: Always use file mutex to prevent concurrent file operations
    if (this.fileMutex) {
      return this.fileMutex.runExclusive(async () => {
        // Add a small delay to ensure any pending operations complete
        await new Promise(resolve => setTimeout(resolve, 5));
        // Use global read limiter to prevent file descriptor exhaustion
        return this.readLimiter(() => this._readWithStreamingInternal(criteria, options, matchesCriteria, serializer));
      });
    } else {
      // Use global read limiter to prevent file descriptor exhaustion
      return this.readLimiter(() => this._readWithStreamingInternal(criteria, options, matchesCriteria, serializer));
    }
  }
  async _readWithStreamingInternal(criteria, options = {}, matchesCriteria, serializer = null) {
    const {
      limit,
      skip = 0
    } = options; // No default limit
    const results = [];
    let lineNumber = 0;
    let processed = 0;
    let skipped = 0;
    let matched = 0;
    try {
      // Check if file exists before trying to read it
      if (!(await this.exists())) {
        return results; // Return empty results if file doesn't exist
      }

      // All files are now JSONL format - use line-by-line reading
      // Create optimized read stream
      const stream = fs.createReadStream(this.file, {
        highWaterMark: 64 * 1024,
        // 64KB chunks
        encoding: 'utf8'
      });

      // Create readline interface
      const rl = readline.createInterface({
        input: stream,
        crlfDelay: Infinity // Better performance
      });

      // Process line by line
      var _iteratorAbruptCompletion3 = false;
      var _didIteratorError3 = false;
      var _iteratorError3;
      try {
        for (var _iterator3 = _asyncIterator(rl), _step3; _iteratorAbruptCompletion3 = !(_step3 = await _iterator3.next()).done; _iteratorAbruptCompletion3 = false) {
          const line = _step3.value;
          {
            if (lineNumber >= skip) {
              try {
                let record;
                if (serializer && typeof serializer.deserialize === 'function') {
                  // Use serializer for deserialization
                  record = serializer.deserialize(line);
                } else {
                  // Fallback to JSON.parse for backward compatibility
                  record = JSON.parse(line);
                }
                if (record && matchesCriteria(record, criteria)) {
                  // Return raw data - term mapping will be handled by Database layer
                  results.push({
                    ...record,
                    _: lineNumber
                  });
                  matched++;

                  // Check if we've reached the limit
                  if (results.length >= limit) {
                    break;
                  }
                }
              } catch (error) {
                // CRITICAL FIX: Only log errors if they're not expected during concurrent operations
                // Don't log JSON parsing errors that occur during file writes
                if (this.opts && this.opts.debugMode && !error.message.includes('Unexpected')) {
                  console.log(`Error reading line ${lineNumber}:`, error.message);
                }
                // Ignore invalid lines - they may be partial writes
              }
            } else {
              skipped++;
            }
            lineNumber++;
            processed++;
          }
        }
      } catch (err) {
        _didIteratorError3 = true;
        _iteratorError3 = err;
      } finally {
        try {
          if (_iteratorAbruptCompletion3 && _iterator3.return != null) {
            await _iterator3.return();
          }
        } finally {
          if (_didIteratorError3) {
            throw _iteratorError3;
          }
        }
      }
      if (this.opts && this.opts.debugMode) {
        console.log(`📊 Streaming read completed: ${results.length} results, ${processed} processed, ${skipped} skipped, ${matched} matched`);
      }
      return results;
    } catch (error) {
      console.error('Error in readWithStreaming:', error);
      throw error;
    }
  }

  /**
   * Count records with streaming
   * @param {Object} criteria - Filter criteria
   * @param {Object} options - Options (limit)
   * @param {Function} matchesCriteria - Function to check if record matches criteria
   * @returns {Promise<number>} - Number of records
   */
  async countWithStreaming(criteria, options = {}, matchesCriteria, serializer = null) {
    const {
      limit
    } = options;
    let count = 0;
    let processed = 0;
    try {
      const stream = fs.createReadStream(this.file, {
        highWaterMark: 64 * 1024,
        encoding: 'utf8'
      });
      const rl = readline.createInterface({
        input: stream,
        crlfDelay: Infinity
      });
      var _iteratorAbruptCompletion4 = false;
      var _didIteratorError4 = false;
      var _iteratorError4;
      try {
        for (var _iterator4 = _asyncIterator(rl), _step4; _iteratorAbruptCompletion4 = !(_step4 = await _iterator4.next()).done; _iteratorAbruptCompletion4 = false) {
          const line = _step4.value;
          {
            if (limit && count >= limit) {
              break;
            }
            try {
              let record;
              if (serializer) {
                // Use serializer for deserialization
                record = await serializer.deserialize(line);
              } else {
                // Fallback to JSON.parse for backward compatibility
                record = JSON.parse(line);
              }
              if (record && matchesCriteria(record, criteria)) {
                count++;
              }
            } catch (error) {
              // Ignore invalid lines
            }
            processed++;
          }
        }
      } catch (err) {
        _didIteratorError4 = true;
        _iteratorError4 = err;
      } finally {
        try {
          if (_iteratorAbruptCompletion4 && _iterator4.return != null) {
            await _iterator4.return();
          }
        } finally {
          if (_didIteratorError4) {
            throw _iteratorError4;
          }
        }
      }
      return count;
    } catch (error) {
      throw error;
    }
  }

  /**
   * Get file statistics
   * @returns {Promise<Object>} - File statistics
   */
  async getFileStats() {
    try {
      const stats = await fs.promises.stat(this.file);
      const lineCount = await this.countLines();
      return {
        filePath: this.file,
        size: stats.size,
        lineCount,
        lastModified: stats.mtime
      };
    } catch (error) {
      throw error;
    }
  }

  /**
   * Count lines in file
   * @returns {Promise<number>} - Number of lines
   */
  async countLines() {
    let lineCount = 0;
    try {
      const stream = fs.createReadStream(this.file, {
        highWaterMark: 64 * 1024,
        encoding: 'utf8'
      });
      const rl = readline.createInterface({
        input: stream,
        crlfDelay: Infinity
      });
      var _iteratorAbruptCompletion5 = false;
      var _didIteratorError5 = false;
      var _iteratorError5;
      try {
        for (var _iterator5 = _asyncIterator(rl), _step5; _iteratorAbruptCompletion5 = !(_step5 = await _iterator5.next()).done; _iteratorAbruptCompletion5 = false) {
          const line = _step5.value;
          {
            lineCount++;
          }
        }
      } catch (err) {
        _didIteratorError5 = true;
        _iteratorError5 = err;
      } finally {
        try {
          if (_iteratorAbruptCompletion5 && _iterator5.return != null) {
            await _iterator5.return();
          }
        } finally {
          if (_didIteratorError5) {
            throw _iteratorError5;
          }
        }
      }
      return lineCount;
    } catch (error) {
      throw error;
    }
  }
  async destroy() {
    // CRITICAL FIX: Close all file handles to prevent resource leaks
    try {
      // Close any open file descriptors
      if (this.fd) {
        await this.fd.close().catch(() => {});
        this.fd = null;
      }

      // Close any open readers/writers
      if (this.reader) {
        await this.reader.close().catch(() => {});
        this.reader = null;
      }
      if (this.writer) {
        await this.writer.close().catch(() => {});
        this.writer = null;
      }

      // Clear any cached file handles
      this.cachedFd = null;
    } catch (error) {
      // Ignore errors during cleanup
    }
  }
  async delete() {
    try {
      // Delete main file
      await fs.promises.unlink(this.file).catch(() => {});

      // Delete index file (which now contains both index and offsets data)
      await fs.promises.unlink(this.indexFile).catch(() => {});
    } catch (error) {
      // Ignore errors if files don't exist
    }
  }
  async writeAll(data) {
    const release = this.fileMutex ? await this.fileMutex.acquire() : () => {};
    try {
      // Use Windows-specific retry logic for file operations
      await this._writeWithRetry(data);
    } finally {
      release();
    }
  }

  /**
   * Optimized batch write operation (OPTIMIZATION)
   * @param {Array} dataChunks - Array of data chunks to write
   * @param {boolean} append - Whether to append or overwrite
   */
  async writeBatch(dataChunks, append = false) {
    if (!dataChunks || !dataChunks.length) return;
    const release = this.fileMutex ? await this.fileMutex.acquire() : () => {};
    try {
      // OPTIMIZATION: Use streaming write for better performance
      if (dataChunks.length === 1 && Buffer.isBuffer(dataChunks[0])) {
        // Single buffer - use direct write
        if (append) {
          await fs.promises.appendFile(this.file, dataChunks[0]);
        } else {
          await this._writeFileWithRetry(this.file, dataChunks[0]);
        }
      } else {
        // Multiple chunks - use streaming approach
        await this._writeBatchStreaming(dataChunks, append);
      }
    } finally {
      release();
    }
  }

  /**
   * OPTIMIZATION: Streaming write for multiple chunks
   * @param {Array} dataChunks - Array of data chunks to write
   * @param {boolean} append - Whether to append or overwrite
   */
  async _writeBatchStreaming(dataChunks, append = false) {
    // OPTIMIZATION: Use createWriteStream for better performance
    const writeStream = fs.createWriteStream(this.file, {
      flags: append ? 'a' : 'w',
      highWaterMark: 64 * 1024 // 64KB buffer
    });
    return new Promise((resolve, reject) => {
      writeStream.on('error', reject);
      writeStream.on('finish', resolve);

      // Write chunks sequentially
      let index = 0;
      const writeNext = () => {
        if (index >= dataChunks.length) {
          writeStream.end();
          return;
        }
        const chunk = dataChunks[index++];
        const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk, 'utf8');
        if (!writeStream.write(buffer)) {
          writeStream.once('drain', writeNext);
        } else {
          writeNext();
        }
      };
      writeNext();
    });
  }

  /**
   * Optimized append operation for single data chunk (OPTIMIZATION)
   * @param {string|Buffer} data - Data to append
   */
  async appendOptimized(data) {
    const release = this.fileMutex ? await this.fileMutex.acquire() : () => {};
    try {
      // OPTIMIZATION: Direct append without retry logic for better performance
      await fs.promises.appendFile(this.file, data);
    } finally {
      release();
    }
  }

  /**
   * Windows-specific retry logic for fs.promises.writeFile operations
   * Based on node-graceful-fs workarounds for EPERM issues
   */
  async _writeFileWithRetry(filePath, data, maxRetries = 3) {
    const isWindows = process.platform === 'win32';
    for (let attempt = 0; attempt < maxRetries; attempt++) {
      try {
        // Ensure data is properly formatted as string or buffer
        if (Buffer.isBuffer(data)) {
          await fs.promises.writeFile(filePath, data);
        } else {
          await fs.promises.writeFile(filePath, data.toString());
        }

        // Windows: add small delay after write operation
        // This helps prevent EPERM issues caused by file handle not being released immediately
        if (isWindows) {
          await new Promise(resolve => setTimeout(resolve, 10));
        }

        // Success - return immediately
        return;
      } catch (err) {
        // Only retry on EPERM errors on Windows
        if (err.code === 'EPERM' && isWindows && attempt < maxRetries - 1) {
          // Exponential backoff: 10ms, 50ms, 250ms
          const delay = Math.pow(10, attempt + 1);
          await new Promise(resolve => setTimeout(resolve, delay));
          continue;
        }

        // Re-throw if not a retryable error or max retries reached
        throw err;
      }
    }
  }

  /**
   * Windows-specific retry logic for file operations
   * Based on node-graceful-fs workarounds for EPERM issues
   */
  async _writeWithRetry(data, maxRetries = 3) {
    const isWindows = process.platform === 'win32';
    for (let attempt = 0; attempt < maxRetries; attempt++) {
      try {
        // CRITICAL FIX: Ensure directory exists before writing file
        const dir = path.dirname(this.file);
        await fs.promises.mkdir(dir, {
          recursive: true
        });
        const fd = await fs.promises.open(this.file, 'w');
        try {
          // Ensure data is properly formatted as string or buffer
          if (Buffer.isBuffer(data)) {
            await fd.write(data);
          } else {
            await fd.write(data.toString());
          }
        } finally {
          await fd.close();

          // Windows: add small delay after closing file handle
          // This helps prevent EPERM issues caused by file handle not being released immediately
          if (isWindows) {
            await new Promise(resolve => setTimeout(resolve, 10));
          }
        }

        // Success - return immediately
        return;
      } catch (err) {
        // Only retry on EPERM errors on Windows
        if (err.code === 'EPERM' && isWindows && attempt < maxRetries - 1) {
          // Exponential backoff: 10ms, 50ms, 250ms
          const delay = Math.pow(10, attempt + 1);
          await new Promise(resolve => setTimeout(resolve, delay));
          continue;
        }

        // Re-throw if not a retryable error or max retries reached
        throw err;
      }
    }
  }
  async readAll() {
    const release = this.fileMutex ? await this.fileMutex.acquire() : () => {};
    try {
      // Check if file exists before trying to read it
      if (!(await this.exists())) {
        return ''; // Return empty string if file doesn't exist
      }
      const fd = await fs.promises.open(this.file, 'r');
      try {
        const stats = await fd.stat();
        const buffer = Buffer.allocUnsafe(stats.size);
        await fd.read(buffer, 0, stats.size, 0);
        return buffer.toString('utf8');
      } finally {
        await fd.close();
      }
    } finally {
      release();
    }
  }

  /**
   * Read specific lines from the file using line numbers
   * This is optimized for partial reads when using indexed queries
   * @param {number[]} lineNumbers - Array of line numbers to read (1-based)
   * @returns {Promise<string>} - Content of the specified lines
   */
  async readSpecificLines(lineNumbers) {
    if (!lineNumbers || lineNumbers.length === 0) {
      return '';
    }
    const release = this.fileMutex ? await this.fileMutex.acquire() : () => {};
    try {
      // Check if file exists before trying to read it
      if (!(await this.exists())) {
        return ''; // Return empty string if file doesn't exist
      }
      const fd = await fs.promises.open(this.file, 'r');
      try {
        const stats = await fd.stat();
        const buffer = Buffer.allocUnsafe(stats.size);
        await fd.read(buffer, 0, stats.size, 0);

        // CRITICAL FIX: Ensure proper UTF-8 decoding for multi-byte characters
        let content;
        try {
          content = buffer.toString('utf8');
        } catch (error) {
          // If UTF-8 decoding fails, try to recover by finding valid UTF-8 boundaries
          console.warn(`UTF-8 decoding failed for file ${this.file}, attempting recovery`);

          // Find the last complete UTF-8 character
          let validLength = buffer.length;
          for (let i = buffer.length - 1; i >= 0; i--) {
            const byte = buffer[i];
            // CRITICAL FIX: Correct UTF-8 start character detection
            // Check if this is the start of a UTF-8 character (not a continuation byte)
            if ((byte & 0x80) === 0 ||
            // ASCII (1 byte) - 0xxxxxxx
            (byte & 0xE0) === 0xC0 ||
            // 2-byte UTF-8 start - 110xxxxx
            (byte & 0xF0) === 0xE0 ||
            // 3-byte UTF-8 start - 1110xxxx
            (byte & 0xF8) === 0xF0) {
              // 4-byte UTF-8 start - 11110xxx
              validLength = i + 1;
              break;
            }
          }
          const validBuffer = buffer.subarray(0, validLength);
          content = validBuffer.toString('utf8');
        }

        // Split content into lines and extract only the requested lines
        const lines = content.split('\n');
        const result = [];
        for (const lineNum of lineNumbers) {
          // Convert to 0-based index and check bounds
          const index = lineNum - 1;
          if (index >= 0 && index < lines.length) {
            result.push(lines[index]);
          }
        }
        return result.join('\n');
      } finally {
        await fd.close();
      }
    } finally {
      release();
    }
  }
}

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

class QueryManager {
  constructor(database) {
    this.database = database;
    this.opts = database.opts;
    this.indexManager = database.indexManager;
    this.fileHandler = database.fileHandler;
    this.serializer = database.serializer;
    this.usageStats = database.usageStats || {
      totalQueries: 0,
      indexedQueries: 0,
      streamingQueries: 0,
      indexedAverageTime: 0,
      streamingAverageTime: 0
    };
  }

  /**
   * Main find method with strategy selection
   * @param {Object} criteria - Query criteria
   * @param {Object} options - Query options
   * @returns {Promise<Array>} - Query results
   */
  async find(criteria, options = {}) {
    if (this.database.destroyed) throw new Error('Database is destroyed');
    if (!this.database.initialized) await this.database.init();

    // Rebuild indexes if needed (when index was corrupted/missing)
    await this.database._rebuildIndexesIfNeeded();

    // Manual save is now the responsibility of the application

    // Preprocess query to handle array field syntax automatically
    const processedCriteria = this.preprocessQuery(criteria);
    const finalCriteria = processedCriteria;

    // Validate strict indexed mode before processing
    if (this.opts.indexedQueryMode === 'strict') {
      this.validateStrictQuery(finalCriteria, options);
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
    if (this.database.destroyed) throw new Error('Database is destroyed');
    if (!this.database.initialized) await this.database.init();
    // Manual save is now the responsibility of the application

    // Preprocess query to handle array field syntax automatically
    const processedCriteria = this.preprocessQuery(criteria);

    // Validate strict indexed mode before processing
    if (this.opts.indexedQueryMode === 'strict') {
      this.validateStrictQuery(processedCriteria, options);
    }
    const startTime = Date.now();
    this.usageStats.totalQueries++;
    try {
      // Decide which strategy to use
      const strategy = this.shouldUseStreaming(processedCriteria, options);
      let results = [];
      if (strategy === 'streaming') {
        results = await this.findWithStreaming(processedCriteria, {
          ...options,
          limit: 1
        });
        this.usageStats.streamingQueries++;
        this.updateAverageTime('streaming', Date.now() - startTime);
      } else {
        results = await this.findWithIndexed(processedCriteria, {
          ...options,
          limit: 1
        });
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
    if (this.database.destroyed) throw new Error('Database is destroyed');
    if (!this.database.initialized) await this.database.init();

    // Rebuild indexes if needed (when index was corrupted/missing)
    await this.database._rebuildIndexesIfNeeded();

    // Manual save is now the responsibility of the application

    // Validate strict indexed mode before processing
    if (this.opts.indexedQueryMode === 'strict') {
      this.validateStrictQuery(criteria, options);
    }

    // Use the same strategy as find method
    const strategy = this.shouldUseStreaming(criteria, options);
    let count = 0;
    if (strategy === 'streaming') {
      // Use streaming approach for non-indexed fields or large result sets
      const results = await this.findWithStreaming(criteria, options);
      count = results.length;
    } else {
      // OPTIMIZATION: For indexed strategy, use indexManager.query().size directly
      // This avoids reading actual records from the file - much faster!
      const lineNumbers = this.indexManager.query(criteria, options);
      if (lineNumbers.size === 0) {
        const missingIndexedFields = this._getIndexedFieldsWithMissingData(criteria);
        if (missingIndexedFields.length > 0 && this._hasAnyRecords()) {
          // Try to rebuild index before falling back to streaming (only if allowIndexRebuild is true)
          if (this.database.opts.allowIndexRebuild) {
            if (this.opts.debugMode) {
              console.log(`⚠️ Indexed count returned 0 because index data is missing for: ${missingIndexedFields.join(', ')}. Attempting index rebuild...`);
            }
            this.database._indexRebuildNeeded = true;
            await this.database._rebuildIndexesIfNeeded();

            // Retry indexed query after rebuild
            const retryLineNumbers = this.indexManager.query(criteria, options);
            if (retryLineNumbers.size > 0) {
              if (this.opts.debugMode) {
                console.log(`✅ Index rebuild successful, using indexed strategy.`);
              }
              count = retryLineNumbers.size;
            } else {
              // Still no results after rebuild, fall back to streaming
              if (this.opts.debugMode) {
                console.log(`⚠️ Index rebuild did not help, falling back to streaming count.`);
              }
              const streamingResults = await this.findWithStreaming(criteria, {
                ...options,
                forceFullScan: true
              });
              count = streamingResults.length;
            }
          } else {
            // allowIndexRebuild is false, fall back to streaming
            if (this.opts.debugMode) {
              console.log(`⚠️ Indexed count returned 0 because index data is missing for: ${missingIndexedFields.join(', ')}. Falling back to streaming count.`);
            }
            const streamingResults = await this.findWithStreaming(criteria, {
              ...options,
              forceFullScan: true
            });
            count = streamingResults.length;
          }
        } else {
          count = 0;
        }
      } else {
        count = lineNumbers.size;
      }
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
    return this.find(criteria, options);
  }

  /**
   * Find using streaming strategy with pre-filtering optimization
   * @param {Object} criteria - Query criteria
   * @param {Object} options - Query options
   * @returns {Promise<Array>} - Query results
   */
  async findWithStreaming(criteria, options = {}) {
    const streamingOptions = {
      ...options
    };
    const forceFullScan = streamingOptions.forceFullScan === true;
    delete streamingOptions.forceFullScan;
    if (this.opts.debugMode) {
      if (forceFullScan) {
        console.log('🌊 Using streaming strategy (forced full scan to bypass missing index data)');
      } else {
        console.log('🌊 Using streaming strategy');
      }
    }
    if (!forceFullScan) {
      // OPTIMIZATION: Try to use indices for pre-filtering when possible
      const indexableFields = this._getIndexableFields(criteria);
      if (indexableFields.length > 0) {
        if (this.opts.debugMode) {
          console.log(`🌊 Using pre-filtered streaming with ${indexableFields.length} indexable fields`);
        }

        // Use indices to pre-filter and reduce streaming scope
        const preFilteredLines = this.indexManager.query(this._extractIndexableCriteria(criteria), streamingOptions);

        // Stream only the pre-filtered records
        return this._streamPreFilteredRecords(preFilteredLines, criteria, streamingOptions);
      }
    }

    // Fallback to full streaming
    if (this.opts.debugMode) {
      console.log('🌊 Using full streaming (no indexable fields found or forced)');
    }
    return this._streamAllRecords(criteria, streamingOptions);
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
      const indexableAndConditions = criteria.$and.map(andCondition => this._extractIndexableCriteria(andCondition)).filter(condition => Object.keys(condition).length > 0);
      if (indexableAndConditions.length > 0) {
        indexableCriteria.$and = indexableAndConditions;
      }
    }

    // Handle $not operator - include it if it can be processed by IndexManager
    if (criteria.$not && typeof criteria.$not === 'object') {
      // Check if $not condition contains only indexable fields
      const notFields = Object.keys(criteria.$not);
      const allNotFieldsIndexed = notFields.every(field => this.indexManager.opts.indexes && this.indexManager.opts.indexes[field]);
      if (allNotFieldsIndexed && notFields.length > 0) {
        // Extract indexable criteria from $not condition
        const indexableNotCriteria = this._extractIndexableCriteria(criteria.$not);
        if (Object.keys(indexableNotCriteria).length > 0) {
          indexableCriteria.$not = indexableNotCriteria;
        }
      }
    }

    // Handle regular field conditions
    for (const [field, condition] of Object.entries(criteria)) {
      if (field.startsWith('$')) continue; // Skip logical operators (already handled above)

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
   * Determine whether the database currently has any records (persisted or pending)
   * @returns {boolean}
   */
  _hasAnyRecords() {
    if (!this.database) {
      return false;
    }
    if (Array.isArray(this.database.offsets) && this.database.offsets.length > 0) {
      return true;
    }
    if (Array.isArray(this.database.writeBuffer) && this.database.writeBuffer.length > 0) {
      return true;
    }
    if (typeof this.database.length === 'number' && this.database.length > 0) {
      return true;
    }
    return false;
  }

  /**
   * Extract all indexed fields referenced in the criteria
   * @param {Object} criteria
   * @param {Set<string>} accumulator
   * @returns {Array<string>}
   */
  _extractIndexedFields(criteria, accumulator = new Set()) {
    if (!criteria) {
      return Array.from(accumulator);
    }
    if (Array.isArray(criteria)) {
      for (const item of criteria) {
        this._extractIndexedFields(item, accumulator);
      }
      return Array.from(accumulator);
    }
    if (typeof criteria !== 'object') {
      return Array.from(accumulator);
    }
    for (const [key, value] of Object.entries(criteria)) {
      if (key.startsWith('$')) {
        this._extractIndexedFields(value, accumulator);
        continue;
      }
      accumulator.add(key);
      if (Array.isArray(value)) {
        for (const nested of value) {
          this._extractIndexedFields(nested, accumulator);
        }
      }
    }
    return Array.from(accumulator);
  }

  /**
   * Identify indexed fields present in criteria whose index data is missing
   * @param {Object} criteria
   * @returns {Array<string>}
   */
  _getIndexedFieldsWithMissingData(criteria) {
    if (!this.indexManager || !criteria) {
      return [];
    }
    const indexedFields = this._extractIndexedFields(criteria);
    const missing = [];
    for (const field of indexedFields) {
      if (!this.indexManager.isFieldIndexed(field)) {
        continue;
      }
      if (!this.indexManager.hasUsableIndexData(field)) {
        missing.push(field);
      }
    }
    return missing;
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
          var _iteratorAbruptCompletion = false;
          var _didIteratorError = false;
          var _iteratorError;
          try {
            for (var _iterator = _asyncIterator(this.fileHandler.readGroupedRange(groupedRange, fd)), _step; _iteratorAbruptCompletion = !(_step = await _iterator.next()).done; _iteratorAbruptCompletion = false) {
              const row = _step.value;
              {
                if (row.line && row.line.trim()) {
                  try {
                    // CRITICAL FIX: Use serializer.deserialize instead of JSON.parse to handle array format
                    const record = this.database.serializer.deserialize(row.line);

                    // OPTIMIZATION 4: Use optimized criteria matching for pre-filtered records
                    if (this._matchesCriteriaOptimized(record, criteria, options)) {
                      // SPACE OPTIMIZATION: Restore term IDs to terms for user (unless disabled)
                      const recordWithTerms = options.restoreTerms !== false ? this.database.restoreTermIdsAfterDeserialization(record) : record;
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
          } catch (err) {
            _didIteratorError = true;
            _iteratorError = err;
          } finally {
            try {
              if (_iteratorAbruptCompletion && _iterator.return != null) {
                await _iterator.return();
              }
            } finally {
              if (_didIteratorError) {
                throw _iteratorError;
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
    const streamingOptions = {
      ...options,
      limit: memoryLimit
    };
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
    let results = [];
    const limit = options.limit; // No default limit - return all results unless explicitly limited

    // Use IndexManager to get line numbers, then read specific records
    const lineNumbers = this.indexManager.query(criteria, options);
    if (this.opts.debugMode) {
      console.log(`🔍 IndexManager returned ${lineNumbers.size} line numbers:`, Array.from(lineNumbers));
    }
    if (lineNumbers.size === 0) {
      const missingIndexedFields = this._getIndexedFieldsWithMissingData(criteria);
      if (missingIndexedFields.length > 0 && this._hasAnyRecords()) {
        // Try to rebuild index before falling back to streaming (only if allowIndexRebuild is true)
        if (this.database.opts.allowIndexRebuild) {
          if (this.opts.debugMode) {
            console.log(`⚠️ Indexed query returned no results because index data is missing for: ${missingIndexedFields.join(', ')}. Attempting index rebuild...`);
          }
          this.database._indexRebuildNeeded = true;
          await this.database._rebuildIndexesIfNeeded();

          // Retry indexed query after rebuild
          const retryLineNumbers = this.indexManager.query(criteria, options);
          if (retryLineNumbers.size > 0) {
            if (this.opts.debugMode) {
              console.log(`✅ Index rebuild successful, using indexed strategy.`);
            }
            // Update lineNumbers to use rebuilt index results
            lineNumbers.clear();
            for (const lineNumber of retryLineNumbers) {
              lineNumbers.add(lineNumber);
            }
          } else {
            // Still no results after rebuild, fall back to streaming
            if (this.opts.debugMode) {
              console.log(`⚠️ Index rebuild did not help, falling back to streaming.`);
            }
            return this.findWithStreaming(criteria, {
              ...options,
              forceFullScan: true
            });
          }
        } else {
          // allowIndexRebuild is false, fall back to streaming
          if (this.opts.debugMode) {
            console.log(`⚠️ Indexed query returned no results because index data is missing for: ${missingIndexedFields.join(', ')}. Falling back to streaming.`);
          }
          return this.findWithStreaming(criteria, {
            ...options,
            forceFullScan: true
          });
        }
      }
    }

    // Read specific records using the line numbers
    if (lineNumbers.size > 0) {
      const lineNumbersArray = Array.from(lineNumbers);
      const persistedCount = Array.isArray(this.database.offsets) ? this.database.offsets.length : 0;

      // Separate lineNumbers into file records and writeBuffer records
      const fileLineNumbers = [];
      const writeBufferLineNumbers = [];
      for (const lineNumber of lineNumbersArray) {
        if (lineNumber >= persistedCount) {
          // This lineNumber points to writeBuffer
          writeBufferLineNumbers.push(lineNumber);
        } else {
          // This lineNumber points to file
          fileLineNumbers.push(lineNumber);
        }
      }

      // Read records from file
      if (fileLineNumbers.length > 0) {
        const ranges = this.database.getRanges(fileLineNumbers);
        if (ranges.length > 0) {
          const groupedRanges = await this.database.fileHandler.groupedRanges(ranges);
          const fs = await import('fs');
          const fd = await fs.promises.open(this.database.fileHandler.file, 'r');
          try {
            for (const groupedRange of groupedRanges) {
              var _iteratorAbruptCompletion2 = false;
              var _didIteratorError2 = false;
              var _iteratorError2;
              try {
                for (var _iterator2 = _asyncIterator(this.database.fileHandler.readGroupedRange(groupedRange, fd)), _step2; _iteratorAbruptCompletion2 = !(_step2 = await _iterator2.next()).done; _iteratorAbruptCompletion2 = false) {
                  const row = _step2.value;
                  {
                    try {
                      const record = this.database.serializer.deserialize(row.line);
                      const recordWithTerms = options.restoreTerms !== false ? this.database.restoreTermIdsAfterDeserialization(record) : record;
                      results.push(recordWithTerms);
                      if (limit && results.length >= limit) break;
                    } catch (error) {
                      // Skip invalid lines
                    }
                  }
                }
              } catch (err) {
                _didIteratorError2 = true;
                _iteratorError2 = err;
              } finally {
                try {
                  if (_iteratorAbruptCompletion2 && _iterator2.return != null) {
                    await _iterator2.return();
                  }
                } finally {
                  if (_didIteratorError2) {
                    throw _iteratorError2;
                  }
                }
              }
              if (limit && results.length >= limit) break;
            }
          } finally {
            await fd.close();
          }
        }
      }

      // Read records from writeBuffer
      if (writeBufferLineNumbers.length > 0 && this.database.writeBuffer) {
        for (const lineNumber of writeBufferLineNumbers) {
          if (limit && results.length >= limit) break;
          const writeBufferIndex = lineNumber - persistedCount;
          if (writeBufferIndex >= 0 && writeBufferIndex < this.database.writeBuffer.length) {
            const record = this.database.writeBuffer[writeBufferIndex];
            if (record) {
              const recordWithTerms = options.restoreTerms !== false ? this.database.restoreTermIdsAfterDeserialization(record) : record;
              results.push(recordWithTerms);
            }
          }
        }
      }
    }
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
      console.log(`🔍 Checking field '${field}':`, {
        value,
        condition,
        record: record.name || record.id
      });
    }

    // Debug logging for term mapping fields
    if (this.database.opts.termMapping && Object.keys(this.database.opts.indexes || {}).includes(field)) {
      if (this.database.opts.debugMode) {
        console.log(`🔍 Checking term mapping field '${field}':`, {
          value,
          condition,
          record: record.name || record.id
        });
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
        const normalizedOperator = normalizeOperator(operator);
        if (!this.matchesOperator(value, normalizedOperator, operatorValue, options)) {
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
      case '$eq':
        return value === operatorValue;
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
        return operatorValue ? value !== undefined && value !== null : value === undefined || value === null;
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
      return criteria;
    }
    const processed = {};
    for (const [field, value] of Object.entries(criteria)) {
      // Check if this is a term mapping field
      const isTermMappingField = this.database.opts.termMapping && this.database.termManager && this.database.termManager.termMappingFields && this.database.termManager.termMappingFields.includes(field);
      if (isTermMappingField) {
        // Handle term mapping field queries
        if (typeof value === 'string') {
          // Convert term to $in query for term mapping fields
          processed[field] = {
            $in: [value]
          };
        } else if (Array.isArray(value)) {
          // Convert array to $in query
          processed[field] = {
            $in: value
          };
        } else if (value && typeof value === 'object') {
          // Handle special query operators for term mapping
          if (value.$in) {
            processed[field] = {
              $in: value.$in
            };
          } else if (value.$all) {
            processed[field] = {
              $all: value.$all
            };
          } else {
            processed[field] = value;
          }
        } else {
          // Invalid value for term mapping field
          throw new Error(`Invalid query for array field '${field}'. Use { $in: [value] } syntax or direct value.`);
        }
        if (this.database.opts.debugMode) {
          console.log(`🔍 Processed term mapping query for field '${field}':`, processed[field]);
        }
      } else {
        // Check if this field is defined as an array in the schema
        const indexes = this.opts.indexes || {};
        const fieldConfig = indexes[field];
        const isArrayField = fieldConfig && (Array.isArray(fieldConfig) && fieldConfig.includes('array') || fieldConfig === 'array:string' || fieldConfig === 'array:number' || fieldConfig === 'array:boolean');
        if (isArrayField) {
          // Handle array field queries
          if (typeof value === 'string' || typeof value === 'number') {
            // Convert direct value to $in query for array fields
            processed[field] = {
              $in: [value]
            };
          } else if (Array.isArray(value)) {
            // Convert array to $in query
            processed[field] = {
              $in: value
            };
          } else if (value && typeof value === 'object') {
            // Already properly formatted query object
            processed[field] = value;
          } else {
            // Invalid value for array field
            throw new Error(`Invalid query for array field '${field}'. Use { $in: [value] } syntax or direct value.`);
          }
        } else {
          // Non-array field, keep as is
          processed[field] = value;
        }
      }
    }
    return processed;
  }

  /**
   * Determine which query strategy to use
   * @param {Object} criteria - Query criteria
   * @param {Object} options - Query options
   * @returns {string} - 'streaming' or 'indexed'
   */
  shouldUseStreaming(criteria, options = {}) {
    const {
      limit
    } = options; // No default limit
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
      const notFields = Object.keys(criteria.$not);
      const allNotFieldsIndexed = notFields.every(field => this.indexManager.opts.indexes && this.indexManager.opts.indexes[field]);
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
            const operators = Object.keys(condition).map(op => normalizeOperator(op));
            const indexType = this.indexManager?.opts?.indexes?.[field];
            const isNumericIndex = indexType === 'number' || indexType === 'auto' || indexType === 'array:number';
            const disallowedForNumeric = ['$all', '$in', '$not', '$regex', '$contains', '$exists', '$size'];
            const disallowedDefault = ['$all', '$in', '$gt', '$gte', '$lt', '$lte', '$ne', '$not', '$regex', '$contains', '$exists', '$size'];

            // Check if this is a term mapping field (array:string or string fields with term mapping)
            const isTermMappingField = this.database.termManager && this.database.termManager.termMappingFields && this.database.termManager.termMappingFields.includes(field);
            if (isTermMappingField) {
              const termMappingDisallowed = ['$gt', '$gte', '$lt', '$lte', '$ne', '$regex', '$contains', '$exists', '$size'];
              return operators.every(op => !termMappingDisallowed.includes(op));
            } else {
              const disallowed = isNumericIndex ? disallowedForNumeric : disallowedDefault;
              return operators.every(op => !disallowed.includes(op));
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
      // Skip $and and $not as they're handled separately above
      if (field === '$and' || field === '$not') return true;
      if (!this.opts.indexes || !this.opts.indexes[field]) {
        if (this.opts.debugMode) {
          console.log(`🔍 Field '${field}' not indexed. Available indexes:`, Object.keys(this.opts.indexes || {}));
        }
        return false;
      }

      // Check if the field uses operators that are supported by IndexManager
      const condition = criteria[field];

      // RegExp cannot be efficiently queried using indices - must use streaming
      if (condition instanceof RegExp) {
        if (this.opts.debugMode) {
          console.log(`🔍 Field '${field}' uses RegExp - requires streaming strategy`);
        }
        return false;
      }
      if (typeof condition === 'object' && !Array.isArray(condition) && condition !== null) {
        const operators = Object.keys(condition).map(op => normalizeOperator(op));
        if (this.opts.debugMode) {
          console.log(`🔍 Field '${field}' has operators:`, operators);
        }
        const indexType = this.indexManager?.opts?.indexes?.[field];
        const isNumericIndex = indexType === 'number' || indexType === 'auto' || indexType === 'array:number';
        const isArrayStringIndex = indexType === 'array:string';
        const disallowedForNumeric = ['$all', '$in', '$not', '$regex', '$contains', '$exists', '$size'];
        const disallowedDefault = ['$all', '$in', '$gt', '$gte', '$lt', '$lte', '$ne', '$not', '$regex', '$contains', '$exists', '$size'];

        // Check if this is a term mapping field (array:string or string fields with term mapping)
        const isTermMappingField = this.database.termManager && this.database.termManager.termMappingFields && this.database.termManager.termMappingFields.includes(field);

        // With term mapping enabled on THIS FIELD, we can support complex operators via partial reads
        // Also support $all for array:string indexed fields (IndexManager.query supports it via Set intersection)
        if (isTermMappingField) {
          const termMappingDisallowed = ['$gt', '$gte', '$lt', '$lte', '$ne', '$regex', '$contains', '$exists', '$size'];
          return operators.every(op => !termMappingDisallowed.includes(op));
        } else {
          let disallowed = isNumericIndex ? disallowedForNumeric : disallowedDefault;
          // Remove $all from disallowed if field is array:string (IndexManager supports $all via Set intersection)
          if (isArrayStringIndex) {
            disallowed = disallowed.filter(op => op !== '$all');
          }
          return operators.every(op => !disallowed.includes(op));
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
        return Math.floor(totalRecords * 0.1);
      // Assume 10% of records match
      case '$regex':
        // Regex is hard to estimate without scanning
        return Math.floor(totalRecords * 0.05);
      // Assume 5% of records match
    }
    return 0;
  }

  /**
   * Validate strict query mode
   * @param {Object} criteria - Query criteria
   * @param {Object} options - Query options
   */
  validateStrictQuery(criteria, options = {}) {
    // Allow bypassing strict mode validation with allowNonIndexed option
    if (options.allowNonIndexed === true) {
      return; // Skip validation for this query
    }
    if (!criteria || Object.keys(criteria).length === 0) {
      return; // Empty criteria are always allowed
    }

    // Handle logical operators at the top level
    if (criteria.$not) {
      this.validateStrictQuery(criteria.$not, options);
      return;
    }
    if (criteria.$or && Array.isArray(criteria.$or)) {
      for (const orCondition of criteria.$or) {
        this.validateStrictQuery(orCondition, options);
      }
      return;
    }
    if (criteria.$and && Array.isArray(criteria.$and)) {
      for (const andCondition of criteria.$and) {
        this.validateStrictQuery(andCondition, options);
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
        if (typeof condition === 'string' || typeof condition === 'object' && condition !== null && condition.$in && Array.isArray(condition.$in)) {
          return true;
        }
      }
    }
    return false;
  }

  // Simplified term mapping - handled in TermManager
}

/**
 * ConcurrencyManager - Handles all concurrency control and synchronization
 * 
 * Responsibilities:
 * - _acquireMutexWithTimeout()
 * - Mutex and fileMutex management
 * - Concurrent operations control
 */

class ConcurrencyManager {
  constructor(database) {
    this.database = database;
    this.opts = database.opts;
    this.mutex = database.mutex;
    this.fileMutex = database.fileMutex;
    this.operationQueue = database.operationQueue;
    this.pendingOperations = database.pendingOperations || 0;
    this.pendingPromises = database.pendingPromises || new Set();
  }

  /**
   * Acquire mutex with timeout
   * @param {Mutex} mutex - Mutex to acquire
   * @param {number} timeout - Timeout in milliseconds
   * @returns {Promise<Function>} - Release function
   */
  async _acquireMutexWithTimeout(mutex, timeout = null) {
    const timeoutMs = timeout || this.opts.mutexTimeout;
    const startTime = Date.now();
    try {
      const release = await Promise.race([mutex.acquire(), new Promise((_, reject) => setTimeout(() => reject(new Error(`Mutex acquisition timeout after ${timeoutMs}ms`)), timeoutMs))]);
      if (this.opts.debugMode) {
        const acquireTime = Date.now() - startTime;
        if (acquireTime > 1000) {
          console.warn(`⚠️ Slow mutex acquisition: ${acquireTime}ms`);
        }
      }

      // Wrap release function to track mutex usage
      const originalRelease = release;
      return () => {
        try {
          originalRelease();
        } catch (error) {
          console.error(`❌ Error releasing mutex: ${error.message}`);
        }
      };
    } catch (error) {
      if (this.opts.debugMode) {
        console.error(`❌ Mutex acquisition failed: ${error.message}`);
      }
      throw error;
    }
  }

  /**
   * Execute operation with queue management
   * @param {Function} operation - Operation to execute
   * @returns {Promise} - Operation result
   */
  async executeWithQueue(operation) {
    if (!this.operationQueue) {
      return operation();
    }
    return this.operationQueue.enqueue(operation);
  }

  /**
   * Wait for all pending operations to complete
   * @returns {Promise<void>}
   */
  async waitForPendingOperations() {
    if (this.pendingOperations === 0) {
      return;
    }
    const pendingPromisesArray = Array.from(this.pendingPromises);
    if (pendingPromisesArray.length === 0) {
      return;
    }
    try {
      await Promise.allSettled(pendingPromisesArray);
      this.pendingPromises.clear();
      this.pendingOperations = 0;
    } catch (error) {
      console.warn('Error waiting for pending operations:', error);
      this.pendingPromises.clear();
      this.pendingOperations = 0;
    }
  }

  /**
   * Get concurrency statistics
   * @returns {Object} - Concurrency statistics
   */
  getConcurrencyStats() {
    return {
      pendingOperations: this.pendingOperations,
      pendingPromises: this.pendingPromises.size,
      mutexTimeout: this.opts.mutexTimeout,
      hasOperationQueue: !!this.operationQueue
    };
  }

  /**
   * Check if system is under high concurrency load
   * @returns {boolean} - True if under high load
   */
  isUnderHighLoad() {
    const maxOperations = this.opts.maxConcurrentOperations || 10;
    return this.pendingOperations >= maxOperations * 0.8; // 80% of max capacity
  }

  /**
   * Get recommended timeout based on current load
   * @returns {number} - Recommended timeout in milliseconds
   */
  getRecommendedTimeout() {
    const baseTimeout = this.opts.mutexTimeout || 15000; // Reduced from 30000 to 15000
    const loadFactor = this.pendingOperations / 10; // Use fixed limit of 10

    // Increase timeout based on load
    return Math.min(baseTimeout * (1 + loadFactor), baseTimeout * 3);
  }

  /**
   * Acquire multiple mutexes in order to prevent deadlocks
   * @param {Array<Mutex>} mutexes - Mutexes to acquire in order
   * @param {number} timeout - Timeout in milliseconds
   * @returns {Promise<Array<Function>>} - Array of release functions
   */
  async acquireMultipleMutexes(mutexes, timeout = null) {
    const releases = [];
    try {
      for (const mutex of mutexes) {
        const release = await this._acquireMutexWithTimeout(mutex, timeout);
        releases.push(release);
      }
      return releases;
    } catch (error) {
      // Release already acquired mutexes on error
      for (const release of releases) {
        try {
          release();
        } catch (releaseError) {
          console.warn('Error releasing mutex:', releaseError);
        }
      }
      throw error;
    }
  }

  /**
   * Execute operation with automatic mutex management
   * @param {Function} operation - Operation to execute
   * @param {Object} options - Options for execution
   * @returns {Promise} - Operation result
   */
  async executeWithMutex(operation, options = {}) {
    const {
      mutex = this.mutex,
      timeout = null,
      retries = 0,
      retryDelay = 100
    } = options;
    let lastError = null;
    for (let attempt = 0; attempt <= retries; attempt++) {
      try {
        const release = await this._acquireMutexWithTimeout(mutex, timeout);
        try {
          const result = await operation();
          return result;
        } finally {
          release();
        }
      } catch (error) {
        lastError = error;
        if (attempt < retries) {
          // Wait before retry with exponential backoff
          const delay = retryDelay * Math.pow(2, attempt);
          await new Promise(resolve => setTimeout(resolve, delay));
        }
      }
    }
    throw lastError;
  }

  /**
   * Create a semaphore for limiting concurrent operations
   * @param {number} limit - Maximum concurrent operations
   * @returns {Object} - Semaphore object
   */
  createSemaphore(limit) {
    let current = 0;
    const queue = [];
    return {
      async acquire() {
        return new Promise(resolve => {
          if (current < limit) {
            current++;
            resolve();
          } else {
            queue.push(resolve);
          }
        });
      },
      release() {
        if (queue.length > 0) {
          const next = queue.shift();
          next();
        } else {
          current--;
        }
      },
      getCurrent() {
        return current;
      },
      getQueueLength() {
        return queue.length;
      }
    };
  }

  /**
   * Cleanup concurrency resources
   */
  cleanup() {
    this.pendingPromises.clear();
    this.pendingOperations = 0;
    if (this.opts.debugMode) {
      console.log('🧹 Concurrency manager cleaned up');
    }
  }
}

/**
 * StatisticsManager - Handles all statistics and metrics collection
 * 
 * Responsibilities:
 * - getJournalStats()
 * - Performance metrics
 * - Usage statistics
 */

class StatisticsManager {
  constructor(database) {
    this.database = database;
    this.opts = database.opts;
    this.usageStats = database.usageStats || {
      totalQueries: 0,
      streamingQueries: 0,
      indexedQueries: 0,
      streamingAverageTime: 0,
      indexedAverageTime: 0
    };
    this.performanceMetrics = {
      startTime: Date.now(),
      lastResetTime: Date.now(),
      totalOperations: 0,
      totalErrors: 0,
      averageOperationTime: 0,
      peakMemoryUsage: 0,
      cacheHits: 0,
      cacheMisses: 0
    };
  }

  /**
   * Get journal statistics
   * @returns {Object} - Journal statistics
   */
  getJournalStats() {
    return {
      enabled: false,
      message: 'Journal mode has been removed'
    };
  }

  /**
   * Get performance metrics
   * @returns {Object} - Performance metrics
   */
  getPerformanceMetrics() {
    const now = Date.now();
    const uptime = now - this.performanceMetrics.startTime;
    return {
      uptime: uptime,
      totalOperations: this.performanceMetrics.totalOperations,
      totalErrors: this.performanceMetrics.totalErrors,
      averageOperationTime: this.performanceMetrics.averageOperationTime,
      operationsPerSecond: this.performanceMetrics.totalOperations / (uptime / 1000),
      errorRate: this.performanceMetrics.totalErrors / Math.max(1, this.performanceMetrics.totalOperations),
      peakMemoryUsage: this.performanceMetrics.peakMemoryUsage,
      cacheHitRate: this.performanceMetrics.cacheHits / Math.max(1, this.performanceMetrics.cacheHits + this.performanceMetrics.cacheMisses),
      lastResetTime: this.performanceMetrics.lastResetTime
    };
  }

  /**
   * Get usage statistics
   * @returns {Object} - Usage statistics
   */
  getUsageStats() {
    return {
      totalQueries: this.usageStats.totalQueries,
      streamingQueries: this.usageStats.streamingQueries,
      indexedQueries: this.usageStats.indexedQueries,
      streamingAverageTime: this.usageStats.streamingAverageTime,
      indexedAverageTime: this.usageStats.indexedAverageTime,
      queryDistribution: {
        streaming: this.usageStats.streamingQueries / Math.max(1, this.usageStats.totalQueries),
        indexed: this.usageStats.indexedQueries / Math.max(1, this.usageStats.totalQueries)
      }
    };
  }

  /**
   * Get database statistics
   * @returns {Object} - Database statistics
   */
  getDatabaseStats() {
    return {
      totalRecords: this.database.offsets?.length || 0,
      indexOffset: this.database.indexOffset || 0,
      writeBufferSize: this.database.writeBuffer?.length || 0,
      indexedFields: Object.keys(this.database.indexManager?.index?.data || {}),
      totalIndexedFields: Object.keys(this.database.indexManager?.index?.data || {}).length,
      isInitialized: this.database.initialized || false,
      isDestroyed: this.database.destroyed || false
    };
  }

  /**
   * Get comprehensive statistics
   * @returns {Object} - All statistics combined
   */
  getComprehensiveStats() {
    return {
      database: this.getDatabaseStats(),
      performance: this.getPerformanceMetrics(),
      usage: this.getUsageStats(),
      journal: this.getJournalStats(),
      timestamp: Date.now()
    };
  }

  /**
   * Record operation performance
   * @param {string} operation - Operation name
   * @param {number} duration - Duration in milliseconds
   * @param {boolean} success - Whether operation was successful
   */
  recordOperation(operation, duration, success = true) {
    this.performanceMetrics.totalOperations++;
    if (!success) {
      this.performanceMetrics.totalErrors++;
    }

    // Update average operation time
    const currentAverage = this.performanceMetrics.averageOperationTime;
    const totalOps = this.performanceMetrics.totalOperations;
    this.performanceMetrics.averageOperationTime = (currentAverage * (totalOps - 1) + duration) / totalOps;

    // Update peak memory usage (if available)
    if (typeof process !== 'undefined' && process.memoryUsage) {
      const memoryUsage = process.memoryUsage();
      this.performanceMetrics.peakMemoryUsage = Math.max(this.performanceMetrics.peakMemoryUsage, memoryUsage.heapUsed);
    }
  }

  /**
   * Record cache hit
   */
  recordCacheHit() {
    this.performanceMetrics.cacheHits++;
  }

  /**
   * Record cache miss
   */
  recordCacheMiss() {
    this.performanceMetrics.cacheMisses++;
  }

  /**
   * Update query statistics
   * @param {string} type - Query type ('streaming' or 'indexed')
   * @param {number} duration - Query duration in milliseconds
   */
  updateQueryStats(type, duration) {
    this.usageStats.totalQueries++;
    if (type === 'streaming') {
      this.usageStats.streamingQueries++;
      this.updateAverageTime('streaming', duration);
    } else if (type === 'indexed') {
      this.usageStats.indexedQueries++;
      this.updateAverageTime('indexed', duration);
    }
  }

  /**
   * Update average time for a query type
   * @param {string} type - Query type
   * @param {number} time - Time taken
   */
  updateAverageTime(type, time) {
    const key = `${type}AverageTime`;
    if (!this.usageStats[key]) {
      this.usageStats[key] = 0;
    }
    const currentAverage = this.usageStats[key];
    const count = this.usageStats[`${type}Queries`] || 1;

    // Calculate running average
    this.usageStats[key] = (currentAverage * (count - 1) + time) / count;
  }

  /**
   * Reset all statistics
   */
  resetStats() {
    this.usageStats = {
      totalQueries: 0,
      streamingQueries: 0,
      indexedQueries: 0,
      streamingAverageTime: 0,
      indexedAverageTime: 0
    };
    this.performanceMetrics = {
      startTime: Date.now(),
      lastResetTime: Date.now(),
      totalOperations: 0,
      totalErrors: 0,
      averageOperationTime: 0,
      peakMemoryUsage: 0,
      cacheHits: 0,
      cacheMisses: 0
    };
    if (this.opts.debugMode) {
      console.log('📊 Statistics reset');
    }
  }

  /**
   * Export statistics to JSON
   * @returns {string} - JSON string of statistics
   */
  exportStats() {
    return JSON.stringify(this.getComprehensiveStats(), null, 2);
  }

  /**
   * Get statistics summary for logging
   * @returns {string} - Summary string
   */
  getStatsSummary() {
    const stats = this.getComprehensiveStats();
    return `
📊 Database Statistics Summary:
  Records: ${stats.database.totalRecords}
  Queries: ${stats.usage.totalQueries} (${Math.round(stats.usage.queryDistribution.streaming * 100)}% streaming, ${Math.round(stats.usage.queryDistribution.indexed * 100)}% indexed)
  Operations: ${stats.performance.totalOperations}
  Errors: ${stats.performance.totalErrors}
  Uptime: ${Math.round(stats.performance.uptime / 1000)}s
  Cache Hit Rate: ${Math.round(stats.performance.cacheHitRate * 100)}%
    `.trim();
  }

  /**
   * Check if statistics collection is enabled
   * @returns {boolean} - True if enabled
   */
  isEnabled() {
    return this.opts.collectStatistics !== false;
  }

  /**
   * Enable or disable statistics collection
   * @param {boolean} enabled - Whether to enable statistics
   */
  setEnabled(enabled) {
    this.opts.collectStatistics = enabled;
    if (this.opts.debugMode) {
      console.log(`📊 Statistics collection ${enabled ? 'enabled' : 'disabled'}`);
    }
  }
}

/**
 * StreamingProcessor - Efficient streaming processing for large datasets
 * 
 * Features:
 * - Memory-efficient processing of large files
 * - Configurable batch sizes
 * - Progress tracking
 * - Error handling and recovery
 * - Transform pipelines
 * - Backpressure control
 */
class StreamingProcessor extends events.EventEmitter {
  constructor(opts = {}) {
    super();
    this.opts = {
      batchSize: opts.batchSize || 1000,
      maxConcurrency: opts.maxConcurrency || 5,
      bufferSize: opts.bufferSize || 64 * 1024,
      // 64KB
      enableProgress: opts.enableProgress !== false,
      progressInterval: opts.progressInterval || 1000,
      // 1 second
      enableBackpressure: opts.enableBackpressure !== false,
      maxPendingBatches: opts.maxPendingBatches || 10,
      ...opts
    };
    this.isProcessing = false;
    this.currentBatch = 0;
    this.totalBatches = 0;
    this.processedItems = 0;
    this.totalItems = 0;
    this.pendingBatches = 0;
    this.stats = {
      startTime: 0,
      endTime: 0,
      totalProcessingTime: 0,
      averageBatchTime: 0,
      itemsPerSecond: 0,
      memoryUsage: 0
    };
    this.progressTimer = null;
    this.transformPipeline = [];
  }

  /**
   * Add a transform function to the pipeline
   */
  addTransform(transformFn) {
    this.transformPipeline.push(transformFn);
    return this;
  }

  /**
   * Process a file stream
   */
  async processFileStream(filePath, processorFn) {
    if (this.isProcessing) {
      throw new Error('Streaming processor is already running');
    }
    this.isProcessing = true;
    this.stats.startTime = Date.now();
    this.currentBatch = 0;
    this.processedItems = 0;
    try {
      // Get file size for progress tracking
      const stats = await fs.promises.stat(filePath);
      this.totalItems = Math.ceil(stats.size / this.opts.bufferSize);

      // Start progress tracking
      if (this.opts.enableProgress) {
        this._startProgressTracking();
      }

      // Create read stream
      const fileStream = fs.createReadStream(filePath, {
        encoding: 'utf8',
        highWaterMark: this.opts.bufferSize
      });

      // Create readline interface
      const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
      });
      let batch = [];
      let lineCount = 0;

      // Process lines in batches
      var _iteratorAbruptCompletion = false;
      var _didIteratorError = false;
      var _iteratorError;
      try {
        for (var _iterator = _asyncIterator(rl), _step; _iteratorAbruptCompletion = !(_step = await _iterator.next()).done; _iteratorAbruptCompletion = false) {
          const line = _step.value;
          {
            if (line.trim()) {
              batch.push(line);
              lineCount++;

              // Process batch when it reaches the configured size
              if (batch.length >= this.opts.batchSize) {
                await this._processBatch(batch, processorFn);
                batch = [];
              }
            }
          }
        }

        // Process remaining items in the last batch
      } catch (err) {
        _didIteratorError = true;
        _iteratorError = err;
      } finally {
        try {
          if (_iteratorAbruptCompletion && _iterator.return != null) {
            await _iterator.return();
          }
        } finally {
          if (_didIteratorError) {
            throw _iteratorError;
          }
        }
      }
      if (batch.length > 0) {
        await this._processBatch(batch, processorFn);
      }
      this.stats.endTime = Date.now();
      this.stats.totalProcessingTime = this.stats.endTime - this.stats.startTime;
      this.stats.itemsPerSecond = this.processedItems / (this.stats.totalProcessingTime / 1000);
      this.emit('complete', {
        totalItems: this.processedItems,
        totalBatches: this.currentBatch,
        processingTime: this.stats.totalProcessingTime,
        itemsPerSecond: this.stats.itemsPerSecond
      });
    } catch (error) {
      this.emit('error', error);
      throw error;
    } finally {
      this.isProcessing = false;
      this._stopProgressTracking();
    }
  }

  /**
   * Process an array of items in streaming fashion
   */
  async processArray(items, processorFn) {
    if (this.isProcessing) {
      throw new Error('Streaming processor is already running');
    }
    this.isProcessing = true;
    this.stats.startTime = Date.now();
    this.currentBatch = 0;
    this.processedItems = 0;
    this.totalItems = items.length;
    this.totalBatches = Math.ceil(items.length / this.opts.batchSize);
    try {
      // Start progress tracking
      if (this.opts.enableProgress) {
        this._startProgressTracking();
      }

      // Process items in batches
      for (let i = 0; i < items.length; i += this.opts.batchSize) {
        const batch = items.slice(i, i + this.opts.batchSize);
        await this._processBatch(batch, processorFn);
      }
      this.stats.endTime = Date.now();
      this.stats.totalProcessingTime = this.stats.endTime - this.stats.startTime;
      this.stats.itemsPerSecond = this.processedItems / (this.stats.totalProcessingTime / 1000);
      this.emit('complete', {
        totalItems: this.processedItems,
        totalBatches: this.currentBatch,
        processingTime: this.stats.totalProcessingTime,
        itemsPerSecond: this.stats.itemsPerSecond
      });
    } catch (error) {
      this.emit('error', error);
      throw error;
    } finally {
      this.isProcessing = false;
      this._stopProgressTracking();
    }
  }

  /**
   * Process a generator function
   */
  async processGenerator(generatorFn, processorFn) {
    if (this.isProcessing) {
      throw new Error('Streaming processor is already running');
    }
    this.isProcessing = true;
    this.stats.startTime = Date.now();
    this.currentBatch = 0;
    this.processedItems = 0;
    this.totalItems = 0; // Unknown for generators

    try {
      // Start progress tracking
      if (this.opts.enableProgress) {
        this._startProgressTracking();
      }
      const generator = generatorFn();
      let batch = [];
      var _iteratorAbruptCompletion2 = false;
      var _didIteratorError2 = false;
      var _iteratorError2;
      try {
        for (var _iterator2 = _asyncIterator(generator), _step2; _iteratorAbruptCompletion2 = !(_step2 = await _iterator2.next()).done; _iteratorAbruptCompletion2 = false) {
          const item = _step2.value;
          {
            batch.push(item);
            this.totalItems++;

            // Process batch when it reaches the configured size
            if (batch.length >= this.opts.batchSize) {
              await this._processBatch(batch, processorFn);
              batch = [];
            }
          }
        }

        // Process remaining items in the last batch
      } catch (err) {
        _didIteratorError2 = true;
        _iteratorError2 = err;
      } finally {
        try {
          if (_iteratorAbruptCompletion2 && _iterator2.return != null) {
            await _iterator2.return();
          }
        } finally {
          if (_didIteratorError2) {
            throw _iteratorError2;
          }
        }
      }
      if (batch.length > 0) {
        await this._processBatch(batch, processorFn);
      }
      this.stats.endTime = Date.now();
      this.stats.totalProcessingTime = this.stats.endTime - this.stats.startTime;
      this.stats.itemsPerSecond = this.processedItems / (this.stats.totalProcessingTime / 1000);
      this.emit('complete', {
        totalItems: this.processedItems,
        totalBatches: this.currentBatch,
        processingTime: this.stats.totalProcessingTime,
        itemsPerSecond: this.stats.itemsPerSecond
      });
    } catch (error) {
      this.emit('error', error);
      throw error;
    } finally {
      this.isProcessing = false;
      this._stopProgressTracking();
    }
  }

  /**
   * Process a single batch
   */
  async _processBatch(batch, processorFn) {
    if (this.opts.enableBackpressure && this.pendingBatches >= this.opts.maxPendingBatches) {
      // Wait for backpressure to reduce
      await this._waitForBackpressure();
    }
    this.pendingBatches++;
    this.currentBatch++;
    try {
      const startTime = Date.now();

      // Apply transform pipeline
      let transformedBatch = batch;
      for (const transform of this.transformPipeline) {
        transformedBatch = await transform(transformedBatch);
      }

      // Process the batch
      const result = await processorFn(transformedBatch, this.currentBatch);
      const batchTime = Date.now() - startTime;
      this.stats.averageBatchTime = (this.stats.averageBatchTime + batchTime) / 2;
      this.processedItems += batch.length;
      this.emit('batchComplete', {
        batchNumber: this.currentBatch,
        batchSize: batch.length,
        processingTime: batchTime,
        result
      });
    } catch (error) {
      this.emit('batchError', {
        batchNumber: this.currentBatch,
        batchSize: batch.length,
        error
      });
      throw error;
    } finally {
      this.pendingBatches--;
    }
  }

  /**
   * Wait for backpressure to reduce
   */
  async _waitForBackpressure() {
    return new Promise(resolve => {
      const checkBackpressure = () => {
        if (this.pendingBatches < this.opts.maxPendingBatches) {
          resolve();
        } else {
          setTimeout(checkBackpressure, 10);
        }
      };
      checkBackpressure();
    });
  }

  /**
   * Start progress tracking
   */
  _startProgressTracking() {
    this.progressTimer = setInterval(() => {
      const progress = {
        currentBatch: this.currentBatch,
        totalBatches: this.totalBatches,
        processedItems: this.processedItems,
        totalItems: this.totalItems,
        percentage: this.totalItems > 0 ? this.processedItems / this.totalItems * 100 : 0,
        itemsPerSecond: this.stats.itemsPerSecond,
        averageBatchTime: this.stats.averageBatchTime,
        memoryUsage: process.memoryUsage().heapUsed / 1024 / 1024 // MB
      };
      this.emit('progress', progress);
    }, this.opts.progressInterval);
    this.progressTimer.unref(); // Allow process to exit without waiting for this timer
  }

  /**
   * Stop progress tracking
   */
  _stopProgressTracking() {
    if (this.progressTimer) {
      clearInterval(this.progressTimer);
      this.progressTimer = null;
    }
  }

  /**
   * Get current statistics
   */
  getStats() {
    return {
      ...this.stats,
      isProcessing: this.isProcessing,
      currentBatch: this.currentBatch,
      totalBatches: this.totalBatches,
      processedItems: this.processedItems,
      totalItems: this.totalItems,
      pendingBatches: this.pendingBatches,
      transformPipelineLength: this.transformPipeline.length
    };
  }

  /**
   * Stop processing
   */
  stop() {
    this.isProcessing = false;
    this._stopProgressTracking();
    this.emit('stopped');
  }

  /**
   * Reset the processor
   */
  reset() {
    this.stop();
    this.currentBatch = 0;
    this.totalBatches = 0;
    this.processedItems = 0;
    this.totalItems = 0;
    this.pendingBatches = 0;
    this.stats = {
      startTime: 0,
      endTime: 0,
      totalProcessingTime: 0,
      averageBatchTime: 0,
      itemsPerSecond: 0,
      memoryUsage: 0
    };
    this.transformPipeline = [];
  }
}

/**
 * TermManager - Manages term-to-ID mapping for efficient storage
 * 
 * Responsibilities:
 * - Map terms to numeric IDs for space efficiency
 * - Track term usage counts for cleanup
 * - Load/save terms from/to index file
 * - Clean up orphaned terms
 */
class TermManager {
  constructor() {
    this.termToId = new Map(); // "bra" -> 1
    this.idToTerm = new Map(); // 1 -> "bra"
    this.termCounts = new Map(); // 1 -> 1500 (how many times used)
    this.nextId = 1;
  }

  /**
   * Get ID for a term (create if doesn't exist)
   * @param {string} term - Term to get ID for
   * @returns {number} - Numeric ID for the term
   */
  getTermId(term) {
    if (this.termToId.has(term)) {
      const id = this.termToId.get(term);
      this.termCounts.set(id, (this.termCounts.get(id) || 0) + 1);
      return id;
    }
    const id = this.nextId++;
    this.termToId.set(term, id);
    this.idToTerm.set(id, term);
    this.termCounts.set(id, 1);
    return id;
  }

  /**
   * Get term ID without incrementing count (for IndexManager use)
   * @param {string} term - Term to get ID for
   * @returns {number|undefined} - Numeric ID for the term, or undefined if not found
   * CRITICAL: Does NOT create new IDs - only returns existing ones
   * This prevents creating invalid term IDs during queries when terms haven't been loaded yet
   */
  getTermIdWithoutIncrement(term) {
    if (this.termToId.has(term)) {
      return this.termToId.get(term);
    }

    // CRITICAL FIX: Don't create new IDs during queries
    // If term doesn't exist, return undefined
    // This ensures queries only work with terms that were actually saved to the database
    return undefined;
  }

  /**
   * Get term by ID
   * @param {number} id - Numeric ID
   * @returns {string|null} - Term or null if not found
   */
  getTerm(id) {
    return this.idToTerm.get(id) || null;
  }

  /**
   * Bulk get term IDs for multiple terms (optimized for performance)
   * @param {string[]} terms - Array of terms to get IDs for
   * @returns {number[]} - Array of term IDs in the same order
   */
  bulkGetTermIds(terms) {
    if (!Array.isArray(terms) || terms.length === 0) {
      return [];
    }
    const termIds = new Array(terms.length);

    // Process all terms in a single pass
    for (let i = 0; i < terms.length; i++) {
      const term = terms[i];
      if (this.termToId.has(term)) {
        const id = this.termToId.get(term);
        this.termCounts.set(id, (this.termCounts.get(id) || 0) + 1);
        termIds[i] = id;
      } else {
        const id = this.nextId++;
        this.termToId.set(term, id);
        this.idToTerm.set(id, term);
        this.termCounts.set(id, 1);
        termIds[i] = id;
      }
    }
    return termIds;
  }

  /**
   * Load terms from file data
   * @param {Object} termsData - Terms data from file
   */
  loadTerms(termsData) {
    if (!termsData || typeof termsData !== 'object') {
      return;
    }
    for (const [id, term] of Object.entries(termsData)) {
      const numericId = parseInt(id);
      if (!isNaN(numericId) && term) {
        this.termToId.set(term, numericId);
        this.idToTerm.set(numericId, term);
        this.nextId = Math.max(this.nextId, numericId + 1);
        // Initialize count to 0 - will be updated as terms are used
        this.termCounts.set(numericId, 0);
      }
    }
  }

  /**
   * Save terms to file format
   * @returns {Object} - Terms data for file
   */
  saveTerms() {
    const termsData = {};
    for (const [id, term] of this.idToTerm) {
      termsData[id] = term;
    }
    return termsData;
  }

  /**
   * Clean up orphaned terms (terms with count 0)
   * @param {boolean} forceCleanup - Force cleanup even if conditions not met
   * @param {Object} options - Cleanup options
   * @returns {number} - Number of orphaned terms removed
   */
  cleanupOrphanedTerms(forceCleanup = false, options = {}) {
    const {
      intelligentCleanup = true,
      minOrphanCount = 10,
      orphanPercentage = 0.15,
      checkSystemState = true
    } = options;

    // INTELLIGENT CLEANUP: Check if cleanup should be performed
    if (!forceCleanup && intelligentCleanup) {
      const stats = this.getStats();
      const orphanedCount = stats.orphanedTerms;
      const totalTerms = stats.totalTerms;

      // Only cleanup if conditions are met
      const shouldCleanup = orphanedCount >= minOrphanCount &&
      // Minimum orphan count
      orphanedCount > totalTerms * orphanPercentage && (
      // Orphans > percentage of total
      !checkSystemState || this.isSystemSafe()) // System is safe (if check enabled)
      ;
      if (!shouldCleanup) {
        return 0; // Don't cleanup if conditions not met
      }
    } else if (!forceCleanup) {
      return 0; // Don't remove anything during normal operations
    }

    // PERFORM CLEANUP: Remove orphaned terms
    const orphanedIds = [];
    for (const [id, count] of this.termCounts) {
      if (count === 0) {
        orphanedIds.push(id);
      }
    }

    // Remove orphaned terms with additional safety checks
    for (const id of orphanedIds) {
      const term = this.idToTerm.get(id);
      if (term && typeof term === 'string') {
        // Extra safety: only remove string terms
        this.termToId.delete(term);
        this.idToTerm.delete(id);
        this.termCounts.delete(id);
      }
    }
    return orphanedIds.length;
  }

  /**
   * Check if system is safe for cleanup operations
   * @returns {boolean} - True if system is safe for cleanup
   */
  isSystemSafe() {
    // This method should be overridden by the database instance
    // to provide system state information
    return true; // Default to safe for backward compatibility
  }

  /**
   * Perform intelligent automatic cleanup
   * @param {Object} options - Cleanup options
   * @returns {number} - Number of orphaned terms removed
   */
  performIntelligentCleanup(options = {}) {
    return this.cleanupOrphanedTerms(false, {
      intelligentCleanup: true,
      minOrphanCount: 5,
      // Lower threshold for automatic cleanup
      orphanPercentage: 0.1,
      // 10% of total terms
      checkSystemState: true,
      ...options
    });
  }

  /**
   * Decrement term count (when term is removed from index)
   * @param {number} termId - Term ID to decrement
   */
  decrementTermCount(termId) {
    const count = this.termCounts.get(termId) || 0;
    this.termCounts.set(termId, Math.max(0, count - 1));
  }

  /**
   * Increment term count (when term is added to index)
   * @param {number} termId - Term ID to increment
   */
  incrementTermCount(termId) {
    const count = this.termCounts.get(termId) || 0;
    this.termCounts.set(termId, count + 1);
  }

  /**
   * Get statistics about terms
   * @returns {Object} - Term statistics
   */
  getStats() {
    return {
      totalTerms: this.termToId.size,
      nextId: this.nextId,
      orphanedTerms: Array.from(this.termCounts.entries()).filter(([_, count]) => count === 0).length
    };
  }

  /**
   * Check if a term exists
   * @param {string} term - Term to check
   * @returns {boolean} - True if term exists
   */
  hasTerm(term) {
    return this.termToId.has(term);
  }

  /**
   * Get all terms
   * @returns {Array} - Array of all terms
   */
  getAllTerms() {
    return Array.from(this.termToId.keys());
  }

  /**
   * Get all term IDs
   * @returns {Array} - Array of all term IDs
   */
  getAllTermIds() {
    return Array.from(this.idToTerm.keys());
  }

  /**
   * Get statistics about term mapping
   * @returns {Object} - Statistics object
   */
  getStatistics() {
    return {
      totalTerms: this.termToId.size,
      nextId: this.nextId,
      termCounts: Object.fromEntries(this.termCounts),
      sampleTerms: Array.from(this.termToId.entries()).slice(0, 5)
    };
  }
}

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

/**
 * InsertSession - Simple batch insertion without memory duplication
 */
class InsertSession {
  constructor(database, sessionOptions = {}) {
    this.database = database;
    this.batchSize = sessionOptions.batchSize || 100;
    this.enableAutoSave = sessionOptions.enableAutoSave !== undefined ? sessionOptions.enableAutoSave : true;
    this.totalInserted = 0;
    this.flushing = false;
    this.batches = []; // Array of batches to avoid slice() in flush()
    this.currentBatch = []; // Current batch being filled
    this.sessionId = Math.random().toString(36).substr(2, 9);

    // Track pending auto-flush operations
    this.pendingAutoFlushes = new Set();

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

    // If batch is full, move it to batches array and trigger auto-flush
    if (this.currentBatch.length >= this.batchSize) {
      this.batches.push(this.currentBatch);
      this.currentBatch = [];

      // Auto-flush in background (non-blocking)
      // This ensures batches are flushed automatically without blocking add()
      this.autoFlush().catch(err => {
        // Log error but don't throw - we don't want to break the add() flow
        console.error('Auto-flush error in InsertSession:', err);
      });
    }
    return finalRecord;
  }
  async autoFlush() {
    // Only flush if not already flushing
    // This method will process all pending batches
    if (this.flushing) return;

    // Create a promise for this auto-flush operation
    const flushPromise = this._doFlush();
    this.pendingAutoFlushes.add(flushPromise);

    // Remove from pending set when complete (success or error)
    flushPromise.then(() => {
      this.pendingAutoFlushes.delete(flushPromise);
    }).catch(err => {
      this.pendingAutoFlushes.delete(flushPromise);
      throw err;
    });
    return flushPromise;
  }
  async _doFlush() {
    // Check if database is destroyed or closed before starting
    if (this.database.destroyed || this.database.closed) {
      // Clear batches if database is closed/destroyed
      this.batches = [];
      this.currentBatch = [];
      return;
    }

    // Prevent concurrent flushes - if already flushing, wait for it
    if (this.flushing) {
      // Wait for the current flush to complete
      while (this.flushing) {
        await new Promise(resolve => setTimeout(resolve, 1));
      }
      // After waiting, check if there's anything left to flush
      // If another flush completed everything, we're done
      if (this.batches.length === 0 && this.currentBatch.length === 0) return;

      // Check again if database was closed during wait
      if (this.database.destroyed || this.database.closed) {
        this.batches = [];
        this.currentBatch = [];
        return;
      }
    }
    this.flushing = true;
    try {
      // Process continuously until queue is completely empty
      // This handles the case where new data is added during the flush
      while (this.batches.length > 0 || this.currentBatch.length > 0) {
        // Check if database was closed during processing
        if (this.database.destroyed || this.database.closed) {
          // Clear remaining batches
          this.batches = [];
          this.currentBatch = [];
          return;
        }

        // Process all complete batches that exist at this moment
        // Note: new batches may be added to this.batches during this loop
        const batchesToProcess = this.batches.length;
        for (let i = 0; i < batchesToProcess; i++) {
          // Check again before each batch
          if (this.database.destroyed || this.database.closed) {
            this.batches = [];
            this.currentBatch = [];
            return;
          }
          const batch = this.batches.shift(); // Remove from front
          await this.database.insertBatch(batch);
        }

        // Process current batch if it has data
        // Note: new records may be added to currentBatch during processing
        if (this.currentBatch.length > 0) {
          // Check if database was closed
          if (this.database.destroyed || this.database.closed) {
            this.batches = [];
            this.currentBatch = [];
            return;
          }

          // Check if currentBatch reached batchSize during processing
          if (this.currentBatch.length >= this.batchSize) {
            // Move it to batches array and process in next iteration
            this.batches.push(this.currentBatch);
            this.currentBatch = [];
            continue;
          }

          // Process the current batch
          const batchToProcess = this.currentBatch;
          this.currentBatch = []; // Clear before processing to allow new adds
          await this.database.insertBatch(batchToProcess);
        }
      }
    } finally {
      this.flushing = false;
    }
  }
  async flush() {
    // Wait for any pending auto-flushes to complete first
    await this.waitForAutoFlushes();

    // Then do a final flush to ensure everything is processed
    await this._doFlush();
  }
  async waitForAutoFlushes() {
    // Wait for all pending auto-flush operations to complete
    if (this.pendingAutoFlushes.size > 0) {
      await Promise.all(Array.from(this.pendingAutoFlushes));
    }
  }
  async commit() {
    // CRITICAL FIX: Make session auto-reusable by removing committed state
    // Allow multiple commits on the same session

    // First, wait for all pending auto-flushes to complete
    await this.waitForAutoFlushes();

    // Then flush any remaining data (including currentBatch)
    // This ensures everything is inserted before commit returns
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

    // Wait for auto-flushes first
    await this.waitForAutoFlushes();
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
    return this.pendingAutoFlushes.size > 0 || this.flushing || this.batches.length > 0 || this.currentBatch.length > 0;
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
    this.pendingAutoFlushes.clear();
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
class Database extends events.EventEmitter {
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
      enableArraySerialization: opts.enableArraySerialization !== false,
      // Enable array serialization by default
      // Index rebuild options
      allowIndexRebuild: opts.allowIndexRebuild === true // Allow automatic index rebuild when corrupted (default false - throws error)
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
    this.operationQueue = new OperationQueue(false); // Disable debug mode for queue

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
    this._offsetRecoveryInProgress = false;
    this.writeBufferTotalSize = 0;

    // Initialize managers
    this.initializeManagers();

    // Initialize file mutex for thread safety
    this.fileMutex = new asyncMutex.Mutex();

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
      if (this.opts.fields) {
        this.validateIndexFields(this.opts.originalIndexes);
      } else if (this.opts.debugMode) {
        console.log('⚠️  Skipping index field validation because no fields configuration was provided');
      }
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
    const supportedTypes = ['string', 'number', 'boolean', 'array:string', 'array:number', 'array:boolean', 'array', 'object', 'auto'];
    const errors = [];
    for (const [fieldName, fieldType] of Object.entries(fields)) {
      if (fieldType === 'auto') {
        continue;
      }

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
    if (Array.isArray(this.opts.indexes)) {
      const indexedFields = {};
      const originalIndexes = [...this.opts.indexes];
      const hasFieldConfig = this.opts.fields && typeof this.opts.fields === 'object';
      for (const fieldName of this.opts.indexes) {
        if (hasFieldConfig && this.opts.fields[fieldName]) {
          indexedFields[fieldName] = this.opts.fields[fieldName];
        } else {
          indexedFields[fieldName] = 'auto';
        }
      }
      this.opts.originalIndexes = originalIndexes;
      this.opts.indexes = indexedFields;
      if (this.opts.debugMode) {
        console.log(`🔍 Normalized indexes array to object: ${Object.keys(indexedFields).join(', ')} [${this.instanceId}]`);
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

    // Handle legacy 'schema' option migration
    if (this.opts.schema) {
      // If fields is already provided and valid, ignore schema
      if (this.opts.fields && typeof this.opts.fields === 'object' && Object.keys(this.opts.fields).length > 0) {
        if (this.opts.debugMode) {
          console.log(`⚠️  Both 'schema' and 'fields' options provided. Ignoring 'schema' and using 'fields'. [${this.instanceId}]`);
        }
      } else if (Array.isArray(this.opts.schema)) {
        // Schema as array is no longer supported
        throw new Error('The "schema" option as an array is no longer supported. Please use "fields" as an object instead. Example: { fields: { id: "number", name: "string" } }');
      } else if (typeof this.opts.schema === 'object' && this.opts.schema !== null) {
        // Schema as object - migrate to fields
        this.opts.fields = {
          ...this.opts.schema
        };
        if (this.opts.debugMode) {
          console.log(`⚠️  Migrated 'schema' option to 'fields'. Please update your code to use 'fields' instead of 'schema'. [${this.instanceId}]`);
        }
      } else {
        throw new Error('The "schema" option must be an object. Example: { schema: { id: "number", name: "string" } }');
      }
    }

    // Validate that fields is provided (mandatory)
    if (!this.opts.fields || typeof this.opts.fields !== 'object' || Object.keys(this.opts.fields).length === 0) {
      throw new Error('The "fields" option is mandatory and must be an object with at least one field definition. Example: { fields: { id: "number", name: "string" } }');
    }

    // CRITICAL FIX: Initialize serializer first - this was missing and causing crashes
    this.serializer = new Serializer(this.opts);

    // Initialize schema for array-based serialization
    if (this.opts.enableArraySerialization !== false) {
      this.initializeSchema();
    }

    // Initialize TermManager - always enabled for optimal performance
    this.termManager = new TermManager();

    // Auto-detect term mapping fields from indexes
    const termMappingFields = this.getTermMappingFields();
    this.termManager.termMappingFields = termMappingFields;
    this.opts.termMapping = true; // Always enable term mapping for optimal performance

    // Validation: Ensure all array:string indexed fields are in term mapping fields
    if (this.opts.indexes) {
      const arrayStringFields = [];
      for (const [field, type] of Object.entries(this.opts.indexes)) {
        if (type === 'array:string' && !termMappingFields.includes(field)) {
          arrayStringFields.push(field);
        }
      }
      if (arrayStringFields.length > 0) {
        console.warn(`⚠️  Warning: The following array:string indexed fields were not added to term mapping: ${arrayStringFields.join(', ')}. This may impact performance.`);
      }
    }
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
    this.indexManager = new IndexManager(this.opts, null, this);
    if (this.opts.debugMode) {
      console.log(`🔍 IndexManager initialized with fields: ${this.indexManager.indexedFields.join(', ')} [${this.instanceId}]`);
    }

    // Mark managers as initialized
    this.managersInitialized = true;
    this.indexOffset = 0;
    this.writeBuffer = [];
    this.writeBufferOffsets = []; // Track offsets for writeBuffer records
    this.writeBufferSizes = []; // Track sizes for writeBuffer records
    this.writeBufferTotalSize = 0;
    this.isInsideOperationQueue = false; // Flag to prevent deadlock in save() calls

    // Initialize other managers
    this.fileHandler = new FileHandler(this.normalizedFile, this.fileMutex, this.opts);
    this.queryManager = new QueryManager(this);
    this.concurrencyManager = new ConcurrencyManager(this.opts);
    this.statisticsManager = new StatisticsManager(this, this.opts);
    this.streamingProcessor = new StreamingProcessor(this.opts);
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
      // Fields that should use term mapping (only array fields)
      if (type === 'array:string') {
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

      // CRITICAL FIX: Ensure IndexManager totalLines is consistent with offsets
      // This prevents data integrity issues when database is initialized without existing data
      if (this.indexManager && this.offsets) {
        this.indexManager.setTotalLines(this.offsets.length);
        if (this.opts.debugMode) {
          console.log(`🔧 Initialized index totalLines to ${this.offsets.length}`);
        }
      }
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
      const idxFileHandler = new FileHandler(idxPath, this.fileMutex, this.opts);
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
      this.writeBufferOffsets = [];
      this.writeBufferSizes = [];
      this.writeBufferTotalSize = 0;

      // recordCount will be determined from loaded offsets
      // If no offsets were loaded, we'll count records only if needed

      // Load index data if available (always try to load offsets, even without indexed fields)
      if (this.indexManager) {
        const idxPath = this.normalizedFile.replace('.jdb', '.idx.jdb');
        try {
          const idxFileHandler = new FileHandler(idxPath, this.fileMutex, this.opts);

          // Check if file exists BEFORE trying to read it
          const fileExists = await idxFileHandler.exists();
          if (!fileExists) {
            // File doesn't exist - this will be handled by catch block
            throw new Error('Index file does not exist');
          }
          const idxData = await idxFileHandler.readAll();

          // If file exists but is empty or has no content, treat as corrupted
          if (!idxData || !idxData.trim()) {
            // File exists but is empty - treat as corrupted
            const fileExists = await this.fileHandler.exists();
            if (fileExists) {
              const stats = await this.fileHandler.getFileStats();
              if (stats && stats.size > 0) {
                // Data file has content but index is empty - corrupted
                if (!this.opts.allowIndexRebuild) {
                  throw new Error(`Index file is corrupted: ${idxPath} exists but contains no index data, ` + `while the data file has ${stats.size} bytes. ` + `Set allowIndexRebuild: true to automatically rebuild the index, ` + `or manually fix/delete the corrupted index file.`);
                }
                // Schedule rebuild if allowed
                if (this.opts.debugMode) {
                  console.log(`⚠️ Index file exists but is empty while data file has ${stats.size} bytes - scheduling rebuild`);
                }
                this._scheduleIndexRebuild();
                // Continue execution - rebuild will happen on first query
                // Don't return - let the code continue to load other things if needed
              }
            }
            // If data file is also empty, just continue (no error needed)
            // Don't return - let the code continue to load other things if needed
          } else {
            // File has content - parse and load it
            const parsedIdxData = JSON.parse(idxData);

            // Always load offsets if available (even without indexed fields)
            if (parsedIdxData.offsets && Array.isArray(parsedIdxData.offsets)) {
              this.offsets = parsedIdxData.offsets;
              // CRITICAL FIX: Update IndexManager totalLines to match offsets length
              // This ensures queries and length property work correctly even if offsets are reset later
              if (this.indexManager && this.offsets.length > 0) {
                this.indexManager.setTotalLines(this.offsets.length);
              }
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

            // Load configuration from .idx file if database exists
            // CRITICAL: Load config FIRST so indexes are available for term mapping detection
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
                if (this.indexManager) {
                  this.indexManager.setIndexesConfig(config.indexes);
                }
                if (this.opts.debugMode) {
                  console.log(`📂 Loaded indexes config from ${idxPath}:`, Object.keys(config.indexes));
                }
              }

              // CRITICAL FIX: Update term mapping fields AFTER loading indexes from config
              // This ensures termManager knows which fields use term mapping
              // (getTermMappingFields() was called during init() before indexes were loaded)
              if (this.termManager && config.indexes) {
                const termMappingFields = this.getTermMappingFields();
                this.termManager.termMappingFields = termMappingFields;
                if (this.opts.debugMode && termMappingFields.length > 0) {
                  console.log(`🔍 Updated term mapping fields after loading indexes: ${termMappingFields.join(', ')}`);
                }
              }
            }

            // Load term mapping data from .idx file if it exists
            // CRITICAL: Load termMapping even if index is empty (terms are needed for queries)
            // NOTE: termMappingFields should already be set above from config.indexes
            if (parsedIdxData.termMapping && this.termManager && this.termManager.termMappingFields && this.termManager.termMappingFields.length > 0) {
              await this.termManager.loadTerms(parsedIdxData.termMapping);
              if (this.opts.debugMode) {
                console.log(`📂 Loaded term mapping from ${idxPath} (${Object.keys(parsedIdxData.termMapping).length} terms)`);
              }
            }

            // Load index data only if available and we have indexed fields
            if (parsedIdxData && parsedIdxData.index && this.indexManager.indexedFields && this.indexManager.indexedFields.length > 0) {
              this.indexManager.load(parsedIdxData.index);
              if (this.opts.debugMode) {
                console.log(`📂 Loaded index data from ${idxPath}`);
              }

              // Check if loaded index is actually empty (corrupted)
              let hasAnyIndexData = false;
              for (const field of this.indexManager.indexedFields) {
                if (this.indexManager.hasUsableIndexData(field)) {
                  hasAnyIndexData = true;
                  break;
                }
              }
              if (this.opts.debugMode) {
                console.log(`📊 Index check: hasAnyIndexData=${hasAnyIndexData}, indexedFields=${this.indexManager.indexedFields.join(',')}`);
              }

              // Schedule rebuild if index is empty AND file exists with data
              if (!hasAnyIndexData) {
                // Check if the actual .jdb file has data
                const fileExists = await this.fileHandler.exists();
                if (this.opts.debugMode) {
                  console.log(`📊 File check: exists=${fileExists}`);
                }
                if (fileExists) {
                  const stats = await this.fileHandler.getFileStats();
                  if (this.opts.debugMode) {
                    console.log(`📊 File stats: size=${stats?.size}`);
                  }
                  if (stats && stats.size > 0) {
                    // File has data but index is empty - corrupted index detected
                    if (!this.opts.allowIndexRebuild) {
                      const idxPath = this.normalizedFile.replace('.jdb', '.idx.jdb');
                      throw new Error(`Index file is corrupted: ${idxPath} exists but contains no index data, ` + `while the data file has ${stats.size} bytes. ` + `Set allowIndexRebuild: true to automatically rebuild the index, ` + `or manually fix/delete the corrupted index file.`);
                    }
                    // Schedule rebuild if allowed
                    if (this.opts.debugMode) {
                      console.log(`⚠️ Index loaded but empty while file has ${stats.size} bytes - scheduling rebuild`);
                    }
                    this._scheduleIndexRebuild();
                  }
                }
              }
            }

            // Continue with remaining config loading
            if (parsedIdxData.config) {
              const config = parsedIdxData.config;
              if (config.originalIndexes) {
                this.opts.originalIndexes = config.originalIndexes;
                if (this.opts.debugMode) {
                  console.log(`📂 Loaded originalIndexes config from ${idxPath}:`, config.originalIndexes.length, 'indexes');
                }
              }

              // Reinitialize schema from saved configuration (only if fields not provided)
              // Note: fields option takes precedence over saved schema
              if (!this.opts.fields && config.schema && this.serializer) {
                this.serializer.initializeSchema(config.schema);
                if (this.opts.debugMode) {
                  console.log(`📂 Loaded schema from ${idxPath}:`, config.schema.join(', '));
                }
              } else if (this.opts.fields && this.serializer) {
                // Use fields option instead of saved schema
                const fieldNames = Object.keys(this.opts.fields);
                if (fieldNames.length > 0) {
                  this.serializer.initializeSchema(fieldNames);
                  if (this.opts.debugMode) {
                    console.log(`📂 Schema initialized from fields option:`, fieldNames.join(', '));
                  }
                }
              }
            }
          }
        } catch (idxError) {
          // Index file doesn't exist or is corrupted, rebuild from data
          // BUT: if error is about rebuild being disabled, re-throw it immediately
          if (idxError.message && (idxError.message.includes('allowIndexRebuild') || idxError.message.includes('corrupted'))) {
            throw idxError;
          }

          // If error is "Index file does not exist", check if we should throw or rebuild
          if (idxError.message && idxError.message.includes('does not exist')) {
            // Check if the actual .jdb file has data that needs indexing
            try {
              const fileExists = await this.fileHandler.exists();
              if (fileExists) {
                const stats = await this.fileHandler.getFileStats();
                if (stats && stats.size > 0) {
                  // File has data but index is missing
                  if (!this.opts.allowIndexRebuild) {
                    const idxPath = this.normalizedFile.replace('.jdb', '.idx.jdb');
                    throw new Error(`Index file is missing or corrupted: ${idxPath} does not exist or is invalid, ` + `while the data file has ${stats.size} bytes. ` + `Set allowIndexRebuild: true to automatically rebuild the index, ` + `or manually create/fix the index file.`);
                  }
                  // Schedule rebuild if allowed
                  if (this.opts.debugMode) {
                    console.log(`⚠️ .jdb file has ${stats.size} bytes but index missing - scheduling rebuild`);
                  }
                  this._scheduleIndexRebuild();
                  return; // Exit early
                }
              }
            } catch (statsError) {
              if (this.opts.debugMode) {
                console.log('⚠️ Could not check file stats:', statsError.message);
              }
              // Re-throw if it's our error about rebuild being disabled
              if (statsError.message && statsError.message.includes('allowIndexRebuild')) {
                throw statsError;
              }
            }
            // If no data file or empty, just continue (no error needed)
            return;
          }
          if (this.opts.debugMode) {
            console.log('📂 No index file found or corrupted, checking if rebuild is needed...');
          }

          // Check if the actual .jdb file has data that needs indexing
          try {
            const fileExists = await this.fileHandler.exists();
            if (fileExists) {
              const stats = await this.fileHandler.getFileStats();
              if (stats && stats.size > 0) {
                // File has data but index is missing or corrupted
                if (!this.opts.allowIndexRebuild) {
                  const idxPath = this.normalizedFile.replace('.jdb', '.idx.jdb');
                  throw new Error(`Index file is missing or corrupted: ${idxPath} does not exist or is invalid, ` + `while the data file has ${stats.size} bytes. ` + `Set allowIndexRebuild: true to automatically rebuild the index, ` + `or manually create/fix the index file.`);
                }
                // Schedule rebuild if allowed
                if (this.opts.debugMode) {
                  console.log(`⚠️ .jdb file has ${stats.size} bytes but index missing - scheduling rebuild`);
                }
                this._scheduleIndexRebuild();
              }
            }
          } catch (statsError) {
            if (this.opts.debugMode) {
              console.log('⚠️ Could not check file stats:', statsError.message);
            }
            // Re-throw if it's our error about rebuild being disabled
            if (statsError.message && statsError.message.includes('allowIndexRebuild')) {
              throw statsError;
            }
          }
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

    // CRITICAL FIX: Wait for all active insert sessions to complete their auto-flushes
    // This prevents race conditions where save() writes data while auto-flushes are still adding to writeBuffer
    if (this.activeInsertSessions && this.activeInsertSessions.size > 0) {
      if (this.opts.debugMode) {
        console.log(`⏳ save(): Waiting for ${this.activeInsertSessions.size} active insert sessions to complete auto-flushes`);
      }
      const sessionPromises = Array.from(this.activeInsertSessions).map(session => session.waitForAutoFlushes().catch(err => {
        if (this.opts.debugMode) {
          console.warn(`⚠️ save(): Error waiting for insert session: ${err.message}`);
        }
      }));
      await Promise.all(sessionPromises);
      if (this.opts.debugMode) {
        console.log(`✅ save(): All insert sessions completed auto-flushes`);
      }
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
      // CRITICAL FIX: Normalize deleted IDs to strings for consistent comparison
      const deletedIdsSnapshot = new Set(Array.from(this.deletedIds).map(id => String(id)));

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

      // CRITICAL FIX: DO NOT flush writeBuffer before processing existing records
      // This prevents duplicating updated records in the file.
      // The _streamExistingRecords() will handle replacing old records with updated ones from writeBufferSnapshot.
      // After processing, all records (existing + updated + new) will be written to file in one operation.
      if (this.opts.debugMode) {
        console.log(`💾 Save: writeBufferSnapshot captured with ${writeBufferSnapshot.length} records (will be processed with existing records)`);
      }

      // OPTIMIZATION: Parallel operations - cleanup and data preparation
      let allData = [];
      let orphanedCount = 0;

      // Check if there are records to save from writeBufferSnapshot
      // CRITICAL FIX: Process writeBufferSnapshot records (both new and updated) with existing records
      // Updated records will replace old ones via _streamExistingRecords, new records will be added
      if (this.opts.debugMode) {
        console.log(`💾 Save: writeBuffer.length=${this.writeBuffer.length}, writeBufferSnapshot.length=${writeBufferSnapshot.length}`);
      }
      if (this.writeBuffer.length > 0 || writeBufferSnapshot.length > 0) {
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
            // CRITICAL FIX: _streamExistingRecords already handles updates via updatedRecordsMap
            // So existingRecords already contains updated records from writeBufferSnapshot
            // We only need to add records from writeBufferSnapshot that are NEW (not updates)
            allData = [...existingRecords];

            // OPTIMIZATION: Use Set for faster lookups of existing record IDs
            // CRITICAL FIX: Normalize IDs to strings for consistent comparison
            const existingRecordIds = new Set(existingRecords.filter(r => r && r.id).map(r => String(r.id)));

            // CRITICAL FIX: Create a map of records in existingRecords by ID for comparison
            const existingRecordsById = new Map();
            existingRecords.forEach(r => {
              if (r && r.id) {
                existingRecordsById.set(String(r.id), r);
              }
            });

            // Add only NEW records from writeBufferSnapshot (not updates, as those are already in existingRecords)
            // CRITICAL FIX: Also ensure that if an updated record wasn't properly replaced, we replace it now
            for (const record of writeBufferSnapshot) {
              if (!record || !record.id) continue;
              if (deletedIdsSnapshot.has(String(record.id))) continue;
              const recordIdStr = String(record.id);
              const existingRecord = existingRecordsById.get(recordIdStr);
              if (!existingRecord) {
                // This is a new record, not an update
                allData.push(record);
                if (this.opts.debugMode) {
                  console.log(`💾 Save: Adding NEW record to allData:`, {
                    id: recordIdStr,
                    price: record.price,
                    app_id: record.app_id,
                    currency: record.currency
                  });
                }
              } else {
                // This is an update - verify that existingRecords contains the updated version
                // If not, replace it (this handles edge cases where substitution might have failed)
                const existingIndex = allData.findIndex(r => r && r.id && String(r.id) === recordIdStr);
                if (existingIndex !== -1) {
                  // Verify if the existing record is actually the updated one
                  // Compare key fields to detect if replacement is needed
                  const needsReplacement = JSON.stringify(allData[existingIndex]) !== JSON.stringify(record);
                  if (needsReplacement) {
                    if (this.opts.debugMode) {
                      console.log(`💾 Save: REPLACING existing record with updated version in allData:`, {
                        old: {
                          id: String(allData[existingIndex].id),
                          price: allData[existingIndex].price
                        },
                        new: {
                          id: recordIdStr,
                          price: record.price
                        }
                      });
                    }
                    allData[existingIndex] = record;
                  } else if (this.opts.debugMode) {
                    console.log(`💾 Save: Record already correctly updated in allData:`, {
                      id: recordIdStr
                    });
                  }
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
              // CRITICAL FIX: _streamExistingRecords already handles updates via updatedRecordsMap
              // So existingRecords already contains updated records from writeBufferSnapshot
              // We only need to add records from writeBufferSnapshot that are NEW (not updates)
              allData = [...existingRecords];

              // OPTIMIZATION: Use Set for faster lookups of existing record IDs
              // CRITICAL FIX: Normalize IDs to strings for consistent comparison
              const existingRecordIds = new Set(existingRecords.filter(r => r && r.id).map(r => String(r.id)));

              // CRITICAL FIX: Create a map of records in existingRecords by ID for comparison
              const existingRecordsById = new Map();
              existingRecords.forEach(r => {
                if (r && r.id) {
                  existingRecordsById.set(String(r.id), r);
                }
              });

              // Add only NEW records from writeBufferSnapshot (not updates, as those are already in existingRecords)
              // CRITICAL FIX: Also ensure that if an updated record wasn't properly replaced, we replace it now
              for (const record of writeBufferSnapshot) {
                if (!record || !record.id) continue;
                if (deletedIdsSnapshot.has(String(record.id))) continue;
                const recordIdStr = String(record.id);
                const existingRecord = existingRecordsById.get(recordIdStr);
                if (!existingRecord) {
                  // This is a new record, not an update
                  allData.push(record);
                  if (this.opts.debugMode) {
                    console.log(`💾 Save: Adding NEW record to allData:`, {
                      id: recordIdStr,
                      price: record.price,
                      app_id: record.app_id,
                      currency: record.currency
                    });
                  }
                } else {
                  // This is an update - verify that existingRecords contains the updated version
                  // If not, replace it (this handles edge cases where substitution might have failed)
                  const existingIndex = allData.findIndex(r => r && r.id && String(r.id) === recordIdStr);
                  if (existingIndex !== -1) {
                    // Verify if the existing record is actually the updated one
                    // Compare key fields to detect if replacement is needed
                    const needsReplacement = JSON.stringify(allData[existingIndex]) !== JSON.stringify(record);
                    if (needsReplacement) {
                      if (this.opts.debugMode) {
                        console.log(`💾 Save: REPLACING existing record with updated version in allData:`, {
                          old: {
                            id: String(allData[existingIndex].id),
                            price: allData[existingIndex].price
                          },
                          new: {
                            id: recordIdStr,
                            price: record.price
                          }
                        });
                      }
                      allData[existingIndex] = record;
                    } else if (this.opts.debugMode) {
                      console.log(`💾 Save: Record already correctly updated in allData:`, {
                        id: recordIdStr
                      });
                    }
                  }
                }
              }
              if (this.opts.debugMode) {
                const updatedCount = writeBufferSnapshot.filter(r => r && r.id && existingRecordIds.has(String(r.id))).length;
                const newCount = writeBufferSnapshot.filter(r => r && r.id && !existingRecordIds.has(String(r.id))).length;
                console.log(`💾 Save: Combined data - existingRecords: ${existingRecords.length}, updatedFromBuffer: ${updatedCount}, newFromBuffer: ${newCount}, total: ${allData.length}`);
                console.log(`💾 Save: WriteBuffer record IDs:`, writeBufferSnapshot.map(r => r && r.id ? String(r.id) : 'no-id'));
                console.log(`💾 Save: Existing record IDs:`, Array.from(existingRecordIds));
                console.log(`💾 Save: All records in allData:`, allData.map(r => r && r.id ? {
                  id: String(r.id),
                  price: r.price,
                  app_id: r.app_id,
                  currency: r.currency
                } : 'no-id'));
                console.log(`💾 Save: Sample existing record:`, existingRecords[0] ? {
                  id: String(existingRecords[0].id),
                  price: existingRecords[0].price,
                  app_id: existingRecords[0].app_id,
                  currency: existingRecords[0].currency
                } : 'null');
                console.log(`💾 Save: Sample writeBuffer record:`, writeBufferSnapshot[0] ? {
                  id: String(writeBufferSnapshot[0].id),
                  price: writeBufferSnapshot[0].price,
                  app_id: writeBufferSnapshot[0].app_id,
                  currency: writeBufferSnapshot[0].currency
                } : 'null');
              }
            }).catch(error => {
              if (this.opts.debugMode) {
                console.log(`💾 Save: _streamExistingRecords failed:`, error.message);
              }
              // CRITICAL FIX: Use safe fallback to preserve existing data instead of losing it
              return this._loadExistingRecordsFallback(deletedIdsSnapshot, writeBufferSnapshot).then(fallbackRecords => {
                // CRITICAL FIX: Avoid duplicating updated records
                const fallbackRecordIds = new Set(fallbackRecords.map(r => r.id));
                const newRecordsFromBuffer = writeBufferSnapshot.filter(record => !deletedIdsSnapshot.has(String(record.id)) && !fallbackRecordIds.has(record.id));
                allData = [...fallbackRecords, ...newRecordsFromBuffer];
                if (this.opts.debugMode) {
                  console.log(`💾 Save: Fallback preserved ${fallbackRecords.length} existing records, total: ${allData.length}`);
                }
              }).catch(fallbackError => {
                if (this.opts.debugMode) {
                  console.log(`💾 Save: All fallback methods failed:`, fallbackError.message);
                  console.log(`💾 Save: CRITICAL - Data loss may occur, only writeBuffer will be saved`);
                }
                // Last resort: at least save what we have in writeBuffer
                allData = writeBufferSnapshot.filter(record => !deletedIdsSnapshot.has(String(record.id)));
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
              // CRITICAL FIX: Avoid duplicating updated records
              const fallbackRecordIds = new Set(fallbackRecords.map(r => r.id));
              const newRecordsFromBuffer = writeBufferSnapshot.filter(record => !deletedIdsSnapshot.has(String(record.id)) && !fallbackRecordIds.has(record.id));
              allData = [...fallbackRecords, ...newRecordsFromBuffer];
              if (this.opts.debugMode) {
                console.log(`💾 Save: Fallback preserved ${fallbackRecords.length} existing records, total: ${allData.length}`);
              }
            } catch (fallbackError) {
              if (this.opts.debugMode) {
                console.log(`💾 Save: All fallback methods failed:`, fallbackError.message);
                console.log(`💾 Save: CRITICAL - Data loss may occur, only writeBuffer will be saved`);
              }
              // Last resort: at least save what we have in writeBuffer
              allData = writeBufferSnapshot.filter(record => !deletedIdsSnapshot.has(String(record.id)));
            }
          }
        } else {
          // No existing data, use only writeBuffer
          allData = writeBufferSnapshot.filter(record => !deletedIdsSnapshot.has(String(record.id)));
        }
      }

      // CRITICAL FIX: Calculate offsets based on actual serialized data that will be written
      // This ensures consistency between offset calculation and file writing
      // CRITICAL FIX: Remove term IDs before serialization to ensure proper serialization
      const cleanedData = allData.map(record => {
        if (!record || typeof record !== 'object') {
          if (this.opts.debugMode) {
            console.log(`💾 Save: WARNING - Invalid record in allData:`, record);
          }
          return record;
        }
        return this.removeTermIdsForSerialization(record);
      });
      if (this.opts.debugMode) {
        console.log(`💾 Save: allData.length=${allData.length}, cleanedData.length=${cleanedData.length}`);
        console.log(`💾 Save: Current offsets.length before recalculation: ${this.offsets.length}`);
        console.log(`💾 Save: All records in allData before serialization:`, allData.map(r => r && r.id ? {
          id: String(r.id),
          price: r.price,
          app_id: r.app_id,
          currency: r.currency
        } : 'no-id'));
        console.log(`💾 Save: Sample cleaned record:`, cleanedData[0] ? Object.keys(cleanedData[0]) : 'null');
      }
      const jsonlData = cleanedData.length > 0 ? this.serializer.serializeBatch(cleanedData) : '';
      const jsonlString = jsonlData.toString('utf8');
      const lines = jsonlString.split('\n').filter(line => line.trim());
      if (this.opts.debugMode) {
        console.log(`💾 Save: Serialized ${lines.length} lines`);
        console.log(`💾 Save: All records in allData after serialization check:`, allData.map(r => r && r.id ? {
          id: String(r.id),
          price: r.price,
          app_id: r.app_id,
          currency: r.currency
        } : 'no-id'));
        if (lines.length > 0) {
          console.log(`💾 Save: First line (first 200 chars):`, lines[0].substring(0, 200));
        }
      }

      // CRITICAL FIX: Always recalculate offsets from serialized data to ensure consistency
      // Even if _streamExistingRecords updated offsets, we need to recalculate based on actual serialized data
      this.offsets = [];
      let currentOffset = 0;
      for (let i = 0; i < lines.length; i++) {
        this.offsets.push(currentOffset);
        // CRITICAL FIX: Use actual line length including newline for accurate offset calculation
        // This accounts for UTF-8 encoding differences (e.g., 'ação' vs 'acao')
        const lineWithNewline = lines[i] + '\n';
        currentOffset += Buffer.byteLength(lineWithNewline, 'utf8');
      }
      if (this.opts.debugMode) {
        console.log(`💾 Save: Recalculated offsets.length=${this.offsets.length}, should match lines.length=${lines.length}`);
      }

      // CRITICAL FIX: Ensure indexOffset matches actual file size
      this.indexOffset = currentOffset;
      if (this.opts.debugMode) {
        console.log(`💾 Save: Calculated indexOffset: ${this.indexOffset}, allData.length: ${allData.length}`);
      }

      // CRITICAL FIX: Write main data file first
      // Index will be saved AFTER reconstruction to ensure it contains correct data
      await this.fileHandler.writeBatch([jsonlData]);
      if (this.opts.debugMode) {
        console.log(`💾 Saved ${allData.length} records to ${this.normalizedFile}`);
      }

      // CRITICAL FIX: Invalidate file size cache after save operation
      this._cachedFileStats = null;
      this.shouldSave = false;
      this.lastSaveTime = Date.now();

      // CRITICAL FIX: Always clear deletedIds and rebuild index if there were deletions,
      // even if allData.length === 0 (all records were deleted)
      const hadDeletedRecords = deletedIdsSnapshot.size > 0;
      const hadUpdatedRecords = writeBufferSnapshot.length > 0;

      // Clear writeBuffer and deletedIds after successful save
      // Also rebuild index if records were deleted or updated, even if allData is empty
      if (allData.length > 0 || hadDeletedRecords || hadUpdatedRecords) {
        // Rebuild index when records were deleted or updated to maintain consistency
        if (this.indexManager && this.indexManager.indexedFields && this.indexManager.indexedFields.length > 0) {
          if (hadDeletedRecords || hadUpdatedRecords) {
            // Clear the index and rebuild it from the saved records
            // This ensures that lineNumbers point to the correct positions in the file
            this.indexManager.clear();
            if (this.opts.debugMode) {
              if (hadDeletedRecords && hadUpdatedRecords) {
                console.log(`🧹 Rebuilding index after removing ${deletedIdsSnapshot.size} deleted records and updating ${writeBufferSnapshot.length} records`);
              } else if (hadDeletedRecords) {
                console.log(`🧹 Rebuilding index after removing ${deletedIdsSnapshot.size} deleted records`);
              } else {
                console.log(`🧹 Rebuilding index after updating ${writeBufferSnapshot.length} records`);
              }
            }

            // Rebuild index from the saved records
            // CRITICAL: Process term mapping for records loaded from file to ensure ${field}Ids are available
            if (this.opts.debugMode) {
              console.log(`💾 Save: Rebuilding index from ${allData.length} records in allData`);
            }
            for (let i = 0; i < allData.length; i++) {
              let record = allData[i];
              if (this.opts.debugMode && i < 3) {
                console.log(`💾 Save: Rebuilding index record[${i}]:`, {
                  id: String(record.id),
                  price: record.price,
                  app_id: record.app_id,
                  currency: record.currency
                });
              }

              // CRITICAL FIX: Ensure records have ${field}Ids for term mapping fields
              // Records from writeBuffer already have ${field}Ids from processTermMapping
              // Records from file need to be processed to restore ${field}Ids
              const termMappingFields = this.getTermMappingFields();
              if (termMappingFields.length > 0 && this.termManager) {
                for (const field of termMappingFields) {
                  if (record[field] && Array.isArray(record[field])) {
                    // Check if field contains term IDs (numbers) or terms (strings)
                    const firstValue = record[field][0];
                    if (typeof firstValue === 'number') {
                      // Already term IDs, create ${field}Ids
                      record[`${field}Ids`] = record[field];
                    } else if (typeof firstValue === 'string') {
                      // Terms, need to convert to term IDs
                      const termIds = record[field].map(term => {
                        const termId = this.termManager.getTermIdWithoutIncrement(term);
                        return termId !== undefined ? termId : this.termManager.getTermId(term);
                      });
                      record[`${field}Ids`] = termIds;
                    }
                  }
                }
              }
              await this.indexManager.add(record, i);
            }

            // VALIDATION: Ensure index consistency after rebuild
            // Check that all indexed records have valid line numbers
            const indexedRecordCount = this.indexManager.getIndexedRecordCount?.() || allData.length;
            if (indexedRecordCount !== this.offsets.length) {
              console.warn(`⚠️ Index inconsistency detected: indexed ${indexedRecordCount} records but offsets has ${this.offsets.length} entries`);
              // Force consistency by setting totalLines to match offsets
              this.indexManager.setTotalLines(this.offsets.length);
            } else {
              this.indexManager.setTotalLines(this.offsets.length);
            }
            if (this.opts.debugMode) {
              console.log(`💾 Save: Index rebuilt with ${allData.length} records, totalLines set to ${this.offsets.length}`);
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
      } else if (hadDeletedRecords) {
        // CRITICAL FIX: Even if allData is empty, clear deletedIds and rebuild index
        // when records were deleted to ensure consistency
        if (this.indexManager && this.indexManager.indexedFields && this.indexManager.indexedFields.length > 0) {
          // Clear the index since all records were deleted
          this.indexManager.clear();
          this.indexManager.setTotalLines(0);
          if (this.opts.debugMode) {
            console.log(`🧹 Cleared index after removing all ${deletedIdsSnapshot.size} deleted records`);
          }
        }

        // Clear deletedIds even when allData is empty
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
          this.writeBufferTotalSize = 0;
          this.writeBufferTotalSize = 0;
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
            // Check if field contains term IDs (numbers) or terms (strings)
            const firstValue = oldRecord[field][0];
            if (typeof firstValue === 'number') {
              // Field contains term IDs (from find with restoreTerms: false)
              for (const termId of oldRecord[field]) {
                this.termManager.decrementTermCount(termId);
              }
            } else if (typeof firstValue === 'string') {
              // Field contains terms (strings) - convert to term IDs
              for (const term of oldRecord[field]) {
                const termId = this.termManager.termToId.get(term);
                if (termId) {
                  this.termManager.decrementTermCount(termId);
                }
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
    for (const record of records) {
      for (const field of termMappingFields) {
        if (record[field] && Array.isArray(record[field])) {
          const termIds = record[field].map(term => termIdMap.get(term));
          record[`${field}Ids`] = termIds;
        }
      }
    }
    return records;
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
        // This will discard any fields not in the schema
        const schemaEnforcedRecord = this.applySchemaEnforcement(record);

        // Don't store in this.data - only use writeBuffer and index
        this.writeBuffer.push(schemaEnforcedRecord);
        if (this.opts.debugMode) {
          console.log(`🔍 INSERT: Added record to writeBuffer, length now: ${this.writeBuffer.length}`);
        }

        // OPTIMIZATION: Calculate and store offset and size for writeBuffer record
        // SPACE OPTIMIZATION: Remove term IDs before serialization
        const cleanRecord = this.removeTermIdsForSerialization(record);
        const recordBuffer = this.serializer.serialize(cleanRecord);
        const recordSize = recordBuffer.length;

        // Calculate offset based on end of file + previous writeBuffer sizes
        const previousWriteBufferSize = this.writeBufferTotalSize;
        const recordOffset = this.indexOffset + previousWriteBufferSize;
        this.writeBufferOffsets.push(recordOffset);
        this.writeBufferSizes.push(recordSize);
        this.writeBufferTotalSize += recordSize;

        // OPTIMIZATION: Use the absolute line number (persisted records + writeBuffer index)
        const lineNumber = this._getAbsoluteLineNumber(this.writeBuffer.length - 1);

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
    const existingWriteBufferLength = this.writeBuffer.length;

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
    let runningTotalSize = this.writeBufferTotalSize;
    for (let i = 0; i < processedRecords.length; i++) {
      const record = processedRecords[i];
      // SPACE OPTIMIZATION: Remove term IDs before serialization
      const cleanRecord = this.removeTermIdsForSerialization(record);
      const recordBuffer = this.serializer.serialize(cleanRecord);
      const recordSize = recordBuffer.length;
      const recordOffset = this.indexOffset + runningTotalSize;
      runningTotalSize += recordSize;
      this.writeBufferOffsets.push(recordOffset);
      this.writeBufferSizes.push(recordSize);
    }
    this.writeBufferTotalSize = runningTotalSize;

    // OPTIMIZATION: Batch process index updates
    if (!this.pendingIndexUpdates) {
      this.pendingIndexUpdates = [];
    }
    for (let i = 0; i < processedRecords.length; i++) {
      const lineNumber = this._getAbsoluteLineNumber(existingWriteBufferLength + i);
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
      // INTEGRITY CHECK: Validate data consistency before querying
      // Check if index and offsets are synchronized
      if (this.indexManager && this.offsets && this.offsets.length > 0) {
        const indexTotalLines = this.indexManager.totalLines || 0;
        const offsetsLength = this.offsets.length;
        if (indexTotalLines !== offsetsLength) {
          console.warn(`⚠️ Data integrity issue detected: index.totalLines=${indexTotalLines}, offsets.length=${offsetsLength}`);
          // Auto-correct by updating index totalLines to match offsets
          this.indexManager.setTotalLines(offsetsLength);
          if (this.opts.debugMode) {
            console.log(`🔧 Auto-corrected index totalLines to ${offsetsLength}`);
          }

          // CRITICAL FIX: Also save the corrected index to prevent persistence of inconsistency
          // This ensures the .idx.jdb file contains the correct totalLines value
          try {
            await this._saveIndexDataToFile();
            if (this.opts.debugMode) {
              console.log(`💾 Saved corrected index data to prevent future inconsistencies`);
            }
          } catch (error) {
            if (this.opts.debugMode) {
              console.warn(`⚠️ Failed to save corrected index: ${error.message}`);
            }
          }

          // Verify the fix worked
          const newIndexTotalLines = this.indexManager.totalLines || 0;
          if (newIndexTotalLines === offsetsLength) {
            console.log(`✅ Data integrity successfully corrected: index.totalLines=${newIndexTotalLines}, offsets.length=${offsetsLength}`);
          } else {
            console.error(`❌ Data integrity correction failed: index.totalLines=${newIndexTotalLines}, offsets.length=${offsetsLength}`);
          }
        }
      }

      // Validate indexed query mode if enabled
      if (this.opts.indexedQueryMode === 'strict') {
        this._validateIndexedQuery(criteria, options);
      }

      // Get results from file (QueryManager already handles term ID restoration)
      const fileResultsWithTerms = await this.queryManager.find(criteria, options);

      // Get results from writeBuffer
      const allPendingRecords = [...this.writeBuffer];
      const writeBufferResults = this.queryManager.matchesCriteria ? allPendingRecords.filter(record => this.queryManager.matchesCriteria(record, criteria, options)) : allPendingRecords;

      // SPACE OPTIMIZATION: Restore term IDs to terms for writeBuffer results (unless disabled)
      const writeBufferResultsWithTerms = options.restoreTerms !== false ? writeBufferResults.map(record => this.restoreTermIdsAfterDeserialization(record)) : writeBufferResults;

      // Combine results, removing duplicates (writeBuffer takes precedence)
      // OPTIMIZATION: Unified efficient approach with consistent precedence rules
      let allResults;

      // Create efficient lookup map for writeBuffer records
      const writeBufferMap = new Map();
      writeBufferResultsWithTerms.forEach(record => {
        if (record && record.id) {
          writeBufferMap.set(record.id, record);
        }
      });

      // Filter file results to exclude any records that exist in writeBuffer
      // This ensures writeBuffer always takes precedence
      const filteredFileResults = fileResultsWithTerms.filter(record => record && record.id && !writeBufferMap.has(record.id));

      // Combine results: file results (filtered) + all writeBuffer results
      allResults = [...filteredFileResults, ...writeBufferResultsWithTerms];

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
   * @param {Object} criteria - Query criteria
   * @param {Object} options - Query options
   */
  _validateIndexedQuery(criteria, options = {}) {
    // Allow bypassing strict mode validation with allowNonIndexed option
    if (options.allowNonIndexed === true) {
      return; // Skip validation for this query
    }
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
        if (this.opts.debugMode) {
          console.log(`🔄 UPDATE: About to process ${records.length} records`);
          console.log(`🔄 UPDATE: Records:`, records.map(r => ({
            id: r.id,
            value: r.value
          })));
        }
        for (const record of records) {
          const recordStart = Date.now();
          if (this.opts.debugMode) {
            console.log(`🔄 UPDATE: Processing record ${record.id}`);
          }
          const updated = {
            ...record,
            ...updateData
          };

          // DEBUG: Log the update operation details
          if (this.opts.debugMode) {
            console.log(`🔄 UPDATE: Original record ID: ${record.id}, type: ${typeof record.id}`);
            console.log(`🔄 UPDATE: Updated record ID: ${updated.id}, type: ${typeof updated.id}`);
            console.log(`🔄 UPDATE: Update data keys:`, Object.keys(updateData));
            console.log(`🔄 UPDATE: Updated record keys:`, Object.keys(updated));
          }

          // Process term mapping for update
          const termMappingStart = Date.now();
          this.processTermMapping(updated, true, record);
          if (this.opts.debugMode) {
            console.log(`🔄 UPDATE: Term mapping completed in ${Date.now() - termMappingStart}ms`);
            console.log(`🔄 UPDATE: After term mapping - ID: ${updated.id}, type: ${typeof updated.id}`);
          }

          // CRITICAL FIX: Remove old terms from index before adding new ones
          if (this.indexManager) {
            await this.indexManager.remove(record);
            if (this.opts.debugMode) {
              console.log(`🔄 UPDATE: Removed old terms from index for record ${record.id}`);
            }
          }

          // CRITICAL FIX: Update record in writeBuffer or add to writeBuffer if not present
          // For records in the file, we need to ensure they are properly marked for replacement
          const index = this.writeBuffer.findIndex(r => r.id === record.id);
          let lineNumber = null;
          if (this.opts.debugMode) {
            console.log(`🔄 UPDATE: writeBuffer.findIndex for ${record.id} returned ${index}`);
            console.log(`🔄 UPDATE: writeBuffer length: ${this.writeBuffer.length}`);
            console.log(`🔄 UPDATE: writeBuffer IDs:`, this.writeBuffer.map(r => r.id));
          }
          if (index !== -1) {
            // Record is already in writeBuffer, update it
            this.writeBuffer[index] = updated;
            lineNumber = this._getAbsoluteLineNumber(index);
            if (this.opts.debugMode) {
              console.log(`🔄 UPDATE: Updated existing writeBuffer record at index ${index}`);
              console.log(`🔄 UPDATE: writeBuffer now has ${this.writeBuffer.length} records`);
            }
          } else {
            // Record is in file, add updated version to writeBuffer
            // CRITICAL FIX: Ensure the old record in file will be replaced by checking if it exists in offsets
            // The save() method will handle replacement via _streamExistingRecords which checks updatedRecordsMap
            this.writeBuffer.push(updated);
            lineNumber = this._getAbsoluteLineNumber(this.writeBuffer.length - 1);
            if (this.opts.debugMode) {
              console.log(`🔄 UPDATE: Added updated record to writeBuffer (will replace file record ${record.id})`);
              console.log(`🔄 UPDATE: writeBuffer now has ${this.writeBuffer.length} records`);
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

        // 🔧 NEW: Validate indexed query mode for delete operations
        if (this.opts.indexedQueryMode === 'strict') {
          this._validateIndexedQuery(criteria, {
            operation: 'delete'
          });
        }

        // ⚠️ NEW: Warn about non-indexed fields in permissive mode
        if (this.opts.indexedQueryMode !== 'strict') {
          const indexedFields = Object.keys(this.opts.indexes || {});
          const queryFields = this._extractQueryFields(criteria);
          const nonIndexedFields = queryFields.filter(field => !indexedFields.includes(field));
          if (nonIndexedFields.length > 0) {
            if (this.opts.debugMode) {
              console.warn(`⚠️ Delete operation using non-indexed fields: ${nonIndexedFields.join(', ')}`);
              console.warn(`   This may be slow or fail silently. Consider indexing these fields.`);
            }
          }
        }
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

    // Initialize from fields configuration (mandatory)
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

    // Try to auto-detect schema from existing data (fallback for migration scenarios)
    if (this.data && this.data.length > 0) {
      this.serializer.initializeSchema(this.data, true); // autoDetect = true
      if (this.opts.debugMode) {
        console.log(`🔍 Schema auto-detected from data: ${this.serializer.getSchema().join(', ')} [${this.instanceId}]`);
      }
      return;
    }
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

    // CRITICAL FIX: If offsets are empty but indexOffset exists, use fallback calculation
    // This handles cases where offsets weren't loaded or were reset
    if (savedRecords === 0 && this.indexOffset > 0 && this.initialized) {
      // Try to use IndexManager totalLines if available
      if (this.indexManager && this.indexManager.totalLines > 0) {
        return this.indexManager.totalLines + writeBufferRecords;
      }

      // Fallback: estimate from indexOffset (less accurate but better than 0)
      // This is a defensive fix for cases where offsets are missing but file has data
      if (this.opts.debugMode) {
        console.log(`⚠️  LENGTH: offsets array is empty but indexOffset=${this.indexOffset}, using IndexManager.totalLines or estimation`);
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
            const stats = fs.statSync(this.fileHandler.file);
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
    return this.writeBufferTotalSize || 0;
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
   * Schedule index rebuild when index data is missing or corrupted
   * @private
   */
  _scheduleIndexRebuild() {
    // Mark that rebuild is needed
    this._indexRebuildNeeded = true;

    // Rebuild will happen lazily on first query if index is empty
    // This avoids blocking init() but ensures index is available when needed
  }

  /**
   * Rebuild indexes from data file if needed
   * @private
   */
  async _rebuildIndexesIfNeeded() {
    if (this.opts.debugMode) {
      console.log(`🔍 _rebuildIndexesIfNeeded called: _indexRebuildNeeded=${this._indexRebuildNeeded}`);
    }
    if (!this._indexRebuildNeeded) return;
    if (!this.indexManager || !this.indexManager.indexedFields || this.indexManager.indexedFields.length === 0) return;

    // Check if index actually needs rebuilding
    let needsRebuild = false;
    for (const field of this.indexManager.indexedFields) {
      if (!this.indexManager.hasUsableIndexData(field)) {
        needsRebuild = true;
        break;
      }
    }
    if (!needsRebuild) {
      this._indexRebuildNeeded = false;
      return;
    }

    // Check if rebuild is allowed
    if (!this.opts.allowIndexRebuild) {
      const idxPath = this.normalizedFile.replace('.jdb', '.idx.jdb');
      throw new Error(`Index rebuild required but disabled: Index file ${idxPath} is corrupted or missing, ` + `and allowIndexRebuild is set to false. ` + `Set allowIndexRebuild: true to automatically rebuild the index, ` + `or manually fix/delete the corrupted index file.`);
    }
    if (this.opts.debugMode) {
      console.log('🔨 Rebuilding indexes from data file...');
    }
    try {
      // Read all records and rebuild index
      let count = 0;
      const startTime = Date.now();

      // Auto-detect schema from first line if not initialized
      if (!this.serializer.schemaManager.isInitialized) {
        const fs = await import('fs');
        const readline = await import('readline');
        const stream = fs.createReadStream(this.fileHandler.file, {
          highWaterMark: 64 * 1024,
          encoding: 'utf8'
        });
        const rl = readline.createInterface({
          input: stream,
          crlfDelay: Infinity
        });
        var _iteratorAbruptCompletion = false;
        var _didIteratorError = false;
        var _iteratorError;
        try {
          for (var _iterator = _asyncIterator(rl), _step; _iteratorAbruptCompletion = !(_step = await _iterator.next()).done; _iteratorAbruptCompletion = false) {
            const line = _step.value;
            {
              if (line && line.trim()) {
                try {
                  const firstRecord = JSON.parse(line);
                  if (Array.isArray(firstRecord)) {
                    // Try to infer schema from opts.fields if available
                    if (this.opts.fields && typeof this.opts.fields === 'object') {
                      const fieldNames = Object.keys(this.opts.fields);
                      if (fieldNames.length >= firstRecord.length) {
                        // Use first N fields from opts.fields to match array length
                        const schema = fieldNames.slice(0, firstRecord.length);
                        this.serializer.initializeSchema(schema);
                        if (this.opts.debugMode) {
                          console.log(`🔍 Inferred schema from opts.fields: ${schema.join(', ')}`);
                        }
                      } else {
                        throw new Error(`Cannot rebuild index: array has ${firstRecord.length} elements but opts.fields only defines ${fieldNames.length} fields. Schema must be explicitly provided.`);
                      }
                    } else {
                      throw new Error('Cannot rebuild index: schema missing, file uses array format, and opts.fields not provided. The .idx.jdb file is corrupted.');
                    }
                  } else {
                    // Object format, initialize from object keys
                    this.serializer.initializeSchema(firstRecord, true);
                    if (this.opts.debugMode) {
                      console.log(`🔍 Auto-detected schema from object: ${Object.keys(firstRecord).join(', ')}`);
                    }
                  }
                  break;
                } catch (error) {
                  if (this.opts.debugMode) {
                    console.error('❌ Failed to auto-detect schema:', error.message);
                  }
                  throw error;
                }
              }
            }
          }
        } catch (err) {
          _didIteratorError = true;
          _iteratorError = err;
        } finally {
          try {
            if (_iteratorAbruptCompletion && _iterator.return != null) {
              await _iterator.return();
            }
          } finally {
            if (_didIteratorError) {
              throw _iteratorError;
            }
          }
        }
        stream.destroy();
      }

      // Use streaming to read records without loading everything into memory
      // Also rebuild offsets while we're at it
      const fs = await import('fs');
      const readline = await import('readline');
      this.offsets = [];
      let currentOffset = 0;
      const stream = fs.createReadStream(this.fileHandler.file, {
        highWaterMark: 64 * 1024,
        encoding: 'utf8'
      });
      const rl = readline.createInterface({
        input: stream,
        crlfDelay: Infinity
      });
      try {
        var _iteratorAbruptCompletion2 = false;
        var _didIteratorError2 = false;
        var _iteratorError2;
        try {
          for (var _iterator2 = _asyncIterator(rl), _step2; _iteratorAbruptCompletion2 = !(_step2 = await _iterator2.next()).done; _iteratorAbruptCompletion2 = false) {
            const line = _step2.value;
            {
              if (line && line.trim()) {
                try {
                  // Record the offset for this line
                  this.offsets.push(currentOffset);
                  const record = this.serializer.deserialize(line);
                  const recordWithTerms = this.restoreTermIdsAfterDeserialization(record);
                  await this.indexManager.add(recordWithTerms, count);
                  count++;
                } catch (error) {
                  // Skip invalid lines
                  if (this.opts.debugMode) {
                    console.log(`⚠️ Rebuild: Failed to deserialize line ${count}:`, error.message);
                  }
                }
              }
              // Update offset for next line (including newline character)
              currentOffset += Buffer.byteLength(line, 'utf8') + 1;
            }
          }
        } catch (err) {
          _didIteratorError2 = true;
          _iteratorError2 = err;
        } finally {
          try {
            if (_iteratorAbruptCompletion2 && _iterator2.return != null) {
              await _iterator2.return();
            }
          } finally {
            if (_didIteratorError2) {
              throw _iteratorError2;
            }
          }
        }
      } finally {
        stream.destroy();
      }

      // Update indexManager totalLines
      if (this.indexManager) {
        this.indexManager.setTotalLines(this.offsets.length);
      }
      this._indexRebuildNeeded = false;
      if (this.opts.debugMode) {
        console.log(`✅ Index rebuilt from ${count} records in ${Date.now() - startTime}ms`);
      }

      // Save the rebuilt index
      await this._saveIndexDataToFile();
    } catch (error) {
      if (this.opts.debugMode) {
        console.error('❌ Failed to rebuild indexes:', error.message);
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
      this.writeBufferTotalSize = 0;
      this.writeBufferTotalSize = 0;
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

    // OPTIMIZATION: Use queryManager.count() instead of find() for better performance
    // This is especially faster for indexed queries which can use indexManager.query().size
    const fileCount = await this.queryManager.count(criteria, options);

    // Count matching records in writeBuffer
    const writeBufferCount = this.writeBuffer.filter(record => this.queryManager.matchesCriteria(record, criteria, options)).length;
    return fileCount + writeBufferCount;
  }

  /**
   * Check if any records exist for given field and terms (index-only, ultra-fast)
   * Delegates to IndexManager.exists() for maximum performance
   * 
   * @param {string} fieldName - Indexed field name
   * @param {string|Array<string>} terms - Single term or array of terms
   * @param {Object} options - Options: { $all: true/false, caseInsensitive: true/false, excludes: Array<string> }
   * @returns {Promise<boolean>} - True if at least one match exists
   * 
   * @example
   * // Check if channel exists
   * const exists = await db.exists('nameTerms', ['a', 'e'], { $all: true });
   * 
   * @example
   * // Check if 'tv' exists but not 'globo'
   * const exists = await db.exists('nameTerms', 'tv', { excludes: ['globo'] });
   */
  async exists(fieldName, terms, options = {}) {
    this._validateInitialization('exists');
    return this.indexManager.exists(fieldName, terms, options);
  }

  /**
   * Calculate coverage for grouped include/exclude term sets
   * @param {string} fieldName - Name of the indexed field
   * @param {Array<object>} groups - Array of { terms, excludes } objects
   * @param {object} options - Optional settings
   * @returns {Promise<number>} Coverage percentage between 0 and 100
   */
  async coverage(fieldName, groups, options = {}) {
    this._validateInitialization('coverage');
    if (typeof fieldName !== 'string' || !fieldName.trim()) {
      throw new Error('fieldName must be a non-empty string');
    }
    if (!Array.isArray(groups)) {
      throw new Error('groups must be an array');
    }
    if (groups.length === 0) {
      return 0;
    }
    if (!this.opts.indexes || !this.opts.indexes[fieldName]) {
      throw new Error(`Field "${fieldName}" is not indexed`);
    }
    const fieldType = this.opts.indexes[fieldName];
    const supportedTypes = ['array:string', 'string'];
    if (!supportedTypes.includes(fieldType)) {
      throw new Error(`coverage() only supports fields of type ${supportedTypes.join(', ')} (found: ${fieldType})`);
    }
    const fieldIndex = this.indexManager?.index?.data?.[fieldName];
    if (!fieldIndex) {
      return 0;
    }
    const isTermMapped = this.termManager && this.termManager.termMappingFields && this.termManager.termMappingFields.includes(fieldName);
    const normalizeTerm = term => {
      if (term === undefined || term === null) {
        return '';
      }
      return String(term).trim();
    };
    const resolveKey = term => {
      if (isTermMapped) {
        const termId = this.termManager.getTermIdWithoutIncrement(term);
        if (termId === null || termId === undefined) {
          return null;
        }
        return String(termId);
      }
      return String(term);
    };
    let matchedGroups = 0;
    for (const group of groups) {
      if (!group || typeof group !== 'object') {
        throw new Error('Each coverage group must be an object');
      }
      const includeTermsRaw = Array.isArray(group.terms) ? group.terms : [];
      const excludeTermsRaw = Array.isArray(group.excludes) ? group.excludes : [];
      const includeTerms = Array.from(new Set(includeTermsRaw.map(normalizeTerm).filter(term => term.length > 0)));
      if (includeTerms.length === 0) {
        throw new Error('Each coverage group must define at least one term');
      }
      const excludeTerms = Array.from(new Set(excludeTermsRaw.map(normalizeTerm).filter(term => term.length > 0)));
      let candidateLines = null;
      let groupMatched = true;
      for (const term of includeTerms) {
        const key = resolveKey(term);
        if (key === null) {
          groupMatched = false;
          break;
        }
        const termData = fieldIndex[key];
        if (!termData) {
          groupMatched = false;
          break;
        }
        const lineNumbers = this.indexManager._getAllLineNumbers(termData);
        if (!lineNumbers || lineNumbers.length === 0) {
          groupMatched = false;
          break;
        }
        if (candidateLines === null) {
          candidateLines = new Set(lineNumbers);
        } else {
          const termSet = new Set(lineNumbers);
          for (const line of Array.from(candidateLines)) {
            if (!termSet.has(line)) {
              candidateLines.delete(line);
            }
          }
        }
        if (!candidateLines || candidateLines.size === 0) {
          groupMatched = false;
          break;
        }
      }
      if (!groupMatched || !candidateLines || candidateLines.size === 0) {
        continue;
      }
      for (const term of excludeTerms) {
        const key = resolveKey(term);
        if (key === null) {
          continue;
        }
        const termData = fieldIndex[key];
        if (!termData) {
          continue;
        }
        const excludeLines = this.indexManager._getAllLineNumbers(termData);
        if (!excludeLines || excludeLines.length === 0) {
          continue;
        }
        for (const line of excludeLines) {
          if (!candidateLines.size) {
            break;
          }
          candidateLines.delete(line);
        }
        if (!candidateLines.size) {
          break;
        }
      }
      if (candidateLines && candidateLines.size > 0) {
        matchedGroups++;
      }
    }
    if (matchedGroups === 0) {
      return 0;
    }
    const precision = typeof options.precision === 'number' && options.precision >= 0 ? options.precision : 2;
    const coverageValue = matchedGroups / groups.length * 100;
    return Number(coverageValue.toFixed(precision));
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
    this._validateInitialization('score');

    // Set default options
    const opts = {
      limit: options.limit ?? 100,
      sort: options.sort ?? 'desc',
      includeScore: options.includeScore !== false,
      mode: options.mode ?? 'sum'
    };

    // Validate fieldName
    if (typeof fieldName !== 'string' || !fieldName) {
      throw new Error('fieldName must be a non-empty string');
    }

    // Validate scores object
    if (!scores || typeof scores !== 'object' || Array.isArray(scores)) {
      throw new Error('scores must be an object');
    }

    // Handle empty scores - return empty array as specified
    if (Object.keys(scores).length === 0) {
      return [];
    }

    // Validate scores values are numeric
    for (const [term, weight] of Object.entries(scores)) {
      if (typeof weight !== 'number' || isNaN(weight)) {
        throw new Error(`Score value for term "${term}" must be a number`);
      }
    }

    // Validate mode
    const allowedModes = new Set(['sum', 'max', 'avg', 'first']);
    if (!allowedModes.has(opts.mode)) {
      throw new Error(`Invalid score mode "${opts.mode}". Must be one of: ${Array.from(allowedModes).join(', ')}`);
    }

    // Check if field is indexed and is array:string type
    if (!this.opts.indexes || !this.opts.indexes[fieldName]) {
      throw new Error(`Field "${fieldName}" is not indexed`);
    }
    const fieldType = this.opts.indexes[fieldName];
    if (fieldType !== 'array:string') {
      throw new Error(`Field "${fieldName}" must be of type "array:string" (found: ${fieldType})`);
    }

    // Check if this is a term-mapped field
    const isTermMapped = this.termManager && this.termManager.termMappingFields && this.termManager.termMappingFields.includes(fieldName);

    // Access the index for this field
    const fieldIndex = this.indexManager.index.data[fieldName];
    if (!fieldIndex) {
      return [];
    }

    // Accumulate scores for each line number
    const scoreMap = new Map();
    const countMap = opts.mode === 'avg' ? new Map() : null;

    // Iterate through each term in the scores object
    for (const [term, weight] of Object.entries(scores)) {
      // Get term ID if this is a term-mapped field
      let termKey;
      if (isTermMapped) {
        // For term-mapped fields, convert term to term ID
        const termId = this.termManager.getTermIdWithoutIncrement(term);
        if (termId === null || termId === undefined) {
          // Term doesn't exist, skip it
          continue;
        }
        termKey = String(termId);
      } else {
        termKey = String(term);
      }

      // Look up line numbers for this term
      const termData = fieldIndex[termKey];
      if (!termData) {
        // Term doesn't exist in index, skip
        continue;
      }

      // Get all line numbers for this term
      const lineNumbers = this.indexManager._getAllLineNumbers(termData);

      // Add weight to score for each line number
      for (const lineNumber of lineNumbers) {
        const currentScore = scoreMap.get(lineNumber);
        switch (opts.mode) {
          case 'sum':
            {
              const nextScore = (currentScore || 0) + weight;
              scoreMap.set(lineNumber, nextScore);
              break;
            }
          case 'max':
            {
              if (currentScore === undefined) {
                scoreMap.set(lineNumber, weight);
              } else {
                scoreMap.set(lineNumber, Math.max(currentScore, weight));
              }
              break;
            }
          case 'avg':
            {
              const previous = currentScore || 0;
              scoreMap.set(lineNumber, previous + weight);
              const count = (countMap.get(lineNumber) || 0) + 1;
              countMap.set(lineNumber, count);
              break;
            }
          case 'first':
            {
              if (currentScore === undefined) {
                scoreMap.set(lineNumber, weight);
              }
              break;
            }
        }
      }
    }

    // For average mode, divide total by count
    if (opts.mode === 'avg') {
      for (const [lineNumber, totalScore] of scoreMap.entries()) {
        const count = countMap.get(lineNumber) || 1;
        scoreMap.set(lineNumber, totalScore / count);
      }
    }

    // Filter out zero scores and sort by score
    const scoredEntries = Array.from(scoreMap.entries()).filter(([, score]) => score > 0);

    // Sort by score
    scoredEntries.sort((a, b) => {
      return opts.sort === 'asc' ? a[1] - b[1] : b[1] - a[1];
    });

    // Apply limit
    const limitedEntries = opts.limit > 0 ? scoredEntries.slice(0, opts.limit) : scoredEntries;
    if (limitedEntries.length === 0) {
      return [];
    }

    // Fetch actual records
    const lineNumbers = limitedEntries.map(([lineNumber]) => lineNumber);
    const scoresByLineNumber = new Map(limitedEntries);
    const persistedCount = Array.isArray(this.offsets) ? this.offsets.length : 0;

    // Separate lineNumbers into file records and writeBuffer records
    const fileLineNumbers = [];
    const writeBufferLineNumbers = [];
    for (const lineNumber of lineNumbers) {
      if (lineNumber >= persistedCount) {
        // This lineNumber points to writeBuffer
        writeBufferLineNumbers.push(lineNumber);
      } else {
        // This lineNumber points to file
        fileLineNumbers.push(lineNumber);
      }
    }
    const results = [];

    // Read records from file
    if (fileLineNumbers.length > 0) {
      const ranges = this.getRanges(fileLineNumbers);
      if (ranges.length > 0) {
        // Create a map from start offset to lineNumber for accurate mapping
        const startToLineNumber = new Map();
        for (const range of ranges) {
          if (range.index !== undefined) {
            startToLineNumber.set(range.start, range.index);
          }
        }
        const groupedRanges = await this.fileHandler.groupedRanges(ranges);
        const fs = await import('fs');
        const fd = await fs.promises.open(this.fileHandler.file, 'r');
        try {
          for (const groupedRange of groupedRanges) {
            var _iteratorAbruptCompletion3 = false;
            var _didIteratorError3 = false;
            var _iteratorError3;
            try {
              for (var _iterator3 = _asyncIterator(this.fileHandler.readGroupedRange(groupedRange, fd)), _step3; _iteratorAbruptCompletion3 = !(_step3 = await _iterator3.next()).done; _iteratorAbruptCompletion3 = false) {
                const row = _step3.value;
                {
                  try {
                    const record = this.serializer.deserialize(row.line);

                    // Get line number from the row, fallback to start offset mapping
                    let lineNumber = row._ !== null && row._ !== undefined ? row._ : startToLineNumber.get(row.start) ?? 0;

                    // Restore term IDs to terms
                    const recordWithTerms = this.restoreTermIdsAfterDeserialization(record);

                    // Add line number
                    recordWithTerms._ = lineNumber;

                    // Add score if includeScore is true (default is true)
                    if (opts.includeScore !== false) {
                      recordWithTerms.score = scoresByLineNumber.get(lineNumber) || 0;
                    }
                    results.push(recordWithTerms);
                  } catch (error) {
                    // Skip invalid lines
                    if (this.opts.debugMode) {
                      console.error('Error deserializing record in score():', error);
                    }
                  }
                }
              }
            } catch (err) {
              _didIteratorError3 = true;
              _iteratorError3 = err;
            } finally {
              try {
                if (_iteratorAbruptCompletion3 && _iterator3.return != null) {
                  await _iterator3.return();
                }
              } finally {
                if (_didIteratorError3) {
                  throw _iteratorError3;
                }
              }
            }
          }
        } finally {
          await fd.close();
        }
      }
    }

    // Read records from writeBuffer
    if (writeBufferLineNumbers.length > 0 && this.writeBuffer) {
      for (const lineNumber of writeBufferLineNumbers) {
        const writeBufferIndex = lineNumber - persistedCount;
        if (writeBufferIndex >= 0 && writeBufferIndex < this.writeBuffer.length) {
          const record = this.writeBuffer[writeBufferIndex];
          if (record) {
            // Restore term IDs to terms
            const recordWithTerms = this.restoreTermIdsAfterDeserialization(record);

            // Add line number
            recordWithTerms._ = lineNumber;

            // Add score if includeScore is true
            if (opts.includeScore) {
              recordWithTerms.score = scoresByLineNumber.get(lineNumber) || 0;
            }
            results.push(recordWithTerms);
          }
        }
      }
    }

    // Re-sort results to maintain score order (since reads might be out of order)
    results.sort((a, b) => {
      const scoreA = scoresByLineNumber.get(a._) || 0;
      const scoreB = scoresByLineNumber.get(b._) || 0;
      return opts.sort === 'asc' ? scoreA - scoreB : scoreB - scoreA;
    });
    return results;
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
    const beforeLength = this.writeBuffer.length;
    if (beforeLength > 0) {
      const originalRecords = this.writeBuffer;
      const originalOffsets = this.writeBufferOffsets;
      const originalSizes = this.writeBufferSizes;
      const retainedRecords = [];
      const retainedOffsets = [];
      const retainedSizes = [];
      let retainedTotal = 0;
      let removedCount = 0;
      for (let i = 0; i < originalRecords.length; i++) {
        const record = originalRecords[i];
        if (itemsToProcess.has(record)) {
          removedCount++;
          continue;
        }
        retainedRecords.push(record);
        if (originalOffsets && i < originalOffsets.length) {
          retainedOffsets.push(originalOffsets[i]);
        }
        if (originalSizes && i < originalSizes.length) {
          const size = originalSizes[i];
          if (size !== undefined) {
            retainedSizes.push(size);
            retainedTotal += size;
          }
        }
      }
      if (removedCount > 0) {
        this.writeBuffer = retainedRecords;
        this.writeBufferOffsets = retainedOffsets;
        this.writeBufferSizes = retainedSizes;
        this.writeBufferTotalSize = retainedTotal;
      }
    }
    const afterLength = this.writeBuffer.length;
    if (afterLength === 0) {
      this.writeBufferOffsets = [];
      this.writeBufferSizes = [];
      this.writeBufferTotalSize = 0;
    }
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
        const fs = await import('fs');
        const fileContent = await fs.promises.readFile(this.normalizedFile, 'utf8');
        const lines = fileContent.split('\n').filter(line => line.trim());
        for (let i = 0; i < lines.length && i < this.offsets.length; i++) {
          try {
            const record = this.serializer.deserialize(lines[i]);
            if (record && !deletedIdsSnapshot.has(String(record.id))) {
              // Check if this record is not being updated in writeBuffer
              // CRITICAL FIX: Normalize IDs to strings for consistent comparison
              const normalizedRecordId = String(record.id);
              const updatedRecord = writeBufferSnapshot.find(r => r && r.id && String(r.id) === normalizedRecordId);
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
    // CRITICAL FIX: Normalize IDs to strings for consistent comparison
    const updatedRecordsMap = new Map();
    writeBufferSnapshot.forEach((record, index) => {
      if (record && record.id !== undefined && record.id !== null) {
        // Normalize ID to string for consistent comparison
        const normalizedId = String(record.id);
        updatedRecordsMap.set(normalizedId, record);
        if (this.opts.debugMode) {
          console.log(`💾 Save: Added to updatedRecordsMap: ID=${normalizedId} (original: ${record.id}, type: ${typeof record.id}), index=${index}`);
        }
      } else if (this.opts.debugMode) {
        console.log(`⚠️ Save: Skipped record in writeBufferSnapshot[${index}] - missing or invalid ID:`, record ? {
          id: record.id,
          keys: Object.keys(record)
        } : 'null');
      }
    });
    if (this.opts.debugMode) {
      console.log(`💾 Save: updatedRecordsMap size: ${updatedRecordsMap.size}, keys:`, Array.from(updatedRecordsMap.keys()));
    }

    // OPTIMIZATION: Cache file stats to avoid repeated stat() calls
    let fileSize = 0;
    if (this._cachedFileStats && this._cachedFileStats.timestamp > Date.now() - 1000) {
      // Use cached stats if less than 1 second old
      fileSize = this._cachedFileStats.size;
    } else {
      // Get fresh stats and cache them
      const fileStats = (await this.fileHandler.exists()) ? await fs.promises.stat(this.normalizedFile) : null;
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
            // CRITICAL FIX: Use schema to find ID position, not hardcoded position
            // The schema defines the order of fields in the array
            if (this.serializer && this.serializer.schemaManager && this.serializer.schemaManager.isInitialized) {
              const schema = this.serializer.schemaManager.getSchema();
              const idIndex = schema.indexOf('id');
              if (idIndex !== -1 && arrayData.length > idIndex) {
                // ID is at the position defined by schema
                recordId = arrayData[idIndex];
              } else if (arrayData.length > schema.length) {
                // ID might be appended after schema fields (for backward compatibility)
                recordId = arrayData[schema.length];
              } else {
                // Fallback: use first element
                recordId = arrayData[0];
              }
            } else {
              // No schema available, try common positions
              if (arrayData.length > 2) {
                // Try position 2 (common in older formats)
                recordId = arrayData[2];
              } else {
                // Fallback: use first element
                recordId = arrayData[0];
              }
            }
            if (recordId !== undefined && recordId !== null) {
              recordId = String(recordId);
              // Check if this record needs full parsing (updated or deleted)
              // CRITICAL FIX: Normalize ID to string for consistent comparison
              needsFullParse = updatedRecordsMap.has(recordId) || deletedIdsSnapshot.has(String(recordId));
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
          // CRITICAL FIX: Normalize ID to string for consistent comparison
          needsFullParse = updatedRecordsMap.has(String(recordId)) || deletedIdsSnapshot.has(String(recordId));
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

        // CRITICAL FIX: Normalize ID to string for consistent comparison
        const normalizedId = String(recordWithIds.id);
        if (this.opts.debugMode) {
          console.log(`💾 Save: Checking record ID=${normalizedId} (original: ${recordWithIds.id}, type: ${typeof recordWithIds.id}) in updatedRecordsMap`);
          console.log(`💾 Save: updatedRecordsMap.has(${normalizedId}): ${updatedRecordsMap.has(normalizedId)}`);
          if (!updatedRecordsMap.has(normalizedId)) {
            console.log(`💾 Save: Record ${normalizedId} NOT found in updatedRecordsMap. Available keys:`, Array.from(updatedRecordsMap.keys()));
          }
        }
        if (updatedRecordsMap.has(normalizedId)) {
          // Replace with updated version
          const updatedRecord = updatedRecordsMap.get(normalizedId);
          if (this.opts.debugMode) {
            console.log(`💾 Save: ✅ REPLACING record ${recordWithIds.id} with updated version`);
            console.log(`💾 Save: Old record:`, {
              id: recordWithIds.id,
              price: recordWithIds.price,
              app_id: recordWithIds.app_id,
              currency: recordWithIds.currency
            });
            console.log(`💾 Save: New record:`, {
              id: updatedRecord.id,
              price: updatedRecord.price,
              app_id: updatedRecord.app_id,
              currency: updatedRecord.currency
            });
          }
          return {
            type: 'updated',
            record: updatedRecord,
            id: recordWithIds.id,
            needsParse: false
          };
        } else if (!deletedIdsSnapshot.has(String(recordWithIds.id))) {
          // Keep existing record if not deleted
          if (this.opts.debugMode) {
            console.log(`💾 Save: Kept record ${recordWithIds.id} (${recordWithIds.name || 'Unnamed'}) - not in deletedIdsSnapshot`);
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
            console.log(`💾 Save: Skipped record ${recordWithIds.id} (${recordWithIds.name || 'Unnamed'}) - deleted (found in deletedIdsSnapshot)`);
            console.log(`💾 Save: deletedIdsSnapshot contains:`, Array.from(deletedIdsSnapshot));
            console.log(`💾 Save: Record ID check: String(${recordWithIds.id}) = "${String(recordWithIds.id)}", has() = ${deletedIdsSnapshot.has(String(recordWithIds.id))}`);
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
          // CRITICAL FIX: Verify that unchanged records are not deleted
          // Extract ID from the line to check against deletedIdsSnapshot
          let unchangedRecordId = null;
          try {
            if (result.line.startsWith('[') && result.line.endsWith(']')) {
              const arrayData = JSON.parse(result.line);
              if (Array.isArray(arrayData) && arrayData.length > 0) {
                // CRITICAL FIX: Use schema to find ID position, not hardcoded position
                if (this.serializer && this.serializer.schemaManager && this.serializer.schemaManager.isInitialized) {
                  const schema = this.serializer.schemaManager.getSchema();
                  const idIndex = schema.indexOf('id');
                  if (idIndex !== -1 && arrayData.length > idIndex) {
                    unchangedRecordId = String(arrayData[idIndex]);
                  } else if (arrayData.length > schema.length) {
                    unchangedRecordId = String(arrayData[schema.length]);
                  } else {
                    unchangedRecordId = String(arrayData[0]);
                  }
                } else {
                  // No schema, try common positions
                  if (arrayData.length > 2) {
                    unchangedRecordId = String(arrayData[2]);
                  } else {
                    unchangedRecordId = String(arrayData[0]);
                  }
                }
              }
            } else {
              const obj = JSON.parse(result.line);
              unchangedRecordId = obj.id ? String(obj.id) : null;
            }
          } catch (e) {
            // If we can't parse, skip this record to be safe
            if (this.opts.debugMode) {
              console.log(`💾 Save: Could not parse unchanged record to check deletion: ${e.message}`);
            }
            continue;
          }

          // Skip if this record is deleted
          if (unchangedRecordId && deletedIdsSnapshot.has(unchangedRecordId)) {
            if (this.opts.debugMode) {
              console.log(`💾 Save: Skipping unchanged record ${unchangedRecordId} - deleted`);
            }
            deletedOffsets.add(offset);
            break;
          }

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
   * Get the base line number for writeBuffer entries (number of persisted records)
   * @private
   */
  _getWriteBufferBaseLineNumber() {
    return Array.isArray(this.offsets) ? this.offsets.length : 0;
  }

  /**
   * Convert a writeBuffer index into an absolute line number
   * @param {number} writeBufferIndex - Index inside writeBuffer (0-based)
   * @returns {number} Absolute line number (0-based)
   * @private
   */
  _getAbsoluteLineNumber(writeBufferIndex) {
    if (typeof writeBufferIndex !== 'number' || writeBufferIndex < 0) {
      throw new Error('Invalid writeBuffer index');
    }
    return this._getWriteBufferBaseLineNumber() + writeBufferIndex;
  }
  _streamingRecoveryGenerator(_x, _x2) {
    var _this = this;
    return _wrapAsyncGenerator(function* (criteria, options, alreadyYielded = 0, map = null, remainingSkipValue = 0) {
      if (_this._offsetRecoveryInProgress) {
        return;
      }
      if (!_this.fileHandler || !_this.fileHandler.file) {
        return;
      }
      _this._offsetRecoveryInProgress = true;
      const fsModule = _this._fsModule || (_this._fsModule = yield _awaitAsyncGenerator(import('fs')));
      let fd;
      try {
        fd = yield _awaitAsyncGenerator(fsModule.promises.open(_this.fileHandler.file, 'r'));
      } catch (error) {
        _this._offsetRecoveryInProgress = false;
        if (_this.opts.debugMode) {
          console.warn(`⚠️ Offset recovery skipped: ${error.message}`);
        }
        return;
      }
      const chunkSize = _this.opts.offsetRecoveryChunkSize || 64 * 1024;
      let buffer = Buffer.alloc(0);
      let readOffset = 0;
      const originalOffsets = Array.isArray(_this.offsets) ? [..._this.offsets] : [];
      const newOffsets = [];
      let offsetAdjusted = false;
      let lineIndex = 0;
      let lastLineEnd = 0;
      let producedTotal = alreadyYielded || 0;
      let remainingSkip = remainingSkipValue || 0;
      let remainingAlreadyYielded = alreadyYielded || 0;
      const limit = typeof options?.limit === 'number' ? options.limit : null;
      const includeOffsets = options?.includeOffsets === true;
      const includeLinePosition = _this.opts.includeLinePosition;
      const mapSet = map instanceof Set ? new Set(map) : Array.isArray(map) ? new Set(map) : null;
      const criteriaIsObject = criteria && typeof criteria === 'object' && !Array.isArray(criteria) && !(criteria instanceof Set);
      const hasCriteria = criteriaIsObject && Object.keys(criteria).length > 0;
      const decodeLineBuffer = lineBuffer => {
        let trimmed = lineBuffer;
        if (trimmed.length > 0 && trimmed[trimmed.length - 1] === 0x0A) {
          trimmed = trimmed.subarray(0, trimmed.length - 1);
        }
        if (trimmed.length > 0 && trimmed[trimmed.length - 1] === 0x0D) {
          trimmed = trimmed.subarray(0, trimmed.length - 1);
        }
        return trimmed;
      };
      const processLine = async (lineBuffer, lineStart) => {
        const lineLength = lineBuffer.length;
        newOffsets[lineIndex] = lineStart;
        const expected = originalOffsets[lineIndex];
        if (expected !== undefined && expected !== lineStart) {
          offsetAdjusted = true;
          if (_this.opts.debugMode) {
            console.warn(`⚠️ Offset mismatch detected at line ${lineIndex}: expected ${expected}, actual ${lineStart}`);
          }
        } else if (expected === undefined) {
          offsetAdjusted = true;
        }
        lastLineEnd = Math.max(lastLineEnd, lineStart + lineLength);
        let entryWithTerms = null;
        let shouldYield = false;
        const decodedBuffer = decodeLineBuffer(lineBuffer);
        if (decodedBuffer.length > 0) {
          let lineString;
          try {
            lineString = decodedBuffer.toString('utf8');
          } catch (error) {
            lineString = decodedBuffer.toString('utf8', {
              replacement: '?'
            });
          }
          try {
            const record = await _this.serializer.deserialize(lineString);
            if (record && typeof record === 'object') {
              entryWithTerms = _this.restoreTermIdsAfterDeserialization(record);
              if (includeLinePosition) {
                entryWithTerms._ = lineIndex;
              }
              if (mapSet) {
                shouldYield = mapSet.has(lineIndex);
                if (shouldYield) {
                  mapSet.delete(lineIndex);
                }
              } else if (hasCriteria) {
                shouldYield = _this.queryManager.matchesCriteria(entryWithTerms, criteria, options);
              } else {
                shouldYield = true;
              }
            }
          } catch (error) {
            if (_this.opts.debugMode) {
              console.warn(`⚠️ Offset recovery failed to deserialize line ${lineIndex} at ${lineStart}: ${error.message}`);
            }
          }
        }
        let yieldedEntry = null;
        if (shouldYield && entryWithTerms) {
          if (remainingSkip > 0) {
            remainingSkip--;
          } else if (remainingAlreadyYielded > 0) {
            remainingAlreadyYielded--;
          } else if (!limit || producedTotal < limit) {
            producedTotal++;
            yieldedEntry = includeOffsets ? {
              entry: entryWithTerms,
              start: lineStart,
              _: lineIndex
            } : entryWithTerms;
          } else ;
        }
        lineIndex++;
        if (yieldedEntry) {
          return yieldedEntry;
        }
        return null;
      };
      let recoveryFailed = false;
      try {
        while (true) {
          const tempBuffer = Buffer.allocUnsafe(chunkSize);
          const {
            bytesRead
          } = yield _awaitAsyncGenerator(fd.read(tempBuffer, 0, chunkSize, readOffset));
          if (bytesRead === 0) {
            if (buffer.length > 0) {
              const lineStart = readOffset - buffer.length;
              const yieldedEntry = yield _awaitAsyncGenerator(processLine(buffer, lineStart));
              if (yieldedEntry) {
                yield yieldedEntry;
              }
            }
            break;
          }
          readOffset += bytesRead;
          let chunk = buffer.length > 0 ? Buffer.concat([buffer, tempBuffer.subarray(0, bytesRead)]) : tempBuffer.subarray(0, bytesRead);
          let processedUpTo = 0;
          const chunkBaseOffset = readOffset - chunk.length;
          while (true) {
            const newlineIndex = chunk.indexOf(0x0A, processedUpTo);
            if (newlineIndex === -1) {
              break;
            }
            const lineBuffer = chunk.subarray(processedUpTo, newlineIndex + 1);
            const lineStart = chunkBaseOffset + processedUpTo;
            const yieldedEntry = yield _awaitAsyncGenerator(processLine(lineBuffer, lineStart));
            processedUpTo = newlineIndex + 1;
            if (yieldedEntry) {
              yield yieldedEntry;
            }
          }
          buffer = chunk.subarray(processedUpTo);
        }
      } catch (error) {
        recoveryFailed = true;
        if (_this.opts.debugMode) {
          console.warn(`⚠️ Offset recovery aborted: ${error.message}`);
        }
      } finally {
        yield _awaitAsyncGenerator(fd.close().catch(() => {}));
        _this._offsetRecoveryInProgress = false;
        if (recoveryFailed) {
          return;
        }
        _this.offsets = newOffsets;
        if (lineIndex < _this.offsets.length) {
          _this.offsets.length = lineIndex;
        }
        if (originalOffsets.length !== newOffsets.length) {
          offsetAdjusted = true;
        }
        _this.indexOffset = lastLineEnd;
        if (offsetAdjusted) {
          _this.shouldSave = true;
          try {
            yield _awaitAsyncGenerator(_this._saveIndexDataToFile());
          } catch (error) {
            if (_this.opts.debugMode) {
              console.warn(`⚠️ Failed to persist recovered offsets: ${error.message}`);
            }
          }
        }
      }
    }).apply(this, arguments);
  }

  /**
   * Walk through records using streaming (real implementation)
   */
  walk(_x3) {
    var _this2 = this;
    return _wrapAsyncGenerator(function* (criteria, options = {}) {
      // CRITICAL FIX: Validate state before walk operation to prevent crashes
      _this2.validateState();
      if (!_this2.initialized) yield _awaitAsyncGenerator(_this2.init());

      // If no data at all, return empty
      if (_this2.indexOffset === 0 && _this2.writeBuffer.length === 0) return;
      let count = 0;
      let remainingSkip = options.skip || 0;
      let map;
      if (!Array.isArray(criteria)) {
        if (criteria instanceof Set) {
          map = [...criteria];
        } else if (criteria && typeof criteria === 'object' && Object.keys(criteria).length > 0) {
          // Only use indexManager.query if criteria has actual filters
          map = [..._this2.indexManager.query(criteria, options)];
        } else {
          // For empty criteria {} or null/undefined, get all records
          const totalRecords = _this2.offsets && _this2.offsets.length > 0 ? _this2.offsets.length : _this2.writeBuffer.length;
          map = [...Array(totalRecords).keys()];
        }
      } else {
        map = criteria;
      }

      // Use writeBuffer when available (unsaved data)
      if (_this2.writeBuffer.length > 0) {
        let count = 0;

        // If map is empty (no index results) but we have criteria, filter writeBuffer directly
        if (map.length === 0 && criteria && typeof criteria === 'object' && Object.keys(criteria).length > 0) {
          for (let i = 0; i < _this2.writeBuffer.length; i++) {
            if (options.limit && count >= options.limit) {
              break;
            }
            const entry = _this2.writeBuffer[i];
            if (entry && _this2.queryManager.matchesCriteria(entry, criteria, options)) {
              if (remainingSkip > 0) {
                remainingSkip--;
                continue;
              }
              count++;
              if (options.includeOffsets) {
                yield {
                  entry,
                  start: 0,
                  _: i
                };
              } else {
                if (_this2.opts.includeLinePosition) {
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
            if (lineNumber < _this2.writeBuffer.length) {
              const entry = _this2.writeBuffer[lineNumber];
              if (entry) {
                if (remainingSkip > 0) {
                  remainingSkip--;
                  continue;
                }
                count++;
                if (options.includeOffsets) {
                  yield {
                    entry,
                    start: 0,
                    _: lineNumber
                  };
                } else {
                  if (_this2.opts.includeLinePosition) {
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
      if (_this2.writeBuffer.length === 0 && _this2.indexOffset > 0) {
        // Load data from file for querying
        try {
          let data;
          let lines;

          // Smart threshold: decide between partial reads vs full read
          const resultPercentage = map ? map.length / _this2.indexOffset * 100 : 100;
          const threshold = _this2.opts.partialReadThreshold || 60; // Default 60% threshold

          // Use partial reads when:
          // 1. We have specific line numbers from index
          // 2. Results are below threshold percentage
          // 3. Database is large enough to benefit from partial reads
          const shouldUsePartialReads = map && map.length > 0 && resultPercentage < threshold && _this2.indexOffset > 100; // Only for databases with >100 records

          if (shouldUsePartialReads) {
            if (_this2.opts.debugMode) {
              console.log(`🔍 Using PARTIAL READS: ${map.length}/${_this2.indexOffset} records (${resultPercentage.toFixed(1)}% < ${threshold}% threshold)`);
            }
            // OPTIMIZATION: Use ranges instead of reading entire file
            const ranges = _this2.getRanges(map);
            const groupedRanges = yield _awaitAsyncGenerator(_this2.fileHandler.groupedRanges(ranges));
            const fs = yield _awaitAsyncGenerator(import('fs'));
            const fd = yield _awaitAsyncGenerator(fs.promises.open(_this2.fileHandler.file, 'r'));
            try {
              for (const groupedRange of groupedRanges) {
                var _iteratorAbruptCompletion4 = false;
                var _didIteratorError4 = false;
                var _iteratorError4;
                try {
                  for (var _iterator4 = _asyncIterator(_this2.fileHandler.readGroupedRange(groupedRange, fd)), _step4; _iteratorAbruptCompletion4 = !(_step4 = yield _awaitAsyncGenerator(_iterator4.next())).done; _iteratorAbruptCompletion4 = false) {
                    const row = _step4.value;
                    {
                      if (options.limit && count >= options.limit) {
                        break;
                      }
                      try {
                        // CRITICAL FIX: Use serializer.deserialize instead of JSON.parse to handle array format
                        const record = _this2.serializer.deserialize(row.line);
                        // SPACE OPTIMIZATION: Restore term IDs to terms for user
                        const recordWithTerms = _this2.restoreTermIdsAfterDeserialization(record);
                        if (remainingSkip > 0) {
                          remainingSkip--;
                          continue;
                        }
                        count++;
                        if (options.includeOffsets) {
                          yield {
                            entry: recordWithTerms,
                            start: row.start,
                            _: row._ || 0
                          };
                        } else {
                          if (_this2.opts.includeLinePosition) {
                            recordWithTerms._ = row._ || 0;
                          }
                          yield recordWithTerms;
                        }
                      } catch (error) {
                        // CRITICAL FIX: Log deserialization errors instead of silently ignoring them
                        // This helps identify data corruption issues
                        if (1 || _this2.opts.debugMode) {
                          console.warn(`⚠️ walk(): Failed to deserialize record at offset ${row.start}: ${error.message}`);
                          console.warn(`⚠️ walk(): Problematic line (first 200 chars): ${row.line.substring(0, 200)}`);
                        }
                        if (!_this2._offsetRecoveryInProgress) {
                          var _iteratorAbruptCompletion5 = false;
                          var _didIteratorError5 = false;
                          var _iteratorError5;
                          try {
                            for (var _iterator5 = _asyncIterator(_this2._streamingRecoveryGenerator(criteria, options, count, map, remainingSkip)), _step5; _iteratorAbruptCompletion5 = !(_step5 = yield _awaitAsyncGenerator(_iterator5.next())).done; _iteratorAbruptCompletion5 = false) {
                              const recoveredEntry = _step5.value;
                              {
                                yield recoveredEntry;
                                count++;
                              }
                            }
                          } catch (err) {
                            _didIteratorError5 = true;
                            _iteratorError5 = err;
                          } finally {
                            try {
                              if (_iteratorAbruptCompletion5 && _iterator5.return != null) {
                                yield _awaitAsyncGenerator(_iterator5.return());
                              }
                            } finally {
                              if (_didIteratorError5) {
                                throw _iteratorError5;
                              }
                            }
                          }
                          return;
                        }
                        // Skip invalid lines but continue processing
                        // This prevents one corrupted record from stopping the entire walk operation
                      }
                    }
                  }
                } catch (err) {
                  _didIteratorError4 = true;
                  _iteratorError4 = err;
                } finally {
                  try {
                    if (_iteratorAbruptCompletion4 && _iterator4.return != null) {
                      yield _awaitAsyncGenerator(_iterator4.return());
                    }
                  } finally {
                    if (_didIteratorError4) {
                      throw _iteratorError4;
                    }
                  }
                }
                if (options.limit && count >= options.limit) {
                  break;
                }
              }
            } finally {
              yield _awaitAsyncGenerator(fd.close());
            }
            return; // Exit early since we processed partial reads
          } else {
            if (_this2.opts.debugMode) {
              console.log(`🔍 Using STREAMING READ: ${map?.length || 0}/${_this2.indexOffset} records (${resultPercentage.toFixed(1)}% >= ${threshold}% threshold or small DB)`);
            }
            // Use streaming instead of loading all data in memory
            // This prevents memory issues with large databases
            const streamingResults = yield _awaitAsyncGenerator(_this2.fileHandler.readWithStreaming(criteria, {
              limit: options.limit,
              skip: options.skip
            }, matchesCriteria, _this2.serializer));

            // Process streaming results directly without loading all lines
            for (const record of streamingResults) {
              if (options.limit && count >= options.limit) {
                break;
              }
              if (remainingSkip > 0) {
                remainingSkip--;
                continue;
              }
              count++;

              // SPACE OPTIMIZATION: Restore term IDs to terms for user
              const recordWithTerms = _this2.restoreTermIdsAfterDeserialization(record);
              if (options.includeOffsets) {
                yield {
                  entry: recordWithTerms,
                  start: 0,
                  _: 0
                };
              } else {
                if (_this2.opts.includeLinePosition) {
                  recordWithTerms._ = 0;
                }
                yield recordWithTerms;
              }
            }
            return; // Exit early since we processed streaming results
          }
        } catch (error) {
          // If file reading fails, continue to file-based streaming
        }
      }

      // Use file-based streaming for saved data
      const ranges = _this2.getRanges(map);
      const groupedRanges = yield _awaitAsyncGenerator(_this2.fileHandler.groupedRanges(ranges));
      const fd = yield _awaitAsyncGenerator(fs.promises.open(_this2.fileHandler.file, 'r'));
      try {
        let count = 0;
        for (const groupedRange of groupedRanges) {
          if (options.limit && count >= options.limit) {
            break;
          }
          var _iteratorAbruptCompletion6 = false;
          var _didIteratorError6 = false;
          var _iteratorError6;
          try {
            for (var _iterator6 = _asyncIterator(_this2.fileHandler.readGroupedRange(groupedRange, fd)), _step6; _iteratorAbruptCompletion6 = !(_step6 = yield _awaitAsyncGenerator(_iterator6.next())).done; _iteratorAbruptCompletion6 = false) {
              const row = _step6.value;
              {
                if (options.limit && count >= options.limit) {
                  break;
                }
                try {
                  const entry = yield _awaitAsyncGenerator(_this2.serializer.deserialize(row.line, {
                    compress: _this2.opts.compress,
                    v8: _this2.opts.v8
                  }));
                  if (entry === null) continue;

                  // SPACE OPTIMIZATION: Restore term IDs to terms for user
                  const entryWithTerms = _this2.restoreTermIdsAfterDeserialization(entry);
                  if (remainingSkip > 0) {
                    remainingSkip--;
                    continue;
                  }
                  count++;
                  if (options.includeOffsets) {
                    yield {
                      entry: entryWithTerms,
                      start: row.start,
                      _: row._ || _this2.offsets.findIndex(n => n === row.start)
                    };
                  } else {
                    if (_this2.opts.includeLinePosition) {
                      entryWithTerms._ = row._ || _this2.offsets.findIndex(n => n === row.start);
                    }
                    yield entryWithTerms;
                  }
                } catch (error) {
                  // CRITICAL FIX: Log deserialization errors instead of silently ignoring them
                  // This helps identify data corruption issues
                  if (1 || _this2.opts.debugMode) {
                    console.warn(`⚠️ walk(): Failed to deserialize record at offset ${row.start}: ${error.message}`);
                    console.warn(`⚠️ walk(): Problematic line (first 200 chars): ${row.line.substring(0, 200)}`);
                  }
                  if (!_this2._offsetRecoveryInProgress) {
                    var _iteratorAbruptCompletion7 = false;
                    var _didIteratorError7 = false;
                    var _iteratorError7;
                    try {
                      for (var _iterator7 = _asyncIterator(_this2._streamingRecoveryGenerator(criteria, options, count, map, remainingSkip)), _step7; _iteratorAbruptCompletion7 = !(_step7 = yield _awaitAsyncGenerator(_iterator7.next())).done; _iteratorAbruptCompletion7 = false) {
                        const recoveredEntry = _step7.value;
                        {
                          yield recoveredEntry;
                          count++;
                        }
                      }
                    } catch (err) {
                      _didIteratorError7 = true;
                      _iteratorError7 = err;
                    } finally {
                      try {
                        if (_iteratorAbruptCompletion7 && _iterator7.return != null) {
                          yield _awaitAsyncGenerator(_iterator7.return());
                        }
                      } finally {
                        if (_didIteratorError7) {
                          throw _iteratorError7;
                        }
                      }
                    }
                    return;
                  }
                  // Skip invalid lines but continue processing
                  // This prevents one corrupted record from stopping the entire walk operation
                }
              }
            }
          } catch (err) {
            _didIteratorError6 = true;
            _iteratorError6 = err;
          } finally {
            try {
              if (_iteratorAbruptCompletion6 && _iterator6.return != null) {
                yield _awaitAsyncGenerator(_iterator6.return());
              }
            } finally {
              if (_didIteratorError6) {
                throw _iteratorError6;
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
  iterate(_x4) {
    var _this3 = this;
    return _wrapAsyncGenerator(function* (criteria, options = {}) {
      // CRITICAL FIX: Validate state before iterate operation
      _this3.validateState();
      if (!_this3.initialized) yield _awaitAsyncGenerator(_this3.init());

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
      if (_this3.indexOffset === 0 && _this3.writeBuffer.length === 0) return;
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
        var _iteratorAbruptCompletion8 = false;
        var _didIteratorError8 = false;
        var _iteratorError8;
        try {
          for (var _iterator8 = _asyncIterator(_this3.walk(criteria, options)), _step8; _iteratorAbruptCompletion8 = !(_step8 = yield _awaitAsyncGenerator(_iterator8.next())).done; _iteratorAbruptCompletion8 = false) {
            const entry = _step8.value;
            {
              processedCount++;

              // Store original record for change detection BEFORE yielding
              let originalRecord = null;
              if (opts.detectChanges) {
                originalRecord = _this3._createShallowCopy(entry);
                originalRecords.set(entry.id, originalRecord);
              }

              // Create wrapper based on performance preference
              const entryWrapper = opts.highPerformance ? _this3._createHighPerformanceWrapper(entry, originalRecord) : _this3._createEntryProxy(entry, originalRecord);

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
                if (_this3._hasRecordChanged(entry, originalRecord)) {
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
                yield _awaitAsyncGenerator(_this3._processIterateBatch(updateBuffer, deleteBuffer, opts));

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
          _didIteratorError8 = true;
          _iteratorError8 = err;
        } finally {
          try {
            if (_iteratorAbruptCompletion8 && _iterator8.return != null) {
              yield _awaitAsyncGenerator(_iterator8.return());
            }
          } finally {
            if (_didIteratorError8) {
              throw _iteratorError8;
            }
          }
        }
        if (updateBuffer.length > 0 || deleteBuffer.size > 0) {
          yield _awaitAsyncGenerator(_this3._processIterateBatch(updateBuffer, deleteBuffer, opts));
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
        if (_this3.opts.debugMode) {
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
          let targetIndex;
          if (index !== -1) {
            // Record is already in writeBuffer, update it
            this.writeBuffer[index] = record;
            targetIndex = index;
          } else {
            // Record is in file, add updated version to writeBuffer
            this.writeBuffer.push(record);
            targetIndex = this.writeBuffer.length - 1;
          }

          // Update index
          const absoluteLineNumber = this._getAbsoluteLineNumber(targetIndex);
          await this.indexManager.update(record, record, absoluteLineNumber);
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
        // Only save index data if it actually has content
        // Don't overwrite a valid index with an empty one
        if (this.indexManager && this.indexManager.indexedFields && this.indexManager.indexedFields.length > 0) {
          let hasIndexData = false;
          for (const field of this.indexManager.indexedFields) {
            if (this.indexManager.hasUsableIndexData(field)) {
              hasIndexData = true;
              break;
            }
          }
          // Only save if we have actual index data OR if offsets are populated
          // (offsets being populated means we've processed data)
          if (hasIndexData || this.offsets && this.offsets.length > 0) {
            await this._saveIndexDataToFile();
          } else if (this.opts.debugMode) {
            console.log('⚠️ close(): Skipping index save - index is empty and no offsets');
          }
        }
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
        const indexJSON = this.indexManager.indexedFields && this.indexManager.indexedFields.length > 0 ? this.indexManager.toJSON() : {};

        // Check if index is empty
        const isEmpty = !indexJSON || Object.keys(indexJSON).length === 0 || this.indexManager.indexedFields && this.indexManager.indexedFields.every(field => {
          const fieldIndex = indexJSON[field];
          return !fieldIndex || typeof fieldIndex === 'object' && Object.keys(fieldIndex).length === 0;
        });

        // PROTECTION: Don't overwrite a valid index file with empty data
        // If the .idx.jdb file exists and has data, and we're trying to save empty index,
        // skip the save to prevent corruption
        if (isEmpty && !this.offsets?.length) {
          const fs = await import('fs');
          if (fs.existsSync(idxPath)) {
            try {
              const existingData = JSON.parse(await fs.promises.readFile(idxPath, 'utf8'));
              const existingHasData = existingData.index && Object.keys(existingData.index).length > 0;
              const existingHasOffsets = existingData.offsets && existingData.offsets.length > 0;
              if (existingHasData || existingHasOffsets) {
                if (this.opts.debugMode) {
                  console.log(`⚠️ _saveIndexDataToFile: Skipping save - would overwrite valid index with empty data`);
                }
                return; // Don't overwrite valid index with empty one
              }
            } catch (error) {
              // If we can't read existing file, proceed with save (might be corrupted)
              if (this.opts.debugMode) {
                console.log(`⚠️ _saveIndexDataToFile: Could not read existing index file, proceeding with save`);
              }
            }
          }
        }
        const indexData = {
          index: indexJSON,
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
exports.default = Database;
