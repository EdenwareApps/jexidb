/**
 * IndexManager - In-memory index management
 * Supports different data types and query operations
 */
class IndexManager {
  constructor(indexes = {}) {
    this.indexes = {};
    this.offsets = [];
    this.recordCount = 0;
    
    // Initialize indexes based on configuration
    for (const [field, type] of Object.entries(indexes)) {
      this.indexes[field] = {
        type,
        values: new Map() // Map<value, Set<offsetIndex>>
      };
    }
  }

  /**
   * Adds a record to the index
   */
  addRecord(record, offsetIndex) {
    this.offsets[offsetIndex] = record._offset || 0;
    this.recordCount = Math.max(this.recordCount, offsetIndex + 1);
    
    // Add to indexes
    for (const [field, index] of Object.entries(this.indexes)) {
      const value = this.getNestedValue(record, field);
      if (value !== undefined) {
        if (!index.values.has(value)) {
          index.values.set(value, new Set());
        }
        index.values.get(value).add(offsetIndex);
      }
    }
  }

  /**
   * Removes a record from the index
   */
  removeRecord(offsetIndex) {
    // Remove from indexes
    for (const [field, index] of Object.entries(this.indexes)) {
      for (const [value, offsetSet] of index.values.entries()) {
        offsetSet.delete(offsetIndex);
        if (offsetSet.size === 0) {
          index.values.delete(value);
        }
      }
    }
    
    // Mark as removed in the offsets array
    this.offsets[offsetIndex] = null;
  }

  /**
   * Updates a record in the index
   */
  updateRecord(record, offsetIndex, oldRecord = null) {
    // Remove old values from indexes
    if (oldRecord) {
      for (const [field, index] of Object.entries(this.indexes)) {
        const oldValue = this.getNestedValue(oldRecord, field);
        if (oldValue !== undefined) {
          const offsetSet = index.values.get(oldValue);
          if (offsetSet) {
            offsetSet.delete(offsetIndex);
            if (offsetSet.size === 0) {
              index.values.delete(oldValue);
            }
          }
        }
      }
    }
    
    // Add new values to indexes
    for (const [field, index] of Object.entries(this.indexes)) {
      const value = this.getNestedValue(record, field);
      if (value !== undefined) {
        if (!index.values.has(value)) {
          index.values.set(value, new Set());
        }
        index.values.get(value).add(offsetIndex);
      }
    }
    
    // Update offset
    this.offsets[offsetIndex] = record._offset || 0;
  }

  /**
   * Searches records based on criteria
   */
  findRecords(criteria, options = {}) {
    const { caseInsensitive = false, matchAny = false } = options;
    
    if (!criteria || Object.keys(criteria).length === 0) {
      // Returns all valid records
      return Array.from(this.offsets.keys()).filter(i => this.offsets[i] !== null);
    }
    
    let matchingOffsets = null;
    
    for (const [field, criteriaValue] of Object.entries(criteria)) {
      const index = this.indexes[field];
      if (!index) {
        // If no index exists for this field, we need to scan all records
        // For now, return empty result if any field doesn't have an index
        return [];
      }
      
      const fieldOffsets = this.findFieldMatches(field, criteriaValue, caseInsensitive);
      
      if (matchingOffsets === null) {
        matchingOffsets = fieldOffsets;
      } else if (matchAny) {
        // Union (OR)
        matchingOffsets = new Set([...matchingOffsets, ...fieldOffsets]);
      } else {
        // Intersection (AND)
        matchingOffsets = new Set([...matchingOffsets].filter(x => fieldOffsets.has(x)));
      }
      
      if (!matchAny && matchingOffsets.size === 0) {
        break; // No intersection, stop search
      }
    }
    
    return matchingOffsets ? Array.from(matchingOffsets) : [];
  }

  /**
   * Searches for matches in a specific field
   */
  findFieldMatches(field, criteriaValue, caseInsensitive) {
    const index = this.indexes[field];
    if (!index) return new Set();
    
    const matches = new Set();
    
    if (typeof criteriaValue === 'object' && !Array.isArray(criteriaValue)) {
      // Comparison operators
      for (const [value, offsetSet] of index.values.entries()) {
        if (this.matchesOperator(value, criteriaValue, caseInsensitive)) {
          for (const offset of offsetSet) {
            matches.add(offset);
          }
        }
      }
    } else {
      // Direct comparison
      const values = Array.isArray(criteriaValue) ? criteriaValue : [criteriaValue];
      for (const searchValue of values) {
        const offsetSet = index.values.get(searchValue);
        if (offsetSet) {
          for (const offset of offsetSet) {
            matches.add(offset);
          }
        }
      }
    }
    
    return matches;
  }

  /**
   * Checks if a value matches the operators
   */
  matchesOperator(value, operators, caseInsensitive) {
    for (const [operator, operatorValue] of Object.entries(operators)) {
      switch (operator) {
        case '>':
          if (value <= operatorValue) return false;
          break;
        case '>=':
          if (value < operatorValue) return false;
          break;
        case '<':
          if (value >= operatorValue) return false;
          break;
        case '<=':
          if (value > operatorValue) return false;
          break;
        case '!=':
          if (value === operatorValue) return false;
          break;
        case 'in':
          if (!Array.isArray(operatorValue) || !operatorValue.includes(value)) return false;
          break;
        case 'nin':
          if (Array.isArray(operatorValue) && operatorValue.includes(value)) return false;
          break;
        case 'regex':
          const regex = new RegExp(operatorValue, caseInsensitive ? 'i' : '');
          if (!regex.test(String(value))) return false;
          break;
        case 'contains':
          const searchStr = String(operatorValue);
          const valueStr = String(value);
          if (caseInsensitive) {
            if (!valueStr.toLowerCase().includes(searchStr.toLowerCase())) return false;
          } else {
            if (!valueStr.includes(searchStr)) return false;
          }
          break;
      }
    }
    return true;
  }

  /**
   * Gets nested value from an object
   */
  getNestedValue(obj, path) {
    return path.split('.').reduce((current, key) => {
      return current && current[key] !== undefined ? current[key] : undefined;
    }, obj);
  }

  /**
   * Recalculates all offsets after modifications
   */
  recalculateOffsets() {
    let currentOffset = 0;
    const newOffsets = [];
    
    for (let i = 0; i < this.offsets.length; i++) {
      if (this.offsets[i] !== null) {
        newOffsets[i] = currentOffset;
        currentOffset += this.offsets[i];
      }
    }
    
    this.offsets = newOffsets;
  }

  /**
   * Recalculates offsets after a file rewrite
   * This method should be called after the file has been rewritten
   */
  async recalculateOffsetsFromFile(fileHandler) {
    const newOffsets = [];
    let currentOffset = 0;
    let recordIndex = 0;
    
    // Read the file line by line and recalculate offsets
    for await (const line of this.walkFile(fileHandler)) {
      if (line && !line._deleted) {
        newOffsets[recordIndex] = currentOffset;
        currentOffset += fileHandler.getByteLength(fileHandler.serialize(line));
        recordIndex++;
      }
    }
    
    this.offsets = newOffsets;
    this.recordCount = recordIndex;
  }

  /**
   * Walks through the file to read all records
   */
  async *walkFile(fileHandler) {
    let offset = 0;
    
    while (true) {
      const line = await fileHandler.readLine(offset);
      if (line === null) break;
      
      try {
        const record = fileHandler.deserialize(line);
        yield record;
        offset += fileHandler.getByteLength(line + '\n');
      } catch (error) {
        // Skip corrupted lines
        offset += fileHandler.getByteLength(line + '\n');
      }
    }
  }

  /**
   * Gets index statistics
   */
  getStats() {
    const stats = {
      recordCount: this.recordCount,
      indexCount: Object.keys(this.indexes).length,
      indexes: {}
    };
    
    for (const [field, index] of Object.entries(this.indexes)) {
      stats.indexes[field] = {
        type: index.type,
        uniqueValues: index.values.size,
        totalReferences: Array.from(index.values.values()).reduce((sum, set) => sum + set.size, 0)
      };
    }
    
    return stats;
  }

  /**
   * Clears all indexes
   */
  clear() {
    this.indexes = {};
    this.offsets = [];
    this.recordCount = 0;
  }

  /**
   * Serializes indexes for persistence
   */
  serialize() {
    const serialized = {
      indexes: {},
      offsets: this.offsets,
      recordCount: this.recordCount
    };
    
    for (const [field, index] of Object.entries(this.indexes)) {
      serialized.indexes[field] = {
        type: index.type,
        values: {}
      };
      
      for (const [value, offsetSet] of index.values.entries()) {
        serialized.indexes[field].values[value] = Array.from(offsetSet);
      }
    }
    
    return serialized;
  }

  /**
   * Deserializes indexes from persistence
   */
  deserialize(data) {
    if (!data || !data.indexes) return;
    this.indexes = {};
    this.offsets = data.offsets || [];
    this.recordCount = data.recordCount || 0;
    
    for (const [field, indexData] of Object.entries(data.indexes)) {
      const type = indexData.type;
      const values = new Map();
      for (const [valueStr, offsetArr] of Object.entries(indexData.values)) {
        let key;
        if (type === 'number') {
          key = Number(valueStr);
        } else if (type === 'boolean') {
          key = valueStr === 'true';
        } else {
          key = valueStr;
        }
        values.set(key, new Set(offsetArr));
      }
      this.indexes[field] = { type, values };
    }
  }
}

export default IndexManager; 