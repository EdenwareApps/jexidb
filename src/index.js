import JSONLDatabase from './JSONLDatabase.js';
import FileHandler from './FileHandler.js';
import IndexManager from './IndexManager.js';
import IntegrityChecker from './IntegrityChecker.js';

/**
 * JexiDB Compatibility Wrapper
 * Extends JSONLDatabase to provide backward compatibility with JexiDB 1.x test expectations
 */
class JexiDBCompatibility extends JSONLDatabase {
  constructor(filePath, options = {}) {
    // Support both .jdb and .jsonl extensions
    // .jdb is the preferred extension for JexiDB databases
    let normalizedPath = filePath;
    
    // If no extension is provided, default to .jdb
    if (!filePath.includes('.')) {
      normalizedPath = filePath + '.jdb';
    }
    
    // If .jdb extension is used, it's internally stored as JSONL format
    // but the user sees .jdb for better branding
    if (normalizedPath.endsWith('.jdb')) {
      // Store internally as .jsonl but present as .jdb to user
      const jsonlPath = normalizedPath.replace('.jdb', '.jsonl');
      super(jsonlPath, options);
      this.userPath = normalizedPath; // Keep track of user's preferred path
    } else {
      super(normalizedPath, options);
      this.userPath = normalizedPath;
    }
    
    this.isDestroyed = false;
  }

  /**
   * Get the user's preferred file path (with .jdb extension if used)
   */
  get userFilePath() {
    return this.userPath;
  }

  /**
   * Compatibility method: destroy() -> close()
   */
  async destroy() {
    this.isDestroyed = true;
    return this.close();
  }

  /**
   * Compatibility method: findOne() -> find() with limit 1
   */
  async findOne(criteria = {}) {
    const results = await this.find(criteria, { limit: 1 });
    return results.length > 0 ? results[0] : null;
  }

  /**
   * Enhanced find method with options support
   */
  async find(criteria = {}, options = {}) {
    let results = await super.find(criteria);
    
    // Apply sorting
    if (options.sort) {
      results = this.sortResults(results, options.sort);
    }
    
    // Apply skip
    if (options.skip) {
      results = results.slice(options.skip);
    }
    
    // Apply limit
    if (options.limit) {
      results = results.slice(0, options.limit);
    }
    
    return results;
  }

  /**
   * Retrocompatibility method: query() -> find()
   * Supports the same API as JexiDB 1.x
   */
  async query(criteria = {}, options = {}) {
    // Handle caseInsensitive option from JexiDB 1.x
    if (options.caseInsensitive) {
      // For case insensitive queries, we need to modify the criteria
      const caseInsensitiveCriteria = {};
      for (const [key, value] of Object.entries(criteria)) {
        if (typeof value === 'string') {
          // Convert string values to regex for case insensitive matching
          caseInsensitiveCriteria[key] = { $regex: value, $options: 'i' };
        } else {
          caseInsensitiveCriteria[key] = value;
        }
      }
      criteria = caseInsensitiveCriteria;
    }
    
    return await this.find(criteria, options);
  }

  /**
   * Sort results based on criteria
   */
  sortResults(results, sortCriteria) {
    return results.sort((a, b) => {
      for (const [field, direction] of Object.entries(sortCriteria)) {
        const aValue = this.getNestedValue(a, field);
        const bValue = this.getNestedValue(b, field);
        
        if (aValue < bValue) return direction === 1 ? -1 : 1;
        if (aValue > bValue) return direction === 1 ? 1 : -1;
      }
      return 0;
    });
  }

  /**
   * Get nested value from record (copied from parent class)
   */
  getNestedValue(record, field) {
    const parts = field.split('.');
    let value = record;
    
    for (const part of parts) {
      if (value && typeof value === 'object' && part in value) {
        value = value[part];
      } else {
        return undefined;
      }
    }
    
    return value;
  }

  /**
   * Compatibility method: insertMany() -> multiple insert() calls
   */
  async insertMany(records) {
    const results = [];
    for (const record of records) {
      const result = await this.insert(record);
      results.push(result);
    }
    return results;
  }

  /**
   * Override insert to add _updated field for compatibility
   */
  async insert(data) {
    const record = await super.insert(data);
    record._updated = record._created; // Set _updated to same as _created for new records
    return record;
  }

  /**
   * Compatibility method: count() -> find() with length
   */
  async count(criteria = {}) {
    const results = await this.find(criteria);
    return results.length;
  }

  /**
   * Compatibility method: getStats() -> stats getter
   */
  async getStats() {
    // Call the parent class getStats method to get actual file size
    return await super.getStats();
  }

  /**
   * Compatibility method: validateIntegrity() -> basic validation
   */
  async validateIntegrity() {
    // Call the parent class validateIntegrity method to get actual file integrity check
    return await super.validateIntegrity();
  }

  /**
   * Compatibility method: walk() -> find() with async iteration
   */
  async *walk(options = {}) {
    const results = await this.find({}, options);
    for (const record of results) {
      yield record;
    }
  }

  /**
   * Compatibility property: indexStats
   */
  get indexStats() {
    const stats = this.stats;
    return {
      recordCount: stats.recordCount,
      indexCount: stats.indexedFields.length
    };
  }

  /**
   * Override update to return array format for compatibility
   */
  async update(criteria, updates) {
    const result = await super.update(criteria, updates);
    // Convert { updatedCount: n } to array format for tests
    if (typeof result === 'object' && result.updatedCount !== undefined) {
      const updatedRecords = await this.find(criteria);
      return updatedRecords;
    }
    return result;
  }

  /**
   * Override delete to return number format for compatibility
   */
  async delete(criteria) {
    const result = await super.delete(criteria);
    // Convert { deletedCount: n } to number format for tests
    if (typeof result === 'object' && result.deletedCount !== undefined) {
      return result.deletedCount;
    }
    return result;
  }

  /**
   * Compatibility method: readColumnIndex - gets unique values from indexed columns only
   * Maintains compatibility with JexiDB v1 code
   * @param {string} column - The column name to get unique values from
   * @returns {Set} Set of unique values in the column (indexed columns only)
   */
  readColumnIndex(column) {
    return super.readColumnIndex(column);
  }
}

/**
 * JexiDB - Robust JSONL database
 * Complete rewrite of JexiDB with JSONL architecture, fixing all critical bugs from version 1.x
 * 
 * Features:
 * - One file per table (pure JSONL)
 * - Punctual reading (doesn't load everything in memory)
 * - In-memory indexes for performance
 * - Safe truncation after operations
 * - Integrity validation
 * - Event-driven architecture
 * 
 * API similar to JexiDB 1.x:
 * - new JexiDB('file.jsonl', { indexes: { id: 'number' } })
 * - await db.init()
 * - await db.insert({ id: 1, name: 'John' })
 * - await db.find({ id: 1 })
 * - await db.update({ id: 1 }, { name: 'John Smith' })
 * - await db.delete({ id: 1 })
 * - await db.save()
 * - await db.destroy()
 * - await db.walk() - Iterator for large volumes
 * - await db.validateIntegrity() - Manual verification
 */

// Export the compatibility wrapper as default
export default JexiDBCompatibility;

// Export auxiliary classes for advanced use
export { FileHandler, IndexManager, IntegrityChecker };

// Export useful constants
export const OPERATORS = {
  GT: '>',
  GTE: '>=',
  LT: '<',
  LTE: '<=',
  NE: '!=',
  IN: 'in',
  NIN: 'nin',
  REGEX: 'regex',
  CONTAINS: 'contains'
};

// Export utility functions
export const utils = {
  /**
   * Creates a database with default settings
   */
  createDatabase(filePath, indexes = {}) {
    return new JSONLDatabase(filePath, { indexes });
  },

  /**
   * Validates if a JSONL file is valid
   * @param {string} filePath - Path to the JSONL file
   * @returns {Promise<Object>} Validation result with errors and line count
   */
  async validateJSONLFile(filePath) {
    const { promises: fs } = await import('fs');
    const readline = await import('readline');
    const { createReadStream } = await import('fs');

    try {
      const fileStream = createReadStream(filePath);
      const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
      });

      let lineNumber = 0;
      const errors = [];

      for await (const line of rl) {
        lineNumber++;
        if (line.trim() !== '') {
          try {
            JSON.parse(line);
          } catch (error) {
            errors.push(`Line ${lineNumber}: ${error.message}`);
          }
        }
      }

      return {
        isValid: errors.length === 0,
        errors,
        lineCount: lineNumber
      };
    } catch (error) {
      return {
        isValid: false,
        errors: [`Error reading file: ${error.message}`],
        lineCount: 0
      };
    }
  },

  /**
   * Converts a JSON file to JSONL (basic conversion)
   * @param {string} jsonFilePath - Path to the JSON file
   * @param {string} jsonlFilePath - Path to the output JSONL file
   * @returns {Promise<Object>} Conversion result
   */
  async convertJSONToJSONL(jsonFilePath, jsonlFilePath) {
    const { promises: fs } = await import('fs');
    
    try {
      const jsonData = await fs.readFile(jsonFilePath, 'utf8');
      const data = JSON.parse(jsonData);
      
      const records = Array.isArray(data) ? data : [data];
      const jsonlContent = records.map(record => JSON.stringify(record)).join('\n') + '\n';
      
      await fs.writeFile(jsonlFilePath, jsonlContent, 'utf8');
      
      return {
        success: true,
        recordCount: records.length
      };
    } catch (error) {
      return {
        success: false,
        error: error.message
      };
    }
  },

  /**
   * Converts a JSONL file to JSON
   * @param {string} jsonlFilePath - Path to the JSONL file
   * @param {string} jsonFilePath - Path to the output JSON file
   * @returns {Promise<Object>} Conversion result
   */
  async convertJSONLToJSON(jsonlFilePath, jsonFilePath) {
    const { promises: fs } = await import('fs');
    const readline = await import('readline');
    const { createReadStream } = await import('fs');

    try {
      const fileStream = createReadStream(jsonlFilePath);
      const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
      });

      const records = [];

      for await (const line of rl) {
        if (line.trim() !== '') {
          try {
            const record = JSON.parse(line);
            records.push(record);
          } catch (error) {
            console.warn(`Line ignored: ${error.message}`);
          }
        }
      }

      const jsonContent = JSON.stringify(records, null, 2);
      await fs.writeFile(jsonFilePath, jsonContent, 'utf8');

      return {
        success: true,
        recordCount: records.length
      };
    } catch (error) {
      return {
        success: false,
        error: error.message
      };
    }
  },

  /**
   * Creates a JexiDB database from a JSON file with automatic index detection
   * @param {string} jsonFilePath - Path to the JSON file
   * @param {string} dbFilePath - Path to the output JexiDB file
   * @param {Object} options - Options for database creation
   * @param {Object} options.indexes - Manual index configuration
   * @param {boolean} options.autoDetectIndexes - Auto-detect common index fields (default: true)
   * @param {Array<string>} options.autoIndexFields - Fields to auto-index (default: ['id', '_id', 'email', 'name'])
   * @returns {Promise<Object>} Database creation result
   */
  async createDatabaseFromJSON(jsonFilePath, dbFilePath, options = {}) {
    const { promises: fs } = await import('fs');
    
    try {
      // Read JSON data
      const jsonData = await fs.readFile(jsonFilePath, 'utf8');
      const data = JSON.parse(jsonData);
      const records = Array.isArray(data) ? data : [data];
      
      if (records.length === 0) {
        return {
          success: false,
          error: 'No records found in JSON file'
        };
      }

      // Auto-detect indexes if enabled
      let indexes = options.indexes || {};
      
      if (options.autoDetectIndexes !== false) {
        const autoIndexFields = options.autoIndexFields || ['id', '_id', 'email', 'name', 'username'];
        const sampleRecord = records[0];
        
        for (const field of autoIndexFields) {
          if (sampleRecord.hasOwnProperty(field)) {
            const value = sampleRecord[field];
            if (typeof value === 'number') {
              indexes[field] = 'number';
            } else if (typeof value === 'string') {
              indexes[field] = 'string';
            }
          }
        }
      }

      // Create database
      const db = new JSONLDatabase(dbFilePath, { 
        indexes,
        autoSave: false,
        validateOnInit: false
      });

      await db.init();

      // Insert all records
      for (const record of records) {
        await db.insert(record);
      }

      // Save and close the database
      await db.save();
      await db.close();

      return {
        success: true,
        recordCount: records.length,
        indexes: Object.keys(indexes),
        dbPath: dbFilePath
      };
    } catch (error) {
      return {
        success: false,
        error: error.message
      };
    }
  },

  /**
   * Analyzes a JSON file and suggests optimal indexes
   * @param {string} jsonFilePath - Path to the JSON file
   * @param {number} sampleSize - Number of records to analyze (default: 100)
   * @returns {Promise<Object>} Index suggestions
   */
  async analyzeJSONForIndexes(jsonFilePath, sampleSize = 100) {
    const { promises: fs } = await import('fs');
    
    try {
      const jsonData = await fs.readFile(jsonFilePath, 'utf8');
      const data = JSON.parse(jsonData);
      const records = Array.isArray(data) ? data : [data];
      
      if (records.length === 0) {
        return {
          success: false,
          error: 'No records found in JSON file'
        };
      }

      // Analyze sample records
      const sample = records.slice(0, Math.min(sampleSize, records.length));
      const fieldAnalysis = {};
      
      // First, collect all possible fields from all records
      const allFields = new Set();
      for (const record of sample) {
        for (const field of Object.keys(record)) {
          allFields.add(field);
        }
      }
      
      // Initialize analysis for all fields
      for (const field of allFields) {
        fieldAnalysis[field] = {
          type: 'unknown',
          uniqueValues: new Set(),
          nullCount: 0,
          totalCount: 0
        };
      }
      
      // Analyze each field across all records
      for (const record of sample) {
        for (const field of allFields) {
          const value = record[field];
          fieldAnalysis[field].totalCount++;
          
          if (value === null || value === undefined) {
            fieldAnalysis[field].nullCount++;
          } else {
            if (fieldAnalysis[field].type === 'unknown') {
              fieldAnalysis[field].type = typeof value;
            }
            fieldAnalysis[field].uniqueValues.add(value);
          }
        }
      }

      // Generate suggestions
      const suggestions = {
        recommended: [],
        optional: [],
        notRecommended: []
      };

      for (const [field, analysis] of Object.entries(fieldAnalysis)) {
        const coverage = (analysis.totalCount - analysis.nullCount) / analysis.totalCount;
        const uniqueness = analysis.uniqueValues.size / analysis.totalCount;
        
        const suggestion = {
          field,
          type: analysis.type,
          coverage: Math.round(coverage * 100),
          uniqueness: Math.round(uniqueness * 100),
          uniqueValues: analysis.uniqueValues.size
        };

        // Recommendation logic
        if (coverage > 0.9 && uniqueness > 0.8) {
          suggestions.recommended.push(suggestion);
        } else if (coverage > 0.7 && uniqueness > 0.5) {
          suggestions.optional.push(suggestion);
        } else {
          suggestions.notRecommended.push(suggestion);
        }
      }

      // Convert suggestions to the expected format for tests
      const suggestedIndexes = {};
      for (const suggestion of suggestions.recommended) {
        suggestedIndexes[suggestion.field] = suggestion.type;
      }

      return {
        success: true,
        totalRecords: records.length,
        analyzedRecords: sample.length,
        suggestedIndexes,
        suggestions
      };
    } catch (error) {
      return {
        success: false,
        error: error.message
      };
    }
  },


}; 