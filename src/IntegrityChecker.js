import { promises as fs } from 'fs';

/**
 * IntegrityChecker - JSONL file integrity validation
 * Checks consistency between data, indexes and offsets
 */
class IntegrityChecker {
  constructor(fileHandler, indexManager) {
    this.fileHandler = fileHandler;
    this.indexManager = indexManager;
  }

  /**
   * Validates the complete integrity of the database
   */
  async validateIntegrity(options = {}) {
    const {
      checkData = true,
      checkIndexes = true,
      checkOffsets = true,
      verbose = false
    } = options;

    const results = {
      isValid: true,
      errors: [],
      warnings: [],
      stats: {
        totalRecords: 0,
        validRecords: 0,
        corruptedRecords: 0,
        missingIndexes: 0,
        orphanedIndexes: 0
      }
    };

    if (verbose) {
      console.log('🔍 Starting integrity validation...');
    }

    // Check if file exists
    const fileExists = await this.fileHandler.exists();
    if (!fileExists) {
      results.errors.push('Data file does not exist');
      results.isValid = false;
      return results;
    }

    // Validate file data
    if (checkData) {
      const dataResults = await this.validateDataFile(verbose);
      results.errors.push(...dataResults.errors);
      results.warnings.push(...dataResults.warnings);
      results.stats = { ...results.stats, ...dataResults.stats };
    }

    // Validate indexes
    if (checkIndexes) {
      const indexResults = await this.validateIndexes(verbose);
      results.errors.push(...indexResults.errors);
      results.warnings.push(...indexResults.warnings);
      results.stats = { ...results.stats, ...indexResults.stats };
    }

    // Validate offsets
    if (checkOffsets) {
      const offsetResults = await this.validateOffsets(verbose);
      results.errors.push(...offsetResults.errors);
      results.warnings.push(...offsetResults.warnings);
    }

    // Determine if valid
    results.isValid = results.errors.length === 0;

    if (verbose) {
      console.log(`✅ Validation completed: ${results.isValid ? 'VALID' : 'INVALID'}`);
      console.log(`📊 Statistics:`, results.stats);
      if (results.errors.length > 0) {
        console.log(`❌ Errors found:`, results.errors);
      }
      if (results.warnings.length > 0) {
        console.log(`⚠️ Warnings:`, results.warnings);
      }
    }

    return results;
  }

  /**
   * Validates the JSONL data file
   */
  async validateDataFile(verbose = false) {
    const results = {
      errors: [],
      warnings: [],
      stats: {
        totalRecords: 0,
        validRecords: 0,
        corruptedRecords: 0
      }
    };

    try {
      const fd = await fs.open(this.fileHandler.filePath, 'r');
      let lineNumber = 0;
      let offset = 0;
      const buffer = Buffer.alloc(8192);
      let lineBuffer = '';

      try {
        while (true) {
          const { bytesRead } = await fd.read(buffer, 0, buffer.length, offset);
          if (bytesRead === 0) break;

          const chunk = buffer.toString('utf8', 0, bytesRead);
          lineBuffer += chunk;

          // Process complete lines
          let newlineIndex;
          while ((newlineIndex = lineBuffer.indexOf('\n')) !== -1) {
            const line = lineBuffer.substring(0, newlineIndex);
            lineBuffer = lineBuffer.substring(newlineIndex + 1);

            results.stats.totalRecords++;

            if (line.trim() === '') {
              results.warnings.push(`Line ${lineNumber + 1}: Empty line`);
            } else {
              try {
                const record = JSON.parse(line);
                
                // Check if it's a deleted record
                if (record._deleted) {
                  if (verbose) {
                    console.log(`🗑️ Line ${lineNumber + 1}: Deleted record`);
                  }
                } else {
                  results.stats.validRecords++;
                  if (verbose) {
                    console.log(`✅ Line ${lineNumber + 1}: Valid record`);
                  }
                }
              } catch (error) {
                results.stats.corruptedRecords++;
                results.errors.push(`Line ${lineNumber + 1}: Invalid JSON - ${error.message}`);
                if (verbose) {
                  console.log(`❌ Line ${lineNumber + 1}: Corrupted JSON`);
                }
              }
            }

            lineNumber++;
          }

          offset += bytesRead;
        }

        // Process last line if it doesn't end with \n
        if (lineBuffer.trim() !== '') {
          results.warnings.push(`Line ${lineNumber + 1}: File doesn't end with newline`);
        }

      } finally {
        await fd.close();
      }

    } catch (error) {
      results.errors.push(`Error reading file: ${error.message}`);
    }

    return results;
  }

  /**
   * Validates index consistency
   */
  async validateIndexes(verbose = false) {
    const results = {
      errors: [],
      warnings: [],
      stats: {
        missingIndexes: 0,
        orphanedIndexes: 0
      }
    };

    const indexData = this.indexManager.serialize();
    const validOffsets = new Set();

    // Collect all valid offsets
    for (let i = 0; i < this.indexManager.offsets.length; i++) {
      if (this.indexManager.offsets[i] !== null) {
        validOffsets.add(i);
      }
    }

    // Check each index
    for (const [field, fieldIndexData] of Object.entries(indexData.indexes)) {
      if (verbose) {
        console.log(`🔍 Checking index: ${field}`);
      }

      for (const [value, offsetArray] of Object.entries(fieldIndexData.values)) {
        for (const offsetIndex of offsetArray) {
          if (!validOffsets.has(offsetIndex)) {
            results.stats.orphanedIndexes++;
            results.errors.push(`Orphaned index: ${field}=${value} points to non-existent offset ${offsetIndex}`);
          }
        }
      }
    }

    // Check if there are valid records without index
    for (const offsetIndex of validOffsets) {
      let hasIndex = false;
      for (const [field, index] of Object.entries(this.indexManager.indexes)) {
        for (const [value, offsetSet] of index.values.entries()) {
          if (offsetSet.has(offsetIndex)) {
            hasIndex = true;
            break;
          }
        }
        if (hasIndex) break;
      }

      if (!hasIndex) {
        results.stats.missingIndexes++;
        results.warnings.push(`Record at offset ${offsetIndex} is not indexed`);
      }
    }

    return results;
  }

  /**
   * Validates offset consistency
   */
  async validateOffsets(verbose = false) {
    const results = {
      errors: [],
      warnings: []
    };

    const stats = await this.fileHandler.getStats();
    const fileSize = stats.size;

    // Check if offsets are valid
    for (let i = 0; i < this.indexManager.offsets.length; i++) {
      const offset = this.indexManager.offsets[i];
      if (offset !== null) {
        if (offset < 0) {
          results.errors.push(`Offset ${i}: Negative value (${offset})`);
        } else if (offset >= fileSize) {
          results.errors.push(`Offset ${i}: Out of file bounds (${offset} >= ${fileSize})`);
        }
      }
    }

    return results;
  }

  /**
   * Rebuilds indexes from the data file
   */
  async rebuildIndexes(verbose = false) {
    if (verbose) {
      console.log('🔧 Rebuilding indexes...');
    }

    // Store the configured indexes before clearing
    const configuredIndexes = this.indexManager.indexes;
    
    // Clear current indexes but preserve configuration
    this.indexManager.clear();
    
    // Restore the configured indexes
    for (const [field, indexConfig] of Object.entries(configuredIndexes)) {
      this.indexManager.indexes[field] = {
        type: indexConfig.type,
        values: new Map()
      };
    }

    try {
      const fd = await fs.open(this.fileHandler.filePath, 'r');
      let lineNumber = 0;
      let offset = 0;
      const buffer = Buffer.alloc(8192);
      let lineBuffer = '';

      try {
        while (true) {
          const { bytesRead } = await fd.read(buffer, 0, buffer.length, offset);
          if (bytesRead === 0) break;

          const chunk = buffer.toString('utf8', 0, bytesRead);
          lineBuffer += chunk;

          // Process complete lines
          let newlineIndex;
          while ((newlineIndex = lineBuffer.indexOf('\n')) !== -1) {
            const line = lineBuffer.substring(0, newlineIndex);
            lineBuffer = lineBuffer.substring(newlineIndex + 1);

            if (line.trim() !== '') {
              try {
                const record = JSON.parse(line);
                
                // Only index non-deleted records
                if (!record._deleted) {
                  record._offset = offset;
                  this.indexManager.addRecord(record, lineNumber);
                  
                  if (verbose) {
                    console.log(`✅ Reindexed: line ${lineNumber + 1}`);
                  }
                }
              } catch (error) {
                if (verbose) {
                  console.log(`⚠️ Line ${lineNumber + 1}: Ignored (invalid JSON)`);
                }
              }
            }

            lineNumber++;
            offset += this.fileHandler.getByteLength(line + '\n');
          }
        }

      } finally {
        await fd.close();
      }

      // Save rebuilt indexes
      await this.fileHandler.writeIndex(this.indexManager.serialize());

      if (verbose) {
        console.log('✅ Indexes rebuilt successfully');
      }

      return true;

    } catch (error) {
      if (verbose) {
        console.error('❌ Error rebuilding indexes:', error.message);
      }
      throw error;
    }
  }

  /**
   * Exports detailed statistics
   */
  async exportStats() {
    const stats = await this.fileHandler.getStats();
    const indexStats = this.indexManager.getStats();
    const integrityResults = await this.validateIntegrity({ verbose: false });

    return {
      file: {
        path: this.fileHandler.filePath,
        size: stats.size,
        created: stats.created,
        modified: stats.modified
      },
      indexes: indexStats,
      integrity: integrityResults,
      summary: {
        totalRecords: indexStats.recordCount,
        fileSize: stats.size,
        isValid: integrityResults.isValid,
        errorCount: integrityResults.errors.length,
        warningCount: integrityResults.warnings.length
      }
    };
  }
}

export default IntegrityChecker; 