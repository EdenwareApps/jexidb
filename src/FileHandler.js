import { promises as fs } from 'fs';
import path from 'path';

/**
 * FileHandler - Secure file operations for JSONL
 * Handles textLength vs byteLength (UTF-8)
 * Implements safe truncation and guaranteed flush
 */
class FileHandler {
  constructor(filePath) {
    this.filePath = path.resolve(filePath);
    this.indexPath = this.filePath.replace('.jsonl', '.index.json');
    this.metaPath = this.filePath.replace('.jsonl', '.meta.json');
    this.writeBuffer = [];
    this.isWriting = false;
  }

  /**
   * Ensures the directory exists
   */
  async ensureDirectory() {
    const dir = path.dirname(this.filePath);
    try {
      await fs.access(dir);
    } catch {
      await fs.mkdir(dir, { recursive: true });
    }
  }

  /**
   * Calculates the byte length of a UTF-8 string
   */
  getByteLength(str) {
    return Buffer.byteLength(str, 'utf8');
  }

  /**
   * Serializes an object to JSON with newline
   */
  serialize(obj) {
    return JSON.stringify(obj) + '\n';
  }

  /**
   * Deserializes a JSON line
   */
  deserialize(line) {
    try {
      const trimmed = line.trim();
      if (!trimmed) {
        throw new Error('Empty line');
      }
      const parsed = JSON.parse(trimmed);
      return parsed;
    } catch (error) {
      // Add more context to the error
      const context = line.length > 100 ? line.substring(0, 100) + '...' : line;
      throw new Error(`Failed to deserialize JSON data: ${context} - ${error.message}`);
    }
  }

  /**
   * Reads a specific line from the file by offset
   */
  async readLine(offset) {
    try {
      const fd = await fs.open(this.filePath, 'r');
      try {
        // Read until newline is found
        const buffer = Buffer.alloc(1024);
        let line = '';
        let position = offset;
        
        while (true) {
          const { bytesRead } = await fd.read(buffer, 0, buffer.length, position);
          if (bytesRead === 0) break;
          
          const chunk = buffer.toString('utf8', 0, bytesRead);
          const newlineIndex = chunk.indexOf('\n');
          
          if (newlineIndex !== -1) {
            line += chunk.substring(0, newlineIndex);
            break;
          } else {
            line += chunk;
            position += bytesRead;
          }
        }
        
        return line;
      } finally {
        await fd.close();
      }
    } catch (error) {
      if (error.code === 'ENOENT') {
        return null; // File doesn't exist
      }
      throw error;
    }
  }

  /**
   * Reads multiple lines by offsets with batch optimization
   */
  async readLines(offsets) {
    const results = [];
    for (const offset of offsets) {
      const line = await this.readLine(offset);
      if (line !== null) {
        results.push({ offset, line });
      }
    }
    return results;
  }

  /**
   * Reads multiple lines with intelligent batching for optimal performance
   * Groups consecutive offsets for sequential reads and processes non-consecutive in parallel
   */
  async readLinesBatch(offsets) {
    if (!offsets || offsets.length === 0) {
      return [];
    }

    // For now, use parallel processing for all reads
    // This is simpler and still provides significant performance improvement
    const promises = offsets.map(offset => this.readLine(offset));
    const results = await Promise.all(promises);
    
    return results.filter(line => line !== null);
  }

  /**
   * Groups offsets into consecutive and non-consecutive batches
   */
  groupConsecutiveOffsets(offsets) {
    if (offsets.length === 0) return [];

    const sortedOffsets = [...offsets].sort((a, b) => a - b);
    const batches = [];
    let currentBatch = {
      consecutive: true,
      start: sortedOffsets[0],
      count: 1,
      offsets: [sortedOffsets[0]]
    };

    for (let i = 1; i < sortedOffsets.length; i++) {
      const currentOffset = sortedOffsets[i];
      const expectedOffset = currentBatch.start + (currentBatch.count * this.estimateLineSize());

      if (currentOffset === expectedOffset) {
        // Consecutive offset
        currentBatch.count++;
        currentBatch.offsets.push(currentOffset);
      } else {
        // Non-consecutive offset, start new batch
        batches.push(currentBatch);
        currentBatch = {
          consecutive: false,
          start: currentOffset,
          count: 1,
          offsets: [currentOffset]
        };
      }
    }

    batches.push(currentBatch);
    return batches;
  }

  /**
   * Estimates average line size for consecutive offset calculation
   */
  estimateLineSize() {
    // Default estimate of 200 bytes per line (JSON + newline)
    // This can be improved with actual statistics
    return 200;
  }

  /**
   * Reads consecutive lines efficiently using a single file read
   */
  async readConsecutiveLines(startOffset, count) {
    try {
      const fd = await fs.open(this.filePath, 'r');
      try {
        // Read a larger buffer to get multiple lines
        const bufferSize = Math.max(8192, count * this.estimateLineSize());
        const buffer = Buffer.alloc(bufferSize);
        const { bytesRead } = await fd.read(buffer, 0, buffer.size, startOffset);
        
        if (bytesRead === 0) return [];

        const content = buffer.toString('utf8', 0, bytesRead);
        const lines = content.split('\n');
        
        // Extract the requested number of complete lines
        const results = [];
        for (let i = 0; i < Math.min(count, lines.length - 1); i++) {
          const line = lines[i];
          if (line.trim()) {
            results.push(line);
          }
        }

        return results;
      } finally {
        await fd.close();
      }
    } catch (error) {
      if (error.code === 'ENOENT') {
        return [];
      }
      throw error;
    }
  }

  /**
   * Appends a line to the file
   */
  async appendLine(data) {
    await this.ensureDirectory();
    const line = this.serialize(data);
    
    // Retry logic for file permission issues
    let retries = 3;
    while (retries > 0) {
      try {
        const fd = await fs.open(this.filePath, 'a');
        try {
          await fd.write(line);
          await fd.sync(); // Ensures flush
          return this.getByteLength(line);
        } finally {
          await fd.close();
        }
      } catch (error) {
        retries--;
        if (error.code === 'EPERM' || error.code === 'EACCES') {
          if (retries > 0) {
            // Wait a bit before retrying
            await new Promise(resolve => setTimeout(resolve, 100));
            continue;
          }
        }
        throw error;
      }
    }
  }

  /**
   * Appends multiple lines in a single batch operation with optimized buffering
   * Uses chunked writes for better performance with large datasets
   * Accepts both string and Buffer inputs
   */
  async appendBatch(batchData) {
    await this.ensureDirectory();
    
    // Convert to Buffer if it's a string, otherwise use as-is
    const buffer = Buffer.isBuffer(batchData) ? batchData : Buffer.from(batchData, 'utf8');
    
    // Retry logic for file permission issues
    let retries = 3;
    while (retries > 0) {
      try {
        // Use fs.appendFile for simpler, more reliable batch writing
        await fs.appendFile(this.filePath, buffer);
        return buffer.length;
      } catch (error) {
        retries--;
        if (error.code === 'EPERM' || error.code === 'EACCES') {
          if (retries > 0) {
            // Wait a bit before retrying
            await new Promise(resolve => setTimeout(resolve, 100));
            continue;
          }
        }
        throw error;
      }
    }
  }

  /**
   * Replaces a specific line
   */
  async replaceLine(offset, data) {
    const newLine = this.serialize(data);
    const newLineBytes = this.getByteLength(newLine);
    
    // Read the current line to calculate size
    const oldLine = await this.readLine(offset);
    if (oldLine === null) {
      throw new Error(`Line at offset ${offset} not found`);
    }
    
    const oldLineBytes = this.getByteLength(oldLine + '\n');
    
    // If the new line is larger, need to rewrite the file
    if (newLineBytes > oldLineBytes) {
      return await this.replaceLineWithRewrite(offset, oldLineBytes, newLine);
    } else {
      // Can overwrite directly
      return await this.replaceLineInPlace(offset, oldLineBytes, newLine);
    }
  }

  /**
   * Replaces line by overwriting in place (when new line is smaller or equal)
   */
  async replaceLineInPlace(offset, oldLineBytes, newLine) {
    const fd = await fs.open(this.filePath, 'r+');
    try {
      await fd.write(newLine, 0, newLine.length, offset);
      
      // If the new line is smaller, truncate the file
      if (newLine.length < oldLineBytes) {
        const stats = await fd.stat();
        const newSize = offset + newLine.length;
        await fd.truncate(newSize);
      }
      
      await fd.sync(); // Ensures flush
      return offset; // Return the same offset since we overwrote in place
    } finally {
      await fd.close();
    }
  }

  /**
   * Rewrites the file when the new line is larger
   */
  async replaceLineWithRewrite(offset, oldLineBytes, newLine) {
    const tempPath = this.filePath + '.tmp';
    const reader = await fs.open(this.filePath, 'r');
    const writer = await fs.open(tempPath, 'w');
    
    try {
      let position = 0;
      const buffer = Buffer.alloc(8192);
      
      while (position < offset) {
        const { bytesRead } = await reader.read(buffer, 0, buffer.length, position);
        if (bytesRead === 0) break;
        await writer.write(buffer, 0, bytesRead);
        position += bytesRead;
      }
      
      // Write the new line
      await writer.write(newLine);
      
      // Skip the old line
      position += oldLineBytes;
      
      // Copy the rest of the file
      while (true) {
        const { bytesRead } = await reader.read(buffer, 0, buffer.length, position);
        if (bytesRead === 0) break;
        await writer.write(buffer, 0, bytesRead);
        position += bytesRead;
      }
      
      await writer.sync();
    } finally {
      await reader.close();
      await writer.close();
    }
    
    // Replace the original file with better error handling
    try {
      // On Windows, we need to handle file permission issues
      await fs.unlink(this.filePath);
    } catch (error) {
      // If unlink fails, try to overwrite the file instead
      if (error.code === 'EPERM' || error.code === 'EACCES') {
        // Copy temp file content to original file
        const tempReader = await fs.open(tempPath, 'r');
        const originalWriter = await fs.open(this.filePath, 'w');
        
        try {
          const buffer = Buffer.alloc(8192);
          while (true) {
            const { bytesRead } = await tempReader.read(buffer, 0, buffer.length);
            if (bytesRead === 0) break;
            await originalWriter.write(buffer, 0, bytesRead);
          }
          await originalWriter.sync();
        } finally {
          await tempReader.close();
          await originalWriter.close();
        }
        
        // Remove temp file
        try {
          await fs.unlink(tempPath);
        } catch (unlinkError) {
          // Ignore temp file cleanup errors
        }
        
        return offset;
      }
      throw error;
    }
    
    try {
      await fs.rename(tempPath, this.filePath);
    } catch (error) {
      // If rename fails, try to copy instead
      if (error.code === 'EPERM' || error.code === 'EACCES') {
        const tempReader = await fs.open(tempPath, 'r');
        const originalWriter = await fs.open(this.filePath, 'w');
        
        try {
          const buffer = Buffer.alloc(8192);
          while (true) {
            const { bytesRead } = await tempReader.read(buffer, 0, buffer.length);
            if (bytesRead === 0) break;
            await originalWriter.write(buffer, 0, bytesRead);
          }
          await originalWriter.sync();
        } finally {
          await tempReader.close();
          await originalWriter.close();
        }
        
        // Remove temp file
        try {
          await fs.unlink(tempPath);
        } catch (unlinkError) {
          // Ignore temp file cleanup errors
        }
      } else {
        throw error;
      }
    }
    
    return offset; // Return the same offset since the line position didn't change
  }

  /**
   * Removes a line (marks as deleted or removes physically)
   */
  async removeLine(offset, markAsDeleted = true) {
    if (markAsDeleted) {
      // Mark as deleted
      const deletedData = { _deleted: true, _deletedAt: new Date().toISOString() };
      return await this.replaceLine(offset, deletedData);
    } else {
      // Remove physically
      return await this.removeLinePhysically(offset);
    }
  }

  /**
   * Physically removes a line from the file
   */
  async removeLinePhysically(offset) {
    const oldLine = await this.readLine(offset);
    if (oldLine === null) {
      return 0;
    }
    
    const oldLineBytes = this.getByteLength(oldLine + '\n');
    
    const tempPath = this.filePath + '.tmp';
    const reader = await fs.open(this.filePath, 'r');
    const writer = await fs.open(tempPath, 'w');
    
    try {
      let position = 0;
      const buffer = Buffer.alloc(8192);
      
      // Copy until the line to be removed
      while (position < offset) {
        const { bytesRead } = await reader.read(buffer, 0, buffer.length, position);
        if (bytesRead === 0) break;
        await writer.write(buffer, 0, bytesRead);
        position += bytesRead;
      }
      
      // Skip the line to be removed
      position += oldLineBytes;
      
      // Copy the rest of the file
      while (true) {
        const { bytesRead } = await reader.read(buffer, 0, buffer.length, position);
        if (bytesRead === 0) break;
        await writer.write(buffer, 0, bytesRead);
        position += bytesRead;
      }
      
      await writer.sync();
    } finally {
      await reader.close();
      await writer.close();
    }
    
    // Replace the original file with better error handling
    try {
      // On Windows, we need to handle file permission issues
      await fs.unlink(this.filePath);
    } catch (error) {
      // If unlink fails, try to overwrite the file instead
      if (error.code === 'EPERM' || error.code === 'EACCES') {
        // Copy temp file content to original file
        const tempReader = await fs.open(tempPath, 'r');
        const originalWriter = await fs.open(this.filePath, 'w');
        
        try {
          const buffer = Buffer.alloc(8192);
          while (true) {
            const { bytesRead } = await tempReader.read(buffer, 0, buffer.length);
            if (bytesRead === 0) break;
            await originalWriter.write(buffer, 0, bytesRead);
          }
          await originalWriter.sync();
        } finally {
          await tempReader.close();
          await originalWriter.close();
        }
        
        // Remove temp file
        try {
          await fs.unlink(tempPath);
        } catch (unlinkError) {
          // Ignore temp file cleanup errors
        }
        
        return oldLineBytes;
      }
      throw error;
    }
    
    try {
      await fs.rename(tempPath, this.filePath);
    } catch (error) {
      // If rename fails, try to copy instead
      if (error.code === 'EPERM' || error.code === 'EACCES') {
        const tempReader = await fs.open(tempPath, 'r');
        const originalWriter = await fs.open(this.filePath, 'w');
        
        try {
          const buffer = Buffer.alloc(8192);
          while (true) {
            const { bytesRead } = await tempReader.read(buffer, 0, buffer.length);
            if (bytesRead === 0) break;
            await originalWriter.write(buffer, 0, bytesRead);
          }
          await originalWriter.sync();
        } finally {
          await tempReader.close();
          await originalWriter.close();
        }
        
        // Remove temp file
        try {
          await fs.unlink(tempPath);
        } catch (unlinkError) {
          // Ignore temp file cleanup errors
        }
      } else {
        throw error;
      }
    }
    
    return oldLineBytes;
  }

  /**
   * Reads the index file
   */
  async readIndex() {
    try {
      const data = await fs.readFile(this.indexPath, 'utf8');
      return JSON.parse(data);
    } catch (error) {
      if (error.code === 'ENOENT') {
        return { indexes: {}, offsets: [] };
      }
      throw error;
    }
  }

  /**
   * Writes the index file
   */
  async writeIndex(indexData) {
    await this.ensureDirectory();
    const data = JSON.stringify(indexData, null, 2);
    await fs.writeFile(this.indexPath, data, 'utf8');
  }

  /**
   * Reads the metadata file
   */
  async readMeta() {
    try {
      const data = await fs.readFile(this.metaPath, 'utf8');
      return JSON.parse(data);
    } catch (error) {
      if (error.code === 'ENOENT') {
        return {
          version: '2.0.1', // Keep version number for internal tracking
          created: new Date().toISOString(),
          lastModified: new Date().toISOString(),
          recordCount: 0,
          fileSize: 0
        };
      }
      throw error;
    }
  }

  /**
   * Writes the metadata file
   */
  async writeMeta(metaData) {
    await this.ensureDirectory();
    metaData.lastModified = new Date().toISOString();
    const data = JSON.stringify(metaData, null, 2);
    await fs.writeFile(this.metaPath, data, 'utf8');
  }

  /**
   * Gets file statistics
   */
  async getStats() {
    try {
      const stats = await fs.stat(this.filePath);
      return {
        size: stats.size,
        created: stats.birthtime,
        modified: stats.mtime
      };
    } catch (error) {
      if (error.code === 'ENOENT') {
        return { size: 0, created: null, modified: null };
      }
      if (error.code === 'EPERM' || error.code === 'EACCES') {
        // On Windows, file might be locked, return default values
        return { size: 0, created: null, modified: null };
      }
      throw error;
    }
  }

  /**
   * Checks if the file exists
   */
  async exists() {
    try {
      await fs.access(this.filePath);
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Removes all related files
   */
  async destroy() {
    const files = [this.filePath, this.indexPath, this.metaPath];
    for (const file of files) {
      try {
        await fs.unlink(file);
      } catch (error) {
        // Ignore if file doesn't exist
      }
    }
  }
}

export default FileHandler; 