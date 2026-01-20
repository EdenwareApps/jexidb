import fs from 'fs'
import path from 'path'
import readline from 'readline'
import pLimit from 'p-limit'

export default class FileHandler {
  constructor(file, fileMutex = null, opts = {}) {
    this.file = file
    this.indexFile = file ? file.replace(/\.jdb$/, '.idx.jdb') : null
    this.fileMutex = fileMutex
    this.opts = opts
    this.maxBufferSize = opts.maxBufferSize || 4 * 1024 * 1024 // 4MB default
    // Global I/O limiter to prevent file descriptor exhaustion in concurrent operations
    this.readLimiter = pLimit(opts.maxConcurrentReads || 4)
  }

  async truncate(offset) {
    try {
      await fs.promises.access(this.file, fs.constants.F_OK)
      await fs.promises.truncate(this.file, offset)
    } catch (err) {
      await fs.promises.writeFile(this.file, '')
    }
  }

  async writeOffsets(data) {
    // Write offsets to the index file (will be combined with index data)
    await fs.promises.writeFile(this.indexFile, data)
  }

  async readOffsets() {
    try {
      return await fs.promises.readFile(this.indexFile)
    } catch (err) {
      return null
    }
  }

  async writeIndex(data) {
    // Write index data to the index file (will be combined with offsets)
    // Use Windows-specific retry logic for file operations
    await this._writeFileWithRetry(this.indexFile, data)
  }

  async readIndex() {
    try {
      return await fs.promises.readFile(this.indexFile)
    } catch (err) {
      return null
    }
  }

  async exists() {
    try {
      await fs.promises.access(this.file, fs.constants.F_OK)
      return true
    } catch (err) {
      return false
    }
  }


  async indexExists() {
    try {
      await fs.promises.access(this.indexFile, fs.constants.F_OK)
      return true
    } catch (err) {
      return false
    }
  }

  async isLegacyFormat() {
    if (!await this.exists()) return false
    if (await this.indexExists()) return false
    
    // Check if main file contains offsets at the end (legacy format)
    try {
      const lastLine = await this.readLastLine()
      if (!lastLine || !lastLine.length) return false
      
      // Try to parse as offsets array
      const content = lastLine.toString('utf-8').trim()
      const parsed = JSON.parse(content)
      return Array.isArray(parsed)
    } catch (err) {
      return false
    }
  }

  async migrateLegacyFormat(serializer) {
    if (!await this.isLegacyFormat()) return false

    console.log('Migrating from legacy format to new 3-file format...')
    
    // Read the legacy file
    const lastLine = await this.readLastLine()
    const offsets = JSON.parse(lastLine.toString('utf-8').trim())
    
    // Get index offset and truncate offsets array
    const indexOffset = offsets[offsets.length - 2]
    const dataOffsets = offsets.slice(0, -2)
    
    // Read index data
    const indexStart = indexOffset
    const indexEnd = offsets[offsets.length - 1]
    const indexBuffer = await this.readRange(indexStart, indexEnd)
    const indexData = await serializer.deserialize(indexBuffer)
    
    // Write offsets to separate file
    const offsetsString = await serializer.serialize(dataOffsets, { linebreak: false })
    await this.writeOffsets(offsetsString)
    
    // Write index to separate file
    const indexString = await serializer.serialize(indexData, { linebreak: false })
    await this.writeIndex(indexString)
    
    // Truncate main file to remove index and offsets
    await this.truncate(indexOffset)
    
    console.log('Migration completed successfully!')
    return true
  }

  async readRange(start, end) {
    // Check if file exists before trying to read it
    if (!await this.exists()) {
      return Buffer.alloc(0) // Return empty buffer if file doesn't exist
    }
    
    let fd = await fs.promises.open(this.file, 'r')
    try {
      // CRITICAL FIX: Check file size before attempting to read
      const stats = await fd.stat()
      const fileSize = stats.size
      
      // If start position is beyond file size, return empty buffer
      if (start >= fileSize) {
        await fd.close()
        return Buffer.alloc(0)
      }
      
      // Adjust end position if it's beyond file size
      const actualEnd = Math.min(end, fileSize)
      const length = actualEnd - start
      
      // If length is 0 or negative, return empty buffer
      if (length <= 0) {
        await fd.close()
        return Buffer.alloc(0)
      }
      
      let buffer = Buffer.alloc(length)
      const { bytesRead } = await fd.read(buffer, 0, length, start)
      await fd.close()
      
      // CRITICAL FIX: Ensure we read the expected amount of data
      if (bytesRead !== length) {
        const errorMsg = `CRITICAL: Expected to read ${length} bytes, but read ${bytesRead} bytes at position ${start}`
        console.error(`⚠️ ${errorMsg}`)
        
        // This indicates a race condition or file corruption
        // Don't retry - the caller should handle synchronization properly
        if (bytesRead === 0) {
          throw new Error(`File corruption detected: ${errorMsg}`)
        }
        
        // Return partial data with warning - caller should handle this
        return buffer.subarray(0, bytesRead)
      }
      
      return buffer
    } catch (error) {
      await fd.close().catch(() => {})
      throw error
    }
  }
  
  async readRanges(ranges, mapper) {
    const lines = {}
    
    // Check if file exists before trying to read it
    if (!await this.exists()) {
      return lines // Return empty object if file doesn't exist
    }
    
    const fd = await fs.promises.open(this.file, 'r')
    const groupedRanges = await this.groupedRanges(ranges)
    try {
      await Promise.allSettled(groupedRanges.map(async (groupedRange) => {
        await this.readLimiter(async () => {
          for await (const row of this.readGroupedRange(groupedRange, fd)) {
            lines[row.start] = mapper ? (await mapper(row.line, { start: row.start, end: row.start + row.line.length })) : row.line
          }
        })
      }))
    } catch (e) {
      console.error('Error reading ranges:', e)
    } finally {
      await fd.close()
    }
    return lines
  }

  async groupedRanges(ranges) { // expects ordered ranges from Database.getRanges()
    const readSize = 512 * 1024 // 512KB  
    const groupedRanges = []
    let currentGroup = []
    let currentSize = 0

    // each range is a {start: number, end: number} object
    for (let i = 0; i < ranges.length; i++) {
      const range = ranges[i]
      const rangeSize = range.end - range.start
      
      if (currentGroup.length > 0) {
        const lastRange = currentGroup[currentGroup.length - 1]
        if (lastRange.end !== range.start || currentSize + rangeSize > readSize) {
          groupedRanges.push(currentGroup)
          currentGroup = []
          currentSize = 0
        }
      }
    
      currentGroup.push(range)
      currentSize += rangeSize
    }

    if (currentGroup.length > 0) {
      groupedRanges.push(currentGroup)
    }

    return groupedRanges
  }

  /**
   * Ensure a line is complete by reading until newline if JSON appears truncated
   * @param {string} line - The potentially incomplete line
   * @param {number} fd - File descriptor
   * @param {number} currentOffset - Current read offset
   * @returns {string} Complete line
   */
  async ensureCompleteLine(line, fd, currentOffset) {
    // Fast check: if line already ends with newline, it's likely complete
    if (line.endsWith('\n')) {
      return line
    }

    // Check if the line contains valid JSON by trying to parse it
    const trimmedLine = line.trim()
    if (trimmedLine.length === 0) {
      return line
    }

    // Try to parse as JSON to see if it's complete
    try {
      JSON.parse(trimmedLine)
      // If parsing succeeds, the line is complete (but missing newline)
      // This is unusual but possible, return as-is
      return line
    } catch (jsonError) {
      // JSON is incomplete, try to read more until we find a newline
      const bufferSize = 2048 // Read in 2KB chunks for better performance
      const additionalBuffer = Buffer.allocUnsafe(bufferSize)
      let additionalOffset = currentOffset
      let additionalContent = line

      // Try reading up to 20KB more to find the newline (increased for safety)
      const maxAdditionalRead = 20480
      let totalAdditionalRead = 0

      while (totalAdditionalRead < maxAdditionalRead) {
        const { bytesRead } = await fd.read(additionalBuffer, 0, bufferSize, additionalOffset)

        if (bytesRead === 0) {
          // EOF reached, check if the accumulated content is now valid JSON
          const finalTrimmed = additionalContent.trim()
          try {
            JSON.parse(finalTrimmed)
            // If parsing succeeds now, return the content
            return additionalContent
          } catch {
            // Still invalid, return original line to avoid data loss
            return line
          }
        }

        const chunk = additionalBuffer.toString('utf8', 0, bytesRead)
        additionalContent += chunk
        totalAdditionalRead += bytesRead

        // Check if we found a newline in the entire accumulated content
        const newlineIndex = additionalContent.indexOf('\n', line.length)
        if (newlineIndex !== -1) {
          // Found newline, return content up to and including the newline
          const completeLine = additionalContent.substring(0, newlineIndex + 1)

          // Validate that the complete line contains valid JSON
          const trimmedComplete = completeLine.trim()
          try {
            JSON.parse(trimmedComplete)
            return completeLine
          } catch {
            // Even with newline, JSON is invalid - this suggests data corruption
            // Return original line to trigger normal error handling
            return line
          }
        }

        additionalOffset += bytesRead
      }

      // If we couldn't find a newline within the limit, return the original line
      // This prevents infinite reading and excessive memory usage
      return line
    }
  }

  /**
   * Split content into complete JSON lines, handling special characters and escaped quotes
   * CRITICAL FIX: Prevents "Expected ',' or ']'" and "Unterminated string" errors by ensuring
   * each line is a complete, valid JSON object/array, even when containing special characters
   * @param {string} content - Raw content containing multiple JSON lines
   * @returns {string[]} Array of complete JSON lines
   */
  splitJsonLines(content) {
    const lines = []
    let currentLine = ''
    let inString = false
    let escapeNext = false
    let braceCount = 0
    let bracketCount = 0

    for (let i = 0; i < content.length; i++) {
      const char = content[i]
      const prevChar = i > 0 ? content[i - 1] : null

      currentLine += char

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
        else if (char === '}') braceCount--
        else if (char === '[') bracketCount++
        else if (char === ']') bracketCount--
        else if (char === '\n' && braceCount === 0 && bracketCount === 0) {
          // Found complete JSON object/array at newline
          const trimmedLine = currentLine.trim()
          if (trimmedLine.length > 0) {
            lines.push(trimmedLine.replace(/\n$/, '')) // Remove trailing newline
          }
          currentLine = ''
          braceCount = 0
          bracketCount = 0
          inString = false
          escapeNext = false
        }
      }
    }

    // Add remaining content if it's a complete JSON object/array
    const trimmedLine = currentLine.trim()
    if (trimmedLine.length > 0 && braceCount === 0 && bracketCount === 0) {
      lines.push(trimmedLine)
    }

    return lines.filter(line => line.trim().length > 0)
  }

  async *readGroupedRange(groupedRange, fd) {
    if (groupedRange.length === 0) return

    // OPTIMIZATION: For single range, use direct approach
    if (groupedRange.length === 1) {
      const range = groupedRange[0]
      const bufferSize = range.end - range.start

      if (bufferSize <= 0 || bufferSize > this.maxBufferSize) {
        throw new Error(`Invalid buffer size: ${bufferSize}. Start: ${range.start}, End: ${range.end}. Max allowed: ${this.maxBufferSize}`)
      }

      const buffer = Buffer.allocUnsafe(bufferSize)
      const { bytesRead } = await fd.read(buffer, 0, bufferSize, range.start)
      const actualBuffer = bytesRead < bufferSize ? buffer.subarray(0, bytesRead) : buffer

      if (actualBuffer.length === 0) return

      let lineString
      try {
        lineString = actualBuffer.toString('utf8')
      } catch (error) {
        lineString = actualBuffer.toString('utf8', { replacement: '?' })
      }

      // CRITICAL FIX: For single ranges, check if JSON appears truncated and try to complete it
      // Only attempt completion if the line doesn't end with newline (indicating possible truncation)
      if (!lineString.endsWith('\n')) {
        const completeLine = await this.ensureCompleteLine(lineString, fd, range.start + actualBuffer.length)
        if (completeLine !== lineString) {
          lineString = completeLine.trimEnd()
        }
      } else {
        lineString = lineString.trimEnd()
      }

      yield {
        line: lineString,
        start: range.start,
        _: range.index !== undefined ? range.index : (range._ || null)
      }
      return
    }
    
    // OPTIMIZATION: For multiple ranges, read as single buffer and split by offsets
    const firstRange = groupedRange[0]
    const lastRange = groupedRange[groupedRange.length - 1]
    const totalSize = lastRange.end - firstRange.start
    
    if (totalSize <= 0 || totalSize > this.maxBufferSize) {
      throw new Error(`Invalid total buffer size: ${totalSize}. Start: ${firstRange.start}, End: ${lastRange.end}. Max allowed: ${this.maxBufferSize}`)
    }
    
    // Read entire grouped range as single buffer
    const buffer = Buffer.allocUnsafe(totalSize)
    const { bytesRead } = await fd.read(buffer, 0, totalSize, firstRange.start)
    const actualBuffer = bytesRead < totalSize ? buffer.subarray(0, bytesRead) : buffer
    
    if (actualBuffer.length === 0) return
    
    // Convert to string once
    let content
    try {
      content = actualBuffer.toString('utf8')
    } catch (error) {
      content = actualBuffer.toString('utf8', { replacement: '?' })
    }
    
    // CRITICAL FIX: Validate buffer completeness to prevent UTF-8 corruption
    // When reading non-adjacent ranges, the buffer may be incomplete (last line cut mid-character)
    const lastNewlineIndex = content.lastIndexOf('\n')
    if (lastNewlineIndex === -1 || lastNewlineIndex < content.length - 2) {
      // Buffer may be incomplete - truncate to last complete line
      if (this.opts.debugMode) {
        console.warn(`⚠️ Incomplete buffer detected at offset ${firstRange.start}, truncating to last complete line`)
      }
      if (lastNewlineIndex > 0) {
        content = content.substring(0, lastNewlineIndex + 1)
      } else {
        // No complete lines found - may be a serious issue
        if (this.opts.debugMode) {
          console.warn(`⚠️ No complete lines found in buffer at offset ${firstRange.start}`)
        }
      }
    }
    
    // CRITICAL FIX: Handle ranges more carefully to prevent corruption
    if (groupedRange.length === 2 && groupedRange[0].end === groupedRange[1].start) {
      // Special case: Adjacent ranges - split by COMPLETE JSON lines, not just newlines
      // This prevents corruption when lines contain special characters or unescaped quotes
      const lines = this.splitJsonLines(content)

      for (let i = 0; i < Math.min(lines.length, groupedRange.length); i++) {
        const range = groupedRange[i]
        yield {
          line: lines[i],
          start: range.start,
          _: range.index !== undefined ? range.index : (range._ || null)
        }
      }
    } else {
      // CRITICAL FIX: For non-adjacent ranges, use the range.end directly
      // because range.end already excludes the newline (calculated as offsets[n+1] - 1)
      // We just need to find the line start (beginning of the line in the buffer)
      for (let i = 0; i < groupedRange.length; i++) {
        const range = groupedRange[i]
        const relativeStart = range.start - firstRange.start
        const relativeEnd = range.end - firstRange.start
        
        // OPTIMIZATION 2: Find line start only if necessary
        // Check if we're already at a line boundary to avoid unnecessary backwards search
        let lineStart = relativeStart
        if (relativeStart > 0 && content[relativeStart - 1] !== '\n') {
          // Only search backwards if we're not already at a line boundary
          while (lineStart > 0 && content[lineStart - 1] !== '\n') {
            lineStart--
          }
        }
        
        // OPTIMIZATION 3: Use slice() instead of substring() for better performance
        // CRITICAL FIX: range.end = offsets[n+1] - 1 points to the newline character
        // slice(start, end) includes characters from start to end-1 (end is exclusive)
        // So if relativeEnd points to the newline, slice will include it
        let rangeContent = content.slice(lineStart, relativeEnd)
        
        // OPTIMIZATION 4: Direct character check instead of regex/trimEnd
        // Remove trailing newlines and whitespace efficiently
        // CRITICAL FIX: Prevents incomplete JSON line reading that caused "Expected ',' or ']'" parsing errors
        // trimEnd() is actually optimized in V8, but we can check if there's anything to trim first
        const len = rangeContent.length
        if (len > 0) {
          // Quick check: if last char is not whitespace, skip trimEnd
          const lastChar = rangeContent[len - 1]
          if (lastChar === '\n' || lastChar === '\r' || lastChar === ' ' || lastChar === '\t') {
            // Only call trimEnd if we detected trailing whitespace
            rangeContent = rangeContent.trimEnd()
          }
        }
        
        if (rangeContent.length === 0) continue

        // CRITICAL FIX: For multiple ranges, we cannot safely expand reading
        // because offsets are pre-calculated. Instead, validate JSON and let
        // the deserializer handle incomplete lines (which will trigger recovery)
        const trimmedContent = rangeContent.trim()
        let finalContent = rangeContent

        if (trimmedContent.length > 0) {
          try {
            JSON.parse(trimmedContent)
            // JSON is valid, use as-is
          } catch (jsonError) {
            // JSON appears incomplete - this is expected for truncated ranges
            // Let the deserializer handle it (will trigger streaming recovery if needed)
            // We don't try to expand reading here because offsets are pre-calculated
          }
        }

        yield {
          line: finalContent,
          start: range.start,
          _: range.index !== undefined ? range.index : (range._ || null)
        }
      }
    }
  }

  async *walk(ranges) {
    // CRITICAL FIX: Acquire file mutex to prevent race conditions with concurrent writes
    const release = this.fileMutex ? await this.fileMutex.acquire() : () => {}
    try {
      // Check if file exists before trying to read it
      if (!await this.exists()) {
        return // Return empty generator if file doesn't exist
      }
      
      const fd = await fs.promises.open(this.file, 'r')
      try {
        const groupedRanges = await this.groupedRanges(ranges)
        for(const groupedRange of groupedRanges) {
          for await (const row of this.readGroupedRange(groupedRange, fd)) {
            yield row
          }
        }
      } finally {
        await fd.close()
      }
    } finally {
      release()
    }
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
        const { size } = await reader.stat();
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
      if (reader) await reader.close().catch(() => { });
      if (writer) await writer.close().catch(() => { });
      await fs.promises.unlink(tmpFile).catch(() => { });
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
          if (this.opts.debugMode) {
            console.warn(`⚠️ Invalid JSON in temp file at line ${i + 1}, skipping:`, lines[i].substring(0, 100));
          }
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
    await fd.write(data)
  }

  async writeDataAsync(data) {
    // CRITICAL FIX: Ensure directory exists before writing
    const dir = path.dirname(this.file)
    await fs.promises.mkdir(dir, { recursive: true })
    
    await fs.promises.appendFile(this.file, data)
  }

  /**
   * Check if data appears to be binary (always false since we only use JSON now)
   */
  isBinaryData(data) {
    // All data is now JSON format
    return false
  }

  /**
   * Check if file is binary (always false since we only use JSON now)
   */
  async isBinaryFile() {
    // All files are now JSON format
    return false
  }

  async readLastLine() {
    // Use global read limiter to prevent file descriptor exhaustion
    return this.readLimiter(async () => {
      // Check if file exists before trying to read it
      if (!await this.exists()) {
        return null // Return null if file doesn't exist
      }
      
      const reader = await fs.promises.open(this.file, 'r')
      try {
        const { size } = await reader.stat()
        if (size < 1) throw 'empty file'
        this.size = size
        const bufferSize = 16384
        let buffer, isFirstRead = true, lastReadSize, readPosition = Math.max(size - bufferSize, 0)
        while (readPosition >= 0) {
          const readSize = Math.min(bufferSize, size - readPosition)
          if (readSize !== lastReadSize) {
            lastReadSize = readSize
            buffer = Buffer.alloc(readSize)
          }
          const { bytesRead } = await reader.read(buffer, 0, isFirstRead ? (readSize - 1) : readSize, readPosition)
          if (isFirstRead) isFirstRead = false
          if (bytesRead === 0) break
          const newlineIndex = buffer.lastIndexOf(10)
          const start = readPosition + newlineIndex + 1
          if (newlineIndex !== -1) {
            const lastLine = Buffer.alloc(size - start)
            await reader.read(lastLine, 0, size - start, start)
            if (!lastLine || !lastLine.length) {
              throw 'no metadata or empty file'
            }
            return lastLine
          } else {
            readPosition -= bufferSize
          }
        }
      } catch (e) {
        String(e).includes('empty file') || console.error('Error reading last line:', e)
      } finally {
        reader.close()
      }
    })
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
    const { limit, skip = 0 } = options; // No default limit
    const results = [];
    let lineNumber = 0;
    let processed = 0;
    let skipped = 0;
    let matched = 0;

    try {
      // Check if file exists before trying to read it
      if (!await this.exists()) {
        return results; // Return empty results if file doesn't exist
      }
      
      // All files are now JSONL format - use line-by-line reading
      // Create optimized read stream
      const stream = fs.createReadStream(this.file, {
        highWaterMark: 64 * 1024, // 64KB chunks
        encoding: 'utf8'
      });

      // Create readline interface
      const rl = readline.createInterface({
        input: stream,
        crlfDelay: Infinity // Better performance
      });

      // Process line by line
      for await (const line of rl) {
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
              results.push({ ...record, _: lineNumber });
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
    const { limit } = options;
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

      for await (const line of rl) {
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

      for await (const line of rl) {
        lineCount++;
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
        await this.fd.close().catch(() => {})
        this.fd = null
      }
      
      // Close any open readers/writers
      if (this.reader) {
        await this.reader.close().catch(() => {})
        this.reader = null
      }
      
      if (this.writer) {
        await this.writer.close().catch(() => {})
        this.writer = null
      }
      
      // Clear any cached file handles
      this.cachedFd = null
      
    } catch (error) {
      // Ignore errors during cleanup
    }
  }

  async delete() {
    try {
      // Delete main file
      await fs.promises.unlink(this.file).catch(() => {})
      
      // Delete index file (which now contains both index and offsets data)
      await fs.promises.unlink(this.indexFile).catch(() => {})
    } catch (error) {
      // Ignore errors if files don't exist
    }
  }

  async writeAll(data) {
    const release = this.fileMutex ? await this.fileMutex.acquire() : () => {}
    try {
      // Use Windows-specific retry logic for file operations
      await this._writeWithRetry(data)
    } finally {
      release()
    }
  }

  /**
   * Optimized batch write operation (OPTIMIZATION)
   * @param {Array} dataChunks - Array of data chunks to write
   * @param {boolean} append - Whether to append or overwrite
   */
  async writeBatch(dataChunks, append = false) {
    if (!dataChunks || !dataChunks.length) return
    
    const release = this.fileMutex ? await this.fileMutex.acquire() : () => {}
    try {
      // OPTIMIZATION: Use streaming write for better performance
      if (dataChunks.length === 1 && Buffer.isBuffer(dataChunks[0])) {
        // Single buffer - use direct write
        if (append) {
          await fs.promises.appendFile(this.file, dataChunks[0])
        } else {
          await this._writeFileWithRetry(this.file, dataChunks[0])
        }
      } else {
        // Multiple chunks - use streaming approach
        await this._writeBatchStreaming(dataChunks, append)
      }
    } finally {
      release()
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
    })
    
    return new Promise((resolve, reject) => {
      writeStream.on('error', reject)
      writeStream.on('finish', resolve)
      
      // Write chunks sequentially
      let index = 0
      const writeNext = () => {
        if (index >= dataChunks.length) {
          writeStream.end()
          return
        }
        
        const chunk = dataChunks[index++]
        const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk, 'utf8')
        
        if (!writeStream.write(buffer)) {
          writeStream.once('drain', writeNext)
        } else {
          writeNext()
        }
      }
      
      writeNext()
    })
  }

  /**
   * Optimized append operation for single data chunk (OPTIMIZATION)
   * @param {string|Buffer} data - Data to append
   */
  async appendOptimized(data) {
    const release = this.fileMutex ? await this.fileMutex.acquire() : () => {}
    try {
      // OPTIMIZATION: Direct append without retry logic for better performance
      await fs.promises.appendFile(this.file, data)
    } finally {
      release()
    }
  }

  /**
   * Windows-specific retry logic for fs.promises.writeFile operations
   * Based on node-graceful-fs workarounds for EPERM issues
   */
  async _writeFileWithRetry(filePath, data, maxRetries = 3) {
    const isWindows = process.platform === 'win32'
    
    for (let attempt = 0; attempt < maxRetries; attempt++) {
      try {
        // Ensure data is properly formatted as string or buffer
        if (Buffer.isBuffer(data)) {
          await fs.promises.writeFile(filePath, data)
        } else {
          await fs.promises.writeFile(filePath, data.toString())
        }
        
        // Windows: add small delay after write operation
        // This helps prevent EPERM issues caused by file handle not being released immediately
        if (isWindows) {
          await new Promise(resolve => setTimeout(resolve, 10))
        }
        
        // Success - return immediately
        return
        
      } catch (err) {
        // Only retry on EPERM errors on Windows
        if (err.code === 'EPERM' && isWindows && attempt < maxRetries - 1) {
          // Exponential backoff: 10ms, 50ms, 250ms
          const delay = Math.pow(10, attempt + 1)
          await new Promise(resolve => setTimeout(resolve, delay))
          continue
        }
        
        // Re-throw if not a retryable error or max retries reached
        throw err
      }
    }
  }

  /**
   * Windows-specific retry logic for file operations
   * Based on node-graceful-fs workarounds for EPERM issues
   */
  async _writeWithRetry(data, maxRetries = 3) {
    const isWindows = process.platform === 'win32'
    
    for (let attempt = 0; attempt < maxRetries; attempt++) {
      try {
        // CRITICAL FIX: Ensure directory exists before writing file
        const dir = path.dirname(this.file)
        await fs.promises.mkdir(dir, { recursive: true })
        
        const fd = await fs.promises.open(this.file, 'w')
        try {
          // Ensure data is properly formatted as string or buffer
          if (Buffer.isBuffer(data)) {
            await fd.write(data)
          } else {
            await fd.write(data.toString())
          }
        } finally {
          await fd.close()
          
          // Windows: add small delay after closing file handle
          // This helps prevent EPERM issues caused by file handle not being released immediately
          if (isWindows) {
            await new Promise(resolve => setTimeout(resolve, 10))
          }
        }
        
        // Success - return immediately
        return
        
      } catch (err) {
        // Only retry on EPERM errors on Windows
        if (err.code === 'EPERM' && isWindows && attempt < maxRetries - 1) {
          // Exponential backoff: 10ms, 50ms, 250ms
          const delay = Math.pow(10, attempt + 1)
          await new Promise(resolve => setTimeout(resolve, delay))
          continue
        }
        
        // Re-throw if not a retryable error or max retries reached
        throw err
      }
    }
  }

  async readAll() {
    const release = this.fileMutex ? await this.fileMutex.acquire() : () => {}
    try {
      // Check if file exists before trying to read it
      if (!await this.exists()) {
        return '' // Return empty string if file doesn't exist
      }
      
      const fd = await fs.promises.open(this.file, 'r')
      try {
        const stats = await fd.stat()
        const buffer = Buffer.allocUnsafe(stats.size)
        await fd.read(buffer, 0, stats.size, 0)
        return buffer.toString('utf8')
      } finally {
        await fd.close()
      }
    } finally {
      release()
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
      return ''
    }

    const release = this.fileMutex ? await this.fileMutex.acquire() : () => {}
    try {
      // Check if file exists before trying to read it
      if (!await this.exists()) {
        return '' // Return empty string if file doesn't exist
      }
      
      const fd = await fs.promises.open(this.file, 'r')
      try {
        const stats = await fd.stat()
        const buffer = Buffer.allocUnsafe(stats.size)
        await fd.read(buffer, 0, stats.size, 0)
        
        // CRITICAL FIX: Ensure proper UTF-8 decoding for multi-byte characters
        let content
        try {
          content = buffer.toString('utf8')
        } catch (error) {
          // If UTF-8 decoding fails, try to recover by finding valid UTF-8 boundaries
          if (this.opts.debugMode) {
            console.warn(`UTF-8 decoding failed for file ${this.file}, attempting recovery`)
          }
          
          // Find the last complete UTF-8 character
          let validLength = buffer.length
          for (let i = buffer.length - 1; i >= 0; i--) {
            const byte = buffer[i]
            // CRITICAL FIX: Correct UTF-8 start character detection
            // Check if this is the start of a UTF-8 character (not a continuation byte)
            if ((byte & 0x80) === 0 || // ASCII (1 byte) - 0xxxxxxx
                (byte & 0xE0) === 0xC0 || // 2-byte UTF-8 start - 110xxxxx
                (byte & 0xF0) === 0xE0 || // 3-byte UTF-8 start - 1110xxxx
                (byte & 0xF8) === 0xF0) { // 4-byte UTF-8 start - 11110xxx
              validLength = i + 1
              break
            }
          }
          
          const validBuffer = buffer.subarray(0, validLength)
          content = validBuffer.toString('utf8')
        }
        
        // Split content into lines and extract only the requested lines
        const lines = content.split('\n')
        const result = []
        
        for (const lineNum of lineNumbers) {
          // Convert to 0-based index and check bounds
          const index = lineNum - 1
          if (index >= 0 && index < lines.length) {
            result.push(lines[index])
          }
        }
        
        return result.join('\n')
      } finally {
        await fd.close()
      }
    } finally {
      release()
    }
  }

}

