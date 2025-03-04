import fs from 'fs'
import pLimit from 'p-limit'

export default class FileHandler {
  constructor(file) {
    this.file = file
  }

  async truncate(offset) {
    try {
      await fs.promises.access(this.file, fs.constants.F_OK)
      await fs.promises.truncate(this.file, offset)
    } catch (err) {
      await fs.promises.writeFile(this.file, '')
    }
  }

  async readRange(start, end) {
    let fd = await fs.promises.open(this.file, 'r')
    const length = end - start
    let buffer = Buffer.alloc(length)
    const { bytesRead } = await fd.read(buffer, 0, length, start).catch(console.error)    
    await fd.close()
    if(buffer.length > bytesRead) return buffer.subarray(0, bytesRead)
    return buffer
  }
  
  async readRanges(ranges, mapper) {
    const lines = {}, limit = pLimit(4)
    const fd = await fs.promises.open(this.file, 'r')
    const groupedRanges = await this.groupedRanges(ranges)
    try {
      await Promise.allSettled(groupedRanges.map(async (groupedRange) => {
        await limit(async () => {
          for await (const row of this.readGroupedRange(groupedRange, fd)) {
            lines[row.start] = mapper ? (await mapper(row.line, groupedRange)) : row.line
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
    for(const range of ranges) {
      const rangeSize = range.end - range.start
      
      if(currentGroup.length > 0) {
        const lastRange = currentGroup[currentGroup.length - 1]
        if(lastRange.end !== range.start || currentSize + rangeSize > readSize) {
          groupedRanges.push(currentGroup)
          currentGroup = []
          currentSize = 0
        }
      }
    
      currentGroup.push(range)
      currentSize += rangeSize
    }

    if(currentGroup.length > 0) {
      groupedRanges.push(currentGroup)
    }

    return groupedRanges
  }

  async *readGroupedRange(groupedRange, fd) {
    const options = {start: groupedRange[0].start, end: groupedRange[groupedRange.length - 1].end}
    
    let i = 0, buffer = Buffer.alloc(options.end - options.start)
    const results = {}, { bytesRead } = await fd.read(buffer, 0, options.end - options.start, options.start)
    if(buffer.length > bytesRead) buffer = buffer.subarray(0, bytesRead)

    for (const range of groupedRange) {
      const startOffset = range.start - options.start;
      let endOffset = range.end - options.start;
      if (endOffset > buffer.length) {
        endOffset = buffer.length;
      }
      if (startOffset >= buffer.length) {
        continue;
      }
      const line = buffer.subarray(startOffset, endOffset);
      if (line.length === 0) continue;
      yield { line, start: range.start };
    }


    return results
  }

  async *walk(ranges) {
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
  }

  async replaceLines(ranges, lines) {
    const tmpFile = this.file + '.tmp';
    const writer = await fs.promises.open(tmpFile, 'w+');
    const reader = await fs.promises.open(this.file, 'r');
    try {
      let position = 0;
      let lineIndex = 0;

      for (const range of ranges) {
        if (position < range.start) {
          const buffer = await this.readRange(position, range.start);
          await writer.write(buffer);
        }
        if (lineIndex < lines.length && lines[lineIndex]) {
          await writer.write(lines[lineIndex]);
        }
        position = range.end;
        lineIndex++;
      }

      const { size } = await reader.stat();
      if (position < size) {
        const buffer = await this.readRange(position, size);
        await writer.write(buffer);
      }

      await reader.close();
      await writer.close();
      await fs.promises.rename(tmpFile, this.file);
    } catch (e) {
      console.error('Erro ao substituir linhas:', e);
      throw e;
    } finally {
      await reader.close().catch(() => { });
      await writer.close().catch(() => { });
      await fs.promises.unlink(tmpFile).catch(() => { });
    }
  }

  async writeData(data, immediate, fd) {
    await fd.write(data)
  }

  writeDataSync(data) {
    fs.writeFileSync(this.file, data, { flag: 'a' })
  }

  async readLastLine() {
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
  }

  async destroy() {}
}
