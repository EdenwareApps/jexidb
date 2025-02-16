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
    try {
      const tasks = ranges.map(r => {
        return async () => {
          let err
          const length = r.end - r.start
          let buffer = Buffer.alloc(length)
          const { bytesRead } = await fd.read(buffer, 0, length, r.start).catch(e => err = e)
          if (buffer.length > bytesRead) buffer = buffer.subarray(0, bytesRead)
          lines[r.start] = mapper ? (await mapper(buffer, r)) : buffer
        }
      })
      await Promise.allSettled(tasks.map(limit))
    } catch (e) {
      console.error('Error reading ranges:', e)
    } finally {
      await fd.close()
    }
    return lines
  }

  async replaceLines(ranges, lines) {
    let closed
    const tmpFile = this.file + '.tmp'
    const writer = await fs.promises.open(tmpFile, 'w+')
    const reader = await fs.promises.open(this.file, 'r')
    try {
      let i = 0, start = 0
      for (const r of ranges) {
        const length = r.start - start
        const buffer = Buffer.alloc(length)
        await reader.read(buffer, 0, length, start)
        start = r.end
        buffer.length && await writer.write(buffer)
        if (lines[i]) {
          await writer.write(lines[i])
        }
        i++
      }
      const size = (await reader.stat()).size
      const length = size - start
      const buffer = Buffer.alloc(length)
      await reader.read(buffer, 0, length, start)
      await writer.write(buffer)
      await reader.close()
      await writer.close()
      closed = true
      await fs.promises.copyFile(tmpFile, this.file)
    } catch (e) {
      console.error('Error replacing lines:', e)
    } finally {
      if(!closed) {
        await reader.close()
        await writer.close()
      }
      await fs.promises.unlink(tmpFile).catch(() => {})
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
      let buffer, lastReadSize, readPosition = Math.max(size - bufferSize, 0)
      while (readPosition >= 0) {
        const readSize = Math.min(bufferSize, size - readPosition)
        if (readSize !== lastReadSize) {
          lastReadSize = readSize
          buffer = Buffer.alloc(readSize)
        }
        const { bytesRead } = await reader.read(buffer, 0, readSize, readPosition)
        if (bytesRead === 0) break
        const newlineIndex = buffer.lastIndexOf(10, size - 4) // 0x0A is the ASCII code for '\n'
        if (newlineIndex !== -1) {
          const start = readPosition + newlineIndex + 1
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
