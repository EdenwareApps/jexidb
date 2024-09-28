import fs from 'fs'

export default class FileHandler {
  constructor(filePath) {
    this.filePath = filePath
    this.descriptors = {}
  }

  async open(mode, file) {
    if (!file) {
      file = this.filePath
    }
    const key = file +'-'+ mode
    if (this.descriptors[key]) {
      this.descriptors[key].clients++
      return this.descriptors[key]
    }
    this.descriptors[key] = {fd: await fs.promises.open(this.filePath, mode), clients: 1}
    this.descriptors[key].fd.leave = async immediate => {
      this.descriptors[key].clients--
      if (this.descriptors[key].clients === 0) {
        if (immediate) {
          await this.descriptors[key].fd.close()
          delete this.descriptors[key]
        } else {
          setTimeout(async () => {
            if (this.descriptors[key].clients === 0) {
              await this.descriptors[key].fd.close()
              delete this.descriptors[key]
            }
          }, 1000)
        }
      }
    }
    return this.descriptors[key]
  }

  async truncate(offset) {
    await fs.promises.truncate(this.filePath, offset)
  }

  async *iterate(map) {
    if (map) {
      if (!map.length) {
        return
      }
      map.sort()
    }
    let closed = false, max, i = 0
    const rl = readline.createInterface({
      input: fs.createReadStream(this.filePath, { highWaterMark: 1024 * 64 }),
      crlfDelay: Infinity
    })
    max = map ? Math.max(...map) : -1
    for await (const line of rl) {
      if (!map || map.includes(i)) {
        if (!line || !line.startsWith('{')) {
          if (map || !line.startsWith('[')) {
            console.error('Bad line readen', this.filePath, i, line)
          }
        } else {
          const ret = await callback(line, i)
          if (ret === -1) {
            break
          } else {
            yield ret
          }
        }
      }
      if (i === max) {
        closed = true
        rl.close()
      }
      i++
    }
    closed || rl.close()
  }

  async readRange(start, end) {
    let data = []
    const stream = fs.createReadStream(this.filePath, { start, end });
    stream.on('data', chunk => data.push(chunk))
    return await new Promise((resolve, reject) => {
      stream.on('end', () => resolve(Buffer.concat(data).toString('utf-8')))
      stream.on('error', err => reject(err))
    })
  }

  async readRanges(ranges) {
    const lines = {}
    let fd = await this.open('r')
    for (const r of ranges) {
      try {
        const length = r.end - r.start
        let buffer = Buffer.alloc(length)
        const { bytesRead } = await fd.read(buffer, 0, length, r.start)
        if (bytesRead < buffer.length) {
          buffer = buffer.slice(0, bytesRead)
        }
        lines[r.start] = buffer.toString('utf-8')
      } catch (error) {
        console.error(error)
      }
    }
    await fd.leave()
    return lines
  }

  async replaceLines(ranges, lines) {
    const tmpFile = this.filePath + '.tmp'
    const reader = await this.open('r')
    const writer = await this.open('w+', tmpFile)
    let i = 0, start = 0
    for (const r of ranges) {
      const length = r.start - start
      const buffer = Buffer.alloc(length)
      const { bytesRead } = await reader.read(buffer, 0, length, start)
      if (bytesRead < buffer.length) {
        buffer = buffer.slice(0, bytesRead)
      }
      start = r.end + 1
      buffer.length && await writer.write(buffer)
      console.log('write replace', JSON.stringify(String(buffer)), buffer.length)
      if (lines[i]) {
        await writer.write(lines[i])
        console.log('write replace*', JSON.stringify(String(lines[i])))
      }
      i++
    }
    const size = (await reader.stat()).size
    const length = size - start
    const buffer = Buffer.alloc(length)
    await reader.read(buffer, 0, length, start)
    await reader.leave()
    await writer.write(buffer)
    console.log('write last', JSON.stringify(String(buffer)))
    await writer.leave()
    await fs.promises.copyFile(tmpFile, this.filePath)
    await fs.promises.unlink(tmpFile)
    console.log('read updated', String(await fs.promises.readFile(this.filePath)))
  }

  async writeData(data, immediate) {
    const fd = await this.open('a')
    await fd.write(this.filePath, data)
    await fd.leave(immediate)
  }

  async readLastLine() {
    const reader = await this.open('r')
    try {
      const { size } = await reader.stat()
      if (size < 1) throw 'empty file **'
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
        const newlineIndex = buffer.lastIndexOf(0x0A) // 0x0A is the ASCII code for '\n'
        if (newlineIndex !== -1) {
          const start = readPosition + newlineIndex + 1
          const lastLine = Buffer.alloc(size - start)
          await reader.read(lastLine, 0, size - start, start)
          if (!lastLine || !lastLine.length) {
            throw 'empty file ***'
          }
          return lastLine
        } else {
          readPosition -= bufferSize
        }
      }
    } catch (e) {
      console.error('Error reading last line:', e)
    } finally {
      reader.leave()
    }
  }

  async destroy() {
    for (const key in this.descriptors) {
      await this.descriptors[key].fd.close()
    }
  }
}
