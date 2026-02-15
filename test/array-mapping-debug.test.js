import { Database } from '../src/Database.mjs'
import fs from 'fs'
import path from 'path'

describe('Array Mapping Debug Test', () => {
  let testDir
  let db

  beforeEach(() => {
    testDir = path.join(process.cwd(), 'test-files', 'array-mapping-debug')
    fs.mkdirSync(testDir, { recursive: true })
  })

  afterEach(async () => {
    if (db && !db.destroyed) {
      await db.waitForOperations()
      await db.close()
    }
    if (fs.existsSync(testDir)) {
      try {
        fs.rmSync(testDir, { recursive: true, force: true })
      } catch (error) {
        console.warn('Could not clean up test directory:', testDir)
      }
    }
  })

  test('debug array to object mapping', async () => {
    const dbPath = path.join(testDir, 'mapping-test.jdb')

    // Create database with specific field order
    db = new Database(dbPath, {
      fields: {
        user_id: 'string',
        email: 'string',
        payment_email: 'string',
        name: 'string',
        company: 'string',
        country: 'string',
        commission_rate: 'number',
        total_earnings: 'number',
        total_sales: 'number',
        total_referrals: 'number',
        status: 'string',
        language: 'string'
      },
      clear: true,
      create: true,
      debugMode: true
    })
    await db.init()

    // Insert test record that matches the reported problematic data
    const testRecord = {
      user_id: 'wp_3',
      email: 'alex.taylor@example.com',
      payment_email: null,
      name: 'Alex Taylor',
      company: '',
      country: 'BR',
      commission_rate: 0.45,    // Position 6 in schema
      total_earnings: 7.5,      // Position 7 in schema
      total_sales: 1,           // Position 8 in schema
      total_referrals: 0,       // Position 9 in schema
      status: 'active',         // Position 10 in schema
      language: 'pt'            // Position 11 in schema
    }

    console.log('Schema order:', db.serializer.schemaManager.getSchema())
    console.log('Inserting record:', testRecord)

    await db.insert(testRecord)
    await db.save()

    // Read back the record
    const records = await db.find({})
    console.log('Records found:', records.length)
    console.log('First record:', records[0])

    const record = records[0]
    console.log('\n=== Field Analysis ===')
    console.log('commission_rate:', record.commission_rate, '(expected: 0.45)')
    console.log('total_earnings:', record.total_earnings, '(expected: 7.5)')
    console.log('total_sales:', record.total_sales, '(expected: 1)')
    console.log('total_referrals:', record.total_referrals, '(expected: 0)')

    // Check file content
    if (fs.existsSync(dbPath)) {
      const fileContent = fs.readFileSync(dbPath, 'utf8')
      const lines = fileContent.split('\n').filter(l => l.trim())
      console.log('\n=== File Content ===')
      console.log('Lines in file:', lines.length)
      if (lines.length > 0) {
        const firstLine = lines[0]
        console.log('First line:', firstLine)
        try {
          const parsed = JSON.parse(firstLine)
          console.log('Parsed array:', parsed)
          console.log('Array length:', parsed.length)
          console.log('Position 6 (commission_rate):', parsed[6])
          console.log('Position 7 (total_earnings):', parsed[7])
          console.log('Position 8 (total_sales):', parsed[8])
        } catch (e) {
          console.log('Parse error:', e.message)
        }
      }
    }

    // Verify correctness
    expect(record.commission_rate).toBe(0.45)
    expect(record.total_earnings).toBe(7.5)
    expect(record.total_sales).toBe(1)
    expect(record.total_referrals).toBe(0)
  })
})




