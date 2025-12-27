import { Database } from '../src/Database.mjs'
import fs from 'fs'
import path from 'path'

describe('Schema Migration Debug Test', () => {
  let testDir
  let db

  beforeEach(() => {
    testDir = path.join(process.cwd(), 'test-files', 'schema-migration-debug')
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

  test('debug schema migration detection', async () => {
    const dbPath = path.join(testDir, 'schema-test.jdb')

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

    const testRecord = {
      user_id: 'wp_3',
      email: 'developerguru99@gmail.com',
      payment_email: null,
      name: 'Krishna Rungta',
      company: '',
      country: 'BR',
      commission_rate: 0.45,
      total_earnings: 7.5,
      total_sales: 1,
      total_referrals: 0,
      status: 'active',
      language: 'pt'
    }

    console.log('Schema:', db.serializer.schemaManager.getSchema())

    await db.insert(testRecord)
    await db.save()

    // Check raw file content
    if (fs.existsSync(dbPath)) {
      const fileContent = fs.readFileSync(dbPath, 'utf8')
      const lines = fileContent.split('\n').filter(l => l.trim())
      console.log('\n=== Raw File Content ===')
      if (lines.length > 0) {
        const rawArray = JSON.parse(lines[0])
        console.log('Raw JSON array:', rawArray)
        console.log('Array length:', rawArray.length)
        console.log('Expected length (schema):', db.serializer.schemaManager.getSchema().length)

        // Test arrayToObject conversion manually
        console.log('\n=== Manual arrayToObject Test ===')
        const converted = db.serializer.schemaManager.arrayToObject(rawArray)
        console.log('Converted object:', converted)

        console.log('\n=== Field Position Analysis ===')
        const schema = db.serializer.schemaManager.getSchema()
        schema.forEach((field, index) => {
          console.log(`${field} (pos ${index}): ${rawArray[index]} -> ${converted[field]}`)
        })
      }
    }

    // Read back through database
    const records = await db.find({})
    const record = records[0]

    console.log('\n=== Database Read Results ===')
    console.log('commission_rate:', record.commission_rate, '(expected: 0.45)')
    console.log('total_earnings:', record.total_earnings, '(expected: 7.5)')
    console.log('total_sales:', record.total_sales, '(expected: 1)')

    expect(record.commission_rate).toBe(0.45)
    expect(record.total_earnings).toBe(7.5)
    expect(record.total_sales).toBe(1)
  })
})



