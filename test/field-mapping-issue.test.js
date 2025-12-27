import { Database } from '../src/Database.mjs'
import fs from 'fs'
import path from 'path'

describe('Field Mapping Issue Test', () => {
  let testDir
  let db

  beforeEach(() => {
    testDir = path.join(process.cwd(), 'test-files', 'field-mapping-issue')
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

  test('reproduce exact field mapping issue from bug report', async () => {
    const dbPath = path.join(testDir, 'field-mapping-bug.jdb')

    // Exact configuration from the bug report
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
      indexes: {
        user_id: 'string',
        email: 'string'
      },
      allowIndexRebuild: true
    })
    await db.init()

    // Insert the exact record from the bug report
    const krishnaRecord = {
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
    console.log('Inserting Krishna record:', krishnaRecord)

    await db.insert(krishnaRecord)
    await db.save()

    // Read back using the exact query from bug report
    const records = await db.findOne({ user_id: 'wp_3' })
    console.log('Read back record:', records)

    // Check the problematic fields
    console.log('\n=== Field Analysis (Bug Report Issue) ===')
    console.log('commission_rate:', records.commission_rate, '(EXPECTED: 0.45)')
    console.log('total_earnings:', records.total_earnings, '(EXPECTED: 7.5)')
    console.log('total_sales:', records.total_sales, '(EXPECTED: 1)')

    // Check raw file content
    if (fs.existsSync(dbPath)) {
      const fileContent = fs.readFileSync(dbPath, 'utf8')
      const lines = fileContent.split('\n').filter(l => l.trim())
      console.log('\n=== Raw File Content ===')
      console.log('File has', lines.length, 'lines')
      if (lines.length > 0) {
        const rawArray = JSON.parse(lines[0])
        console.log('Raw JSON array:', rawArray)
        console.log('Array length:', rawArray.length)
        console.log('Expected mapping:')
        console.log('  [6] commission_rate should be:', rawArray[6])
        console.log('  [7] total_earnings should be:', rawArray[7])
        console.log('  [8] total_sales should be:', rawArray[8])
      }
    }

    // The bug report shows these wrong values:
    // commission_rate: 7.5 (should be 0.45)
    // total_earnings: 1 (should be 7.5)

    expect(records.commission_rate).toBe(0.45)
    expect(records.total_earnings).toBe(7.5)
    expect(records.total_sales).toBe(1)
  })

  test('test update scenario from bug report', async () => {
    const dbPath = path.join(testDir, 'update-bug.jdb')

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
      indexes: {
        user_id: 'string',
        email: 'string'
      },
      allowIndexRebuild: true
    })
    await db.init()

    // Insert initial record
    const initialRecord = {
      user_id: 'wp_3',
      email: 'developerguru99@gmail.com',
      payment_email: null,
      name: 'Krishna Rungta',
      company: '',
      country: 'BR',
      commission_rate: 0.35,  // Different initial value
      total_earnings: 5.0,    // Different initial value
      total_sales: 1,
      total_referrals: 0,
      status: 'active',
      language: 'pt'
    }

    await db.insert(initialRecord)
    await db.save()

    // Read initial record
    let krishna = await db.findOne({ user_id: 'wp_3' })
    console.log('Initial record - commission_rate:', krishna.commission_rate, 'total_earnings:', krishna.total_earnings)

    // Update as described in bug report
    await db.update({ user_id: krishna.user_id }, { total_earnings: 7.5 })
    await db.save()

    // Read after update
    krishna = await db.findOne({ user_id: 'wp_3' })
    console.log('After update - commission_rate:', krishna.commission_rate, 'total_earnings:', krishna.total_earnings)

    // Bug report says: updates are written to files but not reflected in reads
    expect(krishna.total_earnings).toBe(7.5)
    expect(krishna.commission_rate).toBe(0.35) // Should remain unchanged
  })
})



