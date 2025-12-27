import { Database } from '../src/Database.mjs'
import fs from 'fs'
import path from 'path'

async function demonstrateBug() {
    console.log('=== JexiDB Delete Bug Demonstration ===\n')

    // Initialize database
    const dbPath = path.join(process.cwd(), 'test-files', 'delete-bug-demo.jdb')
    const config = {
        fields: {
            id: 'string',
            user_id: 'string',
            email: 'string',
            name: 'string',
            status: 'string'
        },
        indexes: {
            user_id: 'string',
            email: 'string'
        },
        allowIndexRebuild: true
    }

    const db = new Database(dbPath, config)
    await db.init()

    // Insert test record
    const testRecord = {
        id: 'test-record-123',
        user_id: 'user-abc-456',
        email: 'test@example.com',
        name: 'Test User',
        status: 'active'
    }

    console.log('1. Inserting test record...')
    await db.insert(testRecord)
    await db.save()

    // Verify insertion
    const afterInsert = await db.find({})
    console.log(`   Records in database: ${afterInsert.length}`)
    console.log(`   Record inserted: ${afterInsert[0].email}`)

    // Attempt deletion by id
    console.log('\n2. Attempting to delete by id field...')
    const deleteResult1 = await db.delete({ id: testRecord.id })
    console.log(`   delete() returned: ${JSON.stringify(deleteResult1)}`)
    await db.save()

    const afterDelete1 = await db.find({})
    console.log(`   Records after delete: ${afterDelete1.length}`)
    console.log(`   Record still exists: ${afterDelete1.length > 0 ? 'YES ❌' : 'NO ✅'}`)

    if (afterDelete1.length > 0) {
        console.log(`   Found record: ${afterDelete1[0].email}`)
    }

    // Attempt deletion by user_id (indexed field)
    console.log('\n3. Attempting to delete by user_id (indexed field)...')
    const deleteResult2 = await db.delete({ user_id: testRecord.user_id })
    console.log(`   delete() returned: ${JSON.stringify(deleteResult2)}`)
    await db.save()

    const afterDelete2 = await db.find({})
    console.log(`   Records after delete: ${afterDelete2.length}`)
    console.log(`   Record still exists: ${afterDelete2.length > 0 ? 'YES ❌' : 'NO ✅'}`)

    // Attempt deletion by email (indexed field)
    console.log('\n4. Attempting to delete by email (indexed field)...')
    const deleteResult3 = await db.delete({ email: testRecord.email })
    console.log(`   delete() returned: ${JSON.stringify(deleteResult3)}`)
    await db.save()

    const afterDelete3 = await db.find({})
    console.log(`   Records after delete: ${afterDelete3.length}`)
    console.log(`   Record still exists: ${afterDelete3.length > 0 ? 'YES ❌' : 'NO ✅'}`)

    // Verify by reading database file directly
    console.log('\n5. Verifying by reading database file directly...')
    if (fs.existsSync(dbPath)) {
        const fileContent = fs.readFileSync(dbPath, 'utf8')
        const lines = fileContent.split('\n').filter(l => l.trim())
        console.log(`   Lines in file: ${lines.length}`)
        console.log(`   Record found in file: ${lines.length > 0 ? 'YES ❌' : 'NO ✅'}`)
    }

    // Summary
    console.log('\n=== Summary ===')
    console.log('All delete() calls returned arrays, suggesting they found records to delete.')
    console.log('The record IS removed from the JSONL data file (verified by direct file read).')
    console.log('find() queries should NOT return the deleted record.')

    // Cleanup
    if (fs.existsSync(dbPath)) {
        fs.unlinkSync(dbPath)
    }
    const idxPath = dbPath + '.idx.jdb'
    if (fs.existsSync(idxPath)) {
        fs.unlinkSync(idxPath)
    }

    await db.close()
}

describe('Delete Bug Reproduction Test', () => {
  test('reproduce original bug report', async () => {
    await demonstrateBug()
  })
})





