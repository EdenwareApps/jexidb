import { Database } from '../src/Database.mjs'
import fs from 'fs'
import path from 'path'

describe('Insert Key Order Test', () => {
  let testDir
  let db

  beforeEach(() => {
    testDir = path.join(process.cwd(), 'test-files', 'insert-key-order')
    fs.mkdirSync(testDir, { recursive: true })
  })

  afterEach(async () => {
    if (db && !db.destroyed) {
      await db.waitForOperations()
      await db.close()
    }
    // Clean up test files
    if (fs.existsSync(testDir)) {
      try {
        fs.rmSync(testDir, { recursive: true, force: true })
      } catch (error) {
        console.warn('Could not clean up test directory:', testDir)
      }
    }
  })

  test('insert() should work regardless of key order', async () => {
    const dbPath = path.join(testDir, 'key-order-test.jdb')
    
    // Create database with explicit fields
    db = new Database(dbPath, { 
      fields: { name: 'string', age: 'number', city: 'string' },
      clear: true, 
      create: true 
    })
    await db.init()

    // Insert records with different key orders
    const record1 = await db.insert({ name: 'John', age: 30, city: 'NYC' })
    const record2 = await db.insert({ age: 25, city: 'LA', name: 'Jane' })
    const record3 = await db.insert({ city: 'SF', name: 'Bob', age: 35 })
    
    // Save to persist
    await db.save()

    // Verify all records were inserted correctly
    expect(record1).toBeDefined()
    expect(record1.id).toBeDefined()
    expect(record1.name).toBe('John')
    expect(record1.age).toBe(30)
    expect(record1.city).toBe('NYC')

    expect(record2).toBeDefined()
    expect(record2.id).toBeDefined()
    expect(record2.name).toBe('Jane')
    expect(record2.age).toBe(25)
    expect(record2.city).toBe('LA')

    expect(record3).toBeDefined()
    expect(record3.id).toBeDefined()
    expect(record3.name).toBe('Bob')
    expect(record3.age).toBe(35)
    expect(record3.city).toBe('SF')

    // Query all records and verify they can be retrieved correctly
    const allRecords = await db.find({})
    expect(allRecords.length).toBe(3)

    const john = allRecords.find(r => r.name === 'John')
    const jane = allRecords.find(r => r.name === 'Jane')
    const bob = allRecords.find(r => r.name === 'Bob')

    expect(john).toBeDefined()
    expect(john.age).toBe(30)
    expect(john.city).toBe('NYC')

    expect(jane).toBeDefined()
    expect(jane.age).toBe(25)
    expect(jane.city).toBe('LA')

    expect(bob).toBeDefined()
    expect(bob.age).toBe(35)
    expect(bob.city).toBe('SF')
  })

  test('insert() should work with explicit fields configuration regardless of key order', async () => {
    const dbPath = path.join(testDir, 'key-order-explicit-fields.jdb')
    
    // Create database with explicit fields
    db = new Database(dbPath, { 
      fields: { name: 'string', age: 'number', city: 'string' },
      clear: true, 
      create: true 
    })
    await db.init()

    // Insert records with different key orders
    const record1 = await db.insert({ name: 'Alice', age: 28, city: 'Boston' })
    const record2 = await db.insert({ age: 32, city: 'Seattle', name: 'Charlie' })
    const record3 = await db.insert({ city: 'Miami', name: 'David', age: 27 })
    
    // Save to persist
    await db.save()

    // Verify all records were inserted correctly
    expect(record1.name).toBe('Alice')
    expect(record1.age).toBe(28)
    expect(record1.city).toBe('Boston')

    expect(record2.name).toBe('Charlie')
    expect(record2.age).toBe(32)
    expect(record2.city).toBe('Seattle')

    expect(record3.name).toBe('David')
    expect(record3.age).toBe(27)
    expect(record3.city).toBe('Miami')

    // Close and reopen to test persistence
    await db.close()
    db = new Database(dbPath, {
      fields: { name: 'string', age: 'number', city: 'string' }
    })
    await db.init()

    // Query all records after reopening
    const allRecords = await db.find({})
    expect(allRecords.length).toBe(3)

    const alice = allRecords.find(r => r.name === 'Alice')
    const charlie = allRecords.find(r => r.name === 'Charlie')
    const david = allRecords.find(r => r.name === 'David')

    expect(alice).toBeDefined()
    expect(alice.age).toBe(28)
    expect(alice.city).toBe('Boston')

    expect(charlie).toBeDefined()
    expect(charlie.age).toBe(32)
    expect(charlie.city).toBe('Seattle')

    expect(david).toBeDefined()
    expect(david.age).toBe(27)
    expect(david.city).toBe('Miami')
  })

  test('insert() should preserve data when keys are in different order across multiple inserts', async () => {
    const dbPath = path.join(testDir, 'key-order-multiple.jdb')
    
    db = new Database(dbPath, { 
      fields: { name: 'string', age: 'number', city: 'string' },
      clear: true, 
      create: true 
    })
    await db.init()

    // Insert 10 records with randomized key orders
    const records = []
    const names = ['Alice', 'Bob', 'Charlie', 'David', 'Eve', 'Frank', 'Grace', 'Henry', 'Ivy', 'Jack']
    const ages = [20, 25, 30, 35, 40, 45, 50, 55, 60, 65]
    const cities = ['NYC', 'LA', 'SF', 'Boston', 'Seattle', 'Miami', 'Chicago', 'Denver', 'Austin', 'Portland']

    // Create records with different key orders
    const keyOrders = [
      ['name', 'age', 'city'],
      ['age', 'city', 'name'],
      ['city', 'name', 'age'],
      ['name', 'city', 'age'],
      ['age', 'name', 'city'],
      ['city', 'age', 'name'],
      ['name', 'age', 'city'],
      ['age', 'city', 'name'],
      ['city', 'name', 'age'],
      ['name', 'city', 'age']
    ]

    for (let i = 0; i < 10; i++) {
      const obj = {}
      keyOrders[i].forEach(key => {
        if (key === 'name') obj.name = names[i]
        if (key === 'age') obj.age = ages[i]
        if (key === 'city') obj.city = cities[i]
      })
      const record = await db.insert(obj)
      records.push(record)
    }

    // Save to persist
    await db.save()

    // Verify all records
    expect(records.length).toBe(10)
    records.forEach((record, i) => {
      expect(record.name).toBe(names[i])
      expect(record.age).toBe(ages[i])
      expect(record.city).toBe(cities[i])
    })

    // Query and verify
    const allRecords = await db.find({})
    expect(allRecords.length).toBe(10)

    names.forEach((name, i) => {
      const found = allRecords.find(r => r.name === name)
      expect(found).toBeDefined()
      expect(found.age).toBe(ages[i])
      expect(found.city).toBe(cities[i])
    })
  })

  test('insert() should handle missing fields regardless of key order', async () => {
    const dbPath = path.join(testDir, 'key-order-missing-fields.jdb')
    
    db = new Database(dbPath, { 
      fields: { name: 'string', age: 'number', city: 'string', email: 'string' },
      clear: true, 
      create: true 
    })
    await db.init()

    // Insert records with different key orders and some missing fields
    const record1 = await db.insert({ name: 'Test1', age: 20 })
    const record2 = await db.insert({ age: 25, name: 'Test2' })
    const record3 = await db.insert({ city: 'NYC', name: 'Test3' })
    const record4 = await db.insert({ email: 'test@test.com', age: 30, name: 'Test4' })

    await db.save()

    // Verify records
    expect(record1.name).toBe('Test1')
    expect(record1.age).toBe(20)
    expect(record1.city).toBeUndefined()
    expect(record1.email).toBeUndefined()

    expect(record2.name).toBe('Test2')
    expect(record2.age).toBe(25)

    expect(record3.name).toBe('Test3')
    expect(record3.city).toBe('NYC')

    expect(record4.name).toBe('Test4')
    expect(record4.age).toBe(30)
    expect(record4.email).toBe('test@test.com')
  })

  test('insert() should produce identical results regardless of key order after save/reload', async () => {
    const dbPath = path.join(testDir, 'key-order-serialization.jdb')
    
    db = new Database(dbPath, { 
      fields: { name: 'string', age: 'number', city: 'string', active: 'boolean' },
      clear: true, 
      create: true 
    })
    await db.init()

    // Insert same data with different key orders
    const data1 = { name: 'Same', age: 42, city: 'Test', active: true }
    const data2 = { age: 42, city: 'Test', active: true, name: 'Same' }
    const data3 = { city: 'Test', active: true, name: 'Same', age: 42 }
    const data4 = { active: true, name: 'Same', age: 42, city: 'Test' }

    const record1 = await db.insert(data1)
    const record2 = await db.insert(data2)
    const record3 = await db.insert(data3)
    const record4 = await db.insert(data4)

    // All should have same values (except id)
    expect(record1.name).toBe(record2.name)
    expect(record1.age).toBe(record2.age)
    expect(record1.city).toBe(record2.city)
    expect(record1.active).toBe(record2.active)

    expect(record2.name).toBe(record3.name)
    expect(record2.age).toBe(record3.age)
    expect(record2.city).toBe(record3.city)
    expect(record2.active).toBe(record3.active)

    expect(record3.name).toBe(record4.name)
    expect(record3.age).toBe(record4.age)
    expect(record3.city).toBe(record4.city)
    expect(record3.active).toBe(record4.active)

    // Save and reload
    await db.save()
    await db.close()

    db = new Database(dbPath, {
      fields: { name: 'string', age: 'number', city: 'string', active: 'boolean' }
    })
    await db.init()

    // Query all records
    const allRecords = await db.find({ name: 'Same' })
    expect(allRecords.length).toBe(4)

    // All records should have identical values
    const firstRecord = allRecords[0]
    allRecords.forEach(record => {
      expect(record.name).toBe(firstRecord.name)
      expect(record.age).toBe(firstRecord.age)
      expect(record.city).toBe(firstRecord.city)
      expect(record.active).toBe(firstRecord.active)
    })
  })

  test('insert() should handle complex nested objects regardless of key order', async () => {
    const dbPath = path.join(testDir, 'key-order-complex.jdb')
    
    // Note: jexidb may not support nested objects directly, but we test flat structures
    db = new Database(dbPath, { 
      fields: { 
        firstName: 'string', 
        lastName: 'string', 
        birthYear: 'number', 
        salary: 'number',
        department: 'string'
      },
      clear: true, 
      create: true 
    })
    await db.init()

    // Insert employee records with different key orders
    const employees = [
      { firstName: 'John', lastName: 'Doe', birthYear: 1990, salary: 50000, department: 'IT' },
      { lastName: 'Smith', firstName: 'Jane', department: 'HR', birthYear: 1985, salary: 60000 },
      { salary: 55000, firstName: 'Bob', department: 'Finance', lastName: 'Johnson', birthYear: 1992 },
      { department: 'IT', birthYear: 1988, salary: 65000, lastName: 'Williams', firstName: 'Alice' }
    ]

    const inserted = []
    for (const emp of employees) {
      const record = await db.insert(emp)
      inserted.push(record)
    }

    await db.save()

    // Verify all records
    inserted.forEach((record, i) => {
      expect(record.firstName).toBe(employees[i].firstName)
      expect(record.lastName).toBe(employees[i].lastName)
      expect(record.birthYear).toBe(employees[i].birthYear)
      expect(record.salary).toBe(employees[i].salary)
      expect(record.department).toBe(employees[i].department)
    })

    // Query and verify persistence
    await db.close()
    db = new Database(dbPath, {
      fields: { 
        firstName: 'string', 
        lastName: 'string', 
        birthYear: 'number', 
        salary: 'number',
        department: 'string'
      }
    })
    await db.init()

    const allRecords = await db.find({})
    expect(allRecords.length).toBe(4)

    employees.forEach(emp => {
      const found = allRecords.find(r => r.firstName === emp.firstName && r.lastName === emp.lastName)
      expect(found).toBeDefined()
      expect(found.birthYear).toBe(emp.birthYear)
      expect(found.salary).toBe(emp.salary)
      expect(found.department).toBe(emp.department)
    })
  })
})

