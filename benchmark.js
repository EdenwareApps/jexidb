import { Database } from './src/Database.mjs';
import fs from 'fs';
import path from 'path';

async function benchmark() {
  const testDir = path.join(process.cwd(), 'test-files', 'benchmark');
  const dbPath = path.join(testDir, 'bench.jdb');

  // Cleanup
  if (fs.existsSync(testDir)) {
    fs.rmSync(testDir, { recursive: true, force: true });
  }
  fs.mkdirSync(testDir, { recursive: true });

  console.log('🚀 Starting JexiDB Performance Benchmark\n');

  const db = new Database(dbPath, {
    fields: {
      id: 'string',
      name: 'string',
      value: 'number',
      category: 'string'
    },
    indexes: {
      category: 'string'
    },
    clear: true,
    create: true
  });

  await db.init();

  // Insert 1000 records
  console.log('📝 Inserting 1000 records...');
  const records = [];
  for (let i = 0; i < 1000; i++) {
    records.push({
      id: `record-${i}`,
      name: `Name ${i}`,
      value: Math.random() * 1000,
      category: `cat-${i % 10}`
    });
  }

  const insertStart = Date.now();
  await db.insertBatch(records);
  await db.save();
  const insertTime = Date.now() - insertStart;
  console.log(`✅ Inserted 1000 records in ${insertTime}ms\n`);

  // Test 1: Find all records
  console.log('🔍 Testing find() performance...');
  const start1 = Date.now();
  const allRecords = await db.find({});
  const time1 = Date.now() - start1;
  console.log(`📊 Find all (${allRecords.length} records): ${time1}ms`);

  // Test 2: Find with criteria (indexed)
  const start2 = Date.now();
  const filteredRecords = await db.find({ category: 'cat-5' });
  const time2 = Date.now() - start2;
  console.log(`📊 Find filtered (${filteredRecords.length} records): ${time2}ms`);

  // Test 3: Multiple queries
  const start3 = Date.now();
  for (let i = 0; i < 10; i++) {
    await db.find({ category: `cat-${i % 10}` });
  }
  const time3 = Date.now() - start3;
  console.log(`📊 10 consecutive filtered finds: ${time3}ms\n`);

  // Test 4: Update + Save
  console.log('✏️ Testing update performance...');
  const updateStart = Date.now();
  const recordToUpdate = allRecords[0];
  recordToUpdate.value = 9999;
  await db.update(recordToUpdate);
  await db.save();
  const updateTime = Date.now() - updateStart;
  console.log(`✅ Update + save: ${updateTime}ms\n`);

  // Results
  console.log('📈 Performance Results:');
  console.log(`   • Insert 1000 records: ${insertTime}ms (${(1000/insertTime*1000).toFixed(0)} ops/sec)`);
  console.log(`   • Find all 1000 records: ${time1}ms`);
  console.log(`   • Find filtered (indexed): ${time2}ms`);
  console.log(`   • Update + save: ${updateTime}ms`);

  // Assessment
  const isFast = time1 < 500 && time2 < 100 && insertTime < 2000;
  console.log(`\n🎯 Assessment: ${isFast ? '✅ FAST - Good performance!' : '⚠️ SLOW - May need optimization'}`);

  await db.destroy();

  // Cleanup
  fs.rmSync(testDir, { recursive: true, force: true });
}

benchmark().catch(console.error);