const { Database } = require('../dist/Database.cjs');
const fs = require('fs');
const path = require('path');

async function debugUpdate() {
  const testFile = path.join(__dirname, 'test-files', 'debug-update-multiple.jdb');
  
  // Clean up
  if (fs.existsSync(testFile)) fs.unlinkSync(testFile);
  if (fs.existsSync(testFile + '.idx.jdb')) fs.unlinkSync(testFile + '.idx.jdb');
  
  const db = new Database(testFile, { clear: true, create: true, debugMode: true });
  await db.init();
  
  console.log('\n=== STEP 1: Insert records ===');
  const record1 = await db.insert({ name: 'Alice', age: 20 });
  const record2 = await db.insert({ name: 'Bob', age: 25 });
  console.log('Record1 ID:', record1.id);
  console.log('Record2 ID:', record2.id);
  
  console.log('\n=== STEP 2: Save after insert ===');
  await db.save();
  
  console.log('\n=== STEP 3: Verify records before update ===');
  const before1 = await db.findOne({ id: record1.id });
  const before2 = await db.findOne({ id: record2.id });
  console.log('Before1:', JSON.stringify(before1, null, 2));
  console.log('Before2:', JSON.stringify(before2, null, 2));
  
  console.log('\n=== STEP 4: Update multiple records ===');
  const updateResult = await db.update({ age: { $lt: 30 } }, { status: 'young' });
  console.log('Update result count:', updateResult.length);
  console.log('Update results:', updateResult.map(r => ({ id: r.id, status: r.status })));
  
  console.log('\n=== STEP 5: Verify in memory after update ===');
  const inMemory1 = await db.findOne({ id: record1.id });
  const inMemory2 = await db.findOne({ id: record2.id });
  console.log('InMemory1:', JSON.stringify(inMemory1, null, 2));
  console.log('InMemory2:', JSON.stringify(inMemory2, null, 2));
  console.log('InMemory1.status:', inMemory1?.status);
  console.log('InMemory2.status:', inMemory2?.status);
  
  console.log('\n=== STEP 6: Save after update ===');
  await db.save();
  
  console.log('\n=== STEP 7: Verify after save (before close) ===');
  const afterSave1 = await db.findOne({ id: record1.id });
  const afterSave2 = await db.findOne({ id: record2.id });
  console.log('AfterSave1:', JSON.stringify(afterSave1, null, 2));
  console.log('AfterSave2:', JSON.stringify(afterSave2, null, 2));
  console.log('AfterSave1.status:', afterSave1?.status);
  console.log('AfterSave2.status:', afterSave2?.status);
  
  console.log('\n=== STEP 8: Close and reopen ===');
  await db.close();
  
  const db2 = new Database(testFile, { create: true, debugMode: true });
  await db2.init();
  
  console.log('\n=== STEP 9: Verify after reopen ===');
  const afterReopen1 = await db2.findOne({ id: record1.id });
  const afterReopen2 = await db2.findOne({ id: record2.id });
  console.log('AfterReopen1:', JSON.stringify(afterReopen1, null, 2));
  console.log('AfterReopen2:', JSON.stringify(afterReopen2, null, 2));
  console.log('AfterReopen1.status:', afterReopen1?.status);
  console.log('AfterReopen2.status:', afterReopen2?.status);
  
  // Read file directly to see what's actually stored
  console.log('\n=== STEP 10: Read file directly ===');
  if (fs.existsSync(testFile)) {
    const fileContent = fs.readFileSync(testFile, 'utf8');
    const lines = fileContent.split('\n').filter(l => l.trim());
    console.log('File has', lines.length, 'lines');
    lines.forEach((line, i) => {
      try {
        const parsed = JSON.parse(line);
        console.log(`Line ${i} (raw):`, line.substring(0, 150));
        console.log(`Line ${i} (parsed):`, parsed);
        // If it's array format, try to convert back to object
        if (Array.isArray(parsed)) {
          // Array format: [age, name, id, ...other fields]
          // We need to know the schema to convert properly
          // For now, just show what we have
          console.log(`Line ${i} is array format with ${parsed.length} elements`);
        } else {
          console.log(`Line ${i}:`, { id: parsed.id, name: parsed.name, age: parsed.age, status: parsed.status });
        }
      } catch (e) {
        console.log(`Line ${i}: [PARSE ERROR]`, line.substring(0, 100), e.message);
      }
    });
  }
  
  await db2.close();
  
  // Clean up
  if (fs.existsSync(testFile)) fs.unlinkSync(testFile);
  if (fs.existsSync(testFile + '.idx.jdb')) fs.unlinkSync(testFile + '.idx.jdb');
}

debugUpdate().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});

