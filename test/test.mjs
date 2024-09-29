import fs from 'fs';
import path from 'path';
import { Database } from '../src/Database.mjs';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Path to the test file
const testFilePath = path.join(__dirname, 'test.jdb');

// Function to clear the test file before each run
const clearTestFile = () => {
    fs.writeFileSync(testFilePath, '', { encoding: null });
};

// Function to run the tests
const runTests = async () => {
    clearTestFile(); // Clears the file before starting tests
    const db = new Database(testFilePath, {
        indexes: {id: 'number'},
        v8: false,
        compressIndex: false
    }); // Instantiate the database
    
    // 1. Test if the instance is created correctly
    await db.init(); // Call init() right after instantiation
    console.assert(db.initialized === true, 'Test failed: Database is not loaded.');

    // 2. Test data insertion
    await db.insert({ id: 1, name: 'Alice' });
    await db.insert({ id: 2, name: 'Bob' });
    
    // 3. Test if data was inserted correctly
    let results = await db.query([0, 1]);
    console.assert(results.length === 2, 'Test failed: Expected 2 entries after insertion.');
    console.assert(results[0].name === 'Alice', 'Test failed: Incorrect name for the first entry.');
  
    // 4. Test data update
    await db.update({ id: 1 }, { name: 'Alice Updated' });
    results = await db.query([0, 1]);
    console.log(results)
    console.assert(results[0].name === 'Alice Updated', 'Test failed: First entry name was not updated.');

    // 5. Test data deletion
    await db.delete({ id: 1 });
    
    results = await db.query([0, 1]);
    console.assert(results.length === 1, 'Test failed: Expected 1 entry after deletion.');
    console.assert(results[0].name === 'Bob', 'Test failed: Remaining entry is incorrect.');

    // 6. Test reading lines
    await db.insert({ id: 3, name: 'Charlie' });
    await db.insert({ id: 4, name: 'Diana' });
    results = await db.query([0, 1, 2]);
    console.assert(results.length === 3, 'Test failed: Expected 3 entries when reading lines.');

    // 7. Test query with criteria
    const queryResults = await db.query({ id: { '>': 2 } });
    console.assert(queryResults.length === 2, 'Test failed: Expected 2 entries for id > 2.');

    await db.save();
    await db.destroy();
    console.log('All tests ran successfully!');
};

// Run the tests
runTests().catch(error => console.error('Error during tests:', error));
