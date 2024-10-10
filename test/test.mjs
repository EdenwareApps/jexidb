import fs from 'fs';
import path from 'path';
import { Database } from '../src/Database.mjs';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Function to run the tests
const runTests = async (id, name, format, opts) => {

    // Path to the test file
    const testFilePath = path.join(__dirname, 'test-'+ name +'.jdb');
    
    // Function to clear the test file before each run
    const clearTestFile = () => {
        fs.writeFileSync(testFilePath, '', { encoding: null });
    }

    console.log('Battle #'+ id +' is starting...\n');
    clearTestFile(); // Clears the file before starting tests
    const db = new Database(testFilePath, opts); // Instantiate the database

    // 1. Test if the instance is created correctly
    await db.init(); // Call init() right after instantiation
    console.assert(db.initialized === true, "Test failed: Database didn't initialize. Looks like Raiden needs to give it a shock!");

    // 2. Test data insertion with Mortal Kombat characters
    await db.insert({ id: 1, name: 'Scorpion', signatureMove: 'Spear', powerType: 'Hellfire' });
    await db.insert({ id: 2, name: 'Sub-Zero', signatureMove: 'Ice Ball', powerType: 'Cryomancy' });
    await db.insert({ id: 3, name: 'Raiden', signatureMove: 'Electric Fly', powerType: 'Lightning' });
    await db.insert({ id: 4, name: 'Jax', signatureMove: 'Ground Pound', powerType: 'Strength' });
    await db.insert({ id: 5, name: 'Sindel', signatureMove: 'Sonic Scream', powerType: 'Sound' });
    await db.insert({ id: 6, name: 'Ermac', signatureMove: 'Telekinesis', powerType: 'Telekinesis' });
    await db.insert({ id: 7, name: 'Mileena', signatureMove: 'Sai Throw', powerType: 'Teleportation' });
    await db.insert({ id: 8, name: 'Kenshi', signatureMove: 'Telekinetic Slash', powerType: 'Telekinesis' });
    await db.insert({ id: 9, name: 'D\'Vorah', signatureMove: 'Swarm', powerType: 'Insects' });
    await db.insert({ id: 10, name: 'Scarlet', signatureMove: 'Blood Tentacle', powerType: 'Blood Manipulation' });
    await db.insert({ id: 11, name: 'Frost', signatureMove: 'Ice Daggers', powerType: 'Cryomancy' });

    // "Flawless Victory" message if insertion is successful
    console.log('Round 1 - CREATE: Flawless Victory! All characters inserted successfully.');

    // 3. Test if data was inserted correctly
    let results = await db.query({ id: { '<=': 5 } });
    const pass1 = results.length === 5
    const pass2 = results[0].name === 'Scorpion'
    console.assert(pass1, 'Round 2 - READ: Test failed: Where is everyone? Did Scorpion pull a "GET OVER HERE" on the missing entries?');
    console.assert(pass2, 'Round 2 - READ: Test failed: Scorpion seems to have teleported out of the database!');
    if(pass1 && pass2) console.log('Round 2 - READ: Flawless Victory! All characters inserted successfully.');

    // 4. Test data update
    await db.update({ id: 1 }, { name: 'Scorpion Updated' });
    results = await db.query({ id: 1 });
    const pass4 = results.length === 1 && results[0].name === 'Scorpion Updated'
    console.assert(pass4, 'Round 3 - UPDATE: Test failed: Scorpion refuses to update. Maybe he’s stuck in the Netherrealm?');
    if(pass4) console.log('Round 3 - UPDATE: Flawless Victory! Scorpion has been updated successfully.');

    // 5. Test data deletion
    await db.delete({ name: 'Scorpion Updated' });
    
    results = await db.query({ id: { '<=': 2 } });
    const pass5 = results.length === 1
    const pass6 = results[0].name === 'Sub-Zero'
    console.assert(pass5, 'Round 4 - DELETE: Test failed: I thought Scorpion was gone, but he’s still here! Must be that "Hellfire Resurrection."');
    console.assert(pass6, 'Round 4 - DELETE: Test failed: Sub-Zero is nowhere to be seen. Did he freeze the system?');
    if(pass5 && pass6) console.log('Round 4 - DELETE: Flawless Victory! Scorpion has been eliminated successfully.');

    // 6. Test query with criteria (IDs greater than 5)
    const queryResults = await db.query({ id: { '>': 5 } });
    console.assert(queryResults.length === 6, 'Test failed: Sindel probably screamed so loudly, she scared off the other characters with id > 5.');

    let i = 0;
    for await (const entry of db.walk()) {
        i++;
    }
    console.assert(i === 10, 'Test failed: Expected 10 entries on walk(). Who is missing? Did Ermac telekinetically mess up the list?');
    
    db.index.myCustomValue = true;
    await db.save();
    await db.destroy();
    
    const bd = new Database(testFilePath, opts);
    await bd.init();
    console.assert(bd.index.myCustomValue === true, 'Test failed: Even after reloading, the arbitrary value got lost in the Netherrealm!');

    // Insert a new character after reloading
    await bd.insert({ id: 12, name: 'Shang Tsung', signatureMove: 'Soul Steal', powerType: 'Soul Manipulation' });

    results = await bd.query({ name: 'Shang Tsung' });
    console.assert(results.length === 1, 'Test failed: Shang Tsung failed to appear. Did someone steal his soul?');
    console.assert(results[0].name === 'Shang Tsung', 'Test failed: Shang Tsung’s soul is corrupted!');

    results = await bd.query({ id: 5 });
    console.assert(results.length === 1, 'Test failed: Sindel disappeared from the list! Is it because of her Sonic Scream?');
    console.assert(results[0].name === 'Sindel', 'Test failed: Sindel’s data got messed up. Did she scream too loud again?');

    if(pass1 && pass2 && pass4 && pass5 && pass6) {
        console.log('\nBattle #'+ id +' finished: All tests with format "'+ format +'" ran successfully! Fatality avoided this time.\n\n');
    } else {
        throw '\nBattle #'+ id +' finished: Some tests failed with format "'+ format +'"! Time to train harder.\n\n';
    }
};

async function runAllTests() {
    let err
    await runTests(1, 'json', 'JSON', {
        indexes: {id: 'number', name: 'string'},
        v8: false,
        compress: false,
        compressIndex: false
    }).catch(e => err = e)
    await runTests(2, 'v8', 'V8 serialization', {
        indexes: {id: 'number', name: 'string'},
        v8: true,
        compress: false,
        compressIndex: false
    }).catch(e => err = e)
    await runTests(3, 'json-compressed', 'JSON with Brotli compression', {
        indexes: {id: 'number', name: 'string'},
        v8: false,
        compress: false,
        compressIndex: true
    }).catch(e => err = e)
    process.exit(err ? 1 : 0)
}

// Run the tests
runAllTests().catch(error => console.error('Error during tests:', error));
