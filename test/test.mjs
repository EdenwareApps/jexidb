import fs from 'fs';
import path from 'path';
import { Database } from '../src/Database.mjs';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const benchmarks = {}

// Array of objects containing the name and specific messages for each character
const characters = [
    {
        name: 'Scorpion',
        missingMessage: 'Did Scorpion pull a "GET OVER HERE" on the missing entries?',
        updateMessage: 'Scorpion refuses to update. Maybe he’s stuck in the Netherrealm?',
        deleteMessage: 'I thought Scorpion was gone, but he’s still here! Must be that "Hellfire Resurrection."',
        signatureMove: 'Spear', powerType: 'Hellfire'
    },
    {
        name: 'Scarlet',
        missingMessage: 'Did Scarlet drain the life from the missing entries with her blood magic?',
        updateMessage: 'Scarlet refuses to update. Maybe her blood magic is causing interference?',
        deleteMessage: 'I thought Scarlet was gone, but she’s still here! Must be that blood regeneration ability.',
        signatureMove: 'Blood Tentacle', powerType: 'Blood Manipulation'
    },
    {
        name: 'Frost',
        missingMessage: 'Did Frost freeze the missing entries?',
        updateMessage: 'Frost refuses to update. Maybe she’s stuck in an ice block?',
        deleteMessage: 'I thought Frost was gone, but she’s still here! Must be that "Ice Shield."',
        signatureMove: 'Ice Daggers', 
        powerType: 'Cryomancy'
    },
    {
        name: 'Frost',
        missingMessage: 'Did Frost freeze the missing entries?',
        updateMessage: 'Frost refuses to update. Maybe she’s stuck in an ice block?',
        deleteMessage: 'I thought Frost was gone, but she’s still here! Must be that "Ice Shield."',
        signatureMove: 'Ice Daggers', 
        powerType: 'Cryomancy'
    }    
];

// Function to run the tests
const runTests = async (id, name, format, opts) => {

    // Define the character for this battle based on the battle ID
    const character = characters[(id - 1) % characters.length];

    // Path to the test file
    const testFilePath = path.join(__dirname, 'test-' + name + '.jdb');
    
    // Function to clear the test file before each run
    const clearTestFile = () => {
        fs.writeFileSync(testFilePath, '', { encoding: null });
    }

    console.log('Battle #' + id + ' (' + format + ') is starting...\n');
    clearTestFile(); // Clear the file before starting the tests
    const start = Date.now()
    const db = new Database(testFilePath, opts); // Instantiate the database

    // 1. Test if the instance was created correctly
    await db.init(); // Call init() right after the instance is created
    console.assert(db.initialized === true, `Test failed: Database didn't initialize. Looks like Raiden needs to give it a shock!`);

    // 2. Test data insertion with Mortal Kombat characters
    await db.insert({ id: 1, name: character.name, signatureMove: character.signatureMove, powerType: character.powerType });
    await db.insert({ id: 2, name: 'Sub-Zero', signatureMove: 'Ice Ball', powerType: 'Cryomancy' });
    await db.insert({ id: 3, name: 'Raiden', signatureMove: 'Electric Fly', powerType: 'Lightning' });
    await db.insert({ id: 4, name: 'Jax', signatureMove: 'Ground Pound', powerType: 'Strength' });
    await db.insert({ id: 5, name: 'Sindel', signatureMove: 'Sonic Scream', powerType: 'Sound' });
    await db.insert({ id: 6, name: 'Ermac', signatureMove: 'Telekinesis', powerType: 'Telekinesis' });
    await db.insert({ id: 7, name: 'Mileena', signatureMove: 'Sai Throw', powerType: 'Teleportation' });
    await db.insert({ id: 8, name: 'Kenshi', signatureMove: 'Telekinetic Slash', powerType: 'Telekinesis' });
    await db.insert({ id: 9, name: 'D\'Vorah', signatureMove: 'Swarm', powerType: 'Insects' });
    await db.insert({ id: 10, name: 'Sonya Blade', signatureMove: 'Energy Rings', powerType: 'Special Forces Technology' });
    await db.insert({ id: 11, name: 'Kotal Kahn', signatureMove: 'Sunstone', powerType: 'Osh-Tekk Strength' });   

    // "Flawless Victory" if the insertion is successful
    console.log('Round 1 - CREATE: Flawless Victory! All characters inserted successfully.');

    // 3. Test if the data was inserted correctly
    let results = await db.query({ id: { '<=': 5 } });
    const pass1 = results.length === 5;
    const pass2 = results[0].name === character.name;
    console.assert(pass1, `Round 2 - READ: Test failed: Where is everyone? ${character.missingMessage}`);
    console.assert(pass2, `Round 2 - READ: Test failed: ${character.name} seems to have been teleported out of the database!`);
    if(pass1 && pass2) console.log(`Round 2 - READ: Flawless Victory! All characters inserted successfully, led by ${character.name}.`);

    // 4. Test data update
    await db.update({ id: 1 }, { name: character.name + ' Updated' });
    results = await db.query({ id: 1 });
    const pass4 = results.length === 1 && results[0].name === character.name + ' Updated';
    console.assert(pass4, `Round 3 - UPDATE: Test failed: ${character.updateMessage}`);
    if(pass4) console.log(`Round 3 - UPDATE: Flawless Victory! ${character.name} has been updated successfully.`);

    // 5. Test data deletion
    await db.delete({ name: character.name + ' Updated' });
    
    results = await db.query({ id: { '<=': 2 } });
    const pass5 = results.length === 1;
    const pass6 = results[0].name === 'Sub-Zero';
    console.assert(pass5, `Round 4 - DELETE: Test failed: ${character.deleteMessage}`);
    console.assert(pass6, `Round 4 - DELETE: Test failed: Sub-Zero is nowhere to be seen. Did he freeze the system?`);
    if(pass5 && pass6) console.log(`Round 4 - DELETE: Flawless Victory! ${character.name} has been eliminated successfully.`);

    // End the battle and log the result
    if(pass1 && pass2 && pass4 && pass5 && pass6) {
        let err, elapsed = Date.now() - start;
        elapsed = elapsed < 1000 ? elapsed + 'ms' : (elapsed / 1000).toFixed(3) + 's';
        const { size } = await fs.promises.stat(testFilePath);
        benchmarks[format] = { elapsed, size };
        console.log(`\nBattle #${id} ended: All tests with format "${format}" ran successfully! Fatality avoided this time.\n\n`);
    } else {
        benchmarks[format] = { elapsed: 'Error', size: 'Error' };
        throw `\nBattle #${id} ended: Some tests failed with format "${format}"! Time to train harder.\n\n`;
    }
}

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
    await runTests(3, 'v8-compressed', 'V8 with Brotli compression', {
        indexes: {id: 'number', name: 'string'},
        v8: true,
        compress: false,
        compressIndex: true
    }).catch(e => err = e)
    console.table(benchmarks)
    process.exit(err ? 1 : 0)
}

// Run the tests
runAllTests().catch(error => console.error('Error during tests:', error));
