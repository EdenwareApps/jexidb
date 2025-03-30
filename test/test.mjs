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
    
    console.log('Battle #' + id + ' (' + format + ') is starting...\n');
    fs.writeFileSync(testFilePath, '', { encoding: null }); // Clear the file before starting the tests
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

    // 4. Test indexes
    const pass3 = await db.indexManager.readColumnIndex('name').has(character.name)
    console.assert(pass3, `Round 3 - INDEX: Test failed: ${character.name} is not in the index.`);
    if(pass3) console.log(`Round 3 - INDEX: Flawless Victory! ${character.name} is in the index.`);

    // 5. Test data update
    await db.update({ id: 1 }, { name: character.name + ' Updated' });
    results = await db.query({ id: 1 });
    const pass4 = results.length === 1 && results[0].name === character.name + ' Updated';
    console.assert(pass4, `Round 3 - UPDATE: Test failed: ${character.updateMessage}`);
    if(pass4) console.log(`Round 3 - UPDATE: Flawless Victory! ${character.name} has been updated successfully.`);

    // 6. Test data deletion
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
        const { size } = await fs.promises.stat(testFilePath);
        if(!benchmarks[format]) {
            benchmarks[format] = { elapsed, size }
        } else {
            benchmarks[format].elapsed = (elapsed + benchmarks[format].elapsed)
            benchmarks[format].size = (size + benchmarks[format].size)
        }
        console.log(`\nBattle #${id} ended: All tests with format "${format}" ran successfully! Fatality avoided this time.\n\n`);
        global.gc()
    } else {
        benchmarks[format] = { elapsed: 'Error', size: 'Error' };
        global.gc();
        throw `\nBattle #${id} ended: Some tests failed with format "${format}"! Time to train harder.\n\n`;
    }
}

async function runAllTests() {
    const depth = 10
    let err, i = 1
    let tests = [
        ['json', 'JSON', { indexes: { id: 'number', name: 'string' }, v8: false, compress: false, compressIndex: false }],
        ['v8', 'V8 serialization', { indexes: { id: 'number', name: 'string' }, v8: true, compress: false, compressIndex: false }],
        ['json-compressed', 'JSON with Brotli compression', { indexes: { id: 'number', name: 'string' }, v8: false, compress: false, compressIndex: true }],
        ['v8-compressed', 'V8 with Deflate compression', { indexes: { id: 'number', name: 'string' }, v8: true, compress: false, compressIndex: true }]
    ]
    tests = Array(depth).fill(tests).flat()
    tests = tests.map(value => ({ value, sort: Math.random() })).sort((a, b) => a.sort - b.sort).map(({ value }) => value)
    for(const test of tests) {
        await runTests(i++, test[0], test[1], test[2]).catch(e => {
            benchmarks[test[1]] = { elapsed: 'Error', size: 'Error' };
            console.error(e)
            err = e
        })
    }
    const winners = {}
    for (const [format, result] of Object.entries(benchmarks)) {
        if (result.elapsed !== 'Error' && result.size !== 'Error') {
            if (typeof(winners.elapsed) === 'undefined' || result.elapsed < winners.elapsed) {
                winners.elapsed = result.elapsed
                winners.format = format
            }
            if (typeof(winners.size) === 'undefined' || result.size < winners.size) {
                winners.size = result.size
                winners.format = format
            }
        }
    }
    for(const format in benchmarks) {
        for(const prop of ['elapsed', 'size']) {
            if(benchmarks[format][prop] === winners[prop]) {
                benchmarks[format][prop] += ' \uD83C\uDFC6'
            }
        }
    }
    console.log('Benchmarks results after '+ tests.length +' battles:')
    console.table(benchmarks)
    // setInterval(() => {}, 1000)
    // global.Database = Database
    // global.__dirname = __dirname
    process.exit(err ? 1 : 0)
}

// Run the tests
runAllTests().catch(error => console.error('Error during tests:', error));
