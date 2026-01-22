#!/usr/bin/env node

import { Database } from './dist/Database.cjs';

async function simpleTest() {
    const db = new Database('simple-test.jdb', {
        fields: {
            name: 'string',
            mediaType: 'string'
        },
        indexes: {
            mediaType: 'string'
        }
    });

    await db.init();

    // Insert test data
    await db.insert({ name: 'Test', mediaType: 'live' });
    await db.save();

    // Test find
    console.log('Testing find...');
    const findResult = await db.find({ mediaType: 'live' });
    console.log('find result:', findResult.length);

    // Test exists
    console.log('Testing exists...');
    const existsResult = await db.exists({ mediaType: 'live' });
    console.log('exists result:', existsResult);

    await db.destroy();

    const fs = await import('fs');
    try {
        if (fs.existsSync('simple-test.jdb')) fs.unlinkSync('simple-test.jdb');
        if (fs.existsSync('simple-test.idx.jdb')) fs.unlinkSync('simple-test.idx.jdb');
    } catch (e) {}
}

simpleTest().catch(console.error);