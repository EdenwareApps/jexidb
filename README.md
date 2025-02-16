<p align="center">
  <img width="270" src="https://edenware.app/jexidb/images/jexidb-logo-icon.jpg" alt="JexiDB logo" title="JexiDB logo" />
</p>

## Overview

JexiDB is a lightweight, standalone JavaScript database manager that stores data on disk using either JSON format or V8 serialization. It supports indexing and querying capabilities for efficient data operations. Ideal for local Node.js projects as well as apps built with Electron or NW.js. Written in pure JavaScript, it requires no compilation and is compatible with both CommonJS and ESM modules.

## Installation

To install JexiDB, you can use npm:

```bash
npm install EdenwareApps/jexidb
```

## Usage

### Creating a Database Instance

To create a new instance of the database, you need to provide a file path where the database will be stored and an optional configuration object for indexes.

```javascript
// const { Database } = require('jexidb'); // commonjs

import { Database } from 'jexidb'; // ESM

const db = new Database('path/to/database.jdb', { // file will be created if it does not already exist
  v8: false, // false by default, set to true to use V8 serialization instead of JSON.
  compress: false, // set to true to compress each entry
  compressIndex: false, // set to true to compress the index only
  indexes: { // keys to use in queries, only those key values ​​are kept in memory, so fewer specified keys lead to improved performance
    id: 'number',
    name: 'string'
  }
});
```
You can [learn a bit more about these options at this link](https://github.com/EdenwareApps/jexidb/tree/main/test#readme).


### Initializing the Database

Before using the database, you need to initialize it. This will load the existing data and indexes from the file.

```javascript
await db.init();
```
Only the values ​​specified as indexes are kept in memory for faster queries. JexiDB will never load the entire file into memory.


### Inserting Data

You can insert data into the database by using the `insert` method. The data should be an object that contains the defined indexes. All object values will be saved into database.

```javascript
await db.insert({ id: 1, name: 'John Doe' });
await db.insert({ id: 2, name: 'Jane Doe', anyArbitraryField: '1' });
```

### Querying Data

The `query` method allows you to retrieve data based on specific criteria. You can specify criteria for multiple fields.

```javascript
const results = await db.query({ name: 'John Doe' }, { caseInsensitive: true });
console.log(results); // [{ id: 1, name: 'John Doe' }]
```

Note: For now the query should be limited to using the fields specified as 'indexes' when instantiating the class.

#### Querying with Conditions

You can use conditions to perform more complex queries:

```javascript
const results = await db.query({ id: { '>': 1 } });
console.log(results); // [{ id: 2, name: 'Jane Doe' }]
```

### Updating Data

To update existing records, use the `update` method with the criteria to find the records and the new data.

```javascript
await db.update({ id: 1 }, { name: 'John Smith' });
```

### Deleting Data

You can delete records that match certain criteria using the `delete` method.

```javascript
const deletedCount = await db.delete({ name: 'Jane Doe' });
console.log(`Deleted ${deletedCount} record(s).`);
```

### Iterating Through Records

You can iterate through records in the database using the `walk` method, which returns an async generator.

```javascript
for await (const record of db.walk()) {
  console.log(record);
}
```

### Saving Changes

After making any changes to the database, you need to save them using the `save` method. This will persist the changes to disk.

```javascript
await db.save();
```

## Conclusion

JexiDB provides a simple yet powerful way to store and manage data in JavaScript applications. With its indexing and querying features, you can build efficient data-driven applications.

<p align="center">
  <img width="380" src="https://edenware.app/jexidb/images/jexidb-mascot3.jpg" alt="JexiDB mascot" title="JexiDB mascot" />
</p>

# Contributing

Please, feel free to contribute to the project by opening a discussion under Issues section or sending your PR.

If you find this library useful, please consider making a donation of any amount via [PayPal by clicking here](https://www.paypal.com/donate/?item_name=megacubo.tv&cmd=_donations&business=efox.web%40gmail.com) to help the developer continue to dedicate himself to the project. ❤
