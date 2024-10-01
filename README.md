<p align="center">
  <img width="270" src="https://edenware.app/jexidb/images/jexidb-logo-icon.jpg" alt="JexiDB logo" title="JexiDB logo" />
</p>

## Overview

JexiDB is a lightweight, standalone JavaScript database manager that stores data on disk using JSON format. It supports advanced indexing and querying capabilities to efficiently handle data operations.

## Installation

To install JexiDB, you can use npm:

```bash
npm install EdenwareApps/jexidb
```

## Usage

### Creating a Database Instance

To create a new instance of the database, you need to provide a file path where the database will be stored and an optional configuration object for indexes.

```javascript
import { Database } from 'jexidb';

const db = new Database('path/to/database.jdb', {
  indexes: {
    id: 'number',
    name: 'string'
  }
});
```

### Initializing the Database

Before using the database, you need to initialize it. This will load the existing data and indexes from the file.

```javascript
await db.init();
```

### Inserting Data

You can insert data into the database by using the `insert` method. The data should be an object that matches the defined indexes.

```javascript
await db.insert({ id: 1, name: 'John Doe' });
await db.insert({ id: 2, name: 'Jane Doe' });
```

### Querying Data

The `query` method allows you to retrieve data based on specific criteria. You can specify criteria for multiple fields.

```javascript
const results = await db.query({ name: 'John Doe' });
console.log(results); // [{ id: 1, name: 'John Doe' }]
```

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
