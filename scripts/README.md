# JexiDB Development Scripts

This directory contains essential scripts for JexiDB development, testing, and benchmarking.

## Scripts

### `generate-test-db.js`
Main script that generates comprehensive test data for JexiDB testing.

**Features:**
- Generates multiple types of sample data (users, products, orders, posts, complex data)
- Creates indexed databases for optimal performance
- Validates database integrity after generation
- Tests sample queries to ensure functionality
- Supports both single and multiple database generation

### `run-test-db.js`
Simple runner script with command-line interface.

### `extreme-benchmark.js`
Comprehensive benchmark comparing JexiDB 2.0.1 vs JexiDB 1.x performance.

### `benchmark-performance.js`
Performance testing suite for JexiDB operations.

### `cleanup-temp-files.js`
Removes temporary test files and directories.

### `generate-docs.js`
Generates comprehensive documentation.

### `verify-test-db.js`
Verifies test database integrity and functionality.

## Usage

### Using npm scripts (recommended)

```bash
# Generate a single comprehensive test database
npm run generate-test-db

# Generate multiple separate databases
npm run generate-test-db:multiple

# Generate with custom path
npm run generate-test-db -- ./my-custom-db.jsonl
```

### Using Node directly

```bash
# Generate single database
node scripts/run-test-db.js

# Generate with custom path
node scripts/run-test-db.js ./my-test-db.jsonl

# Generate multiple databases
node scripts/run-test-db.js --multiple

# Show help
node scripts/run-test-db.js --help
```

### Using the module directly

```javascript
const { generateTestDatabase, generateMultipleTestDatabases } = require('./scripts/generate-test-db');

// Generate single database
await generateTestDatabase('./my-db.jsonl');

// Generate multiple databases
await generateMultipleTestDatabases();
```

## Generated Data

The test database generator creates the following types of sample data:

### Users
- 10 user records with fields: id, name, email, age, city, active, role
- Various roles: admin, user, moderator
- Different cities and age ranges

### Products
- 10 product records with fields: id, name, category, price, stock, rating, tags
- Categories: Electronics, Home, Sports, Travel
- Price ranges and stock levels

### Orders
- 10 order records with fields: id, userId, productId, quantity, total, status, date
- Various statuses: completed, shipped, pending, cancelled
- Links users to products

### Blog Posts
- 3 blog post records with nested data
- Includes metadata (views, likes, published, category)
- Contains comments arrays
- Demonstrates nested querying capabilities

### Complex Data
- 2 project records with deeply nested structures
- Team information with departments
- Milestones arrays
- Budget objects
- Tests complex nested field operations

## Database Configuration

The generated databases include indexes for common fields:
- `id` (number)
- `email` (string)
- `name` (string)
- `category` (string)
- `status` (string)
- `userId` (number)
- `productId` (number)
- `age` (number)
- `price` (number)
- `total` (number)

## Output Files

### Single Database Mode
- `test-database.jsonl` - Main database file
- `test-database.jsonl.index` - Index file
- `test-database.jsonl.meta` - Metadata file

### Multiple Database Mode
- `test-users.jsonl` - Users database
- `test-products.jsonl` - Products database
- `test-orders.jsonl` - Orders database
- `test-posts.jsonl` - Blog posts database
- `test-complex.jsonl` - Complex data database

Each database includes its corresponding `.index` and `.meta` files.

## Testing Queries

The generator automatically tests several query types:

1. **Range queries**: `{ age: { '<': 30 } }`
2. **Exact matches**: `{ category: 'Electronics' }`
3. **Status filters**: `{ status: 'completed' }`
4. **Nested queries**: `{ 'metadata.views': { '>': 1000 } }`
5. **Complex nested**: `{ status: 'active' }`

## Example Usage in Tests

```javascript
const JexiDB = require('../src/index');

describe('Database Tests', () => {
  let db;

  beforeEach(async () => {
    db = new JexiDB('./test-database.jsonl');
    await db.init();
  });

  afterEach(async () => {
    await db.destroy();
  });

  test('should find users by age', async () => {
    const youngUsers = await db.find({ age: { '<': 30 } });
    expect(youngUsers.length).toBeGreaterThan(0);
  });

  test('should find products by category', async () => {
    const electronics = await db.find({ category: 'Electronics' });
    expect(electronics.length).toBe(4); // 4 electronics products
  });
});
```

## Notes

- All generated databases are validated for integrity
- The script includes comprehensive error handling
- Generated data is realistic and varied for thorough testing
- Comments and documentation are in English as per project requirements
- Scripts exit with proper error codes on failure 