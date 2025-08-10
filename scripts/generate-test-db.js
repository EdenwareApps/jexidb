const Database = require('../src/index');
const fs = require('fs').promises;
const path = require('path');

/**
 * Test Database Generator for JexiDB
 * Generates comprehensive test data for database testing
 */

// Sample data generators
const sampleData = {
  // User data
  users: [
    { id: 1, name: 'John Doe', email: 'john@example.com', age: 30, city: 'New York', active: true, role: 'admin' },
    { id: 2, name: 'Jane Smith', email: 'jane@example.com', age: 25, city: 'Los Angeles', active: true, role: 'user' },
    { id: 3, name: 'Bob Johnson', email: 'bob@example.com', age: 35, city: 'Chicago', active: false, role: 'user' },
    { id: 4, name: 'Alice Brown', email: 'alice@example.com', age: 28, city: 'Houston', active: true, role: 'moderator' },
    { id: 5, name: 'Charlie Wilson', email: 'charlie@example.com', age: 32, city: 'Phoenix', active: true, role: 'user' },
    { id: 6, name: 'Diana Davis', email: 'diana@example.com', age: 27, city: 'Philadelphia', active: false, role: 'user' },
    { id: 7, name: 'Edward Miller', email: 'edward@example.com', age: 40, city: 'San Antonio', active: true, role: 'admin' },
    { id: 8, name: 'Fiona Garcia', email: 'fiona@example.com', age: 29, city: 'San Diego', active: true, role: 'user' },
    { id: 9, name: 'George Martinez', email: 'george@example.com', age: 33, city: 'Dallas', active: true, role: 'moderator' },
    { id: 10, name: 'Helen Rodriguez', email: 'helen@example.com', age: 31, city: 'San Jose', active: false, role: 'user' }
  ],

  // Product data
  products: [
    { id: 1, name: 'Laptop', category: 'Electronics', price: 999.99, stock: 50, rating: 4.5, tags: ['computer', 'portable'] },
    { id: 2, name: 'Smartphone', category: 'Electronics', price: 699.99, stock: 100, rating: 4.2, tags: ['mobile', 'communication'] },
    { id: 3, name: 'Coffee Maker', category: 'Home', price: 89.99, stock: 25, rating: 4.0, tags: ['kitchen', 'appliance'] },
    { id: 4, name: 'Running Shoes', category: 'Sports', price: 129.99, stock: 75, rating: 4.7, tags: ['footwear', 'fitness'] },
    { id: 5, name: 'Backpack', category: 'Travel', price: 59.99, stock: 40, rating: 4.3, tags: ['bag', 'outdoor'] },
    { id: 6, name: 'Wireless Headphones', category: 'Electronics', price: 199.99, stock: 30, rating: 4.6, tags: ['audio', 'wireless'] },
    { id: 7, name: 'Yoga Mat', category: 'Sports', price: 29.99, stock: 60, rating: 4.1, tags: ['fitness', 'exercise'] },
    { id: 8, name: 'Blender', category: 'Home', price: 79.99, stock: 35, rating: 4.4, tags: ['kitchen', 'appliance'] },
    { id: 9, name: 'Tablet', category: 'Electronics', price: 399.99, stock: 45, rating: 4.3, tags: ['computer', 'portable'] },
    { id: 10, name: 'Water Bottle', category: 'Sports', price: 19.99, stock: 120, rating: 4.8, tags: ['hydration', 'fitness'] }
  ],

  // Order data
  orders: [
    { id: 1, userId: 1, productId: 1, quantity: 1, total: 999.99, status: 'completed', date: '2024-01-15' },
    { id: 2, userId: 2, productId: 2, quantity: 1, total: 699.99, status: 'shipped', date: '2024-01-16' },
    { id: 3, userId: 3, productId: 3, quantity: 2, total: 179.98, status: 'pending', date: '2024-01-17' },
    { id: 4, userId: 4, productId: 4, quantity: 1, total: 129.99, status: 'completed', date: '2024-01-18' },
    { id: 5, userId: 5, productId: 5, quantity: 1, total: 59.99, status: 'cancelled', date: '2024-01-19' },
    { id: 6, userId: 1, productId: 6, quantity: 1, total: 199.99, status: 'completed', date: '2024-01-20' },
    { id: 7, userId: 2, productId: 7, quantity: 1, total: 29.99, status: 'shipped', date: '2024-01-21' },
    { id: 8, userId: 3, productId: 8, quantity: 1, total: 79.99, status: 'pending', date: '2024-01-22' },
    { id: 9, userId: 4, productId: 9, quantity: 1, total: 399.99, status: 'completed', date: '2024-01-23' },
    { id: 10, userId: 5, productId: 10, quantity: 3, total: 59.97, status: 'shipped', date: '2024-01-24' }
  ],

  // Blog posts with nested data
  posts: [
    {
      id: 1,
      title: 'Getting Started with JexiDB',
      author: 'John Doe',
              content: 'JexiDB is a robust JSONL database system...',
      tags: ['database', 'tutorial'],
      metadata: {
        views: 1250,
        likes: 45,
        published: true,
        category: 'Technology'
      },
      comments: [
        { id: 1, user: 'Alice', text: 'Great tutorial!', date: '2024-01-15' },
        { id: 2, user: 'Bob', text: 'Very helpful', date: '2024-01-16' }
      ]
    },
    {
      id: 2,
      title: 'Advanced Querying Techniques',
      author: 'Jane Smith',
      content: 'Learn advanced querying patterns...',
      tags: ['database', 'advanced'],
      metadata: {
        views: 890,
        likes: 32,
        published: true,
        category: 'Technology'
      },
      comments: [
        { id: 3, user: 'Charlie', text: 'Excellent content', date: '2024-01-17' }
      ]
    },
    {
      id: 3,
      title: 'Performance Optimization Tips',
      author: 'Bob Johnson',
      content: 'Optimize your database performance...',
      tags: ['performance', 'optimization'],
      metadata: {
        views: 2100,
        likes: 78,
        published: true,
        category: 'Technology'
      },
      comments: []
    }
  ],

  // Complex nested data for testing
  complexData: [
    {
      id: 1,
      name: 'Project Alpha',
      status: 'active',
      team: {
        lead: 'John Doe',
        members: ['Jane Smith', 'Bob Johnson', 'Alice Brown'],
        departments: {
          engineering: ['John Doe', 'Bob Johnson'],
          design: ['Jane Smith'],
          marketing: ['Alice Brown']
        }
      },
      milestones: [
        { id: 1, name: 'Planning', completed: true, date: '2024-01-01' },
        { id: 2, name: 'Development', completed: false, date: '2024-02-01' },
        { id: 3, name: 'Testing', completed: false, date: '2024-03-01' }
      ],
      budget: {
        allocated: 50000,
        spent: 25000,
        currency: 'USD'
      }
    },
    {
      id: 2,
      name: 'Project Beta',
      status: 'completed',
      team: {
        lead: 'Jane Smith',
        members: ['Charlie Wilson', 'Diana Davis'],
        departments: {
          engineering: ['Charlie Wilson'],
          design: ['Diana Davis']
        }
      },
      milestones: [
        { id: 1, name: 'Planning', completed: true, date: '2023-11-01' },
        { id: 2, name: 'Development', completed: true, date: '2023-12-01' },
        { id: 3, name: 'Testing', completed: true, date: '2023-12-15' }
      ],
      budget: {
        allocated: 30000,
        spent: 28000,
        currency: 'USD'
      }
    }
  ]
};

/**
 * Generates a test database with comprehensive sample data
 */
async function generateTestDatabase(dbPath = './test-database.jdb') {
  console.log('🚀 Generating test database...');
  
  try {
    // Create database with indexes for common fields
          const db = new Database(dbPath, {
      indexes: {
        id: 'number',
        email: 'string',
        name: 'string',
        category: 'string',
        status: 'string',
        userId: 'number',
        productId: 'number',
        age: 'number',
        price: 'number',
        total: 'number'
      },
      autoSave: true,
      validateOnInit: false
    });

    await db.init();
    console.log('✅ Database initialized');

    // Insert users
    console.log('📝 Inserting users...');
    for (const user of sampleData.users) {
      await db.insert(user);
    }
    console.log(`✅ Inserted ${sampleData.users.length} users`);

    // Insert products
    console.log('📝 Inserting products...');
    for (const product of sampleData.products) {
      await db.insert(product);
    }
    console.log(`✅ Inserted ${sampleData.products.length} products`);

    // Insert orders
    console.log('📝 Inserting orders...');
    for (const order of sampleData.orders) {
      await db.insert(order);
    }
    console.log(`✅ Inserted ${sampleData.orders.length} orders`);

    // Insert blog posts
    console.log('📝 Inserting blog posts...');
    for (const post of sampleData.posts) {
      await db.insert(post);
    }
    console.log(`✅ Inserted ${sampleData.posts.length} blog posts`);

    // Insert complex data
    console.log('📝 Inserting complex data...');
    for (const item of sampleData.complexData) {
      await db.insert(item);
    }
    console.log(`✅ Inserted ${sampleData.complexData.length} complex items`);

    // Save database
    await db.save();
    console.log('💾 Database saved');

    // Get statistics
    const stats = await db.getStats();
    console.log('\n📊 Database Statistics:');
    console.log(`Total records: ${stats.totalRecords}`);
    console.log(`File size: ${(stats.fileSize / 1024).toFixed(2)} KB`);
    console.log(`Indexes: ${Object.keys(stats.indexes).join(', ')}`);

    // Validate integrity
    console.log('\n🔍 Validating database integrity...');
    const integrity = await db.validateIntegrity({ verbose: false });
    if (integrity.isValid) {
      console.log('✅ Database integrity validated');
    } else {
      console.log('⚠️ Integrity issues found:', integrity.errors);
    }

    // Test some queries
    console.log('\n🧪 Testing sample queries...');
    
    // Find users by age range
    const youngUsers = await db.find({ age: { '<': 30 } });
    console.log(`Users under 30: ${youngUsers.length}`);

    // Find products by category
    const electronics = await db.find({ category: 'Electronics' });
    console.log(`Electronics products: ${electronics.length}`);

    // Find orders by status
    const completedOrders = await db.find({ status: 'completed' });
    console.log(`Completed orders: ${completedOrders.length}`);

    // Find posts with nested query
    const popularPosts = await db.find({ 'metadata.views': { '>': 1000 } });
    console.log(`Popular posts (>1000 views): ${popularPosts.length}`);

    // Find complex data with nested query
    const activeProjects = await db.find({ status: 'active' });
    console.log(`Active projects: ${activeProjects.length}`);

    // Don't destroy the database - keep the files for testing
    console.log('\n🎉 Test database generated successfully!');
    console.log(`📁 Database file: ${dbPath}`);
    console.log('💡 Database files preserved for testing (not destroyed)');
    
    return {
      success: true,
      dbPath,
      totalRecords: stats.totalRecords,
      fileSize: stats.fileSize
    };

  } catch (error) {
    console.error('❌ Error generating test database:', error.message);
    process.exit(1);
  }
}

/**
 * Generates multiple test databases for different scenarios
 */
async function generateMultipleTestDatabases() {
  const databases = [
    { name: 'users', path: './test-users.jdb', data: sampleData.users },
    { name: 'products', path: './test-products.jdb', data: sampleData.products },
    { name: 'orders', path: './test-orders.jdb', data: sampleData.orders },
    { name: 'posts', path: './test-posts.jdb', data: sampleData.posts },
    { name: 'complex', path: './test-complex.jdb', data: sampleData.complexData }
  ];

  console.log('🚀 Generating multiple test databases...\n');

  for (const dbConfig of databases) {
    console.log(`📝 Creating ${dbConfig.name} database...`);
    
          const db = new Database(dbConfig.path, {
      indexes: { id: 'number' },
      autoSave: true
    });

    await db.init();
    
    for (const item of dbConfig.data) {
      await db.insert(item);
    }
    
    await db.save();
    const stats = await db.getStats();
    // Don't destroy - keep files for testing
    
    console.log(`✅ ${dbConfig.name}: ${stats.totalRecords} records, ${(stats.fileSize / 1024).toFixed(2)} KB`);
  }

  console.log('\n🎉 All test databases generated successfully!');
}

// Main execution
async function main() {
  const args = process.argv.slice(2);
  
  if (args.includes('--multiple') || args.includes('-m')) {
    await generateMultipleTestDatabases();
  } else {
    const dbPath = args[0] || './test-database.jdb';
    await generateTestDatabase(dbPath);
  }
}

// Run if called directly
if (require.main === module) {
  main().catch(error => {
    console.error('❌ Fatal error:', error.message);
    process.exit(1);
  });
}

module.exports = {
  generateTestDatabase,
  generateMultipleTestDatabases,
  sampleData
}; 