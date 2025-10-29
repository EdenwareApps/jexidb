# JexiDB Examples

This document provides practical examples of using JexiDB in real-world scenarios.

## Table of Contents

1. [Basic Usage](#basic-usage)
2. [User Management System](#user-management-system)
3. [Product Catalog](#product-catalog)
4. [Blog System](#blog-system)
5. [Analytics Dashboard](#analytics-dashboard)
6. [Performance Optimization](#performance-optimization)

## Basic Usage

### Simple Todo List

```javascript
import { Database } from 'jexidb'

const todos = new Database('todos.jdb', {
  fields: {                    // REQUIRED - Define schema
    id: 'number',
    title: 'string',
    completed: 'boolean',
    priority: 'string'
  },
  indexes: {                    // OPTIONAL - Only fields you query frequently
    completed: 'boolean',       // ✅ Filter by completion status
    priority: 'string'          // ✅ Filter by priority level
  }
})

await todos.init()

// Add todos
await todos.insert({ id: 1, title: 'Learn JexiDB', completed: false, priority: 'high' })
await todos.insert({ id: 2, title: 'Build app', completed: false, priority: 'medium' })
await todos.insert({ id: 3, title: 'Deploy app', completed: true, priority: 'low' })

// Query todos
const pending = await todos.find({ completed: false })
const highPriority = await todos.find({ priority: 'high' })

// Update todo
await todos.update({ id: 1 }, { completed: true })

// Save changes
await todos.save()

// Clean up
await todos.destroy()
```

## User Management System

### User Registration and Authentication

```javascript
import { Database } from 'jexidb'
import bcrypt from 'bcrypt'

const users = new Database('users.jdb', {
  fields: {                    // REQUIRED - Define schema
    id: 'number',
    email: 'string',
    username: 'string',
    role: 'string',
    createdAt: 'number'
  },
  indexes: {                    // OPTIONAL - Only fields you query frequently
    email: 'string',           // ✅ Login queries
    role: 'string'             // ✅ Filter by user role
  }
})

await users.init()

// Register user
async function registerUser(userData) {
  const hashedPassword = await bcrypt.hash(userData.password, 10)
  
  await users.insert({
    id: Date.now(),
    email: userData.email,
    username: userData.username,
    password: hashedPassword,
    role: 'user',
    createdAt: Date.now(),
    profile: {
      firstName: userData.firstName,
      lastName: userData.lastName,
      avatar: userData.avatar
    }
  })
  
  await users.save()
}

// Find user by email
async function findUserByEmail(email) {
  return await users.findOne({ email })
}

// Get users by role
async function getUsersByRole(role) {
  return await users.find({ role })
}

// Update user profile
async function updateProfile(userId, updates) {
  await users.update({ id: userId }, { profile: updates })
  await users.save()
}

// Example usage
await registerUser({
  email: 'john@example.com',
  username: 'john_doe',
  password: 'secure123',
  firstName: 'John',
  lastName: 'Doe'
})

const user = await findUserByEmail('john@example.com')
console.log(user.username) // 'john_doe'
```

## Product Catalog

### E-commerce Product Management

```javascript
import { Database } from 'jexidb'

const products = new Database('products.jdb', {
  fields: {                    // REQUIRED - Define schema
    id: 'string',
    name: 'string',
    category: 'string',
    price: 'number',
    inStock: 'boolean',
    rating: 'number',
    tags: 'array:string',
    features: 'array:string',
    categories: 'array:string'
  },
  indexes: {                    // OPTIONAL - Only fields you query frequently
    name: 'string',            // ✅ Search by product name
    category: 'string',         // ✅ Filter by category
    price: 'number',           // ✅ Filter by price range
    inStock: 'boolean',        // ✅ Filter by stock status
    tags: 'array:string'       // ✅ Search by tags
  }
  // termMapping is now auto-enabled for array:string fields
})

await products.init()

// Add products
const productData = [
  {
    id: 'prod-001',
    name: 'Wireless Headphones',
    category: 'electronics',
    price: 99.99,
    inStock: true,
    rating: 4.5,
    tags: ['wireless', 'audio', 'bluetooth'],
    features: ['noise-canceling', 'battery-life', 'comfort'],
    description: 'High-quality wireless headphones with noise cancellation'
  },
  {
    id: 'prod-002', 
    name: 'Smart Watch',
    category: 'electronics',
    price: 199.99,
    inStock: true,
    rating: 4.2,
    tags: ['smart', 'fitness', 'watch'],
    features: ['heart-rate', 'gps', 'waterproof'],
    description: 'Advanced smartwatch with health monitoring'
  }
]

for (const product of productData) {
  await products.insert(product)
}

// Search products
async function searchProducts(query) {
  return await products.find({
    $or: [
      { name: { $regex: query, $options: 'i' } },
      { tags: query },
      { category: query }
    ]
  })
}

// Get products by price range
async function getProductsByPrice(min, max) {
  return await products.find({
    price: { '>=': min, '<=': max },
    inStock: true
  })
}

// Update product stock
async function updateStock(productId, newStock) {
  await products.update({ id: productId }, { 
    inStock: newStock > 0,
    stockCount: newStock 
  })
}

// Bulk price update
async function updatePrices(category, discountPercent) {
  for await (const product of products.iterate({ category })) {
    product.price = Math.round(product.price * (1 - discountPercent/100) * 100) / 100
    product.lastPriceUpdate = Date.now()
  }
  await products.save()
}

// Example usage
const searchResults = await searchProducts('wireless')
const affordableProducts = await getProductsByPrice(50, 150)
await updatePrices('electronics', 10) // 10% discount
```

## Blog System

### Blog Posts with Comments

```javascript
import { Database } from 'jexidb'

const blog = new Database('blog.jdb', {
  fields: {                    // REQUIRED - Define schema
    id: 'string',
    title: 'string',
    author: 'string',
    published: 'boolean',
    createdAt: 'number',
    category: 'string',
    tags: 'array:string',
    keywords: 'array:string'
  },
  indexes: {                    // OPTIONAL - Only fields you query frequently
    category: 'string',        // ✅ Filter by category
    published: 'boolean',       // ✅ Filter published posts
    title: 'string',           // ✅ Search by title
    tags: 'array:string',      // ✅ Search by tags
    keywords: 'array:string'   // ✅ Search by keywords
  }
  // termMapping is now auto-enabled for array:string fields
})

await blog.init()

// Create blog post
async function createPost(postData) {
  const post = {
    id: `post-${Date.now()}`,
    title: postData.title,
    content: postData.content,
    author: postData.author,
    tags: postData.tags || [],
    keywords: postData.keywords || [],
    category: postData.category,
    published: false,
    createdAt: Date.now(),
    updatedAt: Date.now(),
    views: 0,
    likes: 0,
    comments: []
  }
  
  await blog.insert(post)
  await blog.save()
  return post.id
}

// Publish post
async function publishPost(postId) {
  await blog.update({ id: postId }, {
    published: true,
    publishedAt: Date.now()
  })
  await blog.save()
}

// Add comment
async function addComment(postId, comment) {
  const post = await blog.findOne({ id: postId })
  if (post) {
    const newComment = {
      id: `comment-${Date.now()}`,
      author: comment.author,
      content: comment.content,
      createdAt: Date.now(),
      likes: 0
    }
    
    post.comments.push(newComment)
    await blog.update({ id: postId }, { comments: post.comments })
    await blog.save()
  }
}

// Get posts by category
async function getPostsByCategory(category) {
  return await blog.find({ 
    category,
    published: true 
  })
}

// Search posts
async function searchPosts(query) {
  return await blog.find({
    $and: [
      { published: true },
      {
        $or: [
          { title: { $regex: query, $options: 'i' } },
          { tags: query },
          { keywords: query }
        ]
      }
    ]
  })
}

// Update view count
async function incrementViews(postId) {
  await blog.update({ id: postId }, { 
    views: { $inc: 1 } 
  })
}

// Example usage
const postId = await createPost({
  title: 'Getting Started with JexiDB',
  content: 'JexiDB is a powerful...',
  author: 'john_doe',
  tags: ['database', 'javascript', 'tutorial'],
  keywords: ['jexidb', 'nosql', 'nodejs'],
  category: 'tutorial'
})

await publishPost(postId)
await addComment(postId, {
  author: 'jane_smith',
  content: 'Great tutorial! Very helpful.'
})

const tutorialPosts = await getPostsByCategory('tutorial')
```

## Analytics Dashboard

### Data Analytics with Aggregations

```javascript
import { Database } from 'jexidb'

const analytics = new Database('analytics.jdb', {
  fields: {                    // REQUIRED - Define schema
    id: 'string',
    event: 'string',
    userId: 'string',
    timestamp: 'number',
    category: 'string'
  },
  indexes: {                    // OPTIONAL - Only fields you query frequently
    timestamp: 'number',       // ✅ Filter by date range
    userId: 'string',         // ✅ Filter by user
    event: 'string',          // ✅ Filter by event type
    category: 'string'        // ✅ Filter by category
  }
})

await analytics.init()

// Track events
async function trackEvent(eventData) {
  await analytics.insert({
    id: `event-${Date.now()}-${Math.random()}`,
    event: eventData.event,
    userId: eventData.userId,
    timestamp: Date.now(),
    category: eventData.category,
    properties: eventData.properties || {}
  })
}

// Get daily statistics
async function getDailyStats(date) {
  const startOfDay = new Date(date).setHours(0, 0, 0, 0)
  const endOfDay = new Date(date).setHours(23, 59, 59, 999)
  
  const events = await analytics.find({
    timestamp: { '>=': startOfDay, '<=': endOfDay }
  })
  
  // Aggregate data
  const stats = {
    totalEvents: events.length,
    uniqueUsers: new Set(events.map(e => e.userId)).size,
    eventsByCategory: {},
    eventsByType: {}
  }
  
  events.forEach(event => {
    // Count by category
    stats.eventsByCategory[event.category] = 
      (stats.eventsByCategory[event.category] || 0) + 1
    
    // Count by event type
    stats.eventsByType[event.event] = 
      (stats.eventsByType[event.event] || 0) + 1
  })
  
  return stats
}

// Get user activity
async function getUserActivity(userId, days = 30) {
  const since = Date.now() - (days * 24 * 60 * 60 * 1000)
  
  return await analytics.find({
    userId,
    timestamp: { '>=': since }
  })
}

// Top performing content
async function getTopContent(limit = 10) {
  const events = await analytics.find({
    event: 'content_view'
  })
  
  const contentViews = {}
  events.forEach(event => {
    const contentId = event.properties.contentId
    if (contentId) {
      contentViews[contentId] = (contentViews[contentId] || 0) + 1
    }
  })
  
  return Object.entries(contentViews)
    .sort(([,a], [,b]) => b - a)
    .slice(0, limit)
    .map(([contentId, views]) => ({ contentId, views }))
}

// Example usage
await trackEvent({
  event: 'page_view',
  userId: 'user123',
  category: 'navigation',
  properties: { page: '/dashboard', referrer: 'google.com' }
})

const todayStats = await getDailyStats(new Date())
const userActivity = await getUserActivity('user123', 7)
const topContent = await getTopContent(5)
```

## Performance Optimization

### Large Dataset Handling

```javascript
import { Database } from 'jexidb'

const largeDB = new Database('large-dataset.jdb', {
  fields: {                    // REQUIRED - Define schema
    id: 'number',
    type: 'string',
    status: 'string',
    tags: 'array:string',
    categories: 'array:string'
  },
  indexes: {                    // OPTIONAL - Only fields you query frequently
    status: 'string',          // ✅ Filter by status
    type: 'string',            // ✅ Filter by type
    tags: 'array:string'       // ✅ Search by tags
  }
  // termMapping is now auto-enabled for array:string fields
})

await largeDB.init()

// Bulk insert with progress tracking
async function bulkInsert(data, batchSize = 1000) {
  console.log(`Inserting ${data.length} records...`)
  
  for (let i = 0; i < data.length; i += batchSize) {
    const batch = data.slice(i, i + batchSize)
    
    for (const record of batch) {
      await largeDB.insert(record)
    }
    
    if (i % (batchSize * 10) === 0) {
      console.log(`Processed ${i + batch.length} records`)
    }
  }
  
  await largeDB.save()
  console.log('Bulk insert completed')
}

// Bulk update with iterate()
async function bulkUpdateStatus(oldStatus, newStatus) {
  console.log(`Updating status from ${oldStatus} to ${newStatus}`)
  
  let processed = 0
  let modified = 0
  
  for await (const record of largeDB.iterate({ status: oldStatus })) {
    record.status = newStatus
    record.updatedAt = Date.now()
    
    processed++
    if (processed % 1000 === 0) {
      console.log(`Processed ${processed} records, modified ${modified}`)
    }
  }
  
  await largeDB.save()
  console.log(`Bulk update completed: ${processed} processed, ${modified} modified`)
}

// Memory-efficient querying
async function getPaginatedResults(criteria, page = 1, limit = 100) {
  const skip = (page - 1) * limit
  
  // Use walk() for memory efficiency
  const results = []
  let count = 0
  
  for await (const record of largeDB.walk()) {
    // Apply criteria filtering
    if (matchesCriteria(record, criteria)) {
      if (count >= skip && results.length < limit) {
        results.push(record)
      }
      count++
      
      if (results.length >= limit) break
    }
  }
  
  return {
    data: results,
    total: count,
    page,
    limit,
    hasMore: count > skip + limit
  }
}

// Helper function for criteria matching
function matchesCriteria(record, criteria) {
  for (const [key, value] of Object.entries(criteria)) {
    if (typeof value === 'object' && value !== null) {
      // Handle operators like { '>': 10 }
      for (const [op, opValue] of Object.entries(value)) {
        if (!evaluateOperator(record[key], op, opValue)) {
          return false
        }
      }
    } else if (record[key] !== value) {
      return false
    }
  }
  return true
}

function evaluateOperator(recordValue, operator, criteriaValue) {
  switch (operator) {
    case '>': return recordValue > criteriaValue
    case '>=': return recordValue >= criteriaValue
    case '<': return recordValue < criteriaValue
    case '<=': return recordValue <= criteriaValue
    case '!=': return recordValue !== criteriaValue
    case '$in': return criteriaValue.includes(recordValue)
    default: return recordValue === criteriaValue
  }
}

// Example usage
const largeDataset = Array.from({ length: 100000 }, (_, i) => ({
  id: i + 1,
  type: ['A', 'B', 'C'][i % 3],
  status: ['active', 'inactive', 'pending'][i % 3],
  tags: [`tag${i % 10}`, `category${i % 5}`],
  createdAt: Date.now() - (i * 1000)
}))

await bulkInsert(largeDataset)
await bulkUpdateStatus('pending', 'active')

const page1 = await getPaginatedResults({ type: 'A' }, 1, 50)
console.log(`Page 1: ${page1.data.length} results, ${page1.total} total`)
```

## Error Handling Examples

### Robust Error Handling

```javascript
import { Database } from 'jexidb'

async function robustDatabaseOperations() {
  const db = new Database('robust-db.jdb', {
    fields: {                    // REQUIRED - Define schema
      id: 'number', 
      name: 'string'
    },
    indexes: {                    // OPTIONAL - Only fields you query frequently
      name: 'string'             // ✅ Search by name
    }
  })
  
  try {
    await db.init()
    
    // Insert with validation
    try {
      await db.insert({ id: 1, name: 'Test' })
      await db.save()
      console.log('Insert successful')
    } catch (insertError) {
      console.error('Insert failed:', insertError.message)
      // Handle insert error (e.g., duplicate ID)
    }
    
    // Query with fallback
    try {
      const results = await db.find({ name: 'Test' })
      console.log('Query successful:', results.length, 'results')
    } catch (queryError) {
      console.error('Query failed:', queryError.message)
      // Return empty results or cached data
    }
    
  } catch (initError) {
    console.error('Database initialization failed:', initError.message)
    // Handle initialization error
  } finally {
    try {
      await db.destroy()
    } catch (destroyError) {
      console.error('Database cleanup failed:', destroyError.message)
    }
  }
}

// Retry mechanism
async function retryOperation(operation, maxRetries = 3) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await operation()
    } catch (error) {
      console.log(`Attempt ${attempt} failed:`, error.message)
      
      if (attempt === maxRetries) {
        throw new Error(`Operation failed after ${maxRetries} attempts: ${error.message}`)
      }
      
      // Wait before retry (exponential backoff)
      await new Promise(resolve => setTimeout(resolve, Math.pow(2, attempt) * 1000))
    }
  }
}

// Example usage with retry
const db = new Database('retry-db.jdb', {
  fields: {                    // REQUIRED - Define schema
    id: 'number',
    data: 'string'
  }
})
await db.init()

try {
  await retryOperation(async () => {
    await db.insert({ id: Date.now(), data: 'important' })
    await db.save()
  })
} catch (error) {
  console.error('All retry attempts failed:', error.message)
}
```

These examples demonstrate various real-world use cases for JexiDB, from simple todo lists to complex analytics systems. Each example includes error handling and performance considerations.
