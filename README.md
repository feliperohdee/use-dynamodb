# Use DynamoDB

A TypeScript library that provides a simplified interface for interacting with Amazon DynamoDB, using the AWS SDK v3.

[![TypeScript](https://img.shields.io/badge/-TypeScript-3178C6?style=flat-square&logo=typescript&logoColor=white)](https://www.typescriptlang.org/)
[![Vitest](https://img.shields.io/badge/-Vitest-729B1B?style=flat-square&logo=vitest&logoColor=white)](https://vitest.dev/)
[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

## 🚀 Features

- ✅ Type-safe CRUD operations (Create, Read, Update, Delete)
- 🔍 Support for local and global secondary indexes (LSI & GSI)
- 📦 Batch operations with automatic chunking
- 🔎 Query and Scan operations with filtering
- 🔄 Optimistic locking with versioning
- 📄 Automatic pagination
- 🕒 Built-in timestamp management (**createdAt, **updatedAt, \_\_ts)
- 🔒 Conditional updates and transactions
- 🎯 Change tracking with callbacks
- 🔄 Custom retry strategy with exponential backoff
- 🔗 Advanced metadata attribute generation with custom transformations

## 📚 Documentation

- [Main Documentation](README.md) - Core DynamoDB wrapper functionality
- [Layer Module Documentation](README_LAYERS.md) - Event sourcing and caching capabilities

## 📦 Installation

```bash
yarn add use-dynamodb
```

## 🛠️ Usage

### Initialization

The library supports several configuration options for customizing its behavior:

#### Basic Configuration

- `accessKeyId` and `secretAccessKey`: Your AWS credentials
- `region`: AWS region for DynamoDB
- `table`: Name of your DynamoDB table
- `schema`: Defines the table's partition and sort keys
- `indexes`: Array of GSI (Global Secondary Indexes) and LSI (Local Secondary Indexes) configurations

#### Enhanced Metadata Configuration

The `metaAttributes` configuration now supports two formats for more flexible metadata generation:

1. Simple Array Format:

```typescript
metaAttributes: {
  'combined-field': ['field1', 'field2']  // Uses default joiner '#'
}
```

2. Advanced Options Format:

```typescript
metaAttributes: {
  'combined-field': {
    attributes: ['field1', 'field2'],
    joiner: '-',  // Custom joiner
    transform: (attribute: string, value: any) => string | undefined  // Optional transform function
  }
}
```

Example with both formats:

```typescript
import Dynamodb from 'use-dynamodb';

type Item = {
	pk: string;
	sk: string;
	title: string;
	category: string;
	tags: string[];
};

const db = new Dynamodb<Item>({
	accessKeyId: 'YOUR_ACCESS_KEY',
	secretAccessKey: 'YOUR_SECRET_KEY',
	region: 'us-east-1',
	table: 'YOUR_TABLE_NAME',
	schema: { partition: 'pk', sort: 'sk' },
	// Advanced metadata attribute configuration
	metaAttributes: {
		// Simple format - uses default joiner '#'
		'title-category': ['title', 'category'],

		// Advanced format with custom joiner and transform
		'searchable-tags': {
			attributes: ['tags'],
			joiner: '|',
			transform: (attribute, value) => {
				if (attribute === 'tags' && Array.isArray(value)) {
					return value.join('|').toLowerCase();
				}
				return;
			}
		}
	}
});
```

### Index Projections

The library supports configuring projections for both Global Secondary Indexes (GSI) and Local Secondary Indexes (LSI). You can specify which attributes should be projected into the index using the `projection` property:

```typescript
const db = new Dynamodb<Item>({
	// ... other config
	indexes: [
		{
			name: 'status-index',
			partition: 'status',
			sort: 'createdAt',
			projection: {
				type: 'INCLUDE', // Can be 'ALL', 'KEYS_ONLY', or 'INCLUDE'
				nonKeyAttributes: ['title', 'description'] // Required when type is 'INCLUDE'
			}
		},
		{
			name: 'category-index',
			partition: 'category',
			projection: {
				type: 'ALL' // Project all attributes
			}
		},
		{
			name: 'date-index',
			partition: 'date',
			projection: {
				type: 'KEYS_ONLY' // Only project key attributes
			}
		}
	]
});
```

The `projection` configuration supports three types:

- `ALL` - Projects all attributes from the base table
- `KEYS_ONLY` - Projects only the index and primary keys
- `INCLUDE` - Projects only the specified attributes via `nonKeyAttributes`

Using projections effectively can help optimize storage costs and improve query performance by limiting the attributes stored in secondary indexes.

The transform function allows you to:

- Modify values before they're combined
- Filter out values by returning undefined
- Apply custom formatting or normalization
- Handle different data types appropriately

### Table Operations

#### Create Table

```typescript
await db.createTable();
```

### Basic Operations

#### Put Item

```typescript
// Simple put with automatic condition to prevent overwrites
const item = await db.put({
	pk: 'user#123',
	sk: 'profile',
	foo: 'bar'
});

// Put with overwrite allowed
const overwrittenItem = await db.put(
	{
		pk: 'user#123',
		sk: 'profile',
		foo: 'baz'
	},
	{
		overwrite: true
	}
);

// Put with conditions
const conditionalItem = await db.put(
	{
		pk: 'user#123',
		sk: 'profile',
		foo: 'bar'
	},
	{
		attributeNames: { '#foo': 'foo' },
		attributeValues: { ':foo': 'bar' },
		conditionExpression: '#foo <> :foo'
	}
);
```

#### Get Item

```typescript
// Get by partition and sort key
const item = await db.get({
	item: { pk: 'user#123', sk: 'profile' }
});

// Get with specific attributes
const partialItem = await db.get({
	item: { pk: 'user#123', sk: 'profile' },
	select: ['foo']
});

// Get using query expression
const queriedItem = await db.get({
	attributeNames: { '#pk': 'pk' },
	attributeValues: { ':pk': 'user#123' },
	queryExpression: '#pk = :pk'
});

// Get last by partition and sort key
const item = await db.getLast({
	item: { pk: 'user#123', sk: 'profile' }
});
```

#### Update Item

```typescript
// Update using function
const updatedItem = await db.update({
	filter: {
		item: { pk: 'user#123', sk: 'profile' }
	},
	updateFunction: item => ({
		...item,
		foo: 'updated'
	})
});

// Update using expression
const expressionUpdatedItem = await db.update({
	filter: {
		item: { pk: 'user#123', sk: 'profile' }
	},
	attributeNames: { '#foo': 'foo' },
	attributeValues: { ':foo': 'updated' },
	updateExpression: 'SET #foo = :foo'
});

// Upsert
const upsertedItem = await db.update({
	filter: {
		item: { pk: 'user#123', sk: 'profile' }
	},
	updateFunction: item => ({
		...item,
		foo: 'new'
	}),
	upsert: true
});

// Update with partition/sort key change
const movedItem = await db.update({
	allowUpdatePartitionAndSort: true,
	filter: {
		item: { pk: 'user#123', sk: 'profile' }
	},
	updateFunction: item => ({
		...item,
		pk: 'user#124'
	})
});
```

#### Delete Item

```typescript
// Delete by key
const deletedItem = await db.delete({
	filter: {
		item: { pk: 'user#123', sk: 'profile' }
	}
});

// Delete with condition
const conditionalDelete = await db.delete({
	attributeNames: { '#foo': 'foo' },
	attributeValues: { ':foo': 'bar' },
	conditionExpression: '#foo = :foo',
	filter: {
		item: { pk: 'user#123', sk: 'profile' }
	}
});
```

### Query Operations

```typescript
// Query by partition key
const { items, count, lastEvaluatedKey } = await db.query({
	item: { pk: 'user#123' }
});

// Query with prefix matching
const prefixResults = await db.query({
	item: { pk: 'user#123', sk: 'profile#' },
	prefix: true
});

// Query with filter
const filteredResults = await db.query({
	attributeNames: { '#foo': 'foo' },
	attributeValues: { ':foo': 'bar' },
	filterExpression: '#foo = :foo',
	item: { pk: 'user#123' }
});

// Query with pagination
const paginatedResults = await db.query({
	item: { pk: 'user#123' },
	limit: 10,
	startKey: lastEvaluatedKey
});

// Query using index
const indexResults = await db.query({
	item: { gsiPk: 'status#active' },
	index: 'gs-index'
});

// Query with chunks processing
const chunkedResults = await db.query({
	item: { pk: 'user#123' },
	chunkLimit: 10,
	onChunk: async ({ items, count }) => {
		// Process items in chunks
		console.log(`Processing ${count} items`);
	}
});
```

### Scan Operations

```typescript
// Basic scan
const { items, count, lastEvaluatedKey } = await db.scan();

// Filtered scan
const filteredScan = await db.scan({
	attributeNames: { '#foo': 'foo' },
	attributeValues: { ':foo': 'bar' },
	filterExpression: '#foo = :foo'
});

// Scan with selection
const partialScan = await db.scan({
	select: ['foo', 'bar']
});

// Paginated scan
const paginatedScan = await db.scan({
	limit: 10,
	startKey: lastEvaluatedKey
});
```

### Batch Operations

```typescript
// Batch write
const items = await db.batchWrite([
	{ pk: 'user#1', sk: 'profile', foo: 'bar' },
	{ pk: 'user#2', sk: 'profile', foo: 'baz' }
]);

// Batch get
const retrievedItems = await db.batchGet([
	{ pk: 'user#1', sk: 'profile' },
	{ pk: 'user#2', sk: 'profile' }
]);

// Batch delete
const deletedItems = await db.batchDelete([
	{ pk: 'user#1', sk: 'profile' },
	{ pk: 'user#2', sk: 'profile' }
]);

// Clear table
await db.clear(); // Clear entire table
await db.clear('user#123'); // Clear by partition key
```

### Filter Operations

```typescript
// Filter is a higher-level abstraction that combines query and scan
const results = await db.filter({
	item: { pk: 'user#123' }, // Uses query
	// OR
	queryExpression: '#pk = :pk', // Uses query
	// OR
	filterExpression: '#status = :status' // Uses scan
});
```

## Types

### Key Types

```typescript
type TableSchema = {
	partition: string;
	sort?: string;
};

type TableGSI = {
	name: string;
	partition: string;
	partitionType: 'S' | 'N';
	sort?: string;
	sortType?: 'S' | 'N';
};

type TableLSI = {
	name: string;
	partition: string;
	sort?: string;
	sortType: 'S' | 'N';
};
```

### Item Types

```typescript
type Dict = Record<string, any>;

type PersistedItem<T extends Dict = Dict> = T & {
	__createdAt: string;
	__ts: number;
	__updatedAt: string;
};
```

### Change Tracking

```typescript
type ChangeType = 'PUT' | 'UPDATE' | 'DELETE';

type ChangeEvent<T extends Dict = Dict> = {
	item: PersistedItem<T>;
	partition: string;
	sort?: string | null;
	table: string;
	type: ChangeType;
};

type OnChange<T extends Dict = Dict> = (events: ChangeEvent<T>[]) => Promise<void>;
```

## 🧪 Testing

```bash
# Set environment variables
export AWS_ACCESS_KEY='YOUR_ACCESS_KEY'
export AWS_SECRET_KEY='YOUR_SECRET_KEY'

# Run tests
yarn test
```

## 📝 Notes

- The library automatically handles optimistic locking using the `__ts` attribute
- All write operations (put, update, delete) trigger change events if an onChange handler is provided
- Batch operations automatically handle chunking according to DynamoDB limits
- The library provides built-in retry strategy with exponential backoff
- All timestamps are managed automatically (**createdAt, **updatedAt, \_\_ts)
- Queries automatically handle pagination for large result sets

## 📝 License

MIT © [Felipe Rohde](mailto:feliperohdee@gmail.com)

## ⭐ Show your support

Give a ⭐️ if this project helped you!

## 👨‍💻 Author

**Felipe Rohde**

- Twitter: [@felipe_rohde](https://twitter.com/felipe_rohde)
- Github: [@feliperohdee](https://github.com/feliperohdee)
- Email: feliperohdee@gmail.com

## 🙏 Acknowledgements

- [AWS SDK for JavaScript v3](https://github.com/aws/aws-sdk-js-v3)
- [Amazon DynamoDB](https://aws.amazon.com/dynamodb/)
