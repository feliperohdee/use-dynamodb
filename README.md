# Use DynamoDB

A TypeScript library that provides a simplified interface for interacting with Amazon DynamoDB, using the AWS SDK v3.

## 🚀 Features

- ✅ Support for CRUD operations (Create, Read, Update, Delete)
- 🔍 Support for local and global secondary indexes
- 📦 Batch operations (batch write and delete)
- 🔎 Optimized queries with filtering
- 🔒 Conditional updates
- 📄 Pagination support
- 🔄 Upsert support
- ⏱️ Automatic timestamp management

## 📦 Installation

```bash
yarn add use-dynamodb
```

## 🛠️ Usage

### Initialization

```typescript
import Dynamodb from 'use-dynamodb';

const dynamodb = new Dynamodb({
	accessKeyId: 'YOUR_ACCESS_KEY',
	secretAccessKey: 'YOUR_SECRET_KEY',
	region: 'us-east-1',
	table: 'YOUR_TABLE_NAME',
	schema: { partition: 'pk', sort: 'sk' },
	indexes: [
		{
			name: 'ls-index',
			partition: 'pk',
			sort: 'lsiSk',
			type: 'S'
		},
		{
			name: 'gs-index',
			partition: 'gsiPk',
			sort: 'gsiSk',
			type: 'S'
		}
	]
});
```

### Basic Operations

#### 📝 Create/Update Item (Put)

```typescript
const item = await dynamodb.put({
	pk: 'user#123',
	sk: 'profile',
	name: 'John Doe',
	email: 'john@example.com'
});
```

#### 📖 Get Item

```typescript
const item = await dynamodb.get({
	pk: 'user#123',
	sk: 'profile'
});
```

#### 🔄 Update Item

```typescript
const updatedItem = await dynamodb.update(
	{
		pk: 'user#123',
		sk: 'profile'
	},
	{
		updateFn: item => ({
			...item,
			email: 'newemail@example.com'
		})
	}
);
```

#### 🔄 Update Item (updating partition and sort)

```typescript
const updatedItem = await dynamodb.update(
	{
		pk: 'user#123',
		sk: 'profile'
	},
	{
		allowUpdatePartitionAndSort: true,
		updateFn: item => ({
			...item,
			pk: 'user#123__disabled',
			sk: 'profile',
			email: 'newemail@example.com'
		})
	}
);
```

#### 🗑️ Delete Item

```typescript
const deletedItem = await dynamodb.delete({
	pk: 'user#123',
	sk: 'profile'
});
```

### Advanced Query Operations

#### 🔍 Fetch (Query) Items

```typescript
const { items, count, lastEvaluatedKey } = await dynamodb.fetch(
	{
		pk: 'user#123'
	},
	{
		consistentRead: true,
		select: ['a', 'b']
	}
);
```

#### 🔎 Fetch with Filter

```typescript
const { items, count, lastEvaluatedKey } = await dynamodb.fetch(
	{ pk: 'user#123' },
	{
		attributeNames: { '#foo': 'foo' },
		attributeValues: { ':foo': 'foo-0' },
		filterExpression: '#foo = :foo'
	}
);
```

#### 📄 Fetch with Pagination

```typescript
const { items, count, lastEvaluatedKey } = await dynamodb.fetch(
	{ pk: 'user#123' },
	{
		limit: 10,
		startKey: lastEvaluatedKey // from previous query
	}
);
```

### Scan Operations

#### 🔍 Scan Items

```typescript
const { items, count, lastEvaluatedKey } = await dynamodb.scan({
	consistentRead: true,
	select: ['a', 'b']
});
```

#### 🔎 Scan with Filter

```typescript
const { items, count, lastEvaluatedKey } = await dynamodb.scan({
	attributeNames: { '#foo': 'foo' },
	attributeValues: { ':foo': 'foo-0' },
	filterExpression: '#foo = :foo'
});
```

#### 📄 Scan with Pagination

```typescript
const { items, count, lastEvaluatedKey } = await dynamodb.scan({
	limit: 10,
	startKey: lastEvaluatedKey // from previous scan
});
```

### Batch Operations

#### 📦 Batch Write

```typescript
const items = [
	{ pk: 'user#1', sk: 'profile', name: 'User 1' },
	{ pk: 'user#2', sk: 'profile', name: 'User 2' }
];
await dynamodb.batchWrite(items);
```

#### 🗑️ Batch Delete

```typescript
await dynamodb.batchDelete({ pk: 'user#123' });
```

### Using Indexes

#### 🔍 Query using Local Secondary Index

```typescript
const { items, count, lastEvaluatedKey } = await dynamodb.fetch({
	pk: 'user#123',
	lsiSk: 'lsi-value'
});
```

#### 🔎 Query using Global Secondary Index

```typescript
const { items, count, lastEvaluatedKey } = await dynamodb.fetch({
	gsiPk: 'gsi-partition-value',
	gsiSk: 'gsi-sort-value'
});
```

## 🧪 Testing

This library includes a comprehensive set of tests. To run the tests:

1. Set the required environment variables:

```bash
export AWS_REGION='us-east-1'
export AWS_ACCESS_KEY='YOUR_ACCESS_KEY'
export AWS_SECRET_KEY='YOUR_SECRET_KEY'
```

2. Run the tests:

```bash
yarn test
```

Make sure to replace 'YOUR_ACCESS_KEY' and 'YOUR_SECRET_KEY' with your actual AWS credentials.

## 🙏 Acknowledgements

- [AWS SDK for JavaScript v3](https://github.com/aws/aws-sdk-js-v3)
- [Amazon DynamoDB](https://aws.amazon.com/dynamodb/)
