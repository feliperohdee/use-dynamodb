# DynamoDB Layer Module

The Layer module provides event sourcing and caching capabilities for DynamoDB, allowing you to track changes, maintain a history of modifications, and sync data to external storage systems like S3.

## Features

- 🔄 Event-driven change tracking
- 📝 Maintains modification history with cursors
- 🔄 Eventual consistency with external storage
- 🗃️ Partition-based partitioning
- ⏱️ TTL support for events
- 🔍 Query support for both current and historical state

## Installation

The Layer module is part of the use-dynamodb package:

```bash
yarn add use-dynamodb
```

## Basic Usage

### Initialization

```typescript
import Dynamodb from 'use-dynamodb';

type Item = {
	ps: string;
	sk: string;
	state: string;
};

// First, create a DynamoDB instance for the events table
const eventDb = new Dynamodb<PendingEvent<Item>>({
	accessKeyId: 'YOUR_ACCESS_KEY',
	secretAccessKey: 'YOUR_SECRET_KEY',
	region: 'us-east-1',
	table: 'events-table',
	schema: { partition: 'pk', sort: 'sk' },
	indexes: [
		{
			name: 'cursor-index',
			partition: 'cursor',
			partitionType: 'S',
			sort: 'pk',
			sortType: 'S'
		}
	]
});

// Then create the Layer instance
const layer = new Dynamodb.Layer({
	db: eventDb,
	table: 'source-table',
	// Function to get partition key from item
	getItemPartition: item => item.pk,
	// Function to get unique identifier from item
	getItemUniqueIdentifier: item => item.sk,
	// Function to get current state from storage
	getter: async partition => {
		// Implement retrieval from your storage (e.g., S3)
		return [];
	},
	// Function to set current state in storage
	setter: async (partition, items) => {
		// Implement saving to your storage (e.g., S3)
	}
});
```

### Track Changes

```typescript
// Tracking PUT events
await layer.set([
	{
		item: {
			pk: 'users',
			sk: 'item-1',
			state: 'active'
		},
		partition: 'users',
		sort: 'item-1',
		table: 'source-table',
		type: 'PUT'
	}
]);

// Tracking DELETE events
await layer.set([
	{
		item: {
			partition: 'users',
			sk: 'item-1',
			state: 'active'
		},
		partition: 'users',
		sort: 'item-1',
		table: 'source-table',
		type: 'DELETE'
	}
]);
```

### Query Current State

```typescript
// Get all items in a partition
const items = await layer.get('users');

// Get all items (if not using partitions)
const allItems = await layer.get();
```

### Sync with Storage

```typescript
// Sync pending events with storage
const { count } = await layer.sync();
console.log(`Synced ${count} events`);

// Reset state from source table
const sourceDb = new Dynamodb<Item>({
	accessKeyId: 'YOUR_ACCESS_KEY',
	secretAccessKey: 'YOUR_SECRET_KEY',
	region: 'us-east-1',
	table: 'source-table',
	schema: { partition: 'pk', sort: 'sk' }
});

// Reset all partitions
await layer.reset(sourceDb);

// Reset specific partition
await layer.reset(sourceDb, 'users');
```

## Types

### PendingEvent

```typescript
type PendingEvent<T extends Dict = Dict> = {
	cursor: string; // Event cursor (timestamp)
	item: PersistedItem<T>; // The actual item data
	pk: string; // Partition key
	sk: string; // Sort key
	ttl: number; // TTL for the event
	type: ChangeType; // PUT, UPDATE, or DELETE
};
```

### Layer Constructor Options

```typescript
type LayerOptions<T extends Dict = Dict> = {
	db: Db<PendingEvent<T>>; // DynamoDB instance for events
	table: string; // Source table name
	getItemPartition: (item: T) => string; // Get partition from item
	getItemUniqueIdentifier: (item: T) => string; // Get unique ID from item
	getter: (partition: string) => Promise<T[]>; // Get items from storage
	setter: (partition: string, items: T[]) => Promise<void>; // Save to storage
};
```

## Important Notes

1. **Events Table Schema**

   - Must use 'pk' as partition key
   - Must use 'sk' as sort key
   - Requires a GSI with:
     - 'cursor' as partition key (String)
     - 'pk' as sort key (String)

2. **Event Cursors**

   - Initial cursor is '**INITIAL**'
   - New cursors are ISO timestamps
   - Events are ordered by cursor for consistency

3. **TTL**

   - Events have a 5-day TTL by default
   - After TTL, events are automatically deleted by DynamoDB

4. **Performance**
   - Uses batching for write operations
   - Supports pagination for large datasets
   - Efficient partition-based partitioning

## Example Integration with S3

```typescript
import { S3 } from '@aws-sdk/client-s3';

const s3 = new S3({
	region: 'us-east-1',
	credentials: {
		accessKeyId: 'YOUR_ACCESS_KEY',
		secretAccessKey: 'YOUR_SECRET_KEY'
	}
});

const layer = new Dynamodb.Layer({
	db: eventDb,
	table: 'source-table',
	getItemPartition: item => item.pk,
	getItemUniqueIdentifier: item => item.sk,
	getter: async partition => {
		try {
			const response = await s3.getObject({
				Bucket: 'your-bucket',
				Key: `cache/${partition}.json`
			});

			const content = await response.Body?.transformToString();
			return JSON.parse(content || '[]');
		} catch (err) {
			return [];
		}
	},
	setter: async (partition, items) => {
		await s3.putObject({
			Bucket: 'your-bucket',
			Key: `cache/${partition}.json`,
			Body: JSON.stringify(items),
			ContentType: 'application/json'
		});
	}
});
```

## Use Cases

1. **Event Sourcing**

   - Track all changes to your DynamoDB tables
   - Maintain audit history
   - Support event replay

2. **Caching Layer**

   - Cache table data in S3 or similar storage
   - Reduce DynamoDB read costs
   - Improve read performance

3. **Data Synchronization**

   - Keep multiple data stores in sync
   - Support eventual consistency
   - Enable offline-first applications

4. **Analytics**
   - Track changes for analytics
   - Build event-driven architectures
   - Support data warehousing

## Best Practices

1. **Partitioning**

   - Use meaningful partition keys
   - Consider access patterns
   - Balance partition sizes

2. **Error Handling**

   - Implement retry logic in getter/setter
   - Handle storage failures gracefully
   - Monitor sync operations

3. **Maintenance**

   - Regularly check sync status
   - Monitor TTL deletions
   - Backup important events

4. **Performance**
   - Batch related changes
   - Use appropriate partition sizes
   - Monitor DynamoDB capacity

## Testing

```typescript
import Dynamodb from 'use-dynamodb';

describe('Layer', () => {
	let layer: Layer<Item>;
	const getter = vi.fn(async () => []);
	const setter = vi.fn();

	beforeAll(async () => {
		layer = new Dynamodb.Layer({
			db: eventDb,
			getter,
			setter,
			table: 'test-table',
			getItemPartition: item => item.pk,
			getItemUniqueIdentifier: item => item.sk
		});
	});

	it('should track and sync changes', async () => {
		await layer.set([
			/* your test events */
		]);
		await layer.sync();
		expect(setter).toHaveBeenCalled();
	});
});
```

## 🙏 Acknowledgements

- [Amazon DynamoDB](https://aws.amazon.com/dynamodb/)
- [Amazon S3](https://aws.amazon.com/s3/)
- [Event Sourcing Pattern](https://martinfowler.com/eaaDev/EventSourcing.html)