# DynamoDB Layer Module

The Layer module provides event sourcing and caching capabilities for DynamoDB, allowing you to track changes, maintain a history of modifications, and sync data to external storage systems like S3.

[![TypeScript](https://img.shields.io/badge/-TypeScript-3178C6?style=flat-square&logo=typescript&logoColor=white)](https://www.typescriptlang.org/)
[![Vitest](https://img.shields.io/badge/-Vitest-729B1B?style=flat-square&logo=vitest&logoColor=white)](https://vitest.dev/)
[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

## Features

- 🔄 Event-driven change tracking
- 📝 Maintains modification history with cursors
- 🔄 Eventual consistency with external storage
- 🗃️ Partition-based organization
- ⏱️ TTL support for events
- 🔍 Query support for both current and historical state
- 🔄 Background sync support
- 📊 Comprehensive sync metrics

## Installation

The Layer module is part of the use-dynamodb package:

```bash
yarn add use-dynamodb
```

## Basic Usage

### Initialization

```typescript
import Dynamodb from 'use-dynamodb';
import DynamodbLayer from 'use-dynamodb/layer';

type Item = {
	pk: string;
	sk: string;
	state: string;
};

// First, create a DynamoDB instance for the events table
const eventDb = new Dynamodb<LayerPendingEvent<Item>>({
	accessKeyId: 'YOUR_ACCESS_KEY',
	secretAccessKey: 'YOUR_SECRET_KEY',
	region: 'us-east-1',
	table: 'events-table',
	schema: { partition: 'pk', sort: 'sk' },
	indexes: [
		{
			name: 'cursor-index',
			partition: 'cursor',
			partitionType: 'N',
			sort: 'pk',
			sortType: 'S'
		}
	]
});

// Then create the Layer instance
const layer = new DynamodbLayer({
	db: eventDb,
	table: 'source-table',
	// Optional background runner for sync operations
	backgroundRunner: promise => {
		// Handle background sync
	},
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
	},
	// Optional sync strategy
	syncStrategy: (meta: LayerMeta) => {
		// Return true to trigger sync
		return meta.unsyncedTotal > 100;
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
			sk: 'user-1',
			state: 'active'
		},
		partition: 'users',
		sort: 'user-1',
		table: 'source-table',
		type: 'PUT'
	}
]);

// Tracking DELETE events
await layer.set([
	{
		item: {
			pk: 'users',
			sk: 'user-1',
			state: 'active'
		},
		partition: 'users',
		sort: 'user-1',
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

// Get items with custom sorting
const sortedItems = await layer.get('users', true); // true enables sorting by unique identifier and timestamp
```

### Sync and Metrics

```typescript
// Get sync metrics
const meta = await layer.meta();
console.log(meta);
/*
{
  cursor: string;           // Current cursor position
  loaded: boolean;         // Whether meta data is loaded
  syncedLastTotal: number; // Items in last sync
  syncedTimes: number;     // Number of syncs
  syncedTotal: number;     // Total synced items
  unsyncedLastTotal: number; // Unsynced items in last check
  unsyncedTotal: number;    // Total unsynced items
}
*/

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

### LayerMeta

```typescript
type LayerMeta = {
	cursor: string;
	loaded: boolean;
	syncedLastTotal: number;
	syncedTimes: number;
	syncedTotal: number;
	unsyncedLastTotal: number;
	unsyncedTotal: number;
};
```

### LayerPendingEvent

```typescript
type LayerPendingEvent<T extends Dict = Dict> = {
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
	backgroundRunner?: (promise: Promise<void>) => void;
	db: Db<LayerPendingEvent<T>>;
	getItemPartition?: (item: PersistedItem<T>) => string;
	getItemUniqueIdentifier: (item: PersistedItem<T>) => string;
	getter: LayerGetter<PersistedItem<T>>;
	setter: LayerSetter<PersistedItem<T>>;
	syncStrategy?: (meta: LayerMeta) => boolean;
	table: string;
	ttl?: number | ((item: PersistedItem<T>) => number);
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
   - Custom TTL can be specified per item
   - After TTL, events are automatically deleted by DynamoDB

4. **Background Sync**

   - Optional background sync support via `backgroundRunner`
   - Sync strategy can be customized
   - Comprehensive metrics tracking via `meta()`

5. **Performance**
   - Uses batching for write operations
   - Supports sorting options for queries
   - Efficient partition-based organization
   - Merge operations for handling concurrent modifications

## Example Integration with S3

```typescript
import { S3 } from '@aws-sdk/client-s3';
import DynamodbLayer from 'use-dynamodb/layer';

const s3 = new S3({
	region: 'us-east-1',
	credentials: {
		accessKeyId: 'YOUR_ACCESS_KEY',
		secretAccessKey: 'YOUR_SECRET_KEY'
	}
});

const layer = new DynamodbLayer({
	db: eventDb,
	table: 'source-table',
	backgroundRunner: promise => {
		promise.catch(console.error);
	},
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
	},
	syncStrategy: meta => meta.unsyncedTotal > 100
});
```

## Best Practices

1. **Partitioning**

   - Use meaningful partition keys
   - Consider access patterns
   - Balance partition sizes

2. **Error Handling**

   - Implement retry logic in getter/setter
   - Handle storage failures gracefully
   - Monitor sync operations

3. **Background Sync**

   - Implement proper error handling in backgroundRunner
   - Use appropriate sync strategies
   - Monitor sync metrics

4. **Performance**
   - Batch related changes
   - Use appropriate partition sizes
   - Consider sorting requirements

## Testing

The module includes comprehensive test coverage. Here's a basic example:

```typescript
import Dynamodb from 'use-dynamodb';
import DynamodbLayer from 'use-dynamodb/layer';

describe('Layer', () => {
	let layer: Layer<Item>;
	const getter = vi.fn(async () => []);
	const setter = vi.fn();
	const backgroundRunner = vi.fn();

	beforeAll(async () => {
		layer = new DynamodbLayer({
			db: eventDb,
			backgroundRunner,
			getter,
			setter,
			table: 'test-table',
			getItemPartition: item => item.pk,
			getItemUniqueIdentifier: item => item.sk,
			syncStrategy: meta => meta.unsyncedTotal > 10
		});
	});

	it('should track and sync changes', async () => {
		await layer.set([
			/* your test events */
		]);
		expect(backgroundRunner).toHaveBeenCalled();
	});
});
```

## 📝 License

MIT © [Felipe Rohde](mailto:feliperohdee@gmail.com)

## 🤝 Contributing

Contributions, issues and feature requests are welcome! Feel free to check [issues page](https://github.com/yourusername/use-retry-fn/issues).

## ⭐ Show your support

Give a ⭐️ if this project helped you!

## 👨‍💻 Author

**Felipe Rohde**

- Twitter: [@felipe_rohde](https://twitter.com/felipe_rohde)
- Github: [@feliperohdee](https://github.com/feliperohdee)
- Email: feliperohdee@gmail.com

## 🙏 Acknowledgements

- [Amazon DynamoDB](https://aws.amazon.com/dynamodb/)
- [Amazon S3](https://aws.amazon.com/s3/)
- [Event Sourcing Pattern](https://martinfowler.com/eaaDev/EventSourcing.html)
