import _ from 'lodash';
import { ConfiguredRetryStrategy } from '@smithy/util-retry';
import {
	BatchGetCommand,
	BatchWriteCommand,
	DeleteCommand,
	DeleteCommandInput,
	DynamoDBDocumentClient,
	PutCommand,
	PutCommandInput,
	QueryCommand,
	QueryCommandInput,
	ScanCommand,
	ScanCommandInput,
	TransactWriteCommand,
	TransactWriteCommandInput,
	TransactWriteCommandOutput,
	UpdateCommand,
	UpdateCommandInput
} from '@aws-sdk/lib-dynamodb';
import {
	AttributeDefinition,
	CreateTableCommand,
	CreateTableCommandInput,
	CreateTableCommandOutput,
	DescribeTableCommand,
	DescribeTableCommandOutput,
	DynamoDBClient,
	GlobalSecondaryIndex,
	LocalSecondaryIndex
} from '@aws-sdk/client-dynamodb';

import { concatConditionExpression, concatUpdateExpression } from './expressions-helper.js';
import Layer from './layer.js';

type Dict = Record<string, any>;

namespace Dynamodb {
	export type PersistedItem<T extends Dict = Dict> = T & {
		__createdAt: string;
		__ts: number;
		__updatedAt: string;
	};

	export type ChangeType = 'PUT' | 'UPDATE' | 'DELETE';
	export type ChangeEvent<T extends Dict = Dict> = {
		item: PersistedItem<T>;
		partition: string;
		sort?: string | null;
		table: string;
		type: ChangeType;
	};

	export type OnChange<T extends Dict = Dict> = (events: ChangeEvent<T>[]) => Promise<void> | void;
	export type TableSchema = { partition: string; sort?: string };
	export type TableGSI = {
		name: string;
		partition: string;
		partitionType: 'S' | 'N';
		sort?: string;
		sortType?: 'S' | 'N';
	};

	export type TableLSI = {
		name: string;
		partition: string;
		sort?: string;
		sortType?: 'S' | 'N';
	};

	export type FilterOptions<T extends Dict = Dict> = {
		attributeNames?: Record<string, string>;
		attributeValues?: Record<string, string | number>;
		chunkLimit?: number;
		consistentRead?: boolean;
		filterExpression?: string;
		index?: string;
		item?: Dict;
		limit?: number;
		onChunk?: ({ count, items }: { count: number; items: PersistedItem<T>[] }) => Promise<void> | void;
		prefix?: boolean;
		queryExpression?: string;
		select?: string[];
		startKey?: Dict | null;
	};

	export type MultiResponse<T extends Dict = Dict> = {
		count: number;
		items: PersistedItem<T>[];
		lastEvaluatedKey: Dict | null;
	};
}

class Dynamodb<T extends Dict = Dict> {
	public static Layer = Layer;

	public client: DynamoDBDocumentClient;
	public indexes: (Dynamodb.TableGSI | Dynamodb.TableLSI)[];
	public schema: Dynamodb.TableSchema;

	private onChange: Dynamodb.OnChange<T> | null;
	private table: string;

	constructor(options: {
		accessKeyId: string;
		indexes?: (Dynamodb.TableGSI | Dynamodb.TableLSI)[];
		onChange?: Dynamodb.OnChange<T>;
		region: string;
		retryTimes?: number;
		retryStrategy?: (attempt: number) => number;
		schema: Dynamodb.TableSchema;
		secretAccessKey: string;
		table: string;
	}) {
		this.client = DynamoDBDocumentClient.from(
			new DynamoDBClient({
				credentials: {
					accessKeyId: options.accessKeyId,
					secretAccessKey: options.secretAccessKey
				},
				region: options.region,
				retryStrategy: new ConfiguredRetryStrategy(
					options.retryTimes ?? 4,
					options.retryStrategy ??
						((attempt: number) => {
							return 100 + attempt * 1000;
						})
				)
			})
		);

		this.indexes = options.indexes || [];
		this.onChange = null;
		this.schema = options.schema;
		this.table = options.table;

		if (_.isFunction(options.onChange)) {
			this.onChange = options.onChange;
		}
	}

	async batchDelete(keys: Dict[]): Promise<Dict[]> {
		keys = _.map(keys, this.getSchemaKeys.bind(this));

		const chunks = _.chunk(keys, 25);

		for (const chunk of chunks) {
			await this.client.send(
				new BatchWriteCommand({
					RequestItems: {
						[this.table]: _.map(chunk, key => {
							return {
								DeleteRequest: { Key: key }
							};
						})
					}
				})
			);
		}

		if (_.size(keys)) {
			await this.notifyChanges(
				_.map(keys, key => {
					return {
						item: key,
						partition: key[this.schema.partition],
						sort: this.schema.sort ? key[this.schema.sort] : null,
						table: this.table,
						type: 'DELETE'
					} as Dynamodb.ChangeEvent<T>;
				})
			);
		}

		return keys;
	}

	async batchGet<R extends Dict = T>(keys: Dict[]): Promise<Dynamodb.PersistedItem<R>[]> {
		keys = _.map(keys, this.getSchemaKeys.bind(this));

		let chunks = _.chunk(keys, 100);
		let items: Dynamodb.PersistedItem<R>[] = [];

		for (const chunk of chunks) {
			const res = await this.client.send(
				new BatchGetCommand({
					RequestItems: {
						[this.table]: {
							Keys: chunk
						}
					}
				})
			);

			if (res.Responses) {
				items = [...items, ...(res.Responses[this.table] as Dynamodb.PersistedItem<R>[])];
			}
		}

		return items;
	}

	async batchWrite<R extends Dict = T>(items: Dict[], ts: number = _.now()): Promise<Dynamodb.PersistedItem<R>[]> {
		const nowISO = new Date(ts).toISOString();
		const persistedItems = _.map(items, item => {
			return {
				...item,
				__createdAt: nowISO,
				__ts: ts,
				__updatedAt: nowISO
			};
		}) as Dynamodb.PersistedItem<R>[];

		const chunks = _.chunk(persistedItems, 25);

		for (const chunk of chunks) {
			await this.client.send(
				new BatchWriteCommand({
					RequestItems: {
						[this.table]: _.map(chunk, item => {
							return {
								PutRequest: { Item: item }
							};
						})
					}
				})
			);
		}

		if (_.size(persistedItems)) {
			await this.notifyChanges(
				_.map(persistedItems, item => {
					return {
						item,
						partition: item[this.schema.partition],
						sort: this.schema.sort ? item[this.schema.sort] : null,
						table: this.table,
						type: 'PUT'
					} as Dynamodb.ChangeEvent<R>;
				})
			);
		}

		return persistedItems;
	}

	async clear(pk?: string) {
		if (pk) {
			const { count } = await this.query({
				item: { [this.schema.partition]: pk },
				limit: Infinity,
				onChunk: async ({ items }) => {
					await this.batchDelete(items);
				}
			});

			return { count };
		}

		const { count } = await this.scan({
			limit: Infinity,
			onChunk: async ({ items }) => {
				await this.batchDelete(items);
			}
		});

		return { count };
	}

	async delete<R extends Dict = T>(options: {
		attributeNames?: Record<string, string>;
		attributeValues?: Record<string, string | number>;
		conditionExpression?: string;
		filter: Omit<Dynamodb.FilterOptions, 'chunkLimit' | 'limit' | 'onChunk' | 'startKey'>;
	}): Promise<Dynamodb.PersistedItem<R> | null> {
		const currentItem = await this.get(options.filter);

		if (!currentItem) {
			return null;
		}

		let conditionExpression = '(attribute_exists(#__ts) AND #__ts = :__ts)';

		if (options.conditionExpression) {
			conditionExpression = concatConditionExpression(conditionExpression, options.conditionExpression);
		}

		const res = await this.client.send(
			new DeleteCommand({
				ConditionExpression: conditionExpression,
				ExpressionAttributeNames: { ...options.attributeNames, '#__ts': '__ts' },
				ExpressionAttributeValues: { ...options.attributeValues, ':__ts': currentItem.__ts },
				Key: this.getSchemaKeys(currentItem),
				ReturnValues: 'ALL_OLD',
				TableName: this.table
			})
		);

		const deletedItem = (res.Attributes as Dynamodb.PersistedItem<R>) || null;

		if (deletedItem) {
			await this.notifyChanges([
				{
					item: deletedItem,
					partition: deletedItem[this.schema.partition],
					sort: this.schema.sort ? deletedItem[this.schema.sort] : null,
					table: this.table,
					type: 'DELETE'
				}
			]);
		}

		return deletedItem;
	}

	async deleteMany<R extends Dict = T>(
		options: Omit<Dynamodb.FilterOptions, 'chunkLimit' | 'consitentRead' | 'limit' | 'onChunk' | 'startKey'>
	): Promise<Dynamodb.PersistedItem<R>[]> {
		const { items } = await this.filter<R>({
			...options,
			consistentRead: true,
			limit: Infinity,
			onChunk: async ({ items }) => {
				await this.batchDelete(items);
			},
			startKey: null
		});

		return items;
	}

	async filter<R extends Dict = T>(options: Dynamodb.FilterOptions<R>): Promise<Dynamodb.MultiResponse<R>> {
		let res: Dynamodb.MultiResponse<R> = {
			count: 0,
			items: [],
			lastEvaluatedKey: null
		};

		if (!options.item && !options.queryExpression && !options.filterExpression) {
			throw new Error('Must provide either item, queryExpression or filterExpression');
		}

		if (options.item) {
			res = await this.query<R>({
				...options,
				item: options.item
			});
		} else if (options.queryExpression) {
			res = await this.query<R>({
				...options,
				queryExpression: options.queryExpression
			});
		} else if (options.filterExpression) {
			res = await this.scan<R>({
				...options,
				filterExpression: options.filterExpression
			});
		}

		return res;
	}

	async get<R extends Dict = T>(
		options: Omit<Dynamodb.FilterOptions, 'chunkLimit' | 'limit' | 'onChunk' | 'startKey'>
	): Promise<Dynamodb.PersistedItem<R> | null> {
		const { items } = await this.filter<R>({
			...options,
			onChunk: () => {},
			limit: 1,
			startKey: null
		});

		return _.size(items) > 0 ? items[0] : null;
	}

	private getLastEvaluatedKey(items: Dict[], index?: string): Dict | null {
		if (!_.size(items)) {
			return null;
		}

		const lastItem = _.last(items)!;

		if (index) {
			return {
				...this.getSchemaKeys(lastItem, index),
				...this.getSchemaKeys(lastItem)
			};
		}

		return this.getSchemaKeys(lastItem);
	}

	private getSchemaKeys(item: Dict, index?: string) {
		if (index) {
			const matchedIndex = _.find(this.indexes, { name: index });

			if (matchedIndex) {
				return _.pick(item, _.compact([matchedIndex.partition, matchedIndex.sort]));
			}
		}

		return _.pick(item, _.compact([this.schema.partition, this.schema.sort]));
	}

	private async notifyChanges(events: Dynamodb.ChangeEvent[]) {
		if (!_.isFunction(this.onChange)) {
			return;
		}

		await this.onChange(events as Dynamodb.ChangeEvent<T>[]);
	}

	async put<R extends Dict = T>(
		item: Dict,
		options?: {
			attributeNames?: Record<string, string>;
			attributeValues?: Record<string, string | number>;
			conditionExpression?: string;
			overwrite?: boolean;
		},
		ts: number = _.now()
	): Promise<Dynamodb.PersistedItem<R>> {
		// avoid mutation on tests
		options = { ...options };

		let conditionExpression = '';

		if (!options.overwrite) {
			conditionExpression = 'attribute_not_exists(#__pk)';
			options.attributeNames = {
				...options.attributeNames,
				'#__pk': this.schema.partition
			};
		}

		if (options.conditionExpression) {
			conditionExpression = concatConditionExpression(conditionExpression, options.conditionExpression);
		}

		const nowISO = new Date(ts).toISOString();
		const persistedItem = {
			...item,
			__createdAt: item.__createdAt ?? nowISO,
			__ts: ts,
			__updatedAt: nowISO
		} as Dynamodb.PersistedItem<R>;

		const putCommandInput: PutCommandInput = {
			Item: persistedItem,
			TableName: this.table
		};

		if (options.attributeNames) {
			putCommandInput.ExpressionAttributeNames = options.attributeNames;
		}

		if (options.attributeValues) {
			putCommandInput.ExpressionAttributeValues = options.attributeValues;
		}

		if (conditionExpression) {
			putCommandInput.ConditionExpression = conditionExpression;
		}

		await this.client.send(new PutCommand(putCommandInput));
		await this.notifyChanges([
			{
				item: persistedItem,
				partition: item[this.schema.partition],
				sort: this.schema.sort ? item[this.schema.sort] : null,
				table: this.table,
				type: 'PUT'
			}
		]);

		return persistedItem;
	}

	async query<R extends Dict = T>(options: {
		attributeNames?: Record<string, string>;
		attributeValues?: Record<string, string | number>;
		chunkLimit?: number;
		consistentRead?: boolean;
		filterExpression?: string;
		index?: string;
		item?: Dict;
		limit?: number;
		onChunk?: ({ count, items }: { count: number; items: Dynamodb.PersistedItem<R>[] }) => Promise<void> | void;
		prefix?: boolean;
		queryExpression?: string;
		select?: string[];
		startKey?: Dict | null;
	}): Promise<Dynamodb.MultiResponse<R>> {
		if (!options.item && !options.queryExpression) {
			throw new Error('Must provide either item or queryExpression');
		}

		options = _.defaults({}, options, {
			chunkLimit: Infinity,
			limit: 100
		});

		const queryCommandInput: QueryCommandInput = {
			ConsistentRead: options.consistentRead ?? false,
			TableName: this.table
		};

		if (_.size(options.attributeNames) > 0) {
			queryCommandInput.ExpressionAttributeNames = options.attributeNames;
		}

		if (_.size(options.attributeValues) > 0) {
			queryCommandInput.ExpressionAttributeValues = options.attributeValues;
		}

		if (_.isFinite(options.chunkLimit)) {
			queryCommandInput.Limit = options.chunkLimit;
		} else if (_.isFinite(options.limit)) {
			queryCommandInput.Limit = options.limit;
		}

		if (options.filterExpression) {
			queryCommandInput.FilterExpression = options.filterExpression;
		}

		if (options.index) {
			queryCommandInput.IndexName = options.index;
		}

		if (_.size(options.select) > 0) {
			queryCommandInput.ExpressionAttributeNames = {
				...queryCommandInput.ExpressionAttributeNames,
				..._.reduce(
					options.select,
					(reduction, attr, index) => {
						reduction[`#__pe${index + 1}`] = attr;

						return reduction;
					},
					{} as Record<string, string>
				)
			};

			queryCommandInput.ProjectionExpression = _.map(options.select, (attr, index) => {
				return `#__pe${index + 1}`;
			}).join(', ');
		}

		if (options.startKey) {
			queryCommandInput.ExclusiveStartKey = options.startKey;
		}

		if (options.item) {
			const { index, schema } = this.resolveSchema(options.item);

			queryCommandInput.KeyConditionExpression = '#__pk = :__pk';
			queryCommandInput.ExpressionAttributeNames = {
				...queryCommandInput.ExpressionAttributeNames,
				'#__pk': schema.partition
			};

			queryCommandInput.ExpressionAttributeValues = {
				...queryCommandInput.ExpressionAttributeValues,
				':__pk': options.item[schema.partition]
			};

			if (index) {
				if (index !== 'sort' && !queryCommandInput.IndexName) {
					queryCommandInput.IndexName = index;
				}

				if (schema.sort) {
					if (options.prefix) {
						queryCommandInput.KeyConditionExpression += ' AND begins_with(#__sk, :__sk)';
					} else {
						queryCommandInput.KeyConditionExpression += ' AND #__sk = :__sk';
					}

					queryCommandInput.ExpressionAttributeNames = {
						...queryCommandInput.ExpressionAttributeNames,
						'#__sk': schema.sort
					};

					queryCommandInput.ExpressionAttributeValues = {
						...queryCommandInput.ExpressionAttributeValues,
						':__sk': options.item[schema.sort]
					};
				}
			}

			if (options.queryExpression) {
				queryCommandInput.KeyConditionExpression = concatConditionExpression(
					queryCommandInput.KeyConditionExpression,
					options.queryExpression
				);
			}
		} else if (options.queryExpression) {
			queryCommandInput.KeyConditionExpression = options.queryExpression;
		}

		let res = await this.client.send(new QueryCommand(queryCommandInput));
		let items = (res.Items || []) as Dynamodb.PersistedItem<R>[];
		let count = _.size(items);
		let evaluateLimit = queryCommandInput.Limit ?? Infinity;

		if (_.isFunction(options.onChunk)) {
			await options.onChunk({ count, items });
		}

		while (res.LastEvaluatedKey && count < options.limit!) {
			// if less than limit, increase limit to get more items
			evaluateLimit *= 2;

			res = await this.client.send(
				new QueryCommand({
					...queryCommandInput,
					ExclusiveStartKey: res.LastEvaluatedKey,
					Limit: evaluateLimit
				})
			);

			if (_.isFunction(options.onChunk)) {
				await options.onChunk({
					count: _.size(res.Items),
					items: (res.Items || []) as Dynamodb.PersistedItem<R>[]
				});
			}

			if (res.Items) {
				items = [...items, ...(res.Items as Dynamodb.PersistedItem<R>[])];
				count = _.size(items);
			}
		}

		items = _.take(items, options.limit);
		count = _.size(items);

		return {
			count,
			items,
			lastEvaluatedKey: res.LastEvaluatedKey ? this.getLastEvaluatedKey(items, queryCommandInput.IndexName) : null
		};
	}

	async replace<R extends Dict = T>(
		item: Dict,
		replacedItem: Dynamodb.PersistedItem,
		options?: {
			attributeNames?: Record<string, string>;
			attributeValues?: Record<string, string | number>;
			conditionExpression?: string;
			overwrite?: boolean;
		},
		ts: number = _.now()
	): Promise<Dynamodb.PersistedItem<R>> {
		// avoid mutation on tests
		options = { ...options };

		const nowISO = new Date(ts).toISOString();
		const newItem = {
			...item,
			__createdAt: replacedItem.__createdAt ?? nowISO,
			__ts: ts,
			__updatedAt: nowISO
		} as Dynamodb.PersistedItem<R>;

		const deleteCommandInput: DeleteCommandInput = {
			ConditionExpression: '#__ts = :__ts',
			ExpressionAttributeNames: { '#__ts': '__ts' },
			ExpressionAttributeValues: { ':__ts': replacedItem.__ts },
			Key: this.getSchemaKeys(replacedItem),
			TableName: this.table
		};

		const putCommandInput: PutCommandInput = {
			Item: newItem,
			TableName: this.table
		};

		if (!options.overwrite) {
			putCommandInput.ConditionExpression = 'attribute_not_exists(#__pk)';
			putCommandInput.ExpressionAttributeNames = {
				...putCommandInput.ExpressionAttributeNames,
				'#__pk': this.schema.partition
			};
		}

		if (_.size(options.attributeNames) > 0) {
			putCommandInput.ExpressionAttributeNames = {
				...putCommandInput.ExpressionAttributeNames,
				...options.attributeNames
			};
		}

		if (_.size(options.attributeValues) > 0) {
			putCommandInput.ExpressionAttributeValues = options.attributeValues;
		}

		if (options.conditionExpression) {
			putCommandInput.ConditionExpression = concatConditionExpression(
				putCommandInput.ConditionExpression || '',
				options.conditionExpression
			);
		}

		await this.transaction({
			TransactItems: [
				{
					Delete: deleteCommandInput
				},
				{
					Put: putCommandInput
				}
			]
		});

		await this.notifyChanges([
			{
				item: replacedItem,
				partition: replacedItem[this.schema.partition],
				sort: this.schema.sort ? replacedItem[this.schema.sort] : null,
				table: this.table,
				type: 'DELETE'
			},
			{
				item: newItem,
				partition: newItem[this.schema.partition],
				sort: this.schema.sort ? newItem[this.schema.sort] : null,
				table: this.table,
				type: 'PUT'
			}
		]);

		return newItem;
	}

	private resolveSchema(item: Dict): { index: string; schema: Dynamodb.TableSchema } {
		// test if has partition and sort keys
		if (_.has(item, this.schema.partition) && this.schema.sort && _.has(item, this.schema.sort)) {
			return {
				index: 'sort',
				schema: {
					partition: this.schema.partition,
					sort: this.schema.sort
				}
			};
		}

		// test if match any index's schema
		for (const { name, partition, sort } of this.indexes) {
			if (!sort) {
				continue;
			}

			if (_.has(item, partition) && _.has(item, sort)) {
				return { index: name, schema: { partition, sort: sort } };
			}
		}

		// test if has only partition key
		if (_.has(item, this.schema.partition)) {
			return {
				index: '',
				schema: {
					partition: this.schema.partition,
					sort: ''
				}
			};
		}

		// test if match any index's partition key
		for (const { name, partition } of this.indexes) {
			if (_.has(item, partition)) {
				return { index: name, schema: { partition, sort: '' } };
			}
		}

		return { index: '', schema: { partition: '', sort: '' } };
	}

	async scan<R extends Dict = T>(options?: {
		attributeNames?: Record<string, string>;
		attributeValues?: Record<string, string | number>;
		chunkLimit?: number;
		consistentRead?: boolean;
		filterExpression?: string;
		index?: string;
		limit?: number;
		onChunk?: ({ count, items }: { count: number; items: Dynamodb.PersistedItem<R>[] }) => Promise<void> | void;
		select?: string[];
		startKey?: Dict | null;
	}): Promise<Dynamodb.MultiResponse<R>> {
		options = _.defaults({}, options, {
			chunkLimit: Infinity,
			limit: 100
		});

		const scanCommandInput: ScanCommandInput = {
			ConsistentRead: options.consistentRead ?? false,
			TableName: this.table
		};

		if (_.size(options.attributeNames) > 0) {
			scanCommandInput.ExpressionAttributeNames = options.attributeNames;
		}

		if (_.size(options.attributeValues) > 0) {
			scanCommandInput.ExpressionAttributeValues = options.attributeValues;
		}

		if (_.isFinite(options.chunkLimit)) {
			scanCommandInput.Limit = options.chunkLimit;
		} else if (_.isFinite(options.limit)) {
			scanCommandInput.Limit = options.limit;
		}

		if (options.filterExpression) {
			scanCommandInput.FilterExpression = options.filterExpression;
		}

		if (options.index) {
			scanCommandInput.IndexName = options.index;
		}

		if (_.size(options.select) > 0) {
			scanCommandInput.ExpressionAttributeNames = {
				...scanCommandInput.ExpressionAttributeNames,
				..._.reduce(
					options.select,
					(reduction, attr, index) => {
						reduction[`#__pe${index + 1}`] = attr;

						return reduction;
					},
					{} as Record<string, string>
				)
			};

			scanCommandInput.ProjectionExpression = _.map(options.select, (attr, index) => {
				return `#__pe${index + 1}`;
			}).join(', ');
		}

		if (options.startKey) {
			scanCommandInput.ExclusiveStartKey = options.startKey;
		}

		let res = await this.client.send(new ScanCommand(scanCommandInput));
		let items = (res.Items || []) as Dynamodb.PersistedItem<R>[];
		let count = _.size(items);
		let evaluateLimit = scanCommandInput.Limit ?? Infinity;

		if (_.isFunction(options.onChunk)) {
			await options.onChunk({ count, items });
		}

		while (res.LastEvaluatedKey && count < options.limit!) {
			// if less than limit, increase limit to get more items
			evaluateLimit *= 2;

			res = await this.client.send(
				new ScanCommand({
					...scanCommandInput,
					ExclusiveStartKey: res.LastEvaluatedKey,
					Limit: evaluateLimit
				})
			);

			if (_.isFunction(options.onChunk)) {
				await options.onChunk({
					count: _.size(res.Items),
					items: (res.Items || []) as Dynamodb.PersistedItem<R>[]
				});
			}

			if (res.Items) {
				items = [...items, ...(res.Items as Dynamodb.PersistedItem<R>[])];
				count = _.size(items);
			}
		}

		items = _.take(items, options.limit);
		count = _.size(items);

		return {
			count,
			items,
			lastEvaluatedKey: res.LastEvaluatedKey ? this.getLastEvaluatedKey(items, scanCommandInput.IndexName) : null
		};
	}

	async transaction(input: TransactWriteCommandInput) {
		let chunks = _.chunk(input.TransactItems, 100);
		let output: TransactWriteCommandOutput[] = [];

		for (const chunk of chunks) {
			output = [
				...output,
				await this.client.send(
					new TransactWriteCommand({
						ReturnConsumedCapacity: input.ReturnConsumedCapacity ?? 'TOTAL',
						ReturnItemCollectionMetrics: input.ReturnItemCollectionMetrics ?? 'NONE',
						TransactItems: chunk
					})
				)
			];
		}

		return output;
	}

	async update<R extends Dict = T>(
		options: {
			allowUpdatePartitionAndSort?: boolean;
			attributeNames?: Record<string, string>;
			attributeValues?: Record<string, string | number>;
			conditionExpression?: string;
			filter: Omit<Dynamodb.FilterOptions, 'limit' | 'onChunk' | 'startKey'>;
			updateExpression?: string;
			updateFunction?: (item: Dynamodb.PersistedItem<R> | Dict, exists: boolean) => Dict;
			upsert?: boolean;
		},
		ts: number = _.now()
	): Promise<Dynamodb.PersistedItem<R>> {
		// avoid mutation on tests
		options = { ...options };

		const currentItem = await this.get(options.filter);

		if (!currentItem && !options.upsert) {
			throw new Error('Item not found');
		}

		if (!currentItem && !options.filter.item) {
			throw new Error('Existing item or filter.item must be provided');
		}

		let conditionExpression = '';
		let referenceKey = this.getSchemaKeys(currentItem || options.filter.item!);

		if (options.conditionExpression) {
			conditionExpression = options.conditionExpression;
		}

		// start of updateExpression
		if (options.updateExpression) {
			const nowISO = new Date(ts).toISOString();

			options.updateExpression = concatUpdateExpression(
				options.updateExpression,
				'SET #__cr = if_not_exists(#__cr, :__cr), #__ts = :__ts, #__up = :__up'
			);

			options.attributeNames = {
				...options.attributeNames,
				'#__cr': '__createdAt',
				'#__ts': '__ts',
				'#__up': '__updatedAt'
			};

			options.attributeValues = {
				...options.attributeValues,
				':__cr': nowISO,
				':__up': nowISO,
				':__ts': ts
			};

			if (!options.upsert) {
				// for updateExpression we check for existence, not last update timestamp because updateExpression is atomic
				conditionExpression = concatConditionExpression('attribute_exists(#__pk)', conditionExpression);
				options.attributeNames = {
					...options.attributeNames,
					'#__pk': this.schema.partition
				};
			}

			const updateCommandInput: UpdateCommandInput = {
				ExpressionAttributeNames: options.attributeNames,
				ExpressionAttributeValues: options.attributeValues,
				Key: referenceKey,
				ReturnValues: 'ALL_NEW',
				TableName: this.table,
				UpdateExpression: options.updateExpression
			};

			if (conditionExpression) {
				updateCommandInput.ConditionExpression = conditionExpression;
			}

			const res = await this.client.send(new UpdateCommand(updateCommandInput));
			const updatedItem = res.Attributes as Dynamodb.PersistedItem<R>;

			await this.notifyChanges([
				{
					item: updatedItem,
					partition: updatedItem[this.schema.partition],
					sort: this.schema.sort ? updatedItem[this.schema.sort] : null,
					table: this.table,
					type: 'UPDATE'
				}
			]);

			return updatedItem;
		}
		// end of updateExpression

		const updatedItem = options.updateFunction
			? options.updateFunction(currentItem || referenceKey, Boolean(currentItem))
			: currentItem || referenceKey;

		if (currentItem && options.allowUpdatePartitionAndSort) {
			if (
				updatedItem[this.schema.partition] !== currentItem[this.schema.partition] ||
				(this.schema.sort && updatedItem[this.schema.sort] !== currentItem[this.schema.sort])
			) {
				return this.replace(
					updatedItem,
					currentItem,
					{
						attributeNames: options.attributeNames,
						attributeValues: options.attributeValues,
						conditionExpression
					},
					ts
				);
			}
		}

		if (!options.upsert) {
			// for updateFunction (not upsert) we check for existence and last update timestamp to ensure atomicity
			conditionExpression = concatConditionExpression('(attribute_exists(#__pk) AND #__ts = :__curr_ts)', conditionExpression);
			options.attributeNames = {
				...options.attributeNames,
				'#__pk': this.schema.partition
			};
		} else {
			// for updateFunction (with possible upsert) we check for non existence or last update timestamp to ensure atomicity
			conditionExpression = concatConditionExpression('(attribute_not_exists(#__ts) OR #__ts = :__curr_ts)', conditionExpression);
		}

		options.attributeNames = {
			...options.attributeNames,
			'#__ts': '__ts'
		};

		options.attributeValues = {
			...options.attributeValues,
			':__curr_ts': currentItem?.__ts ?? 0
		};

		return this.put(updatedItem, {
			attributeNames: options.attributeNames,
			attributeValues: options.attributeValues,
			conditionExpression,
			overwrite: true
		});
	}

	async createTable(): Promise<DescribeTableCommandOutput | CreateTableCommandOutput> {
		try {
			return await this.client.send(
				new DescribeTableCommand({
					TableName: this.table
				})
			);
		} catch (err) {
			const inexistentTable = (err as Error).message.includes('resource not found');

			if (inexistentTable) {
				const gsi = _.filter(this.indexes, index => {
					return index.partition !== this.schema.partition;
				}) as Dynamodb.TableGSI[];

				const globalIndexes = _.map(gsi, index => {
					return {
						IndexName: index.name,
						KeySchema: _.compact([
							{
								AttributeName: index.partition,
								KeyType: 'HASH'
							},
							index.sort
								? {
										AttributeName: index.sort,
										KeyType: 'RANGE'
									}
								: null
						]),
						Projection: {
							ProjectionType: 'ALL'
						}
					};
				}) as GlobalSecondaryIndex[];

				const globalIndexesDefinitions = _.flatMap(gsi, index => {
					return _.compact([
						{
							AttributeName: index.partition,
							AttributeType: index.partitionType
						},
						index.sort
							? {
									AttributeName: index.sort,
									AttributeType: index.sortType
								}
							: null
					]);
				}) as AttributeDefinition[];

				const lsi = _.filter(this.indexes, index => {
					return index.partition === this.schema.partition;
				}) as Dynamodb.TableLSI[];

				const localIndexes = _.map(lsi, index => {
					return {
						IndexName: index.name,
						KeySchema: [
							{
								AttributeName: this.schema.partition,
								KeyType: 'HASH'
							},
							{
								// local index always has sort key
								AttributeName: index.sort,
								KeyType: 'RANGE'
							}
						],
						Projection: {
							ProjectionType: 'ALL'
						}
					};
				}) as LocalSecondaryIndex[];

				const localIndexesDefinitions = _.map(lsi, index => {
					return {
						AttributeName: index.sort,
						AttributeType: index.sortType
					};
				}) as AttributeDefinition[];

				const baseDefinitions = _.compact([
					{
						AttributeName: this.schema.partition,
						AttributeType: 'S'
					},
					this.schema.sort
						? {
								AttributeName: this.schema.sort,
								AttributeType: 'S'
							}
						: null
				]) as AttributeDefinition[];

				const commandInput: CreateTableCommandInput = {
					AttributeDefinitions: _.uniqBy([...baseDefinitions, ...globalIndexesDefinitions, ...localIndexesDefinitions], 'AttributeName'),
					BillingMode: 'PAY_PER_REQUEST',
					KeySchema: _.compact([
						{
							AttributeName: this.schema.partition,
							KeyType: 'HASH'
						},
						this.schema.sort
							? {
									AttributeName: this.schema.sort,
									KeyType: 'RANGE'
								}
							: null
					]),
					TableName: this.table
				};

				if (_.size(globalIndexes)) {
					commandInput.GlobalSecondaryIndexes = globalIndexes;
				}

				if (_.size(localIndexes)) {
					commandInput.LocalSecondaryIndexes = localIndexes;
				}

				// @ts-ignore-next-line
				return this.client.send(new CreateTableCommand(commandInput));
			}
		}

		return {} as DescribeTableCommandOutput;
	}
}

export { Dict };
export default Dynamodb;
