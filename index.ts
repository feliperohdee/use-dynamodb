import _ from 'lodash';
import { ConfiguredRetryStrategy } from '@smithy/util-retry';
import {
	BatchGetCommand,
	BatchWriteCommand,
	DeleteCommand,
	DeleteCommandInput,
	DynamoDBDocumentClient,
	GetCommand,
	GetCommandInput,
	PutCommand,
	PutCommandInput,
	QueryCommand,
	QueryCommandInput,
	ScanCommand,
	ScanCommandInput,
	TransactWriteCommand,
	TransactWriteCommandInput,
	TransactWriteCommandOutput,
	TranslateConfig,
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

import { concatConditionExpression, concatUpdateExpression } from './expressions-helper';

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

	export type ConstructorOptions<T extends Dict = Dict> = {
		accessKeyId: string;
		indexes?: Dynamodb.TableIndex[];
		metaAttributes?: Record<string, string[] | MetaAttributeOptions>;
		onChange?: Dynamodb.OnChange<T>;
		region: string;
		retryTimes?: number;
		retryStrategy?: (attempt: number) => number;
		schema: Dynamodb.TableSchema;
		secretAccessKey: string;
		table: string;
		translateConfig?: TranslateConfig;
	};

	export type BatchGetOptions = {
		consistentRead?: boolean;
		returnNullIfNotFound?: boolean;
		select?: string[];
	};

	export type DeleteOptions = {
		attributeNames?: Record<string, string>;
		attributeValues?: Record<string, any>;
		conditionExpression?: string;
		consistencyCheck?: boolean;
		filter: Omit<FilterOptions, 'chunkLimit' | 'limit' | 'onChunk' | 'startKey'>;
	};

	export type DeleteManyOptions = Omit<FilterOptions, 'chunkLimit' | 'limit' | 'onChunk' | 'startKey'>;
	export type FilterOptions<T extends Dict = Dict> = {
		attributeNames?: Record<string, string>;
		attributeValues?: Record<string, any>;
		chunkLimit?: number;
		consistentRead?: boolean;
		filterExpression?: string;
		index?: string;
		item?: Dict;
		limit?: number;
		onChunk?: ({ count, items }: { count: number; items: PersistedItem<T>[] }) => Promise<void> | void;
		prefix?: boolean;
		queryExpression?: string;
		scanIndexForward?: boolean;
		select?: string[];
		startKey?: Dict | null;
	};

	export type GetOptions = Omit<FilterOptions, 'chunkLimit' | 'limit' | 'onChunk' | 'startKey'>;
	export type GetLastOptions = Omit<FilterOptions, 'chunkLimit' | 'limit' | 'onChunk' | 'startKey'>;

	export type MetaAttributeOptions = {
		attributes: string[];
		joiner: string;
		transform?: (attribute: string, value: any) => string | void;
	};

	export type MultiResponse<T extends Dict = Dict, ReturnAsPersistedItem extends boolean = true> = {
		count: number;
		items: ReturnAsPersistedItem extends true ? PersistedItem<T>[] : T[];
		lastEvaluatedKey: Dict | null;
	};

	export type OnChange<T extends Dict = Dict> = (events: ChangeEvent<T>[]) => Promise<void> | void;
	export type PutOptions = {
		attributeNames?: Record<string, string>;
		attributeValues?: Record<string, any>;
		conditionExpression?: string;
		overwrite?: boolean;
		useCurrentCreatedAtIfExists?: boolean;
	};

	export type QueryOptions<R extends Dict = Dict> = {
		attributeNames?: Record<string, string>;
		attributeValues?: Record<string, any>;
		chunkLimit?: number;
		consistentRead?: boolean;
		filterExpression?: string;
		index?: string;
		item?: Dict;
		limit?: number;
		onChunk?: ({ count, items }: { count: number; items: PersistedItem<R>[] }) => Promise<void> | void;
		prefix?: boolean;
		queryExpression?: string;
		scanIndexForward?: boolean;
		select?: string[];
		startKey?: Dict | null;
		strictChunkLimit?: boolean;
	};

	export type ReplaceOptions = {
		attributeNames?: Record<string, string>;
		attributeValues?: Record<string, any>;
		conditionExpression?: string;
		consistencyCheck?: boolean;
		overwrite?: boolean;
		useCurrentCreatedAtIfExists?: boolean;
	};

	export type ScanOptions<R extends Dict = Dict> = {
		attributeNames?: Record<string, string>;
		attributeValues?: Record<string, any>;
		chunkLimit?: number;
		consistentRead?: boolean;
		filterExpression?: string;
		index?: string;
		limit?: number;
		onChunk?: ({ count, items }: { count: number; items: Dynamodb.PersistedItem<R>[] }) => Promise<void> | void;
		segment?: number;
		select?: string[];
		startKey?: Dict | null;
		strictChunkLimit?: boolean;
		totalSegments?: number;
	};

	export type TableSchema = {
		partition: string;
		sort?: string;
		sortType?: 'S' | 'N';
	};

	export type TableIndex = {
		forceGlobal?: boolean;
		name: string;
		partition: string;
		partitionType?: 'S' | 'N';
		projection?: TableIndexProjection;
		sort?: string;
		sortType?: 'S' | 'N';
	};

	export type TableIndexProjection = {
		nonKeyAttributes?: string[];
		type: 'ALL' | 'KEYS_ONLY' | 'INCLUDE';
	};

	export type UpdateOptions<R extends Dict = Dict> = {
		allowUpdatePartitionAndSort?: boolean;
		attributeNames?: Record<string, string>;
		attributeValues?: Record<string, any>;
		conditionExpression?: string;
		consistencyCheck?: boolean;
		filter: Omit<Dynamodb.FilterOptions, 'limit' | 'onChunk' | 'startKey'>;
		updateExpression?: string;
		updateFunction?: (item: Dynamodb.PersistedItem<R> | Dict, exists: boolean) => Dict | Promise<Dict>;
		upsert?: boolean;
	};
}

class Dynamodb<T extends Dict = Dict> {
	public client: DynamoDBDocumentClient;
	public indexes: Dynamodb.TableIndex[];
	public metaAttributes: Record<string, Dynamodb.MetaAttributeOptions>;
	public schema: Dynamodb.TableSchema;
	public table: string;

	private onChange: Dynamodb.OnChange<T> | null;

	constructor(options: Dynamodb.ConstructorOptions<T>) {
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
			}),
			options.translateConfig ?? {
				marshallOptions: {
					removeUndefinedValues: true
				}
			}
		);

		this.indexes = options.indexes || [];
		this.metaAttributes = _.mapValues(options.metaAttributes || {}, (value, key) => {
			return _.isArray(value) ? { attributes: value, joiner: '#' } : value;
		});
		this.onChange = null;
		this.schema = options.schema;
		this.table = options.table;

		if (_.isFunction(options.onChange)) {
			this.onChange = options.onChange;
		}
	}

	async batchDelete(keys: Dict[]): Promise<Dict[]> {
		keys = _.map(keys, item => {
			return this.getSchemaKeys(item);
		});

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

			await this.notifyChanges(
				_.map(chunk, key => {
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

	async batchGet<R extends Dict = T>(
		keys: Dict[],
		options?: Dynamodb.BatchGetOptions
	): Promise<(Dynamodb.PersistedItem<R> | null)[] | Dynamodb.PersistedItem<R>[]> {
		keys = _.map(keys, item => {
			return this.getSchemaKeys(item);
		});

		let chunks = _.chunk(keys, 100);
		let items: (Dynamodb.PersistedItem<R> | null)[] = [];
		let opts: {
			attributeNames: Record<string, string>;
			consistentRead: boolean;
			projectionExpression: string;
		} = {
			attributeNames: {},
			consistentRead: options?.consistentRead ?? false,
			projectionExpression: ''
		};

		if (options?.select && _.size(options.select) > 0) {
			opts.attributeNames = {
				...opts.attributeNames,
				..._.reduce(
					options.select,
					(reduction, attr, index) => {
						reduction[`#__pe${index + 1}`] = attr;

						return reduction;
					},
					{} as Record<string, string>
				)
			};

			opts.projectionExpression = _.map(options.select, (attr, index) => {
				return `#__pe${index + 1}`;
			}).join(', ');
		}

		for (const chunk of chunks) {
			const res = await this.client.send(
				new BatchGetCommand({
					RequestItems: {
						[this.table]:
							opts.projectionExpression && _.size(opts.attributeNames) > 0
								? {
										ConsistentRead: opts.consistentRead,
										ExpressionAttributeNames: opts.attributeNames,
										Keys: chunk,
										ProjectionExpression: opts.projectionExpression
									}
								: {
										ConsistentRead: opts.consistentRead,
										Keys: chunk
									}
					}
				})
			);

			if (res.Responses) {
				const responseItems = res.Responses[this.table] as Dynamodb.PersistedItem<R>[];

				if (options?.returnNullIfNotFound) {
					items = new Array(keys.length).fill(null);

					// Match returned items with their corresponding positions in the input keys array
					for (const item of responseItems) {
						const keyMatch = this.getSchemaKeys(item);
						const keyIndex = _.findIndex(keys, k => {
							return _.isEqual(k, keyMatch);
						});

						if (keyIndex !== -1) {
							items[keyIndex] = item;
						}
					}
				} else {
					items = [...items, ...(res.Responses[this.table] as Dynamodb.PersistedItem<R>[])];
				}
			}
		}

		return items;
	}

	async batchWrite<R extends Dict = T>(items: Dict[], ts: number = _.now()): Promise<Dynamodb.PersistedItem<R>[]> {
		const nowISO = new Date(ts).toISOString();
		const persistedItems = _.map(items, item => {
			const metaAttributesValues = this.generateMetaAttributes(item);

			return {
				...metaAttributesValues,
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

			await this.notifyChanges(
				_.map(chunk, item => {
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

	async delete<R extends Dict = T>(options: Dynamodb.DeleteOptions): Promise<Dynamodb.PersistedItem<R> | null> {
		const currentItem = await this.get(options.filter);

		if (!currentItem) {
			return null;
		}

		const deleteCommandInput: DeleteCommandInput = {
			ExpressionAttributeNames: options.attributeNames,
			ExpressionAttributeValues: options.attributeValues,
			Key: this.getSchemaKeys(currentItem),
			ReturnValues: 'ALL_OLD',
			TableName: this.table
		};

		if (options.consistencyCheck ?? true) {
			deleteCommandInput.ExpressionAttributeNames = {
				...deleteCommandInput.ExpressionAttributeNames,
				'#__pk': this.schema.partition,
				'#__ts': '__ts'
			};

			deleteCommandInput.ExpressionAttributeValues = {
				...deleteCommandInput.ExpressionAttributeValues,
				':__curr_ts': currentItem.__ts
			};

			deleteCommandInput.ConditionExpression = '(attribute_exists(#__pk) AND #__ts = :__curr_ts)';
		} else {
			deleteCommandInput.ExpressionAttributeNames = {
				...deleteCommandInput.ExpressionAttributeNames,
				'#__pk': this.schema.partition
			};

			deleteCommandInput.ConditionExpression = 'attribute_exists(#__pk)';
		}

		if (options.conditionExpression) {
			deleteCommandInput.ConditionExpression = concatConditionExpression(
				deleteCommandInput.ConditionExpression || '',
				options.conditionExpression
			);
		}

		const res = await this.client.send(new DeleteCommand(deleteCommandInput));
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

	async deleteMany<R extends Dict = T>(options: Dynamodb.DeleteManyOptions): Promise<Dynamodb.PersistedItem<R>[]> {
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

		if (options.item || options.queryExpression) {
			res = await this.query<R>({
				...options,
				item: options.item,
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

	private generateMetaAttributes(item: Dict): Dict {
		const metaAttributesValues: Dict = {};

		_.forEach(this.metaAttributes, ({ attributes, joiner, transform }, key) => {
			metaAttributesValues[key] = _.chain(attributes)
				.map(attribute => {
					let value = item[attribute];

					if (_.isFunction(transform)) {
						value = _.trim(transform(attribute, value) ?? '') || value;
					}

					return _.toString(value);
				})
				.compact()
				.join(joiner)
				.value();
		});

		return metaAttributesValues;
	}

	async get<R extends Dict = T>(options: Dynamodb.GetOptions): Promise<Dynamodb.PersistedItem<R> | null> {
		if (
			options.item &&
			!options.filterExpression &&
			!options.index &&
			!options.prefix &&
			!options.scanIndexForward &&
			!options.queryExpression
		) {
			if (
				(this.schema.partition && this.schema.sort && options.item[this.schema.partition] && options.item[this.schema.sort]) ||
				(this.schema.partition && !this.schema.sort && options.item[this.schema.partition])
			) {
				const getCommandInput: GetCommandInput = {
					Key: this.getSchemaKeys(options.item),
					TableName: this.table
				};

				if (_.size(options.select) > 0) {
					getCommandInput.ExpressionAttributeNames = {
						...getCommandInput.ExpressionAttributeNames,
						..._.reduce(
							options.select,
							(reduction, attr, index) => {
								reduction[`#__pe${index + 1}`] = attr;

								return reduction;
							},
							{} as Record<string, string>
						)
					};

					getCommandInput.ProjectionExpression = _.map(options.select, (attr, index) => {
						return `#__pe${index + 1}`;
					}).join(', ');
				}

				const res = await this.client.send(new GetCommand(getCommandInput));

				return (res.Item || null) as Dynamodb.PersistedItem<R> | null;
			}
		}

		const { items } = await this.filter<R>({
			...options,
			onChunk: () => {},
			limit: 1,
			startKey: null
		});

		return _.size(items) > 0 ? items[0] : null;
	}

	async getLast<R extends Dict = T>(options: Dynamodb.GetLastOptions): Promise<Dynamodb.PersistedItem<R> | null> {
		const { items } = await this.query<R>({
			...options,
			limit: 1,
			scanIndexForward: false
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

	async put<R extends Dict = T>(item: Dict, options: Dynamodb.PutOptions = {}, ts: number = _.now()): Promise<Dynamodb.PersistedItem<R>> {
		const nowISO = new Date(ts).toISOString();
		const metaAttributesValues = this.generateMetaAttributes(item);
		const persistedItem = {
			...metaAttributesValues,
			...item,
			__createdAt: options.useCurrentCreatedAtIfExists ? item.__createdAt || nowISO : nowISO,
			__ts: ts,
			__updatedAt: nowISO
		} as Dynamodb.PersistedItem<R>;

		const putCommandInput: PutCommandInput = options.overwrite
			? {
					ExpressionAttributeNames: options.attributeNames,
					ExpressionAttributeValues: options.attributeValues,
					Item: persistedItem,
					TableName: this.table
				}
			: {
					ConditionExpression: 'attribute_not_exists(#__pk)',
					ExpressionAttributeNames: {
						...options.attributeNames,
						'#__pk': this.schema.partition
					},
					ExpressionAttributeValues: options.attributeValues,
					Item: persistedItem,
					TableName: this.table
				};

		if (options.conditionExpression) {
			putCommandInput.ConditionExpression = concatConditionExpression(
				putCommandInput.ConditionExpression || '',
				options.conditionExpression
			);
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

	async query<R extends Dict = T>(options: Dynamodb.QueryOptions<R>): Promise<Dynamodb.MultiResponse<R>> {
		if (!options.item && !options.queryExpression) {
			throw new Error('Must provide either item or queryExpression');
		}

		options = _.defaults({}, options, {
			chunkLimit: Infinity,
			limit: 100
		});

		const queryCommandInput: QueryCommandInput = {
			ConsistentRead: options.consistentRead ?? false,
			ExpressionAttributeNames: options.attributeNames,
			ExpressionAttributeValues: options.attributeValues,
			TableName: this.table
		};

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

		if (!_.isUndefined(options.scanIndexForward)) {
			queryCommandInput.ScanIndexForward = options.scanIndexForward;
		}

		let res = await this.client.send(new QueryCommand(queryCommandInput));
		let items = (res.Items || []) as Dynamodb.PersistedItem<R>[];
		let count = _.size(items);
		let evaluateLimit = queryCommandInput.Limit ?? Infinity;
		let mustIncreaseEvaluateLimit = true;

		if (options.strictChunkLimit && options.chunkLimit && _.isFinite(options.chunkLimit)) {
			mustIncreaseEvaluateLimit = false;
		}

		if (_.isFunction(options.onChunk)) {
			await options.onChunk({ count, items });
		}

		while (res.LastEvaluatedKey && count < options.limit!) {
			if (mustIncreaseEvaluateLimit) {
				// if less than limit, increase limit to get more items
				evaluateLimit *= 2;
			}

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
		options: Dynamodb.ReplaceOptions = {},
		ts: number = _.now()
	): Promise<Dynamodb.PersistedItem<R>> {
		const nowISO = new Date(ts).toISOString();
		const metaAttributesValues = this.generateMetaAttributes(item);
		const newItem = {
			...metaAttributesValues,
			...item,
			__createdAt: options.useCurrentCreatedAtIfExists ? item.__createdAt || replacedItem.__createdAt : replacedItem.__createdAt,
			__ts: ts,
			__updatedAt: nowISO
		} as Dynamodb.PersistedItem<R>;

		const deleteCommandInput: DeleteCommandInput = {
			Key: this.getSchemaKeys(replacedItem),
			TableName: this.table
		};

		if (options.consistencyCheck ?? true) {
			deleteCommandInput.ExpressionAttributeNames = {
				...deleteCommandInput.ExpressionAttributeNames,
				'#__pk': this.schema.partition,
				'#__ts': '__ts'
			};

			deleteCommandInput.ExpressionAttributeValues = {
				...deleteCommandInput.ExpressionAttributeValues,
				':__curr_ts': replacedItem.__ts
			};

			deleteCommandInput.ConditionExpression = '(attribute_exists(#__pk) AND #__ts = :__curr_ts)';
		} else {
			deleteCommandInput.ExpressionAttributeNames = {
				...deleteCommandInput.ExpressionAttributeNames,
				'#__pk': this.schema.partition
			};

			deleteCommandInput.ConditionExpression = 'attribute_exists(#__pk)';
		}

		const putCommandInput: PutCommandInput = options.overwrite
			? {
					ExpressionAttributeNames: options.attributeNames,
					ExpressionAttributeValues: options.attributeValues,
					Item: newItem,
					TableName: this.table
				}
			: {
					ConditionExpression: 'attribute_not_exists(#__pk)',
					ExpressionAttributeNames: {
						...options.attributeNames,
						'#__pk': this.schema.partition
					},
					ExpressionAttributeValues: options.attributeValues,
					Item: newItem,
					TableName: this.table
				};

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

	async scan<R extends Dict = T>(options?: Dynamodb.ScanOptions<R>): Promise<Dynamodb.MultiResponse<R>> {
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

		if (_.isNumber(options.segment)) {
			scanCommandInput.Segment = options.segment;
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

		if (options.totalSegments) {
			scanCommandInput.TotalSegments = options.totalSegments;
		}

		let res = await this.client.send(new ScanCommand(scanCommandInput));
		let items = (res.Items || []) as Dynamodb.PersistedItem<R>[];
		let count = _.size(items);
		let evaluateLimit = scanCommandInput.Limit ?? Infinity;
		let mustIncreaseEvaluateLimit = true;

		if (options.strictChunkLimit && options.chunkLimit && _.isFinite(options.chunkLimit)) {
			mustIncreaseEvaluateLimit = false;
		}

		if (_.isFunction(options.onChunk)) {
			await options.onChunk({ count, items });
		}

		while (res.LastEvaluatedKey && count < options.limit!) {
			if (mustIncreaseEvaluateLimit) {
				// if less than limit, increase limit to get more items
				evaluateLimit *= 2;
			}

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

	async update<R extends Dict = T>(options: Dynamodb.UpdateOptions<R>, ts: number = _.now()): Promise<Dynamodb.PersistedItem<R>> {
		// start of updateExpression
		if (options.updateExpression) {
			const referenceItem =
				options.filter.item ||
				(await this.get({
					...options.filter,
					consistentRead: true
				}));

			if (!referenceItem) {
				throw new Error('Existing item or filter.item must be provided');
			}

			const referenceKey = this.getSchemaKeys(referenceItem);
			const nowISO = new Date(ts).toISOString();
			const updateCommandInput: UpdateCommandInput = options.upsert
				? {
						ExpressionAttributeNames: options.attributeNames,
						ExpressionAttributeValues: options.attributeValues,
						Key: referenceKey,
						ReturnValues: 'ALL_NEW',
						TableName: this.table,
						UpdateExpression: options.updateExpression
					}
				: {
						ConditionExpression: 'attribute_exists(#__pk)',
						ExpressionAttributeNames: {
							...options.attributeNames,
							'#__pk': this.schema.partition
						},
						ExpressionAttributeValues: options.attributeValues,
						Key: referenceKey,
						ReturnValues: 'ALL_NEW',
						TableName: this.table,
						UpdateExpression: options.updateExpression
					};

			updateCommandInput.UpdateExpression = concatUpdateExpression(
				updateCommandInput.UpdateExpression || '',
				'SET #__cr = if_not_exists(#__cr, :__cr), #__ts = :__ts, #__up = :__up'
			);

			updateCommandInput.ExpressionAttributeNames = {
				...updateCommandInput.ExpressionAttributeNames,
				'#__cr': '__createdAt',
				'#__ts': '__ts',
				'#__up': '__updatedAt'
			};

			updateCommandInput.ExpressionAttributeValues = {
				...updateCommandInput.ExpressionAttributeValues,
				':__cr': nowISO,
				':__up': nowISO,
				':__ts': ts
			};

			if (options.conditionExpression) {
				updateCommandInput.ConditionExpression = concatConditionExpression(
					updateCommandInput.ConditionExpression || '',
					options.conditionExpression
				);
			}

			let res = await this.client.send(new UpdateCommand(updateCommandInput));
			let updatedItem = res.Attributes as Dynamodb.PersistedItem<R>;

			// Generate and update metaAttributes after the update operation
			if (_.size(this.metaAttributes) > 0) {
				const attributeNamesInUpdateExpression = updateCommandInput.UpdateExpression.match(/#\w+/g);
				const updatedAttributes = _.reduce(
					attributeNamesInUpdateExpression,
					(reduction, name) => {
						const attribute = updateCommandInput.ExpressionAttributeNames?.[name];

						if (attribute) {
							return reduction.add(attribute);
						}

						return reduction;
					},
					new Set<string>()
				);

				const affectedMetaAttributes = _.some(this.metaAttributes, ({ attributes }, key) => {
					// if updatedAttributes has the key, it means the attribute is already updated
					if (updatedAttributes.has(key)) {
						return false;
					}

					return _.some(attributes, attribute => {
						return updatedAttributes.has(attribute);
					});
				});

				if (affectedMetaAttributes) {
					const metaAttributesValues = this.generateMetaAttributes(updatedItem);
					const toSnakeCase = _.memoize((key: string) => {
						return _.snakeCase(key);
					});

					if (_.size(metaAttributesValues)) {
						const res = await this.client.send(
							new UpdateCommand({
								ExpressionAttributeNames: _.reduce<Dict, Record<string, string>>(
									metaAttributesValues,
									(reduction, value, key) => {
										reduction[`#${toSnakeCase(key)}`] = key;

										return reduction;
									},
									{}
								),
								ExpressionAttributeValues: _.reduce<Dict, Dict>(
									metaAttributesValues,
									(reduction, value, key) => {
										reduction[`:${toSnakeCase(key)}`] = value;

										return reduction;
									},
									{}
								),
								Key: this.getSchemaKeys(updatedItem),
								ReturnValues: 'ALL_NEW',
								TableName: this.table,
								UpdateExpression:
									'SET ' +
									_.map(metaAttributesValues, (value, key) => {
										key = toSnakeCase(key);

										return `#${key} = :${key}`;
									}).join(', ')
							})
						);

						updatedItem = res.Attributes as Dynamodb.PersistedItem<R>;
					}
				}
			}

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

		const currentItem = await this.get({
			...options.filter,
			consistentRead: true
		});

		if (!currentItem && !options.upsert) {
			throw new Error('Item not found');
		}

		if (!currentItem && !options.filter.item) {
			throw new Error('Existing item or filter.item must be provided');
		}

		const referenceKey = this.getSchemaKeys(currentItem || options.filter.item!);
		const updatedItem = options.updateFunction
			? await options.updateFunction(currentItem || referenceKey, Boolean(currentItem))
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
						conditionExpression: options.conditionExpression,
						useCurrentCreatedAtIfExists: true
					},
					ts
				);
			}
		}

		const putOptions: Dynamodb.PutOptions = options.upsert
			? {
					attributeNames: options.attributeNames,
					attributeValues: options.attributeValues,
					overwrite: true,
					useCurrentCreatedAtIfExists: true
				}
			: {
					attributeNames: {
						...options.attributeNames,
						'#__pk': this.schema.partition
					},
					attributeValues: options.attributeValues,
					conditionExpression: 'attribute_exists(#__pk)',
					overwrite: true,
					useCurrentCreatedAtIfExists: true
				};

		if (options.consistencyCheck ?? true) {
			putOptions.conditionExpression = options.upsert
				? '(attribute_not_exists(#__pk) OR #__ts = :__curr_ts)'
				: '(attribute_exists(#__pk) AND #__ts = :__curr_ts)';

			putOptions.attributeNames = {
				...putOptions.attributeNames,
				'#__pk': this.schema.partition,
				'#__ts': '__ts'
			};

			putOptions.attributeValues = {
				...putOptions.attributeValues,
				':__curr_ts': currentItem?.__ts ?? 0
			};
		}

		if (options.conditionExpression) {
			putOptions.conditionExpression = concatConditionExpression(putOptions.conditionExpression || '', options.conditionExpression);
		}

		const metaAttributes = this.generateMetaAttributes(updatedItem);
		const metaAttributesKeys = _.keys(metaAttributes);

		return this.put(_.omit(updatedItem, metaAttributesKeys), putOptions);
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
					return index.forceGlobal || index.partition !== this.schema.partition;
				});

				const globalIndexes = _.map(gsi, index => {
					const projection: {
						ProjectionType: 'ALL' | 'KEYS_ONLY' | 'INCLUDE';
						NonKeyAttributes?: string[];
					} = {
						ProjectionType: index.projection?.type || 'ALL'
					};

					if (index.projection?.type === 'INCLUDE') {
						projection.NonKeyAttributes = index.projection.nonKeyAttributes;
					}

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
						Projection: projection
					};
				}) as GlobalSecondaryIndex[];

				const globalIndexesDefinitions = _.flatMap(gsi, index => {
					return _.compact([
						{
							AttributeName: index.partition,
							AttributeType: index.partitionType || 'S'
						},
						index.sort
							? {
									AttributeName: index.sort,
									AttributeType: index.sortType || 'S'
								}
							: null
					]);
				}) as AttributeDefinition[];

				const lsi = _.filter(this.indexes, index => {
					return !index.forceGlobal && index.partition === this.schema.partition;
				});

				const localIndexes = _.map(lsi, index => {
					const projection: {
						ProjectionType: 'ALL' | 'KEYS_ONLY' | 'INCLUDE';
						NonKeyAttributes?: string[];
					} = {
						ProjectionType: index.projection?.type || 'ALL'
					};

					if (index.projection?.type === 'INCLUDE') {
						projection.NonKeyAttributes = index.projection.nonKeyAttributes;
					}

					return {
						IndexName: index.name,
						KeySchema: [
							{
								AttributeName: this.schema.partition,
								KeyType: 'HASH'
							},
							{
								AttributeName: index.sort,
								KeyType: 'RANGE'
							}
						],
						Projection: projection
					};
				}) as LocalSecondaryIndex[];

				const localIndexesDefinitions = _.map(lsi, index => {
					return {
						AttributeName: index.sort,
						AttributeType: index.sortType || 'S'
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
								AttributeType: this.schema.sortType || 'S'
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

export { concatConditionExpression, concatUpdateExpression, Dict };
export default Dynamodb;
