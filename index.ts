import _ from 'lodash';
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
	UpdateCommand
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

type Dict = Record<string, any>;
type PersistedItem<T extends Dict = Dict> = T & {
	__createdAt: string;
	__ts: number;
	__updatedAt: string;
};

type ChangeType = 'PUT' | 'UPDATE' | 'DELETE';
type ChangeEvent<T extends Dict = Dict> = {
	item: PersistedItem<T>;
	partition: string;
	sort?: string | null;
	table: string;
	type: ChangeType;
};

type OnChange<T extends Dict = Dict> = (events: ChangeEvent<T>[]) => Promise<void>;
type TableSchema = { partition: string; sort?: string };
type TableIndex = { name: string; partition: string; sort?: string; type: 'S' | 'N' };

type ConstructorOptions = {
	accessKeyId: string;
	indexes?: TableIndex[];
	onChange?: OnChange;
	region: string;
	schema: TableSchema;
	secretAccessKey: string;
	table: string;
};

type FilterOptions<T extends Dict = Dict> = {
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

type MultiResponse<T extends Dict = Dict> = {
	count: number;
	items: PersistedItem<T>[];
	lastEvaluatedKey: Dict | null;
};

const concatConditionExpression = (exp1: string, exp2: string): string => {
	const JOINERS = ['AND', 'OR'];

	// Trim both expressions
	const trimmedExp1 = _.trim(exp1);
	const trimmedExp2 = _.trim(exp2);

	// Check if exp2 starts with a joiner
	const startsWithJoiner = _.some(JOINERS, joiner => {
		return _.startsWith(trimmedExp2, joiner);
	});

	// Concatenate expressions
	const concatenated = startsWithJoiner ? `${trimmedExp1} ${trimmedExp2}` : `${trimmedExp1} AND ${trimmedExp2}`;

	// Remove leading 'AND' or 'OR' and trim
	return _.trim(_.replace(concatenated, /^\s+(AND|OR)\s+/, ''));
};

const concatUpdateExpression = (exp1: string, exp2: string): string => {
	const TRIM = ', ';

	const extractSection = (exp: string, sec: string): string => {
		const regex = new RegExp(`${sec}\\s+([^A-Z]+)(?=[A-Z]|$)`, 'g');
		const match = exp.match(regex);

		return match ? _.trim(match[0], ' ,') : '';
	};

	exp1 = _.trim(exp1, TRIM);
	exp2 = _.trim(exp2, TRIM);

	const sections = ['SET', 'ADD', 'DELETE', 'REMOVE'];
	const parts: { [key: string]: string[] } = {};

	_.forEach(sections, sec => {
		const part1 = extractSection(exp1, sec);
		const part2 = extractSection(exp2, sec);

		if (part1 || part2) {
			const items1 = part1 ? part1.replace(`${sec}`, '').split(',') : [];
			const items2 = part2 ? part2.replace(`${sec}`, '').split(',') : [];

			parts[sec] = _.uniq(
				[...items1, ...items2].map(item => {
					return _.trim(item, TRIM);
				})
			);
		}
	});

	let result = _.trim(
		_.map(sections, sec => {
			if (parts[sec] && parts[sec].length > 0) {
				return `${sec} ${_.join(parts[sec], ', ')}`;
			}
			return '';
		})
			.filter(Boolean)
			.join(' ')
	);

	if (_.isEmpty(result)) {
		const combinedExp = _.trim(`${exp1}, ${exp2}`, TRIM);

		result = combinedExp ? `SET ${combinedExp}` : '';
	}

	return result;
};

class Dynamodb<T extends Dict = Dict> {
	public client: DynamoDBDocumentClient;
	public schema: TableSchema;

	private indexes: TableIndex[];
	private onChange: OnChange | null;
	private table: string;

	constructor(options: ConstructorOptions) {
		this.client = DynamoDBDocumentClient.from(
			new DynamoDBClient({
				credentials: {
					accessKeyId: options.accessKeyId,
					secretAccessKey: options.secretAccessKey
				},
				region: options.region
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
			const r = await this.client.send(
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
					} as ChangeEvent<T>;
				})
			);
		}

		return keys;
	}

	async batchGet(keys: Dict[]): Promise<PersistedItem<T>[]> {
		keys = _.map(keys, this.getSchemaKeys.bind(this));

		let chunks = _.chunk(keys, 100);
		let items: PersistedItem<T>[] = [];

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
				items = [...items, ...(res.Responses[this.table] as PersistedItem<T>[])];
			}
		}

		return items;
	}

	async batchWrite(items: Dict[], ts: number = _.now()): Promise<PersistedItem<T>[]> {
		const nowISO = new Date().toISOString();
		const persistedItems = _.map(items, item => {
			return {
				...item,
				__createdAt: nowISO,
				__ts: ts,
				__updatedAt: nowISO
			};
		}) as PersistedItem<T>[];

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
					} as ChangeEvent<T>;
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

	async delete(options: {
		attributeNames?: Record<string, string>;
		attributeValues?: Record<string, string | number>;
		conditionExpression?: string;
		filter: Omit<FilterOptions, 'chunkLimit' | 'limit' | 'onChunk' | 'startKey'>;
	}): Promise<PersistedItem<T> | null> {
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

		const deletedItem = (res.Attributes as PersistedItem<T>) || null;

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

	async deleteMany(
		options: Omit<FilterOptions, 'chunkLimit' | 'consitentRead' | 'limit' | 'onChunk' | 'startKey'>
	): Promise<PersistedItem<T>[]> {
		const { items } = await this.filter({
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

	async filter(options: FilterOptions<T>): Promise<MultiResponse<T>> {
		let res: MultiResponse<T> = {
			count: 0,
			items: [],
			lastEvaluatedKey: null
		};

		if (!options.item && !options.queryExpression && !options.filterExpression) {
			throw new Error('Must provide either item, queryExpression or filterExpression');
		}

		if (options.item) {
			res = await this.query({
				...options,
				item: options.item
			});
		} else if (options.queryExpression) {
			res = await this.query({
				...options,
				queryExpression: options.queryExpression
			});
		} else if (options.filterExpression) {
			res = await this.scan({
				...options,
				filterExpression: options.filterExpression
			});
		}

		return res;
	}

	async get(options: Omit<FilterOptions, 'chunkLimit' | 'limit' | 'onChunk' | 'startKey'>): Promise<PersistedItem<T> | null> {
		const { items } = await this.filter({
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

	private async notifyChanges(events: ChangeEvent[]) {
		if (!_.isFunction(this.onChange)) {
			return;
		}

		await this.onChange(events);
	}

	async put(
		item: Dict,
		options?: {
			attributeNames?: Record<string, string>;
			attributeValues?: Record<string, string | number>;
			conditionExpression?: string;
			overwrite?: boolean;
		},
		ts: number = _.now()
	): Promise<PersistedItem<T>> {
		options = options || {};

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

		const nowISO = new Date().toISOString();
		const persistedItem = {
			...item,
			__createdAt: item.__createdAt ?? nowISO,
			__ts: ts,
			__updatedAt: nowISO
		} as PersistedItem<T>;

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

	async query(options: {
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
	}): Promise<MultiResponse<T>> {
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
		let items = (res.Items || []) as PersistedItem<T>[];
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
					items: (res.Items || []) as PersistedItem<T>[]
				});
			}

			if (res.Items) {
				items = [...items, ...(res.Items as PersistedItem<T>[])];
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

	async replace(
		item: Dict,
		replacedItem: PersistedItem,
		options?: {
			attributeNames?: Record<string, string>;
			attributeValues?: Record<string, string | number>;
			conditionExpression?: string;
			overwrite?: boolean;
		},
		ts: number = _.now()
	): Promise<PersistedItem<T>> {
		options = options || {};

		const nowISO = new Date().toISOString();
		const newItem = {
			...item,
			__createdAt: replacedItem.__createdAt ?? nowISO,
			__ts: ts,
			__updatedAt: nowISO
		} as PersistedItem<T>;

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

	private resolveSchema(item: Dict): { index: string; schema: TableSchema } {
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

	async scan(options?: {
		attributeNames?: Record<string, string>;
		attributeValues?: Record<string, string | number>;
		chunkLimit?: number;
		consistentRead?: boolean;
		filterExpression?: string;
		index?: string;
		limit?: number;
		onChunk?: ({ count, items }: { count: number; items: PersistedItem<T>[] }) => Promise<void> | void;
		select?: string[];
		startKey?: Dict | null;
	}): Promise<MultiResponse<T>> {
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
		let items = (res.Items || []) as PersistedItem<T>[];
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
					items: (res.Items || []) as PersistedItem<T>[]
				});
			}

			if (res.Items) {
				items = [...items, ...(res.Items as PersistedItem<T>[])];
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

	async update(
		options: {
			allowUpdatePartitionAndSort?: boolean;
			attributeNames?: Record<string, string>;
			attributeValues?: Record<string, string | number>;
			conditionExpression?: string;
			filter: Omit<FilterOptions, 'limit' | 'onChunk' | 'startKey'>;
			updateExpression?: string;
			updateFunction?: (item: PersistedItem<T> | Dict, exists: boolean) => Dict;
			upsert?: boolean;
		},
		ts: number = _.now()
	): Promise<PersistedItem<T>> {
		if (!options.updateFunction && !options.updateExpression) {
			throw new Error('updateFunction or updateExpression must be provided');
		}

		const currentItem = await this.get(options.filter);

		if (!currentItem && !options.upsert) {
			throw new Error('Item not found');
		}

		if (!currentItem && !options.filter.item) {
			throw new Error('Existing item or filter.item must be provided');
		}

		let conditionExpression = '(attribute_not_exists(#__ts) OR #__ts = :__curr_ts)';
		let referenceKey = this.getSchemaKeys(currentItem || options.filter.item!);

		if (options.conditionExpression) {
			conditionExpression = concatConditionExpression(conditionExpression, options.conditionExpression);
		}

		options.attributeNames = {
			...options.attributeNames,
			'#__ts': '__ts'
		};

		options.attributeValues = {
			...options.attributeValues,
			':__curr_ts': currentItem?.__ts ?? 0
		};

		if (options.updateExpression) {
			const nowISO = new Date().toISOString();

			options.updateExpression = concatUpdateExpression(
				options.updateExpression,
				'SET #__cr = if_not_exists(#__cr, :__cr), #__ts = :__ts, #__up = :__up'
			);

			options.attributeNames = {
				...options.attributeNames,
				'#__cr': '__createdAt',
				'#__up': '__updatedAt'
			};

			options.attributeValues = {
				...options.attributeValues,
				':__cr': nowISO,
				':__up': nowISO,
				':__ts': ts
			};

			if (!options.upsert) {
				conditionExpression = `(attribute_exists(#__pk) AND ${conditionExpression})`;
				options.attributeNames = {
					...options.attributeNames,
					'#__pk': this.schema.partition
				};
			}

			const res = await this.client.send(
				new UpdateCommand({
					ConditionExpression: conditionExpression,
					ExpressionAttributeNames: options.attributeNames,
					ExpressionAttributeValues: options.attributeValues,
					Key: referenceKey,
					ReturnValues: 'ALL_NEW',
					TableName: this.table,
					UpdateExpression: options.updateExpression
				})
			);

			const updatedItem = res.Attributes as PersistedItem<T>;

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

		const updatedItem = options.updateFunction!(currentItem || referenceKey, Boolean(currentItem));

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
			conditionExpression = `(attribute_exists(#__pk) AND ${conditionExpression})`;
			options.attributeNames = {
				...options.attributeNames,
				'#__pk': this.schema.partition,
				'#__ts': '__ts'
			};
		}

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
				});

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
							AttributeType: 'S'
						},
						index.sort
							? {
									AttributeName: index.sort,
									AttributeType: index.type
								}
							: null
					]);
				}) as AttributeDefinition[];

				const lsi = _.filter(this.indexes, index => {
					return index.partition === this.schema.partition;
				});

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
						AttributeType: index.type
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

export { ChangeEvent, ChangeType, PersistedItem, Dict };
export { concatConditionExpression, concatUpdateExpression };
export default Dynamodb;
