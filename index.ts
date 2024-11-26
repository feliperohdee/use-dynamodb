import _ from 'lodash';
import {
	BatchWriteCommand,
	DeleteCommand,
	DynamoDBDocumentClient,
	PutCommand,
	PutCommandInput,
	QueryCommand,
	QueryCommandInput,
	ScanCommandInput,
	ScanCommand,
	UpdateCommand,
	TransactWriteCommand
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

type ItemRaw = Record<string, any>;
type ItemPersisted<T extends ItemRaw = ItemRaw> = T & {
	__createdAt: string;
	__ts: number;
	__updatedAt: string;
};

type ChangeType = 'PUT' | 'UPDATE' | 'DELETE';
type ChangeEvent<T extends ItemRaw = ItemRaw> = {
	item: ItemPersisted<T>;
	partition: string;
	sort?: string | null;
	table: string;
	type: ChangeType;
};

type OnChange<T extends ItemRaw = ItemRaw> = (events: ChangeEvent<T>[]) => Promise<void>;
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

type SharedOptions = {
	attributeNames?: Record<string, string>;
	attributeValues?: Record<string, string | number>;
	filterExpression?: string;
	index?: string;
	prefix?: boolean;
	select?: string[];
};

type DeleteOptions = SharedOptions & {
	conditionExpression?: string;
};

type DeleteManyOptions = SharedOptions & {
	expression?: string;
};

type GetOptions = SharedOptions & {
	consistentRead?: boolean;
};

type QueryOptions<T extends ItemRaw = ItemRaw> = SharedOptions & {
	all?: boolean;
	expression?: string;
	consistentRead?: boolean;
	onChunk?: ({ count, items }: { count: number; items: ItemPersisted<T>[] }) => Promise<void> | void;
	limit?: number;
	startKey?: Record<string, string> | null;
};

type PutOptions = SharedOptions & {
	conditionExpression?: string;
	overwrite: boolean;
};

type ScanOptions<T extends ItemRaw = ItemRaw> = {
	all?: boolean;
	attributeNames?: Record<string, string>;
	attributeValues?: Record<string, string | number>;
	consistentRead?: boolean;
	filterExpression?: string;
	index?: string;
	limit?: number;
	onChunk?: ({ count, items }: { count: number; items: ItemPersisted<T>[] }) => Promise<void> | void;
	select?: string[];
	startKey?: Record<string, string> | null;
};

type UpdateOptions<T extends ItemRaw = ItemRaw> = SharedOptions & {
	allowUpdatePartitionAndSort?: boolean;
	conditionExpression?: string;
	expression?: string;
	updateFn?: (item: ItemPersisted<T> | ItemRaw) => ItemRaw | null;
	upsert?: boolean;
};

const DEFAULT_DELETE_OPTIONS: DeleteOptions = {
	attributeNames: {},
	attributeValues: {},
	conditionExpression: '',
	filterExpression: '',
	index: '',
	prefix: false
};

const DEFAULT_DELETE_MANY_OPTIONS: DeleteManyOptions = {
	attributeNames: {},
	attributeValues: {},
	expression: '',
	filterExpression: '',
	index: '',
	prefix: false
};

const DEFAULT_QUERY_OPTIONS: QueryOptions = {
	all: false,
	attributeNames: {},
	attributeValues: {},
	consistentRead: false,
	expression: '',
	filterExpression: '',
	index: '',
	limit: Infinity,
	onChunk: () => {},
	prefix: false,
	startKey: null,
	select: []
};

const DEFAULT_GET_OPTIONS: GetOptions = {
	attributeNames: {},
	attributeValues: {},
	consistentRead: false,
	filterExpression: '',
	index: '',
	prefix: false,
	select: []
};

const DEFAULT_PUT_OPTIONS: PutOptions = {
	attributeNames: {},
	attributeValues: {},
	conditionExpression: '',
	overwrite: false
};

const DEFAULT_SCAN_OPTIONS: ScanOptions = {
	all: false,
	attributeNames: {},
	attributeValues: {},
	consistentRead: false,
	filterExpression: '',
	index: '',
	limit: Infinity,
	onChunk: () => {},
	select: [],
	startKey: null
};

const DEFAULT_UPDATE_OPTIONS: UpdateOptions = {
	allowUpdatePartitionAndSort: false,
	attributeNames: {},
	attributeValues: {},
	conditionExpression: '',
	expression: '',
	filterExpression: '',
	index: '',
	prefix: false,
	updateFn: _.identity,
	upsert: false
};

const CONSTRUCTOR_OPTIONS: ConstructorOptions = {
	accessKeyId: '',
	indexes: [],
	region: '',
	schema: { partition: 'namespace', sort: 'id' },
	secretAccessKey: '',
	table: ''
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

class Dynamodb<T extends ItemRaw = ItemRaw> {
	public client: DynamoDBDocumentClient;
	public schema: TableSchema;

	private indexes: TableIndex[];
	private onChange: OnChange | null;
	private table: string;

	constructor(options = CONSTRUCTOR_OPTIONS) {
		options = _.defaults({}, options, CONSTRUCTOR_OPTIONS);

		this.client = DynamoDBDocumentClient.from(
			new DynamoDBClient({
				credentials: {
					accessKeyId: options.accessKeyId,
					secretAccessKey: options.secretAccessKey
				},
				region: options.region
			})
		);

		this.onChange = null;
		this.indexes = options.indexes || [];
		this.schema = options.schema;
		this.table = options.table;

		if (_.isFunction(options.onChange)) {
			this.onChange = options.onChange;
		}
	}

	async batchWrite(items: ItemRaw[], ts = _.now()): Promise<ItemPersisted<T>[]> {
		const nowISO = new Date().toISOString();
		const persistedItems = _.map(items, item => {
			return {
				...item,
				__createdAt: nowISO,
				__ts: ts,
				__updatedAt: nowISO
			};
		}) as ItemPersisted<T>[];

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

	async batchDelete(items: ItemRaw[]): Promise<ItemRaw[]> {
		const chunkItems = _.map(items, item => {
			return _.pick(item, _.compact([this.schema.partition, this.schema.sort]));
		});

		const chunks = _.chunk(chunkItems, 25);

		for (const chunk of chunks) {
			await this.client.send(
				new BatchWriteCommand({
					RequestItems: {
						[this.table]: _.map(chunk, item => {
							return {
								DeleteRequest: {
									Key: this.schema.sort
										? {
												[this.schema.partition]: item[this.schema.partition],
												[this.schema.sort]: item[this.schema.sort]
											}
										: {
												[this.schema.partition]: item[this.schema.partition]
											}
								}
							};
						})
					}
				})
			);
		}

		return items;
	}

	async delete(item: ItemRaw, options?: DeleteOptions): Promise<ItemPersisted<T> | null> {
		options = _.defaults({}, options, DEFAULT_DELETE_OPTIONS);

		const current = await this.get(item, options);

		if (!current) {
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
				ExpressionAttributeValues: { ...options.attributeValues, ':__ts': current.__ts },
				Key: this.schema.sort
					? {
							[this.schema.partition]: current[this.schema.partition],
							[this.schema.sort]: current[this.schema.sort]
						}
					: {
							[this.schema.partition]: current[this.schema.partition]
						},
				ReturnValues: 'ALL_OLD',
				TableName: this.table
			})
		);

		const deletedItem = res.Attributes as ItemPersisted<T>;

		await this.notifyChanges([
			{
				item: deletedItem,
				partition: current[this.schema.partition],
				sort: this.schema.sort ? current[this.schema.sort] : null,
				table: this.table,
				type: 'DELETE'
			}
		]);

		return deletedItem;
	}

	async deleteMany(item: ItemRaw, options?: DeleteManyOptions): Promise<ItemPersisted<T>[]> {
		options = _.defaults({}, options, DEFAULT_DELETE_MANY_OPTIONS);

		const { items } = this.schema.sort ? await this.query(item, {
			...options,
			all: true,
			onChunk: async ({ items }) => {
				await this.batchDelete(items);
			}
		}) : await this.scan({
			...options,
			all: true,
			attributeNames: {
				...options.attributeNames,
				'#__pk': this.schema.partition
			},
			attributeValues: {
				...options.attributeValues,
				':__pk': item[this.schema.partition]
			},
			filterExpression: options.filterExpression || '#__pk = :__pk',
			onChunk: async ({ items }) => {
				await this.batchDelete(items);
			}
		});

		if (_.size(items)) {
			await this.notifyChanges(
				_.map(items, item => {
					return {
						item,
						partition: item[this.schema.partition],
						sort: this.schema.sort ? item[this.schema.sort] : null,
						table: this.table,
						type: 'DELETE'
					} as ChangeEvent<T>;
				})
			);
		}

		return items;
	}

	async get(item: ItemRaw, options?: GetOptions): Promise<ItemPersisted<T> | null> {
		options = _.defaults({}, options, DEFAULT_GET_OPTIONS);

		const res = await this.query(item, {
			...options,
			consistentRead: options.consistentRead,
			limit: 1,
			select: options.select
		});

		return _.size(res.items) > 0 ? res.items[0] : null;
	}

	private async notifyChanges(events: ChangeEvent[]) {
		if (!_.isFunction(this.onChange)) {
			return;
		}

		await this.onChange(events);
	}

	optimisticResolveSchema(item: Record<string, string>): { index: string; schema: TableSchema } {
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

	async put(item: ItemRaw, options?: PutOptions, ts = _.now()): Promise<ItemPersisted<T>> {
		options = _.defaults({}, options, DEFAULT_PUT_OPTIONS);

		let conditionExpression = '';

		if (!options.overwrite) {
			conditionExpression = '(attribute_not_exists(#__pk))';
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
		} as ItemPersisted<T>;

		const putParams: PutCommandInput = {
			Item: persistedItem,
			TableName: this.table
		};

		if (conditionExpression) {
			putParams.ConditionExpression = conditionExpression;
		}

		if (_.size(options.attributeNames)) {
			putParams.ExpressionAttributeNames = options.attributeNames;
		}

		if (_.size(options.attributeValues)) {
			putParams.ExpressionAttributeValues = options.attributeValues;
		}

		await this.client.send(new PutCommand(putParams));
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

	async query(
		item: ItemRaw,
		options?: QueryOptions<T>
	): Promise<{
		count: number;
		items: ItemPersisted<T>[];
		lastEvaluatedKey: Record<string, string> | null;
	}> {
		options = _.defaults({}, options, DEFAULT_QUERY_OPTIONS);

		let queryParams: QueryCommandInput = {
			ConsistentRead: options.consistentRead,
			ExpressionAttributeNames: {},
			ExpressionAttributeValues: {},
			KeyConditionExpression: '#__pk = :__pk',
			TableName: this.table
		};

		if (options.limit && options.limit !== Infinity) {
			queryParams.Limit = options.limit;
		}

		if (options.startKey) {
			queryParams.ExclusiveStartKey = options.startKey;
		}

		let { index, schema } = this.optimisticResolveSchema(item);

		if (options.index) {
			queryParams.IndexName = options.index;
		}

		queryParams = {
			...queryParams,
			ExpressionAttributeNames: {
				...queryParams.ExpressionAttributeNames,
				'#__pk': schema.partition
			},
			ExpressionAttributeValues: {
				...queryParams.ExpressionAttributeValues,
				':__pk': item[schema.partition]
			}
		};

		if (index) {
			if (index !== 'sort' && !queryParams.IndexName) {
				queryParams.IndexName = index;
			}

			if (schema.sort) {
				if (options.prefix) {
					queryParams.KeyConditionExpression += ' AND begins_with(#__sk, :__sk)';
				} else {
					queryParams.KeyConditionExpression += ' AND #__sk = :__sk';
				}

				queryParams = {
					...queryParams,
					ExpressionAttributeNames: {
						...queryParams.ExpressionAttributeNames,
						'#__sk': schema.sort
					},
					ExpressionAttributeValues: {
						...queryParams.ExpressionAttributeValues,
						':__sk': item[schema.sort]
					}
				};
			}
		}

		if (_.size(options.attributeNames)) {
			queryParams.ExpressionAttributeNames = {
				...queryParams.ExpressionAttributeNames,
				...options.attributeNames
			};
		}

		if (_.size(options.attributeValues)) {
			queryParams.ExpressionAttributeValues = {
				...queryParams.ExpressionAttributeValues,
				...options.attributeValues
			};
		}

		if (options.expression) {
			queryParams.KeyConditionExpression = concatConditionExpression(queryParams.KeyConditionExpression || '', options.expression);
		}

		if (options.filterExpression) {
			queryParams.FilterExpression = options.filterExpression;
		}

		if (_.size(options.select) > 0) {
			queryParams.ExpressionAttributeNames = {
				...queryParams.ExpressionAttributeNames,
				..._.reduce(
					options.select,
					(reduction, attr, index) => {
						reduction[`#__pe${index + 1}`] = attr;

						return reduction;
					},
					{} as Record<string, string>
				)
			};

			queryParams.ProjectionExpression = _.map(options.select, (attr, index) => {
				return `#__pe${index + 1}`;
			}).join(', ');
		}

		let res = await this.client.send(new QueryCommand(queryParams));
		let items = (res.Items || []) as ItemPersisted<T>[];

		if (_.isFunction(options.onChunk)) {
			await options.onChunk({
				count: _.size(items),
				items
			});
		}

		if (options.all) {
			while (res.LastEvaluatedKey) {
				res = await this.client.send(
					new QueryCommand({
						...queryParams,
						ExclusiveStartKey: res.LastEvaluatedKey
					})
				);

				if (_.isFunction(options.onChunk)) {
					await options.onChunk({
						count: _.size(res.Items),
						items: (res.Items || []) as ItemPersisted<T>[]
					});
				}

				if (res.Items) {
					items = [...items, ...(res.Items as ItemPersisted<T>[])];
				}
			}
		}

		return {
			count: _.size(items),
			items,
			lastEvaluatedKey: res.LastEvaluatedKey || null
		};
	}

	private async replace(item: ItemRaw, replacedItem: ItemPersisted, options?: UpdateOptions<T>, ts = _.now()): Promise<ItemPersisted<T>> {
		const deleteParams = {
			Delete: {
				Key: this.schema.sort
					? {
							[this.schema.partition]: replacedItem[this.schema.partition],
							[this.schema.sort]: replacedItem[this.schema.sort]
						}
					: {
							[this.schema.partition]: replacedItem[this.schema.partition]
						},
				TableName: this.table
			}
		};

		const nowISO = new Date().toISOString();
		const newItem = {
			...item,
			__createdAt: replacedItem.__createdAt ?? nowISO,
			__ts: ts,
			__updatedAt: nowISO
		} as ItemPersisted<T>;

		const putParams: PutCommandInput = {
			Item: newItem,
			TableName: this.table
		};

		if (options?.conditionExpression) {
			putParams.ConditionExpression = options.conditionExpression;
		}

		if (options?.attributeNames && _.size(options.attributeNames)) {
			putParams.ExpressionAttributeNames = options.attributeNames;
		}

		if (options?.attributeValues && _.size(options.attributeValues)) {
			putParams.ExpressionAttributeValues = options.attributeValues;
		}

		await this.client.send(
			new TransactWriteCommand({
				TransactItems: [deleteParams, { Put: putParams }]
			})
		);

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

	async scan(options?: ScanOptions<T>): Promise<{
		count: number;
		items: ItemPersisted<T>[];
		lastEvaluatedKey: Record<string, string> | null;
	}> {
		options = _.defaults({}, options, DEFAULT_SCAN_OPTIONS);

		let scanParams: ScanCommandInput = {
			ConsistentRead: options.consistentRead,
			TableName: this.table
		};

		if (options.index) {
			scanParams.IndexName = options.index;
		}

		if (options.limit && options.limit !== Infinity) {
			scanParams.Limit = options.limit;
		}

		if (options.startKey) {
			scanParams.ExclusiveStartKey = options.startKey;
		}

		if (_.size(options.attributeNames)) {
			scanParams.ExpressionAttributeNames = options.attributeNames;
		}

		if (_.size(options.attributeValues)) {
			scanParams.ExpressionAttributeValues = options.attributeValues;
		}

		if (options.filterExpression) {
			scanParams.FilterExpression = options.filterExpression;
		}

		if (_.size(options.select) > 0) {
			scanParams.ExpressionAttributeNames = {
				...scanParams.ExpressionAttributeNames,
				..._.reduce(
					options.select,
					(reduction, attr, index) => {
						reduction[`#__pe${index + 1}`] = attr;

						return reduction;
					},
					{} as Record<string, string>
				)
			};

			scanParams.ProjectionExpression = _.map(options.select, (attr, index) => {
				return `#__pe${index + 1}`;
			}).join(', ');
		}

		let res = await this.client.send(new ScanCommand(scanParams));
		let items = (res.Items || []) as ItemPersisted<T>[];

		if (_.isFunction(options.onChunk)) {
			await options.onChunk({
				count: _.size(items),
				items
			});
		}

		if (options.all) {
			while (res.LastEvaluatedKey) {
				res = await this.client.send(
					new ScanCommand({
						...scanParams,
						ExclusiveStartKey: res.LastEvaluatedKey
					})
				);

				if (_.isFunction(options.onChunk)) {
					await options.onChunk({
						count: _.size(res.Items),
						items: (res.Items || []) as ItemPersisted<T>[]
					});
				}

				if (res.Items) {
					items = [...items, ...(res.Items as ItemPersisted<T>[])];
				}
			}
		}

		return {
			count: _.size(items),
			items,
			lastEvaluatedKey: res.LastEvaluatedKey || null
		};
	}

	async update(item: ItemRaw, options?: UpdateOptions<T>, ts = _.now()): Promise<ItemPersisted<T>> {
		options = _.defaults({}, options, DEFAULT_UPDATE_OPTIONS);

		let current = await this.get(item);

		if (!current && !options.upsert) {
			throw new Error('Item not found');
		}

		let conditionExpression = '(attribute_not_exists(#__ts) OR #__ts = :__curr_ts)';

		if (options.conditionExpression) {
			conditionExpression = concatConditionExpression(conditionExpression, options.conditionExpression);
		}

		options.attributeNames = {
			...options.attributeNames,
			'#__ts': '__ts'
		};

		options.attributeValues = {
			...options.attributeValues,
			':__curr_ts': current?.__ts ?? 0
		};

		if (options.expression) {
			const nowISO = new Date().toISOString();

			options.expression = concatUpdateExpression(
				options.expression,
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
					Key: this.schema.sort
						? {
								[this.schema.partition]: item[this.schema.partition],
								[this.schema.sort]: item[this.schema.sort]
							}
						: {
								[this.schema.partition]: item[this.schema.partition]
							},
					ReturnValues: 'ALL_NEW',
					TableName: this.table,
					UpdateExpression: options.expression
				})
			);

			const updatedItem = res.Attributes as ItemPersisted<T>;

			await this.notifyChanges([
				{
					item: updatedItem,
					partition: item[this.schema.partition],
					sort: this.schema.sort ? item[this.schema.sort] : null,
					table: this.table,
					type: 'UPDATE'
				}
			]);

			return updatedItem;
		}

		if (_.isFunction(options.updateFn)) {
			const updateFnRes = options.updateFn(_.isNil(current) ? item : current);

			if (!_.isNil(updateFnRes)) {
				item = updateFnRes;
			}
		}

		if (current && options.allowUpdatePartitionAndSort) {
			if (
				item[this.schema.partition] !== current[this.schema.partition] ||
				(this.schema.sort && item[this.schema.sort] !== current[this.schema.sort])
			) {
				return this.replace(item, current, {
					attributeNames: options.attributeNames,
					attributeValues: options.attributeValues,
					conditionExpression
				}, ts);
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

		return this.put(item as T, {
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

export { ChangeEvent, ChangeType, ItemPersisted, ItemRaw };
export { concatConditionExpression, concatUpdateExpression };
export default Dynamodb;
