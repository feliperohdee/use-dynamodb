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

type Dict<T = any> = { [key: string]: T };
type TableSchema = { partition: string; sort: string };
type TableIndex = { name: string; partition: string; sort?: string; type: 'S' | 'N' };

type ConstructorOptions = {
	accessKeyId: string;
	indexes?: TableIndex[];
	region: string;
	schema: TableSchema;
	secretAccessKey: string;
	table: string;
	timestamps?: {
		createdAtField?: string;
		updatedAtField?: string;
	};
};

type ConstructorTimestampOptions = {
	createdAtField: string;
	updatedAtField: string;
};

type SharedOptions = {
	attributeNames?: Dict<string>;
	attributeValues?: Dict<string | number>;
	filterExpression?: string;
	index?: string;
	prefix?: boolean;
	select?: string[];
};

type BatchDeleteOptions = SharedOptions & {
	expression?: string;
};

type DeleteOptions = SharedOptions & {
	conditionExpression?: string;
};

type GetOptions = SharedOptions & {
	consistentRead?: boolean;
};

type QueryOptions<T> = SharedOptions & {
	all?: boolean;
	expression?: string;
	consistentRead?: boolean;
	onChunk?: ({ count, items }: { count: number; items: T[] }) => Promise<void> | void;
	limit?: number;
	startKey?: Dict<string> | null;
};

type PutOptions = SharedOptions & {
	conditionExpression?: string;
	overwrite: boolean;
};

type ScanOptions<T> = {
	all?: boolean;
	attributeNames?: Dict<string>;
	attributeValues?: Dict<string | number>;
	consistentRead?: boolean;
	filterExpression?: string;
	index?: string;
	limit?: number;
	onChunk?: ({ count, items }: { count: number; items: T[] }) => Promise<void> | void;
	select?: string[];
	startKey?: Dict<string> | null;
};

type UpdateOptions<T> = SharedOptions & {
	allowUpdatePartitionAndSort?: boolean;
	conditionExpression?: string;
	expression?: string;
	updateFn?: (item: T) => Dict | null;
	upsert?: boolean;
};

const DEFAULT_BATCH_DELETE_OPTIONS: BatchDeleteOptions = {
	attributeNames: {},
	attributeValues: {},
	expression: '',
	filterExpression: '',
	index: '',
	prefix: false
};

const DEFAULT_DELETE_OPTIONS: DeleteOptions = {
	attributeNames: {},
	attributeValues: {},
	conditionExpression: '',
	filterExpression: '',
	index: '',
	prefix: false
};

const DEFAULT_QUERY_OPTIONS: QueryOptions<Dict> = {
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

const DEFAULT_SCAN_OPTIONS: ScanOptions<Dict> = {
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

const DEFAULT_UPDATE_OPTIONS: UpdateOptions<Dict> = {
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

const CONSTRUCTOR_TIMESTAMP_OPTIONS: ConstructorTimestampOptions = {
	createdAtField: 'createdAt',
	updatedAtField: 'updatedAt'
};

const CONSTRUCTOR_OPTIONS: ConstructorOptions = {
	accessKeyId: '',
	indexes: [],
	region: '',
	schema: { partition: 'namespace', sort: 'id' },
	secretAccessKey: '',
	table: '',
	timestamps: CONSTRUCTOR_TIMESTAMP_OPTIONS
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

class Dynamodb {
	public client: DynamoDBDocumentClient;
	private indexes: TableIndex[];
	private schema: TableSchema;
	private table: string;
	private timestamps: {
		createdAtField: string;
		updatedAtField: string;
	};

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

		this.indexes = options.indexes || [];
		this.schema = options.schema;
		this.table = options.table;
		this.timestamps = _.defaults({}, options.timestamps, CONSTRUCTOR_TIMESTAMP_OPTIONS);
	}

	async batchWrite<T extends Dict = Dict>(items: Dict[]): Promise<T[]> {
		const { createdAtField = 'createdAt', updatedAtField = 'updatedAt' } = this.timestamps;
		const now = new Date().toISOString();
		const ts = _.now();

		items = _.map(items, item => {
			return {
				...item,
				[createdAtField]: now,
				[updatedAtField]: now,
				__ts: ts
			};
		});

		const chunks = _.chunk(items, 25);

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

		return items as T[];
	}

	async batchDelete<T extends Dict = Dict>(item: Dict, options?: BatchDeleteOptions): Promise<T[]> {
		options = _.defaults({}, options, DEFAULT_BATCH_DELETE_OPTIONS);

		const del = async (items: T[]) => {
			const chunkItems = _.map(items, item => {
				return _.pick(item, [this.schema.partition, this.schema.sort]);
			});

			const chunks = _.chunk(chunkItems, 25);

			for (const chunk of chunks) {
				await this.client.send(
					new BatchWriteCommand({
						RequestItems: {
							[this.table]: _.map(chunk, item => {
								return {
									DeleteRequest: {
										Key: {
											[this.schema.partition]: item[this.schema.partition],
											[this.schema.sort]: item[this.schema.sort]
										}
									}
								};
							})
						}
					})
				);
			}
		};

		const { items } = await this.query<T>(item, {
			...options,
			all: true,
			onChunk: async ({ items }) => {
				await del(items);
			}
		});

		return items;
	}

	async delete<T extends Dict = Dict>(item: Dict, options?: DeleteOptions): Promise<T | null> {
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
				Key: {
					[this.schema.partition]: current[this.schema.partition],
					[this.schema.sort]: current[this.schema.sort]
				},
				ReturnValues: 'ALL_OLD',
				TableName: this.table
			})
		);

		return res.Attributes as T;
	}

	async get<T extends Dict = Dict>(item: Dict, options?: GetOptions): Promise<T | null> {
		options = _.defaults({}, options, DEFAULT_GET_OPTIONS);

		const res = await this.query(item, {
			...options,
			consistentRead: options.consistentRead,
			limit: 1,
			select: options.select
		});

		return _.size(res.items) > 0 ? (res.items[0] as T) : null;
	}

	optimisticResolveSchema(item: Dict): { index: string; schema: TableSchema } {
		// test if has partition and sort keys
		if (_.has(item, this.schema.partition) && _.has(item, this.schema.sort)) {
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

	async put<T extends Dict = Dict>(item: Dict, options?: PutOptions): Promise<T> {
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

		const now = new Date().toISOString();
		const { createdAtField = 'createdAt', updatedAtField = 'updatedAt' } = this.timestamps;

		item = {
			...item,
			[createdAtField]: item[createdAtField] ?? now,
			[updatedAtField]: now,
			__ts: _.now()
		};

		const putParams: PutCommandInput = {
			Item: item,
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

		return item as T;
	}

	async query<T extends Dict = Dict>(
		item: Dict,
		options?: QueryOptions<T>
	): Promise<{
		count: number;
		items: T[];
		lastEvaluatedKey: Dict<string> | null;
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
					{} as Dict<string>
				)
			};

			queryParams.ProjectionExpression = _.map(options.select, (attr, index) => {
				return `#__pe${index + 1}`;
			}).join(', ');
		}

		let res = await this.client.send(new QueryCommand(queryParams));
		let items = (res.Items || []) as T[];

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
						items: (res.Items || []) as T[]
					});
				}

				if (res.Items) {
					items = [...items, ...(res.Items as T[])];
				}
			}
		}

		return {
			count: _.size(items),
			items: items as T[],
			lastEvaluatedKey: res.LastEvaluatedKey || null
		};
	}

	async scan<T extends Dict = Dict>(
		options?: ScanOptions<T>
	): Promise<{
		count: number;
		items: T[];
		lastEvaluatedKey: Dict<string> | null;
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
					{} as Dict<string>
				)
			};

			scanParams.ProjectionExpression = _.map(options.select, (attr, index) => {
				return `#__pe${index + 1}`;
			}).join(', ');
		}

		let res = await this.client.send(new ScanCommand(scanParams));
		let items = (res.Items || []) as T[];

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
						items: (res.Items || []) as T[]
					});
				}

				if (res.Items) {
					items = [...items, ...(res.Items as T[])];
				}
			}
		}

		return {
			count: _.size(items),
			items: items as T[],
			lastEvaluatedKey: res.LastEvaluatedKey || null
		};
	}

	async update<T extends Dict = Dict>(item: Dict, options?: UpdateOptions<T>): Promise<T> {
		options = _.defaults({}, options, DEFAULT_UPDATE_OPTIONS);

		let current = await this.get(item);

		if (!current && !options.upsert) {
			throw new Error('Item not found');
		}

		let conditionExpression = '(attribute_not_exists(#__ts) OR #__ts = :__ts)';
		let { createdAtField = 'createdAt', updatedAtField = 'updatedAt' } = this.timestamps;

		if (options.conditionExpression) {
			conditionExpression = concatConditionExpression(conditionExpression, options.conditionExpression);
		}

		options.attributeNames = {
			...options.attributeNames,
			'#__ts': '__ts'
		};

		options.attributeValues = {
			...options.attributeValues,
			':__ts': current?.__ts ?? _.now()
		};

		if (options.expression) {
			const now = new Date().toISOString();

			options.expression = concatUpdateExpression(
				options.expression,
				'SET #__cr = if_not_exists(#__cr, :__cr), #__ts = :__ts, #__up = :__up'
			);

			options.attributeNames = {
				...options.attributeNames,
				'#__cr': createdAtField,
				'#__up': updatedAtField
			};

			options.attributeValues = {
				...options.attributeValues,
				':__cr': now,
				':__up': now
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
					Key: {
						[this.schema.partition]: item[this.schema.partition],
						[this.schema.sort]: item[this.schema.sort]
					},
					ReturnValues: 'ALL_NEW',
					TableName: this.table,
					UpdateExpression: options.expression
				})
			);

			return res.Attributes as T;
		}

		if (_.isFunction(options.updateFn)) {
			const updateFnRes = options.updateFn((_.isNil(current) ? item : current) as T);

			if (!_.isNil(updateFnRes)) {
				item = updateFnRes;
			}
		}

		if (current && options.allowUpdatePartitionAndSort) {
			if (item[this.schema.partition] !== current[this.schema.partition] || item[this.schema.sort] !== current[this.schema.sort]) {
				return this.updateWithTransaction(item, current, {
					attributeNames: options.attributeNames,
					attributeValues: options.attributeValues,
					conditionExpression
				});
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

		return this.put(item, {
			attributeNames: options.attributeNames,
			attributeValues: options.attributeValues,
			conditionExpression,
			overwrite: true
		});
	}

	private async updateWithTransaction<T extends Dict = Dict>(item: Dict, current: Dict, options?: UpdateOptions<T>): Promise<T> {
		const deleteParams = {
			Delete: {
				Key: {
					[this.schema.partition]: current[this.schema.partition],
					[this.schema.sort]: current[this.schema.sort]
				},
				TableName: this.table
			}
		};

		const putParams: PutCommandInput = {
			Item: { ...item, __ts: _.now() },
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

		return item as T;
	}

	async createTable(): Promise<DescribeTableCommandOutput | CreateTableCommandOutput> {
		try {
			return await this.client.send(
				new DescribeTableCommand({
					TableName: this.table
				})
			);
		} catch (err) {
			console.log(err);
			const inexistentTable = (err as Error).message.includes('resource not found');

			if (inexistentTable) {
				const gi = _.filter(this.indexes, index => {
					return index.partition !== this.schema.partition;
				});

				const globalIndexes = _.map(gi, index => {
					const keySchema = [
						{
							AttributeName: index.partition,
							KeyType: 'HASH'
						}
					];

					if (index.sort) {
						keySchema.push({
							AttributeName: index.sort,
							KeyType: 'RANGE'
						});
					}

					return {
						IndexName: index.name,
						KeySchema: keySchema,
						Projection: {
							ProjectionType: 'ALL'
						}
					};
				}) as GlobalSecondaryIndex[];

				const globalIndexesDefinitions = _.flatMap(gi, index => {
					const definition = [
						{
							AttributeName: index.partition,
							AttributeType: 'S'
						}
					];

					if (index.sort) {
						definition.push({
							AttributeName: index.sort,
							AttributeType: index.type
						});
					}

					return definition;
				}) as AttributeDefinition[];

				const li = _.filter(this.indexes, index => {
					return index.partition === this.schema.partition;
				});

				const localIndexes = _.map(li, index => {
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
						Projection: {
							ProjectionType: 'ALL'
						}
					};
				}) as LocalSecondaryIndex[];

				const localIndexesDefinitions = _.map(li, index => {
					return {
						AttributeName: index.sort,
						AttributeType: index.type
					};
				}) as AttributeDefinition[];

				const commandInput: CreateTableCommandInput = {
					AttributeDefinitions: _.uniqBy(
						[
							{
								AttributeName: this.schema.sort,
								AttributeType: 'S'
							},
							{
								AttributeName: this.schema.partition,
								AttributeType: 'S'
							},
							...globalIndexesDefinitions,
							...localIndexesDefinitions
						],
						'AttributeName'
					),
					BillingMode: 'PAY_PER_REQUEST',
					KeySchema: [
						{
							AttributeName: this.schema.partition,
							KeyType: 'HASH'
						},
						{
							AttributeName: this.schema.sort,
							KeyType: 'RANGE'
						}
					],
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

export { concatConditionExpression, concatUpdateExpression };
export default Dynamodb;
