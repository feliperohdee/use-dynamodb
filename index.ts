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

type SharedOptions = {
	attributeNames?: Dict<string>;
	attributeValues?: Dict<string | number>;
	filterExpression?: string;
	index?: string;
	prefix?: boolean;
	select?: string[];
};

const BATCH_DELETE_OPTS: SharedOptions & {
	expression?: string;
} = {
	attributeNames: {},
	attributeValues: {},
	expression: '',
	filterExpression: '',
	index: '',
	prefix: false
};

const DELETE_OPTS: SharedOptions & {
	conditionExpression?: string;
} = {
	attributeNames: {},
	attributeValues: {},
	conditionExpression: '',
	filterExpression: '',
	index: '',
	prefix: false
};

const QUERY_OPTS: SharedOptions & {
	all?: boolean;
	expression?: string;
	consistentRead?: boolean;
	onChunk?: ({ count, items }: { count: number; items: Dict[] }) => Promise<void> | void;
	limit?: number;
	startKey?: Dict<string> | null;
} = {
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

const GET_OPTS: SharedOptions & {
	consistentRead?: boolean;
} = {
	attributeNames: {},
	attributeValues: {},
	consistentRead: false,
	filterExpression: '',
	index: '',
	prefix: false,
	select: []
};

const PUT_OPTS: {
	attributeNames?: Dict<string>;
	attributeValues?: Dict<string | number>;
	conditionExpression?: string;
	overwrite: boolean;
} = {
	attributeNames: {},
	attributeValues: {},
	conditionExpression: '',
	overwrite: false
};

const SCAN_OPTS: {
	all?: boolean;
	attributeNames?: Dict<string>;
	attributeValues?: Dict<string | number>;
	consistentRead?: boolean;
	filterExpression?: string;
	index?: string;
	limit?: number;
	onChunk?: ({ count, items }: { count: number; items: Dict[] }) => Promise<void> | void;
	select?: string[];
	startKey?: Dict<string> | null;
} = {
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

const UPDATE_OPTS: SharedOptions & {
	allowUpdatePartitionAndSort?: boolean;
	conditionExpression?: string;
	expression?: string;
	updateFn?: (item: Dict) => Dict | null;
	upsert?: boolean;
} = {
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

const CONSTRUCTOR_TIMESTAMP_OPTS: {
	createdAtField: string;
	updatedAtField: string;
} = {
	createdAtField: 'createdAt',
	updatedAtField: 'updatedAt'
};

const CONSTRUCTOR_OPTS: {
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
} = {
	accessKeyId: '',
	indexes: [],
	region: '',
	schema: { partition: 'namespace', sort: 'id' },
	secretAccessKey: '',
	table: '',
	timestamps: CONSTRUCTOR_TIMESTAMP_OPTS
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

	constructor(opts = CONSTRUCTOR_OPTS) {
		opts = _.defaults({}, opts, CONSTRUCTOR_OPTS);

		this.client = DynamoDBDocumentClient.from(
			new DynamoDBClient({
				credentials: {
					accessKeyId: opts.accessKeyId,
					secretAccessKey: opts.secretAccessKey
				},
				region: opts.region
			})
		);

		this.indexes = opts.indexes || [];
		this.schema = opts.schema;
		this.table = opts.table;
		this.timestamps = _.defaults({}, opts.timestamps, CONSTRUCTOR_TIMESTAMP_OPTS);
	}

	async batchWrite<T = Dict>(items: Dict[]): Promise<T[]> {
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

	async batchDelete<T = Dict>(item: Dict, opts = BATCH_DELETE_OPTS): Promise<T[]> {
		opts = _.defaults({}, opts, BATCH_DELETE_OPTS);

		const del = async (items: Dict[]) => {
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
			...opts,
			all: true,
			onChunk: async ({ items }) => {
				await del(items);
			}
		});

		return items;
	}

	async delete<T = Dict>(item: Dict, opts = DELETE_OPTS): Promise<T | null> {
		opts = _.defaults({}, opts, DELETE_OPTS);

		const current = await this.get(item, opts);

		if (!current) {
			return null;
		}

		let conditionExpression = '(attribute_exists(#__ts) AND #__ts = :__ts)';

		if (opts.conditionExpression) {
			conditionExpression = concatConditionExpression(conditionExpression, opts.conditionExpression);
		}

		const res = await this.client.send(
			new DeleteCommand({
				ConditionExpression: conditionExpression,
				ExpressionAttributeNames: { ...opts.attributeNames, '#__ts': '__ts' },
				ExpressionAttributeValues: { ...opts.attributeValues, ':__ts': current.__ts },
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

	async get<T = Dict>(item: Dict, opts = GET_OPTS): Promise<T | null> {
		opts = _.defaults({}, opts, GET_OPTS);

		const res = await this.query(item, {
			...opts,
			consistentRead: opts.consistentRead,
			limit: 1,
			select: opts.select
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

	async put<T = Dict>(item: Dict, opts = PUT_OPTS): Promise<T> {
		opts = _.defaults({}, opts, PUT_OPTS);

		let conditionExpression = '';

		if (!opts.overwrite) {
			conditionExpression = '(attribute_not_exists(#__pk))';
			opts.attributeNames = {
				...opts.attributeNames,
				'#__pk': this.schema.partition
			};
		}

		if (opts.conditionExpression) {
			conditionExpression = concatConditionExpression(conditionExpression, opts.conditionExpression);
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

		if (_.size(opts.attributeNames)) {
			putParams.ExpressionAttributeNames = opts.attributeNames;
		}

		if (_.size(opts.attributeValues)) {
			putParams.ExpressionAttributeValues = opts.attributeValues;
		}

		await this.client.send(new PutCommand(putParams));

		return item as T;
	}

	async query<T = Dict>(
		item: Dict,
		opts = QUERY_OPTS
	): Promise<{
		count: number;
		items: T[];
		lastEvaluatedKey: Dict<string> | null;
	}> {
		opts = _.defaults({}, opts, QUERY_OPTS);

		let queryParams: QueryCommandInput = {
			ConsistentRead: opts.consistentRead,
			ExpressionAttributeNames: {},
			ExpressionAttributeValues: {},
			KeyConditionExpression: '#__pk = :__pk',
			TableName: this.table
		};

		if (opts.limit && opts.limit !== Infinity) {
			queryParams.Limit = opts.limit;
		}

		if (opts.startKey) {
			queryParams.ExclusiveStartKey = opts.startKey;
		}

		let { index, schema } = this.optimisticResolveSchema(item);

		if (opts.index) {
			queryParams.IndexName = opts.index;
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
				if (opts.prefix) {
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

		if (_.size(opts.attributeNames)) {
			queryParams.ExpressionAttributeNames = {
				...queryParams.ExpressionAttributeNames,
				...opts.attributeNames
			};
		}

		if (_.size(opts.attributeValues)) {
			queryParams.ExpressionAttributeValues = {
				...queryParams.ExpressionAttributeValues,
				...opts.attributeValues
			};
		}

		if (opts.expression) {
			queryParams.KeyConditionExpression = concatConditionExpression(queryParams.KeyConditionExpression || '', opts.expression);
		}

		if (opts.filterExpression) {
			queryParams.FilterExpression = opts.filterExpression;
		}

		if (_.size(opts.select) > 0) {
			queryParams.ExpressionAttributeNames = {
				...queryParams.ExpressionAttributeNames,
				..._.reduce(
					opts.select,
					(reduction, attr, index) => {
						reduction[`#__pe${index + 1}`] = attr;

						return reduction;
					},
					{} as Dict<string>
				)
			};

			queryParams.ProjectionExpression = _.map(opts.select, (attr, index) => {
				return `#__pe${index + 1}`;
			}).join(', ');
		}

		let res = await this.client.send(new QueryCommand(queryParams));
		let items = res.Items || [];

		if (_.isFunction(opts.onChunk)) {
			await opts.onChunk({
				count: _.size(items),
				items
			});
		}

		if (opts.all) {
			while (res.LastEvaluatedKey) {
				res = await this.client.send(
					new QueryCommand({
						...queryParams,
						ExclusiveStartKey: res.LastEvaluatedKey
					})
				);

				if (_.isFunction(opts.onChunk)) {
					await opts.onChunk({
						count: _.size(res.Items),
						items: res.Items || []
					});
				}

				if (res.Items) {
					items = [...items, ...res.Items];
				}
			}
		}

		return {
			count: _.size(items),
			items: items as T[],
			lastEvaluatedKey: res.LastEvaluatedKey || null
		};
	}

	async scan<T = Dict>(
		opts = SCAN_OPTS
	): Promise<{
		count: number;
		items: T[];
		lastEvaluatedKey: Dict<string> | null;
	}> {
		opts = _.defaults({}, opts, SCAN_OPTS);

		let scanParams: ScanCommandInput = {
			ConsistentRead: opts.consistentRead,
			TableName: this.table
		};

		if (opts.index) {
			scanParams.IndexName = opts.index;
		}

		if (opts.limit && opts.limit !== Infinity) {
			scanParams.Limit = opts.limit;
		}

		if (opts.startKey) {
			scanParams.ExclusiveStartKey = opts.startKey;
		}

		if (_.size(opts.attributeNames)) {
			scanParams.ExpressionAttributeNames = opts.attributeNames;
		}

		if (_.size(opts.attributeValues)) {
			scanParams.ExpressionAttributeValues = opts.attributeValues;
		}

		if (opts.filterExpression) {
			scanParams.FilterExpression = opts.filterExpression;
		}

		if (_.size(opts.select) > 0) {
			scanParams.ExpressionAttributeNames = {
				...scanParams.ExpressionAttributeNames,
				..._.reduce(
					opts.select,
					(reduction, attr, index) => {
						reduction[`#__pe${index + 1}`] = attr;

						return reduction;
					},
					{} as Dict<string>
				)
			};

			scanParams.ProjectionExpression = _.map(opts.select, (attr, index) => {
				return `#__pe${index + 1}`;
			}).join(', ');
		}

		let res = await this.client.send(new ScanCommand(scanParams));
		let items = res.Items || [];

		if (_.isFunction(opts.onChunk)) {
			await opts.onChunk({
				count: _.size(items),
				items
			});
		}

		if (opts.all) {
			while (res.LastEvaluatedKey) {
				res = await this.client.send(
					new ScanCommand({
						...scanParams,
						ExclusiveStartKey: res.LastEvaluatedKey
					})
				);

				if (_.isFunction(opts.onChunk)) {
					await opts.onChunk({
						count: _.size(res.Items),
						items: res.Items || []
					});
				}

				if (res.Items) {
					items = [...items, ...res.Items];
				}
			}
		}

		return {
			count: _.size(items),
			items: items as T[],
			lastEvaluatedKey: res.LastEvaluatedKey || null
		};
	}

	async update<T = Dict>(item: Dict, opts = UPDATE_OPTS): Promise<T> {
		opts = _.defaults({}, opts, UPDATE_OPTS);

		let current = await this.get(item);

		if (!current && !opts.upsert) {
			throw new Error('Item not found');
		}

		let conditionExpression = '(attribute_not_exists(#__ts) OR #__ts = :__ts)';
		let { createdAtField = 'createdAt', updatedAtField = 'updatedAt' } = this.timestamps;

		if (opts.conditionExpression) {
			conditionExpression = concatConditionExpression(conditionExpression, opts.conditionExpression);
		}

		opts.attributeNames = {
			...opts.attributeNames,
			'#__ts': '__ts'
		};

		opts.attributeValues = {
			...opts.attributeValues,
			':__ts': current?.__ts ?? _.now()
		};

		if (opts.expression) {
			const now = new Date().toISOString();

			opts.expression = concatUpdateExpression(opts.expression, 'SET #__cr = if_not_exists(#__cr, :__cr), #__ts = :__ts, #__up = :__up');
			opts.attributeNames = {
				...opts.attributeNames,
				'#__cr': createdAtField,
				'#__up': updatedAtField
			};

			opts.attributeValues = {
				...opts.attributeValues,
				':__cr': now,
				':__up': now
			};

			if (!opts.upsert) {
				conditionExpression = `(attribute_exists(#__pk) AND ${conditionExpression})`;
				opts.attributeNames = {
					...opts.attributeNames,
					'#__pk': this.schema.partition
				};
			}

			const res = await this.client.send(
				new UpdateCommand({
					ConditionExpression: conditionExpression,
					ExpressionAttributeNames: opts.attributeNames,
					ExpressionAttributeValues: opts.attributeValues,
					Key: {
						[this.schema.partition]: item[this.schema.partition],
						[this.schema.sort]: item[this.schema.sort]
					},
					ReturnValues: 'ALL_NEW',
					TableName: this.table,
					UpdateExpression: opts.expression
				})
			);

			return res.Attributes as T;
		}

		if (_.isFunction(opts.updateFn)) {
			const updateFnRes = opts.updateFn(_.isNil(current) ? item : current);

			if (!_.isNil(updateFnRes)) {
				item = updateFnRes;
			}
		}

		if (current && opts.allowUpdatePartitionAndSort) {
			if (item[this.schema.partition] !== current[this.schema.partition] || item[this.schema.sort] !== current[this.schema.sort]) {
				return this.updateWithTransaction(item, current, {
					attributeNames: opts.attributeNames,
					attributeValues: opts.attributeValues,
					conditionExpression
				});
			}
		}

		if (!opts.upsert) {
			conditionExpression = `(attribute_exists(#__pk) AND ${conditionExpression})`;
			opts.attributeNames = {
				...opts.attributeNames,
				'#__pk': this.schema.partition,
				'#__ts': '__ts'
			};
		}

		return this.put(item, {
			attributeNames: opts.attributeNames,
			attributeValues: opts.attributeValues,
			conditionExpression,
			overwrite: true
		});
	}

	private async updateWithTransaction<T = Dict>(item: Dict, current: Dict, opts: typeof UPDATE_OPTS): Promise<T> {
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

		if (opts.conditionExpression) {
			putParams.ConditionExpression = opts.conditionExpression;
		}

		if (_.size(opts.attributeNames)) {
			putParams.ExpressionAttributeNames = opts.attributeNames;
		}

		if (_.size(opts.attributeValues)) {
			putParams.ExpressionAttributeValues = opts.attributeValues;
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

export { concatConditionExpression, concatUpdateExpression, SCAN_OPTS };
export default Dynamodb;
