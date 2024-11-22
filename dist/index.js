import _ from 'lodash';
import { BatchWriteCommand, DeleteCommand, DynamoDBDocumentClient, PutCommand, QueryCommand, UpdateCommand, TransactWriteCommand } from '@aws-sdk/lib-dynamodb';
import { CreateTableCommand, DescribeTableCommand, DynamoDBClient } from '@aws-sdk/client-dynamodb';
const BATCH_DELETE_OPTS = {
    attributeNames: {},
    attributeValues: {},
    expression: '',
    filterExpression: '',
    index: '',
    prefix: false
};
const DELETE_OPTS = {
    attributeNames: {},
    attributeValues: {},
    conditionExpression: '',
    filterExpression: '',
    index: '',
    prefix: false
};
const FETCH_OPTS = {
    all: false,
    attributeNames: {},
    attributeValues: {},
    expression: '',
    filterExpression: '',
    index: '',
    limit: Infinity,
    onChunk: () => { },
    prefix: false,
    startKey: null
};
const GET_OPTS = {
    attributeNames: {},
    attributeValues: {},
    filterExpression: '',
    index: '',
    prefix: false
};
const PUT_OPTS = {
    attributeNames: {},
    attributeValues: {},
    conditionExpression: '',
    overwrite: false
};
const UPDATE_OPTS = {
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
const CONSTRUCTOR_TIMESTAMP_OPTS = {
    createdAtField: 'createdAt',
    updatedAtField: 'updatedAt'
};
const CONSTRUCTOR_OPTS = {
    accessKeyId: '',
    indexes: [],
    region: '',
    schema: { partition: 'namespace', sort: 'id' },
    secretAccessKey: '',
    table: '',
    timestamps: CONSTRUCTOR_TIMESTAMP_OPTS
};
const concatConditionExpression = (exp1, exp2) => {
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
const concatUpdateExpression = (exp1, exp2) => {
    const TRIM = ', ';
    const extractSection = (exp, sec) => {
        const regex = new RegExp(`${sec}\\s+([^A-Z]+)(?=[A-Z]|$)`, 'g');
        const match = exp.match(regex);
        return match ? _.trim(match[0], ' ,') : '';
    };
    exp1 = _.trim(exp1, TRIM);
    exp2 = _.trim(exp2, TRIM);
    const sections = ['SET', 'ADD', 'DELETE', 'REMOVE'];
    const parts = {};
    _.forEach(sections, sec => {
        const part1 = extractSection(exp1, sec);
        const part2 = extractSection(exp2, sec);
        if (part1 || part2) {
            const items1 = part1 ? part1.replace(`${sec}`, '').split(',') : [];
            const items2 = part2 ? part2.replace(`${sec}`, '').split(',') : [];
            parts[sec] = _.uniq([...items1, ...items2].map(item => {
                return _.trim(item, TRIM);
            }));
        }
    });
    let result = _.trim(_.map(sections, sec => {
        if (parts[sec] && parts[sec].length > 0) {
            return `${sec} ${_.join(parts[sec], ', ')}`;
        }
        return '';
    })
        .filter(Boolean)
        .join(' '));
    if (_.isEmpty(result)) {
        const combinedExp = _.trim(`${exp1}, ${exp2}`, TRIM);
        result = combinedExp ? `SET ${combinedExp}` : '';
    }
    return result;
};
class Dynamodb {
    constructor(opts = CONSTRUCTOR_OPTS) {
        opts = _.defaults({}, opts, CONSTRUCTOR_OPTS);
        this.client = DynamoDBDocumentClient.from(new DynamoDBClient({
            credentials: {
                accessKeyId: opts.accessKeyId,
                secretAccessKey: opts.secretAccessKey
            },
            region: opts.region
        }));
        this.indexes = opts.indexes || [];
        this.schema = opts.schema;
        this.table = opts.table;
        this.timestamps = _.defaults({}, opts.timestamps, CONSTRUCTOR_TIMESTAMP_OPTS);
    }
    async batchWrite(items) {
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
            await this.client.send(new BatchWriteCommand({
                RequestItems: {
                    [this.table]: _.map(chunk, item => {
                        return {
                            PutRequest: { Item: item }
                        };
                    })
                }
            }));
        }
        return items;
    }
    async batchDelete(item, opts = BATCH_DELETE_OPTS) {
        opts = _.defaults({}, opts, BATCH_DELETE_OPTS);
        const del = async (items) => {
            const chunkItems = _.map(items, item => {
                return _.pick(item, [this.schema.partition, this.schema.sort]);
            });
            const chunks = _.chunk(chunkItems, 25);
            for (const chunk of chunks) {
                await this.client.send(new BatchWriteCommand({
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
                }));
            }
        };
        const { items } = await this.fetch(item, {
            ...opts,
            all: true,
            onChunk: async ({ items }) => {
                await del(items);
            }
        });
        return items;
    }
    async delete(item, opts = DELETE_OPTS) {
        opts = _.defaults({}, opts, DELETE_OPTS);
        const current = await this.get(item, opts);
        if (!current) {
            return null;
        }
        let conditionExpression = '(attribute_exists(#__ts) AND #__ts = :__ts)';
        if (opts.conditionExpression) {
            conditionExpression = concatConditionExpression(conditionExpression, opts.conditionExpression);
        }
        const res = await this.client.send(new DeleteCommand({
            ConditionExpression: conditionExpression,
            ExpressionAttributeNames: { ...opts.attributeNames, '#__ts': '__ts' },
            ExpressionAttributeValues: { ...opts.attributeValues, ':__ts': current.__ts },
            Key: {
                [this.schema.partition]: current[this.schema.partition],
                [this.schema.sort]: current[this.schema.sort]
            },
            ReturnValues: 'ALL_OLD',
            TableName: this.table
        }));
        return res.Attributes;
    }
    async fetch(item, opts = FETCH_OPTS) {
        opts = _.defaults({}, opts, FETCH_OPTS);
        let queryParams = {
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
                }
                else {
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
                res = await this.client.send(new QueryCommand({
                    ...queryParams,
                    ExclusiveStartKey: res.LastEvaluatedKey
                }));
                if (_.isFunction(opts.onChunk)) {
                    opts.onChunk({
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
            items: items,
            lastEvaluatedKey: res.LastEvaluatedKey || null
        };
    }
    async get(item, opts = GET_OPTS) {
        opts = _.defaults({}, opts, GET_OPTS);
        const res = await this.fetch(item, {
            ...opts,
            limit: 1
        });
        return _.size(res.items) > 0 ? res.items[0] : null;
    }
    async put(item, opts = PUT_OPTS) {
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
        const putParams = {
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
        return item;
    }
    optimisticResolveSchema(item) {
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
    async update(item, opts = UPDATE_OPTS) {
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
            const res = await this.client.send(new UpdateCommand({
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
            }));
            return res.Attributes;
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
    async updateWithTransaction(item, current, opts) {
        const deleteParams = {
            Delete: {
                Key: {
                    [this.schema.partition]: current[this.schema.partition],
                    [this.schema.sort]: current[this.schema.sort]
                },
                TableName: this.table
            }
        };
        const putParams = {
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
        await this.client.send(new TransactWriteCommand({
            TransactItems: [deleteParams, { Put: putParams }]
        }));
        return item;
    }
    async createTable() {
        try {
            return await this.client.send(new DescribeTableCommand({
                TableName: this.table
            }));
        }
        catch (err) {
            const inexistentTable = err.message.includes('resource not found');
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
                });
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
                });
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
                });
                const localIndexesDefinitions = _.map(li, index => {
                    return {
                        AttributeName: index.sort,
                        AttributeType: index.type
                    };
                });
                const commandInput = {
                    AttributeDefinitions: _.uniqBy([
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
                    ], 'AttributeName'),
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
        return {};
    }
}
export { concatConditionExpression, concatUpdateExpression };
export default Dynamodb;
