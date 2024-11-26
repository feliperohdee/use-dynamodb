import _ from 'lodash';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, Mock, vi } from 'vitest';

import Db, { concatConditionExpression, concatUpdateExpression } from './index';

type DbRecord = {
	foo: string;
	gsiPk: string;
	gsiSk: string;
	lsiSk: string;
	pk: string;
	sk: string;
};

const createItems = (count: number) => {
	return _.times(count, i => {
		return {
			foo: `foo-${i}`,
			gsiPk: `gsi-pk-${i % 2}`,
			gsiSk: `gsi-sk-${i}`,
			lsiSk: `lsi-sk-${i}`,
			sk: `sk-${i}`,
			pk: `pk-${i % 2}`
		};
	});
};

const factory = ({
	onChange
}: {
	onChange: Mock
}) => {
	return new Db<DbRecord>({
		accessKeyId: process.env.AWS_ACCESS_KEY || '',
		region: 'us-east-1',
		secretAccessKey: process.env.AWS_SECRET_KEY || '',
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
		],
		onChange,
		schema: { partition: 'pk', sort: 'sk' },
		table: 'use-dynamodb-spec'
	});
};

describe('/index.ts', () => {
	let db: Db<DbRecord>;
	let onChangeMock: Mock;

	beforeAll(async () => {
		onChangeMock = vi.fn();
		db = factory({ onChange: onChangeMock });
		
		await db.createTable();
	});

	beforeEach(() => {
		onChangeMock = vi.fn();
		db = factory({ onChange: onChangeMock });
	});

	describe('concatConditionExpression', () => {
		it('should works', () => {
			expect(concatConditionExpression('a  ', '  b')).toEqual('a AND b');
			expect(concatConditionExpression('a  ', '  OR b')).toEqual('a OR b');
		});
	});

	describe('concatUpdateExpression', () => {
		it('should works', () => {
			expect(concatUpdateExpression('#a = :a,', '')).toEqual('SET #a = :a');
			expect(concatUpdateExpression('#a = :a,', 'b = :b')).toEqual('SET #a = :a, b = :b');
			expect(concatUpdateExpression('SET #a = :a,', 'SET b = :b,c = :c,')).toEqual('SET #a = :a, b = :b, c = :c');
			expect(concatUpdateExpression('SET #a = :a,', 'ADD d SET b = :b,c = :c,')).toEqual('SET #a = :a, b = :b, c = :c ADD d');
		});
	});

	describe('createTable', () => {
		it('should works', async () => {
			const res = await db.createTable();

			if ('Table' in res) {
				expect(res.Table?.TableName).toEqual('use-dynamodb-spec');
			} else if ('TableDescription' in res) {
				expect(res.TableDescription?.TableName).toEqual('use-dynamodb-spec');
			} else {
				throw new Error('Table not created');
			}
		});
	});

	describe('batchWrite / batchDelete / deleteMany', () => {
		beforeEach(async () => {
			vi.spyOn(db, 'query');
			vi.spyOn(db.client, 'send');
		});

		afterAll(async () => {
			await Promise.all([
				db.deleteMany({
					pk: 'pk-0'
				}),
				db.deleteMany({
					pk: 'pk-1'
				})
			]);
		});

		it('should batch write and batch delete', async () => {
			const wroteItems = await db.batchWrite(createItems(52));

			expect(
				_.every(wroteItems, item => {
					return _.isNumber(item.__ts);
				})
			).toBeTruthy();

			expect(db.client.send).toHaveBeenCalledTimes(3);
			vi.mocked(db.client.send).mockClear();

			const deleteItems = await Promise.all([
				db.deleteMany({
					pk: 'pk-0'
				}),
				db.deleteMany(
					{
						pk: 'pk-1'
					},
					{
						attributeNames: { '#sk': 'sk' },
						attributeValues: { ':from': 'sk-0', ':to': 'sk-999' },
						expression: '#sk BETWEEN :from AND :to'
					}
				)
			]);

			expect(db.client.send).toHaveBeenCalledTimes(6);
			expect(db.query).toHaveBeenCalledTimes(2);
			expect(db.query).toHaveBeenCalledWith(
				{
					pk: 'pk-0'
				},
				{
					all: true,
					attributeNames: {},
					attributeValues: {},
					expression: '',
					filterExpression: '',
					index: '',
					onChunk: expect.any(Function),
					prefix: false
				}
			);
			expect(db.query).toHaveBeenCalledWith(
				{
					pk: 'pk-1'
				},
				{
					all: true,
					attributeNames: { '#sk': 'sk' },
					attributeValues: { ':from': 'sk-0', ':to': 'sk-999' },
					expression: '#sk BETWEEN :from AND :to',
					filterExpression: '',
					index: '',
					onChunk: expect.any(Function),
					prefix: false
				}
			);

			expect(deleteItems[0]).toHaveLength(26);
			expect(deleteItems[1]).toHaveLength(26);

			const res = await Promise.all([
				db.query({
					pk: 'pk-0'
				}),
				db.query({
					pk: 'pk-1'
				})
			]);

			expect(res[0].items).toHaveLength(0);
			expect(res[1].items).toHaveLength(0);
			expect(onChangeMock).toHaveBeenCalledTimes(3);
		});
	});

	describe('delete', () => {
		beforeEach(async () => {
			await db.batchWrite(createItems(1));

			vi.spyOn(db, 'get');
			vi.spyOn(db.client, 'send');
		});

		afterAll(async () => {
			await Promise.all([
				db.deleteMany({
					pk: 'pk-0'
				}),
				db.deleteMany({
					pk: 'pk-1'
				})
			]);
		});

		it('should return null if item not found', async () => {
			const item = await db.delete({
				pk: 'pk-0',
				sk: 'sk-100'
			});

			expect(item).toBeNull();
		});

		it('should delete', async () => {
			const item = await db.delete({
				pk: 'pk-0',
				sk: 'sk-0'
			});

			expect(db.get).toHaveBeenCalledWith(
				{
					pk: 'pk-0',
					sk: 'sk-0'
				},
				{
					attributeNames: {},
					attributeValues: {},
					conditionExpression: '',
					filterExpression: '',
					index: '',
					prefix: false
				}
			);

			expect(db.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConditionExpression: '(attribute_exists(#__ts) AND #__ts = :__ts)',
						ExpressionAttributeNames: {
							'#__ts': '__ts'
						},
						ExpressionAttributeValues: {
							':__ts': expect.any(Number)
						},
						Key: {
							pk: 'pk-0',
							sk: 'sk-0'
						},
						ReturnValues: 'ALL_OLD',
						TableName: 'use-dynamodb-spec'
					})
				})
			);

			expect(item).toEqual(
				expect.objectContaining({
					foo: 'foo-0',
					gsiPk: 'gsi-pk-0',
					gsiSk: 'gsi-sk-0',
					lsiSk: 'lsi-sk-0',
					sk: 'sk-0',
					pk: 'pk-0'
				})
			);

			expect(onChangeMock).toHaveBeenCalledTimes(2);
		});

		it('should delete by prefix', async () => {
			const item = await db.delete(
				{
					pk: 'pk-0',
					sk: 'sk-'
				},
				{
					prefix: true
				}
			);

			expect(db.get).toHaveBeenCalledWith(
				{
					pk: 'pk-0',
					sk: 'sk-'
				},
				{
					attributeNames: {},
					attributeValues: {},
					conditionExpression: '',
					filterExpression: '',
					index: '',
					prefix: true
				}
			);

			expect(db.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConditionExpression: '(attribute_exists(#__ts) AND #__ts = :__ts)',
						ExpressionAttributeNames: {
							'#__ts': '__ts'
						},
						ExpressionAttributeValues: {
							':__ts': expect.any(Number)
						},
						Key: {
							pk: 'pk-0',
							sk: 'sk-0'
						},
						ReturnValues: 'ALL_OLD',
						TableName: 'use-dynamodb-spec'
					})
				})
			);

			expect(item).toEqual(
				expect.objectContaining({
					foo: 'foo-0',
					gsiPk: 'gsi-pk-0',
					gsiSk: 'gsi-sk-0',
					lsiSk: 'lsi-sk-0',
					pk: 'pk-0',
					sk: 'sk-0'
				})
			);

			expect(onChangeMock).toHaveBeenCalledTimes(2);
		});

		it('should delete by local secondary index', async () => {
			const item = await db.delete({
				lsiSk: 'lsi-sk-0',
				pk: 'pk-0'
			});

			expect(db.get).toHaveBeenCalledWith(
				{
					lsiSk: 'lsi-sk-0',
					pk: 'pk-0'
				},
				{
					attributeNames: {},
					attributeValues: {},
					conditionExpression: '',
					filterExpression: '',
					index: '',
					prefix: false
				}
			);

			expect(db.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConditionExpression: '(attribute_exists(#__ts) AND #__ts = :__ts)',
						ExpressionAttributeNames: {
							'#__ts': '__ts'
						},
						ExpressionAttributeValues: {
							':__ts': expect.any(Number)
						},
						Key: {
							pk: 'pk-0',
							sk: 'sk-0'
						},
						ReturnValues: 'ALL_OLD',
						TableName: 'use-dynamodb-spec'
					})
				})
			);

			expect(item).toEqual(
				expect.objectContaining({
					foo: 'foo-0',
					gsiPk: 'gsi-pk-0',
					gsiSk: 'gsi-sk-0',
					lsiSk: 'lsi-sk-0',
					pk: 'pk-0',
					sk: 'sk-0'
				})
			);

			expect(onChangeMock).toHaveBeenCalledTimes(2);
		});

		it('should delete by global secondary index', async () => {
			const item = await db.delete({
				gsiPk: 'gsi-pk-0',
				gsiSk: 'gsi-sk-0'
			});

			expect(db.get).toHaveBeenCalledWith(
				{
					gsiPk: 'gsi-pk-0',
					gsiSk: 'gsi-sk-0'
				},
				{
					attributeNames: {},
					attributeValues: {},
					conditionExpression: '',
					filterExpression: '',
					index: '',
					prefix: false
				}
			);

			expect(db.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConditionExpression: '(attribute_exists(#__ts) AND #__ts = :__ts)',
						ExpressionAttributeNames: {
							'#__ts': '__ts'
						},
						ExpressionAttributeValues: {
							':__ts': expect.any(Number)
						},
						Key: {
							pk: 'pk-0',
							sk: 'sk-0'
						},
						ReturnValues: 'ALL_OLD',
						TableName: 'use-dynamodb-spec'
					})
				})
			);

			expect(item).toEqual(
				expect.objectContaining({
					foo: 'foo-0',
					gsiPk: 'gsi-pk-0',
					gsiSk: 'gsi-sk-0',
					lsiSk: 'lsi-sk-0',
					pk: 'pk-0',
					sk: 'sk-0'
				})
			);

			expect(onChangeMock).toHaveBeenCalledTimes(2);
		});
	});

	describe('get', () => {
		beforeAll(async () => {
			await db.batchWrite(createItems(1));
		});

		afterAll(async () => {
			await Promise.all([
				db.deleteMany({
					pk: 'pk-0'
				}),
				db.deleteMany({
					pk: 'pk-1'
				})
			]);
		});

		beforeEach(() => {
			vi.spyOn(db, 'query');
		});

		it('should return null if not found', async () => {
			const item = await db.get({
				pk: 'pk-0',
				sk: 'sk-100'
			});

			expect(item).toBeNull();
		});

		it('should get', async () => {
			const item = await db.get({
				pk: 'pk-0',
				sk: 'sk-0'
			});

			expect(db.query).toHaveBeenCalledWith(
				{
					pk: 'pk-0',
					sk: 'sk-0'
				},
				{
					attributeNames: {},
					attributeValues: {},
					consistentRead: false,
					filterExpression: '',
					index: '',
					limit: 1,
					prefix: false,
					select: []
				}
			);

			expect(item).toEqual(
				expect.objectContaining({
					foo: 'foo-0',
					gsiPk: 'gsi-pk-0',
					gsiSk: 'gsi-sk-0',
					lsiSk: 'lsi-sk-0',
					pk: 'pk-0',
					sk: 'sk-0'
				})
			);
		});

		it('should get with options', async () => {
			const item = await db.get(
				{
					pk: 'pk-0',
					sk: 'sk-0'
				},
				{
					attributeNames: { '#foo': 'foo' },
					attributeValues: { ':foo': 'foo-0' },
					filterExpression: '#foo = :foo'
				}
			);

			expect(db.query).toHaveBeenCalledWith(
				{
					pk: 'pk-0',
					sk: 'sk-0'
				},
				{
					attributeNames: { '#foo': 'foo' },
					attributeValues: { ':foo': 'foo-0' },
					consistentRead: false,
					filterExpression: '#foo = :foo',
					index: '',
					limit: 1,
					prefix: false,
					select: []
				}
			);

			expect(item).toEqual(
				expect.objectContaining({
					foo: 'foo-0',
					gsiPk: 'gsi-pk-0',
					gsiSk: 'gsi-sk-0',
					lsiSk: 'lsi-sk-0',
					pk: 'pk-0',
					sk: 'sk-0'
				})
			);
		});

		it('should get with select', async () => {
			const item = await db.get(
				{
					pk: 'pk-0',
					sk: 'sk-0'
				},
				{
					select: ['foo', 'gsiPk']
				}
			);

			expect(db.query).toHaveBeenCalledWith(
				{
					pk: 'pk-0',
					sk: 'sk-0'
				},
				{
					attributeNames: {},
					attributeValues: {},
					consistentRead: false,
					filterExpression: '',
					index: '',
					limit: 1,
					prefix: false,
					select: ['foo', 'gsiPk']
				}
			);

			expect(item).toEqual(
				expect.objectContaining({
					foo: 'foo-0',
					gsiPk: 'gsi-pk-0'
				})
			);
		});
	});

	describe('optimisticResolveSchema', () => {
		it('should resolve', () => {
			const { index, schema } = db.optimisticResolveSchema({
				pk: 'pk-0',
				sk: 'sk-0'
			});

			expect(index).toEqual('sort');
			expect(schema).toEqual({
				partition: 'pk',
				sort: 'sk'
			});
		});

		it('should resolve only partition', () => {
			const { index, schema } = db.optimisticResolveSchema({
				pk: 'pk-0'
			});

			expect(index).toEqual('');
			expect(schema).toEqual({
				partition: 'pk',
				sort: ''
			});
		});

		it('should resolve by local secondary index', () => {
			const { index, schema } = db.optimisticResolveSchema({
				pk: 'pk-0',
				lsiSk: 'lsi-sk-0'
			});

			expect(index).toEqual('ls-index');
			expect(schema).toEqual({
				partition: 'pk',
				sort: 'lsiSk'
			});
		});

		it('should resolve by global secondary index', () => {
			const { index, schema } = db.optimisticResolveSchema({
				gsiPk: 'gsi-pk-0',
				gsiSk: 'gsi-sk-0'
			});

			expect(index).toEqual('gs-index');
			expect(schema).toEqual({
				partition: 'gsiPk',
				sort: 'gsiSk'
			});
		});

		it('should resolve only partition by global secondary index', () => {
			const { index, schema } = db.optimisticResolveSchema({
				gsiPk: 'gsi-pk-0'
			});

			expect(index).toEqual('gs-index');
			expect(schema).toEqual({
				partition: 'gsiPk',
				sort: ''
			});
		});
	});

	describe('put', () => {
		beforeEach(() => {
			vi.spyOn(db.client, 'send');
		});

		afterAll(async () => {
			await Promise.all([
				db.deleteMany({
					pk: 'pk-0'
				}),
				db.deleteMany({
					pk: 'pk-1'
				})
			]);
		});

		it('should put', async () => {
			const item = await db.put({
				sk: 'sk-0',
				pk: 'pk-0'
			});

			expect(db.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConditionExpression: '(attribute_not_exists(#__pk))',
						ExpressionAttributeNames: { '#__pk': 'pk' },
						Item: {
							__createdAt: expect.any(String),
							__ts: expect.any(Number),
							__updatedAt: expect.any(String),
							pk: 'pk-0',
							sk: 'sk-0'
						},
						TableName: 'use-dynamodb-spec'
					})
				})
			);

			expect(item.__createdAt).toEqual(item.__updatedAt);
			expect(item).toEqual(
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-0'
				})
			);

			expect(onChangeMock).toHaveBeenCalledOnce();
		});

		it('should throw on overwrite', async () => {
			try {
				await db.put({
					pk: 'pk-0',
					sk: 'sk-0'
				});

				throw new Error('expected to throw');
			} catch (err) {
				expect((err as Error).name).toEqual('ConditionalCheckFailedException');
			}
		});

		it('should put ovewriting', async () => {
			const item = await db.get({
				pk: 'pk-0',
				sk: 'sk-0'
			});

			const overwriteItem = await db.put(
				{
					pk: 'pk-0',
					sk: 'sk-0'
				},
				{
					overwrite: true
				}
			);

			expect(db.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						Item: {
							__createdAt: expect.any(String),
							__ts: expect.any(Number),
							__updatedAt: expect.any(String),
							pk: 'pk-0',
							sk: 'sk-0'
						},
						TableName: 'use-dynamodb-spec'
					})
				})
			);

			expect(overwriteItem.__ts).toBeGreaterThan(item!.__ts);
			expect(overwriteItem.__createdAt).not.toEqual(item!.__createdAt);
			expect(overwriteItem.__createdAt).toEqual(overwriteItem.__updatedAt);
			expect(overwriteItem).toEqual(
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-0'
				})
			);

			expect(onChangeMock).toHaveBeenCalledOnce();
		});

		it('should put with options', async () => {
			const item = await db.put(
				{
					pk: 'pk-0',
					sk: 'sk-1'
				},
				{
					attributeNames: { '#foo': 'foo' },
					attributeValues: { ':foo': 'foo-0' },
					conditionExpression: '#foo <> :foo',
					overwrite: false
				}
			);

			expect(db.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConditionExpression: '(attribute_not_exists(#__pk)) AND #foo <> :foo',
						ExpressionAttributeNames: { '#foo': 'foo', '#__pk': 'pk' },
						ExpressionAttributeValues: { ':foo': 'foo-0' },
						Item: {
							__createdAt: expect.any(String),
							__ts: expect.any(Number),
							__updatedAt: expect.any(String),
							pk: 'pk-0',
							sk: 'sk-1'
						},
						TableName: 'use-dynamodb-spec'
					})
				})
			);

			expect(item.__createdAt).toEqual(item.__updatedAt);
			expect(item).toEqual(
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-1'
				})
			);

			expect(onChangeMock).toHaveBeenCalledOnce();
		});
	});

	describe('query', () => {
		beforeAll(async () => {
			await db.batchWrite(createItems(10));
		});

		afterAll(async () => {
			await Promise.all([
				db.deleteMany({
					pk: 'pk-0'
				}),
				db.deleteMany({
					pk: 'pk-1'
				})
			]);
		});

		beforeEach(() => {
			vi.spyOn(db.client, 'send');
		});

		it('should query with partition', async () => {
			const { count, lastEvaluatedKey } = await db.query({
				pk: 'pk-0'
			});

			expect(db.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConsistentRead: false,
						ExpressionAttributeNames: {
							'#__pk': 'pk'
						},
						ExpressionAttributeValues: {
							':__pk': 'pk-0'
						},
						KeyConditionExpression: '#__pk = :__pk',
						TableName: 'use-dynamodb-spec'
					})
				})
			);

			expect(count).toEqual(5);
			expect(lastEvaluatedKey).toBeNull();
		});

		it('should query by partition/sort', async () => {
			const { count, lastEvaluatedKey } = await db.query({
				pk: 'pk-0',
				sk: 'sk-0'
			});

			expect(db.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConsistentRead: false,
						ExpressionAttributeNames: {
							'#__pk': 'pk',
							'#__sk': 'sk'
						},
						ExpressionAttributeValues: {
							':__pk': 'pk-0',
							':__sk': 'sk-0'
						},
						KeyConditionExpression: '#__pk = :__pk AND #__sk = :__sk',
						TableName: 'use-dynamodb-spec'
					})
				})
			);

			expect(count).toEqual(1);
			expect(lastEvaluatedKey).toBeNull();
		});

		it('should query by partition/sort with prefix', async () => {
			const { count, lastEvaluatedKey } = await db.query(
				{
					pk: 'pk-0',
					sk: 'sk-'
				},
				{
					prefix: true
				}
			);

			expect(db.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConsistentRead: false,
						ExpressionAttributeNames: {
							'#__pk': 'pk',
							'#__sk': 'sk'
						},
						ExpressionAttributeValues: {
							':__pk': 'pk-0',
							':__sk': 'sk-'
						},
						KeyConditionExpression: '#__pk = :__pk AND begins_with(#__sk, :__sk)',
						TableName: 'use-dynamodb-spec'
					})
				})
			);

			expect(count).toEqual(5);
			expect(lastEvaluatedKey).toBeNull();
		});

		it('should query by local secondary index', async () => {
			const { count, lastEvaluatedKey } = await db.query({
				pk: 'pk-0',
				lsiSk: 'lsi-sk-0'
			});

			expect(db.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConsistentRead: false,
						ExpressionAttributeNames: {
							'#__pk': 'pk',
							'#__sk': 'lsiSk'
						},
						ExpressionAttributeValues: {
							':__pk': 'pk-0',
							':__sk': 'lsi-sk-0'
						},
						IndexName: 'ls-index',
						KeyConditionExpression: '#__pk = :__pk AND #__sk = :__sk',
						TableName: 'use-dynamodb-spec'
					})
				})
			);

			expect(count).toEqual(1);
			expect(lastEvaluatedKey).toBeNull();
		});

		it('should query by global secondary index', async () => {
			const { count, lastEvaluatedKey } = await db.query({
				gsiPk: 'gsi-pk-0',
				gsiSk: 'gsi-sk-0'
			});

			expect(db.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConsistentRead: false,
						ExpressionAttributeNames: {
							'#__pk': 'gsiPk',
							'#__sk': 'gsiSk'
						},
						ExpressionAttributeValues: {
							':__pk': 'gsi-pk-0',
							':__sk': 'gsi-sk-0'
						},
						IndexName: 'gs-index',
						KeyConditionExpression: '#__pk = :__pk AND #__sk = :__sk',
						TableName: 'use-dynamodb-spec'
					})
				})
			);

			expect(count).toEqual(1);
			expect(lastEvaluatedKey).toBeNull();
		});

		it('should query by global secondary index with partition', async () => {
			const { count, lastEvaluatedKey } = await db.query({
				gsiPk: 'gsi-pk-0'
			});

			expect(db.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConsistentRead: false,
						ExpressionAttributeNames: {
							'#__pk': 'gsiPk'
						},
						ExpressionAttributeValues: {
							':__pk': 'gsi-pk-0'
						},
						IndexName: 'gs-index',
						KeyConditionExpression: '#__pk = :__pk',
						TableName: 'use-dynamodb-spec'
					})
				})
			);

			expect(count).toEqual(5);
			expect(lastEvaluatedKey).toBeNull();
		});

		it('should query by custom expression', async () => {
			const { count, lastEvaluatedKey } = await db.query(
				{
					pk: 'pk-0'
				},
				{
					attributeNames: { '#lsiSk': 'lsiSk' },
					attributeValues: { ':from': 'lsi-sk-0', ':to': 'lsi-sk-3' },
					index: 'ls-index',
					expression: ' #lsiSk BETWEEN :from AND :to'
				}
			);

			expect(db.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConsistentRead: false,
						ExpressionAttributeNames: {
							'#__pk': 'pk',
							'#lsiSk': 'lsiSk'
						},
						ExpressionAttributeValues: {
							':__pk': 'pk-0',
							':from': 'lsi-sk-0',
							':to': 'lsi-sk-3'
						},
						IndexName: 'ls-index',
						KeyConditionExpression: '#__pk = :__pk AND #lsiSk BETWEEN :from AND :to',
						TableName: 'use-dynamodb-spec'
					})
				})
			);

			expect(count).toEqual(2);
			expect(lastEvaluatedKey).toBeNull();
		});

		it('should query by filterExpression', async () => {
			const { count, lastEvaluatedKey } = await db.query(
				{
					pk: 'pk-0'
				},
				{
					attributeNames: { '#foo': 'foo' },
					attributeValues: { ':foo': 'foo-0' },
					filterExpression: '#foo = :foo'
				}
			);

			expect(db.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConsistentRead: false,
						ExpressionAttributeNames: {
							'#__pk': 'pk',
							'#foo': 'foo'
						},
						ExpressionAttributeValues: {
							':__pk': 'pk-0',
							':foo': 'foo-0'
						},
						FilterExpression: '#foo = :foo',
						KeyConditionExpression: '#__pk = :__pk',
						TableName: 'use-dynamodb-spec'
					})
				})
			);

			expect(count).toEqual(1);
			expect(lastEvaluatedKey).toBeNull();
		});

		it('should query with limit/startKey', async () => {
			const { count, lastEvaluatedKey } = await db.query(
				{
					pk: 'pk-0'
				},
				{
					limit: 1
				}
			);

			expect(db.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConsistentRead: false,
						ExpressionAttributeNames: {
							'#__pk': 'pk'
						},
						ExpressionAttributeValues: {
							':__pk': 'pk-0'
						},
						KeyConditionExpression: '#__pk = :__pk',
						Limit: 1,
						TableName: 'use-dynamodb-spec'
					})
				})
			);

			expect(count).toEqual(1);
			expect(lastEvaluatedKey).toEqual({ pk: 'pk-0', sk: 'sk-0' });

			const { count: count2, lastEvaluatedKey: lastEvaluatedKey2 } = await db.query(
				{
					pk: 'pk-0'
				},
				{
					startKey: lastEvaluatedKey
				}
			);

			expect(db.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConsistentRead: false,
						ExpressionAttributeNames: {
							'#__pk': 'pk'
						},
						ExpressionAttributeValues: {
							':__pk': 'pk-0'
						},
						ExclusiveStartKey: { pk: 'pk-0', sk: 'sk-0' },
						KeyConditionExpression: '#__pk = :__pk',
						TableName: 'use-dynamodb-spec'
					})
				})
			);

			expect(count2).toEqual(4);
			expect(lastEvaluatedKey2).toBeNull();
		});

		it('should query all with limit/startKey and onChunk', async () => {
			const onChunk = vi.fn();
			const { count, lastEvaluatedKey } = await db.query(
				{
					pk: 'pk-0'
				},
				{
					all: true,
					onChunk,
					limit: 2
				}
			);

			expect(db.client.send).toHaveBeenCalledTimes(3);
			expect(onChunk).toHaveBeenCalledTimes(3);
			expect(onChunk).toHaveBeenCalledWith({
				count: 2,
				items: expect.any(Array)
			});
			expect(onChunk).toHaveBeenCalledWith({
				count: 1,
				items: expect.any(Array)
			});

			expect(count).toEqual(5);
			expect(lastEvaluatedKey).toBeNull();
		});

		it('should query consistent', async () => {
			const { count } = await db.query(
				{
					pk: 'pk-0'
				},
				{
					consistentRead: true
				}
			);

			expect(db.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConsistentRead: true,
						ExpressionAttributeNames: {
							'#__pk': 'pk'
						},
						ExpressionAttributeValues: {
							':__pk': 'pk-0'
						},
						KeyConditionExpression: '#__pk = :__pk',
						TableName: 'use-dynamodb-spec'
					})
				})
			);

			expect(count).toEqual(5);
		});

		it('should query with select', async () => {
			const { count, items } = await db.query(
				{
					pk: 'pk-0'
				},
				{
					select: ['foo', 'gsiPk']
				}
			);

			expect(db.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ProjectionExpression: '#__pe1, #__pe2',
						ExpressionAttributeNames: {
							'#__pe1': 'foo',
							'#__pe2': 'gsiPk',
							'#__pk': 'pk'
						},
						ExpressionAttributeValues: {
							':__pk': 'pk-0'
						},
						KeyConditionExpression: '#__pk = :__pk',
						TableName: 'use-dynamodb-spec'
					})
				})
			);

			expect(count).toEqual(5);
			expect(items[0]).toEqual(
				expect.objectContaining({
					foo: 'foo-0',
					gsiPk: 'gsi-pk-0'
				})
			);
		});
	});

	describe('scan', () => {
		beforeAll(async () => {
			await db.batchWrite(createItems(10));
		});

		afterAll(async () => {
			await Promise.all([
				db.deleteMany({
					pk: 'pk-0'
				}),
				db.deleteMany({
					pk: 'pk-1'
				})
			]);
		});

		beforeEach(() => {
			vi.spyOn(db.client, 'send');
		});

		it('should scan with limit/startKey', async () => {
			const { count, lastEvaluatedKey } = await db.scan({
				limit: 1
			});

			expect(db.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConsistentRead: false,
						Limit: 1,
						TableName: 'use-dynamodb-spec'
					})
				})
			);

			expect(count).toEqual(1);
			expect(lastEvaluatedKey).not.toBeNull();

			const { count: count2, lastEvaluatedKey: lastEvaluatedKey2 } = await db.scan({
				startKey: lastEvaluatedKey
			});

			expect(db.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConsistentRead: false,
						ExclusiveStartKey: lastEvaluatedKey,
						TableName: 'use-dynamodb-spec'
					})
				})
			);

			expect(count2).toEqual(9);
			expect(lastEvaluatedKey2).toBeNull();
		});

		it('should scan all with limit/startKey and onChunk', async () => {
			const onChunk = vi.fn();
			const { count, lastEvaluatedKey } = await db.scan({
				all: true,
				onChunk,
				limit: 2
			});

			expect(db.client.send).toHaveBeenCalledTimes(6);
			expect(onChunk).toHaveBeenCalledTimes(6);
			expect(onChunk).toHaveBeenCalledWith({
				count: 2,
				items: expect.any(Array)
			});
			expect(onChunk).toHaveBeenCalledWith({
				count: 2,
				items: expect.any(Array)
			});

			expect(count).toEqual(10);
			expect(lastEvaluatedKey).toBeNull();
		});

		it('should scan with select', async () => {
			const { count, items } = await db.scan({
				select: ['foo', 'gsiPk']
			});

			expect(db.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ProjectionExpression: '#__pe1, #__pe2',
						ExpressionAttributeNames: {
							'#__pe1': 'foo',
							'#__pe2': 'gsiPk'
						},
						TableName: 'use-dynamodb-spec'
					})
				})
			);

			expect(count).toEqual(10);
			expect(_.keys(items[0])).toEqual(expect.arrayContaining(['foo', 'gsiPk']));
		});
	});

	describe('update', () => {
		beforeEach(async () => {
			vi.spyOn(db, 'put');
			vi.spyOn(db.client, 'send');
		});

		afterEach(async () => {
			await Promise.all([
				db.deleteMany({
					pk: 'pk-0'
				}),
				db.deleteMany({
					pk: 'pk-1'
				})
			]);
		});

		it('should throw if item not found', async () => {
			try {
				await db.update(
					{
						pk: 'pk-0',
						sk: 'sk-1'
					},
					{
						updateFn: item => {
							return {
								...item,
								foo: 'foo-1'
							};
						}
					}
				);

				throw new Error('expected to throw');
			} catch (err) {
				expect((err as Error).message).toEqual('Item not found');
			}
		});

		it('should update', async () => {
			await db.batchWrite(createItems(1));

			const item = await db.update(
				{
					pk: 'pk-0',
					sk: 'sk-0'
				},
				{
					updateFn: item => {
						return {
							...item,
							foo: 'foo-1'
						};
					}
				}
			);

			expect(db.put).toHaveBeenCalledWith(
				{
					__createdAt: expect.any(String),
					__ts: expect.any(Number),
					__updatedAt: expect.any(String),
					foo: 'foo-1',
					gsiPk: 'gsi-pk-0',
					gsiSk: 'gsi-sk-0',
					lsiSk: 'lsi-sk-0',
					pk: 'pk-0',
					sk: 'sk-0'
				},
				{
					attributeNames: {
						'#__pk': 'pk',
						'#__ts': '__ts'
					},
					attributeValues: { ':__curr_ts': expect.any(Number) },
					conditionExpression: '(attribute_exists(#__pk) AND (attribute_not_exists(#__ts) OR #__ts = :__curr_ts))',
					overwrite: true
				}
			);

			expect(item.__updatedAt).not.toEqual(item.__createdAt);
			expect(item).toEqual(
				expect.objectContaining({
					foo: 'foo-1',
					gsiPk: 'gsi-pk-0',
					gsiSk: 'gsi-sk-0',
					lsiSk: 'lsi-sk-0',
					sk: 'sk-0',
					pk: 'pk-0'
				})
			);

			expect(onChangeMock).toHaveBeenCalledTimes(2);
		});

		it('should not update partition and sort', async () => {
			await db.batchWrite(createItems(1));

			try {
				await db.update(
					{
						pk: 'pk-0',
						sk: 'sk-0'
					},
					{
						updateFn: item => {
							return {
								...item,
								pk: 'pk-1',
								sk: 'sk-1',
								foo: 'foo-1'
							};
						}
					}
				);
			} catch (err) {
				expect(err.name).toContain('ConditionalCheckFailedException');
			}
		});

		it('should upsert', async () => {
			const item = await db.update(
				{
					pk: 'pk-0',
					sk: 'sk-0'
				},
				{
					updateFn: item => {
						return {
							...item,
							foo: 'foo-1'
						};
					},
					upsert: true
				}
			);

			expect(db.put).toHaveBeenCalledWith(
				{
					foo: 'foo-1',
					pk: 'pk-0',
					sk: 'sk-0'
				},
				{
					attributeNames: { '#__ts': '__ts' },
					attributeValues: { ':__curr_ts': expect.any(Number) },
					conditionExpression: '(attribute_not_exists(#__ts) OR #__ts = :__curr_ts)',
					overwrite: true
				}
			);

			expect(item.__createdAt).toEqual(item.__updatedAt);
			expect(item).toEqual(
				expect.objectContaining({
					foo: 'foo-1',
					pk: 'pk-0',
					sk: 'sk-0'
				})
			);

			expect(onChangeMock).toHaveBeenCalledOnce();
		});

		it('should update with expression', async () => {
			await db.batchWrite(createItems(1));

			const item = await db.update(
				{
					pk: 'pk-0',
					sk: 'sk-0'
				},
				{
					attributeNames: {
						'#foo': 'foo',
						'#bar': 'bar'
					},
					attributeValues: {
						':foo': 'foo-1',
						':one': 1
					},
					expression: 'SET #foo = if_not_exists(#foo, :foo) ADD #bar :one'
				}
			);

			expect(db.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConditionExpression: '(attribute_exists(#__pk) AND (attribute_not_exists(#__ts) OR #__ts = :__curr_ts))',
						ExpressionAttributeNames: {
							'#__cr': '__createdAt',
							'#__pk': 'pk',
							'#__ts': '__ts',
							'#__up': '__updatedAt',
							'#bar': 'bar',
							'#foo': 'foo'
						},
						ExpressionAttributeValues: {
							':foo': 'foo-1',
							':one': 1,
							':__cr': expect.any(String),
							':__curr_ts': expect.any(Number),
							':__ts': expect.any(Number),
							':__up': expect.any(String)
						},
						Key: {
							pk: 'pk-0',
							sk: 'sk-0'
						},
						ReturnValues: 'ALL_NEW',
						TableName: 'use-dynamodb-spec',
						UpdateExpression:
							'SET #foo = if_not_exists(#foo, :foo), #__cr = if_not_exists(#__cr, :__cr), #__ts = :__ts, #__up = :__up ADD #bar :one'
					})
				})
			);

			expect(item.__createdAt).not.toEqual(item.__updatedAt);
			expect(item).toEqual(
				expect.objectContaining({
					foo: 'foo-0',
					bar: 1,
					gsiPk: 'gsi-pk-0',
					gsiSk: 'gsi-sk-0',
					lsiSk: 'lsi-sk-0',
					pk: 'pk-0',
					sk: 'sk-0'
				})
			);

			expect(onChangeMock).toHaveBeenCalledTimes(2);
		});

		it('should upsert with expression', async () => {
			const item = await db.update(
				{
					pk: 'pk-0',
					sk: 'sk-0'
				},
				{
					attributeNames: {
						'#foo': 'foo',
						'#bar': 'bar'
					},
					attributeValues: {
						':foo': 'foo-1',
						':one': 1
					},
					expression: 'SET #foo = if_not_exists(#foo, :foo) ADD #bar :one',
					upsert: true
				}
			);

			expect(db.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConditionExpression: '(attribute_not_exists(#__ts) OR #__ts = :__curr_ts)',
						ExpressionAttributeNames: {
							'#bar': 'bar',
							'#foo': 'foo',
							'#__cr': '__createdAt',
							'#__ts': '__ts',
							'#__up': '__updatedAt'
						},
						ExpressionAttributeValues: {
							':foo': 'foo-1',
							':one': 1,
							':__cr': expect.any(String),
							':__curr_ts': expect.any(Number),
							':__ts': expect.any(Number),
							':__up': expect.any(String)
						},
						Key: {
							pk: 'pk-0',
							sk: 'sk-0'
						},
						ReturnValues: 'ALL_NEW',
						TableName: 'use-dynamodb-spec',
						UpdateExpression:
							'SET #foo = if_not_exists(#foo, :foo), #__cr = if_not_exists(#__cr, :__cr), #__ts = :__ts, #__up = :__up ADD #bar :one'
					})
				})
			);

			expect(item.__createdAt).toEqual(item.__updatedAt);
			expect(item).toEqual(
				expect.objectContaining({
					foo: 'foo-1',
					bar: 1,
					pk: 'pk-0',
					sk: 'sk-0'
				})
			);

			expect(onChangeMock).toHaveBeenCalledOnce();
		});

		it('should update partition key with transaction', async () => {
			await db.batchWrite(createItems(1));

			const item = await db.update(
				{
					pk: 'pk-0',
					sk: 'sk-0'
				},
				{
					allowUpdatePartitionAndSort: true,
					updateFn: item => {
						return {
							...item,
							pk: 'pk-1'
						};
					}
				}
			);

			expect(db.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						TransactItems: expect.arrayContaining([
							expect.objectContaining({
								Delete: expect.objectContaining({
									Key: {
										pk: 'pk-0',
										sk: 'sk-0'
									},
									TableName: 'use-dynamodb-spec'
								})
							}),
							expect.objectContaining({
								Put: expect.objectContaining({
									Item: expect.objectContaining({
										pk: 'pk-1',
										sk: 'sk-0',
										__ts: expect.any(Number)
									}),
									TableName: 'use-dynamodb-spec'
								})
							})
						])
					})
				})
			);

			expect(item).toEqual(
				expect.objectContaining({
					pk: 'pk-1',
					sk: 'sk-0'
				})
			);

			expect(onChangeMock).toHaveBeenCalledTimes(2);
		});

		it('should update sort key with transaction', async () => {
			await db.batchWrite(createItems(1));

			const item = await db.update(
				{
					pk: 'pk-0',
					sk: 'sk-0'
				},
				{
					allowUpdatePartitionAndSort: true,
					updateFn: item => {
						return {
							...item,
							sk: 'sk-1'
						};
					}
				}
			);

			expect(db.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						TransactItems: expect.arrayContaining([
							expect.objectContaining({
								Delete: expect.objectContaining({
									Key: {
										pk: 'pk-0',
										sk: 'sk-0'
									},
									TableName: 'use-dynamodb-spec'
								})
							}),
							expect.objectContaining({
								Put: expect.objectContaining({
									Item: expect.objectContaining({
										pk: 'pk-0',
										sk: 'sk-1',
										__ts: expect.any(Number)
									}),
									TableName: 'use-dynamodb-spec'
								})
							})
						])
					})
				})
			);

			expect(item).toEqual(
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-1'
				})
			);

			expect(onChangeMock).toHaveBeenCalledTimes(2);
		});
	});
});
