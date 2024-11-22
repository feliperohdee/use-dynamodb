import _ from 'lodash';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';

import Dynamodb, { concatConditionExpression, concatUpdateExpression } from './index';

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

const factory = () => {
	return new Dynamodb({
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
		schema: { partition: 'pk', sort: 'sk' },
		table: 'simple-img-new-spec'
	});
};

describe('index', () => {
	let dynamodb: Dynamodb;

	beforeAll(() => {
		dynamodb = factory();
	});

	beforeEach(() => {
		dynamodb = factory();
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
			const res = await dynamodb.createTable();

			if ('Table' in res) {
				expect(res.Table?.TableName).toEqual('simple-img-new-spec');
			} else if ('TableDescription' in res) {
				expect(res.TableDescription?.TableName).toEqual('simple-img-new-spec');
			} else {
				throw new Error('Table not created');
			}
		});
	});

	describe('batchWrite / batchDelete', () => {
		beforeEach(async () => {
			vi.spyOn(dynamodb, 'fetch');
			vi.spyOn(dynamodb.client, 'send');
		});

		afterAll(async () => {
			await Promise.all([
				dynamodb.batchDelete({
					pk: 'pk-0'
				}),
				dynamodb.batchDelete({
					pk: 'pk-1'
				})
			]);
		});

		it('should batch write and batch delete', async () => {
			const wroteItems = await dynamodb.batchWrite(createItems(52));

			expect(
				_.every(wroteItems, item => {
					return _.isNumber(item.__ts);
				})
			).toBeTruthy();

			expect(dynamodb.client.send).toHaveBeenCalledTimes(3);
			vi.mocked(dynamodb.client.send).mockClear();

			const deleteItems = await Promise.all([
				dynamodb.batchDelete({
					pk: 'pk-0'
				}),
				dynamodb.batchDelete(
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

			expect(dynamodb.client.send).toHaveBeenCalledTimes(6);
			expect(dynamodb.fetch).toHaveBeenCalledTimes(2);
			expect(dynamodb.fetch).toHaveBeenCalledWith(
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
			expect(dynamodb.fetch).toHaveBeenCalledWith(
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
				dynamodb.fetch({
					pk: 'pk-0'
				}),
				dynamodb.fetch({
					pk: 'pk-1'
				})
			]);

			expect(res[0].items).toHaveLength(0);
			expect(res[1].items).toHaveLength(0);
		});
	});

	describe('delete', () => {
		beforeEach(async () => {
			await dynamodb.batchWrite(createItems(1));

			vi.spyOn(dynamodb, 'get');
			vi.spyOn(dynamodb.client, 'send');
		});

		afterAll(async () => {
			await Promise.all([
				dynamodb.batchDelete({
					pk: 'pk-0'
				}),
				dynamodb.batchDelete({
					pk: 'pk-1'
				})
			]);
		});

		it('should return null if item not found', async () => {
			const item = await dynamodb.delete({
				pk: 'pk-0',
				sk: 'sk-100'
			});

			expect(item).toBeNull();
		});

		it('should delete', async () => {
			const item = await dynamodb.delete({
				pk: 'pk-0',
				sk: 'sk-0'
			});

			expect(dynamodb.get).toHaveBeenCalledWith(
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

			expect(dynamodb.client.send).toHaveBeenCalledWith(
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
						TableName: 'simple-img-new-spec'
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
		});

		it('should delete by prefix', async () => {
			const item = await dynamodb.delete(
				{
					pk: 'pk-0',
					sk: 'sk-'
				},
				{
					prefix: true
				}
			);

			expect(dynamodb.get).toHaveBeenCalledWith(
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

			expect(dynamodb.client.send).toHaveBeenCalledWith(
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
						TableName: 'simple-img-new-spec'
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
		});

		it('should delete by local secondary index', async () => {
			const item = await dynamodb.delete({
				lsiSk: 'lsi-sk-0',
				pk: 'pk-0'
			});

			expect(dynamodb.get).toHaveBeenCalledWith(
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

			expect(dynamodb.client.send).toHaveBeenCalledWith(
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
						TableName: 'simple-img-new-spec'
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
		});

		it('should delete by global secondary index', async () => {
			const item = await dynamodb.delete({
				gsiPk: 'gsi-pk-0',
				gsiSk: 'gsi-sk-0'
			});

			expect(dynamodb.get).toHaveBeenCalledWith(
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

			expect(dynamodb.client.send).toHaveBeenCalledWith(
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
						TableName: 'simple-img-new-spec'
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
		});
	});

	describe('fetch', () => {
		beforeAll(async () => {
			await dynamodb.batchWrite(createItems(10));
		});

		afterAll(async () => {
			await Promise.all([
				dynamodb.batchDelete({
					pk: 'pk-0'
				}),
				dynamodb.batchDelete({
					pk: 'pk-1'
				})
			]);
		});

		beforeEach(() => {
			vi.spyOn(dynamodb.client, 'send');
		});

		it('should fetch with partition', async () => {
			const { count, lastEvaluatedKey } = await dynamodb.fetch({
				pk: 'pk-0'
			});

			expect(dynamodb.client.send).toHaveBeenCalledWith(
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
						TableName: 'simple-img-new-spec'
					})
				})
			);

			expect(count).toEqual(5);
			expect(lastEvaluatedKey).toBeNull();
		});

		it('should fetch by partition/sort', async () => {
			const { count, lastEvaluatedKey } = await dynamodb.fetch({
				pk: 'pk-0',
				sk: 'sk-0'
			});

			expect(dynamodb.client.send).toHaveBeenCalledWith(
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
						TableName: 'simple-img-new-spec'
					})
				})
			);

			expect(count).toEqual(1);
			expect(lastEvaluatedKey).toBeNull();
		});

		it('should fetch by partition/sort with prefix', async () => {
			const { count, lastEvaluatedKey } = await dynamodb.fetch(
				{
					pk: 'pk-0',
					sk: 'sk-'
				},
				{
					prefix: true
				}
			);

			expect(dynamodb.client.send).toHaveBeenCalledWith(
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
						TableName: 'simple-img-new-spec'
					})
				})
			);

			expect(count).toEqual(5);
			expect(lastEvaluatedKey).toBeNull();
		});

		it('should fetch by local secondary index', async () => {
			const { count, lastEvaluatedKey } = await dynamodb.fetch({
				pk: 'pk-0',
				lsiSk: 'lsi-sk-0'
			});

			expect(dynamodb.client.send).toHaveBeenCalledWith(
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
						TableName: 'simple-img-new-spec'
					})
				})
			);

			expect(count).toEqual(1);
			expect(lastEvaluatedKey).toBeNull();
		});

		it('should fetch by global secondary index', async () => {
			const { count, lastEvaluatedKey } = await dynamodb.fetch({
				gsiPk: 'gsi-pk-0',
				gsiSk: 'gsi-sk-0'
			});

			expect(dynamodb.client.send).toHaveBeenCalledWith(
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
						TableName: 'simple-img-new-spec'
					})
				})
			);

			expect(count).toEqual(1);
			expect(lastEvaluatedKey).toBeNull();
		});

		it('should fetch by global secondary index with partition', async () => {
			const { count, lastEvaluatedKey } = await dynamodb.fetch({
				gsiPk: 'gsi-pk-0'
			});

			expect(dynamodb.client.send).toHaveBeenCalledWith(
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
						TableName: 'simple-img-new-spec'
					})
				})
			);

			expect(count).toEqual(5);
			expect(lastEvaluatedKey).toBeNull();
		});

		it('should fetch by custom expression', async () => {
			const { count, lastEvaluatedKey } = await dynamodb.fetch(
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

			expect(dynamodb.client.send).toHaveBeenCalledWith(
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
						TableName: 'simple-img-new-spec'
					})
				})
			);

			expect(count).toEqual(2);
			expect(lastEvaluatedKey).toBeNull();
		});

		it('should fetch by filterExpression', async () => {
			const { count, lastEvaluatedKey } = await dynamodb.fetch(
				{
					pk: 'pk-0'
				},
				{
					attributeNames: { '#foo': 'foo' },
					attributeValues: { ':foo': 'foo-0' },
					filterExpression: '#foo = :foo'
				}
			);

			expect(dynamodb.client.send).toHaveBeenCalledWith(
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
						TableName: 'simple-img-new-spec'
					})
				})
			);

			expect(count).toEqual(1);
			expect(lastEvaluatedKey).toBeNull();
		});

		it('should fetch with limit/startKey', async () => {
			const { count, lastEvaluatedKey } = await dynamodb.fetch(
				{
					pk: 'pk-0'
				},
				{
					limit: 1
				}
			);

			expect(dynamodb.client.send).toHaveBeenCalledWith(
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
						TableName: 'simple-img-new-spec'
					})
				})
			);

			expect(count).toEqual(1);
			expect(lastEvaluatedKey).toEqual({ pk: 'pk-0', sk: 'sk-0' });

			const { count: count2, lastEvaluatedKey: lastEvaluatedKey2 } = await dynamodb.fetch(
				{
					pk: 'pk-0'
				},
				{
					startKey: lastEvaluatedKey
				}
			);

			expect(dynamodb.client.send).toHaveBeenCalledWith(
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
						TableName: 'simple-img-new-spec'
					})
				})
			);

			expect(count2).toEqual(4);
			expect(lastEvaluatedKey2).toBeNull();
		});

		it('should fetch all with limit/startKey and onChunk', async () => {
			const onChunk = vi.fn();
			const { count, lastEvaluatedKey } = await dynamodb.fetch(
				{
					pk: 'pk-0'
				},
				{
					all: true,
					onChunk,
					limit: 2
				}
			);

			expect(dynamodb.client.send).toHaveBeenCalledTimes(3);
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

		it('should fetch consistent', async () => {
			const { count } = await dynamodb.fetch(
				{
					pk: 'pk-0'
				},
				{
					consistentRead: true
				}
			);

			expect(dynamodb.client.send).toHaveBeenCalledWith(
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
						TableName: 'simple-img-new-spec'
					})
				})
			);

			expect(count).toEqual(5);
		});

		it('should fetch with select', async () => {
			const { count, items } = await dynamodb.fetch(
				{
					pk: 'pk-0'
				},
				{
					select: ['foo', 'gsiPk']
				}
			);

			expect(dynamodb.client.send).toHaveBeenCalledWith(
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
						TableName: 'simple-img-new-spec'
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

	describe('get', () => {
		beforeAll(async () => {
			await dynamodb.batchWrite(createItems(1));
		});

		afterAll(async () => {
			await Promise.all([
				dynamodb.batchDelete({
					pk: 'pk-0'
				}),
				dynamodb.batchDelete({
					pk: 'pk-1'
				})
			]);
		});

		beforeEach(() => {
			vi.spyOn(dynamodb, 'fetch');
		});

		it('should return null if not found', async () => {
			const item = await dynamodb.get({
				pk: 'pk-0',
				sk: 'sk-100'
			});

			expect(item).toBeNull();
		});

		it('should get', async () => {
			const item = await dynamodb.get({
				pk: 'pk-0',
				sk: 'sk-0'
			});

			expect(dynamodb.fetch).toHaveBeenCalledWith(
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
			const item = await dynamodb.get(
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

			expect(dynamodb.fetch).toHaveBeenCalledWith(
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
			const item = await dynamodb.get(
				{
					pk: 'pk-0',
					sk: 'sk-0'
				},
				{
					select: ['foo', 'gsiPk']
				}
			);

			expect(dynamodb.fetch).toHaveBeenCalledWith(
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

	describe('put', () => {
		beforeEach(() => {
			vi.spyOn(dynamodb.client, 'send');
		});

		afterAll(async () => {
			await Promise.all([
				dynamodb.batchDelete({
					pk: 'pk-0'
				}),
				dynamodb.batchDelete({
					pk: 'pk-1'
				})
			]);
		});

		it('should put', async () => {
			const item = await dynamodb.put({
				sk: 'sk-0',
				pk: 'pk-0'
			});

			expect(dynamodb.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConditionExpression: '(attribute_not_exists(#__pk))',
						ExpressionAttributeNames: { '#__pk': 'pk' },
						Item: {
							__ts: expect.any(Number),
							createdAt: expect.any(String),
							pk: 'pk-0',
							sk: 'sk-0',
							updatedAt: expect.any(String)
						},
						TableName: 'simple-img-new-spec'
					})
				})
			);

			expect(item.createdAt).toEqual(item.updatedAt);
			expect(item).toEqual(
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-0'
				})
			);
		});

		it('should throw on overwrite', async () => {
			try {
				await dynamodb.put({
					pk: 'pk-0',
					sk: 'sk-0'
				});

				throw new Error('expected to throw');
			} catch (err) {
				expect((err as Error).name).toEqual('ConditionalCheckFailedException');
			}
		});

		it('should put ovewriting', async () => {
			const item = await dynamodb.get({
				pk: 'pk-0',
				sk: 'sk-0'
			});

			const overwriteItem = await dynamodb.put(
				{
					pk: 'pk-0',
					sk: 'sk-0'
				},
				{
					overwrite: true
				}
			);

			expect(dynamodb.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						Item: {
							__ts: expect.any(Number),
							createdAt: expect.any(String),
							pk: 'pk-0',
							sk: 'sk-0',
							updatedAt: expect.any(String)
						},
						TableName: 'simple-img-new-spec'
					})
				})
			);

			expect(overwriteItem.__ts).toBeGreaterThan(item!.__ts);
			expect(overwriteItem.createdAt).not.toEqual(item!.createdAt);
			expect(overwriteItem.createdAt).toEqual(overwriteItem.updatedAt);
			expect(overwriteItem).toEqual(
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-0'
				})
			);
		});

		it('should put with options', async () => {
			const item = await dynamodb.put(
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

			expect(dynamodb.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConditionExpression: '(attribute_not_exists(#__pk)) AND #foo <> :foo',
						ExpressionAttributeNames: { '#foo': 'foo', '#__pk': 'pk' },
						ExpressionAttributeValues: { ':foo': 'foo-0' },
						Item: {
							__ts: expect.any(Number),
							createdAt: expect.any(String),
							pk: 'pk-0',
							sk: 'sk-1',
							updatedAt: expect.any(String)
						},
						TableName: 'simple-img-new-spec'
					})
				})
			);

			expect(item.createdAt).toEqual(item.updatedAt);
			expect(item).toEqual(
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-1'
				})
			);
		});

		it('should put with custom timestamp field names', async () => {
			// @ts-expect-error
			dynamodb.timestamps = {
				createdAtField: 'createdDate',
				updatedAtField: 'updatedDate'
			};

			const item = await dynamodb.put({
				pk: 'pk-0',
				sk: 'sk-3'
			});

			expect(dynamodb.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConditionExpression: '(attribute_not_exists(#__pk))',
						ExpressionAttributeNames: { '#__pk': 'pk' },
						Item: {
							__ts: expect.any(Number),
							createdDate: expect.any(String),
							pk: 'pk-0',
							sk: 'sk-3',
							updatedDate: expect.any(String)
						},
						TableName: 'simple-img-new-spec'
					})
				})
			);

			expect(item.createdDate).toEqual(item.updatedDate);
			expect(item).toEqual(
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-3'
				})
			);
		});
	});

	describe('optimisticResolveSchema', () => {
		it('should resolve', () => {
			const { index, schema } = dynamodb.optimisticResolveSchema({
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
			const { index, schema } = dynamodb.optimisticResolveSchema({
				pk: 'pk-0'
			});

			expect(index).toEqual('');
			expect(schema).toEqual({
				partition: 'pk',
				sort: ''
			});
		});

		it('should resolve by local secondary index', () => {
			const { index, schema } = dynamodb.optimisticResolveSchema({
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
			const { index, schema } = dynamodb.optimisticResolveSchema({
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
			const { index, schema } = dynamodb.optimisticResolveSchema({
				gsiPk: 'gsi-pk-0'
			});

			expect(index).toEqual('gs-index');
			expect(schema).toEqual({
				partition: 'gsiPk',
				sort: ''
			});
		});
	});

	describe('scan', () => {
		beforeAll(async () => {
			await dynamodb.batchWrite(createItems(10));
		});

		afterAll(async () => {
			await Promise.all([
				dynamodb.batchDelete({
					pk: 'pk-0'
				}),
				dynamodb.batchDelete({
					pk: 'pk-1'
				})
			]);
		});

		beforeEach(() => {
			vi.spyOn(dynamodb.client, 'send');
		});

		it('should scan with limit/startKey', async () => {
			const { count, lastEvaluatedKey } = await dynamodb.scan({
				limit: 1
			});

			expect(dynamodb.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConsistentRead: false,
						Limit: 1,
						TableName: 'simple-img-new-spec'
					})
				})
			);

			expect(count).toEqual(1);
			expect(lastEvaluatedKey).not.toBeNull();

			const { count: count2, lastEvaluatedKey: lastEvaluatedKey2 } = await dynamodb.scan({
				startKey: lastEvaluatedKey
			});

			expect(dynamodb.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConsistentRead: false,
						ExclusiveStartKey: lastEvaluatedKey,
						TableName: 'simple-img-new-spec'
					})
				})
			);

			expect(count2).toEqual(9);
			expect(lastEvaluatedKey2).toBeNull();
		});

		it('should scan all with limit/startKey and onChunk', async () => {
			const onChunk = vi.fn();
			const { count, lastEvaluatedKey } = await dynamodb.scan({
				all: true,
				onChunk,
				limit: 2
			});

			expect(dynamodb.client.send).toHaveBeenCalledTimes(6);
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
			const { count, items } = await dynamodb.scan({
				select: ['foo', 'gsiPk']
			});

			expect(dynamodb.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ProjectionExpression: '#__pe1, #__pe2',
						ExpressionAttributeNames: {
							'#__pe1': 'foo',
							'#__pe2': 'gsiPk'
						},
						TableName: 'simple-img-new-spec'
					})
				})
			);

			expect(count).toEqual(10);
			expect(_.keys(items[0])).toEqual(expect.arrayContaining(['foo', 'gsiPk']));
		});
	});

	describe('update', () => {
		beforeEach(async () => {
			vi.spyOn(dynamodb, 'put');
			vi.spyOn(dynamodb.client, 'send');
		});

		afterEach(async () => {
			await Promise.all([
				dynamodb.batchDelete({
					pk: 'pk-0'
				}),
				dynamodb.batchDelete({
					pk: 'pk-1'
				})
			]);
		});

		it('should throw if item not found', async () => {
			try {
				await dynamodb.update(
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
			await dynamodb.batchWrite(createItems(1));

			const item = await dynamodb.update(
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

			expect(dynamodb.put).toHaveBeenCalledWith(
				{
					__ts: expect.any(Number),
					createdAt: expect.any(String),
					foo: 'foo-1',
					gsiPk: 'gsi-pk-0',
					gsiSk: 'gsi-sk-0',
					lsiSk: 'lsi-sk-0',
					pk: 'pk-0',
					sk: 'sk-0',
					updatedAt: expect.any(String)
				},
				{
					attributeNames: {
						'#__pk': 'pk',
						'#__ts': '__ts'
					},
					attributeValues: { ':__ts': expect.any(Number) },
					conditionExpression: '(attribute_exists(#__pk) AND (attribute_not_exists(#__ts) OR #__ts = :__ts))',
					overwrite: true
				}
			);

			expect(item.updatedAt).not.toEqual(item.createdAt);
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
		});

		it('should use custom update timestamp field', async () => {
			// @ts-expect-error
			dynamodb.timestamps = {
				createdAtField: 'createdDate',
				updatedAtField: 'updatedDate'
			};

			await dynamodb.batchWrite(createItems(1));

			const item = await dynamodb.update(
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

			expect(dynamodb.put).toHaveBeenCalledWith(
				{
					__ts: expect.any(Number),
					createdDate: expect.any(String),
					foo: 'foo-1',
					gsiPk: 'gsi-pk-0',
					gsiSk: 'gsi-sk-0',
					lsiSk: 'lsi-sk-0',
					pk: 'pk-0',
					sk: 'sk-0',
					updatedDate: expect.any(String)
				},
				{
					attributeNames: {
						'#__pk': 'pk',
						'#__ts': '__ts'
					},
					attributeValues: { ':__ts': expect.any(Number) },
					conditionExpression: '(attribute_exists(#__pk) AND (attribute_not_exists(#__ts) OR #__ts = :__ts))',
					overwrite: true
				}
			);

			expect(item.createdDate).not.toEqual(item.updatedDate);
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
		});

		it('should not update partition and sort', async () => {
			await dynamodb.batchWrite(createItems(1));

			try {
				await dynamodb.update(
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
			const item = await dynamodb.update(
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

			expect(dynamodb.put).toHaveBeenCalledWith(
				{
					foo: 'foo-1',
					pk: 'pk-0',
					sk: 'sk-0'
				},
				{
					attributeNames: { '#__ts': '__ts' },
					attributeValues: { ':__ts': expect.any(Number) },
					conditionExpression: '(attribute_not_exists(#__ts) OR #__ts = :__ts)',
					overwrite: true
				}
			);

			expect(item.createdAt).toEqual(item.updatedAt);
			expect(item).toEqual(
				expect.objectContaining({
					foo: 'foo-1',
					pk: 'pk-0',
					sk: 'sk-0'
				})
			);
		});

		it('should update with expression', async () => {
			await dynamodb.batchWrite(createItems(1));

			const item = await dynamodb.update(
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

			expect(dynamodb.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConditionExpression: '(attribute_exists(#__pk) AND (attribute_not_exists(#__ts) OR #__ts = :__ts))',
						ExpressionAttributeNames: {
							'#__cr': 'createdAt',
							'#__pk': 'pk',
							'#__ts': '__ts',
							'#__up': 'updatedAt',
							'#bar': 'bar',
							'#foo': 'foo'
						},
						ExpressionAttributeValues: {
							':foo': 'foo-1',
							':one': 1,
							':__cr': expect.any(String),
							':__ts': expect.any(Number),
							':__up': expect.any(String)
						},
						Key: {
							pk: 'pk-0',
							sk: 'sk-0'
						},
						ReturnValues: 'ALL_NEW',
						TableName: 'simple-img-new-spec',
						UpdateExpression:
							'SET #foo = if_not_exists(#foo, :foo), #__cr = if_not_exists(#__cr, :__cr), #__ts = :__ts, #__up = :__up ADD #bar :one'
					})
				})
			);

			expect(item.createdAt).not.toEqual(item.updatedAt);
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
		});

		it('should upsert with expression', async () => {
			const item = await dynamodb.update(
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

			expect(dynamodb.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConditionExpression: '(attribute_not_exists(#__ts) OR #__ts = :__ts)',
						ExpressionAttributeNames: {
							'#bar': 'bar',
							'#foo': 'foo',
							'#__cr': 'createdAt',
							'#__ts': '__ts',
							'#__up': 'updatedAt'
						},
						ExpressionAttributeValues: {
							':foo': 'foo-1',
							':one': 1,
							':__cr': expect.any(String),
							':__ts': expect.any(Number),
							':__up': expect.any(String)
						},
						Key: {
							pk: 'pk-0',
							sk: 'sk-0'
						},
						ReturnValues: 'ALL_NEW',
						TableName: 'simple-img-new-spec',
						UpdateExpression:
							'SET #foo = if_not_exists(#foo, :foo), #__cr = if_not_exists(#__cr, :__cr), #__ts = :__ts, #__up = :__up ADD #bar :one'
					})
				})
			);

			expect(item.createdAt).toEqual(item.updatedAt);
			expect(item).toEqual(
				expect.objectContaining({
					foo: 'foo-1',
					bar: 1,
					pk: 'pk-0',
					sk: 'sk-0'
				})
			);
		});

		it('should update partition key with transaction', async () => {
			await dynamodb.batchWrite(createItems(1));

			const item = await dynamodb.update(
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

			expect(dynamodb.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						TransactItems: expect.arrayContaining([
							expect.objectContaining({
								Delete: expect.objectContaining({
									Key: {
										pk: 'pk-0',
										sk: 'sk-0'
									},
									TableName: 'simple-img-new-spec'
								})
							}),
							expect.objectContaining({
								Put: expect.objectContaining({
									Item: expect.objectContaining({
										pk: 'pk-1',
										sk: 'sk-0',
										__ts: expect.any(Number)
									}),
									TableName: 'simple-img-new-spec'
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
		});

		it('should update sort key with transaction', async () => {
			await dynamodb.batchWrite(createItems(1));

			const item = await dynamodb.update(
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

			expect(dynamodb.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						TransactItems: expect.arrayContaining([
							expect.objectContaining({
								Delete: expect.objectContaining({
									Key: {
										pk: 'pk-0',
										sk: 'sk-0'
									},
									TableName: 'simple-img-new-spec'
								})
							}),
							expect.objectContaining({
								Put: expect.objectContaining({
									Item: expect.objectContaining({
										pk: 'pk-0',
										sk: 'sk-1',
										__ts: expect.any(Number)
									}),
									TableName: 'simple-img-new-spec'
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
		});
	});
});
