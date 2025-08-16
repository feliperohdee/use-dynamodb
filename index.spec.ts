import _ from 'lodash';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, Mock, vi } from 'vitest';

import Db from './index';
import { ENDPOINT } from './constants';

type Item = {
	foo: string;
	gsiPk: string;
	gsiSk: string;
	lsiSk: string;
	pk: string;
	sk: string;
};

const createItems = ({ count, pk = '' }: { count: number; pk?: string }) => {
	return _.times(count, index => {
		const sk = _.padStart(index.toString(), 3, '0');

		return {
			foo: `foo-${index}`,
			gsiPk: `gsi-pk-${index % 2}`,
			gsiSk: `gsi-sk-${sk}`,
			lsiSk: `lsi-sk-${sk}`,
			sk: `sk-${sk}`,
			pk: pk || `pk-${index % 2}`
		};
	});
};

const factory = ({ onChange }: { onChange: Mock }) => {
	return new Db<Item>({
		accessKeyId: 'test',
		endpoint: ENDPOINT,
		indexes: [
			{
				name: 'ls-index',
				partition: 'pk',
				sort: 'lsiSk',
				sortType: 'S'
			},
			{
				name: 'gs-index',
				partition: 'gsiPk',
				partitionType: 'S',
				sort: 'gsiSk',
				sortType: 'S'
			}
		],
		onChange,
		region: 'us-east-1',
		schema: {
			partition: 'pk',
			sort: 'sk'
		},
		secretAccessKey: 'test',
		table: 'use-dynamodb-spec'
	});
};

describe('/index.ts', () => {
	let db: Db<Item>;
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

	describe('getClient', () => {
		it('should all instances use the same client', async () => {
			const clients1 = _.times(2, () => {
				return Db.getClient({
					accessKeyId: 'accessKeyId-1',
					secretAccessKey: 'secretAccessKey-1',
					region: 'region-1'
				});
			});

			const client2 = Db.getClient({
				accessKeyId: 'accessKeyId-2',
				secretAccessKey: 'secretAccessKey-2',
				region: 'region-2'
			});

			expect(clients1[0]).toBe(clients1[1]);
			expect(clients1[0]).not.toBe(client2);
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

	describe('batchGet / batchWrite / batchDelete', () => {
		afterAll(async () => {
			await db.clear();
		});

		it('should batch write, batch get and batch delete', async () => {
			const batchWriteItems = await db.batchWrite(createItems({ count: 52 }));
			expect(
				_.every(batchWriteItems, item => {
					return _.isNumber(item.__ts);
				})
			).toBeTruthy();

			const batchGetItems = await db.batchGet([...batchWriteItems, { pk: 'pk-inexistent', sk: 'sk-inexistent' }]);
			expect(batchGetItems).toHaveLength(52);

			const batchGetItemsWithNull = await db.batchGet([...batchWriteItems, { pk: 'pk-inexistent', sk: 'sk-inexistent' }], {
				returnNullIfNotFound: true
			});
			expect(batchGetItemsWithNull).toHaveLength(53);

			const batchDeleteItems = await Promise.all([
				db.batchDelete(
					_.times(52, i => {
						const sk = _.padStart(i.toString(), 3, '0');

						return { pk: 'pk-0', sk: `sk-${sk}` };
					})
				),
				db.batchDelete(
					_.times(52, i => {
						const sk = _.padStart(i.toString(), 3, '0');

						return { pk: 'pk-1', sk: `sk-${sk}` };
					})
				)
			]);
			expect(batchDeleteItems[0]).toHaveLength(52);
			expect(batchDeleteItems[1]).toHaveLength(52);

			const res = await Promise.all([
				db.query({
					item: { pk: 'pk-0' }
				}),
				db.query({
					item: { pk: 'pk-1' }
				})
			]);
			expect(res[0].count).toEqual(0);
			expect(res[1].count).toEqual(0);
			expect(onChangeMock).toHaveBeenCalledTimes(9);
		});

		it('should batch write, batch get and batch delete with empty string in indexes', async () => {
			const items = [
				{
					gsiSk: '',
					lsiSk: '',
					pk: 'pk-empty',
					sk: 'sk-empty'
				}
			];

			const batchWriteItems = await db.batchWrite(items);
			expect(batchWriteItems[0].sk).toEqual('sk-empty');
			expect(batchWriteItems[0].gsiSk).toEqual('');
			expect(batchWriteItems[0].lsiSk).toEqual('');

			const batchGetItems = await db.batchGet(items);
			expect(batchGetItems?.[0]?.sk).toEqual('sk-empty');
			expect(batchGetItems?.[0]?.gsiSk).toEqual('');
			expect(batchGetItems?.[0]?.lsiSk).toEqual('');

			const batchDeleteItems = await db.batchDelete(items);
			expect(batchDeleteItems[0].sk).toEqual('sk-empty');
		});
	});

	describe('clear', () => {
		afterAll(async () => {
			await db.clear();
		});

		it('should clear', async () => {
			await db.batchWrite(createItems({ count: 10 }));

			const res1 = await db.scan();
			expect(res1.count).toEqual(10);

			const { count } = await db.clear();
			expect(count).toEqual(10);

			const res2 = await db.scan();
			expect(res2.count).toEqual(0);
		});

		it('should clear by pk', async () => {
			await db.batchWrite(createItems({ count: 10 }));

			const res1 = await db.scan();
			expect(res1.count).toEqual(10);

			const { count } = await db.clear('pk-0');
			expect(count).toEqual(5);

			const res2 = await db.scan();
			expect(res2.count).toEqual(5);
		});
	});

	describe('delete', () => {
		beforeEach(async () => {
			await db.batchWrite(createItems({ count: 1 }));

			vi.spyOn(db, 'get');
			vi.spyOn(db.client, 'send');
		});

		afterAll(async () => {
			await db.clear();
		});

		it('should return null if item not found', async () => {
			const res = await db.delete({
				filter: {
					item: { pk: 'pk-0', sk: 'sk-100' }
				}
			});

			expect(res).toBeNull();
		});

		it('should delete', async () => {
			const res = await db.delete({
				filter: {
					item: { pk: 'pk-0', sk: 'sk-000' }
				}
			});

			expect(db.get).toHaveBeenCalledWith({
				item: { pk: 'pk-0', sk: 'sk-000' }
			});

			expect(db.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConditionExpression: '(attribute_exists(#__pk) AND #__ts = :__curr_ts)',
						ExpressionAttributeNames: {
							'#__pk': 'pk',
							'#__ts': '__ts'
						},
						ExpressionAttributeValues: {
							':__curr_ts': expect.any(Number)
						},
						Key: {
							pk: 'pk-0',
							sk: 'sk-000'
						},
						ReturnValues: 'ALL_OLD',
						TableName: 'use-dynamodb-spec'
					})
				})
			);

			expect(res).toEqual(
				expect.objectContaining({
					foo: 'foo-0',
					gsiPk: 'gsi-pk-0',
					gsiSk: 'gsi-sk-000',
					lsiSk: 'lsi-sk-000',
					sk: 'sk-000',
					pk: 'pk-0'
				})
			);

			expect(onChangeMock).toHaveBeenCalledTimes(2);
		});

		it('should delete with consistencyCheck = false', async () => {
			const res = await db.delete({
				consistencyCheck: false,
				filter: {
					item: { pk: 'pk-0', sk: 'sk-000' }
				}
			});

			expect(db.get).toHaveBeenCalledWith({
				item: { pk: 'pk-0', sk: 'sk-000' }
			});

			expect(db.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConditionExpression: 'attribute_exists(#__pk)',
						ExpressionAttributeNames: { '#__pk': 'pk' },
						Key: {
							pk: 'pk-0',
							sk: 'sk-000'
						},
						ReturnValues: 'ALL_OLD',
						TableName: 'use-dynamodb-spec'
					})
				})
			);

			expect(res).toEqual(
				expect.objectContaining({
					foo: 'foo-0',
					gsiPk: 'gsi-pk-0',
					gsiSk: 'gsi-sk-000',
					lsiSk: 'lsi-sk-000',
					sk: 'sk-000',
					pk: 'pk-0'
				})
			);

			expect(onChangeMock).toHaveBeenCalledTimes(2);
		});

		it('should delete by queryExpression and condition', async () => {
			const res = await db.delete({
				attributeNames: { '#__pk': 'pk' },
				attributeValues: { ':__pk': 'pk-0' },
				conditionExpression: '#__pk = :__pk',
				filter: {
					attributeNames: { '#__pk': 'pk' },
					attributeValues: { ':__pk': 'pk-0' },
					queryExpression: '#__pk = :__pk'
				}
			});

			expect(db.get).toHaveBeenCalledWith({
				attributeNames: { '#__pk': 'pk' },
				attributeValues: { ':__pk': 'pk-0' },
				queryExpression: '#__pk = :__pk'
			});

			expect(db.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConditionExpression: '(attribute_exists(#__pk) AND #__ts = :__curr_ts) AND #__pk = :__pk',
						ExpressionAttributeNames: {
							'#__pk': 'pk',
							'#__ts': '__ts'
						},
						ExpressionAttributeValues: {
							':__pk': 'pk-0',
							':__curr_ts': expect.any(Number)
						},
						Key: {
							pk: 'pk-0',
							sk: 'sk-000'
						},
						ReturnValues: 'ALL_OLD',
						TableName: 'use-dynamodb-spec'
					})
				})
			);

			expect(res).toEqual(
				expect.objectContaining({
					foo: 'foo-0',
					gsiPk: 'gsi-pk-0',
					gsiSk: 'gsi-sk-000',
					lsiSk: 'lsi-sk-000',
					sk: 'sk-000',
					pk: 'pk-0'
				})
			);

			expect(onChangeMock).toHaveBeenCalledTimes(2);
		});

		it('should delete with empty string in indexes', async () => {
			await db.put({
				gsiSk: '',
				lsiSk: '',
				pk: 'pk-empty',
				sk: 'sk-empty'
			});

			const res = await db.delete({
				filter: {
					item: { pk: 'pk-empty', sk: 'sk-empty' }
				}
			});

			expect(res).toEqual(
				expect.objectContaining({
					gsiSk: '',
					lsiSk: '',
					pk: 'pk-empty',
					sk: 'sk-empty'
				})
			);
		});
	});

	describe('deleteMany', () => {
		beforeAll(async () => {
			await db.batchWrite(createItems({ count: 52 }));
		});

		afterAll(async () => {
			await db.clear();
		});

		beforeEach(() => {
			vi.spyOn(db, 'batchDelete');
			vi.spyOn(db, 'filter');
		});

		it('should delete', async () => {
			const batchDeleteItems = await db.deleteMany({
				item: { pk: 'pk-0' }
			});

			expect(db.filter).toHaveBeenCalledWith({
				consistentRead: true,
				item: { pk: 'pk-0' },
				limit: Infinity,
				onChunk: expect.any(Function),
				startKey: null
			});

			expect(db.batchDelete).toHaveBeenCalledOnce();
			expect(batchDeleteItems).toHaveLength(26);
		});

		it('should delete by queryExpression', async () => {
			const batchDeleteItems = await db.deleteMany({
				attributeNames: { '#__pk': 'pk', '#sk': 'sk' },
				attributeValues: {
					':__pk': 'pk-1',
					':from': 'sk-000',
					':to': 'sk-999'
				},
				queryExpression: '#__pk = :__pk AND #sk BETWEEN :from AND :to'
			});

			expect(db.filter).toHaveBeenCalledWith({
				attributeNames: { '#__pk': 'pk', '#sk': 'sk' },
				attributeValues: {
					':__pk': 'pk-1',
					':from': 'sk-000',
					':to': 'sk-999'
				},
				consistentRead: true,
				limit: Infinity,
				onChunk: expect.any(Function),
				queryExpression: '#__pk = :__pk AND #sk BETWEEN :from AND :to',
				startKey: null
			});

			expect(db.batchDelete).toHaveBeenCalledOnce();
			expect(batchDeleteItems).toHaveLength(26);
		});
	});

	describe('filter', () => {
		beforeAll(async () => {
			await db.batchWrite(createItems({ count: 10 }));
		});

		afterAll(async () => {
			await db.clear();
		});

		beforeEach(() => {
			vi.spyOn(db, 'query');
			vi.spyOn(db, 'scan');
		});

		it('should throw if invalid parameters', async () => {
			try {
				await db.filter({});

				throw new Error('expected to throw');
			} catch (err) {
				expect((err as Error).message).toEqual('Must provide either item, queryExpression or filterExpression');
			}
		});

		it('should filter by item', async () => {
			const { count, lastEvaluatedKey } = await db.filter({
				item: { pk: 'pk-0' }
			});

			expect(db.query).toHaveBeenCalledWith({
				item: { pk: 'pk-0' }
			});

			expect(count).toEqual(5);
			expect(lastEvaluatedKey).toBeNull();
		});

		it('should filter by query expression', async () => {
			const { count, lastEvaluatedKey } = await db.filter({
				attributeNames: { '#__pk': 'pk' },
				attributeValues: { ':__pk': 'pk-0' },
				queryExpression: '#__pk = :__pk'
			});

			expect(db.query).toHaveBeenCalledWith({
				attributeNames: { '#__pk': 'pk' },
				attributeValues: { ':__pk': 'pk-0' },
				queryExpression: '#__pk = :__pk'
			});

			expect(count).toEqual(5);
			expect(lastEvaluatedKey).toBeNull();
		});

		it('should filter by scan', async () => {
			const { count, lastEvaluatedKey } = await db.filter({
				attributeNames: { '#__pk': 'pk' },
				attributeValues: { ':__pk': 'pk-0' },
				filterExpression: '#__pk = :__pk'
			});

			expect(db.scan).toHaveBeenCalledWith({
				attributeNames: { '#__pk': 'pk' },
				attributeValues: { ':__pk': 'pk-0' },
				filterExpression: '#__pk = :__pk'
			});

			expect(count).toEqual(5);
			expect(lastEvaluatedKey).toBeNull();
		});

		it('should filter with empty string in indexes', async () => {
			await db.put({
				gsiSk: '',
				lsiSk: '',
				pk: 'pk-empty',
				sk: 'sk-empty'
			});

			const { count, items } = await db.filter({
				item: { pk: 'pk-empty', sk: 'sk-empty' }
			});

			expect(count).toEqual(1);
			expect(items[0]).toEqual(
				expect.objectContaining({
					gsiSk: '',
					lsiSk: '',
					pk: 'pk-empty',
					sk: 'sk-empty'
				})
			);
		});
	});

	describe('get', () => {
		beforeAll(async () => {
			await db.batchWrite(createItems({ count: 1 }));
		});

		afterAll(async () => {
			await db.clear();
		});

		beforeEach(() => {
			vi.spyOn(db, 'filter');
			vi.spyOn(db.client, 'send');
		});

		it('should return null if not found', async () => {
			const res = await db.get({
				item: { pk: 'pk-0', sk: 'sk-100' }
			});

			expect(res).toBeNull();
		});

		it('should get', async () => {
			const res = await db.get({
				item: { pk: 'pk-0', sk: 'sk-000' }
			});

			expect(db.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: {
						Key: {
							pk: 'pk-0',
							sk: 'sk-000'
						},
						TableName: 'use-dynamodb-spec'
					}
				})
			);

			expect(res).toEqual(
				expect.objectContaining({
					foo: 'foo-0',
					gsiPk: 'gsi-pk-0',
					gsiSk: 'gsi-sk-000',
					lsiSk: 'lsi-sk-000',
					pk: 'pk-0',
					sk: 'sk-000'
				})
			);
		});

		it('should get by query expression', async () => {
			const res = await db.get({
				attributeNames: { '#__pk': 'pk' },
				attributeValues: { ':__pk': 'pk-0' },
				queryExpression: '#__pk = :__pk'
			});

			expect(db.filter).toHaveBeenCalledWith({
				attributeNames: { '#__pk': 'pk' },
				attributeValues: { ':__pk': 'pk-0' },
				limit: 1,
				onChunk: expect.any(Function),
				queryExpression: '#__pk = :__pk',
				startKey: null
			});

			expect(res).toEqual(
				expect.objectContaining({
					foo: 'foo-0',
					gsiPk: 'gsi-pk-0',
					gsiSk: 'gsi-sk-000',
					lsiSk: 'lsi-sk-000',
					pk: 'pk-0',
					sk: 'sk-000'
				})
			);
		});

		it('should get with select', async () => {
			const res = await db.get({
				item: { pk: 'pk-0', sk: 'sk-000' },
				select: ['foo', 'gsiPk']
			});

			expect(db.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: {
						ExpressionAttributeNames: {
							'#__pe1': 'foo',
							'#__pe2': 'gsiPk'
						},
						Key: {
							pk: 'pk-0',
							sk: 'sk-000'
						},
						ProjectionExpression: '#__pe1, #__pe2',
						TableName: 'use-dynamodb-spec'
					}
				})
			);

			expect(res).toEqual(
				expect.objectContaining({
					foo: 'foo-0',
					gsiPk: 'gsi-pk-0'
				})
			);
		});

		it('should get with empty string in indexes', async () => {
			await db.put({
				gsiSk: '',
				lsiSk: '',
				pk: 'pk-empty',
				sk: 'sk-empty'
			});

			const res = await db.get({
				item: { pk: 'pk-empty', sk: 'sk-empty' }
			});

			expect(res).toEqual(
				expect.objectContaining({
					gsiSk: '',
					lsiSk: '',
					pk: 'pk-empty',
					sk: 'sk-empty'
				})
			);
		});
	});

	describe('getLast', () => {
		beforeAll(async () => {
			await db.batchWrite(createItems({ count: 10 }));
		});

		afterAll(async () => {
			await db.clear();
		});

		beforeEach(() => {
			vi.spyOn(db.client, 'send');
		});

		it('should get the last item by partition key', async () => {
			const res = await db.getLast({
				item: { pk: 'pk-0' }
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
						Limit: 1,
						ScanIndexForward: false,
						TableName: 'use-dynamodb-spec'
					})
				})
			);

			expect(res).toEqual(
				expect.objectContaining({
					foo: 'foo-8',
					gsiPk: 'gsi-pk-0',
					gsiSk: 'gsi-sk-008',
					lsiSk: 'lsi-sk-008',
					pk: 'pk-0',
					sk: 'sk-008'
				})
			);
		});

		it('should get the last item by partition and sort key', async () => {
			const res = await db.getLast({
				item: { pk: 'pk-0', sk: 'sk-008' }
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
							':__sk': 'sk-008'
						},
						KeyConditionExpression: '#__pk = :__pk AND #__sk = :__sk',
						Limit: 1,
						ScanIndexForward: false,
						TableName: 'use-dynamodb-spec'
					})
				})
			);

			expect(res).toEqual(
				expect.objectContaining({
					foo: 'foo-8',
					gsiPk: 'gsi-pk-0',
					gsiSk: 'gsi-sk-008',
					lsiSk: 'lsi-sk-008',
					pk: 'pk-0',
					sk: 'sk-008'
				})
			);
		});

		it('should get the last item with empty string in indexes', async () => {
			await db.put({
				gsiSk: '',
				lsiSk: '',
				pk: 'pk-empty',
				sk: 'sk-empty'
			});

			const res = await db.getLast({
				item: { pk: 'pk-empty' }
			});

			expect(res).toEqual(
				expect.objectContaining({
					gsiSk: '',
					lsiSk: '',
					pk: 'pk-empty',
					sk: 'sk-empty'
				})
			);
		});
	});

	describe('getLastEvaluatedKey', () => {
		it('should returns', () => {
			// @ts-expect-error
			const lastEvaluatedKey = db.getLastEvaluatedKey([
				{
					gsiPk: 'gsi-pk',
					gsiSk: 'gsi-sk',
					lsiSk: 'lsi-sk',
					pk: 'pk',
					sk: 'sk'
				}
			]);

			expect(lastEvaluatedKey).toEqual({ pk: 'pk', sk: 'sk' });
		});

		it('should returns with LSI', () => {
			// @ts-expect-error
			const lastEvaluatedKey = db.getLastEvaluatedKey(
				[
					{
						gsiPk: 'gsi-pk',
						gsiSk: 'gsi-sk',
						lsiSk: 'lsi-sk',
						pk: 'pk',
						sk: 'sk'
					}
				],
				'ls-index'
			);

			expect(lastEvaluatedKey).toEqual({
				lsiSk: 'lsi-sk',
				pk: 'pk',
				sk: 'sk'
			});
		});

		it('should returns with GSI', () => {
			// @ts-expect-error
			const lastEvaluatedKey = db.getLastEvaluatedKey(
				[
					{
						gsiPk: 'gsi-pk',
						gsiSk: 'gsi-sk',
						lsiSk: 'lsi-sk',
						pk: 'pk',
						sk: 'sk'
					}
				],
				'gs-index'
			);

			expect(lastEvaluatedKey).toEqual({
				gsiPk: 'gsi-pk',
				gsiSk: 'gsi-sk',
				pk: 'pk',
				sk: 'sk'
			});
		});

		it('should returns with inexistent index', () => {
			// @ts-expect-error
			const lastEvaluatedKey = db.getLastEvaluatedKey(
				[
					{
						gsiPk: 'gsi-pk',
						gsiSk: 'gsi-sk',
						lsiSk: 'lsi-sk',
						pk: 'pk',
						sk: 'sk'
					}
				],
				'inexistent-index'
			);

			expect(lastEvaluatedKey).toEqual({
				pk: 'pk',
				sk: 'sk'
			});
		});
	});

	describe('getSchemaKeys', () => {
		it('should return schema keys', () => {
			// @ts-expect-error
			const keys = db.getSchemaKeys({
				gsiPk: 'gsi-pk',
				gsiSk: 'gsi-sk',
				lsiSk: 'lsi-sk',
				pk: 'pk',
				sk: 'sk'
			});

			expect(keys).toEqual({ pk: 'pk', sk: 'sk' });
		});

		it('should return schema keys with LSI', () => {
			// @ts-expect-error
			const keys = db.getSchemaKeys(
				{
					gsiPk: 'gsi-pk',
					gsiSk: 'gsi-sk',
					lsiSk: 'lsi-sk',
					pk: 'pk',
					sk: 'sk'
				},
				'ls-index'
			);

			expect(keys).toEqual({
				lsiSk: 'lsi-sk',
				pk: 'pk'
			});
		});

		it('should return schema keys with GSI', () => {
			// @ts-expect-error
			const keys = db.getSchemaKeys(
				{
					gsiPk: 'gsi-pk',
					gsiSk: 'gsi-sk',
					lsiSk: 'lsi-sk',
					pk: 'pk',
					sk: 'sk'
				},
				'gs-index'
			);

			expect(keys).toEqual({
				gsiPk: 'gsi-pk',
				gsiSk: 'gsi-sk'
			});
		});

		it('should return schema keys with inexistent index', () => {
			// @ts-expect-error
			const keys = db.getSchemaKeys(
				{
					gsiPk: 'gsi-pk',
					gsiSk: 'gsi-sk',
					lsiSk: 'lsi-sk',
					pk: 'pk',
					sk: 'sk'
				},
				'inexistent-index'
			);

			expect(keys).toEqual({
				pk: 'pk',
				sk: 'sk'
			});
		});
	});

	describe('getSortSegments', () => {
		beforeAll(async () => {
			await db.batchWrite(createItems({ count: 10, pk: 'pk-0' }));
		});

		afterAll(async () => {
			await db.clear();
		});

		beforeEach(() => {
			vi.spyOn(db.client, 'send');
		});

		it('should get sort segments by 1', async () => {
			const res = await db.getSortSegments({
				partitionKey: 'pk-0',
				segmentsSize: 1
			});

			expect(res).toEqual([
				[null, 'sk-000'],
				['sk-001', 'sk-001'],
				['sk-002', 'sk-002'],
				['sk-003', 'sk-003'],
				['sk-004', 'sk-004'],
				['sk-005', 'sk-005'],
				['sk-006', 'sk-006'],
				['sk-007', 'sk-007'],
				['sk-008', 'sk-008'],
				['sk-009', null]
			]);
		});

		it('should get sort segments by 3', async () => {
			const res = await db.getSortSegments({
				partitionKey: 'pk-0',
				segmentsSize: 3
			});

			expect(res).toEqual([
				[null, 'sk-002'],
				['sk-003', 'sk-005'],
				['sk-006', 'sk-008'],
				['sk-009', null]
			]);
		});

		it('should get sort segments by 5', async () => {
			const res = await db.getSortSegments({
				partitionKey: 'pk-0',
				segmentsSize: 5
			});

			expect(res).toEqual([
				[null, 'sk-004'],
				['sk-005', null]
			]);
		});

		it('should get sort segments by 10', async () => {
			const res = await db.getSortSegments({
				partitionKey: 'pk-0',
				segmentsSize: 10
			});

			expect(res).toEqual([[null, null]]);
		});
	});

	describe('getStringIndexAttributes', () => {
		it('should identify string sort keys from indexes only', () => {
			// @ts-expect-error
			const res = db.getStringIndexAttributes();
			expect(res).toEqual(['lsiSk', 'gsiSk']);
		});

		it('should handle schema without sort key', () => {
			db.indexes = [];
			db.schema = {
				partition: 'pk'
			};

			// @ts-expect-error
			const res = db.getStringIndexAttributes();
			expect(res).toEqual([]);
		});

		it('should handle numeric sort keys', () => {
			db.indexes = [];
			db.schema = {
				partition: 'pk',
				sort: 'sk',
				sortType: 'N'
			};

			// @ts-expect-error
			const res = db.getStringIndexAttributes();
			expect(res).toEqual([]);
		});
	});

	describe('put', () => {
		beforeEach(() => {
			vi.spyOn(db.client, 'send');
		});

		afterAll(async () => {
			await db.clear();
		});

		it('should put', async () => {
			const res = await db.put({
				pk: 'pk-0',
				sk: 'sk-000'
			});

			expect(db.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConditionExpression: 'attribute_not_exists(#__pk)',
						ExpressionAttributeNames: { '#__pk': 'pk' },
						Item: {
							__createdAt: expect.any(String),
							__ts: expect.any(Number),
							__updatedAt: expect.any(String),
							pk: 'pk-0',
							sk: 'sk-000'
						},
						TableName: 'use-dynamodb-spec'
					})
				})
			);

			expect(res.__createdAt).toEqual(res.__updatedAt);
			expect(res).toEqual(
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-000'
				})
			);

			expect(onChangeMock).toHaveBeenCalledOnce();
		});

		it('should throw on overwrite', async () => {
			try {
				await db.put({
					pk: 'pk-0',
					sk: 'sk-000'
				});

				throw new Error('expected to throw');
			} catch (err) {
				expect((err as Error).name).toEqual('ConditionalCheckFailedException');
			}
		});

		it('should put overwriting', async () => {
			const res = await db.get({
				item: { pk: 'pk-0', sk: 'sk-000' }
			});

			const overwriteItem = await db.put(
				{
					pk: 'pk-0',
					sk: 'sk-000'
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
							sk: 'sk-000'
						},
						TableName: 'use-dynamodb-spec'
					})
				})
			);

			expect(overwriteItem.__ts).toBeGreaterThan(res!.__ts);
			expect(overwriteItem.__createdAt).not.toEqual(res!.__createdAt);
			expect(overwriteItem.__createdAt).toEqual(overwriteItem.__updatedAt);
			expect(overwriteItem).toEqual(
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-000'
				})
			);

			expect(onChangeMock).toHaveBeenCalledOnce();
		});

		it('should put with condition', async () => {
			const res = await db.put(
				{
					__createdAt: '2021-01-01T00:00:00.000Z',
					pk: 'pk-0',
					sk: 'sk-001'
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
						ConditionExpression: 'attribute_not_exists(#__pk) AND #foo <> :foo',
						ExpressionAttributeNames: { '#foo': 'foo', '#__pk': 'pk' },
						ExpressionAttributeValues: { ':foo': 'foo-0' },
						Item: {
							__createdAt: expect.any(String),
							__ts: expect.any(Number),
							__updatedAt: expect.any(String),
							pk: 'pk-0',
							sk: 'sk-001'
						},
						TableName: 'use-dynamodb-spec'
					})
				})
			);

			expect(res.__createdAt).toEqual(res.__updatedAt);
			expect(res).toEqual(
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-001'
				})
			);

			expect(onChangeMock).toHaveBeenCalledOnce();
		});

		it('should put overriding createdAt', async () => {
			const res = await db.put(
				{
					__createdAt: '2021-01-01T00:00:00.000Z',
					pk: 'pk-0',
					sk: 'sk-002'
				},
				{
					attributeNames: { '#foo': 'foo' },
					attributeValues: { ':foo': 'foo-0' },
					conditionExpression: '#foo <> :foo',
					overwrite: false,
					useCurrentCreatedAtIfExists: true
				}
			);

			expect(db.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConditionExpression: 'attribute_not_exists(#__pk) AND #foo <> :foo',
						ExpressionAttributeNames: { '#foo': 'foo', '#__pk': 'pk' },
						ExpressionAttributeValues: { ':foo': 'foo-0' },
						Item: {
							__createdAt: expect.any(String),
							__ts: expect.any(Number),
							__updatedAt: expect.any(String),
							pk: 'pk-0',
							sk: 'sk-002'
						},
						TableName: 'use-dynamodb-spec'
					})
				})
			);

			expect(res.__createdAt).not.toEqual(res.__updatedAt);
			expect(res.__createdAt).toEqual('2021-01-01T00:00:00.000Z');
			expect(res).toEqual(
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-002'
				})
			);

			expect(onChangeMock).toHaveBeenCalledOnce();
		});

		it('should put with empty string in indexes', async () => {
			const res = await db.put({
				gsiSk: '',
				lsiSk: '',
				pk: 'pk-empty',
				sk: 'sk-empty'
			});

			expect(res).toEqual(
				expect.objectContaining({
					gsiSk: '',
					lsiSk: '',
					pk: 'pk-empty',
					sk: 'sk-empty'
				})
			);
		});
	});

	describe('query', () => {
		beforeAll(async () => {
			await db.batchWrite(createItems({ count: 10 }));
		});

		afterAll(async () => {
			await db.clear();
		});

		beforeEach(() => {
			vi.spyOn(db.client, 'send');
		});

		it('should throw if invalid parameters', async () => {
			try {
				await db.query({});

				throw new Error('expected to throw');
			} catch (err) {
				expect((err as Error).message).toEqual('Must provide either item or queryExpression');
			}
		});

		it('should query by item with partition', async () => {
			const { count, lastEvaluatedKey } = await db.query({
				item: { pk: 'pk-0' }
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

		it('should query by item with partition/sort', async () => {
			const { count, lastEvaluatedKey } = await db.query({
				item: { pk: 'pk-0', sk: 'sk-000' }
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
							':__sk': 'sk-000'
						},
						KeyConditionExpression: '#__pk = :__pk AND #__sk = :__sk',
						TableName: 'use-dynamodb-spec'
					})
				})
			);

			expect(count).toEqual(1);
			expect(lastEvaluatedKey).toBeNull();
		});

		it('should query by item with partition/sort with prefix', async () => {
			const { count, lastEvaluatedKey } = await db.query({
				item: { pk: 'pk-0', sk: 'sk-' },
				prefix: true
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

		it('should query by item with LSI', async () => {
			const { count, lastEvaluatedKey } = await db.query({
				item: { pk: 'pk-0', lsiSk: 'lsi-sk-000' }
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
							':__sk': 'lsi-sk-000'
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

		it('should query by item with GSI', async () => {
			const { count, lastEvaluatedKey } = await db.query({
				item: { gsiPk: 'gsi-pk-0', gsiSk: 'gsi-sk-000' }
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
							':__sk': 'gsi-sk-000'
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

		it('should query by item with GSI with partition', async () => {
			const { count, lastEvaluatedKey } = await db.query({
				item: { gsiPk: 'gsi-pk-0' }
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

		it('should query by item + query expression', async () => {
			const { count, lastEvaluatedKey } = await db.query({
				attributeNames: { '#lsiSk': 'lsiSk' },
				attributeValues: { ':from': 'lsi-sk-000', ':to': 'lsi-sk-003' },
				index: 'ls-index',
				item: { pk: 'pk-0' },
				queryExpression: ' #lsiSk BETWEEN :from AND :to'
			});

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
							':from': 'lsi-sk-000',
							':to': 'lsi-sk-003'
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

		it('should query by item with filterExpression', async () => {
			const { count, lastEvaluatedKey } = await db.query({
				attributeNames: { '#foo': 'foo' },
				attributeValues: { ':foo': 'foo-0' },
				filterExpression: '#foo = :foo',
				item: { pk: 'pk-0' }
			});

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

		it('should query by item with limit/startKey', async () => {
			const { count, lastEvaluatedKey } = await db.query({
				item: { pk: 'pk-0' },
				limit: 1
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
						Limit: 1,
						TableName: 'use-dynamodb-spec'
					})
				})
			);

			expect(count).toEqual(1);
			expect(lastEvaluatedKey).toEqual({ pk: 'pk-0', sk: 'sk-000' });

			const { count: count2, lastEvaluatedKey: lastEvaluatedKey2 } = await db.query({
				item: { pk: 'pk-0' },
				startKey: lastEvaluatedKey
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
						ExclusiveStartKey: { pk: 'pk-0', sk: 'sk-000' },
						KeyConditionExpression: '#__pk = :__pk',
						TableName: 'use-dynamodb-spec'
					})
				})
			);

			expect(count2).toEqual(4);
			expect(lastEvaluatedKey2).toBeNull();
		});

		it('should query by item until limit with onChunk', async () => {
			const onChunk = vi.fn();
			const { count, lastEvaluatedKey } = await db.query({
				chunkLimit: 1,
				item: { pk: 'pk-0' },
				limit: 2,
				onChunk
			});

			expect(db.client.send).toHaveBeenCalledTimes(2);
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
			expect(db.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConsistentRead: false,
						ExclusiveStartKey: { pk: 'pk-0', sk: 'sk-000' },
						ExpressionAttributeNames: {
							'#__pk': 'pk'
						},
						ExpressionAttributeValues: {
							':__pk': 'pk-0'
						},
						KeyConditionExpression: '#__pk = :__pk',
						Limit: 2,
						TableName: 'use-dynamodb-spec'
					})
				})
			);

			expect(onChunk).toHaveBeenCalledTimes(2);
			expect(onChunk).toHaveBeenCalledWith({
				count: 2,
				items: expect.any(Array)
			});
			expect(onChunk).toHaveBeenCalledWith({
				count: 1,
				items: expect.any(Array)
			});

			expect(count).toEqual(2);
			expect(lastEvaluatedKey).toEqual({ pk: 'pk-0', sk: 'sk-002' });
		});

		it('should query by item until limit with LSI and onChunk', async () => {
			const onChunk = vi.fn();
			const { count, lastEvaluatedKey } = await db.query({
				chunkLimit: 1,
				item: {
					pk: 'pk-0',
					lsiSk: 'lsi-sk-'
				},
				limit: 2,
				prefix: true,
				onChunk
			});

			expect(db.client.send).toHaveBeenCalledTimes(2);
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
							':__sk': 'lsi-sk-'
						},
						KeyConditionExpression: '#__pk = :__pk AND begins_with(#__sk, :__sk)',
						Limit: 1,
						TableName: 'use-dynamodb-spec'
					})
				})
			);
			expect(db.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConsistentRead: false,
						ExclusiveStartKey: {
							lsiSk: 'lsi-sk-000',
							pk: 'pk-0',
							sk: 'sk-000'
						},
						ExpressionAttributeNames: {
							'#__pk': 'pk',
							'#__sk': 'lsiSk'
						},
						ExpressionAttributeValues: {
							':__pk': 'pk-0',
							':__sk': 'lsi-sk-'
						},
						KeyConditionExpression: '#__pk = :__pk AND begins_with(#__sk, :__sk)',
						Limit: 2,
						TableName: 'use-dynamodb-spec'
					})
				})
			);

			expect(onChunk).toHaveBeenCalledTimes(2);
			expect(onChunk).toHaveBeenCalledWith({
				count: 2,
				items: expect.any(Array)
			});
			expect(onChunk).toHaveBeenCalledWith({
				count: 1,
				items: expect.any(Array)
			});

			expect(count).toEqual(2);
			expect(lastEvaluatedKey).toEqual({
				lsiSk: 'lsi-sk-002',
				pk: 'pk-0',
				sk: 'sk-002'
			});
		});

		it('should query by item until limit with GSI and onChunk', async () => {
			const onChunk = vi.fn();
			const { count, lastEvaluatedKey } = await db.query({
				chunkLimit: 1,
				item: {
					gsiPk: 'gsi-pk-0',
					gsiSk: 'gsi-sk-'
				},
				limit: 2,
				prefix: true,
				onChunk
			});

			expect(db.client.send).toHaveBeenCalledTimes(2);
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
							':__sk': 'gsi-sk-'
						},
						KeyConditionExpression: '#__pk = :__pk AND begins_with(#__sk, :__sk)',
						Limit: 1,
						TableName: 'use-dynamodb-spec'
					})
				})
			);
			expect(db.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConsistentRead: false,
						ExclusiveStartKey: {
							gsiPk: 'gsi-pk-0',
							gsiSk: 'gsi-sk-000',
							pk: 'pk-0',
							sk: 'sk-000'
						},
						ExpressionAttributeNames: {
							'#__pk': 'gsiPk',
							'#__sk': 'gsiSk'
						},
						ExpressionAttributeValues: {
							':__pk': 'gsi-pk-0',
							':__sk': 'gsi-sk-'
						},
						KeyConditionExpression: '#__pk = :__pk AND begins_with(#__sk, :__sk)',
						Limit: 2,
						TableName: 'use-dynamodb-spec'
					})
				})
			);

			expect(onChunk).toHaveBeenCalledTimes(2);
			expect(onChunk).toHaveBeenCalledWith({
				count: 2,
				items: expect.any(Array)
			});
			expect(onChunk).toHaveBeenCalledWith({
				count: 1,
				items: expect.any(Array)
			});

			expect(count).toEqual(2);
			expect(lastEvaluatedKey).toEqual({
				gsiPk: 'gsi-pk-0',
				gsiSk: 'gsi-sk-002',
				pk: 'pk-0',
				sk: 'sk-002'
			});
		});

		it('should by item query with consistentRead', async () => {
			const { count } = await db.query({
				item: { pk: 'pk-0' },
				consistentRead: true
			});

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

		it('should query by item with select', async () => {
			const { count, items } = await db.query({
				item: { pk: 'pk-0' },
				select: ['foo', 'gsiPk']
			});

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

		it('should query by expression', async () => {
			const { count, lastEvaluatedKey } = await db.query({
				attributeNames: { '#__pk': 'pk' },
				attributeValues: { ':__pk': 'pk-0' },
				queryExpression: '#__pk = :__pk'
			});

			expect(db.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConsistentRead: false,
						ExpressionAttributeNames: { '#__pk': 'pk' },
						ExpressionAttributeValues: { ':__pk': 'pk-0' },
						KeyConditionExpression: '#__pk = :__pk',
						TableName: 'use-dynamodb-spec'
					})
				})
			);

			expect(count).toEqual(5);
			expect(lastEvaluatedKey).toBeNull();
		});

		it('should query with scanIndexForward true', async () => {
			const { count, items } = await db.query({
				item: { pk: 'pk-0' },
				scanIndexForward: true
			});

			expect(db.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ExpressionAttributeNames: {
							'#__pk': 'pk'
						},
						ExpressionAttributeValues: {
							':__pk': 'pk-0'
						},
						KeyConditionExpression: '#__pk = :__pk',
						ScanIndexForward: true,
						TableName: 'use-dynamodb-spec'
					})
				})
			);

			expect(count).toEqual(5);
			expect(items[0].sk).toEqual('sk-000');
			expect(items[items.length - 1].sk).toEqual('sk-008');
		});

		it('should query with scanIndexForward false', async () => {
			const { count, items } = await db.query({
				item: { pk: 'pk-0' },
				scanIndexForward: false
			});

			expect(db.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ExpressionAttributeNames: {
							'#__pk': 'pk'
						},
						ExpressionAttributeValues: {
							':__pk': 'pk-0'
						},
						KeyConditionExpression: '#__pk = :__pk',
						ScanIndexForward: false,
						TableName: 'use-dynamodb-spec'
					})
				})
			);

			expect(count).toEqual(5);
			expect(items[0].sk).toEqual('sk-008');
			expect(items[items.length - 1].sk).toEqual('sk-000');
		});

		it('should query with empty string in indexes', async () => {
			await db.put({
				gsiSk: '',
				lsiSk: '',
				pk: 'pk-empty',
				sk: 'sk-empty'
			});

			const { count, items } = await db.query({
				item: { pk: 'pk-empty', sk: 'sk-empty' }
			});

			expect(count).toEqual(1);
			expect(items[0]).toEqual(
				expect.objectContaining({
					gsiSk: '',
					lsiSk: '',
					pk: 'pk-empty',
					sk: 'sk-empty'
				})
			);
		});
	});

	describe('replace', () => {
		afterEach(async () => {
			await db.clear();
		});

		beforeEach(() => {
			vi.spyOn(db, 'transaction');
		});

		it('should replace', async () => {
			const replacedItem = await db.put({
				pk: 'pk-0',
				sk: 'sk-000'
			});

			onChangeMock.mockClear();
			const newItem = await db.replace(
				{
					__createdAt: '2021-01-01T00:00:00.000Z',
					pk: 'pk-1',
					sk: 'sk-001'
				},
				replacedItem
			);

			expect(db.transaction).toHaveBeenCalledWith({
				TransactItems: [
					{
						Delete: expect.objectContaining({
							ConditionExpression: '(attribute_exists(#__pk) AND #__ts = :__curr_ts)',
							ExpressionAttributeNames: { '#__pk': 'pk', '#__ts': '__ts' },
							ExpressionAttributeValues: { ':__curr_ts': replacedItem.__ts },
							TableName: 'use-dynamodb-spec'
						})
					},
					{
						Put: expect.objectContaining({
							ConditionExpression: 'attribute_not_exists(#__pk)',
							ExpressionAttributeNames: { '#__pk': 'pk' },
							TableName: 'use-dynamodb-spec'
						})
					}
				]
			});

			expect(newItem.__createdAt).toEqual(replacedItem.__createdAt);
			expect(newItem).toEqual({
				pk: 'pk-1',
				sk: 'sk-001',
				__createdAt: replacedItem.__createdAt,
				__ts: newItem.__ts,
				__updatedAt: newItem.__updatedAt
			});

			expect(onChangeMock).toHaveBeenCalledOnce();
		});

		it('should replace with consistencyCheck = false', async () => {
			const replacedItem = await db.put({
				pk: 'pk-0',
				sk: 'sk-000'
			});

			onChangeMock.mockClear();
			const newItem = await db.replace(
				{
					__createdAt: '2021-01-01T00:00:00.000Z',
					pk: 'pk-1',
					sk: 'sk-001'
				},
				replacedItem,
				{
					consistencyCheck: false
				}
			);

			expect(db.transaction).toHaveBeenCalledWith({
				TransactItems: [
					{
						Delete: expect.objectContaining({
							ConditionExpression: 'attribute_exists(#__pk)',
							ExpressionAttributeNames: { '#__pk': 'pk' },
							TableName: 'use-dynamodb-spec'
						})
					},
					{
						Put: expect.objectContaining({
							ConditionExpression: 'attribute_not_exists(#__pk)',
							ExpressionAttributeNames: { '#__pk': 'pk' },
							TableName: 'use-dynamodb-spec'
						})
					}
				]
			});

			expect(newItem.__createdAt).toEqual(replacedItem.__createdAt);
			expect(newItem).toEqual({
				pk: 'pk-1',
				sk: 'sk-001',
				__createdAt: replacedItem.__createdAt,
				__ts: newItem.__ts,
				__updatedAt: newItem.__updatedAt
			});

			expect(onChangeMock).toHaveBeenCalledOnce();
		});

		it('should replace overriding createdAt', async () => {
			const replacedItem = await db.put({
				pk: 'pk-0',
				sk: 'sk-000'
			});

			onChangeMock.mockClear();
			const newItem = await db.replace(
				{
					__createdAt: '2021-01-01T00:00:00.000Z',
					pk: 'pk-1',
					sk: 'sk-001'
				},
				replacedItem,
				{
					useCurrentCreatedAtIfExists: true
				}
			);

			expect(db.transaction).toHaveBeenCalledWith({
				TransactItems: [
					{
						Delete: expect.objectContaining({
							ConditionExpression: '(attribute_exists(#__pk) AND #__ts = :__curr_ts)',
							ExpressionAttributeNames: { '#__pk': 'pk', '#__ts': '__ts' },
							ExpressionAttributeValues: { ':__curr_ts': replacedItem.__ts },
							TableName: 'use-dynamodb-spec'
						})
					},
					{
						Put: expect.objectContaining({
							ConditionExpression: 'attribute_not_exists(#__pk)',
							ExpressionAttributeNames: { '#__pk': 'pk' },
							TableName: 'use-dynamodb-spec'
						})
					}
				]
			});

			expect(newItem.__createdAt).not.toEqual(replacedItem.__createdAt);
			expect(newItem).toEqual({
				pk: 'pk-1',
				sk: 'sk-001',
				__createdAt: '2021-01-01T00:00:00.000Z',
				__ts: newItem.__ts,
				__updatedAt: newItem.__updatedAt
			});

			expect(onChangeMock).toHaveBeenCalledOnce();
		});

		it('should replace overwriting', async () => {
			await db.put({
				pk: 'pk-1',
				sk: 'sk-001'
			});

			const replacedItem = await db.put({
				pk: 'pk-0',
				sk: 'sk-000'
			});

			onChangeMock.mockClear();
			const newItem = await db.replace(
				{
					pk: 'pk-1',
					sk: 'sk-001'
				},
				replacedItem,
				{ overwrite: true }
			);

			expect(db.transaction).toHaveBeenCalledWith({
				TransactItems: [
					{
						Delete: expect.objectContaining({
							ConditionExpression: '(attribute_exists(#__pk) AND #__ts = :__curr_ts)',
							ExpressionAttributeNames: { '#__pk': 'pk', '#__ts': '__ts' },
							ExpressionAttributeValues: { ':__curr_ts': replacedItem.__ts },
							TableName: 'use-dynamodb-spec'
						})
					},
					{
						Put: expect.objectContaining({
							TableName: 'use-dynamodb-spec'
						})
					}
				]
			});

			expect(newItem.__createdAt).toEqual(replacedItem.__createdAt);
			expect(newItem).toEqual({
				pk: 'pk-1',
				sk: 'sk-001',
				__createdAt: replacedItem.__createdAt,
				__ts: newItem.__ts,
				__updatedAt: newItem.__updatedAt
			});

			expect(onChangeMock).toHaveBeenCalledOnce();
		});

		it('should throw on overwrite', async () => {
			await db.put({
				pk: 'pk-1',
				sk: 'sk-001'
			});

			const replacedItem = await db.put({
				pk: 'pk-0',
				sk: 'sk-000'
			});

			try {
				onChangeMock.mockClear();
				await db.replace(
					{
						pk: 'pk-1',
						sk: 'sk-001'
					},
					replacedItem
				);

				throw new Error('expected to throw');
			} catch (err) {
				expect(db.transaction).toHaveBeenCalledWith({
					TransactItems: [
						{
							Delete: expect.objectContaining({
								ConditionExpression: '(attribute_exists(#__pk) AND #__ts = :__curr_ts)',
								ExpressionAttributeNames: { '#__pk': 'pk', '#__ts': '__ts' },
								ExpressionAttributeValues: { ':__curr_ts': replacedItem.__ts },
								TableName: 'use-dynamodb-spec'
							})
						},
						{
							Put: expect.objectContaining({
								ConditionExpression: 'attribute_not_exists(#__pk)',
								ExpressionAttributeNames: { '#__pk': 'pk' },
								TableName: 'use-dynamodb-spec'
							})
						}
					]
				});

				expect(onChangeMock).not.toHaveBeenCalled();
				expect((err as Error).name).toEqual('TransactionCanceledException');
			}
		});

		it('should replace with empty string in indexes', async () => {
			const replacedItem = await db.put({
				gsiSk: '',
				lsiSk: '',
				pk: 'pk-empty',
				sk: 'sk-empty'
			});

			onChangeMock.mockClear();
			const newItem = await db.replace(
				{
					gsiSk: '',
					lsiSk: '',
					pk: 'pk-empty-1',
					sk: 'sk-empty-1'
				},
				replacedItem
			);

			expect(newItem).toEqual(
				expect.objectContaining({
					gsiSk: '',
					lsiSk: '',
					pk: 'pk-empty-1',
					sk: 'sk-empty-1'
				})
			);

			expect(onChangeMock).toHaveBeenCalledOnce();
		});
	});

	describe('resolveSchema', () => {
		it('should resolve', () => {
			// @ts-expect-error
			const { index, schema } = db.resolveSchema({
				pk: 'pk-0',
				sk: 'sk-000'
			});

			expect(index).toEqual('sort');
			expect(schema).toEqual({
				partition: 'pk',
				sort: 'sk'
			});
		});

		it('should resolve only partition', () => {
			// @ts-expect-error
			const { index, schema } = db.resolveSchema({
				pk: 'pk-0'
			});

			expect(index).toEqual('');
			expect(schema).toEqual({
				partition: 'pk',
				sort: ''
			});
		});

		it('should resolve by LSI', () => {
			// @ts-expect-error
			const { index, schema } = db.resolveSchema({
				pk: 'pk-0',
				lsiSk: 'lsi-sk-000'
			});

			expect(index).toEqual('ls-index');
			expect(schema).toEqual({
				partition: 'pk',
				sort: 'lsiSk'
			});
		});

		it('should resolve by GSI', () => {
			// @ts-expect-error
			const { index, schema } = db.resolveSchema({
				gsiPk: 'gsi-pk-0',
				gsiSk: 'gsi-sk-000'
			});

			expect(index).toEqual('gs-index');
			expect(schema).toEqual({
				partition: 'gsiPk',
				sort: 'gsiSk'
			});
		});

		it('should resolve only partition by GSI', () => {
			// @ts-expect-error
			const { index, schema } = db.resolveSchema({
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
			await db.batchWrite(createItems({ count: 10 }));
		});

		afterAll(async () => {
			await db.clear();
		});

		beforeEach(() => {
			vi.spyOn(db.client, 'send');
		});

		it('should scan until limit with onChunk', async () => {
			const onChunk = vi.fn();
			const { count, lastEvaluatedKey } = await db.scan({
				chunkLimit: 1,
				limit: 2,
				onChunk
			});

			expect(db.client.send).toHaveBeenCalledTimes(2);
			expect(db.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConsistentRead: false,
						Limit: 1,
						TableName: 'use-dynamodb-spec'
					})
				})
			);
			expect(db.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConsistentRead: false,
						ExclusiveStartKey: { pk: 'pk-1', sk: 'sk-001' },
						Limit: 2,
						TableName: 'use-dynamodb-spec'
					})
				})
			);

			expect(count).toEqual(2);
			expect(lastEvaluatedKey).toEqual({ pk: 'pk-1', sk: 'sk-003' });

			vi.mocked(db.client.send).mockClear();
			const { count: count2, lastEvaluatedKey: lastEvaluatedKey2 } = await db.scan({
				startKey: lastEvaluatedKey
			});

			expect(db.client.send).toHaveBeenCalledOnce();
			expect(db.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConsistentRead: false,
						ExclusiveStartKey: lastEvaluatedKey,
						TableName: 'use-dynamodb-spec'
					})
				})
			);

			expect(count2).toEqual(8);
			expect(lastEvaluatedKey2).toBeNull();
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

		it('should scan with segment and totalSegments', async () => {
			const { count } = await db.scan({
				segment: 1,
				totalSegments: 2
			});

			expect(db.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						Segment: 1,
						TotalSegments: 2
					})
				})
			);

			expect(count).toEqual(0);
		});

		it('should scan with empty string in indexes', async () => {
			await db.put({
				gsiSk: '',
				lsiSk: '',
				pk: 'pk-empty',
				sk: 'sk-empty'
			});

			const { count, items } = await db.scan({
				attributeNames: { '#pk': 'pk' },
				attributeValues: { ':pk': 'pk-empty' },
				filterExpression: '#pk = :pk'
			});

			expect(count).toEqual(1);
			expect(items[0]).toEqual(
				expect.objectContaining({
					gsiSk: '',
					lsiSk: '',
					pk: 'pk-empty',
					sk: 'sk-empty'
				})
			);
		});
	});

	describe('scanAllPartition', () => {
		beforeAll(async () => {
			await db.batchWrite(createItems({ count: 100 }));
		});

		afterAll(async () => {
			await db.clear();
		});

		beforeEach(() => {
			vi.spyOn(db, 'query');
		});

		it('should scan by segmentsSize', async () => {
			const res = await db.scanAllPartition({
				partitionKey: 'pk-0',
				segmentsSize: 20
			});

			expect(db.query).toHaveBeenCalledTimes(4);
			expect(db.query).toHaveBeenCalledWith({
				attributeNames: {
					'#__sk': 'sk'
				},
				attributeValues: {
					':__sk_to': 'sk-038'
				},
				chunkLimit: Infinity,
				consistentRead: false,
				filterExpression: undefined,
				item: {
					pk: 'pk-0'
				},
				limit: Infinity,
				onChunk: undefined,
				queryExpression: '#__sk <= :__sk_to',
				scanIndexForward: true,
				select: undefined
			});

			expect(db.query).toHaveBeenCalledWith({
				attributeNames: {
					'#__sk': 'sk'
				},
				attributeValues: {
					':__sk_from': 'sk-040',
					':__sk_to': 'sk-078'
				},
				chunkLimit: Infinity,
				consistentRead: false,
				filterExpression: undefined,
				item: {
					pk: 'pk-0'
				},
				limit: Infinity,
				onChunk: undefined,
				queryExpression: '#__sk BETWEEN :__sk_from AND :__sk_to',
				scanIndexForward: true,
				select: undefined
			});

			expect(db.query).toHaveBeenCalledWith({
				attributeNames: {
					'#__sk': 'sk'
				},
				attributeValues: {
					':__sk_from': 'sk-080'
				},
				chunkLimit: Infinity,
				consistentRead: false,
				filterExpression: undefined,
				item: {
					pk: 'pk-0'
				},
				limit: Infinity,
				onChunk: undefined,
				queryExpression: '#__sk >= :__sk_from',
				scanIndexForward: true,
				select: undefined
			});

			expect(res.count).toEqual(50); // Half of the items have pk-0
			expect(
				res.items.every(item => {
					return item.pk === 'pk-0';
				})
			).toBeTruthy();
			expect(res.lastEvaluatedKey).toBeNull();
		});

		it('should scan by segments', async () => {
			const res = await db.scanAllPartition({
				partitionKey: 'pk-0',
				segments: [
					[null, 'sk-050'],
					['sk-051', null]
				]
			});

			expect(res.count).toEqual(50);
			expect(
				res.items.every(item => {
					return item.pk === 'pk-0';
				})
			).toBeTruthy();
			expect(res.lastEvaluatedKey).toBeNull();
		});
	});

	describe('transformFromStorage', () => {
		it('should replace placeholder with empty strings in index keys only', () => {
			const item = {
				pk: 'test-pk',
				sk: '__EMPTY_STRING__',
				lsiSk: '__EMPTY_STRING__',
				gsiSk: '__EMPTY_STRING__',
				gsiPk: 'test-gsi-pk',
				foo: 'test-value'
			};

			// @ts-expect-error
			const res = db.transformFromStorage(item);
			expect(res).toEqual({
				pk: 'test-pk',
				sk: '__EMPTY_STRING__', // Main sort key should not be transformed
				lsiSk: '',
				gsiSk: '',
				gsiPk: 'test-gsi-pk',
				foo: 'test-value'
			});
		});

		it('should not affect non-placeholder strings', () => {
			const item = {
				pk: 'test-pk',
				sk: 'non-placeholder',
				lsiSk: 'also-non-placeholder',
				gsiSk: 'another-value',
				gsiPk: 'test-gsi-pk',
				foo: 'test-value'
			};

			// @ts-expect-error
			const res = db.transformFromStorage(item);
			expect(res).toEqual(item);
		});

		it('should not affect non-key attributes with placeholder value', () => {
			const item = {
				pk: 'test-pk',
				sk: 'test-sk',
				lsiSk: 'test-lsi',
				gsiSk: 'test-gsi',
				gsiPk: 'test-gsi-pk',
				foo: '__EMPTY_STRING__' // This should remain as is since foo is not a key
			};

			// @ts-expect-error
			const res = db.transformFromStorage(item);
			expect(res).toEqual(item);
		});
	});

	describe('transformForStorage', () => {
		it('should replace empty strings in index keys only with placeholder', () => {
			const item = {
				pk: 'test-pk',
				sk: '',
				lsiSk: '',
				gsiSk: '',
				gsiPk: 'test-gsi-pk',
				foo: 'test-value'
			};

			// @ts-expect-error
			const res = db.transformForStorage(item);
			expect(res).toEqual({
				pk: 'test-pk',
				sk: '', // Main sort key should not be transformed
				lsiSk: '__EMPTY_STRING__',
				gsiSk: '__EMPTY_STRING__',
				gsiPk: 'test-gsi-pk',
				foo: 'test-value'
			});
		});

		it('should not affect non-empty strings', () => {
			const item = {
				pk: 'test-pk',
				sk: 'non-empty',
				lsiSk: 'also-non-empty',
				gsiSk: 'another-value',
				gsiPk: 'test-gsi-pk',
				foo: 'test-value'
			};

			// @ts-expect-error
			const res = db.transformForStorage(item);
			expect(res).toEqual(item);
		});

		it('should not affect non-key string attributes', () => {
			const item = {
				pk: 'test-pk',
				sk: 'test-sk',
				lsiSk: 'test-lsi',
				gsiSk: 'test-gsi',
				gsiPk: 'test-gsi-pk',
				foo: '' // This should remain empty as it's not a key
			};

			// @ts-expect-error
			const res = db.transformForStorage(item);
			expect(res).toEqual(item);
		});
	});

	describe('update', () => {
		beforeEach(async () => {
			vi.spyOn(db, 'get');
			vi.spyOn(db, 'put');
			vi.spyOn(db.client, 'send');
		});

		afterEach(async () => {
			await db.clear();
		});

		it('should update without updateFunction neither updateExpression', async () => {
			await db.batchWrite(createItems({ count: 1 }));

			const res = await db.update({
				filter: {
					item: { pk: 'pk-0', sk: 'sk-000' }
				}
			});

			expect(db.put).toHaveBeenCalledWith(
				{
					__createdAt: expect.any(String),
					__ts: expect.any(Number),
					__updatedAt: expect.any(String),
					foo: 'foo-0',
					gsiPk: 'gsi-pk-0',
					gsiSk: 'gsi-sk-000',
					lsiSk: 'lsi-sk-000',
					pk: 'pk-0',
					sk: 'sk-000'
				},
				{
					attributeNames: {
						'#__pk': 'pk',
						'#__ts': '__ts'
					},
					attributeValues: { ':__curr_ts': expect.any(Number) },
					conditionExpression: '(attribute_exists(#__pk) AND #__ts = :__curr_ts)',
					overwrite: true,
					useCurrentCreatedAtIfExists: true
				}
			);

			expect(res.__updatedAt).not.toEqual(res.__createdAt);
			expect(res).toEqual(
				expect.objectContaining({
					foo: 'foo-0',
					gsiPk: 'gsi-pk-0',
					gsiSk: 'gsi-sk-000',
					lsiSk: 'lsi-sk-000',
					sk: 'sk-000',
					pk: 'pk-0'
				})
			);

			expect(onChangeMock).toHaveBeenCalledTimes(2);
		});

		it('should upsert without updateFunction neither updateExpression', async () => {
			const res = await db.update({
				filter: {
					item: { pk: 'pk-0', sk: 'sk-000' }
				},
				upsert: true
			});

			expect(db.put).toHaveBeenCalledWith(
				{
					pk: 'pk-0',
					sk: 'sk-000'
				},
				{
					attributeNames: {
						'#__pk': 'pk',
						'#__ts': '__ts'
					},
					attributeValues: { ':__curr_ts': 0 },
					conditionExpression: '(attribute_not_exists(#__pk) OR #__ts = :__curr_ts)',
					overwrite: true,
					useCurrentCreatedAtIfExists: true
				}
			);

			expect(res.__updatedAt).toEqual(res.__createdAt);
			expect(res).toEqual(
				expect.objectContaining({
					sk: 'sk-000',
					pk: 'pk-0'
				})
			);

			expect(onChangeMock).toHaveBeenCalledOnce();
		});

		describe('updateExpression', () => {
			it('should throw if no filter.item and inexistent item', async () => {
				try {
					await db.update({
						filter: {
							attributeNames: { '#pk': 'pk' },
							attributeValues: { ':pk': 'inexistent' },
							filterExpression: '#pk = :pk'
						},
						updateExpression: 'SET #pk = :pk'
					});

					throw new Error('expected to throw');
				} catch (err) {
					expect((err as Error).message).toEqual('Existing item or filter.item must be provided');
				}
			});

			it('should update', async () => {
				await db.batchWrite(createItems({ count: 1 }));

				const res = await db.update({
					attributeNames: { '#foo': 'foo', '#bar': 'bar' },
					attributeValues: { ':foo': 'foo-1', ':one': 1 },
					filter: {
						item: { pk: 'pk-0', sk: 'sk-000' }
					},
					updateExpression: 'SET #foo = if_not_exists(#foo, :foo) ADD #bar :one'
				});

				expect(db.get).not.toHaveBeenCalled();
				expect(db.client.send).toHaveBeenCalledWith(
					expect.objectContaining({
						input: expect.objectContaining({
							ConditionExpression: 'attribute_exists(#__pk)',
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
								':__ts': expect.any(Number),
								':__up': expect.any(String)
							},
							Key: {
								pk: 'pk-0',
								sk: 'sk-000'
							},
							ReturnValues: 'ALL_NEW',
							TableName: 'use-dynamodb-spec',
							UpdateExpression:
								'SET #foo = if_not_exists(#foo, :foo), #__cr = if_not_exists(#__cr, :__cr), #__ts = :__ts, #__up = :__up ADD #bar :one'
						})
					})
				);

				expect(res.__createdAt).not.toEqual(res.__updatedAt);
				expect(res).toEqual(
					expect.objectContaining({
						foo: 'foo-0',
						bar: 1,
						gsiPk: 'gsi-pk-0',
						gsiSk: 'gsi-sk-000',
						lsiSk: 'lsi-sk-000',
						pk: 'pk-0',
						sk: 'sk-000'
					})
				);

				expect(onChangeMock).toHaveBeenCalledTimes(2);
			});

			it('should update without filter.item', async () => {
				await db.batchWrite(createItems({ count: 1 }));

				const res = await db.update({
					attributeNames: { '#foo': 'foo', '#bar': 'bar' },
					attributeValues: { ':foo': 'foo-1', ':one': 1 },
					filter: {
						attributeNames: { '#pk': 'pk', '#sk': 'sk' },
						attributeValues: { ':pk': 'pk-0', ':sk': 'sk-000' },
						filterExpression: '#pk = :pk AND #sk = :sk'
					},
					updateExpression: 'SET #foo = if_not_exists(#foo, :foo) ADD #bar :one'
				});

				expect(db.get).toHaveBeenCalledWith({
					attributeNames: { '#pk': 'pk', '#sk': 'sk' },
					attributeValues: { ':pk': 'pk-0', ':sk': 'sk-000' },
					consistentRead: true,
					filterExpression: '#pk = :pk AND #sk = :sk'
				});

				expect(db.client.send).toHaveBeenCalledWith(
					expect.objectContaining({
						input: expect.objectContaining({
							ConditionExpression: 'attribute_exists(#__pk)',
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
								':__ts': expect.any(Number),
								':__up': expect.any(String)
							},
							Key: {
								pk: 'pk-0',
								sk: 'sk-000'
							},
							ReturnValues: 'ALL_NEW',
							TableName: 'use-dynamodb-spec',
							UpdateExpression:
								'SET #foo = if_not_exists(#foo, :foo), #__cr = if_not_exists(#__cr, :__cr), #__ts = :__ts, #__up = :__up ADD #bar :one'
						})
					})
				);

				expect(res.__createdAt).not.toEqual(res.__updatedAt);
				expect(res).toEqual(
					expect.objectContaining({
						foo: 'foo-0',
						bar: 1,
						gsiPk: 'gsi-pk-0',
						gsiSk: 'gsi-sk-000',
						lsiSk: 'lsi-sk-000',
						pk: 'pk-0',
						sk: 'sk-000'
					})
				);

				expect(onChangeMock).toHaveBeenCalledTimes(2);
			});

			it('should upsert', async () => {
				const res = await db.update({
					attributeNames: { '#foo': 'foo', '#bar': 'bar' },
					attributeValues: { ':foo': 'foo-1', ':one': 1 },
					filter: {
						item: { pk: 'pk-0', sk: 'sk-000' }
					},
					updateExpression: 'SET #foo = if_not_exists(#foo, :foo) ADD #bar :one',
					upsert: true
				});

				expect(db.get).not.toHaveBeenCalled();
				expect(db.client.send).toHaveBeenCalledWith(
					expect.objectContaining({
						input: expect.objectContaining({
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
								':__ts': expect.any(Number),
								':__up': expect.any(String)
							},
							Key: {
								pk: 'pk-0',
								sk: 'sk-000'
							},
							ReturnValues: 'ALL_NEW',
							TableName: 'use-dynamodb-spec',
							UpdateExpression:
								'SET #foo = if_not_exists(#foo, :foo), #__cr = if_not_exists(#__cr, :__cr), #__ts = :__ts, #__up = :__up ADD #bar :one'
						})
					})
				);

				expect(res.__createdAt).toEqual(res.__updatedAt);
				expect(res).toEqual(
					expect.objectContaining({
						foo: 'foo-1',
						bar: 1,
						pk: 'pk-0',
						sk: 'sk-000'
					})
				);

				expect(onChangeMock).toHaveBeenCalledOnce();
			});
		});

		describe('updateFunction', () => {
			it('should throw if item not found', async () => {
				try {
					await db.update({
						filter: {
							item: { pk: 'pk-0', sk: 'sk-001' }
						},
						updateFunction: item => {
							return {
								...item,
								foo: 'foo-1'
							};
						}
					});

					throw new Error('expected to throw');
				} catch (err) {
					expect((err as Error).message).toEqual('Item not found');
				}
			});

			it('should throw if no filter.item and inexistent item', async () => {
				try {
					await db.update({
						filter: {
							attributeNames: { '#pk': 'pk' },
							attributeValues: { ':pk': 'inexistent' },
							filterExpression: '#pk = :pk'
						},
						updateFunction: item => {
							return {
								...item,
								foo: 'foo-1'
							};
						}
					});

					throw new Error('expected to throw');
				} catch (err) {
					expect((err as Error).message).toEqual('Item not found');
				}
			});

			it('should update', async () => {
				await db.batchWrite(createItems({ count: 1 }));

				const res = await db.update({
					filter: {
						item: { pk: 'pk-0', sk: 'sk-000' }
					},
					updateFunction: item => {
						return {
							...item,
							foo: 'foo-1'
						};
					}
				});

				expect(db.get).toHaveBeenCalledWith({
					item: { pk: 'pk-0', sk: 'sk-000' },
					consistentRead: true
				});

				expect(db.put).toHaveBeenCalledWith(
					{
						__createdAt: expect.any(String),
						__ts: expect.any(Number),
						__updatedAt: expect.any(String),
						foo: 'foo-1',
						gsiPk: 'gsi-pk-0',
						gsiSk: 'gsi-sk-000',
						lsiSk: 'lsi-sk-000',
						pk: 'pk-0',
						sk: 'sk-000'
					},
					{
						attributeNames: {
							'#__pk': 'pk',
							'#__ts': '__ts'
						},
						attributeValues: { ':__curr_ts': expect.any(Number) },
						conditionExpression: '(attribute_exists(#__pk) AND #__ts = :__curr_ts)',
						overwrite: true,
						useCurrentCreatedAtIfExists: true
					}
				);

				expect(res.__updatedAt).not.toEqual(res.__createdAt);
				expect(res).toEqual(
					expect.objectContaining({
						foo: 'foo-1',
						gsiPk: 'gsi-pk-0',
						gsiSk: 'gsi-sk-000',
						lsiSk: 'lsi-sk-000',
						sk: 'sk-000',
						pk: 'pk-0'
					})
				);

				expect(onChangeMock).toHaveBeenCalledTimes(2);
			});

			it('should update without filter.item', async () => {
				await db.batchWrite(createItems({ count: 1 }));

				const res = await db.update({
					filter: {
						attributeNames: { '#pk': 'pk', '#sk': 'sk' },
						attributeValues: { ':pk': 'pk-0', ':sk': 'sk-000' },
						filterExpression: '#pk = :pk AND #sk = :sk'
					},
					updateFunction: item => {
						return {
							...item,
							foo: 'foo-1'
						};
					}
				});

				expect(db.get).toHaveBeenCalledWith({
					attributeNames: { '#pk': 'pk', '#sk': 'sk' },
					attributeValues: { ':pk': 'pk-0', ':sk': 'sk-000' },
					consistentRead: true,
					filterExpression: '#pk = :pk AND #sk = :sk'
				});

				expect(db.put).toHaveBeenCalledWith(
					{
						__createdAt: expect.any(String),
						__ts: expect.any(Number),
						__updatedAt: expect.any(String),
						foo: 'foo-1',
						gsiPk: 'gsi-pk-0',
						gsiSk: 'gsi-sk-000',
						lsiSk: 'lsi-sk-000',
						pk: 'pk-0',
						sk: 'sk-000'
					},
					{
						attributeNames: {
							'#__pk': 'pk',
							'#__ts': '__ts'
						},
						attributeValues: { ':__curr_ts': expect.any(Number) },
						conditionExpression: '(attribute_exists(#__pk) AND #__ts = :__curr_ts)',
						overwrite: true,
						useCurrentCreatedAtIfExists: true
					}
				);

				expect(res.__updatedAt).not.toEqual(res.__createdAt);
				expect(res).toEqual(
					expect.objectContaining({
						foo: 'foo-1',
						gsiPk: 'gsi-pk-0',
						gsiSk: 'gsi-sk-000',
						lsiSk: 'lsi-sk-000',
						sk: 'sk-000',
						pk: 'pk-0'
					})
				);

				expect(onChangeMock).toHaveBeenCalledTimes(2);
			});

			it('should update with consistencyCheck = false', async () => {
				await db.batchWrite(createItems({ count: 1 }));

				const res = await db.update({
					consistencyCheck: false,
					filter: {
						item: { pk: 'pk-0', sk: 'sk-000' }
					},
					updateFunction: item => {
						return {
							...item,
							foo: 'foo-1'
						};
					}
				});

				expect(db.get).toHaveBeenCalledWith({
					item: { pk: 'pk-0', sk: 'sk-000' },
					consistentRead: true
				});

				expect(db.put).toHaveBeenCalledWith(
					{
						__createdAt: expect.any(String),
						__ts: expect.any(Number),
						__updatedAt: expect.any(String),
						foo: 'foo-1',
						gsiPk: 'gsi-pk-0',
						gsiSk: 'gsi-sk-000',
						lsiSk: 'lsi-sk-000',
						pk: 'pk-0',
						sk: 'sk-000'
					},
					{
						attributeNames: { '#__pk': 'pk' },
						conditionExpression: 'attribute_exists(#__pk)',
						overwrite: true,
						useCurrentCreatedAtIfExists: true
					}
				);

				expect(res.__updatedAt).not.toEqual(res.__createdAt);
				expect(res).toEqual(
					expect.objectContaining({
						foo: 'foo-1',
						gsiPk: 'gsi-pk-0',
						gsiSk: 'gsi-sk-000',
						lsiSk: 'lsi-sk-000',
						sk: 'sk-000',
						pk: 'pk-0'
					})
				);

				expect(onChangeMock).toHaveBeenCalledTimes(2);
			});

			it('should upsert', async () => {
				const res = await db.update({
					filter: {
						item: { pk: 'pk-0', sk: 'sk-000' }
					},
					updateFunction: item => {
						return {
							...item,
							foo: 'foo-1'
						};
					},
					upsert: true
				});

				expect(db.get).toHaveBeenCalledWith({
					item: { pk: 'pk-0', sk: 'sk-000' },
					consistentRead: true
				});

				expect(db.put).toHaveBeenCalledWith(
					{
						foo: 'foo-1',
						pk: 'pk-0',
						sk: 'sk-000'
					},
					{
						attributeNames: {
							'#__pk': 'pk',
							'#__ts': '__ts'
						},
						attributeValues: { ':__curr_ts': 0 },
						conditionExpression: '(attribute_not_exists(#__pk) OR #__ts = :__curr_ts)',
						overwrite: true,
						useCurrentCreatedAtIfExists: true
					}
				);

				expect(res.__createdAt).toEqual(res.__updatedAt);
				expect(res).toEqual(
					expect.objectContaining({
						foo: 'foo-1',
						pk: 'pk-0',
						sk: 'sk-000'
					})
				);

				expect(onChangeMock).toHaveBeenCalledOnce();
			});

			it('should upsert with consistencyCheck = false', async () => {
				const res = await db.update({
					consistencyCheck: false,
					filter: {
						item: { pk: 'pk-0', sk: 'sk-000' }
					},
					updateFunction: item => {
						return {
							...item,
							foo: 'foo-1'
						};
					},
					upsert: true
				});

				expect(db.get).toHaveBeenCalledWith({
					item: { pk: 'pk-0', sk: 'sk-000' },
					consistentRead: true
				});

				expect(db.put).toHaveBeenCalledWith(
					{
						foo: 'foo-1',
						pk: 'pk-0',
						sk: 'sk-000'
					},
					{
						overwrite: true,
						useCurrentCreatedAtIfExists: true
					}
				);

				expect(res.__createdAt).toEqual(res.__updatedAt);
				expect(res).toEqual(
					expect.objectContaining({
						foo: 'foo-1',
						pk: 'pk-0',
						sk: 'sk-000'
					})
				);

				expect(onChangeMock).toHaveBeenCalledOnce();
			});

			it('should not update partition and sort', async () => {
				await db.batchWrite(createItems({ count: 1 }));

				try {
					await db.update({
						filter: {
							item: { pk: 'pk-0', sk: 'sk-000' }
						},
						updateFunction: item => {
							return {
								...item,
								pk: 'pk-1',
								sk: 'sk-001',
								foo: 'foo-1'
							};
						}
					});
				} catch (err) {
					expect(err.name).toContain('ConditionalCheckFailedException');
				}
			});

			it('should update partition key with transaction', async () => {
				await db.batchWrite(createItems({ count: 1 }));

				const res = await db.update({
					allowUpdatePartitionAndSort: true,
					filter: {
						item: { pk: 'pk-0', sk: 'sk-000' }
					},
					updateFunction: item => {
						return {
							...item,
							pk: 'pk-1'
						};
					}
				});

				expect(db.client.send).toHaveBeenCalledWith(
					expect.objectContaining({
						input: expect.objectContaining({
							TransactItems: expect.arrayContaining([
								expect.objectContaining({
									Delete: expect.objectContaining({
										Key: {
											pk: 'pk-0',
											sk: 'sk-000'
										},
										TableName: 'use-dynamodb-spec'
									})
								}),
								expect.objectContaining({
									Put: expect.objectContaining({
										Item: expect.objectContaining({
											pk: 'pk-1',
											sk: 'sk-000',
											__ts: expect.any(Number)
										}),
										TableName: 'use-dynamodb-spec'
									})
								})
							])
						})
					})
				);

				expect(res).toEqual(
					expect.objectContaining({
						pk: 'pk-1',
						sk: 'sk-000'
					})
				);

				expect(onChangeMock).toHaveBeenCalledTimes(2);
			});

			it('should update sort key with transaction', async () => {
				await db.batchWrite(createItems({ count: 1 }));

				const res = await db.update({
					allowUpdatePartitionAndSort: true,
					filter: {
						item: { pk: 'pk-0', sk: 'sk-000' }
					},
					updateFunction: item => {
						return {
							...item,
							sk: 'sk-001'
						};
					}
				});

				expect(db.client.send).toHaveBeenCalledWith(
					expect.objectContaining({
						input: expect.objectContaining({
							TransactItems: expect.arrayContaining([
								expect.objectContaining({
									Delete: expect.objectContaining({
										Key: {
											pk: 'pk-0',
											sk: 'sk-000'
										},
										TableName: 'use-dynamodb-spec'
									})
								}),
								expect.objectContaining({
									Put: expect.objectContaining({
										Item: expect.objectContaining({
											pk: 'pk-0',
											sk: 'sk-001',
											__ts: expect.any(Number)
										}),
										TableName: 'use-dynamodb-spec'
									})
								})
							])
						})
					})
				);

				expect(res).toEqual(
					expect.objectContaining({
						pk: 'pk-0',
						sk: 'sk-001'
					})
				);

				expect(onChangeMock).toHaveBeenCalledTimes(2);
			});

			it('should update with empty string in indexes', async () => {
				await db.put({
					pk: 'pk-update-empty',
					sk: 'sk-0',
					gsiSk: '',
					lsiSk: '',
					foo: 'original-value'
				});

				const res = await db.update({
					filter: {
						item: { pk: 'pk-update-empty', sk: 'sk-0' }
					},
					updateFunction: item => {
						return {
							...item,
							foo: 'updated-value'
						};
					}
				});

				expect(res).toEqual(
					expect.objectContaining({
						pk: 'pk-update-empty',
						sk: 'sk-0',
						gsiSk: '',
						lsiSk: '',
						foo: 'updated-value'
					})
				);
			});
		});
	});
});
