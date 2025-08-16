import _ from 'lodash';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';

import Db from './index';
import { ENDPOINT } from './constants';

type DbRecord = {
	foo: string;
	gsiPk: string;
	gsiSk: string;
	pk: string;
};

const createItems = ({ count }: { count: number }) => {
	return _.times(count, i => {
		return {
			foo: `foo-${i}`,
			gsiPk: `gsi-pk-${i % 2}`,
			gsiSk: `gsi-sk-${i}`,
			pk: `pk-${i}`
		};
	});
};

const factory = () => {
	return new Db<DbRecord>({
		accessKeyId: process.env.AWS_ACCESS_KEY || '',
		endpoint: ENDPOINT,
		region: 'us-east-1',
		secretAccessKey: process.env.AWS_SECRET_KEY || '',
		indexes: [
			{
				name: 'gs-index',
				partition: 'gsiPk',
				sort: 'gsiSk',
				sortType: 'S'
			}
		],
		schema: { partition: 'pk' },
		table: 'use-dynamodb-no-sort-spec'
	});
};

describe('/index-no-sort.ts', () => {
	let db: Db<DbRecord>;

	beforeAll(async () => {
		db = factory();

		await db.createTable();
	});

	beforeEach(() => {
		db = factory();
	});

	describe('createTable', () => {
		it('should works', async () => {
			const res = await db.createTable();

			if ('Table' in res) {
				expect(res.Table?.TableName).toEqual('use-dynamodb-no-sort-spec');
			} else if ('TableDescription' in res) {
				expect(res.TableDescription?.TableName).toEqual('use-dynamodb-no-sort-spec');
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
			const batchWriteItems = await db.batchWrite(createItems({ count: 2 }));
			expect(
				_.every(batchWriteItems, item => {
					return _.isNumber(item.__ts);
				})
			).toBeTruthy();

			const batchGetItems = await db.batchGet(batchWriteItems);
			expect(batchGetItems).toHaveLength(2);

			const batchDeleteItems = await db.batchDelete([{ pk: 'pk-0' }, { pk: 'pk-1' }]);
			expect(batchDeleteItems).toHaveLength(2);

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
		});
	});

	describe('clear', () => {
		afterAll(async () => {
			await db.clear();
		});

		it('should clear', async () => {
			await db.batchWrite(createItems({ count: 2 }));

			const res1 = await db.scan();
			expect(res1.count).toEqual(2);

			const { count } = await db.clear();
			expect(count).toEqual(2);

			const res2 = await db.scan();
			expect(res2.count).toEqual(0);
		});

		it('should clear by pk', async () => {
			await db.batchWrite(createItems({ count: 2 }));

			const res1 = await db.scan();
			expect(res1.count).toEqual(2);

			const { count } = await db.clear('pk-0');
			expect(count).toEqual(1);

			const res2 = await db.scan();
			expect(res2.count).toEqual(1);
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
					item: { pk: 'pk-100' }
				}
			});

			expect(res).toBeNull();
		});

		it('should delete', async () => {
			const res = await db.delete({
				filter: {
					item: { pk: 'pk-0' }
				}
			});

			expect(db.get).toHaveBeenCalledWith({
				item: { pk: 'pk-0' }
			});

			expect(res).toEqual(
				expect.objectContaining({
					foo: 'foo-0',
					gsiPk: 'gsi-pk-0',
					gsiSk: 'gsi-sk-0',
					pk: 'pk-0'
				})
			);
		});
	});

	describe('deleteMany', () => {
		beforeAll(async () => {
			await db.batchWrite(createItems({ count: 2 }));
		});

		afterAll(async () => {
			await db.clear();
		});

		it('should delete', async () => {
			const batchDeleteItems = await db.deleteMany({
				item: { pk: 'pk-0' }
			});

			expect(batchDeleteItems).toHaveLength(1);
		});
	});

	describe('filter', () => {
		beforeAll(async () => {
			await db.batchWrite(createItems({ count: 2 }));
		});

		afterAll(async () => {
			await db.clear();
		});

		it('should filter by item', async () => {
			const { count, lastEvaluatedKey } = await db.filter({
				item: { pk: 'pk-0' }
			});

			expect(count).toEqual(1);
			expect(lastEvaluatedKey).toBeNull();
		});

		it('should filter by query expression', async () => {
			const { count, lastEvaluatedKey } = await db.filter({
				attributeNames: { '#pk': 'pk' },
				attributeValues: { ':pk': 'pk-0' },
				queryExpression: '#pk = :pk'
			});

			expect(count).toEqual(1);
			expect(lastEvaluatedKey).toBeNull();
		});

		it('should filter by scan', async () => {
			const { count, lastEvaluatedKey } = await db.filter({
				attributeNames: { '#pk': 'pk' },
				attributeValues: { ':pk': 'pk-0' },
				filterExpression: '#pk = :pk'
			});

			expect(count).toEqual(1);
			expect(lastEvaluatedKey).toBeNull();
		});
	});

	describe('get', () => {
		beforeAll(async () => {
			await db.batchWrite(createItems({ count: 1 }));
		});

		afterAll(async () => {
			await db.clear();
		});

		it('should return null if not found', async () => {
			const res = await db.get({
				item: { pk: 'pk-100' }
			});

			expect(res).toBeNull();
		});

		it('should get', async () => {
			const res = await db.get({
				item: { pk: 'pk-0' }
			});

			expect(res).toEqual(
				expect.objectContaining({
					foo: 'foo-0',
					gsiPk: 'gsi-pk-0',
					gsiSk: 'gsi-sk-0',
					pk: 'pk-0'
				})
			);
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
			const res = await db.put({ pk: 'pk-0' });

			expect(res.__createdAt).toEqual(res.__updatedAt);
			expect(res).toEqual(expect.objectContaining({ pk: 'pk-0' }));
		});
	});

	describe('query', () => {
		beforeAll(async () => {
			await db.batchWrite(createItems({ count: 1 }));
		});

		afterAll(async () => {
			await db.clear();
		});

		it('should query', async () => {
			const { count, lastEvaluatedKey } = await db.query({
				item: { pk: 'pk-0' }
			});

			expect(count).toEqual(1);
			expect(lastEvaluatedKey).toBeNull();
		});
	});

	describe('replace', () => {
		afterEach(async () => {
			await db.clear();
		});

		it('should replace', async () => {
			const replacedItem = await db.put({ pk: 'pk-0' });
			const newItem = await db.replace({ pk: 'pk-1' }, replacedItem);

			expect(newItem).toEqual({
				pk: 'pk-1',
				__createdAt: replacedItem.__createdAt,
				__ts: newItem.__ts,
				__updatedAt: newItem.__updatedAt
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
			const { items, count, lastEvaluatedKey } = await db.scan({
				chunkLimit: 1,
				limit: 2,
				onChunk
			});

			expect(count).toEqual(2);

			vi.mocked(db.client.send).mockClear();
			const { items: items2, count: count2 } = await db.scan({
				startKey: lastEvaluatedKey
			});

			expect(count2).toEqual(8);
			expect(items[0].pk !== items2[0].pk).toBeTruthy();
		});
	});

	describe('update', () => {
		afterEach(async () => {
			await db.clear();
		});

		it('should update', async () => {
			await db.batchWrite(createItems({ count: 1 }));

			const res = await db.update({
				filter: {
					item: { pk: 'pk-0' }
				},
				updateFunction: item => {
					return {
						...item,
						foo: 'foo-1'
					};
				}
			});

			expect(res.__updatedAt).not.toEqual(res.__createdAt);
			expect(res).toEqual(
				expect.objectContaining({
					foo: 'foo-1',
					gsiPk: 'gsi-pk-0',
					gsiSk: 'gsi-sk-0',
					pk: 'pk-0'
				})
			);
		});

		it('should upsert', async () => {
			const res = await db.update({
				filter: {
					item: { pk: 'pk-0' }
				},
				updateFunction: item => {
					return {
						...item,
						foo: 'foo-1'
					};
				},
				upsert: true
			});

			expect(res.__createdAt).toEqual(res.__updatedAt);
			expect(res).toEqual(
				expect.objectContaining({
					foo: 'foo-1',
					pk: 'pk-0'
				})
			);
		});
	});
});
