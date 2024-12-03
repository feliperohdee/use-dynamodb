import _ from 'lodash';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, Mock, vi } from 'vitest';

import Dynamodb from './index';
import Layer, { DEFAULT_CURRENT_META } from './layer';

type Item = {
	pk: string;
	sk: string;
	state: string;
};

const createItem = (options: { index: number; pk?: number; state?: string; ts?: number }): Dynamodb.PersistedItem<Item> => {
	const { index, pk = 0, state = 'pending', ts = _.now() } = options;
	const nowISO = new Date(ts).toISOString();
	const indexString = _.padStart(`${index}`, 3, '0');

	return {
		__createdAt: nowISO,
		__ts: ts,
		__updatedAt: nowISO,
		pk: `pk-${pk}`,
		sk: `sk-${indexString}`,
		state: `${state}-${indexString}`
	};
};

const createChangeEvents = (options: {
	count: number;
	initialIndex?: number;
	pk?: number;
	state?: string;
	table?: string;
	ts?: number;
	type?: Dynamodb.ChangeType;
}): Dynamodb.ChangeEvent<Item>[] => {
	return _.times(options.count, index => {
		const { initialIndex = 0, pk = 0, state = 'pending', table = 'table-1', ts = _.now(), type = 'PUT' } = options || {};

		index += initialIndex;

		const item = createItem({
			index,
			pk,
			state,
			ts
		});

		const event: Dynamodb.ChangeEvent<Item> = {
			item,
			partition: item.pk,
			sort: item.sk,
			table,
			type
		};

		return event;
	});
};

const factory = async ({
	backgroundRunner,
	createTable = false,
	getter,
	setter,
	syncStrategy
}: {
	backgroundRunner: Mock;
	createTable?: boolean;
	getter: Mock;
	setter: Mock;
	syncStrategy: Mock;
}) => {
	const db = new Dynamodb<Layer.PendingEvent<Item>>(
		Layer.tableOptions({
			accessKeyId: process.env.AWS_ACCESS_KEY || '',
			region: 'us-east-1',
			secretAccessKey: process.env.AWS_SECRET_KEY || '',
			table: 'use-dynamodb-layer-spec'
		})
	);

	if (createTable) {
		await db.createTable();
	}

	return new Layer({
		backgroundRunner,
		db,
		getItemPartition: item => {
			return item.pk;
		},
		getItemUniqueIdentifier: item => {
			return item.sk;
		},
		getter,
		setter,
		syncStrategy,
		table: 'table-1'
	});
};

describe('/layer.ts', () => {
	let layer: Layer<Item>;

	beforeAll(async () => {
		layer = await factory({
			backgroundRunner: vi.fn(),
			createTable: true,
			getter: vi.fn(async () => []),
			setter: vi.fn(),
			syncStrategy: vi.fn()
		});
	});

	afterAll(async () => {
		await layer.db.clear();
	});

	beforeEach(async () => {
		layer = await factory({
			backgroundRunner: vi.fn(),
			createTable: false,
			getter: vi.fn(async () => []),
			setter: vi.fn(),
			syncStrategy: vi.fn()
		});
	});

	describe('acquireLock', () => {
		beforeEach(async () => {
			await layer.db.clear();
		});

		afterAll(async () => {
			await layer.db.clear();
		});

		it('should acquire lock', async () => {
			const lock = await layer.acquireLock(true);

			expect(lock).toBeTruthy();

			const lockItem = await layer.db.get({
				item: { pk: '__table-1__', sk: '__lock__' }
			});
			expect(lockItem).toBeTruthy();
		});

		it('should fail to acquire lock if already locked', async () => {
			await layer.acquireLock(true);

			const lock = await layer.acquireLock(true);

			expect(lock).toBeFalsy();
		});

		it('should release lock', async () => {
			await layer.acquireLock(true);

			const lock = await layer.acquireLock(false);

			expect(lock).toBeTruthy();

			const lockItem = await layer.db.get({
				item: { pk: '__table-1__', sk: '__lock__' }
			});
			expect(lockItem).toBeFalsy();
		});
	});

	describe('get', () => {
		beforeAll(async () => {
			await Promise.all([
				// PUT 0-5
				layer.set(
					createChangeEvents({
						count: 5
					}),
					0
				),
				// DELETE 2-3
				layer.set(
					createChangeEvents({
						count: 2,
						initialIndex: 2,
						type: 'DELETE'
					}),
					0
				),
				// PUT 8-11
				layer.set(
					createChangeEvents({
						count: 4,
						initialIndex: 8
					}),
					0
				)
			]);

			await new Promise(resolve => {
				return setTimeout(resolve, 100);
			});

			await Promise.all([
				// DELETE 3-4
				layer.set(
					createChangeEvents({
						count: 2,
						initialIndex: 3,
						type: 'DELETE'
					}),
					1
				),
				// PUT 11-12
				layer.set(
					createChangeEvents({
						count: 2,
						initialIndex: 11
					}),
					1
				)
			]);
		});

		afterAll(async () => {
			await layer.db.clear();
		});

		beforeEach(async () => {
			vi.mocked(layer.getter).mockImplementation(async (partition: string) => {
				if (_.startsWith(partition, 'table-1#pk-0')) {
					return _.times(10, index => {
						return createItem({
							index,
							state: 'layer'
						});
					});
				}

				return [];
			});

			vi.spyOn(layer, 'merge');
			vi.spyOn(layer, 'meta');
			vi.spyOn(layer.db, 'query');
		});

		it('should returns', async () => {
			const res = await layer.get('pk-0');

			expect(layer.meta).toHaveBeenCalledWith();
			expect(layer.getter).toHaveBeenCalledWith('table-1#pk-0');
			expect(layer.db.query).toHaveBeenCalledWith({
				item: {
					pk: 'table-1#pk-0#0'
				},
				limit: Infinity
			});
			expect(layer.merge).toHaveBeenCalledWith(expect.any(Array), expect.any(Array));

			expect(res).toEqual([
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-000',
					state: 'pending-000'
				}),
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-001',
					state: 'pending-001'
				}),
				// ** DELETED **
				// expect.objectContaining({
				// 	pk: 'pk-0',
				// 	sk: 'sk-002',
				// 	state: 'pending-002'
				// }),
				// expect.objectContaining({
				// 	pk: 'pk-0',
				// 	sk: 'sk-003',
				// 	state: 'pending-003'
				// }),
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-004',
					state: 'pending-004'
				}),
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-005',
					state: 'layer-005'
				}),
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-006',
					state: 'layer-006'
				}),
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-007',
					state: 'layer-007'
				}),
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-008',
					state: 'pending-008'
				}),
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-009',
					state: 'pending-009'
				}),
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-010',
					state: 'pending-010'
				}),
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-011',
					state: 'pending-011'
				})
			]);
		});

		it('should returns on cursor', async () => {
			vi.mocked(layer.meta).mockResolvedValue({ cursor: 1, cursorMax: 1 } as Layer.Meta);

			const res = await layer.get('pk-0');

			expect(layer.meta).toHaveBeenCalledWith();
			expect(layer.getter).toHaveBeenCalledWith('table-1#pk-0');
			expect(layer.db.query).toHaveBeenCalledWith({
				item: {
					pk: 'table-1#pk-0#1'
				},
				limit: Infinity
			});
			expect(layer.merge).toHaveBeenCalledWith(expect.any(Array), expect.any(Array));

			expect(res).toEqual([
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-000',
					state: 'layer-000'
				}),
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-001',
					state: 'layer-001'
				}),
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-002',
					state: 'layer-002'
				}),
				// ** DELETED **
				// expect.objectContaining({
				// 	pk: 'pk-0',
				// 	sk: 'sk-003',
				// 	state: 'layer-003'
				// }),
				// expect.objectContaining({
				// 	pk: 'pk-0',
				// 	sk: 'sk-004',
				// 	state: 'layer-004'
				// }),
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-005',
					state: 'layer-005'
				}),
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-006',
					state: 'layer-006'
				}),
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-007',
					state: 'layer-007'
				}),
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-008',
					state: 'layer-008'
				}),
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-009',
					state: 'layer-009'
				}),
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-011',
					state: 'pending-011'
				}),
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-012',
					state: 'pending-012'
				})
			]);
		});

		it('should returns on multiple cursors', async () => {
			vi.mocked(layer.meta).mockResolvedValue({ cursor: 0, cursorMax: 1 } as Layer.Meta);

			const res = await layer.get('pk-0');

			expect(layer.meta).toHaveBeenCalledWith();
			expect(layer.getter).toHaveBeenCalledWith('table-1#pk-0');
			expect(layer.db.query).toHaveBeenCalledTimes(2);
			expect(layer.db.query).toHaveBeenCalledWith({
				item: {
					pk: 'table-1#pk-0#0'
				},
				limit: Infinity
			});
			expect(layer.db.query).toHaveBeenCalledWith({
				item: {
					pk: 'table-1#pk-0#1'
				},
				limit: Infinity
			});
			expect(layer.merge).toHaveBeenCalledWith(expect.any(Array), expect.any(Array));

			expect(res).toEqual([
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-000',
					state: 'pending-000'
				}),
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-001',
					state: 'pending-001'
				}),
				// ** DELETED **
				// expect.objectContaining({
				// 	pk: 'pk-0',
				// 	sk: 'sk-002',
				// 	state: 'layer-002'
				// }),
				// expect.objectContaining({
				// 	pk: 'pk-0',
				// 	sk: 'sk-003',
				// 	state: 'layer-003'
				// }),
				// expect.objectContaining({
				// 	pk: 'pk-0',
				// 	sk: 'sk-004',
				// 	state: 'layer-004'
				// }),
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-005',
					state: 'layer-005'
				}),
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-006',
					state: 'layer-006'
				}),
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-007',
					state: 'layer-007'
				}),
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-008',
					state: 'pending-008'
				}),
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-009',
					state: 'pending-009'
				}),
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-010',
					state: 'pending-010'
				}),
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-011',
					state: 'pending-011'
				}),
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-012',
					state: 'pending-012'
				})
			]);
		});

		it('should returns empty if no partition', async () => {
			const res = await layer.get('pk-1');

			expect(layer.meta).toHaveBeenCalledWith();
			expect(layer.merge).toHaveBeenCalledWith(expect.any(Array), expect.any(Array));
			expect(res).toEqual([]);
		});

		it('should returns empty if empty partition', async () => {
			const res = await layer.get();

			expect(layer.meta).toHaveBeenCalledWith();
			expect(layer.merge).toHaveBeenCalledWith(expect.any(Array), expect.any(Array));
			expect(res).toEqual([]);
		});
	});

	describe('merge', () => {
		it('should merge pending items', () => {
			const now = _.now();
			const layerItems = _.times(12, index => {
				return createItem({
					index,
					state: 'layer'
				});
			});

			const pendingEventsCursor0 = _.times(10, index => {
				const item = createItem({
					index: 5 + index,
					state: 'pending-cursor-0'
				});

				const pendingEvent: Dynamodb.PersistedItem<Layer.PendingEvent<Item>> = {
					__createdAt: '',
					__updatedAt: '',
					__ts: now,
					cursor: 0,
					item,
					pk: 'table-1#pk-0',
					sk: item.sk,
					type: 'PUT',
					ttl: now
				};

				return pendingEvent;
			});

			const pendingEventsCursor1 = _.times(10, index => {
				const item = createItem({
					index: 5 + index,
					state: 'pending-cursor-1'
				});

				const pendingEvent: Dynamodb.PersistedItem<Layer.PendingEvent<Item>> = {
					__createdAt: '',
					__updatedAt: '',
					__ts: now + (index % 2 ? 1000 : -1000),
					cursor: 1,
					item,
					pk: 'table-1#pk-0',
					sk: item.sk,
					type: 'PUT',
					ttl: now
				};

				return pendingEvent;
			});

			const res = layer.merge(layerItems, [...pendingEventsCursor0, ...pendingEventsCursor1]);

			expect(_.map(res, 'state')).toEqual([
				'layer-000',
				'layer-001',
				'layer-002',
				'layer-003',
				'layer-004',
				'pending-cursor-0-005',
				'pending-cursor-1-006',
				'pending-cursor-0-007',
				'pending-cursor-1-008',
				'pending-cursor-0-009',
				'pending-cursor-1-010',
				'pending-cursor-0-011',
				'pending-cursor-1-012',
				'pending-cursor-0-013',
				'pending-cursor-1-014'
			]);

			expect(_.map(res, 'sk')).toEqual([
				'sk-000',
				'sk-001',
				'sk-002',
				'sk-003',
				'sk-004',
				'sk-005',
				'sk-006',
				'sk-007',
				'sk-008',
				'sk-009',
				'sk-010',
				'sk-011',
				'sk-012',
				'sk-013',
				'sk-014'
			]);
		});

		it('should handle DELETE type', () => {
			const now = _.now();
			const layerItems = _.times(12, index => {
				return createItem({
					index,
					state: 'layer'
				});
			});

			const pendingEventsCursor0 = _.times(10, index => {
				const item = createItem({
					index: 5 + index,
					state: 'pending-cursor-0'
				});

				const pendingEvent: Dynamodb.PersistedItem<Layer.PendingEvent<Item>> = {
					__createdAt: '',
					__updatedAt: '',
					__ts: now,
					cursor: 0,
					item,
					pk: 'table-1#pk-0',
					sk: item.sk,
					type: 'DELETE',
					ttl: now
				};

				return pendingEvent;
			});

			const pendingEventsCursor1 = _.times(5, index => {
				const item = createItem({
					index: 5 + index,
					state: 'pending-cursor-1'
				});

				const pendingEvent: Dynamodb.PersistedItem<Layer.PendingEvent<Item>> = {
					__createdAt: '',
					__updatedAt: '',
					__ts: now + (index % 2 ? 1000 : -1000),
					cursor: 1,
					item,
					pk: 'table-1#pk-0',
					sk: item.sk,
					type: 'PUT',
					ttl: now
				};

				return pendingEvent;
			});

			const res = layer.merge(layerItems, [...pendingEventsCursor0, ...pendingEventsCursor1]);

			expect(_.map(res, 'state')).toEqual([
				'layer-000',
				'layer-001',
				'layer-002',
				'layer-003',
				'layer-004',
				'pending-cursor-1-006',
				'pending-cursor-1-008'
			]);

			expect(_.map(res, 'sk')).toEqual([
				'sk-000',
				'sk-001',
				'sk-002',
				'sk-003',
				'sk-004',
				// 'sk-005',
				'sk-006',
				// 'sk-007',
				'sk-008'
				// 'sk-009'
			]);
		});
	});

	describe('meta', () => {
		beforeEach(async () => {
			vi.spyOn(layer.db, 'update');
			vi.spyOn(layer.db, 'get');
		});

		afterEach(async () => {
			await layer.db.clear();
		});

		it('should throw error if syncedTotal and unsyncedTotal are greater than 0', async () => {
			try {
				await layer.meta({
					advanceCursor: 0,
					syncedTotal: 10,
					unsyncedTotal: 10
				});

				throw new Error('expected to throw');
			} catch (err) {
				expect(err.message).toEqual('Cannot set both syncedTotal and unsyncedTotal at the same time');
			}
		});

		it('should get empty meta', async () => {
			const res = await layer.meta();

			expect(layer.db.get).toHaveBeenCalledWith({
				consistentRead: true,
				item: { pk: '__table-1__', sk: '__meta__' }
			});

			expect(res).toEqual({
				cursor: 0,
				cursorMax: 0,
				loaded: false,
				syncedLastTotal: 0,
				syncedTimes: 0,
				syncedTotal: 0,
				unsyncedLastTotal: 0,
				unsyncedTotal: 0
			});
		});

		it('should get current', async () => {
			// @ts-expect-error
			layer.currentMeta = {
				cursor: 0,
				cursorMax: 0,
				loaded: true,
				syncedLastTotal: 0,
				syncedTimes: 0,
				syncedTotal: 0,
				unsyncedLastTotal: 0,
				unsyncedTotal: 0
			};

			const res = await layer.meta();

			expect(layer.db.get).not.toHaveBeenCalled();
			expect(res).toEqual({
				cursor: 0,
				cursorMax: 0,
				loaded: true,
				syncedLastTotal: 0,
				syncedTimes: 0,
				syncedTotal: 0,
				unsyncedLastTotal: 0,
				unsyncedTotal: 0
			});
		});

		it('should upsert with advanceCursor only', async () => {
			const res = await layer.meta({
				advanceCursor: 1,
				syncedTotal: 0,
				unsyncedTotal: 0
			});

			expect(layer.db.update).toHaveBeenCalledWith({
				attributeNames: {
					'#cursor': 'cursor',
					'#cursorMax': 'cursorMax'
				},
				attributeValues: {
					':cursor': 1,
					':cursorMax': 1,
					':negative': -1,
					':positive': 1
				},
				conditionExpression: '(:cursor = :negative AND #cursor >= :positive) OR :cursor = :positive',
				filter: {
					item: { pk: '__table-1__', sk: '__meta__' }
				},
				updateExpression: 'ADD #cursor :cursor, #cursorMax :cursorMax',
				upsert: true
			});

			expect(res).toEqual({
				cursor: 1,
				cursorMax: 1,
				loaded: true,
				syncedLastTotal: 0,
				syncedTimes: 0,
				syncedTotal: 0,
				unsyncedLastTotal: 0,
				unsyncedTotal: 0
			});
		});

		it('should not upsert negative cursor', async () => {
			const res = await layer.meta({
				advanceCursor: -1,
				syncedTotal: 0,
				unsyncedTotal: 0
			});

			expect(res).toEqual({
				cursor: 0,
				cursorMax: 0,
				loaded: true,
				syncedLastTotal: 0,
				syncedTimes: 0,
				syncedTotal: 0,
				unsyncedLastTotal: 0,
				unsyncedTotal: 0
			});
		});

		it('should upsert with syncedTotal only', async () => {
			const res = await layer.meta({
				advanceCursor: 0,
				syncedTotal: 10,
				unsyncedTotal: 0
			});

			expect(layer.db.update).toHaveBeenCalledWith({
				attributeNames: {
					'#syncedLastTotal': 'syncedLastTotal',
					'#syncedTimes': 'syncedTimes',
					'#syncedTotal': 'syncedTotal',
					'#unsyncedLastTotal': 'unsyncedLastTotal',
					'#unsyncedTotal': 'unsyncedTotal'
				},
				attributeValues: {
					':syncedTimes': 1,
					':syncedTotal': 10,
					':unsyncedTotal': 0
				},
				conditionExpression: '',
				filter: {
					item: { pk: '__table-1__', sk: '__meta__' }
				},
				updateExpression: [
					'SET #syncedLastTotal = :syncedTotal,',
					'#unsyncedLastTotal = :unsyncedTotal,',
					'#unsyncedTotal = :unsyncedTotal',
					'ADD #syncedTimes :syncedTimes, #syncedTotal :syncedTotal'
				].join(' '),
				upsert: true
			});

			expect(res).toEqual({
				cursor: 0,
				cursorMax: 0,
				loaded: true,
				syncedLastTotal: 10,
				syncedTimes: 1,
				syncedTotal: 10,
				unsyncedLastTotal: 0,
				unsyncedTotal: 0
			});
		});

		it('should upsert with unsyncedTotal only', async () => {
			const res = await layer.meta({
				advanceCursor: 0,
				syncedTotal: 0,
				unsyncedTotal: 10
			});

			expect(layer.db.update).toHaveBeenCalledWith({
				attributeNames: {
					'#unsyncedLastTotal': 'unsyncedLastTotal',
					'#unsyncedTotal': 'unsyncedTotal'
				},
				attributeValues: {
					':unsyncedTotal': 10
				},
				conditionExpression: '',
				filter: {
					item: { pk: '__table-1__', sk: '__meta__' }
				},
				updateExpression: ['SET #unsyncedLastTotal = :unsyncedTotal', 'ADD #unsyncedTotal :unsyncedTotal'].join(' '),
				upsert: true
			});

			expect(res).toEqual({
				cursor: 0,
				cursorMax: 0,
				loaded: true,
				syncedLastTotal: 0,
				syncedTimes: 0,
				syncedTotal: 0,
				unsyncedLastTotal: 10,
				unsyncedTotal: 10
			});
		});

		it('should upsert with advanceCursor, syncedTotal', async () => {
			const res = await layer.meta({
				advanceCursor: 1,
				syncedTotal: 10,
				unsyncedTotal: 0
			});

			expect(layer.db.update).toHaveBeenCalledWith({
				attributeNames: {
					'#cursor': 'cursor',
					'#cursorMax': 'cursorMax',
					'#syncedLastTotal': 'syncedLastTotal',
					'#syncedTimes': 'syncedTimes',
					'#syncedTotal': 'syncedTotal',
					'#unsyncedLastTotal': 'unsyncedLastTotal',
					'#unsyncedTotal': 'unsyncedTotal'
				},
				attributeValues: {
					':cursor': 1,
					':cursorMax': 1,
					':syncedTimes': 1,
					':syncedTotal': 10,
					':negative': -1,
					':positive': 1,
					':unsyncedTotal': 0
				},
				conditionExpression: '(:cursor = :negative AND #cursor >= :positive) OR :cursor = :positive',
				filter: {
					item: { pk: '__table-1__', sk: '__meta__' }
				},
				updateExpression: [
					'SET #syncedLastTotal = :syncedTotal,',
					'#unsyncedLastTotal = :unsyncedTotal,',
					'#unsyncedTotal = :unsyncedTotal',
					'ADD #cursor :cursor,',
					'#cursorMax :cursorMax,',
					'#syncedTimes :syncedTimes, #syncedTotal :syncedTotal'
				].join(' '),
				upsert: true
			});

			expect(res).toEqual({
				cursor: 1,
				cursorMax: 1,
				loaded: true,
				syncedLastTotal: 10,
				syncedTimes: 1,
				syncedTotal: 10,
				unsyncedLastTotal: 0,
				unsyncedTotal: 0
			});
		});

		it('should upsert with advanceCursor, unsyncedTotal', async () => {
			const res = await layer.meta({
				advanceCursor: 1,
				syncedTotal: 0,
				unsyncedTotal: 10
			});

			expect(layer.db.update).toHaveBeenCalledWith({
				attributeNames: {
					'#cursor': 'cursor',
					'#cursorMax': 'cursorMax',
					'#unsyncedLastTotal': 'unsyncedLastTotal',
					'#unsyncedTotal': 'unsyncedTotal'
				},
				attributeValues: {
					':cursor': 1,
					':cursorMax': 1,
					':negative': -1,
					':positive': 1,
					':unsyncedTotal': 10
				},
				conditionExpression: '(:cursor = :negative AND #cursor >= :positive) OR :cursor = :positive',
				filter: {
					item: { pk: '__table-1__', sk: '__meta__' }
				},
				updateExpression: [
					'SET #unsyncedLastTotal = :unsyncedTotal',
					'ADD #cursor :cursor,',
					'#cursorMax :cursorMax,',
					'#unsyncedTotal :unsyncedTotal'
				].join(' '),
				upsert: true
			});

			expect(res).toEqual({
				cursor: 1,
				cursorMax: 1,
				loaded: true,
				syncedLastTotal: 0,
				syncedTimes: 0,
				syncedTotal: 0,
				unsyncedLastTotal: 10,
				unsyncedTotal: 10
			});
		});

		it('should update with advanceCursor only', async () => {
			const res1 = await layer.meta({
				advanceCursor: 1,
				syncedTotal: 0,
				unsyncedTotal: 0
			});
			const res2 = await layer.meta({
				advanceCursor: 1,
				syncedTotal: 0,
				unsyncedTotal: 0
			});
			const res3 = await layer.meta({
				advanceCursor: -1,
				syncedTotal: 0,
				unsyncedTotal: 0
			});

			expect(res1.cursor).toBeLessThan(res2.cursor);
			expect(res1.cursorMax).toBeLessThan(res2.cursorMax);
			expect(res1.syncedLastTotal).toEqual(res2.syncedLastTotal);
			expect(res1.syncedTimes).toEqual(res2.syncedTimes);
			expect(res1.syncedTotal).toEqual(res2.syncedTotal);
			expect(res1.unsyncedLastTotal).toEqual(res2.unsyncedLastTotal);
			expect(res1.unsyncedTotal).toEqual(res2.unsyncedTotal);

			expect(res2.cursor).toBeGreaterThan(res3.cursor);
			expect(res2.cursorMax).toEqual(res3.cursorMax);
			expect(res2.syncedLastTotal).toEqual(res3.syncedLastTotal);
			expect(res2.syncedTimes).toEqual(res3.syncedTimes);
			expect(res2.syncedTotal).toEqual(res3.syncedTotal);
			expect(res2.unsyncedLastTotal).toEqual(res3.unsyncedLastTotal);
			expect(res2.unsyncedTotal).toEqual(res3.unsyncedTotal);
		});

		it('should not update negative cursor', async () => {
			await layer.meta({
				advanceCursor: 0,
				syncedTotal: 0,
				unsyncedTotal: 0
			});
			await layer.meta({
				advanceCursor: -1,
				syncedTotal: 0,
				unsyncedTotal: 0
			});
			const res = await layer.meta({
				advanceCursor: -1,
				syncedTotal: 0,
				unsyncedTotal: 0
			});

			expect(res.cursor).toEqual(0);
		});

		it('should update with syncedTotal only', async () => {
			const res1 = await layer.meta({
				advanceCursor: 0,
				syncedTotal: 10,
				unsyncedTotal: 0
			});
			const res2 = await layer.meta({
				advanceCursor: 0,
				syncedTotal: 20,
				unsyncedTotal: 0
			});

			expect(res1.cursor).toEqual(res2.cursor);
			expect(res1.cursorMax).toEqual(res2.cursorMax);
			expect(res1.syncedLastTotal).toBeLessThan(res2.syncedLastTotal);
			expect(res1.syncedTimes).toBeLessThan(res2.syncedTimes);
			expect(res1.syncedTotal).toBeLessThan(res2.syncedTotal);
			expect(res1.unsyncedLastTotal).toEqual(res2.unsyncedLastTotal);
			expect(res1.unsyncedTotal).toEqual(res2.unsyncedTotal);
		});

		it('should update with unsyncedTotal only', async () => {
			const res1 = await layer.meta({
				advanceCursor: 0,
				syncedTotal: 0,
				unsyncedTotal: 10
			});
			const res2 = await layer.meta({
				advanceCursor: 0,
				syncedTotal: 0,
				unsyncedTotal: 20
			});

			expect(res1.cursor).toEqual(res2.cursor);
			expect(res1.cursorMax).toEqual(res2.cursorMax);
			expect(res1.syncedLastTotal).toEqual(res2.syncedLastTotal);
			expect(res1.syncedTimes).toEqual(res2.syncedTimes);
			expect(res1.syncedTotal).toEqual(res2.syncedTotal);
			expect(res1.unsyncedLastTotal).toBeLessThan(res2.unsyncedLastTotal);
			expect(res1.unsyncedTotal).toBeLessThan(res2.unsyncedTotal);
		});

		it('should update with advanceCursor and syncedTotal', async () => {
			const res1 = await layer.meta({
				advanceCursor: 1,
				syncedTotal: 10,
				unsyncedTotal: 0
			});
			const res2 = await layer.meta({
				advanceCursor: 1,
				syncedTotal: 20,
				unsyncedTotal: 0
			});
			const res3 = await layer.meta({
				advanceCursor: -1,
				syncedTotal: 0,
				unsyncedTotal: 0
			});

			expect(res1.cursor).toBeLessThan(res2.cursor);
			expect(res1.cursorMax).toBeLessThan(res2.cursorMax);
			expect(res1.syncedLastTotal).toBeLessThan(res2.syncedLastTotal);
			expect(res1.syncedTimes).toBeLessThan(res2.syncedTimes);
			expect(res1.syncedTotal).toBeLessThan(res2.syncedTotal);
			expect(res1.unsyncedLastTotal).toEqual(res2.unsyncedLastTotal);
			expect(res1.unsyncedTotal).toEqual(res2.unsyncedTotal);

			expect(res2.cursor).toBeGreaterThan(res3.cursor);
			expect(res2.cursorMax).toEqual(res3.cursorMax);
			expect(res2.syncedLastTotal).toEqual(res3.syncedLastTotal);
			expect(res2.syncedTimes).toEqual(res3.syncedTimes);
			expect(res2.syncedTotal).toEqual(res3.syncedTotal);
			expect(res2.unsyncedLastTotal).toEqual(res3.unsyncedLastTotal);
			expect(res2.unsyncedTotal).toEqual(res3.unsyncedTotal);
		});

		it('should update with advanceCursor and unsyncedTotal', async () => {
			const res1 = await layer.meta({
				advanceCursor: 1,
				syncedTotal: 0,
				unsyncedTotal: 10
			});
			const res2 = await layer.meta({
				advanceCursor: 1,
				syncedTotal: 0,
				unsyncedTotal: 20
			});
			const res3 = await layer.meta({
				advanceCursor: -1,
				syncedTotal: 0,
				unsyncedTotal: 0
			});

			expect(res1.cursor).toBeLessThan(res2.cursor);
			expect(res1.cursorMax).toBeLessThan(res2.cursorMax);
			expect(res1.syncedLastTotal).toEqual(res2.syncedLastTotal);
			expect(res1.syncedTimes).toEqual(res2.syncedTimes);
			expect(res1.syncedTotal).toEqual(res2.syncedTotal);
			expect(res1.unsyncedLastTotal).toBeLessThan(res2.unsyncedLastTotal);
			expect(res1.unsyncedTotal).toBeLessThan(res2.unsyncedTotal);

			expect(res2.cursor).toBeGreaterThan(res3.cursor);
			expect(res2.cursorMax).toEqual(res3.cursorMax);
			expect(res2.syncedLastTotal).toEqual(res3.syncedLastTotal);
			expect(res2.syncedTimes).toEqual(res3.syncedTimes);
			expect(res2.syncedTotal).toEqual(res3.syncedTotal);
			expect(res2.unsyncedLastTotal).toEqual(res3.unsyncedLastTotal);
			expect(res2.unsyncedTotal).toEqual(res3.unsyncedTotal);
		});
	});

	describe('reset', () => {
		let db: Dynamodb<Item>;

		beforeAll(async () => {
			db = new Dynamodb<Item>({
				accessKeyId: process.env.AWS_ACCESS_KEY || '',
				region: 'us-east-1',
				schema: { partition: 'pk', sort: 'sk' },
				secretAccessKey: process.env.AWS_SECRET_KEY || '',
				table: 'use-dynamodb-spec'
			});

			await db.createTable();
			await db.batchWrite(
				_.times(10, i => {
					return {
						pk: `pk-${i % 2}`,
						sk: `sk-${i}`,
						state: `db-${i}`
					};
				})
			);
		});

		afterAll(async () => {
			await Promise.all([db.clear(), layer.db.clear()]);
		});

		beforeEach(async () => {
			vi.spyOn(db, 'scan');
			vi.spyOn(layer, 'acquireLock');
			vi.spyOn(layer, 'resetMeta');
			vi.spyOn(layer, 'setter');
			vi.spyOn(layer.db, 'batchDelete');
			vi.spyOn(layer.db, 'scan');

			await Promise.all([
				// pk 0
				layer.set(
					createChangeEvents({
						count: 2
					}),
					0
				),
				// pk 1
				layer.set(
					createChangeEvents({
						count: 2,
						pk: 1
					}),
					1
				)
			]);
		});

		it('should not reset if locked', async () => {
			vi.mocked(layer.acquireLock).mockResolvedValue(false);

			const res = await layer.reset(db);

			expect(layer.acquireLock).toHaveBeenCalledOnce();
			expect(layer.acquireLock).toHaveBeenCalledWith(true);

			expect(res).toEqual({
				count: 0,
				locked: true
			});
		});

		it('must reset', async () => {
			await layer.reset(db, 'pk-0');

			expect(layer.acquireLock).toHaveBeenCalledWith(true);
			expect(layer.acquireLock).toHaveBeenCalledWith(false);

			expect(layer.db.scan).toHaveBeenCalledWith({
				attributeNames: { '#pk': 'pk' },
				attributeValues: { ':pk': 'table-1#pk-0' },
				filterExpression: 'begins_with(#pk, :pk)',
				limit: Infinity,
				onChunk: expect.any(Function)
			});

			expect(layer.db.batchDelete).toHaveBeenCalledOnce();
			expect(layer.db.batchDelete).toHaveBeenCalledWith(
				expect.arrayContaining([
					expect.objectContaining({
						pk: 'table-1#pk-0#0',
						sk: 'sk-000'
					}),
					expect.objectContaining({
						pk: 'table-1#pk-0#0',
						sk: 'sk-001'
					})
				])
			);

			expect(layer.resetMeta).toHaveBeenCalled();
			expect(db.scan).toHaveBeenCalledWith({
				limit: Infinity,
				onChunk: expect.any(Function)
			});

			const setterArgs = vi
				.mocked(layer.setter)
				.mock.calls.map(args => {
					return {
						partition: args[0],
						items: args[1]
					};
				})
				.sort((a, b) => {
					return a.partition.localeCompare(b.partition);
				});

			expect(layer.setter).toHaveBeenCalledOnce();
			expect(layer.setter).toHaveBeenCalledWith('table-1#pk-0', expect.any(Array));
			expect(_.map(setterArgs[0].items, 'state')).toEqual(['db-0', 'db-2', 'db-4', 'db-6', 'db-8']);
			expect(await layer.meta()).toEqual(DEFAULT_CURRENT_META);
		});

		it('must reset without partition argument', async () => {
			await layer.reset(db);

			expect(layer.acquireLock).toHaveBeenCalledWith(true);
			expect(layer.acquireLock).toHaveBeenCalledWith(false);

			expect(layer.db.scan).toHaveBeenCalledWith({
				attributeNames: { '#pk': 'pk' },
				attributeValues: { ':pk': 'table-1' },
				filterExpression: 'begins_with(#pk, :pk)',
				limit: Infinity,
				onChunk: expect.any(Function)
			});

			expect(layer.db.batchDelete).toHaveBeenCalledOnce();
			expect(layer.db.batchDelete).toHaveBeenCalledWith(
				expect.arrayContaining([
					expect.objectContaining({
						pk: 'table-1#pk-0#0',
						sk: 'sk-000'
					}),
					expect.objectContaining({
						pk: 'table-1#pk-0#0',
						sk: 'sk-001'
					}),
					expect.objectContaining({
						pk: 'table-1#pk-1#1',
						sk: 'sk-000'
					}),
					expect.objectContaining({
						pk: 'table-1#pk-1#1',
						sk: 'sk-001'
					})
				])
			);

			expect(layer.resetMeta).toHaveBeenCalled();
			expect(db.scan).toHaveBeenCalledWith({
				limit: Infinity,
				onChunk: expect.any(Function)
			});

			const setterArgs = vi
				.mocked(layer.setter)
				.mock.calls.map(args => {
					return {
						partition: args[0],
						items: args[1]
					};
				})
				.sort((a, b) => {
					return a.partition.localeCompare(b.partition);
				});

			expect(layer.setter).toHaveBeenCalledTimes(2);
			expect(layer.setter).toHaveBeenCalledWith('table-1#pk-0', expect.any(Array));
			expect(layer.setter).toHaveBeenCalledWith('table-1#pk-1', expect.any(Array));
			expect(_.map(setterArgs[0].items, 'state')).toEqual(['db-0', 'db-2', 'db-4', 'db-6', 'db-8']);
			expect(_.map(setterArgs[1].items, 'state')).toEqual(['db-1', 'db-3', 'db-5', 'db-7', 'db-9']);
			expect(await layer.meta()).toEqual(DEFAULT_CURRENT_META);
		});
	});

	describe('resetMeta', () => {
		beforeAll(async () => {
			await layer.meta({
				advanceCursor: 1,
				syncedTotal: 10,
				unsyncedTotal: 0
			});
		});

		afterAll(async () => {
			await layer.db.clear();
		});

		it('should reset meta', async () => {
			await layer.resetMeta();

			expect(await layer.meta()).toEqual(DEFAULT_CURRENT_META);
		});
	});

	describe('resolvePartition', () => {
		it('should resolve partition with table and partition', () => {
			const partition = 'pk-0';
			const resolvedPartition = layer.resolvePartition(0, partition);

			expect(resolvedPartition).toEqual('table-1#pk-0#0');
		});

		it('should resolve partition with only table', () => {
			const resolvedPartition = layer.resolvePartition(0);

			expect(resolvedPartition).toEqual('table-1#0');
		});

		it('should resolve partition with empty partition', () => {
			const resolvedPartition = layer.resolvePartition(0, '');

			expect(resolvedPartition).toEqual('table-1#0');
		});
	});

	describe('set', () => {
		beforeEach(() => {
			vi.spyOn(layer.db, 'batchWrite');
			vi.spyOn(layer, 'sync').mockResolvedValue({
				count: 0,
				locked: false
			});
		});

		afterAll(async () => {
			await layer.db.clear();
		});

		it('should throw error if item has no unique identifier', async () => {
			try {
				const events = createChangeEvents({
					count: 1,
					pk: 1,
					state: 'pending'
				});

				events[0].item.sk = '';
				await layer.set(events);

				throw new Error('expected to throw');
			} catch (err) {
				expect(err.message).toEqual('Item must have an unique identifier');
			}
		});

		it('should set', async () => {
			const events = createChangeEvents({
				count: 3
			});

			await layer.set(events);

			expect(layer.syncStrategy).toHaveBeenCalledWith({
				cursor: 0,
				cursorMax: 0,
				loaded: true,
				syncedLastTotal: 0,
				syncedTimes: 0,
				syncedTotal: 0,
				unsyncedLastTotal: 3,
				unsyncedTotal: 3
			});
			expect(layer.sync).not.toHaveBeenCalled();

			expect(layer.db.batchWrite).toHaveBeenCalledWith(
				expect.arrayContaining([
					expect.objectContaining({
						cursor: 0,
						item: expect.objectContaining({
							pk: 'pk-0',
							sk: 'sk-000',
							state: 'pending-000'
						}),
						pk: 'table-1#pk-0#0',
						sk: 'sk-000',
						type: 'PUT',
						ttl: expect.any(Number)
					}),
					expect.objectContaining({
						cursor: 0,
						item: expect.objectContaining({
							pk: 'pk-0',
							sk: 'sk-001',
							state: 'pending-001'
						}),
						pk: 'table-1#pk-0#0',
						sk: 'sk-001',
						type: 'PUT',
						ttl: expect.any(Number)
					}),
					expect.objectContaining({
						cursor: 0,
						item: expect.objectContaining({
							pk: 'pk-0',
							sk: 'sk-002',
							state: 'pending-002'
						}),
						pk: 'table-1#pk-0#0',
						sk: 'sk-002',
						type: 'PUT',
						ttl: expect.any(Number)
					})
				])
			);
		});

		it('should set calling sync in background', async () => {
			vi.mocked(layer.syncStrategy!).mockReturnValue(true);

			const events = createChangeEvents({
				count: 3
			});

			await layer.set(events);

			expect(layer.syncStrategy).toHaveBeenCalledWith({
				cursor: 0,
				cursorMax: 0,
				loaded: true,
				syncedLastTotal: 0,
				syncedTimes: 0,
				syncedTotal: 0,
				unsyncedLastTotal: 3,
				unsyncedTotal: 6
			});
			expect(layer.sync).toHaveBeenCalled();
			expect(layer.backgroundRunner).toHaveBeenCalledWith(expect.any(Promise));
		});

		it('should set calling sync', async () => {
			vi.mocked(layer.syncStrategy!).mockReturnValue(true);

			const events = createChangeEvents({
				count: 3
			});

			layer.backgroundRunner = undefined;
			await layer.set(events);

			expect(layer.syncStrategy).toHaveBeenCalledWith({
				cursor: 0,
				cursorMax: 0,
				loaded: true,
				syncedLastTotal: 0,
				syncedTimes: 0,
				syncedTotal: 0,
				unsyncedLastTotal: 3,
				unsyncedTotal: 9
			});
			expect(layer.sync).toHaveBeenCalled();
		});
	});

	describe('sync', () => {
		beforeAll(async () => {
			await Promise.all([
				// pk 0
				layer.set(
					createChangeEvents({
						count: 4,
						type: 'PUT'
					}),
					0
				),
				layer.set(
					createChangeEvents({
						count: 2,
						type: 'DELETE'
					}),
					1
				),
				layer.set(
					createChangeEvents({
						count: 4,
						type: 'UPDATE'
					}),
					2
				),
				// pk 1 (empty layer)
				layer.set(
					createChangeEvents({
						count: 4,
						pk: 1,
						type: 'PUT'
					}),
					0
				),
				layer.set(
					createChangeEvents({
						count: 2,
						pk: 1,
						type: 'DELETE'
					}),
					1
				)
			]);
		});

		afterAll(async () => {
			await layer.db.clear();
		});

		beforeEach(async () => {
			vi.spyOn(layer, 'acquireLock');
			vi.spyOn(layer, 'meta');
			vi.spyOn(layer.db, 'query');
			vi.spyOn(layer, 'setter');

			vi.mocked(layer.getter).mockImplementation(async (partition: string) => {
				if (_.startsWith(partition, 'table-1#pk-0')) {
					return _.times(8, index => {
						return createItem({
							index,
							state: 'layer'
						});
					});
				}

				return [];
			});
		});

		it('should not sync if locked', async () => {
			vi.mocked(layer.acquireLock).mockResolvedValue(false);

			const res = await layer.sync();

			expect(layer.acquireLock).toHaveBeenCalledOnce();
			expect(layer.acquireLock).toHaveBeenCalledWith(true);

			expect(layer.setter).not.toHaveBeenCalled();

			expect(res).toEqual({
				count: 0,
				locked: true
			});
		});

		it('should sync on first cursor', async () => {
			const res = await layer.sync();

			expect(layer.acquireLock).toHaveBeenCalledWith(true);
			expect(layer.acquireLock).toHaveBeenCalledWith(false);

			expect(layer.meta).toHaveBeenCalledTimes(3);
			expect(layer.meta).toHaveBeenCalledWith();
			expect(layer.meta).toHaveBeenCalledWith({ advanceCursor: 1, syncedTotal: 0, unsyncedTotal: 0 });
			expect(layer.meta).toHaveBeenCalledWith({ advanceCursor: 0, syncedTotal: 8, unsyncedTotal: 0 });

			expect(layer.getter).toHaveBeenCalledTimes(2);
			expect(layer.getter).toHaveBeenCalledWith('table-1#pk-0');
			expect(layer.getter).toHaveBeenCalledWith('table-1#pk-1');

			expect(layer.db.query).toHaveBeenCalledWith({
				item: { cursor: 0, pk: 'table-1' },
				limit: Infinity,
				onChunk: expect.any(Function),
				prefix: true
			});

			const setterArgs = vi
				.mocked(layer.setter)
				.mock.calls.map(args => {
					return {
						partition: args[0],
						items: args[1]
					};
				})
				.sort((a, b) => {
					return a.partition.localeCompare(b.partition);
				});

			expect(layer.setter).toHaveBeenCalledTimes(2);
			expect(layer.setter).toHaveBeenCalledWith('table-1#pk-0', expect.any(Array));
			expect(layer.setter).toHaveBeenCalledWith('table-1#pk-1', expect.any(Array));

			expect(_.map(setterArgs[0].items, 'state')).toEqual(
				expect.arrayContaining([
					'pending-000',
					'pending-001',
					'pending-002',
					'pending-003',
					'layer-004',
					'layer-005',
					'layer-006',
					'layer-007'
				])
			);

			expect(_.map(setterArgs[1].items, 'state')).toEqual(
				expect.arrayContaining(['pending-000', 'pending-001', 'pending-002', 'pending-003'])
			);

			expect(res).toEqual({
				count: 8,
				locked: false
			});
		});

		it('should sync on 2nd cursor', async () => {
			vi.mocked(layer.meta).mockResolvedValue({ cursor: 1 } as Layer.Meta);

			const res = await layer.sync();

			expect(layer.acquireLock).toHaveBeenCalledWith(true);
			expect(layer.acquireLock).toHaveBeenCalledWith(false);

			expect(layer.meta).toHaveBeenCalledTimes(3);
			expect(layer.meta).toHaveBeenCalledWith();
			expect(layer.meta).toHaveBeenCalledWith({ advanceCursor: 1, syncedTotal: 0, unsyncedTotal: 0 });
			expect(layer.meta).toHaveBeenCalledWith({ advanceCursor: 0, syncedTotal: 4, unsyncedTotal: 0 });

			expect(layer.db.query).toHaveBeenCalledWith({
				item: { cursor: 1, pk: 'table-1' },
				limit: Infinity,
				onChunk: expect.any(Function),
				prefix: true
			});

			expect(layer.getter).toHaveBeenCalledTimes(2);
			expect(layer.getter).toHaveBeenCalledWith('table-1#pk-0');
			expect(layer.getter).toHaveBeenCalledWith('table-1#pk-1');

			const setterArgs = vi
				.mocked(layer.setter)
				.mock.calls.map(args => {
					return {
						partition: args[0],
						items: args[1]
					};
				})
				.sort((a, b) => {
					return a.partition.localeCompare(b.partition);
				});

			expect(layer.setter).toHaveBeenCalledTimes(2);
			expect(layer.setter).toHaveBeenCalledWith('table-1#pk-0', expect.any(Array));
			expect(layer.setter).toHaveBeenCalledWith('table-1#pk-1', expect.any(Array));

			expect(_.map(setterArgs[0].items, 'state')).toEqual(expect.arrayContaining(['layer-004', 'layer-005', 'layer-006', 'layer-007']));
			expect(_.map(setterArgs[1].items, 'state')).toEqual(expect.arrayContaining([]));

			expect(res).toEqual({
				count: 4,
				locked: false
			});
		});

		it('should sync on 3rd cursor', async () => {
			vi.mocked(layer.meta).mockResolvedValue({ cursor: 2 } as Layer.Meta);

			const res = await layer.sync();

			expect(layer.acquireLock).toHaveBeenCalledWith(true);
			expect(layer.acquireLock).toHaveBeenCalledWith(false);

			expect(layer.meta).toHaveBeenCalledTimes(3);
			expect(layer.meta).toHaveBeenCalledWith();
			expect(layer.meta).toHaveBeenCalledWith({ advanceCursor: 1, syncedTotal: 0, unsyncedTotal: 0 });
			expect(layer.meta).toHaveBeenCalledWith({ advanceCursor: 0, syncedTotal: 4, unsyncedTotal: 0 });

			expect(layer.db.query).toHaveBeenCalledWith({
				item: { cursor: 2, pk: 'table-1' },
				limit: Infinity,
				onChunk: expect.any(Function),
				prefix: true
			});

			expect(layer.getter).toHaveBeenCalledOnce();
			expect(layer.getter).toHaveBeenCalledWith('table-1#pk-0');

			const setterArgs = vi
				.mocked(layer.setter)
				.mock.calls.map(args => {
					return {
						partition: args[0],
						items: args[1]
					};
				})
				.sort((a, b) => {
					return a.partition.localeCompare(b.partition);
				});

			expect(layer.setter).toHaveBeenCalledOnce();
			expect(layer.setter).toHaveBeenCalledWith('table-1#pk-0', expect.any(Array));

			expect(_.map(setterArgs[0].items, 'state')).toEqual(
				expect.arrayContaining([
					'pending-000',
					'pending-001',
					'pending-002',
					'pending-003',
					'layer-004',
					'layer-005',
					'layer-006',
					'layer-007'
				])
			);

			expect(res).toEqual({
				count: 4,
				locked: false
			});
		});

		it('should regress cursor on setter error', async () => {
			vi.mocked(layer.setter).mockRejectedValue(new Error('setter error'));

			try {
				await layer.sync();

				throw new Error('expected to throw');
			} catch (err) {
				expect(err.message).toEqual('setter error');

				expect(layer.acquireLock).toHaveBeenCalledWith(true);
				expect(layer.acquireLock).toHaveBeenCalledWith(false);

				expect(layer.meta).toHaveBeenCalledTimes(3);
				expect(layer.meta).toHaveBeenCalledWith();
				expect(layer.meta).toHaveBeenCalledWith({ advanceCursor: 1, syncedTotal: 0, unsyncedTotal: 0 });
				expect(layer.meta).toHaveBeenCalledWith({ advanceCursor: -1, syncedTotal: 0, unsyncedTotal: 0 });
			}
		});
	});
});

describe('/layer.ts (without partition)', () => {
	let layer: Layer<Item>;

	beforeAll(async () => {
		layer = await factory({
			backgroundRunner: vi.fn(),
			createTable: true,
			getter: vi.fn(async () => []),
			setter: vi.fn(),
			syncStrategy: vi.fn()
		});

		// @ts-ignore
		layer.getItemPartition = () => {
			return '';
		};
	});

	afterAll(async () => {
		await layer.db.clear();
	});

	beforeEach(async () => {
		layer = await factory({
			backgroundRunner: vi.fn(),
			createTable: false,
			getter: vi.fn(async () => []),
			setter: vi.fn(),
			syncStrategy: vi.fn()
		});

		// @ts-ignore
		layer.getItemPartition = () => {
			return '';
		};
	});

	describe('get', () => {
		beforeAll(async () => {
			await Promise.all([
				layer.set(
					createChangeEvents({
						count: 2
					}),
					0
				),
				layer.set(
					createChangeEvents({
						count: 2,
						initialIndex: 2,
						type: 'DELETE'
					}),
					0
				)
			]);
		});

		afterAll(async () => {
			await layer.db.clear();
		});

		beforeEach(async () => {
			vi.mocked(layer.getter).mockImplementation(async (partition: string) => {
				if (_.startsWith(partition, 'table-1')) {
					return _.times(6, index => {
						return createItem({
							index,
							state: 'layer'
						});
					});
				}

				return [];
			});

			vi.spyOn(layer, 'meta');
			vi.spyOn(layer, 'merge');
			vi.spyOn(layer.db, 'query');
		});

		it('should returns', async () => {
			const res = await layer.get();

			expect(layer.meta).toHaveBeenCalledWith();
			expect(layer.getter).toHaveBeenCalledWith('table-1');
			expect(layer.merge).toHaveBeenCalledWith(expect.any(Array), expect.any(Array));
			expect(layer.db.query).toHaveBeenCalledWith({
				item: {
					pk: 'table-1#0'
				},
				limit: Infinity
			});

			expect(res).toEqual([
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-000',
					state: 'pending-000'
				}),
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-001',
					state: 'pending-001'
				}),
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-004',
					state: 'layer-004'
				}),
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-005',
					state: 'layer-005'
				})
			]);
		});
	});

	describe('set', () => {
		beforeEach(() => {
			vi.spyOn(layer.db, 'batchWrite');
		});

		afterAll(async () => {
			await layer.db.clear();
		});

		it('should set items', async () => {
			const events = createChangeEvents({
				count: 3
			});

			await layer.set(events);

			expect(layer.db.batchWrite).toHaveBeenCalledWith(
				expect.arrayContaining([
					expect.objectContaining({
						cursor: 0,
						item: expect.objectContaining({
							pk: 'pk-0',
							sk: 'sk-000',
							state: 'pending-000'
						}),
						pk: 'table-1#0',
						sk: 'sk-000',
						type: 'PUT',
						ttl: expect.any(Number)
					}),
					expect.objectContaining({
						cursor: 0,
						item: expect.objectContaining({
							pk: 'pk-0',
							sk: 'sk-001',
							state: 'pending-001'
						}),
						pk: 'table-1#0',
						sk: 'sk-001',
						type: 'PUT',
						ttl: expect.any(Number)
					}),
					expect.objectContaining({
						cursor: 0,
						item: expect.objectContaining({
							pk: 'pk-0',
							sk: 'sk-002',
							state: 'pending-002'
						}),
						pk: 'table-1#0',
						sk: 'sk-002',
						type: 'PUT',
						ttl: expect.any(Number)
					})
				])
			);
		});
	});

	describe('reset', () => {
		let db: Dynamodb<Item>;

		beforeAll(async () => {
			db = new Dynamodb<Item>({
				accessKeyId: process.env.AWS_ACCESS_KEY || '',
				region: 'us-east-1',
				schema: { partition: 'pk', sort: 'sk' },
				secretAccessKey: process.env.AWS_SECRET_KEY || '',
				table: 'use-dynamodb-spec'
			});

			await db.createTable();
			await db.batchWrite(
				_.times(5, i => {
					return {
						pk: `pk-${i % 2}`,
						sk: `sk-${i}`,
						state: `db-${i}`
					};
				})
			);
		});

		afterAll(async () => {
			await Promise.all([db.clear(), layer.db.clear()]);
		});

		beforeEach(async () => {
			vi.spyOn(db, 'scan');
			vi.spyOn(layer, 'acquireLock');
			vi.spyOn(layer, 'resetMeta');
			vi.spyOn(layer, 'setter');
			vi.spyOn(layer.db, 'batchDelete');
			vi.spyOn(layer.db, 'scan');

			await Promise.all([
				// pk 0
				layer.set(
					createChangeEvents({
						count: 2
					}),
					0
				)
			]);
		});

		it('must reset', async () => {
			await layer.reset(db);

			expect(layer.acquireLock).toHaveBeenCalledWith(true);
			expect(layer.acquireLock).toHaveBeenCalledWith(false);

			expect(layer.db.scan).toHaveBeenCalledWith({
				attributeNames: { '#pk': 'pk' },
				attributeValues: { ':pk': 'table-1' },
				filterExpression: 'begins_with(#pk, :pk)',
				limit: Infinity,
				onChunk: expect.any(Function)
			});

			expect(layer.db.batchDelete).toHaveBeenCalledOnce();
			expect(layer.db.batchDelete).toHaveBeenCalledWith(
				expect.arrayContaining([
					expect.objectContaining({
						pk: 'table-1#0',
						sk: 'sk-000'
					}),
					expect.objectContaining({
						pk: 'table-1#0',
						sk: 'sk-001'
					})
				])
			);

			expect(layer.resetMeta).toHaveBeenCalled();
			expect(db.scan).toHaveBeenCalledWith({
				limit: Infinity,
				onChunk: expect.any(Function)
			});

			const setterArgs = vi
				.mocked(layer.setter)
				.mock.calls.map(args => {
					return {
						partition: args[0],
						items: args[1]
					};
				})
				.sort((a, b) => {
					return a.partition.localeCompare(b.partition);
				});

			expect(layer.setter).toHaveBeenCalledOnce();
			expect(layer.setter).toHaveBeenCalledWith('table-1', expect.any(Array));
			expect(_.map(setterArgs[0].items, 'state')).toEqual(expect.arrayContaining(['db-0', 'db-1', 'db-2', 'db-3', 'db-4']));
			expect(await layer.meta()).toEqual(DEFAULT_CURRENT_META);
		});
	});

	describe('sync', () => {
		beforeAll(async () => {
			await Promise.all([
				layer.set(
					createChangeEvents({
						count: 2
					}),
					0
				),
				layer.set(
					createChangeEvents({
						count: 2,
						initialIndex: 2,
						type: 'DELETE'
					}),
					0
				)
			]);
		});

		afterAll(async () => {
			await layer.db.clear();
		});

		beforeEach(async () => {
			vi.spyOn(layer.db, 'query');
			vi.spyOn(layer, 'setter');

			vi.mocked(layer.getter).mockImplementation(async (partition: string) => {
				if (_.startsWith(partition, 'table-1')) {
					return _.times(6, index => {
						return createItem({
							index,
							state: 'layer'
						});
					});
				}

				return [];
			});
		});

		it('should sync', async () => {
			const res = await layer.sync();

			expect(layer.db.query).toHaveBeenCalledWith({
				item: { cursor: 0, pk: 'table-1' },
				limit: Infinity,
				onChunk: expect.any(Function),
				prefix: true
			});

			expect(layer.getter).toHaveBeenCalledWith('table-1');

			const setterArgs = vi
				.mocked(layer.setter)
				.mock.calls.map(args => {
					return {
						partition: args[0],
						items: args[1]
					};
				})
				.sort((a, b) => {
					return a.partition.localeCompare(b.partition);
				});

			expect(layer.setter).toHaveBeenCalledWith('table-1', expect.any(Array));
			expect(_.map(setterArgs[0].items, 'state')).toEqual(expect.arrayContaining(['pending-000', 'pending-001', 'layer-004', 'layer-005']));

			expect(res).toEqual({
				count: 4,
				locked: false
			});
		});
	});
});
