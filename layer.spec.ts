import _ from 'lodash';
import { afterAll, beforeAll, beforeEach, describe, expect, it, Mock, vi } from 'vitest';

import Db, { ChangeEvent, ChangeType, PersistedItem } from './index';
import Layer, { PendingEvent } from './layer';

type Item = {
	id: string;
	namespace: string;
	state: string;
};

const createItem = (options: { index: number; namespace?: number; state?: 'layer' | 'pending'; ts?: number }): PersistedItem<Item> => {
	const { index, namespace = 0, state = 'pending', ts = _.now() } = options;
	const nowISO = new Date(ts).toISOString();
	const indexString = _.padStart(`${index}`, 3, '0');

	return {
		__createdAt: nowISO,
		__ts: ts,
		__updatedAt: nowISO,
		id: `id-${indexString}`,
		namespace: `namespace-${namespace}`,
		state: `${state}-${indexString}`
	};
};

const createChangeEvents = (options: {
	count: number;
	initialIndex?: number;
	namespace?: number;
	state?: 'layer' | 'pending';
	table?: string;
	ts?: number;
	type?: ChangeType;
}): ChangeEvent<Item>[] => {
	return _.times(options.count, index => {
		const { initialIndex = 0, namespace = 0, state = 'pending', table = 'table-1', ts = _.now(), type = 'PUT' } = options || {};

		index += initialIndex;

		const item = createItem({
			index,
			namespace,
			state,
			ts
		});

		const event: ChangeEvent<Item> = {
			item,
			partition: item.namespace,
			sort: item.id,
			table,
			type
		};

		return event;
	});
};

const factory = async ({ createTable = false, getter, setter }: { createTable?: boolean; getter: Mock; setter: Mock }) => {
	const db = new Db<PendingEvent<Item>>({
		accessKeyId: process.env.AWS_ACCESS_KEY || '',
		indexes: [
			{
				name: 'cursor-index',
				partition: 'cursor',
				partitionType: 'S',
				sort: 'pk',
				sortType: 'S'
			}
		],
		region: 'us-east-1',
		secretAccessKey: process.env.AWS_SECRET_KEY || '',
		schema: { partition: 'pk', sort: 'sk' },
		table: 'use-dynamodb-layer-spec'
	});

	if (createTable) {
		await db.createTable();
	}

	return new Layer({
		db,
		getItemPartition: item => {
			return item.namespace || ('pk' in item ? (item.pk as string) : '');
		},
		getItemUniqueIdentifier: item => {
			return item.id;
		},
		getter,
		setter,
		table: 'table-1'
	});
};

describe('/layer.ts', () => {
	let layer: Layer<Item>;

	beforeAll(async () => {
		layer = await factory({
			createTable: true,
			getter: vi.fn(async () => []),
			setter: vi.fn()
		});
	});

	afterAll(async () => {
		await layer.db.clear();
	});

	beforeEach(async () => {
		layer = await factory({
			createTable: false,
			getter: vi.fn(async () => []),
			setter: vi.fn()
		});
	});

	describe('get', () => {
		beforeAll(async () => {
			await Promise.all([
				layer.set(
					createChangeEvents({
						count: 8
					}),
					'__INITIAL__'
				),
				layer.set(
					createChangeEvents({
						count: 2,
						initialIndex: 2,
						type: 'DELETE'
					}),
					'__INITIAL__'
				),
				layer.set(
					createChangeEvents({
						count: 2,
						initialIndex: 8
					}),
					'__INITIAL__'
				),
				layer.set(
					createChangeEvents({
						count: 2,
						initialIndex: 2,
						type: 'DELETE'
					}),
					'2024-11-28T01:00:00.000Z'
				),
				layer.set(
					createChangeEvents({
						count: 2,
						initialIndex: 8
					}),
					'2024-11-28T01:00:00.000Z'
				)
			]);
		});

		afterAll(async () => {
			await layer.db.clear();
		});

		beforeEach(async () => {
			vi.mocked(layer.getter).mockImplementation(async (partition: string) => {
				if (_.startsWith(partition, 'table-1#namespace-0')) {
					return _.times(12, index => {
						return createItem({
							index,
							state: 'layer'
						});
					});
				}

				return [];
			});

			vi.spyOn(layer, 'cursor');
			vi.spyOn(layer, 'mergePendingEvents');
			vi.spyOn(layer.db, 'query');
		});

		it('should returns', async () => {
			const res = await layer.get('namespace-0');

			expect(layer.cursor).toHaveBeenCalledWith();
			expect(layer.mergePendingEvents).toHaveBeenCalledWith('table-1#namespace-0#__INITIAL__', expect.any(Array));
			expect(layer.db.query).toHaveBeenCalledWith({
				item: {
					pk: 'table-1#namespace-0#__INITIAL__'
				},
				limit: Infinity
			});

			expect(res).toEqual([
				expect.objectContaining({
					id: 'id-000',
					namespace: 'namespace-0',
					state: 'pending-000'
				}),
				expect.objectContaining({
					id: 'id-001',
					namespace: 'namespace-0',
					state: 'pending-001'
				}),
				// ** DELETED **
				// expect.objectContaining({
				// 	id: 'id-002',
				// 	namespace: 'namespace-0',
				// 	state: 'pending-002'
				// }),
				// expect.objectContaining({
				// 	id: 'id-003',
				// 	namespace: 'namespace-0',
				// 	state: 'pending-003'
				// }),
				expect.objectContaining({
					id: 'id-004',
					namespace: 'namespace-0',
					state: 'pending-004'
				}),
				expect.objectContaining({
					id: 'id-005',
					namespace: 'namespace-0',
					state: 'pending-005'
				}),
				expect.objectContaining({
					id: 'id-006',
					namespace: 'namespace-0',
					state: 'pending-006'
				}),
				expect.objectContaining({
					id: 'id-007',
					namespace: 'namespace-0',
					state: 'pending-007'
				}),
				expect.objectContaining({
					id: 'id-008',
					namespace: 'namespace-0',
					state: 'pending-008'
				}),
				expect.objectContaining({
					id: 'id-009',
					namespace: 'namespace-0',
					state: 'pending-009'
				}),
				expect.objectContaining({
					id: 'id-010',
					namespace: 'namespace-0',
					state: 'layer-010'
				}),
				expect.objectContaining({
					id: 'id-011',
					namespace: 'namespace-0',
					state: 'layer-011'
				})
			]);
		});

		it('should returns on cursor', async () => {
			vi.mocked(layer.cursor).mockResolvedValue('2024-11-28T01:00:00.000Z');

			const res = await layer.get('namespace-0');

			expect(layer.cursor).toHaveBeenCalledWith();
			expect(layer.mergePendingEvents).toHaveBeenCalledWith('table-1#namespace-0#2024-11-28T01:00:00.000Z', expect.any(Array));
			expect(layer.db.query).toHaveBeenCalledWith({
				item: {
					pk: 'table-1#namespace-0#2024-11-28T01:00:00.000Z'
				},
				limit: Infinity
			});

			expect(res).toEqual([
				expect.objectContaining({
					id: 'id-000',
					namespace: 'namespace-0',
					state: 'layer-000'
				}),
				expect.objectContaining({
					id: 'id-001',
					namespace: 'namespace-0',
					state: 'layer-001'
				}),
				// ** DELETED **
				// expect.objectContaining({
				// 	id: 'id-002',
				// 	namespace: 'namespace-0',
				// 	state: 'layer-002'
				// }),
				// expect.objectContaining({
				// 	id: 'id-003',
				// 	namespace: 'namespace-0',
				// 	state: 'layer-003'
				// }),
				expect.objectContaining({
					id: 'id-004',
					namespace: 'namespace-0',
					state: 'layer-004'
				}),
				expect.objectContaining({
					id: 'id-005',
					namespace: 'namespace-0',
					state: 'layer-005'
				}),
				expect.objectContaining({
					id: 'id-006',
					namespace: 'namespace-0',
					state: 'layer-006'
				}),
				expect.objectContaining({
					id: 'id-007',
					namespace: 'namespace-0',
					state: 'layer-007'
				}),
				expect.objectContaining({
					id: 'id-008',
					namespace: 'namespace-0',
					state: 'pending-008'
				}),
				expect.objectContaining({
					id: 'id-009',
					namespace: 'namespace-0',
					state: 'pending-009'
				}),
				expect.objectContaining({
					id: 'id-010',
					namespace: 'namespace-0',
					state: 'layer-010'
				}),
				expect.objectContaining({
					id: 'id-011',
					namespace: 'namespace-0',
					state: 'layer-011'
				})
			]);
		});

		it('should returns empty if no partition', async () => {
			const res = await layer.get('namespace-1');

			expect(layer.cursor).toHaveBeenCalledWith();
			expect(layer.mergePendingEvents).toHaveBeenCalledWith('table-1#namespace-1#__INITIAL__', expect.any(Array));
			expect(res).toEqual([]);
		});

		it('should returns empty if empty partition', async () => {
			const res = await layer.get();

			expect(layer.cursor).toHaveBeenCalledWith();
			expect(layer.mergePendingEvents).toHaveBeenCalledWith('table-1#__INITIAL__', expect.any(Array));
			expect(res).toEqual([]);
		});
	});

	describe('mergePendingEvents', () => {
		beforeEach(async () => {
			vi.mocked(layer.getter).mockImplementation(async (partition: string) => {
				if (partition === 'table-1#namespace-0') {
					return _.times(12, index => {
						return createItem({
							index,
							state: 'layer'
						});
					});
				}

				return [];
			});
		});

		it('should merge pending items', async () => {
			const pk = 'table-1#namespace-0';
			const pendingEvents = _.times(5, index => {
				const item = createItem({
					index: 10 + index,
					state: 'pending'
				});

				const pendingEvent: PendingEvent<Item> = {
					cursor: '__INITIAL__',
					item,
					pk,
					sk: item.id,
					type: 'PUT',
					ttl: _.now()
				};

				return pendingEvent;
			});

			expect(_.map(await layer.getter(pk), 'state')).toEqual([
				'layer-000',
				'layer-001',
				'layer-002',
				'layer-003',
				'layer-004',
				'layer-005',
				'layer-006',
				'layer-007',
				'layer-008',
				'layer-009',
				'layer-010',
				'layer-011'
			]);

			const res = await layer.mergePendingEvents(pk, pendingEvents);

			expect(_.map(res, 'state')).toEqual([
				'layer-000',
				'layer-001',
				'layer-002',
				'layer-003',
				'layer-004',
				'layer-005',
				'layer-006',
				'layer-007',
				'layer-008',
				'layer-009',
				'pending-010',
				'pending-011',
				'pending-012',
				'pending-013',
				'pending-014'
			]);
		});

		it('should handle DELETE type correctly', async () => {
			const pk = 'table-1#namespace-0';
			const pendingEvents = _.times(5, index => {
				const item = createItem({
					index: 10 + index,
					state: 'pending'
				});

				const pendingEvent: PendingEvent<Item> = {
					cursor: '__INITIAL__',
					item,
					pk,
					sk: item.id,
					type: 'DELETE',
					ttl: _.now()
				};

				return pendingEvent;
			});

			expect(_.map(await layer.getter(pk), 'state')).toEqual([
				'layer-000',
				'layer-001',
				'layer-002',
				'layer-003',
				'layer-004',
				'layer-005',
				'layer-006',
				'layer-007',
				'layer-008',
				'layer-009',
				'layer-010',
				'layer-011'
			]);

			const res = await layer.mergePendingEvents(pk, pendingEvents);

			expect(_.map(res, 'state')).toEqual([
				'layer-000',
				'layer-001',
				'layer-002',
				'layer-003',
				'layer-004',
				'layer-005',
				'layer-006',
				'layer-007',
				'layer-008',
				'layer-009'
			]);
		});
	});

	describe('reset', () => {
		let db: Db<Item>;

		beforeAll(async () => {
			db = new Db<Item>({
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
						sk: `id-${i}`,
						pk: `namespace-${i % 2}`,
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
			vi.spyOn(layer, 'setter');
			vi.spyOn(layer.db, 'batchDelete');
			vi.spyOn(layer.db, 'query');

			await Promise.all([
				// namespace 0
				layer.set(
					createChangeEvents({
						count: 2
					}),
					'__INITIAL__'
				),
				// namespace 1
				layer.set(
					createChangeEvents({
						count: 2,
						namespace: 1
					}),
					'__INITIAL__'
				)
			]);
		});

		it('must reset', async () => {
			await layer.reset(db, 'namespace-0');

			expect(layer.db.query).toHaveBeenCalledWith({
				item: {
					cursor: '__INITIAL__',
					pk: 'table-1#namespace-0#__INITIAL__'
				},
				limit: Infinity,
				onChunk: expect.any(Function)
			});

			expect(layer.db.batchDelete).toHaveBeenCalledOnce();
			expect(layer.db.batchDelete).toHaveBeenCalledWith(
				expect.arrayContaining([
					expect.objectContaining({
						pk: 'table-1#namespace-0#__INITIAL__',
						sk: 'id-000'
					}),
					expect.objectContaining({
						pk: 'table-1#namespace-0#__INITIAL__',
						sk: 'id-001'
					})
				])
			);

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
			expect(layer.setter).toHaveBeenCalledWith('table-1#namespace-0', expect.any(Array));

			expect(_.map(setterArgs[0].items, 'state')).toEqual(['db-0', 'db-2', 'db-4', 'db-6', 'db-8']);
		});

		it('must reset without partition', async () => {
			await layer.reset(db);

			expect(layer.db.query).toHaveBeenCalledWith({
				item: { cursor: '__INITIAL__' },
				limit: Infinity,
				onChunk: expect.any(Function)
			});

			expect(db.scan).toHaveBeenCalledWith({
				limit: Infinity,
				onChunk: expect.any(Function)
			});

			expect(layer.db.batchDelete).toHaveBeenCalledOnce();
			expect(layer.db.batchDelete).toHaveBeenCalledWith(
				expect.arrayContaining([
					expect.objectContaining({
						pk: 'table-1#namespace-0#__INITIAL__',
						sk: 'id-000'
					}),
					expect.objectContaining({
						pk: 'table-1#namespace-0#__INITIAL__',
						sk: 'id-001'
					}),
					expect.objectContaining({
						pk: 'table-1#namespace-1#__INITIAL__',
						sk: 'id-000'
					}),
					expect.objectContaining({
						pk: 'table-1#namespace-1#__INITIAL__',
						sk: 'id-001'
					})
				])
			);

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
			expect(layer.setter).toHaveBeenCalledWith('table-1#namespace-0', expect.any(Array));
			expect(layer.setter).toHaveBeenCalledWith('table-1#namespace-1', expect.any(Array));

			expect(_.map(setterArgs[0].items, 'state')).toEqual(['db-0', 'db-2', 'db-4', 'db-6', 'db-8']);
			expect(_.map(setterArgs[1].items, 'state')).toEqual(['db-1', 'db-3', 'db-5', 'db-7', 'db-9']);
		});
	});

	describe('resolvePartition', () => {
		it('should resolve partition with table and partition', () => {
			const partition = 'namespace-0';
			const resolvedPartition = layer.resolvePartition('__INITIAL__', partition);

			expect(resolvedPartition).toEqual('table-1#namespace-0#__INITIAL__');
		});

		it('should resolve partition with only table', () => {
			const resolvedPartition = layer.resolvePartition('__INITIAL__');

			expect(resolvedPartition).toEqual('table-1#__INITIAL__');
		});

		it('should resolve partition with empty partition', () => {
			const resolvedPartition = layer.resolvePartition('__INITIAL__', '');

			expect(resolvedPartition).toEqual('table-1#__INITIAL__');
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
						cursor: '__INITIAL__',
						item: expect.objectContaining({
							id: 'id-000',
							namespace: 'namespace-0',
							state: 'pending-000'
						}),
						pk: 'table-1#namespace-0#__INITIAL__',
						sk: 'id-000',
						type: 'PUT',
						ttl: expect.any(Number)
					}),
					expect.objectContaining({
						cursor: '__INITIAL__',
						item: expect.objectContaining({
							id: 'id-001',
							namespace: 'namespace-0',
							state: 'pending-001'
						}),
						pk: 'table-1#namespace-0#__INITIAL__',
						sk: 'id-001',
						type: 'PUT',
						ttl: expect.any(Number)
					}),
					expect.objectContaining({
						cursor: '__INITIAL__',
						item: expect.objectContaining({
							id: 'id-002',
							namespace: 'namespace-0',
							state: 'pending-002'
						}),
						pk: 'table-1#namespace-0#__INITIAL__',
						sk: 'id-002',
						type: 'PUT',
						ttl: expect.any(Number)
					})
				])
			);
		});

		it('should throw error if item has no unique identifier', async () => {
			try {
				const events = createChangeEvents({
					count: 1,
					namespace: 1,
					state: 'pending'
				});

				events[0].item.id = '';
				await layer.set(events);

				throw new Error('expected to throw');
			} catch (err) {
				expect(err.message).toEqual('Item must have an unique identifier');
			}
		});
	});

	describe('sync', () => {
		beforeAll(async () => {
			await Promise.all([
				// namespace 0
				layer.set(
					createChangeEvents({
						count: 4,
						type: 'PUT'
					}),
					'__INITIAL__'
				),
				layer.set(
					createChangeEvents({
						count: 2,
						type: 'DELETE'
					}),
					'2024-11-28T01:00:00.000Z'
				),
				layer.set(
					createChangeEvents({
						count: 4,
						type: 'UPDATE'
					}),
					'2024-11-28T02:00:00.000Z'
				),
				// namespace 1 (empty layer)
				layer.set(
					createChangeEvents({
						count: 4,
						namespace: 1,
						type: 'PUT'
					}),
					'__INITIAL__'
				),
				layer.set(
					createChangeEvents({
						count: 2,
						namespace: 1,
						type: 'DELETE'
					}),
					'2024-11-28T01:00:00.000Z'
				)
			]);
		});

		afterAll(async () => {
			await layer.db.clear();
		});

		beforeEach(async () => {
			vi.spyOn(layer, 'cursor');
			vi.spyOn(layer.db, 'query');
			vi.spyOn(layer, 'setter');

			vi.mocked(layer.getter).mockImplementation(async (partition: string) => {
				if (_.startsWith(partition, 'table-1#namespace-0')) {
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

		it('should sync on first cursor', async () => {
			const res = await layer.sync();

			expect(layer.cursor).toHaveBeenCalledTimes(2);
			expect(layer.cursor).toHaveBeenCalledWith();
			expect(layer.cursor).toHaveBeenCalledWith(true);

			expect(layer.db.query).toHaveBeenCalledWith({
				item: { cursor: '__INITIAL__' },
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
			expect(layer.setter).toHaveBeenCalledWith('table-1#namespace-0', expect.any(Array));
			expect(layer.setter).toHaveBeenCalledWith('table-1#namespace-1', expect.any(Array));

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
				count: 8
			});
		});

		it('should sync on 2nd cursor', async () => {
			vi.mocked(layer.cursor).mockResolvedValue('2024-11-28T01:00:00.000Z');

			const res = await layer.sync();

			expect(layer.cursor).toHaveBeenCalledTimes(2);
			expect(layer.cursor).toHaveBeenCalledWith();
			expect(layer.cursor).toHaveBeenCalledWith(true);

			expect(layer.db.query).toHaveBeenCalledWith({
				item: { cursor: '2024-11-28T01:00:00.000Z' },
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
			expect(layer.setter).toHaveBeenCalledWith('table-1#namespace-0', expect.any(Array));
			expect(layer.setter).toHaveBeenCalledWith('table-1#namespace-1', expect.any(Array));

			expect(_.map(setterArgs[0].items, 'state')).toEqual(expect.arrayContaining(['layer-004', 'layer-005', 'layer-006', 'layer-007']));

			expect(_.map(setterArgs[1].items, 'state')).toEqual(expect.arrayContaining([]));

			expect(res).toEqual({
				count: 4
			});
		});

		it('should sync on 3rd cursor', async () => {
			vi.mocked(layer.cursor).mockResolvedValue('2024-11-28T02:00:00.000Z');

			const res = await layer.sync();

			expect(layer.cursor).toHaveBeenCalledTimes(2);
			expect(layer.cursor).toHaveBeenCalledWith();
			expect(layer.cursor).toHaveBeenCalledWith(true);

			expect(layer.db.query).toHaveBeenCalledWith({
				item: { cursor: '2024-11-28T02:00:00.000Z' },
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

			expect(layer.setter).toHaveBeenCalledTimes(1);
			expect(layer.setter).toHaveBeenCalledWith('table-1#namespace-0', expect.any(Array));

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
				count: 4
			});
		});
	});
});

describe('/layer.ts (without partition)', () => {
	let layer: Layer<Item>;

	beforeAll(async () => {
		layer = await factory({
			createTable: true,
			getter: vi.fn(async () => []),
			setter: vi.fn()
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
			createTable: false,
			getter: vi.fn(async () => []),
			setter: vi.fn()
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
					'__INITIAL__'
				),
				layer.set(
					createChangeEvents({
						count: 2,
						initialIndex: 2,
						type: 'DELETE'
					}),
					'__INITIAL__'
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

			vi.spyOn(layer, 'cursor');
			vi.spyOn(layer, 'mergePendingEvents');
			vi.spyOn(layer.db, 'query');
		});

		it('should returns', async () => {
			const res = await layer.get();

			expect(layer.cursor).toHaveBeenCalledWith();
			expect(layer.mergePendingEvents).toHaveBeenCalledWith('table-1#__INITIAL__', expect.any(Array));
			expect(layer.db.query).toHaveBeenCalledWith({
				item: {
					pk: 'table-1#__INITIAL__'
				},
				limit: Infinity
			});

			expect(res).toEqual([
				expect.objectContaining({
					id: 'id-000',
					namespace: 'namespace-0',
					state: 'pending-000'
				}),
				expect.objectContaining({
					id: 'id-001',
					namespace: 'namespace-0',
					state: 'pending-001'
				}),
				expect.objectContaining({
					id: 'id-004',
					namespace: 'namespace-0',
					state: 'layer-004'
				}),
				expect.objectContaining({
					id: 'id-005',
					namespace: 'namespace-0',
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
						cursor: '__INITIAL__',
						item: expect.objectContaining({
							id: 'id-000',
							namespace: 'namespace-0',
							state: 'pending-000'
						}),
						pk: 'table-1#__INITIAL__',
						sk: 'id-000',
						type: 'PUT',
						ttl: expect.any(Number)
					}),
					expect.objectContaining({
						cursor: '__INITIAL__',
						item: expect.objectContaining({
							id: 'id-001',
							namespace: 'namespace-0',
							state: 'pending-001'
						}),
						pk: 'table-1#__INITIAL__',
						sk: 'id-001',
						type: 'PUT',
						ttl: expect.any(Number)
					}),
					expect.objectContaining({
						cursor: '__INITIAL__',
						item: expect.objectContaining({
							id: 'id-002',
							namespace: 'namespace-0',
							state: 'pending-002'
						}),
						pk: 'table-1#__INITIAL__',
						sk: 'id-002',
						type: 'PUT',
						ttl: expect.any(Number)
					})
				])
			);
		});
	});

	describe('reset', () => {
		let db: Db<Item>;

		beforeAll(async () => {
			db = new Db<Item>({
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
						sk: `id-${i}`,
						pk: `namespace-${i % 2}`,
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
			vi.spyOn(layer, 'setter');
			vi.spyOn(layer.db, 'batchDelete');
			vi.spyOn(layer.db, 'query');

			await Promise.all([
				// namespace 0
				layer.set(
					createChangeEvents({
						count: 2
					}),
					'__INITIAL__'
				)
			]);
		});

		it('must reset', async () => {
			await layer.reset(db);

			expect(layer.db.query).toHaveBeenCalledWith({
				item: {
					cursor: '__INITIAL__'
				},
				limit: Infinity,
				onChunk: expect.any(Function)
			});

			expect(layer.db.batchDelete).toHaveBeenCalledOnce();
			expect(layer.db.batchDelete).toHaveBeenCalledWith(
				expect.arrayContaining([
					expect.objectContaining({
						pk: 'table-1#__INITIAL__',
						sk: 'id-000'
					}),
					expect.objectContaining({
						pk: 'table-1#__INITIAL__',
						sk: 'id-001'
					})
				])
			);

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
		});
	});

	describe('sync', () => {
		beforeAll(async () => {
			await Promise.all([
				layer.set(
					createChangeEvents({
						count: 2
					}),
					'__INITIAL__'
				),
				layer.set(
					createChangeEvents({
						count: 2,
						initialIndex: 2,
						type: 'DELETE'
					}),
					'__INITIAL__'
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
				item: { cursor: '__INITIAL__' },
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

			expect(layer.setter).toHaveBeenCalledWith('table-1', expect.any(Array));
			expect(_.map(setterArgs[0].items, 'state')).toEqual(expect.arrayContaining(['pending-000', 'pending-001', 'layer-004', 'layer-005']));

			expect(res).toEqual({
				count: 4
			});
		});
	});
});
