import _ from 'lodash';
import { ConditionalCheckFailedException } from '@aws-sdk/client-dynamodb';

import type Db from './index';
import type { ChangeEvent, ChangeType, Dict, PersistedItem, TableGSI } from './index';

type LayerMeta = {
	cursor: number;
	cursorMax: number;
	loaded: boolean;
	syncedLastTotal: number;
	syncedTimes: number;
	syncedTotal: number;
	unsyncedLastTotal: number;
	unsyncedTotal: number;
};

type LayerGetter<T extends PersistedItem> = (partition: string) => Promise<T[]>;
type LayerSetter<T extends PersistedItem> = (partition: string, value: T[]) => Promise<void>;
type LayerPendingEvent<T extends Dict = Dict> = {
	cursor: number;
	item: PersistedItem<T>;
	pk: string;
	sk: string;
	ttl: number;
	type: ChangeType;
};

const FIVE_DAYS_IN_SECONDS = 5 * 24 * 60 * 60;
const LOCK_TTL_IN_SECONDS = 5 * 60;

class Layer<T extends Dict = Dict> {
	public backgroundRunner?: (promise: Promise<void>) => void;
	public db: Db<LayerPendingEvent<T>>;
	public getter: LayerGetter<PersistedItem<T>>;
	public setter: LayerSetter<PersistedItem<T>>;
	public syncStrategy?: (meta: LayerMeta) => boolean;

	private currentMeta: LayerMeta;
	private getItemUniqueIdentifier: (item: PersistedItem<T>) => string;
	private getItemPartition: (item: PersistedItem<T>) => string;
	private table: string;
	private ttl: (item: PersistedItem<T>) => number;

	constructor(options: {
		backgroundRunner?: () => Promise<void>;
		db: Db<LayerPendingEvent<T>>;
		getItemPartition?: (item: PersistedItem<T>) => string;
		getItemUniqueIdentifier: (item: PersistedItem<T>) => string;
		getter: LayerGetter<PersistedItem<T>>;
		setter: LayerSetter<PersistedItem<T>>;
		syncStrategy?: (meta: LayerMeta) => boolean;
		table: string;
		ttl?: number | ((item: PersistedItem<T>) => number);
	}) {
		this.currentMeta = {
			cursor: 0,
			cursorMax: 0,
			loaded: false,
			syncedLastTotal: 0,
			syncedTimes: 0,
			syncedTotal: 0,
			unsyncedLastTotal: 0,
			unsyncedTotal: 0
		};
		this.backgroundRunner = options.backgroundRunner;
		this.db = options.db;
		this.getItemUniqueIdentifier = options.getItemUniqueIdentifier;
		this.getItemPartition = options.getItemPartition ?? (() => '');
		this.getter = options.getter;
		this.setter = options.setter;
		this.syncStrategy = options.syncStrategy;
		this.table = options.table;

		if (_.isFunction(options.ttl)) {
			this.ttl = options.ttl;
		} else {
			this.ttl = () => {
				return (options.ttl as number) ?? FIVE_DAYS_IN_SECONDS;
			};
		}

		if (this.db.schema.partition !== 'pk') {
			throw new Error('Dynamodb schema partition key must be pk');
		}

		if (this.db.schema.sort !== 'sk') {
			throw new Error('Dynamodb schema sort key must be sk');
		}

		const cursorGSI = _.find(this.db.indexes, (index: TableGSI) => {
			return index.partition === 'cursor' && index.partitionType === 'N' && index.sort === 'pk' && index.sortType === 'S';
		});

		if (!cursorGSI) {
			throw new Error('Dynamodb schema must have a GSI with cursor as partition key and pk as sort key');
		}
	}

	async meta(set?: { advanceCursor: 1 | 0 | -1; unsyncedTotal: number; syncedTotal: number }): Promise<LayerMeta> {
		if (set) {
			if (set.syncedTotal > 0 && set.unsyncedTotal > 0) {
				throw new Error('Cannot set both syncedTotal and unsyncedTotal at the same time');
			}

			let update: {
				atributeNames: Dict;
				attributeValues: Dict;
				conditionExpression: string;
				expression: Dict;
			} = {
				atributeNames: {},
				attributeValues: {},
				conditionExpression: '',
				expression: {
					add: [],
					set: []
				}
			};

			if (set.advanceCursor !== 0) {
				update = {
					...update,
					atributeNames: {
						...update.atributeNames,
						'#cursor': 'cursor',
						'#cursorMax': 'cursorMax'
					},
					attributeValues: {
						...update.attributeValues,
						':cursor': set.advanceCursor,
						':cursorMax': Math.max(0, set.advanceCursor),
						':negative': -1,
						':positive': 1
					},
					conditionExpression: '(:cursor = :negative AND #cursor >= :positive) OR :cursor = :positive',
					expression: {
						...update.expression,
						add: [...update.expression.add, '#cursor :cursor', '#cursorMax :cursorMax']
					}
				};
			}

			if (set.syncedTotal > 0) {
				update = {
					...update,
					atributeNames: {
						...update.atributeNames,
						'#syncedLastTotal': 'syncedLastTotal',
						'#syncedTimes': 'syncedTimes',
						'#syncedTotal': 'syncedTotal',
						'#unsyncedLastTotal': 'unsyncedLastTotal',
						'#unsyncedTotal': 'unsyncedTotal'
					},
					attributeValues: {
						...update.attributeValues,
						':syncedTimes': 1,
						':syncedTotal': set.syncedTotal,
						':unsyncedTotal': 0
					},
					expression: {
						...update.expression,
						add: [...update.expression.add, '#syncedTimes :syncedTimes', '#syncedTotal :syncedTotal'],
						set: [
							...update.expression.set,
							'#syncedLastTotal = :syncedTotal',
							'#unsyncedLastTotal = :unsyncedTotal',
							'#unsyncedTotal = :unsyncedTotal'
						]
					}
				};
			} else if (set.unsyncedTotal > 0) {
				update = {
					...update,
					atributeNames: {
						...update.atributeNames,
						'#unsyncedLastTotal': 'unsyncedLastTotal',
						'#unsyncedTotal': 'unsyncedTotal'
					},
					attributeValues: {
						...update.attributeValues,
						':unsyncedTotal': set.unsyncedTotal
					},
					expression: {
						...update.expression,
						add: [...update.expression.add, '#unsyncedTotal :unsyncedTotal'],
						set: [...update.expression.set, '#unsyncedLastTotal = :unsyncedTotal']
					}
				};
			}

			try {
				const meta = await this.db.update<LayerMeta>({
					conditionExpression: update.conditionExpression,
					filter: {
						item: { pk: `__${this.table}__`, sk: '__meta__' }
					},
					attributeNames: update.atributeNames,
					attributeValues: update.attributeValues,
					updateExpression: (() => {
						let expression = '';

						if (_.size(update.expression.set)) {
							expression += `SET ${_.join(update.expression.set, ', ')}`;
						}

						if (_.size(update.expression.add)) {
							expression += ` ADD ${_.join(update.expression.add, ', ')}`;
						}

						return _.trim(expression);
					})(),
					upsert: true
				});

				this.currentMeta = {
					...this.currentMeta,
					...meta,
					loaded: true
				};
			} catch (err) {
				if (err instanceof ConditionalCheckFailedException) {
					return {
						...this.currentMeta,
						loaded: true
					};
				}

				throw err;
			}
		} else if (!this.currentMeta.loaded) {
			const res = await this.db.get<LayerMeta>({
				consistentRead: true,
				item: { pk: `__${this.table}__`, sk: '__meta__' }
			});

			if (res) {
				this.currentMeta = {
					...this.currentMeta,
					...res,
					loaded: true
				};
			}
		}

		return _.omit(this.currentMeta, ['pk', 'sk', '__createdAt', '__ts', '__updatedAt']) as LayerMeta;
	}

	async get(partition?: string, sorted = true): Promise<PersistedItem<T>[]> {
		const { cursor, cursorMax } = await this.meta();
		const pks = _.map(_.range(cursor, cursorMax + 1), cursor => {
			return this.resolvePartition(cursor, partition);
		});

		const pendingEvents = await Promise.all(
			_.map(pks, async pk => {
				const res = await this.db.query({
					item: { pk },
					limit: Infinity
				});

				return res.items;
			})
		);

		const pkWithoutCursor = pks[0].replace(`#${cursor}`, '');
		const layerItems = await this.getter(pkWithoutCursor);
		const newItems = this.merge(layerItems, _.flatten(pendingEvents));

		if (sorted) {
			return _.sortBy(newItems, [
				item => {
					return this.getItemUniqueIdentifier(item);
				},
				'__ts'
			]);
		}

		return newItems;
	}

	async acquireLock(lock: boolean): Promise<boolean> {
		const now = _.now();

		if (lock) {
			try {
				await this.db.update({
					attributeNames: { '#__ts': '__ts' },
					attributeValues: { ':ts_less_ttl': now - LOCK_TTL_IN_SECONDS * 1000 },
					conditionExpression: 'attribute_not_exists(#__ts) OR #__ts < :ts_less_ttl',
					filter: {
						item: { pk: `__${this.table}__`, sk: '__lock__' }
					},
					upsert: true
				});

				return true; // Lock acquired
			} catch {
				return false; // Failed to acquire lock
			}
		}

		await this.db.delete({
			filter: {
				item: { pk: `__${this.table}__`, sk: '__lock__' }
			}
		});

		return true; // Lock released
	}

	merge(layerItems: PersistedItem<T>[], pendingEvents: PersistedItem<LayerPendingEvent<T>>[]): PersistedItem<T>[] {
		const mostRecentPendingEvents = _(pendingEvents)
			.groupBy(({ item }) => {
				return this.getItemUniqueIdentifier(item);
			})
			.mapValues(group => {
				return _.maxBy(group, '__ts')!;
			})
			.value();

		const [pendingSet, pendingDelete] = _.partition(mostRecentPendingEvents, ({ type }) => {
			return type !== 'DELETE';
		});

		const pendingSetItems = _.map(pendingSet, 'item');
		const pendingDeleteItems = new Set(
			_.map(pendingDelete, ({ item }) => {
				return this.getItemUniqueIdentifier(item);
			})
		);

		const newItemsDict = {
			..._.keyBy(layerItems, this.getItemUniqueIdentifier),
			..._.keyBy(pendingSetItems, this.getItemUniqueIdentifier)
		};

		if (pendingDeleteItems.size) {
			return _.filter(newItemsDict, (item, uniqueIdentifier) => {
				return !pendingDeleteItems.has(uniqueIdentifier);
			});
		}

		return _.values(newItemsDict);
	}

	async reset(db: Db<T>, partition?: string) {
		const { cursor } = await this.meta();
		const pk = this.resolvePartition(cursor, partition);
		const pendingEvents: Record<string, PersistedItem<T>[]> = {};

		// first clean up pending events
		await this.db.query({
			item: partition ? { cursor, pk } : { cursor },
			limit: Infinity,
			onChunk: async ({ items }) => {
				await this.db.batchDelete(items);
			}
		});

		const { count } = await db.scan({
			limit: Infinity,
			onChunk: async ({ items }) => {
				for (const item of items) {
					const itemPartition = this.getItemPartition(item);

					if (partition && itemPartition !== partition) {
						continue;
					}

					const pk = this.resolvePartition(cursor, itemPartition);

					if (!pendingEvents[pk]) {
						pendingEvents[pk] = [];
					}

					pendingEvents[pk] = [...pendingEvents[pk], item];
				}
			}
		});

		for (const pk in pendingEvents) {
			await this.setter(pk.replace(`#${cursor}`, ''), pendingEvents[pk]);
		}

		return {
			count
		};
	}

	resolvePartition(cursor: number, partition?: string) {
		return _.compact([this.table, _.trim(partition), `${cursor}`]).join('#');
	}

	async set(events: ChangeEvent<T>[], cursor?: number) {
		if (!_.size(events)) {
			return;
		}

		cursor =
			cursor ??
			(await (async () => {
				const { cursor } = await this.meta();

				return cursor;
			})());

		const now = _.now();
		const writeEvents = _.map(events, ({ item, type }) => {
			const pk = this.resolvePartition(cursor, this.getItemPartition(item));
			const sk = this.getItemUniqueIdentifier(item);

			if (!sk) {
				throw new Error('Item must have an unique identifier');
			}

			const pendingItem: LayerPendingEvent<T> = {
				cursor,
				item,
				pk,
				sk,
				ttl: Math.floor(now / 1000) + this.ttl(item),
				type
			};

			return pendingItem;
		});

		const meta = await this.meta({
			advanceCursor: 0,
			unsyncedTotal: _.size(writeEvents),
			syncedTotal: 0
		});

		if (_.isFunction(this.syncStrategy)) {
			const sync = this.syncStrategy(meta);

			if (sync) {
				_.isFunction(this.backgroundRunner)
					? this.backgroundRunner(
							(async () => {
								await this.sync();
							})()
						)
					: await this.sync();
			}
		}

		return this.db.batchWrite(writeEvents);
	}

	async sync() {
		const lock = await this.acquireLock(true);

		if (!lock) {
			return {
				count: 0,
				locked: true
			};
		}

		try {
			const { cursor } = await this.meta();

			// advance cursor now to ensure we don't miss any changes from now
			await this.meta({
				advanceCursor: 1,
				syncedTotal: 0,
				unsyncedTotal: 0
			});

			const pendingEvents: Record<string, PersistedItem<LayerPendingEvent<T>>[]> = {};
			const { count } = await this.db.query({
				item: { cursor, pk: this.table },
				limit: Infinity,
				onChunk: async ({ items }) => {
					const pendingEventsByPk = _.groupBy(items, 'pk');

					for (const pk in pendingEventsByPk) {
						if (!pendingEvents[pk]) {
							pendingEvents[pk] = [];
						}

						pendingEvents[pk] = [...pendingEvents[pk], ...pendingEventsByPk[pk]];
					}
				},
				prefix: true
			});

			try {
				for (const pk in pendingEvents) {
					const pkWithoutCursor = pk.replace(`#${cursor}`, '');
					const layerItems = await this.getter(pkWithoutCursor);
					const newItems = this.merge(layerItems, pendingEvents[pk]);

					await this.setter(pkWithoutCursor, newItems);
				}

				await this.meta({
					advanceCursor: 0,
					syncedTotal: count,
					unsyncedTotal: 0
				});
			} catch (err) {
				// if there is an error, we need to rollback the cursor
				await this.meta({
					advanceCursor: -1,
					syncedTotal: 0,
					unsyncedTotal: 0
				});

				throw err;
			}

			return {
				count,
				locked: false
			};
		} finally {
			await this.acquireLock(false);
		}
	}
}

export { LayerMeta, LayerPendingEvent };
export default Layer;
