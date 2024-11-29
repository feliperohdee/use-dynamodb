import _ from 'lodash';

import type Db from './index';
import type { ChangeEvent, ChangeType, Dict, PersistedItem, TableGSI } from './index';

type LayerMeta = {
	cursor: string;
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
	cursor: string;
	item: PersistedItem<T>;
	pk: string;
	sk: string;
	ttl: number;
	type: ChangeType;
};

const FIVE_DAYS_IN_SECONDS = 5 * 24 * 60 * 60;

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
			cursor: '__INITIAL__',
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
			return index.partition === 'cursor' && index.partitionType === 'S' && index.sort === 'pk' && index.sortType === 'S';
		});

		if (!cursorGSI) {
			throw new Error('Dynamodb schema must have a GSI with cursor as partition key and pk as sort key');
		}
	}

	async meta(set?: { advanceCursor: boolean; unsyncedTotal: number; syncedTotal: number }): Promise<LayerMeta> {
		if (set) {
			if (set.syncedTotal > 0 && set.unsyncedTotal > 0) {
				throw new Error('Cannot set both syncedTotal and unsyncedTotal at the same time');
			}

			let update: {
				atributeNames: Dict;
				attributeValues: Dict;
				expression: Dict;
			} = {
				atributeNames: {},
				attributeValues: {},
				expression: {
					add: [],
					set: []
				}
			};

			if (set.advanceCursor) {
				update = {
					...update,
					atributeNames: {
						...update.atributeNames,
						'#cursor': 'cursor'
					},
					attributeValues: {
						...update.attributeValues,
						':cursor': new Date().toISOString()
					},
					expression: {
						...update.expression,
						set: [...update.expression.set, '#cursor = :cursor']
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

			const meta = await this.db.update<LayerMeta>({
				filter: {
					item: { pk: '__meta', sk: '__meta' }
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

					return expression;
				})(),
				upsert: true
			});

			this.currentMeta = {
				...this.currentMeta,
				...meta,
				loaded: true
			};
		} else if (!this.currentMeta.loaded) {
			const res = await this.db.get<LayerMeta>({
				consistentRead: true,
				item: { pk: '__meta', sk: '__meta' }
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
		const { cursor } = await this.meta();
		const pk = this.resolvePartition(cursor, partition);
		const pendingEvents = await this.db.query({
			item: { pk },
			limit: Infinity
		});

		const newItems = await this.mergePendingEvents(pk, pendingEvents.items);

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

	async mergePendingEvents(pk: string = '', pendingEvents: LayerPendingEvent<T>[]): Promise<PersistedItem<T>[]> {
		const layerItems = await this.getter(pk);
		const [pendingSet, pendingDelete] = _.partition(pendingEvents, ({ type }) => {
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

	resolvePartition(cursor: string, partition?: string) {
		return _.compact([this.table, _.trim(partition), cursor]).join('#');
	}

	async set(events: ChangeEvent<T>[], cursor?: string) {
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
			advanceCursor: false,
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
		const { cursor } = await this.meta();

		// advance cursor now to ensure we don't miss any changes from now
		await this.meta({
			advanceCursor: true,
			syncedTotal: 0,
			unsyncedTotal: 0
		});

		const pendingEvents: Record<string, LayerPendingEvent<T>[]> = {};

		const { count } = await this.db.query({
			item: { cursor },
			limit: Infinity,
			onChunk: async ({ items }) => {
				const pendingEventsByPk = _.groupBy(items, 'pk');

				for (const pk in pendingEventsByPk) {
					if (!pendingEvents[pk]) {
						pendingEvents[pk] = [];
					}

					pendingEvents[pk] = [...pendingEvents[pk], ...pendingEventsByPk[pk]];
				}
			}
		});

		for (const pk in pendingEvents) {
			const newItems = await this.mergePendingEvents(pk, pendingEvents[pk]);

			await this.setter(pk.replace(`#${cursor}`, ''), newItems);
		}

		await this.meta({
			advanceCursor: false,
			syncedTotal: count,
			unsyncedTotal: 0
		});

		return {
			count
		};
	}
}

export { LayerMeta, LayerPendingEvent };
export default Layer;
