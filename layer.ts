import _ from 'lodash';

import type { ChangeEvent, ChangeType, Dict, PersistedItem, TableGSI, TableLSI } from './index';
import type Db from './index';

type PendingEvent<T extends Dict = Dict> = {
	cursor: string;
	item: PersistedItem<T>;
	pk: string;
	sk: string;
	ttl: number;
	type: ChangeType;
};

type LayerGetter<T extends PersistedItem> = (partition: string) => Promise<T[]>;
type LayerSetter<T extends PersistedItem> = (partition: string, value: T[]) => Promise<void>;

class Layer<T extends Dict = Dict> {
	public db: Db<PendingEvent<T>>;
	public getter: LayerGetter<PersistedItem<T>>;
	public setter: LayerSetter<PersistedItem<T>>;

	private currentCursor: string;
	private getItemUniqueIdentifier: (item: PersistedItem<T>) => string;
	private getItemPartition: (item: PersistedItem<T>) => string;
	private table: string;

	constructor(options: {
		db: Db<PendingEvent<T>>;
		getItemPartition?: (item: PersistedItem<T>) => string;
		getItemUniqueIdentifier: (item: PersistedItem<T>) => string;
		getter: LayerGetter<PersistedItem<T>>;
		setter: LayerSetter<PersistedItem<T>>;
		table: string;
	}) {
		this.currentCursor = '';
		this.db = options.db;
		this.getItemUniqueIdentifier = options.getItemUniqueIdentifier;
		this.getItemPartition = options.getItemPartition ?? (() => '');
		this.getter = options.getter;
		this.setter = options.setter;
		this.table = options.table;

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

	async cursor(set?: boolean) {
		if (set) {
			const { cursor } = await this.db.update({
				filter: {
					item: {
						cursor: new Date().toISOString(),
						pk: '__meta',
						sk: '__meta'
					}
				},
				upsert: true
			});

			this.currentCursor = cursor;
		} else if (!this.currentCursor) {
			const res = await this.db.get({
				consistentRead: true,
				item: { pk: '__meta', sk: '__meta' }
			});

			this.currentCursor = res?.cursor ?? '__INITIAL__';
		}

		return this.currentCursor;
	}

	async get(partition?: string, sorted = true): Promise<PersistedItem<T>[]> {
		const cursor = await this.cursor();
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

	async mergePendingEvents(pk: string = '', pendingEvents: PendingEvent<T>[]): Promise<PersistedItem<T>[]> {
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
			return _.filter(newItemsDict, (item, id) => {
				return !pendingDeleteItems.has(id);
			});
		}

		return _.values(newItemsDict);
	}

	async reset(db: Db<T>, partition?: string) {
		const cursor = await this.cursor();
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
		cursor = cursor ?? (await this.cursor());

		const now = _.now();
		const writeEvents = _.map(events, ({ item, type }) => {
			const pk = this.resolvePartition(cursor, this.getItemPartition(item));
			const sk = this.getItemUniqueIdentifier(item);

			if (!sk) {
				throw new Error('Item must have an unique identifier');
			}

			const pendingItem: PendingEvent<T> = {
				cursor,
				item,
				pk,
				sk,
				ttl: Math.floor((now + 5 * 1000 * 60 * 60 * 24) / 1000), // 5 days
				type
			};

			return pendingItem;
		});

		return this.db.batchWrite(writeEvents);
	}

	async sync() {
		const cursor = await this.cursor();

		// update cursor now to ensure we don't miss any changes from now
		await this.cursor(true);

		const pendingEvents: Record<string, PendingEvent<T>[]> = {};

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

		return {
			count
		};
	}
}

export { PendingEvent };
export default Layer;
