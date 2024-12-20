import _ from 'lodash';
import { ConditionalCheckFailedException } from '@aws-sdk/client-dynamodb';
import { promiseAll } from 'use-async-helpers';

import type Dynamodb from './index.js';
import type { Dict } from './index.js';

namespace Layer {
	export type Meta = {
		cursor: number;
		cursorMax: number;
		loaded: boolean;
		syncedLastTotal: number;
		syncedTimes: number;
		syncedTotal: number;
		unsyncedLastTotal: number;
		unsyncedTotal: number;
	};

	export type Getter<T extends Dynamodb.PersistedItem> = (partition: string) => Promise<T[]>;
	export type Setter<T extends Dynamodb.PersistedItem> = (partition: string, items: T[]) => Promise<void>;
	export type PendingEvent<T extends Dict = Dict> = {
		cursor: number;
		item: Dynamodb.PersistedItem<T>;
		pk: string;
		sk: string;
		ttl: number;
		type: Dynamodb.ChangeType;
	};
}

const FIVE_DAYS_IN_SECONDS = 5 * 24 * 60 * 60;
const LOCK_TTL_IN_SECONDS = 5 * 60;
const DEFAULT_CURRENT_META = {
	cursor: 0,
	cursorMax: 0,
	loaded: false,
	syncedLastTotal: 0,
	syncedTimes: 0,
	syncedTotal: 0,
	unsyncedLastTotal: 0,
	unsyncedTotal: 0
};

const layerTableOptions = ({
	accessKeyId,
	region,
	secretAccessKey,
	table
}: {
	accessKeyId: string;
	region: string;
	secretAccessKey: string;
	table: string;
}) => {
	return {
		accessKeyId,
		indexes: [
			{
				name: 'cursor-index',
				partition: 'cursor',
				partitionType: 'N' as const,
				sort: 'pk',
				sortType: 'S' as const
			}
		],
		region,
		schema: { partition: 'pk', sort: 'sk' },
		secretAccessKey,
		table
	};
};

class Layer<T extends Dict = Dict> {
	public static tableOptions = layerTableOptions;

	public backgroundRunner?: (promise: Promise<void>) => void;
	public db: Dynamodb<Layer.PendingEvent<T>>;
	public getter: Layer.Getter<Dynamodb.PersistedItem<T>>;
	public setter: Layer.Setter<Dynamodb.PersistedItem<T>>;
	public syncStrategy?: (meta: Layer.Meta) => boolean;

	private currentMeta: Layer.Meta;
	private getItemUniqueIdentifier: (item: Dynamodb.PersistedItem<T>) => string;
	private getItemPartition: (item: Dynamodb.PersistedItem<T>) => string;
	private maxParallelConcurrency: number;
	private table: string;
	private ttl: (item: Dynamodb.PersistedItem<T>) => number;

	constructor(options: {
		backgroundRunner?: (promise: Promise<void>) => void;
		db: Dynamodb<Layer.PendingEvent<T>>;
		getItemPartition?: (item: Dynamodb.PersistedItem<T>) => string;
		getItemUniqueIdentifier: (item: Dynamodb.PersistedItem<T>) => string;
		getter: Layer.Getter<Dynamodb.PersistedItem<T>>;
		maxParallelConcurrency?: number;
		setter: Layer.Setter<Dynamodb.PersistedItem<T>>;
		syncStrategy?: (meta: Layer.Meta) => boolean;
		table: string;
		ttl?: number | ((item: Dynamodb.PersistedItem<T>) => number);
	}) {
		this.currentMeta = DEFAULT_CURRENT_META;
		this.backgroundRunner = options.backgroundRunner;
		this.db = options.db;
		this.getItemUniqueIdentifier = options.getItemUniqueIdentifier;
		this.getItemPartition = options.getItemPartition ?? (() => '');
		this.getter = options.getter;
		this.maxParallelConcurrency = options.maxParallelConcurrency ?? 4;
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

		const cursorGSI = _.find(this.db.indexes, (index: Dynamodb.TableGSI) => {
			return index.partition === 'cursor' && index.partitionType === 'N' && index.sort === 'pk' && index.sortType === 'S';
		});

		if (!cursorGSI) {
			throw new Error('Dynamodb schema must have a GSI with cursor as partition key and pk as sort key');
		}
	}

	async meta(set?: { advanceCursor: 1 | 0 | -1; unsyncedTotal: number; syncedTotal: number }): Promise<Layer.Meta> {
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
				const meta = await this.db.update<Layer.Meta>({
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
			const res = await this.db.get<Layer.Meta>({
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

		return _.omit(this.currentMeta, ['pk', 'sk', '__createdAt', '__ts', '__updatedAt']) as Layer.Meta;
	}

	async get(partition?: string, sorted = true): Promise<Dynamodb.PersistedItem<T>[]> {
		const { cursor, cursorMax } = await this.meta();
		const pks = _.map(_.range(cursor, cursorMax + 1), cursor => {
			return this.resolvePartition(cursor, partition);
		});

		const pendingEvents = await promiseAll(
			_.map(pks, (pk, i) => {
				return async () => {
					const res = await this.db.query({
						item: { pk },
						limit: Infinity
					});

					return res.items;
				};
			}),
			this.maxParallelConcurrency
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

	merge(
		layerItems: Dynamodb.PersistedItem<T>[],
		pendingEvents: Dynamodb.PersistedItem<Layer.PendingEvent<T>>[]
	): Dynamodb.PersistedItem<T>[] {
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

	async reset(db: Dynamodb<T>, partition?: string) {
		const lock = await this.acquireLock(true);

		if (!lock) {
			return {
				count: 0,
				locked: true
			};
		}

		try {
			const pendingEvents: Record<string, Dynamodb.PersistedItem<T>[]> = {};

			// first clean up pending events
			await this.db.scan({
				attributeNames: { '#pk': 'pk' },
				attributeValues: { ':pk': partition ? `${this.table}#${partition}` : this.table },
				filterExpression: 'begins_with(#pk, :pk)',
				limit: Infinity,
				onChunk: async ({ items }) => {
					await this.db.batchDelete(items);
				}
			});

			await this.resetMeta();

			const { count } = await db.scan({
				limit: Infinity,
				onChunk: async ({ items }) => {
					for (const item of items) {
						const itemPartition = this.getItemPartition(item);

						if (partition && itemPartition !== partition) {
							continue;
						}

						// cursor was resetted to 0
						const pk = this.resolvePartition(0, itemPartition);

						if (!pendingEvents[pk]) {
							pendingEvents[pk] = [];
						}

						pendingEvents[pk] = [...pendingEvents[pk], item];
					}
				}
			});

			for (const pk in pendingEvents) {
				await this.setter(pk.replace('#0', ''), pendingEvents[pk]);
			}

			return {
				count,
				locked: false
			};
		} finally {
			await this.acquireLock(false);
		}
	}

	async resetMeta() {
		await this.db.delete({
			filter: {
				item: { pk: `__${this.table}__`, sk: '__meta__' }
			}
		});

		this.currentMeta = DEFAULT_CURRENT_META;
	}

	resolvePartition(cursor: number, partition?: string) {
		return _.compact([this.table, _.trim(partition), `${cursor}`]).join('#');
	}

	async set(events: Dynamodb.ChangeEvent<T>[], cursor?: number) {
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

			const pendingItem: Layer.PendingEvent<T> = {
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

			const pendingEvents: Record<string, Dynamodb.PersistedItem<Layer.PendingEvent<T>>[]> = {};
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

export { DEFAULT_CURRENT_META };
export default Layer;
