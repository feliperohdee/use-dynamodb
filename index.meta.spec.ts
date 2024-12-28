import _ from 'lodash';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, Mock, vi } from 'vitest';

import Db from './index';

type Item = {
	foo: string;
	gsiPk: string;
	gsiSk: string;
	lsiSk: string;
	pk: string;
	sk: string;
};

const createItems = (count: number) => {
	return _.times(count, index => {
		return {
			bar: `bar-${index}`,
			foo: `foo-${index}`,
			sk: `sk-${index}`,
			pk: `pk-${index % 2}`
		};
	});
};

const factory = (metaAttributes?: Record<string, string[] | { attributes: string[]; joiner: string }>) => {
	return new Db<Item>({
		accessKeyId: process.env.AWS_ACCESS_KEY || '',
		metaAttributes: metaAttributes ?? {
			'pk-bar': {
				attributes: ['pk', 'bar'],
				joiner: '#',
				transform: (key, value) => {
					if (key === 'bar') {
						return _.snakeCase(value);
					}
				}
			}
		},
		region: process.env.AWS_REGION || '',
		schema: {
			partition: 'pk',
			sort: 'sk'
		},
		secretAccessKey: process.env.AWS_SECRET_KEY || '',
		table: 'use-dynamodb-spec'
	});
};

describe('/index.ts', () => {
	let db: Db<Item>;

	beforeAll(async () => {
		db = factory();

		await db.createTable();
	});

	beforeEach(() => {
		db = factory();
	});

	describe('constructor', () => {
		it('should set metaAttributes', () => {
			const db = factory({ 'pk-bar': ['pk', 'bar'] });

			expect(db.metaAttributes['pk-bar']).toEqual({ attributes: ['pk', 'bar'], joiner: '#' });
			expect(db.metaAttributes['pk-bar'].joiner).toEqual('#');
		});
	});

	describe('batchGet / batchWrite', () => {
		afterEach(async () => {
			await db.clear();
		});

		it('should batch write and batch get', async () => {
			const batchWriteItems = await db.batchWrite(createItems(2));
			expect(
				_.every(batchWriteItems, item => {
					return _.isNumber(item.__ts);
				})
			).toBeTruthy();

			const batchGetItems = await db.batchGet(batchWriteItems);

			batchGetItems.sort((a, b) => {
				return a.sk.localeCompare(b.sk);
			});

			expect(batchGetItems).toHaveLength(2);
			expect(batchGetItems[0]['pk-bar']).toEqual('pk-0#bar_0');
			expect(batchGetItems[1]['pk-bar']).toEqual('pk-1#bar_1');
		});
	});

	describe('generateMetaAttributes', () => {
		it('should generate', () => {
			const item = {
				bar: 'bar-0',
				foo: 'foo-0',
				pk: 'pk-0',
				sk: 'sk-0'
			};

			// @ts-expect-error
			const metaAttributes = db.generateMetaAttributes(item);

			expect(metaAttributes['pk-bar']).toEqual('pk-0#bar_0');
		});

		it('should generate partial', () => {
			const item = {
				pk: 'pk-0',
				sk: 'sk-0'
			};

			// @ts-expect-error
			const metaAttributes = db.generateMetaAttributes(item);

			expect(metaAttributes['pk-bar']).toEqual('pk-0');
		});
	});

	describe('put', () => {
		afterEach(async () => {
			await db.clear();
		});

		it('should put item', async () => {
			const res = await db.put({
				bar: 'bar-0',
				foo: 'foo-0',
				pk: 'pk-0',
				sk: 'sk-0'
			});

			expect(res['pk-bar']).toEqual('pk-0#bar_0');
		});
	});

	describe('replace', () => {
		afterEach(async () => {
			await db.clear();
		});

		it('should replace item', async () => {
			const replaceItem = await db.put({
				bar: 'bar-0',
				foo: 'foo-0',
				pk: 'pk-0',
				sk: 'sk-0'
			});

			const res = await db.replace(
				{
					bar: 'bar-1',
					foo: 'foo-1',
					pk: 'pk-0',
					sk: 'sk-1'
				},
				replaceItem
			);

			expect(res['pk-bar']).toEqual('pk-0#bar_1');
		});
	});

	describe('update', () => {
		beforeEach(async () => {
			await db.put({
				bar: 'bar-0',
				foo: 'foo-0',
				pk: 'pk-0',
				sk: 'sk-0'
			});

			vi.spyOn(db.client, 'send');
		});

		afterEach(async () => {
			await db.clear();
		});

		describe('updateExpression', () => {
			it('should not update meta', async () => {
				const res = await db.update({
					attributeNames: { '#foo': 'foo' },
					attributeValues: { ':foo': 'foo-1' },
					filter: {
						item: { pk: 'pk-0', sk: 'sk-0' }
					},
					updateExpression: 'SET #foo = :foo'
				});

				// get / update
				expect(db.client.send).toHaveBeenCalledTimes(2);
				expect(res['pk-bar']).toEqual('pk-0#bar_0');
			});

			it('should not update meta if have settled all meta attributes', async () => {
				const res = await db.update({
					attributeNames: {
						'#bar': 'bar',
						'#pk_bar': 'pk-bar'
					},
					attributeValues: {
						':bar': 'bar-1',
						':pk_bar': 'pk-0#bar-1'
					},
					filter: {
						item: { pk: 'pk-0', sk: 'sk-0' }
					},
					updateExpression: 'SET #bar = :bar, #pk_bar = :pk_bar'
				});

				// get / update
				expect(db.client.send).toHaveBeenCalledTimes(2);
				expect(res['pk-bar']).toEqual('pk-0#bar-1');
			});

			it('should update meta', async () => {
				const res = await db.update({
					attributeNames: { '#bar': 'bar' },
					attributeValues: { ':bar': 'bar-1' },
					filter: {
						item: { pk: 'pk-0', sk: 'sk-0' }
					},
					updateExpression: 'SET #bar = :bar'
				});

				// get / update
				expect(db.client.send).toHaveBeenCalledTimes(3);
				expect(db.client.send).toHaveBeenCalledWith(
					expect.objectContaining({
						input: expect.objectContaining({
							ExpressionAttributeNames: { '#pk_bar': 'pk-bar' },
							ExpressionAttributeValues: { ':pk_bar': 'pk-0#bar_1' },
							Key: { pk: 'pk-0', sk: 'sk-0' },
							ReturnValues: 'ALL_NEW',
							TableName: 'use-dynamodb-spec',
							UpdateExpression: 'SET #pk_bar = :pk_bar'
						})
					})
				);

				expect(res['pk-bar']).toEqual('pk-0#bar_1');
			});
		});

		describe('updateFunction', () => {
			beforeEach(() => {
				vi.spyOn(db, 'put');
			});

			it('should not update meta', async () => {
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

				// get / update
				expect(db.client.send).toHaveBeenCalledTimes(2);
				expect(db.put).toHaveBeenCalledWith(
					{
						__createdAt: expect.any(String),
						__ts: expect.any(Number),
						__updatedAt: expect.any(String),
						bar: 'bar-0',
						foo: 'foo-1',
						pk: 'pk-0',
						sk: 'sk-0'
					},
					{
						attributeNames: {
							'#__pk': 'pk',
							'#__ts': '__ts'
						},
						attributeValues: {
							':__curr_ts': expect.any(Number)
						},
						conditionExpression: '(attribute_exists(#__pk) AND #__ts = :__curr_ts)',
						overwrite: true,
						useCurrentCreatedAtIfExists: true
					}
				);

				expect(res['pk-bar']).toEqual('pk-0#bar_0');
			});

			it('should update meta', async () => {
				const res = await db.update({
					filter: {
						item: { pk: 'pk-0' }
					},
					updateFunction: item => {
						return {
							...item,
							bar: 'bar-1'
						};
					}
				});

				// get / update
				expect(db.client.send).toHaveBeenCalledTimes(2);
				expect(db.put).toHaveBeenCalledWith(
					{
						__createdAt: expect.any(String),
						__ts: expect.any(Number),
						__updatedAt: expect.any(String),
						bar: 'bar-1',
						foo: 'foo-0',
						pk: 'pk-0',
						sk: 'sk-0'
					},
					{
						attributeNames: {
							'#__pk': 'pk',
							'#__ts': '__ts'
						},
						attributeValues: {
							':__curr_ts': expect.any(Number)
						},
						conditionExpression: '(attribute_exists(#__pk) AND #__ts = :__curr_ts)',
						overwrite: true,
						useCurrentCreatedAtIfExists: true
					}
				);

				expect(res['pk-bar']).toEqual('pk-0#bar_1');
			});
		});
	});
});
