import { describe, expect, it } from 'vitest';

import { concatConditionExpression, concatUpdateExpression } from './expressions-helper';

describe('/expressions-helper.ts', () => {
	describe('concatConditionExpression', () => {
		it('should works', () => {
			expect(concatConditionExpression('a  ', '  b')).toEqual('a AND b');
			expect(concatConditionExpression('a  ', '  OR b')).toEqual('a OR b');
			expect(concatConditionExpression('a  ', '')).toEqual('a');
		});
	});

	describe('concatUpdateExpression', () => {
		it('should works', () => {
			expect(concatUpdateExpression('#a = :a,', '')).toEqual('SET #a = :a');
			expect(concatUpdateExpression('#a = :a,', 'b = :b')).toEqual('SET #a = :a, b = :b');
			expect(concatUpdateExpression('SET #a = :a,', 'SET b = :b,c = :c,')).toEqual('SET #a = :a, b = :b, c = :c');
			expect(concatUpdateExpression('SET #a = :a,', 'ADD d SET b = :b,c = :c,')).toEqual('SET #a = :a, b = :b, c = :c ADD d');
			expect(concatUpdateExpression('SET #A = :A,', 'ADD D SET B = :B,C = :C,')).toEqual('SET #A = :A, B = :B, C = :C ADD D');
		});
	});
});
