import _ from 'lodash';

const concatConditionExpression = (exp1: string, exp2: string): string => {
	const JOINERS = ['AND', 'OR'];

	// Trim both expressions
	const trimmedExp1 = _.trim(exp1);
	const trimmedExp2 = _.trim(exp2);

	// If either expression is empty, return the other one
	if (!trimmedExp1) {
		return trimmedExp2;
	}

	if (!trimmedExp2) {
		return trimmedExp1;
	}

	// Check if exp2 starts with a joiner
	const startsWithJoiner = _.some(JOINERS, joiner => {
		return _.startsWith(trimmedExp2, joiner);
	});

	// Concatenate expressions
	const concatenated = startsWithJoiner ? `${trimmedExp1} ${trimmedExp2}` : `${trimmedExp1} AND ${trimmedExp2}`;

	// Remove leading 'AND' or 'OR' and trim
	return _.trim(_.replace(concatenated, /^\s+(AND|OR)\s+/, ''));
};

const concatUpdateExpression = (exp1: string, exp2: string): string => {
	const TRIM = ', ';
	const sections = ['SET', 'ADD', 'DELETE', 'REMOVE'];

	// Helper function to parse an expression into sections
	const parseExpression = (exp: string): { [key: string]: string[] } => {
		const result: { [key: string]: string[] } = {};
		let currentSection = 'SET'; // Default section

		// Split by space and process each part
		const parts = exp.split(' ');
		let currentItems: string[] = [];

		for (let i = 0; i < _.size(parts); i++) {
			const part = parts[i];

			if (sections.includes(part)) {
				// If we encounter a new section, save current items and switch section
				if (_.size(currentItems) > 0) {
					result[currentSection] = (result[currentSection] || []).concat(currentItems);
				}

				currentSection = part;
				currentItems = [];
			} else if (part.trim()) {
				currentItems.push(part);
			}
		}

		// Don't forget to save the last section
		if (_.size(currentItems) > 0) {
			result[currentSection] = (result[currentSection] || []).concat(currentItems);
		}

		return result;
	};

	// Parse both expressions
	const parsed1 = parseExpression(_.trim(exp1, TRIM));
	const parsed2 = parseExpression(_.trim(exp2, TRIM));

	// Merge the sections
	const mergedSections: { [key: string]: string[] } = {};

	_.forEach(sections, section => {
		const items1 = parsed1[section] || [];
		const items2 = parsed2[section] || [];

		if (_.size(items1) || _.size(items2)) {
			// Join items with commas and split again to handle cases where items contain multiple assignments
			const combinedItems = [...items1.join(' ').split(','), ...items2.join(' ').split(',')]
				.map(item => {
					return _.trim(item, TRIM);
				})
				.filter(Boolean);

			mergedSections[section] = _.uniq(combinedItems);
		}
	});

	// Build the final expression
	return sections
		.map(section => {
			if (_.size(mergedSections[section])) {
				return `${section} ${mergedSections[section].join(', ')}`;
			}
			return '';
		})
		.filter(Boolean)
		.join(' ');
};

export { concatConditionExpression, concatUpdateExpression };
