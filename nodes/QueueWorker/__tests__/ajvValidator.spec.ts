import { AjvValidator } from '../validator/ajvValidator';

describe('AjvValidator', () => {
	let validator: AjvValidator;

	beforeEach(() => {
		validator = new AjvValidator();
	});

	const sampleSchema = {
		type: 'object',
		properties: {
			author: { type: 'string' },
			title: { type: 'string' },
			pages: { type: 'integer', minimum: 1 },
		},
		required: ['author', 'title'],
	};

	test('should validate a correct payload successfully', () => {
		const payload = {
			author: 'Chellapandi',
			title: 'Queue Worker Node Dev Guide',
			pages: 150,
		};

		const result = validator.validate(payload, sampleSchema);

		expect(result.valid).toBe(true);
		expect(result.payload).toEqual(payload);
		expect(result.errors).toBeUndefined();
	});

	test('should return validation errors for missing required fields', () => {
		const payload = {
			pages: 150,
		};

		const result = validator.validate(payload, sampleSchema);

		expect(result.valid).toBe(false);
		expect(result.errors).toBeDefined();
		expect(result.errors?.length).toBeGreaterThanOrEqual(1);

		const fieldErrors = result.errors?.map((e) => e.field);
		expect(fieldErrors).toContain('author');
		expect(fieldErrors).toContain('title');
	});

	test('should return validation errors for incorrect data types', () => {
		const payload = {
			author: 'Chellapandi',
			title: 'Queue Worker Node Dev Guide',
			pages: -10, // Invalid: minimum is 1
		};

		const result = validator.validate(payload, sampleSchema);

		expect(result.valid).toBe(false);
		expect(result.errors?.length).toBe(1);
		expect(result.errors?.[0].field).toBe('pages');
		expect(result.errors?.[0].message).toContain('must be >= 1');
	});

	test('should handle stringified JSON schemas correctly', () => {
		const payload = {
			author: 'Chellapandi',
			title: 'Queue Worker Node Dev Guide',
		};
		const stringifiedSchema = JSON.stringify(sampleSchema);

		const result = validator.validate(payload, stringifiedSchema);

		expect(result.valid).toBe(true);
	});

	test('should return error for invalid JSON schema string', () => {
		const payload = { author: 'Chellapandi' };
		const malformedSchema = '{ invalid json ';

		const result = validator.validate(payload, malformedSchema);

		expect(result.valid).toBe(false);
		expect(result.errors?.[0].field).toBe('schema');
		expect(result.errors?.[0].message).toContain('Failed to parse JSON Schema');
	});
});
