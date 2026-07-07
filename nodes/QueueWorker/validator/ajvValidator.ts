import Ajv from 'ajv';
import { ValidationResult } from '../types';

export class AjvValidator {
	private ajv: Ajv;

	constructor() {
		this.ajv = new Ajv({
			allErrors: true,
			allowUnionTypes: true,
			strict: false,
		});
	}

	/**
	 * Validates a payload against a JSON schema.
	 * @param payload The data to validate
	 * @param schema The JSON Schema as a parsed object or JSON string
	 */
	validate(payload: unknown, schema: object | string): ValidationResult {
		let schemaObj: object;
		
		if (typeof schema === 'string') {
			try {
				schemaObj = JSON.parse(schema);
			} catch (err: any) {
				return {
					valid: false,
					errors: [
						{
							field: 'schema',
							message: `Failed to parse JSON Schema: ${err.message}`,
						},
					],
				};
			}
		} else {
			schemaObj = schema;
		}

		try {
			// Compile and validate
			const validateFn = this.ajv.compile(schemaObj);
			const valid = validateFn(payload);

			if (valid) {
				return {
					valid: true,
					payload,
				};
			} else {
				const errors = (validateFn.errors || []).map((err) => {
					// Clean up the field name (instancePath) for readability
					const field = err.instancePath
						? err.instancePath.replace(/^\//, '').replace(/\//g, '.')
						: err.params.missingProperty
						? err.params.missingProperty
						: 'payload';

					return {
						field,
						message: err.message || 'Validation error',
					};
				});

				return {
					valid: false,
					errors,
				};
			}
		} catch (err: any) {
			return {
				valid: false,
				errors: [
					{
						field: 'validator',
						message: `Schema compilation or validation failed: ${err.message}`,
					},
				],
			};
		}
	}
}
