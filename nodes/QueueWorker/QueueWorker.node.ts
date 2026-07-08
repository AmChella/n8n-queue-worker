import {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
} from 'n8n-workflow';
import { QueueFactory } from './QueueFactory';
import { AjvValidator } from './validator/ajvValidator';
import { getPackageVersion } from './utils';

export class QueueWorker implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Queue Worker',
		name: 'queueWorker',
		icon: 'fa:exchange-alt',
		group: ['transform'],
		version: 1,
		description: 'Publish, validate, and acknowledge messages in RabbitMQ, Kafka, or SQS',
		defaults: {
			name: 'Queue Worker',
		},
		inputs: ['main'],
		outputs: ['main'],
		credentials: [
			{
				name: 'rabbitMq',
				required: true,
				displayOptions: {
					show: {
						provider: ['rabbitmq'],
					},
				},
			},
			{
				name: 'kafka',
				required: true,
				displayOptions: {
					show: {
						provider: ['kafka'],
					},
				},
			},
			{
				name: 'awsSqs',
				required: true,
				displayOptions: {
					show: {
						provider: ['sqs'],
					},
				},
			},
		],
		properties: [
			{
				displayName: `App Version: ${getPackageVersion()}`,
				name: 'appVersionNotice',
				type: 'notice',
				default: '',
			},
			{
				displayName: 'Queue Provider',
				name: 'provider',
				type: 'options',
				options: [
					{
						name: 'RabbitMQ',
						value: 'rabbitmq',
					},
					{
						name: 'Apache Kafka',
						value: 'kafka',
					},
					{
						name: 'AWS SQS',
						value: 'sqs',
					},
				],
				default: 'rabbitmq',
				description: 'Select the queue provider to interact with',
			},
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				options: [
					{
						name: 'Publish Message',
						value: 'publish',
						description: 'Publish a message to a queue or topic',
					},
					{
						name: 'Validate Message',
						value: 'validate',
						description: 'Validate message payload using JSON Schema',
					},
					{
						name: 'Ack Message',
						value: 'ack',
						description: 'Manually acknowledge a consumed message',
					},
					{
						name: 'Nack Message',
						value: 'nack',
						description: 'Reject a consumed message (optional DLQ or retry)',
					},
					{
						name: 'Send to DLQ',
						value: 'sendToDlq',
						description: 'Explicitly route a message to the Dead Letter Queue',
					},
				],
				default: 'publish',
				description: 'Operation to perform',
			},

			// --- Publish Fields ---
			{
				displayName: 'Queue / Topic Name',
				name: 'queueName',
				type: 'string',
				default: '',
				required: true,
				displayOptions: {
					show: {
						operation: ['publish', 'sendToDlq'],
					},
				},
				description: 'Name of the queue or topic to publish/send to',
			},
			{
				displayName: 'Message Payload',
				name: 'payload',
				type: 'json',
				default: '{}',
				required: true,
				displayOptions: {
					show: {
						operation: ['publish', 'validate', 'sendToDlq'],
					},
				},
				description: 'The JSON message payload',
			},
			{
				displayName: 'Metadata & Headers',
				name: 'options',
				type: 'collection',
				placeholder: 'Add Metadata',
				default: {},
				displayOptions: {
					show: {
						operation: ['publish', 'sendToDlq'],
					},
				},
				options: [
					{
						displayName: 'Correlation ID',
						name: 'correlationId',
						type: 'string',
						default: '',
						description: 'Correlation ID for tracking/tracing',
					},
					{
						displayName: 'Reply To',
						name: 'replyTo',
						type: 'string',
						default: '',
						description: 'Queue or topic name to send replies to',
					},
					{
						displayName: 'Tenant',
						name: 'tenant',
						type: 'string',
						default: '',
						description: 'Tenant identifier',
					},
					{
						displayName: 'Workflow Name',
						name: 'workflow',
						type: 'string',
						default: '',
						description: 'Associated workflow name',
					},
					{
						displayName: 'Schema Name',
						name: 'schema',
						type: 'string',
						default: '',
						description: 'JSON Schema identifier',
					},
				],
			},

			// --- Validate Fields ---
			{
				displayName: 'JSON Schema Source',
				name: 'schemaSource',
				type: 'options',
				options: [
					{
						name: 'Parameter',
						value: 'parameter',
					},
				],
				default: 'parameter',
				displayOptions: {
					show: {
						operation: ['validate'],
					},
				},
			},
			{
				displayName: 'JSON Schema',
				name: 'jsonSchema',
				type: 'json',
				default: '{}',
				required: true,
				displayOptions: {
					show: {
						operation: ['validate'],
						schemaSource: ['parameter'],
					},
				},
				description: 'The JSON Schema against which to validate the payload',
			},

			// --- Ack / Nack Fields ---
			{
				displayName: 'Message Object',
				name: 'messageObject',
				type: 'json',
				default: '',
				required: true,
				displayOptions: {
					show: {
						operation: ['ack', 'nack'],
					},
				},
				description: 'The complete message object returned by the Queue Worker Trigger node',
			},
			{
				displayName: 'Dead Letter Queue (Nack Option)',
				name: 'deadLetterQueue',
				type: 'string',
				default: '',
				displayOptions: {
					show: {
						operation: ['nack'],
					},
				},
				description: 'If specified, rejects the message by publishing it to this DLQ and acking the original',
			},
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		const returnData: INodeExecutionData[] = [];
		const provider = this.getNodeParameter('provider', 0) as string;
		const operation = this.getNodeParameter('operation', 0) as string;

		let credentials: any = {};
		if (operation !== 'validate') {
			let credentialName = 'rabbitMq';
			if (provider === 'kafka') credentialName = 'kafka';
			if (provider === 'sqs') credentialName = 'awsSqs';

			credentials = await this.getCredentials(credentialName);
		}

		for (let i = 0; i < items.length; i++) {
			try {
				if (operation === 'publish' || operation === 'sendToDlq') {
					const queueName = this.getNodeParameter('queueName', i) as string;
					const rawPayload = this.getNodeParameter('payload', i);
					const payload = typeof rawPayload === 'string' ? JSON.parse(rawPayload) : rawPayload;
					const options = this.getNodeParameter('options', i, {}) as any;

					const adapter = QueueFactory.getAdapter(provider, credentials, {});
					await adapter.connect();
					await adapter.publish(queueName, payload, options);
					await adapter.disconnect();

					returnData.push({
						json: {
							success: true,
							provider,
							queueName,
							options,
						},
						pairedItem: i,
					});
				} else if (operation === 'validate') {
					const rawPayload = this.getNodeParameter('payload', i);
					const payload = typeof rawPayload === 'string' ? JSON.parse(rawPayload) : rawPayload;
					
					const rawSchema = this.getNodeParameter('jsonSchema', i);
					const schema = typeof rawSchema === 'string' ? JSON.parse(rawSchema) : rawSchema;

					const validator = new AjvValidator();
					const result = validator.validate(payload, schema);

					returnData.push({
						json: result as any,
						pairedItem: i,
					});
				} else if (operation === 'ack' || operation === 'nack') {
					const rawMessage = this.getNodeParameter('messageObject', i);
					const message = typeof rawMessage === 'string' ? JSON.parse(rawMessage) : rawMessage;

					if (!message || !message.raw) {
						throw new Error('Invalid message object. Must contain the original "raw" message structure.');
					}

					let deadLetterQueue: string | undefined;
					if (operation === 'nack') {
						deadLetterQueue = this.getNodeParameter('deadLetterQueue', i, '') as string;
					}

					const adapterOptions: any = {};
					if (message.metadata?.topic) {
						adapterOptions.queueName = message.metadata.topic; // Kafka
					} else if (message.metadata?.headers?.originalQueue) {
						adapterOptions.queueName = message.metadata.headers.originalQueue;
					} else {
						// For SQS and RabbitMQ, we resolve from active params or headers
						adapterOptions.queueName = message.raw?.fields?.routingKey || '';
					}

					if (deadLetterQueue) {
						adapterOptions.deadLetterQueue = deadLetterQueue;
					}

					const adapter = QueueFactory.getAdapter(provider, credentials, adapterOptions);
					await adapter.connect();

					if (operation === 'ack') {
						await adapter.ack(message);
					} else {
						await adapter.nack(message);
					}

					await adapter.disconnect();

					returnData.push({
						json: {
							success: true,
							provider,
							operation,
							messageId: message.messageId,
						},
						pairedItem: i,
					});
				}
			} catch (error: any) {
				if (this.continueOnFail()) {
					returnData.push({
						json: {
							error: error.message,
						},
						pairedItem: i,
					});
				} else {
					throw error;
				}
			}
		}

		return [returnData];
	}
}
