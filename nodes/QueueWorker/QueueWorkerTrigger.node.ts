import {
	ITriggerFunctions,
	ITriggerResponse,
	INodeType,
	INodeTypeDescription,
} from 'n8n-workflow';
import { QueueFactory } from './QueueFactory';
import { AjvValidator } from './validator/ajvValidator';

export class QueueWorkerTrigger implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Queue Worker Trigger',
		name: 'queueWorkerTrigger',
		icon: 'fa:bolt',
		group: ['trigger'],
		version: 1,
		description: 'Continuously consume and validate messages from RabbitMQ, Kafka, or SQS',
		defaults: {
			name: 'Queue Worker Trigger',
		},
		inputs: [],
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
				description: 'Select the queue provider to listen to',
			},
			{
				displayName: 'Queue / Topic Name',
				name: 'queueName',
				type: 'string',
				default: '',
				required: true,
				description: 'Name of the queue or topic to consume from',
			},
			{
				displayName: 'Auto ACK',
				name: 'autoAck',
				type: 'boolean',
				default: false,
				description: 'Whether to automatically acknowledge messages when they are received',
			},
			{
				displayName: 'Prefetch Limit',
				name: 'prefetch',
				type: 'number',
				default: 10,
				displayOptions: {
					show: {
						provider: ['rabbitmq'],
					},
				},
				description: 'Number of messages to prefetch from RabbitMQ',
			},
			{
				displayName: 'Dead Letter Queue',
				name: 'deadLetterQueue',
				type: 'string',
				default: '',
				description: 'DLQ queue/topic name to route rejected messages',
			},
			{
				displayName: 'JSON Schema Validation',
				name: 'schemaSource',
				type: 'options',
				options: [
					{
						name: 'None',
						value: 'none',
					},
					{
						name: 'Parameter Schema',
						value: 'parameter',
					},
				],
				default: 'none',
				description: 'Whether to validate message payloads against a JSON schema',
			},
			{
				displayName: 'JSON Schema',
				name: 'jsonSchema',
				type: 'json',
				default: '{}',
				required: true,
				displayOptions: {
					show: {
						schemaSource: ['parameter'],
					},
				},
				description: 'The JSON Schema against which to validate the payload',
			},
		],
	};

	async trigger(this: ITriggerFunctions): Promise<ITriggerResponse> {
		const provider = this.getNodeParameter('provider') as string;
		const queueName = this.getNodeParameter('queueName') as string;
		const autoAck = this.getNodeParameter('autoAck', false) as boolean;
		const prefetch = this.getNodeParameter('prefetch', 10) as number;
		const deadLetterQueue = this.getNodeParameter('deadLetterQueue', '') as string;
		const schemaSource = this.getNodeParameter('schemaSource', 'none') as string;
		
		let jsonSchema: string | undefined;
		if (schemaSource === 'parameter') {
			jsonSchema = this.getNodeParameter('jsonSchema', '') as string;
		}

		let credentialName = 'rabbitMq';
		if (provider === 'kafka') credentialName = 'kafka';
		if (provider === 'sqs') credentialName = 'awsSqs';

		const credentials = await this.getCredentials(credentialName);

		const options: any = {
			queueName,
			autoAck,
			prefetch,
			deadLetterQueue,
		};

		const adapter = QueueFactory.getAdapter(provider, credentials, options);
		await adapter.connect();

		let isRunning = true;
		const validator = new AjvValidator();

		const consumeLoop = async () => {
			while (isRunning) {
				try {
					const message = await adapter.consume();
					
					let validationResult: any = { valid: true };
					if (jsonSchema) {
						validationResult = validator.validate(message.payload, jsonSchema);
					}

					const outputData = {
						messageId: message.messageId,
						workflow: message.workflow,
						schema: message.schema,
						replyTo: message.replyTo,
						payload: message.payload,
						metadata: message.metadata,
						validation: validationResult,
						// Expose the raw message structure so manual Ack/Nack operations can reference it
						raw: message.raw,
					};

					this.emit([this.helpers.returnJsonArray(outputData)]);
				} catch (err: any) {
					// Add a delay to prevent CPU thrashing in case of persistent errors
					await new Promise((resolve) => setTimeout(resolve, 2000));
				}
			}
		};

		// Start loop asynchronously
		consumeLoop();

		const closeFunction = async () => {
			isRunning = false;
			await adapter.disconnect();
		};

		return {
			closeFunction,
		};
	}
}
