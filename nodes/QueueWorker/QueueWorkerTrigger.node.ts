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
				displayName: 'Execution Mode',
				name: 'executionMode',
				type: 'options',
				options: [
					{
						name: 'Watch Continuously',
						value: 'continuous',
						description: 'Keep the trigger active and consume messages continuously as they arrive',
					},
					{
						name: 'Scheduled Polling',
						value: 'scheduled',
						description: 'Connect to the queue periodically at scheduled intervals, consume available messages, and process them',
					},
					{
						name: 'At Once',
						value: 'once',
						description: 'Connect, consume all currently available messages (up to limits), process them immediately, and then stop',
					},
				],
				default: 'continuous',
				description: 'How the trigger should listen for and consume messages',
			},
			{
				displayName: 'Polling Interval',
				name: 'pollingInterval',
				type: 'number',
				default: 60,
				displayOptions: {
					show: {
						executionMode: ['scheduled'],
					},
				},
				description: 'How often to poll the queue',
			},
			{
				displayName: 'Interval Unit',
				name: 'intervalUnit',
				type: 'options',
				options: [
					{
						name: 'Seconds',
						value: 'seconds',
					},
					{
						name: 'Minutes',
						value: 'minutes',
					},
					{
						name: 'Hours',
						value: 'hours',
					},
				],
				default: 'seconds',
				displayOptions: {
					show: {
						executionMode: ['scheduled'],
					},
				},
				description: 'The unit of time for the polling interval',
			},
			{
				displayName: 'Max Messages per Poll',
				name: 'maxMessages',
				type: 'number',
				default: 10,
				displayOptions: {
					show: {
						executionMode: ['scheduled', 'once'],
					},
				},
				description: 'The maximum number of messages to consume in a single polling run',
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
		const executionMode = this.getNodeParameter('executionMode', 'continuous') as string;
		
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
		let isRunning = true;
		const validator = new AjvValidator();

		const processMessage = (message: any) => {
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
		};

		const runContinuous = async () => {
			while (isRunning) {
				try {
					await adapter.connect();
					while (isRunning) {
						try {
							const message = await adapter.consume();
							if (message && isRunning) {
								processMessage(message);
							}
						} catch (err: any) {
							if (isRunning) {
								// Add a delay to prevent CPU thrashing in case of persistent errors
								await new Promise((resolve) => setTimeout(resolve, 2000));
							}
						}
					}
				} catch (err) {
					if (isRunning) {
						// Wait 5 seconds before trying to reconnect on connection failures
						await new Promise((resolve) => setTimeout(resolve, 5000));
					}
				} finally {
					try {
						await adapter.disconnect();
					} catch (dErr) {
						// Ignore disconnect errors
					}
				}
			}
		};

		const runOnce = async () => {
			try {
				await adapter.connect();
				let count = 0;
				const max = this.getNodeParameter('maxMessages', 10) as number;
				while (isRunning && count < max) {
					const message = await adapter.consume(1000);
					if (!message) {
						break;
					}
					processMessage(message);
					count++;
				}
			} catch (err) {
				// Ignore/log
			} finally {
				await adapter.disconnect();
			}
		};

		const runScheduled = async () => {
			const interval = this.getNodeParameter('pollingInterval', 60) as number;
			const unit = this.getNodeParameter('intervalUnit', 'seconds') as string;
			const max = this.getNodeParameter('maxMessages', 10) as number;

			let multiplier = 1000;
			if (unit === 'minutes') multiplier = 60 * 1000;
			if (unit === 'hours') multiplier = 60 * 60 * 1000;

			const intervalMs = interval * multiplier;

			while (isRunning) {
				try {
					await adapter.connect();
					let count = 0;
					while (isRunning && count < max) {
						const message = await adapter.consume(1000);
						if (!message) {
							break;
						}
						processMessage(message);
						count++;
					}
				} catch (err) {
					// Ignore/log
				} finally {
					await adapter.disconnect();
				}

				if (isRunning) {
					await new Promise((resolve) => setTimeout(resolve, intervalMs));
				}
			}
		};

		if (executionMode === 'continuous') {
			runContinuous();
		} else if (executionMode === 'once') {
			runOnce();
		} else if (executionMode === 'scheduled') {
			runScheduled();
		}

		const closeFunction = async () => {
			isRunning = false;
			await adapter.disconnect();
		};

		return {
			closeFunction,
		};
	}
}
