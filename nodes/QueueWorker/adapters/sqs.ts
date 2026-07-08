import {
	SQSClient,
	ReceiveMessageCommand,
	SendMessageCommand,
	DeleteMessageCommand,
	ChangeMessageVisibilityCommand,
	GetQueueUrlCommand,
	Message,
} from '@aws-sdk/client-sqs';
import { QueueAdapter, QueueMessage } from '../types';

export class AwsSqsAdapter implements QueueAdapter {
	private client: SQSClient | null = null;
	private queueUrl: string | null = null;
	private messageBuffer: QueueMessage[] = [];
	private pendingResolvers: Array<(msg: QueueMessage) => void> = [];
	private isConsuming = false;
	private pollPromise: Promise<void> | null = null;

	constructor(
		private credentials: any,
		private options: {
			queueName?: string; // Queue name or full Queue URL
			autoAck?: boolean;
			visibilityTimeout?: number;
			waitTimeSeconds?: number;
			maxNumberOfMessages?: number;
			deadLetterQueue?: string; // DLQ Queue Name or URL
			[key: string]: any;
		}
	) {}

	async connect(): Promise<void> {
		const region = this.credentials.region || 'us-east-1';
		const authMethod = this.credentials.authMethod || 'credentials';

		const config: any = { region };

		if (authMethod === 'credentials') {
			config.credentials = {
				accessKeyId: this.credentials.accessKeyId,
				secretAccessKey: this.credentials.secretAccessKey,
			};
			if (this.credentials.sessionToken) {
				config.credentials.sessionToken = this.credentials.sessionToken;
			}
		}

		this.client = new SQSClient(config);

		const queueName = this.options.queueName;
		if (!queueName) {
			throw new Error('Queue Name or URL must be specified.');
		}

		if (queueName.startsWith('http://') || queueName.startsWith('https://')) {
			this.queueUrl = queueName;
		} else {
			// Resolve queue name to queue URL
			try {
				const command = new GetQueueUrlCommand({ QueueName: queueName });
				const response = await this.client.send(command);
				this.queueUrl = response.QueueUrl || null;
			} catch (err: any) {
				throw new Error(`Failed to resolve SQS Queue URL for queue name "${queueName}": ${err.message}`);
			}
		}

		if (!this.queueUrl) {
			throw new Error(`Could not resolve Queue URL for "${queueName}".`);
		}
	}

	private async startPollingLoop(): Promise<void> {
		if (!this.client || !this.queueUrl) return;

		this.isConsuming = true;

		while (this.isConsuming) {
			try {
				const command = new ReceiveMessageCommand({
					QueueUrl: this.queueUrl,
					MaxNumberOfMessages: this.options.maxNumberOfMessages || 1,
					WaitTimeSeconds: this.options.waitTimeSeconds !== undefined ? this.options.waitTimeSeconds : 10,
					VisibilityTimeout: this.options.visibilityTimeout !== undefined ? this.options.visibilityTimeout : 30,
					MessageAttributeNames: ['All'],
				});

				const response = await this.client.send(command);
				const messages = response.Messages || [];

				for (const msg of messages) {
					const queueMessage = this.mapSqsMessage(msg);

					if (this.options.autoAck) {
						await this.ack(queueMessage);
					}

					if (this.pendingResolvers.length > 0) {
						const resolve = this.pendingResolvers.shift();
						resolve!(queueMessage);
					} else {
						this.messageBuffer.push(queueMessage);
					}
				}
			} catch (err) {
				// Prevent tight-loop CPU thrashing on error
				await new Promise((r) => setTimeout(r, 2000));
			}
		}
	}

	private mapSqsMessage(msg: Message): QueueMessage {
		let payload: any;
		const body = msg.Body || '';
		try {
			payload = JSON.parse(body);
		} catch {
			payload = body;
		}

		const attribs = msg.MessageAttributes || {};
		const workflow = attribs.workflow?.StringValue;
		const schema = attribs.schema?.StringValue;
		const replyTo = attribs.replyTo?.StringValue;

		const tenant = attribs.tenant?.StringValue;
		const correlationId = attribs.correlationId?.StringValue;

		const headers: Record<string, any> = {};
		for (const [key, value] of Object.entries(attribs)) {
			headers[key] = value.StringValue;
		}

		return {
			messageId: msg.MessageId || String(Date.now()),
			workflow,
			schema,
			replyTo,
			payload,
			metadata: {
				tenant,
				correlationId,
				headers,
			},
			raw: {
				ReceiptHandle: msg.ReceiptHandle,
				MessageId: msg.MessageId,
			},
		};
	}

	async consume(timeoutMs?: number): Promise<QueueMessage | null> {
		if (!this.client || !this.queueUrl) {
			throw new Error('SQS client not initialized. Call connect() first.');
		}

		if (!this.isConsuming) {
			this.pollPromise = this.startPollingLoop();
		}

		if (this.messageBuffer.length > 0) {
			return this.messageBuffer.shift()!;
		}

		if (timeoutMs !== undefined) {
			return new Promise<QueueMessage | null>((resolve) => {
				let timer: NodeJS.Timeout | null = setTimeout(() => {
					const index = this.pendingResolvers.indexOf(resolveWrapper);
					if (index !== -1) {
						this.pendingResolvers.splice(index, 1);
					}
					resolve(null);
				}, timeoutMs);

				const resolveWrapper = (msg: QueueMessage) => {
					if (timer) {
						clearTimeout(timer);
						timer = null;
					}
					resolve(msg);
				};

				this.pendingResolvers.push(resolveWrapper);
			});
		}

		return new Promise<QueueMessage | null>((resolve) => {
			this.pendingResolvers.push(resolve);
		});
	}

	async publish(queueOrTopic: string, payload: unknown, options?: Record<string, any>): Promise<void> {
		if (!this.client) {
			throw new Error('SQS client not initialized. Call connect() first.');
		}

		let targetQueueUrl = queueOrTopic;
		if (!queueOrTopic.startsWith('http://') && !queueOrTopic.startsWith('https://')) {
			const command = new GetQueueUrlCommand({ QueueName: queueOrTopic });
			const response = await this.client.send(command);
			targetQueueUrl = response.QueueUrl || '';
		}

		const messageBody = typeof payload === 'string' ? payload : JSON.stringify(payload);

		const messageAttributes: Record<string, any> = {};
		if (options?.workflow) {
			messageAttributes.workflow = { DataType: 'String', StringValue: options.workflow };
		}
		if (options?.schema) {
			messageAttributes.schema = { DataType: 'String', StringValue: options.schema };
		}
		if (options?.tenant) {
			messageAttributes.tenant = { DataType: 'String', StringValue: options.tenant };
		}
		if (options?.replyTo) {
			messageAttributes.replyTo = { DataType: 'String', StringValue: options.replyTo };
		}
		if (options?.correlationId) {
			messageAttributes.correlationId = { DataType: 'String', StringValue: options.correlationId };
		}

		if (options?.headers) {
			for (const [k, v] of Object.entries(options.headers)) {
				messageAttributes[k] = { DataType: 'String', StringValue: String(v) };
			}
		}

		const command = new SendMessageCommand({
			QueueUrl: targetQueueUrl,
			MessageBody: messageBody,
			MessageAttributes: messageAttributes,
		});

		await this.client.send(command);
	}

	async ack(message: QueueMessage): Promise<void> {
		if (!this.client || !this.queueUrl) {
			throw new Error('SQS client not initialized.');
		}

		const receiptHandle = message.raw?.ReceiptHandle;
		if (!receiptHandle) return;

		const command = new DeleteMessageCommand({
			QueueUrl: this.queueUrl,
			ReceiptHandle: receiptHandle,
		});

		await this.client.send(command);
	}

	async nack(message: QueueMessage): Promise<void> {
		if (!this.client || !this.queueUrl) {
			throw new Error('SQS client not initialized.');
		}

		const receiptHandle = message.raw?.ReceiptHandle;
		if (!receiptHandle) return;

		// SQS "Nack": Reset visibility timeout to 0, making the message instantly retriable by other consumers
		// If a DLQ is specified and we want client-side routing:
		if (this.options.deadLetterQueue) {
			let dlqUrl = this.options.deadLetterQueue;
			if (!dlqUrl.startsWith('http://') && !dlqUrl.startsWith('https://')) {
				const getUrlCmd = new GetQueueUrlCommand({ QueueName: dlqUrl });
				const res = await this.client.send(getUrlCmd);
				dlqUrl = res.QueueUrl || '';
			}

			// Publish to DLQ
			await this.publish(dlqUrl, message.payload, {
				correlationId: message.metadata.correlationId,
				workflow: message.workflow,
				schema: message.schema,
				tenant: message.metadata.tenant,
				headers: {
					...message.metadata.headers,
					nackReason: 'Manual nack / DLQ routing',
				},
			});

			// Ack/delete original message
			await this.ack(message);
		} else {
			// standard nack: make immediately visible
			const command = new ChangeMessageVisibilityCommand({
				QueueUrl: this.queueUrl,
				ReceiptHandle: receiptHandle,
				VisibilityTimeout: 0,
			});
			await this.client.send(command);
		}
	}

	async disconnect(): Promise<void> {
		this.isConsuming = false;
		this.pendingResolvers = [];
		this.messageBuffer = [];
		
		if (this.pollPromise) {
			try {
				await this.pollPromise;
			} catch {
				// ignore
			}
			this.pollPromise = null;
		}

		if (this.client) {
			this.client.destroy();
			this.client = null;
		}
	}
}
