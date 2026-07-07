import * as amqp from 'amqplib';
import { QueueAdapter, QueueMessage } from '../types';

export class RabbitMqAdapter implements QueueAdapter {
	private connection: amqp.Connection | null = null;
	private channel: amqp.Channel | null = null;
	private messageBuffer: QueueMessage[] = [];
	private pendingResolvers: Array<(msg: QueueMessage) => void> = [];
	private isConsuming = false;

	constructor(
		private credentials: any,
		private options: {
			queueName?: string;
			prefetch?: number;
			autoAck?: boolean;
			deadLetterQueue?: string;
			[key: string]: any;
		}
	) {}

	async connect(): Promise<void> {
		const host = this.credentials.host || 'localhost';
		const port = this.credentials.port || 5672;
		const user = this.credentials.user || 'guest';
		const password = this.credentials.password || 'guest';
		const vhost = this.credentials.vhost || '/';
		const ssl = this.credentials.ssl ? 's' : '';

		const protocol = `amqp${ssl}`;
		// URL encode credentials to prevent parsing errors
		const encodedUser = encodeURIComponent(user);
		const encodedPassword = encodeURIComponent(password);
		const encodedVhost = vhost === '/' ? '' : encodeURIComponent(vhost);

		const url = `${protocol}://${encodedUser}:${encodedPassword}@${host}:${port}/${encodedVhost}`;

		this.connection = await amqp.connect(url);
		this.channel = await this.connection.createChannel();

		// Configure prefetch if specified
		const prefetch = this.options.prefetch || 10;
		await this.channel.prefetch(prefetch);

		// Assert queue is present if provided
		if (this.options.queueName) {
			const queueArgs: amqp.Options.AssertQueue = {};
			if (this.options.deadLetterQueue) {
				queueArgs.deadLetterExchange = '';
				queueArgs.deadLetterRoutingKey = this.options.deadLetterQueue;
			}
			await this.channel.assertQueue(this.options.queueName, {
				durable: true,
				arguments: queueArgs,
			});
		}
	}

	async consume(): Promise<QueueMessage> {
		if (!this.channel) {
			throw new Error('RabbitMQ channel not initialized. Call connect() first.');
		}

		const queueName = this.options.queueName;
		if (!queueName) {
			throw new Error('Queue name must be specified to consume.');
		}

		if (!this.isConsuming) {
			this.isConsuming = true;
			await this.channel.consume(
				queueName,
				(msg) => {
					if (!msg) return; // Consumer cancelled by RabbitMQ

					let payload: any;
					const contentStr = msg.content.toString();
					try {
						payload = JSON.parse(contentStr);
					} catch {
						payload = contentStr; // Fallback to raw string
					}

					const correlationId = msg.properties.correlationId;
					const replyTo = msg.properties.replyTo;
					const headers = msg.properties.headers || {};
					const messageId = msg.properties.messageId || String(Date.now());

					const queueMessage: QueueMessage = {
						messageId,
						workflow: headers.workflow,
						schema: headers.schema,
						replyTo,
						payload,
						metadata: {
							tenant: headers.tenant,
							correlationId,
							headers,
						},
						raw: msg,
					};

					if (this.options.autoAck) {
						this.channel?.ack(msg);
					}

					if (this.pendingResolvers.length > 0) {
						const resolve = this.pendingResolvers.shift();
						resolve!(queueMessage);
					} else {
						this.messageBuffer.push(queueMessage);
					}
				},
				{ noAck: !!this.options.autoAck }
			);
		}

		// Return next message from buffer, or wait for next message
		if (this.messageBuffer.length > 0) {
			return this.messageBuffer.shift()!;
		}

		return new Promise<QueueMessage>((resolve) => {
			this.pendingResolvers.push(resolve);
		});
	}

	async publish(queueOrTopic: string, payload: unknown, options?: Record<string, any>): Promise<void> {
		if (!this.channel) {
			throw new Error('RabbitMQ channel not initialized. Call connect() first.');
		}

		// Ensure queue exists
		await this.channel.assertQueue(queueOrTopic, { durable: true });

		const buffer = Buffer.from(
			typeof payload === 'string' ? payload : JSON.stringify(payload)
		);

		const publishOptions: amqp.Options.Publish = {
			persistent: true,
		};

		if (options?.correlationId) {
			publishOptions.correlationId = options.correlationId;
		}
		if (options?.replyTo) {
			publishOptions.replyTo = options.replyTo;
		}

		// Build headers
		const headers: Record<string, any> = {};
		if (options?.workflow) headers.workflow = options.workflow;
		if (options?.schema) headers.schema = options.schema;
		if (options?.tenant) headers.tenant = options.tenant;
		if (options?.headers) {
			Object.assign(headers, options.headers);
		}
		publishOptions.headers = headers;

		this.channel.sendToQueue(queueOrTopic, buffer, publishOptions);
	}

	async ack(message: QueueMessage): Promise<void> {
		if (!this.channel) {
			throw new Error('RabbitMQ channel not initialized.');
		}
		if (message.raw) {
			this.channel.ack(message.raw);
		}
	}

	async nack(message: QueueMessage): Promise<void> {
		if (!this.channel) {
			throw new Error('RabbitMQ channel not initialized.');
		}
		if (message.raw) {
			// Nack without requeueing (sends to DLQ if configured)
			this.channel.nack(message.raw, false, false);
		}
	}

	async disconnect(): Promise<void> {
		this.isConsuming = false;
		this.pendingResolvers = [];
		this.messageBuffer = [];
		
		try {
			if (this.channel) {
				await this.channel.close();
				this.channel = null;
			}
			if (this.connection) {
				await this.connection.close();
				this.connection = null;
			}
		} catch (err) {
			// Ignore connection closing errors
		}
	}
}
