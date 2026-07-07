import { Kafka, Producer, Consumer, SASLOptions } from 'kafkajs';
import { QueueAdapter, QueueMessage } from '../types';

export class KafkaAdapter implements QueueAdapter {
	private kafka: Kafka | null = null;
	private producer: Producer | null = null;
	private consumer: Consumer | null = null;
	private messageBuffer: QueueMessage[] = [];
	private pendingResolvers: Array<(msg: QueueMessage) => void> = [];
	private isConsuming = false;

	constructor(
		private credentials: any,
		private options: {
			queueName?: string; // Topic in Kafka
			groupId?: string;
			fromBeginning?: boolean;
			autoAck?: boolean;
			deadLetterQueue?: string;
			[key: string]: any;
		}
	) {}

	async connect(): Promise<void> {
		const brokers = (this.credentials.brokers || 'localhost:9092')
			.split(',')
			.map((b: string) => b.trim());
		const clientId = this.credentials.clientId || 'n8n-queue-worker';
		const ssl = !!this.credentials.ssl;

		let sasl: SASLOptions | undefined;
		const mechanism = this.credentials.saslMechanism || 'none';

		if (mechanism !== 'none') {
			sasl = {
				mechanism: mechanism as any,
				username: this.credentials.username,
				password: this.credentials.password,
			};
		}

		this.kafka = new Kafka({
			clientId,
			brokers,
			ssl,
			sasl,
		});

		// Create producer
		this.producer = this.kafka.producer();
		await this.producer.connect();

		// Create consumer
		const groupId = this.options.groupId || 'n8n-group';
		this.consumer = this.kafka.consumer({ groupId });
		await this.consumer.connect();
	}

	async consume(): Promise<QueueMessage> {
		if (!this.consumer) {
			throw new Error('Kafka consumer not initialized. Call connect() first.');
		}

		const topic = this.options.queueName;
		if (!topic) {
			throw new Error('Topic (Queue Name) must be specified to consume.');
		}

		if (!this.isConsuming) {
			this.isConsuming = true;
			await this.consumer.subscribe({
				topic,
				fromBeginning: !!this.options.fromBeginning,
			});

			const autoCommit = !!this.options.autoAck;

			await this.consumer.run({
				autoCommit,
				eachMessage: async ({ topic: msgTopic, partition, message }) => {
					let payload: any;
					const contentStr = message.value?.toString() || '';
					try {
						payload = JSON.parse(contentStr);
					} catch {
						payload = contentStr;
					}

					const headers: Record<string, any> = {};
					if (message.headers) {
						for (const key of Object.keys(message.headers)) {
							const val = message.headers[key];
							headers[key] = val ? val.toString() : '';
						}
					}

					const correlationId = headers.correlationId || message.key?.toString();
					const messageId = message.key?.toString() || `${partition}-${message.offset}`;

					const queueMessage: QueueMessage = {
						messageId,
						workflow: headers.workflow,
						schema: headers.schema,
						replyTo: headers.replyTo,
						payload,
						metadata: {
							tenant: headers.tenant,
							correlationId,
							headers,
							partition,
							offset: message.offset,
							topic: msgTopic,
						},
						raw: {
							topic: msgTopic,
							partition,
							offset: message.offset,
						},
					};

					if (this.pendingResolvers.length > 0) {
						const resolve = this.pendingResolvers.shift();
						resolve!(queueMessage);
					} else {
						this.messageBuffer.push(queueMessage);
					}
				},
			});
		}

		if (this.messageBuffer.length > 0) {
			return this.messageBuffer.shift()!;
		}

		return new Promise<QueueMessage>((resolve) => {
			this.pendingResolvers.push(resolve);
		});
	}

	async publish(queueOrTopic: string, payload: unknown, options?: Record<string, any>): Promise<void> {
		if (!this.producer) {
			throw new Error('Kafka producer not initialized. Call connect() first.');
		}

		const headers: Record<string, string> = {};
		if (options?.workflow) headers.workflow = options.workflow;
		if (options?.schema) headers.schema = options.schema;
		if (options?.tenant) headers.tenant = options.tenant;
		if (options?.replyTo) headers.replyTo = options.replyTo;
		if (options?.correlationId) headers.correlationId = options.correlationId;

		if (options?.headers) {
			for (const [k, v] of Object.entries(options.headers)) {
				headers[k] = String(v);
			}
		}

		const value = typeof payload === 'string' ? payload : JSON.stringify(payload);
		const key = options?.correlationId || undefined;

		await this.producer.send({
			topic: queueOrTopic,
			messages: [
				{
					key,
					value,
					headers,
				},
			],
		});
	}

	async ack(message: QueueMessage): Promise<void> {
		if (!this.consumer) {
			throw new Error('Kafka consumer not initialized.');
		}
		if (message.raw) {
			const { topic, partition, offset } = message.raw;
			// Commit offset. Offsets are committed at the NEXT message offset (current offset + 1)
			const nextOffset = (BigInt(offset) + 1n).toString();
			await this.consumer.commitOffsets([
				{
					topic,
					partition,
					offset: nextOffset,
				},
			]);
		}
	}

	async nack(message: QueueMessage): Promise<void> {
		if (!this.consumer) {
			throw new Error('Kafka consumer not initialized.');
		}
		if (message.raw) {
			const { topic, partition, offset } = message.raw;
			
			// If a DLQ topic is specified, send the message to the DLQ first, then commit offset to skip it
			if (this.options.deadLetterQueue) {
				await this.publish(this.options.deadLetterQueue, message.payload, {
					correlationId: message.metadata.correlationId,
					workflow: message.workflow,
					schema: message.schema,
					tenant: message.metadata.tenant,
					headers: {
						...message.metadata.headers,
						nackReason: 'Manual nack / DLQ routing',
						originalTopic: topic,
						originalPartition: String(partition),
						originalOffset: String(offset),
					},
				});
			}

			// Commit the offset so the consumer moves forward and doesn't get stuck in a loop
			const nextOffset = (BigInt(offset) + 1n).toString();
			await this.consumer.commitOffsets([
				{
					topic,
					partition,
					offset: nextOffset,
				},
			]);
		}
	}

	async disconnect(): Promise<void> {
		this.isConsuming = false;
		this.pendingResolvers = [];
		this.messageBuffer = [];

		try {
			if (this.consumer) {
				await this.consumer.disconnect();
				this.consumer = null;
			}
			if (this.producer) {
				await this.producer.disconnect();
				this.producer = null;
			}
		} catch (err) {
			// Ignore disconnect errors
		}
	}
}
