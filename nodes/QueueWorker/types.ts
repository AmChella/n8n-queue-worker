export interface QueueMessage {
	messageId: string;
	workflow?: string;
	schema?: string;
	replyTo?: string;
	payload: any;
	metadata: {
		tenant?: string;
		correlationId?: string;
		[key: string]: any;
	};
	// raw field stores the original raw message structure from the specific provider (RabbitMQ message, Kafka event, SQS message)
	// so that ack/nack operations can operate on the exact object.
	raw?: any;
}

export interface QueueAdapter {
	connect(): Promise<void>;
	consume(): Promise<QueueMessage>;
	publish(queueOrTopic: string, payload: unknown, options?: Record<string, any>): Promise<void>;
	ack(message: QueueMessage): Promise<void>;
	nack(message: QueueMessage): Promise<void>;
	disconnect(): Promise<void>;
}

export interface ValidationResult {
	valid: boolean;
	payload?: any;
	errors?: Array<{
		field: string;
		message: string;
	}>;
}
