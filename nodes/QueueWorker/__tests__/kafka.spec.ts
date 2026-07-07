import { Kafka } from 'kafkajs';
import { KafkaAdapter } from '../adapters/kafka';

jest.mock('kafkajs');

describe('KafkaAdapter', () => {
	let mockKafka: any;
	let mockProducer: any;
	let mockConsumer: any;

	beforeEach(() => {
		jest.clearAllMocks();

		mockProducer = {
			connect: jest.fn().mockResolvedValue(undefined),
			send: jest.fn().mockResolvedValue(undefined),
			disconnect: jest.fn().mockResolvedValue(undefined),
		};

		mockConsumer = {
			connect: jest.fn().mockResolvedValue(undefined),
			subscribe: jest.fn().mockResolvedValue(undefined),
			run: jest.fn().mockResolvedValue(undefined),
			commitOffsets: jest.fn().mockResolvedValue(undefined),
			disconnect: jest.fn().mockResolvedValue(undefined),
		};

		mockKafka = {
			producer: jest.fn().mockReturnValue(mockProducer),
			consumer: jest.fn().mockReturnValue(mockConsumer),
		};

		(Kafka as jest.Mock).mockImplementation(() => mockKafka);
	});

	test('should initialize Kafka client and connect producer/consumer', async () => {
		const credentials = {
			brokers: 'localhost:9092,localhost:9093',
			clientId: 'my-client',
			ssl: true,
			saslMechanism: 'plain',
			username: 'my-user',
			password: 'my-password',
		};
		const options = {
			queueName: 'test-topic',
			groupId: 'test-group',
		};

		const adapter = new KafkaAdapter(credentials, options);
		await adapter.connect();

		expect(Kafka).toHaveBeenCalledWith({
			clientId: 'my-client',
			brokers: ['localhost:9092', 'localhost:9093'],
			ssl: true,
			sasl: {
				mechanism: 'plain',
				username: 'my-user',
				password: 'my-password',
			},
		});

		expect(mockKafka.producer).toHaveBeenCalled();
		expect(mockKafka.consumer).toHaveBeenCalledWith({ groupId: 'test-group' });
		expect(mockProducer.connect).toHaveBeenCalled();
		expect(mockConsumer.connect).toHaveBeenCalled();
	});

	test('should publish message to Kafka topic', async () => {
		const adapter = new KafkaAdapter({}, {});
		await adapter.connect();

		const payload = { event: 'user_created' };
		const options = {
			correlationId: 'corr-id',
			workflow: 'user-signup',
			schema: 'v1',
			tenant: 'org-abc',
		};

		await adapter.publish('signup-topic', payload, options);

		expect(mockProducer.send).toHaveBeenCalledWith({
			topic: 'signup-topic',
			messages: [
				{
					key: 'corr-id',
					value: JSON.stringify(payload),
					headers: {
						correlationId: 'corr-id',
						workflow: 'user-signup',
						schema: 'v1',
						tenant: 'org-abc',
					},
				},
			],
		});
	});

	test('should commit offset on ack', async () => {
		const adapter = new KafkaAdapter({}, {});
		await adapter.connect();

		const queueMessage = {
			messageId: '123',
			payload: {},
			metadata: {},
			raw: {
				topic: 'my-topic',
				partition: 2,
				offset: '42',
			},
		};

		await adapter.ack(queueMessage);

		expect(mockConsumer.commitOffsets).toHaveBeenCalledWith([
			{
				topic: 'my-topic',
				partition: 2,
				offset: '43', // (42 + 1)
			},
		]);
	});

	test('should send to DLQ and commit offset on nack if deadLetterQueue is configured', async () => {
		const adapter = new KafkaAdapter({}, { deadLetterQueue: 'dlq-topic' });
		await adapter.connect();

		const queueMessage = {
			messageId: '123',
			payload: { foo: 'bar' },
			metadata: {
				correlationId: 'c-id',
				tenant: 't-id',
				headers: {},
			},
			raw: {
				topic: 'my-topic',
				partition: 1,
				offset: '100',
			},
		};

		await adapter.nack(queueMessage);

		// Send to DLQ
		expect(mockProducer.send).toHaveBeenCalledWith(
			expect.objectContaining({
				topic: 'dlq-topic',
				messages: [
					expect.objectContaining({
						key: 'c-id',
						value: JSON.stringify({ foo: 'bar' }),
					}),
				],
			})
		);

		// Commit offset
		expect(mockConsumer.commitOffsets).toHaveBeenCalledWith([
			{
				topic: 'my-topic',
				partition: 1,
				offset: '101',
			},
		]);
	});

	test('should disconnect both producer and consumer', async () => {
		const adapter = new KafkaAdapter({}, {});
		await adapter.connect();
		await adapter.disconnect();

		expect(mockConsumer.disconnect).toHaveBeenCalled();
		expect(mockProducer.disconnect).toHaveBeenCalled();
	});
});
