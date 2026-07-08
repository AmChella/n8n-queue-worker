import * as amqp from 'amqplib';
import { RabbitMqAdapter } from '../adapters/rabbitmq';

jest.mock('amqplib');

describe('RabbitMqAdapter', () => {
	let mockConnect: jest.Mock;
	let mockChannel: any;
	let mockConnection: any;

	beforeEach(() => {
		jest.clearAllMocks();

		mockChannel = {
			prefetch: jest.fn().mockResolvedValue(undefined),
			assertQueue: jest.fn().mockResolvedValue(undefined),
			consume: jest.fn().mockResolvedValue({ consumerTag: 'abc' }),
			sendToQueue: jest.fn().mockReturnValue(true),
			ack: jest.fn(),
			nack: jest.fn(),
			close: jest.fn().mockResolvedValue(undefined),
		};

		mockConnection = {
			createChannel: jest.fn().mockResolvedValue(mockChannel),
			close: jest.fn().mockResolvedValue(undefined),
		};

		mockConnect = amqp.connect as jest.Mock;
		mockConnect.mockResolvedValue(mockConnection);
	});

	test('should connect and assert the specified queue', async () => {
		const credentials = {
			host: 'test-host',
			port: 5672,
			user: 'test-user',
			password: 'test-password',
			vhost: '/',
		};
		const options = {
			queueName: 'my-test-queue',
			prefetch: 5,
		};

		const adapter = new RabbitMqAdapter(credentials, options);
		await adapter.connect();

		// URL encode check
		expect(mockConnect).toHaveBeenCalledWith('amqp://test-user:test-password@test-host:5672/');
		expect(mockConnection.createChannel).toHaveBeenCalled();
		expect(mockChannel.prefetch).toHaveBeenCalledWith(5);
		expect(mockChannel.assertQueue).toHaveBeenCalledWith('my-test-queue', {
			durable: true,
			arguments: {},
		});
	});

	test('should connect with SSL if requested', async () => {
		const credentials = {
			host: 'test-host',
			port: 5671,
			user: 'user',
			password: 'pass',
			vhost: 'custom-vh',
			ssl: true,
		};
		const options = {
			queueName: 'my-test-queue',
		};

		const adapter = new RabbitMqAdapter(credentials, options);
		await adapter.connect();

		expect(mockConnect).toHaveBeenCalledWith('amqps://user:pass@test-host:5671/custom-vh');
	});

	test('should publish a message successfully with headers and correlation ID', async () => {
		const credentials = {};
		const options = {};
		const adapter = new RabbitMqAdapter(credentials, options);
		await adapter.connect();

		const payload = { test: 'data' };
		const publishOptions = {
			correlationId: 'corr-123',
			replyTo: 'reply-queue',
			workflow: 'test-flow',
			schema: 'test-schema',
			tenant: 'test-tenant',
		};

		await adapter.publish('outbox-queue', payload, publishOptions);

		expect(mockChannel.assertQueue).toHaveBeenCalledWith('outbox-queue', { durable: true });
		expect(mockChannel.sendToQueue).toHaveBeenCalledWith(
			'outbox-queue',
			Buffer.from(JSON.stringify(payload)),
			expect.objectContaining({
				persistent: true,
				correlationId: 'corr-123',
				replyTo: 'reply-queue',
				headers: {
					workflow: 'test-flow',
					schema: 'test-schema',
					tenant: 'test-tenant',
				},
			})
		);
	});

	test('should acknowledge a message using raw fields', async () => {
		const credentials = {};
		const options = {};
		const adapter = new RabbitMqAdapter(credentials, options);
		await adapter.connect();

		const mockRawMsg = { fields: {}, properties: {} };
		const queueMessage = {
			messageId: 'msg-1',
			payload: {},
			metadata: {},
			raw: mockRawMsg,
		};

		await adapter.ack(queueMessage);

		expect(mockChannel.ack).toHaveBeenCalledWith(mockRawMsg);
	});

	test('should reject a message without requeueing (nack)', async () => {
		const credentials = {};
		const options = {};
		const adapter = new RabbitMqAdapter(credentials, options);
		await adapter.connect();

		const mockRawMsg = { fields: {}, properties: {} };
		const queueMessage = {
			messageId: 'msg-1',
			payload: {},
			metadata: {},
			raw: mockRawMsg,
		};

		await adapter.nack(queueMessage);

		expect(mockChannel.nack).toHaveBeenCalledWith(mockRawMsg, false, false);
	});

	test('should close channel and connection on disconnect', async () => {
		const credentials = {};
		const options = {};
		const adapter = new RabbitMqAdapter(credentials, options);
		await adapter.connect();
		await adapter.disconnect();

		expect(mockChannel.close).toHaveBeenCalled();
		expect(mockConnection.close).toHaveBeenCalled();
	});

	test('should return null if consume times out before message is received', async () => {
		const credentials = {};
		const options = { queueName: 'timeout-queue' };
		const adapter = new RabbitMqAdapter(credentials, options);
		await adapter.connect();

		const consumePromise = adapter.consume(100);
		const result = await consumePromise;
		expect(result).toBeNull();
	});

	test('should return message if message is received before timeout', async () => {
		const credentials = {};
		const options = { queueName: 'timeout-queue' };
		const adapter = new RabbitMqAdapter(credentials, options);
		await adapter.connect();

		const consumePromise = adapter.consume(500);

		// Trigger the mock consumer callback
		const consumeCallback = mockChannel.consume.mock.calls[0][1];
		const mockRawMsg = {
			content: Buffer.from(JSON.stringify({ hello: 'world' })),
			properties: {
				correlationId: 'corr-id',
				headers: {
					workflow: 'test-flow',
				},
			},
		};
		consumeCallback(mockRawMsg);

		const result = await consumePromise;
		expect(result).not.toBeNull();
		expect(result?.payload).toEqual({ hello: 'world' });
	});
});
