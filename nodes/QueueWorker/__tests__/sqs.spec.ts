import {
	SQSClient,
	ReceiveMessageCommand,
	SendMessageCommand,
	DeleteMessageCommand,
	ChangeMessageVisibilityCommand,
	GetQueueUrlCommand,
} from '@aws-sdk/client-sqs';
import { AwsSqsAdapter } from '../adapters/sqs';

jest.mock('@aws-sdk/client-sqs', () => {
	return {
		SQSClient: jest.fn(),
		GetQueueUrlCommand: jest.fn().mockImplementation(function(this: any, input: any) { this.input = input; }),
		SendMessageCommand: jest.fn().mockImplementation(function(this: any, input: any) { this.input = input; }),
		DeleteMessageCommand: jest.fn().mockImplementation(function(this: any, input: any) { this.input = input; }),
		ChangeMessageVisibilityCommand: jest.fn().mockImplementation(function(this: any, input: any) { this.input = input; }),
		ReceiveMessageCommand: jest.fn().mockImplementation(function(this: any, input: any) { this.input = input; }),
	};
});



describe('AwsSqsAdapter', () => {
	let mockSend: jest.Mock;
	let mockClient: any;

	beforeEach(() => {
		jest.clearAllMocks();

		mockSend = jest.fn();
		mockClient = {
			send: mockSend,
			destroy: jest.fn(),
		};

		(SQSClient as jest.Mock).mockImplementation(() => mockClient);
	});

	test('should initialize SQSClient and resolve Queue URL', async () => {
		mockSend.mockResolvedValueOnce({
			QueueUrl: 'https://sqs.us-east-1.amazonaws.com/123456789012/my-queue',
		});

		const credentials = {
			authMethod: 'credentials',
			accessKeyId: 'key',
			secretAccessKey: 'secret',
			region: 'us-east-1',
		};
		const options = {
			queueName: 'my-queue',
		};

		const adapter = new AwsSqsAdapter(credentials, options);
		await adapter.connect();

		expect(SQSClient).toHaveBeenCalledWith({
			region: 'us-east-1',
			credentials: {
				accessKeyId: 'key',
				secretAccessKey: 'secret',
			},
		});

		expect(mockSend).toHaveBeenCalledWith(expect.any(GetQueueUrlCommand));
	});

	test('should bypass GetQueueUrlCommand if queueName is already a URL', async () => {
		const credentials = { region: 'us-east-1' };
		const options = {
			queueName: 'https://sqs.us-east-1.amazonaws.com/123456789012/already-url',
		};

		const adapter = new AwsSqsAdapter(credentials, options);
		await adapter.connect();

		expect(mockSend).not.toHaveBeenCalled();
	});

	test('should publish a message using SendMessageCommand', async () => {
		mockSend.mockResolvedValueOnce({ QueueUrl: 'https://target-url' });
		mockSend.mockResolvedValueOnce({ MessageId: 'msg-id' });

		const adapter = new AwsSqsAdapter({}, { queueName: 'https://my-queue-url' });
		await adapter.connect();

		const payload = { event: 'created' };
		const options = {
			correlationId: 'c-id',
			workflow: 'test-workflow',
			schema: 'v1',
			tenant: 'tenant-1',
		};

		await adapter.publish('https://target-url', payload, options);

		expect(mockSend).toHaveBeenLastCalledWith(expect.any(SendMessageCommand));
		const sendArg = mockSend.mock.calls[mockSend.mock.calls.length - 1][0];
		expect(sendArg.input).toEqual({
			QueueUrl: 'https://target-url',
			MessageBody: JSON.stringify(payload),
			MessageAttributes: {
				correlationId: { DataType: 'String', StringValue: 'c-id' },
				workflow: { DataType: 'String', StringValue: 'test-workflow' },
				schema: { DataType: 'String', StringValue: 'v1' },
				tenant: { DataType: 'String', StringValue: 'tenant-1' },
			},
		});
	});

	test('should delete message on ack', async () => {
		const adapter = new AwsSqsAdapter({}, { queueName: 'https://my-queue-url' });
		await adapter.connect();

		const queueMessage = {
			messageId: 'msg-1',
			payload: {},
			metadata: {},
			raw: {
				ReceiptHandle: 'handle-123',
			},
		};

		await adapter.ack(queueMessage);

		expect(mockSend).toHaveBeenCalledWith(expect.any(DeleteMessageCommand));
		const deleteArg = mockSend.mock.calls[0][0];
		expect(deleteArg.input).toEqual({
			QueueUrl: 'https://my-queue-url',
			ReceiptHandle: 'handle-123',
		});
	});

	test('should change visibility timeout to 0 on nack', async () => {
		const adapter = new AwsSqsAdapter({}, { queueName: 'https://my-queue-url' });
		await adapter.connect();

		const queueMessage = {
			messageId: 'msg-1',
			payload: {},
			metadata: {},
			raw: {
				ReceiptHandle: 'handle-123',
			},
		};

		await adapter.nack(queueMessage);

		expect(mockSend).toHaveBeenCalledWith(expect.any(ChangeMessageVisibilityCommand));
		const changeArg = mockSend.mock.calls[0][0];
		expect(changeArg.input).toEqual({
			QueueUrl: 'https://my-queue-url',
			ReceiptHandle: 'handle-123',
			VisibilityTimeout: 0,
		});
	});

	test('should destroy client on disconnect', async () => {
		const adapter = new AwsSqsAdapter({}, { queueName: 'https://my-queue-url' });
		await adapter.connect();
		await adapter.disconnect();

		expect(mockClient.destroy).toHaveBeenCalled();
	});
});
