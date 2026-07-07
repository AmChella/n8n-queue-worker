import { QueueFactory } from '../QueueFactory';
import { RabbitMqAdapter } from '../adapters/rabbitmq';
import { KafkaAdapter } from '../adapters/kafka';
import { AwsSqsAdapter } from '../adapters/sqs';

describe('QueueFactory', () => {
	test('should return RabbitMqAdapter for "rabbitmq" provider', () => {
		const credentials = { host: 'localhost' };
		const options = { queueName: 'test-queue' };
		
		const adapter = QueueFactory.getAdapter('rabbitmq', credentials, options);
		
		expect(adapter).toBeInstanceOf(RabbitMqAdapter);
	});

	test('should return KafkaAdapter for "kafka" provider', () => {
		const credentials = { brokers: 'localhost:9092' };
		const options = { queueName: 'test-topic' };

		const adapter = QueueFactory.getAdapter('kafka', credentials, options);

		expect(adapter).toBeInstanceOf(KafkaAdapter);
	});

	test('should return AwsSqsAdapter for "sqs" or "awssqs" provider', () => {
		const credentials = { region: 'us-east-1' };
		const options = { queueName: 'test-sqs-queue' };

		const adapter1 = QueueFactory.getAdapter('sqs', credentials, options);
		const adapter2 = QueueFactory.getAdapter('awssqs', credentials, options);

		expect(adapter1).toBeInstanceOf(AwsSqsAdapter);
		expect(adapter2).toBeInstanceOf(AwsSqsAdapter);
	});

	test('should throw an error for unsupported provider names', () => {
		expect(() => {
			QueueFactory.getAdapter('unsupported-queue', {}, {});
		}).toThrow('Unsupported queue provider: unsupported-queue');
	});
});
