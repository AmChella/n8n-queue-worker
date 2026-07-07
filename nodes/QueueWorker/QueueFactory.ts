import { QueueAdapter } from './types';
import { RabbitMqAdapter } from './adapters/rabbitmq';
import { KafkaAdapter } from './adapters/kafka';
import { AwsSqsAdapter } from './adapters/sqs';

export class QueueFactory {
	/**
	 * Returns an instance of QueueAdapter based on the provider type.
	 * @param provider 'rabbitmq' | 'kafka' | 'sqs'
	 * @param credentials Credentials for authentication
	 * @param options Additional queue properties/configurations
	 */
	static getAdapter(provider: string, credentials: any, options: any): QueueAdapter {
		switch (provider.toLowerCase()) {
			case 'rabbitmq':
				return new RabbitMqAdapter(credentials, options);
			case 'kafka':
				return new KafkaAdapter(credentials, options);
			case 'sqs':
			case 'awssqs':
				return new AwsSqsAdapter(credentials, options);
			default:
				throw new Error(`Unsupported queue provider: ${provider}`);
		}
	}
}
