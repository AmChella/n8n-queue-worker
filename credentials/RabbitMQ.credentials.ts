import { ICredentialType, INodeProperties } from 'n8n-workflow';

export class RabbitMQ implements ICredentialType {
	name = 'rabbitMq';
	displayName = 'RabbitMQ';
	properties: INodeProperties[] = [
		{
			displayName: 'Host',
			name: 'host',
			type: 'string',
			default: 'localhost',
			required: true,
		},
		{
			displayName: 'Port',
			name: 'port',
			type: 'number',
			default: 5672,
			required: true,
		},
		{
			displayName: 'User',
			name: 'user',
			type: 'string',
			default: 'guest',
			required: true,
		},
		{
			displayName: 'Password',
			name: 'password',
			type: 'string',
			typeOptions: {
				password: true,
			},
			default: 'guest',
			required: true,
		},
		{
			displayName: 'Virtual Host',
			name: 'vhost',
			type: 'string',
			default: '/',
			required: true,
		},
		{
			displayName: 'SSL/TLS',
			name: 'ssl',
			type: 'boolean',
			default: false,
		},
	];
}
