import { ICredentialType, INodeProperties } from 'n8n-workflow';

export class Kafka implements ICredentialType {
	name = 'kafka';
	displayName = 'Apache Kafka';
	properties: INodeProperties[] = [
		{
			displayName: 'Brokers',
			name: 'brokers',
			type: 'string',
			default: 'localhost:9092',
			description: 'Comma-separated list of brokers (e.g. localhost:9092,localhost:9093)',
			required: true,
		},
		{
			displayName: 'Client ID',
			name: 'clientId',
			type: 'string',
			default: 'n8n-queue-worker',
			required: true,
		},
		{
			displayName: 'SSL/TLS',
			name: 'ssl',
			type: 'boolean',
			default: false,
		},
		{
			displayName: 'SASL Mechanism',
			name: 'saslMechanism',
			type: 'options',
			options: [
				{
					name: 'None',
					value: 'none',
				},
				{
					name: 'Plain',
					value: 'plain',
				},
				{
					name: 'Scram-Sha-256',
					value: 'scram-sha-256',
				},
				{
					name: 'Scram-Sha-512',
					value: 'scram-sha-512',
				},
			],
			default: 'none',
		},
		{
			displayName: 'Username',
			name: 'username',
			type: 'string',
			default: '',
			displayOptions: {
				show: {
					saslMechanism: [
						'plain',
						'scram-sha-256',
						'scram-sha-512',
					],
				},
			},
			required: true,
		},
		{
			displayName: 'Password',
			name: 'password',
			type: 'string',
			typeOptions: {
				password: true,
			},
			default: '',
			displayOptions: {
				show: {
					saslMechanism: [
						'plain',
						'scram-sha-256',
						'scram-sha-512',
					],
				},
			},
			required: true,
		},
	];
}
