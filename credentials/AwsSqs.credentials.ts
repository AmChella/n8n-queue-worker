import { ICredentialType, INodeProperties } from 'n8n-workflow';

export class AwsSqs implements ICredentialType {
	name = 'awsSqs';
	displayName = 'AWS SQS';
	properties: INodeProperties[] = [
		{
			displayName: 'Auth Method',
			name: 'authMethod',
			type: 'options',
			options: [
				{
					name: 'Access Key & Secret Key',
					value: 'credentials',
				},
				{
					name: 'IAM Role (AWS Instance Profile)',
					value: 'iamRole',
				},
			],
			default: 'credentials',
		},
		{
			displayName: 'Access Key ID',
			name: 'accessKeyId',
			type: 'string',
			default: '',
			displayOptions: {
				show: {
					authMethod: ['credentials'],
				},
			},
			required: true,
		},
		{
			displayName: 'Secret Access Key',
			name: 'secretAccessKey',
			type: 'string',
			typeOptions: {
				password: true,
			},
			default: '',
			displayOptions: {
				show: {
					authMethod: ['credentials'],
				},
			},
			required: true,
		},
		{
			displayName: 'Session Token',
			name: 'sessionToken',
			type: 'string',
			typeOptions: {
				password: true,
			},
			default: '',
			displayOptions: {
				show: {
					authMethod: ['credentials'],
				},
			},
			description: 'Optional AWS Session Token for temporary credentials',
		},
		{
			displayName: 'AWS Region',
			name: 'region',
			type: 'string',
			default: 'us-east-1',
			description: 'AWS region (e.g. us-east-1, eu-west-1)',
			required: true,
		},
	];
}
