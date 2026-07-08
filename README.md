# n8n-nodes-queue-worker

This is an n8n community node package that provides a **Queue Worker Trigger** and a **Queue Worker Action Node**. It allows n8n workflows to consume, validate, process, acknowledge, and publish messages from multiple queue providers through a common interface.

## Supported Providers

*   **RabbitMQ** (AMQP)
*   **Apache Kafka**
*   **Amazon SQS**

---

## Features

*   **Adapter-Based Architecture**: Allows extension to new providers (e.g. Google Pub/Sub, Redis Streams) without changing the core node code.
*   **Continuous Listening**: The Trigger node runs in the background, listening continuously for incoming messages.
*   **JSON Schema Validation**: Built-in validation using AJV. Validate incoming triggers or validate payloads on-demand.
*   **Flexibility in Acknowledging**: Supports both automatic acknowledgement on receive, and manual acknowledgement (`ack`) / rejection (`nack`) via the Action node.
*   **Dead Letter Queue (DLQ)**: Easily forward failed or invalid messages to a DLQ queue/topic.
*   **Metadata & Correlation IDs**: Propagate tracing metadata (tenant, correlationId, replyTo, etc.) along with the message payload.

---

## Installation

### For n8n Cloud and Docker instances:
1.  Go to **Settings > Community Nodes**.
2.  Click **Install a new node**.
3.  Enter the npm package name: `n8n-nodes-queue-worker`.
4.  Agree to the terms and click **Install**.

### For local development:
In your n8n workspace or global installation directory, run:
```bash
npm install n8n-nodes-queue-worker
```

---

## Credentials Setup

### 1. RabbitMQ
*   **Host**: Hostname of the RabbitMQ broker.
*   **Port**: Port (default `5672` or `5671` for SSL).
*   **User / Password**: Authentication credentials.
*   **Virtual Host**: The vhost to connect to (default `/`).
*   **SSL/TLS**: Enable/disable encrypted connections.

### 2. Apache Kafka
*   **Brokers**: Comma-separated broker string (e.g., `localhost:9092,localhost:9093`).
*   **Client ID**: Client identification.
*   **SSL/TLS**: Enable/disable SSL.
*   **SASL Mechanism**: Option of `none`, `plain`, `scram-sha-256`, or `scram-sha-512`.
*   **Username / Password**: Required if SASL mechanism is enabled.

### 3. AWS SQS
*   **Auth Method**: `Access Key & Secret Key` or `IAM Role (AWS Instance Profile)`.
*   **Access Key ID / Secret Access Key**: AWS credential keys.
*   **Session Token**: Optional, for temporary credentials.
*   **AWS Region**: SQS region (e.g., `us-east-1`).

---

## Core Operations

### Trigger Node: Queue Worker Trigger
Listens for messages from a queue/topic and emits them to the workflow.
*   **Execution Mode**:
    *   `Watch Continuously`: Runs a continuous background consumer daemon (default).
    *   `Scheduled Polling`: Connects periodically at a defined interval, polls available messages, processes them, and disconnects.
    *   `At Once`: Connects, polls all currently available messages (up to limits), processes them immediately, and stops.
*   **Polling Interval / Interval Unit**: Configures the timer interval for `Scheduled Polling`.
*   **Max Messages per Poll**: Limits the number of messages fetched in a single poll under `Scheduled Polling` or `At Once` modes.
*   **Auto ACK**: If enabled, acknowledges the message immediately upon ingestion. If disabled, a downstream Action node must be used to perform manual `ack` or `nack`.
*   **JSON Schema Validation**: Set up validation rules on-trigger. Messages that fail validation are tagged with `validation: { valid: false, errors: [...] }`.

### Action Node: Queue Worker
*   **Publish Message**: Sends a message to a queue/topic with custom payload and headers (tenant, correlationId, replyTo).
*   **Validate Message**: Performs stand-alone JSON Schema validation using AJV.
*   **Ack Message**: Manually acknowledges a consumed message using the reference from the Trigger node.
*   **Nack Message**: Rejects a consumed message. If configured, can route it to a Dead Letter Queue.
*   **Send to DLQ**: Manually routes message payload to a Dead Letter Queue.

---

## Example Workflows

### 1. Manual Acknowledge Workflow
```text
Queue Worker Trigger (Auto ACK: false)
  │
  ▼
Process Data / Logic
  │
  ▼
Queue Worker Action (Operation: Ack Message, Message: ={{ $json }})
```

### 2. Trigger Schema Validation Workflow
```text
Queue Worker Trigger (With Schema Validation)
  │
  ▼
Switch Node (Condition: ={{ $json.validation.valid }})
  ├── True  ──► Process Order ──► Queue Worker (Ack Message)
  └── False ──► Log Error     ──► Queue Worker (Nack Message with DLQ)
```

---

## Development

Build the project:
```bash
npm run build
```

Run tests:
```bash
npm run test
```

Linting:
```bash
npm run lint
```
