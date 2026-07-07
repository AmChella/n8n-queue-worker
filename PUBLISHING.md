# Publishing Instructions for n8n Community Nodes

This guide outlines the steps to verify, publish, and distribute your `n8n-nodes-queue-worker` node so other users can install and use it.

---

## 1. Local Testing & Verification

Before publishing to npm, you should test the node locally inside a running n8n instance.

### Step 1: Link your Node
In the root directory of your node (`n8n-nodes-queue-worker/`):
```bash
npm run build
npm link
```

### Step 2: Link to n8n
Navigate to your local n8n installation directory (usually `~/.n8n/` or where your n8n code is cloned) and run:
```bash
npm link n8n-nodes-queue-worker
```

### Step 3: Run n8n in Dev Mode
Start n8n, ensuring it loads community nodes. If you are developing locally, run:
```bash
n8n start
```
The node will now appear in your n8n workflow editor.

### 1.1 Docker-based Local Testing

If you run n8n inside a Docker container, you can mount your local compiled node directly into the container's custom nodes directory.

#### Step 1: Build the Node locally
Run the build script in your node's project directory to compile TypeScript to JavaScript:
```bash
npm run build
```

#### Step 2: Mount the built directory via Volumes
Map the local directory of your built package to the custom extensions path inside the n8n container.

**Option A: Docker Compose**
Add a volume mapping and env variable in your `docker-compose.yml`:
```yaml
services:
  n8n:
    image: n8nio/n8n:latest
    ports:
      - "5678:5678"
    volumes:
      - ~/.n8n:/home/node/.n8n
      # Mount your local built node directory
      - /absolute/path/to/n8n-nodes-queue-worker:/home/node/.n8n/custom/node_modules/n8n-nodes-queue-worker
    environment:
      # Tell n8n to scan this directory for custom nodes
      - N8N_CUSTOM_EXTENSIONS=/home/node/.n8n/custom/node_modules/n8n-nodes-queue-worker
```

**Option B: Docker Run Command**
Start the n8n container with volume mount and environment variable flags:
```bash
docker run -it --rm \
  --name n8n \
  -p 5678:5678 \
  -v ~/.n8n:/home/node/.n8n \
  -v /absolute/path/to/n8n-nodes-queue-worker:/home/node/.n8n/custom/node_modules/n8n-nodes-queue-worker \
  -e N8N_CUSTOM_EXTENSIONS=/home/node/.n8n/custom/node_modules/n8n-nodes-queue-worker \
  n8nio/n8n:latest
```

#### Step 3: Restart n8n
After making changes to your local code, rebuild the project and restart the n8n container to apply updates:
```bash
npm run build
docker restart n8n
```

---

## 2. npm Package Preparation

Ensure your `package.json` contains the required properties to be indexed as an n8n community node:

1.  **Name**: Must start with `n8n-nodes-` (e.g. `n8n-nodes-queue-worker`).
2.  **Keywords**: Must include `"n8n-community-node-package"`.
3.  **n8n Section**: Must declare the output files:
    ```json
    "n8n": {
      "n8nNodesApiVersion": 1,
      "nodes": [
        "dist/nodes/QueueWorker/QueueWorker.node.js",
        "dist/nodes/QueueWorker/QueueWorkerTrigger.node.js"
      ],
      "credentials": [
        "dist/credentials/RabbitMQ.credentials.js",
        "dist/credentials/Kafka.credentials.js",
        "dist/credentials/AwsSqs.credentials.js"
      ]
    }
    ```

---

## 3. Registering & Publishing on npm

### Step 1: Login to npm
If you don't have an npm account, register at [npmjs.com](https://www.npmjs.com/). Login via the CLI:
```bash
npm login
```

### Step 2: Publish the Package
Publish your package to the public registry:
```bash
npm publish --access public
```

---

## 4. Verification in the n8n Creator Portal

To make your node officially verified and listed within n8n's community search:

1.  Go to the [n8n Creator Portal](https://creator.n8n.io/).
2.  Log in and submit your published npm package name: `n8n-nodes-queue-worker`.
3.  **Provenanced Builds**: n8n requires community nodes seeking verification to be built and published using **GitHub Actions with npm provenance enabled** to guarantee supply-chain security.
    *   Add `provenance: true` to your GitHub Actions publish step:
        ```yaml
        - name: Publish to npm
          run: npm publish --provenance
          env:
            NODE_AUTH_TOKEN: ${{ secrets.NPM_TOKEN }}
        ```
