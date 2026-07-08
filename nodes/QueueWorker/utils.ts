import * as path from 'path';
import * as fs from 'fs';

export function getPackageVersion(): string {
	try {
		// In ts-node / development source directory structure:
		// __dirname is nodes/QueueWorker
		// package.json is 2 directories up: ../../package.json
		// In compiled output:
		// __dirname is dist/nodes/QueueWorker
		// package.json is 3 directories up: ../../../package.json
		const pathsToTry = [
			path.join(__dirname, '..', '..', 'package.json'),
			path.join(__dirname, '..', '..', '..', 'package.json'),
		];
		for (const p of pathsToTry) {
			if (fs.existsSync(p)) {
				const pkg = JSON.parse(fs.readFileSync(p, 'utf8'));
				if (pkg && pkg.version) {
					return pkg.version;
				}
			}
		}
	} catch (error) {
		// Suppress errors and fallback
	}
	return '1.0.3';
}
