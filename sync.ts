/**
 * Resend contacts CSV -> notification_preferences upsert.
 *
 * Standalone: needs only Postgres. No go-api, no auth API, no other service.
 *
 *   npx tsx sync.ts --dry-run
 *   npx tsx sync.ts --limit=100
 *   npx tsx sync.ts
 */

import fs from "node:fs";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";
import pg from "pg";

const { Client } = pg;

const SCRIPT_DIR = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_BATCH_SIZE = 500;
const EMAIL_MAX_LENGTH = 255;
const EXPECTED_HEADER = ["id", "created_at", "first_name", "last_name", "email", "unsubscribed"];
const EMAIL_PATTERN = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const TIMESTAMP_PATTERN = /^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?([+-]\d{2}(:?\d{2})?)?$/;

const INSERT_SQL = `
	INSERT INTO notification_preferences
		(email, allow_promotional_email, allow_push, created_at, updated_at)
	VALUES %s
	ON CONFLICT (email) DO UPDATE SET
		allow_promotional_email = EXCLUDED.allow_promotional_email,
		updated_at = NOW()
	RETURNING (xmax = 0) AS inserted
`;

interface Config {
	dbURL: string;
	csvPath: string;
	batchSize: number;
	dryRun: boolean;
	limit: number;
}

interface RowIssue {
	line: number;
	reason: string;
}

interface PreferenceRow {
	email: string;
	allowPromotionalEmail: boolean;
	createdAt: string;
}

interface RunStats {
	inserted: number;
	updated: number;
}

function loadDotEnv(): void {
	const envPath = path.join(SCRIPT_DIR, ".env");
	if (!fs.existsSync(envPath)) {
		return;
	}

	const contents = fs.readFileSync(envPath, "utf8");
	for (const rawLine of contents.split("\n")) {
		const line = rawLine.trim();
		if (line === "" || line.startsWith("#")) {
			continue;
		}

		const separator = line.indexOf("=");
		if (separator === -1) {
			continue;
		}

		const key = line.slice(0, separator).trim();
		let value = line.slice(separator + 1).trim();
		if (value.length >= 2 && value.startsWith('"') && value.endsWith('"')) {
			value = value.slice(1, -1);
		}

		if (process.env[key] === undefined) {
			process.env[key] = value;
		}
	}
}

function parseArgs(argv: string[]): { dryRun: boolean; limit: number; file?: string } {
	let dryRun = false;
	let limit = 0;
	let file: string | undefined = undefined;

	for (const arg of argv) {
		if (arg === "--dry-run") {
			dryRun = true;
			continue;
		}
		if (arg.startsWith("--limit=")) {
			limit = Number.parseInt(arg.slice("--limit=".length), 10);
			if (!Number.isFinite(limit) || limit < 0) {
				fail(`invalid --limit value in "${arg}"`);
			}
			continue;
		}
		if (arg.startsWith("--file=")) {
			file = arg.slice("--file=".length);
			continue;
		}
		fail(`unknown argument "${arg}"`);
	}

	return { dryRun, limit, file };
}

function loadConfig(): Config {
	const args = parseArgs(process.argv.slice(2));

	const dbURL = process.env.DB_URL;
	if (!dbURL) {
		fail("DB_URL is not set. Add it to .env or export it.");
	}

	const csvPath = args.file ?? process.env.CONTACTS_CSV_PATH;
	if (!csvPath) {
		fail("CONTACTS_CSV_PATH is not set. Add it to .env, export it, or pass --file=.");
	}

	if (!fs.existsSync(csvPath)) {
		fail(`CSV not found: ${csvPath}`);
	}

	const batchSize = Number.parseInt(process.env.BATCH_SIZE ?? "", 10);
	if (process.env.BATCH_SIZE !== undefined && (!Number.isFinite(batchSize) || batchSize < 1)) {
		fail(`invalid BATCH_SIZE "${process.env.BATCH_SIZE}"`);
	}

	return {
		dbURL,
		csvPath,
		batchSize: Number.isFinite(batchSize) && batchSize >= 1 ? batchSize : DEFAULT_BATCH_SIZE,
		dryRun: args.dryRun,
		limit: args.limit,
	};
}

function fail(message: string): never {
	console.error(`error: ${message}`);
	process.exit(1);
}

/** Splits one CSV line into fields, honouring quotes and escaped quotes. */
function parseCsvLine(line: string): string[] {
	const fields: string[] = [];
	let field = "";
	let state: "start" | "unquoted" | "quoted" | "afterQuote" = "start";

	for (let i = 0; i < line.length; i++) {
		const char = line[i] as string;

		if (state === "quoted") {
			if (char === '"') {
				if (line[i + 1] === '"') {
					field += '"';
					i++;
					continue;
				}
				state = "afterQuote";
				continue;
			}
			field += char;
			continue;
		}

		if (char === ",") {
			fields.push(field);
			field = "";
			state = "start";
			continue;
		}

		if (char === '"') {
			if (state === "start") {
				state = "quoted";
				continue;
			}
			field += char;
			state = "unquoted";
			continue;
		}

		field += char;
		state = "unquoted";
	}

	fields.push(field);
	return fields;
}

async function readCsv(csvPath: string, limit: number): Promise<{ rows: PreferenceRow[]; issues: RowIssue[]; read: number }> {
	const issues: RowIssue[] = [];
	const seen = new Map<string, PreferenceRow>();

	let read = 0;
	let header: string[] | null = null;
	let pending = "";
	let sawBom = false;
	let line = 0;

	const stream = fs.createReadStream(csvPath, { encoding: "utf8" });

	for await (const chunk of stream) {
		let text: string = chunk;
		if (!sawBom) {
			sawBom = true;
			text = text.replace(/^\uFEFF/, "");
		}

		pending += text;
		const parts = pending.split(/\r?\n/);
		pending = parts.pop() ?? "";

		for (const raw of parts) {
			line++;
			if (raw === "") {
				continue;
			}
			processLine(raw);
		}

		if (limit > 0 && seen.size >= limit) {
			break;
		}
	}

	stream.destroy();

	if (pending !== "" && (limit === 0 || seen.size < limit)) {
		line++;
		processLine(pending);
	}

	if (header === null) {
		fail("CSV is empty: no header row found.");
	}

	return { rows: [...seen.values()], issues, read };

	function processLine(raw: string): void {
		const fields = parseCsvLine(raw);

		if (header === null) {
			header = fields.map((field) => field.trim());
			if (header.length !== EXPECTED_HEADER.length || EXPECTED_HEADER.some((name, i) => header?.[i] !== name)) {
				issues.push({ line, reason: `header: expected ${EXPECTED_HEADER.join(",")} but got ${header.join(",")}` });
			}
			return;
		}

		if (limit > 0 && seen.size >= limit) {
			return;
		}

		read++;

		if (fields.length !== EXPECTED_HEADER.length) {
			issues.push({ line, reason: `expected ${EXPECTED_HEADER.length} fields, got ${fields.length}` });
			return;
		}

		const email = (fields[4] ?? "").trim().toLowerCase();
		if (email === "") {
			issues.push({ line, reason: "email is empty" });
			return;
		}
		if (email.length > EMAIL_MAX_LENGTH) {
			issues.push({ line, reason: `email longer than ${EMAIL_MAX_LENGTH} characters` });
			return;
		}
		if (!EMAIL_PATTERN.test(email)) {
			issues.push({ line, reason: `malformed email "${email}"` });
			return;
		}

		const createdAt = (fields[1] ?? "").trim();
		if (!TIMESTAMP_PATTERN.test(createdAt)) {
			issues.push({ line, reason: `unparseable created_at "${createdAt}"` });
			return;
		}

		const unsubscribed = (fields[5] ?? "").trim();
		if (unsubscribed !== "true" && unsubscribed !== "false") {
			issues.push({ line, reason: `unsubscribed must be true/false, got "${unsubscribed}"` });
			return;
		}

		const existing = seen.get(email);
		if (existing === undefined) {
			seen.set(email, {
				email,
				allowPromotionalEmail: unsubscribed !== "true",
				createdAt,
			});
			return;
		}

		// Duplicate contact record: an opt-out on any record wins, and the
		// earliest creation time is kept.
		if (unsubscribed === "true") {
			existing.allowPromotionalEmail = false;
		}
		if (createdAt < existing.createdAt) {
			existing.createdAt = createdAt;
		}
	}
}

/** Builds the VALUES list for exactly `count` tuples, starting at parameter 1. */
function valuePlaceholders(count: number): string {
	const tuples: string[] = [];
	for (let row = 0; row < count; row++) {
		const params: string[] = [];
		for (let col = 0; col < 5; col++) {
			params.push(`$${row * 5 + col + 1}`);
		}
		tuples.push(`(${params.join(",")})`);
	}
	return tuples.join(",");
}

async function writeRows(client: pg.Client, rows: PreferenceRow[], batchSize: number): Promise<RunStats> {
	const stats: RunStats = { inserted: 0, updated: 0 };

	await client.query("BEGIN");

	const updatedAt = new Date().toISOString();

	try {
		for (let offset = 0; offset < rows.length; offset += batchSize) {
			const batch = rows.slice(offset, offset + batchSize);
			const sql = INSERT_SQL.replace("%s", valuePlaceholders(batch.length));

			const params: unknown[] = [];
			for (const row of batch) {
				params.push(row.email, row.allowPromotionalEmail, true, row.createdAt, updatedAt);
			}

			const result = await client.query(sql, params);
			for (const record of result.rows as Array<{ inserted: boolean }>) {
				if (record.inserted) {
					stats.inserted++;
				} else {
					stats.updated++;
				}
			}
		}

		await client.query("COMMIT");
	} catch (error) {
		await client.query("ROLLBACK");
		throw error;
	}

	return stats;
}

async function main(): Promise<void> {
	loadDotEnv();
	const config = loadConfig();

	console.log(`csv:  ${config.csvPath}`);
	console.log(`db:   ${config.dbURL.replace(/:[^:@/]*@/, ":***@")}`);
	console.log(`mode: ${config.dryRun ? "dry-run (no writes)" : "live"}`);
	if (config.limit > 0) {
		console.log(`limit: ${config.limit} unique emails`);
	}
	console.log("");

	const { rows, issues, read } = await readCsv(config.csvPath, config.limit);

	console.log(`rows read:            ${read}`);
	console.log(`unique emails:        ${rows.length}`);

	if (issues.length > 0) {
		const shown = issues.slice(0, 20);
		console.log("");
		console.error(`validation failed: ${issues.length} row(s) rejected`);
		for (const issue of shown) {
			console.error(`  line ${issue.line}: ${issue.reason}`);
		}
		if (issues.length > shown.length) {
			console.error(`  ...and ${issues.length - shown.length} more`);
		}
		process.exit(1);
	}

	const unsubscribed = rows.filter((row) => !row.allowPromotionalEmail).length;
	console.log(`will be subscribed:   ${rows.length - unsubscribed}`);
	console.log(`will be unsubscribed: ${unsubscribed}`);

	if (config.dryRun) {
		console.log("");
		console.log("dry-run: nothing written");
		return;
	}

	const client = new Client({ connectionString: config.dbURL });
	await client.connect();

	try {
		const started = Date.now();
		const stats = await writeRows(client, rows, config.batchSize);
		const elapsedMs = Date.now() - started;

		console.log("");
		console.log(`inserted:  ${stats.inserted}`);
		console.log(`updated:   ${stats.updated}`);
		console.log(`elapsed:   ${elapsedMs}ms`);
	} finally {
		await client.end();
	}
}

main().catch((error: unknown) => {
	console.error("sync failed:");
	console.error(error);
	process.exit(1);
});
