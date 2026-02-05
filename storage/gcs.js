/**
 * Google Cloud Storage backend for xAPI Statements.
 * Provides the same interface as jsonl.js but stores to GCS.
 *
 * Object path format: xapi/{YYYY}/{MM}/{DD}/{timestamp}-{uuid}.jsonl
 * Each write creates a unique object to avoid conflicts.
 */

import { Storage } from "@google-cloud/storage";
import { randomUUID } from "crypto";

/**
 * Default configuration.
 */
const DEFAULT_CONFIG = {
    /** GCS bucket name */
    bucket: process.env.GCS_BUCKET || "pxt-xapi-logs",
    /** Base prefix for objects */
    prefix: process.env.GCS_PREFIX || "xapi",
    /** GCS project ID (optional if using ADC) */
    projectId: process.env.GCS_PROJECT_ID || undefined,
};

/**
 * Current configuration.
 */
let config = { ...DEFAULT_CONFIG };

/**
 * GCS Storage client (lazy initialized).
 */
let storage = null;
let bucket = null;

/**
 * Gets or creates the GCS client.
 * @returns {Storage}
 */
function getStorage() {
    if (!storage) {
        const options = {};
        if (config.projectId) {
            options.projectId = config.projectId;
        }
        storage = new Storage(options);
        bucket = storage.bucket(config.bucket);
    }
    return storage;
}

/**
 * Gets the bucket.
 * @returns {Bucket}
 */
function getBucket() {
    getStorage();
    return bucket;
}

/**
 * Updates the storage configuration.
 * @param {Partial<typeof DEFAULT_CONFIG>} newConfig - Configuration to merge
 */
export function configure(newConfig) {
    config = { ...config, ...newConfig };
    // Reset clients to pick up new config
    storage = null;
    bucket = null;
}

/**
 * Generates the object prefix for a given date.
 * Format: prefix/YYYY/MM/DD/
 * @param {Date} date - The date
 * @returns {string} Object prefix
 */
export function getDatePrefix(date) {
    const year = date.getFullYear().toString();
    const month = (date.getMonth() + 1).toString().padStart(2, "0");
    const day = date.getDate().toString().padStart(2, "0");
    return `${config.prefix}/${year}/${month}/${day}/`;
}

/**
 * Generates a unique object name for a batch.
 * Format: prefix/YYYY/MM/DD/{timestamp}-{uuid}.jsonl
 * @param {Date} date - The date
 * @returns {string} Object name
 */
function generateObjectName(date) {
    const prefix = getDatePrefix(date);
    const timestamp = Date.now();
    const uuid = randomUUID().slice(0, 8);
    return `${prefix}${timestamp}-${uuid}.jsonl`;
}

/**
 * Appends a single xAPI Statement to GCS.
 * Creates a new object for this statement.
 * @param {object} statement - The xAPI Statement to append
 * @param {Date} [date] - Optional date (defaults to statement timestamp or now)
 * @returns {Promise<{filePath: string, success: boolean, error?: string}>}
 */
export async function appendStatement(statement, date) {
    const timestamp = date || (statement.timestamp ? new Date(statement.timestamp) : new Date());
    const objectName = generateObjectName(timestamp);

    try {
        const file = getBucket().file(objectName);
        const content = JSON.stringify(statement) + "\n";

        await file.save(content, {
            contentType: "application/x-ndjson",
            resumable: false,
        });

        return { filePath: `gs://${config.bucket}/${objectName}`, success: true };
    } catch (error) {
        return {
            filePath: `gs://${config.bucket}/${objectName}`,
            success: false,
            error: error.message
        };
    }
}

/**
 * Appends multiple xAPI Statements to GCS.
 * Groups statements by date and creates one object per date group.
 * @param {object[]} statements - Array of xAPI Statements
 * @returns {Promise<{total: number, success: number, failed: number, errors: string[]}>}
 */
export async function appendStatements(statements) {
    const result = {
        total: statements.length,
        success: 0,
        failed: 0,
        errors: [],
    };

    // Group statements by date
    const byDate = new Map();
    for (const statement of statements) {
        const timestamp = statement.timestamp ? new Date(statement.timestamp) : new Date();
        const dateKey = timestamp.toISOString().split("T")[0];

        if (!byDate.has(dateKey)) {
            byDate.set(dateKey, { date: timestamp, statements: [] });
        }
        byDate.get(dateKey).statements.push(statement);
    }

    // Write each date group as a single object
    for (const [dateKey, group] of byDate) {
        const objectName = generateObjectName(group.date);

        try {
            const file = getBucket().file(objectName);
            const content = group.statements.map(s => JSON.stringify(s)).join("\n") + "\n";

            await file.save(content, {
                contentType: "application/x-ndjson",
                resumable: false,
            });

            result.success += group.statements.length;
        } catch (error) {
            result.failed += group.statements.length;
            result.errors.push(`${dateKey}: ${error.message}`);
        }
    }

    return result;
}

/**
 * Reads statements from a GCS object.
 * @param {string} objectName - Object name (without gs://bucket/)
 * @returns {Promise<object[]>} Array of parsed statements
 */
export async function readStatements(objectName) {
    try {
        const file = getBucket().file(objectName);
        const [content] = await file.download();
        const lines = content.toString("utf8").split("\n").filter(line => line.trim());

        return lines.map(line => {
            try {
                return JSON.parse(line);
            } catch {
                return null;
            }
        }).filter(Boolean);
    } catch (error) {
        if (error.code === 404) {
            return [];
        }
        throw error;
    }
}

/**
 * Reads statements for a date range.
 * Lists all objects in each day's prefix and reads them.
 * @param {Date} startDate - Start date (inclusive)
 * @param {Date} endDate - End date (inclusive)
 * @returns {AsyncGenerator<object>} Async generator of statements
 */
export async function* readStatementsInRange(startDate, endDate) {
    const current = new Date(startDate);
    current.setHours(0, 0, 0, 0);

    const end = new Date(endDate);
    end.setHours(23, 59, 59, 999);

    while (current <= end) {
        const prefix = getDatePrefix(current);

        try {
            // List all objects with this prefix
            const [files] = await getBucket().getFiles({ prefix });

            // Sort by name (which includes timestamp) for chronological order
            const sortedFiles = files.sort((a, b) => a.name.localeCompare(b.name));

            for (const file of sortedFiles) {
                const statements = await readStatements(file.name);

                for (const statement of statements) {
                    // Filter by timestamp if present
                    if (statement.timestamp) {
                        const stmtDate = new Date(statement.timestamp);
                        if (stmtDate >= startDate && stmtDate <= end) {
                            yield statement;
                        }
                    } else {
                        yield statement;
                    }
                }
            }
        } catch (error) {
            // Skip if prefix doesn't exist
            if (error.code !== 404) {
                console.error(`[gcs] Error reading ${prefix}:`, error.message);
            }
        }

        // Move to next day
        current.setDate(current.getDate() + 1);
    }
}

/**
 * Gets storage statistics for a date.
 * @param {Date} date - The date
 * @returns {Promise<{files: Array<{path: string, size: number}>, totalSize: number}>}
 */
export async function getDateStats(date) {
    const result = { files: [], totalSize: 0 };
    const prefix = getDatePrefix(date);

    try {
        const [files] = await getBucket().getFiles({ prefix });

        for (const file of files) {
            const [metadata] = await file.getMetadata();
            const size = parseInt(metadata.size, 10) || 0;
            result.files.push({
                path: `gs://${config.bucket}/${file.name}`,
                size
            });
            result.totalSize += size;
        }
    } catch (error) {
        if (error.code !== 404) {
            console.error(`[gcs] Error getting stats for ${prefix}:`, error.message);
        }
    }

    return result;
}

/**
 * Gets the current configuration.
 * @returns {typeof DEFAULT_CONFIG}
 */
export function getConfig() {
    return { ...config };
}
