/**
 * JSONL storage for xAPI Statements.
 * Provides date-based file organization, file rotation, and concurrent write safety.
 */

import fs from "fs";
import path from "path";
import { promisify } from "util";

const writeFile = promisify(fs.writeFile);
const appendFile = promisify(fs.appendFile);
const stat = promisify(fs.stat);
const readFile = promisify(fs.readFile);
const readdir = promisify(fs.readdir);

/**
 * Default configuration.
 */
const DEFAULT_CONFIG = {
    /** Base directory for log files */
    baseDir: process.env.XAPI_LOG_DIR || "/var/log/pxt/xapi",
    /** Maximum file size before rotation (100MB) */
    maxFileSize: parseInt(process.env.XAPI_MAX_FILE_SIZE || "104857600", 10),
    /** File extension for JSONL files */
    extension: ".jsonl",
};

/**
 * Current configuration.
 */
let config = { ...DEFAULT_CONFIG };

/**
 * Write locks to prevent concurrent writes to the same file.
 */
const writeLocks = new Map();

/**
 * Updates the storage configuration.
 * @param {Partial<typeof DEFAULT_CONFIG>} newConfig - Configuration to merge
 */
export function configure(newConfig) {
    config = { ...config, ...newConfig };
}

/**
 * Ensures a directory exists, creating it recursively if needed.
 * @param {string} dir - Directory path
 */
function ensureDir(dir) {
    if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
    }
}

/**
 * Generates the directory path for a given date.
 * Format: baseDir/YYYY/MM/
 * @param {Date} date - The date
 * @returns {string} Directory path
 */
export function getDateDirectory(date) {
    const year = date.getFullYear().toString();
    const month = (date.getMonth() + 1).toString().padStart(2, "0");
    return path.join(config.baseDir, year, month);
}

/**
 * Generates the base filename for a given date.
 * Format: DD.jsonl
 * @param {Date} date - The date
 * @returns {string} Base filename
 */
export function getDateFilename(date) {
    const day = date.getDate().toString().padStart(2, "0");
    return `${day}${config.extension}`;
}

/**
 * Generates the full file path for a given date.
 * @param {Date} date - The date
 * @param {number} [rotationIndex] - Optional rotation index (e.g., 1 for DD-001.jsonl)
 * @returns {string} Full file path
 */
export function getFilePath(date, rotationIndex) {
    const dir = getDateDirectory(date);
    let filename = getDateFilename(date);

    if (rotationIndex !== undefined && rotationIndex > 0) {
        const baseName = filename.replace(config.extension, "");
        filename = `${baseName}-${rotationIndex.toString().padStart(3, "0")}${config.extension}`;
    }

    return path.join(dir, filename);
}

/**
 * Gets the current rotation index for a date's log file.
 * @param {Date} date - The date
 * @returns {Promise<number>} Current rotation index (0 if no rotation)
 */
async function getCurrentRotationIndex(date) {
    const dir = getDateDirectory(date);
    const baseFilename = getDateFilename(date).replace(config.extension, "");

    if (!fs.existsSync(dir)) {
        return 0;
    }

    const files = await readdir(dir);
    const pattern = new RegExp(`^${baseFilename}(-\\d{3})?\\${config.extension}$`);

    let maxIndex = 0;
    for (const file of files) {
        const match = file.match(pattern);
        if (match) {
            if (match[1]) {
                const index = parseInt(match[1].slice(1), 10);
                maxIndex = Math.max(maxIndex, index);
            }
        }
    }

    return maxIndex;
}

/**
 * Checks if a file needs rotation based on size.
 * @param {string} filePath - Path to check
 * @returns {Promise<boolean>} True if rotation needed
 */
async function needsRotation(filePath) {
    if (!fs.existsSync(filePath)) {
        return false;
    }

    const stats = await stat(filePath);
    return stats.size >= config.maxFileSize;
}

/**
 * Gets the appropriate file path, handling rotation if needed.
 * @param {Date} date - The date
 * @returns {Promise<string>} File path to write to
 */
async function getWriteFilePath(date) {
    const dir = getDateDirectory(date);
    ensureDir(dir);

    // Start with the base file
    let rotationIndex = await getCurrentRotationIndex(date);
    let filePath = getFilePath(date, rotationIndex || undefined);

    // Check if rotation is needed
    if (await needsRotation(filePath)) {
        rotationIndex++;
        filePath = getFilePath(date, rotationIndex);
    }

    return filePath;
}

/**
 * Acquires a write lock for a file path.
 * @param {string} filePath - File to lock
 * @returns {Promise<void>}
 */
async function acquireLock(filePath) {
    while (writeLocks.get(filePath)) {
        await new Promise((resolve) => setTimeout(resolve, 10));
    }
    writeLocks.set(filePath, true);
}

/**
 * Releases a write lock for a file path.
 * @param {string} filePath - File to unlock
 */
function releaseLock(filePath) {
    writeLocks.delete(filePath);
}

/**
 * Appends a single xAPI Statement to the JSONL file.
 * @param {object} statement - The xAPI Statement to append
 * @param {Date} [date] - Optional date (defaults to statement timestamp or now)
 * @returns {Promise<{filePath: string, success: boolean, error?: string}>}
 */
export async function appendStatement(statement, date) {
    const timestamp = date || (statement.timestamp ? new Date(statement.timestamp) : new Date());
    const filePath = await getWriteFilePath(timestamp);

    await acquireLock(filePath);
    try {
        const line = JSON.stringify(statement) + "\n";
        await appendFile(filePath, line, "utf8");
        return { filePath, success: true };
    } catch (error) {
        return { filePath, success: false, error: error.message };
    } finally {
        releaseLock(filePath);
    }
}

/**
 * Appends multiple xAPI Statements to JSONL files.
 * Groups statements by date for efficient writing.
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

    // Write each date group
    for (const [dateKey, group] of byDate) {
        const filePath = await getWriteFilePath(group.date);

        await acquireLock(filePath);
        try {
            const lines = group.statements.map((s) => JSON.stringify(s)).join("\n") + "\n";
            await appendFile(filePath, lines, "utf8");
            result.success += group.statements.length;
        } catch (error) {
            result.failed += group.statements.length;
            result.errors.push(`${dateKey}: ${error.message}`);
        } finally {
            releaseLock(filePath);
        }
    }

    return result;
}

/**
 * Reads statements from a file.
 * @param {string} filePath - Path to the JSONL file
 * @returns {Promise<object[]>} Array of parsed statements
 */
export async function readStatements(filePath) {
    if (!fs.existsSync(filePath)) {
        return [];
    }

    const content = await readFile(filePath, "utf8");
    const lines = content.split("\n").filter((line) => line.trim());

    return lines.map((line) => {
        try {
            return JSON.parse(line);
        } catch {
            return null;
        }
    }).filter(Boolean);
}

/**
 * Reads statements for a date range.
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
        // Get all files for this date (including rotated)
        const dir = getDateDirectory(current);

        if (fs.existsSync(dir)) {
            const baseFilename = getDateFilename(current).replace(config.extension, "");
            const files = await readdir(dir);
            const pattern = new RegExp(`^${baseFilename}(-\\d{3})?\\${config.extension}$`);

            const matchingFiles = files.filter((f) => pattern.test(f)).sort();

            for (const file of matchingFiles) {
                const filePath = path.join(dir, file);
                const statements = await readStatements(filePath);

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
        }

        // Move to next day
        current.setDate(current.getDate() + 1);
    }
}

/**
 * Gets file statistics for a date.
 * @param {Date} date - The date
 * @returns {Promise<{files: Array<{path: string, size: number}>, totalSize: number}>}
 */
export async function getDateStats(date) {
    const dir = getDateDirectory(date);
    const result = { files: [], totalSize: 0 };

    if (!fs.existsSync(dir)) {
        return result;
    }

    const baseFilename = getDateFilename(date).replace(config.extension, "");
    const files = await readdir(dir);
    const pattern = new RegExp(`^${baseFilename}(-\\d{3})?\\${config.extension}$`);

    for (const file of files) {
        if (pattern.test(file)) {
            const filePath = path.join(dir, file);
            const stats = await stat(filePath);
            result.files.push({ path: filePath, size: stats.size });
            result.totalSize += stats.size;
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
