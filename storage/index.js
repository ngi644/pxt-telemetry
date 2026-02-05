/**
 * Storage backend selector.
 * Exports the appropriate storage module based on STORAGE_BACKEND environment variable.
 *
 * Usage:
 *   STORAGE_BACKEND=local  -> uses jsonl.js (default)
 *   STORAGE_BACKEND=gcs    -> uses gcs.js
 */

const backend = process.env.STORAGE_BACKEND || "local";

let storage;

if (backend === "gcs") {
    console.log("[storage] Using Google Cloud Storage backend");
    storage = await import("./gcs.js");
} else {
    console.log("[storage] Using local filesystem backend");
    storage = await import("./jsonl.js");
}

// Re-export all functions from the selected backend
export const {
    configure,
    appendStatement,
    appendStatements,
    readStatements,
    readStatementsInRange,
    getDateStats,
    getConfig,
} = storage;

// Export backend name for debugging
export const backendName = backend;
