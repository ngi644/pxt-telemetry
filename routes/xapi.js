/**
 * xAPI Statements API routes.
 * Handles receiving, validating, and storing xAPI Statements.
 */

import express from "express";
import { appendStatement, appendStatements, readStatementsInRange, getDateStats } from "../storage/index.js";

const router = express.Router();

/**
 * Required fields for a valid xAPI Statement.
 */
const REQUIRED_FIELDS = ["id", "actor", "verb", "object", "context", "timestamp"];

/**
 * Valid verb IDs.
 */
const VALID_VERB_IDS = [
    "urn:xapi:picapica-2d:verb:placed",
    "urn:xapi:picapica-2d:verb:removed",
    "urn:xapi:picapica-2d:verb:edited",
    "urn:xapi:picapica-2d:verb:executed",
    "urn:xapi:picapica-2d:verb:downloaded",
    "urn:xapi:picapica-2d:verb:saved",
    "urn:xapi:picapica-2d:verb:opened",
    "urn:xapi:picapica-2d:verb:closed",
    "urn:xapi:picapica-2d:verb:moved",
    "urn:xapi:picapica-2d:verb:connected",
    "urn:xapi:picapica-2d:verb:disconnected",
    "urn:xapi:picapica-2d:verb:nested",
    "urn:xapi:picapica-2d:verb:selected",
];

/**
 * Valid activity types.
 */
const VALID_ACTIVITY_TYPES = [
    "urn:xapi:picapica-2d:activity-type:block",
    "urn:xapi:picapica-2d:activity-type:code",
    "urn:xapi:picapica-2d:activity-type:project",
    "urn:xapi:picapica-2d:activity-type:device",
];

/**
 * Validates a UUID format.
 * @param {string} uuid - The string to validate
 * @returns {boolean}
 */
function isValidUUID(uuid) {
    const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
    return uuidRegex.test(uuid);
}

/**
 * Validates an xAPI Statement.
 * @param {object} statement - The statement to validate
 * @returns {{valid: boolean, errors: string[]}}
 */
function validateStatement(statement) {
    const errors = [];

    // Check required fields
    for (const field of REQUIRED_FIELDS) {
        if (!statement[field]) {
            errors.push(`Missing required field: ${field}`);
        }
    }

    if (errors.length > 0) {
        return { valid: false, errors };
    }

    // Validate id format
    if (!isValidUUID(statement.id)) {
        errors.push("Invalid statement id: must be a valid UUID");
    }

    // Validate actor
    if (!statement.actor.account?.homePage || !statement.actor.account?.name) {
        errors.push("Invalid actor: must have account with homePage and name");
    }

    // Validate verb
    if (!statement.verb.id || !statement.verb.display) {
        errors.push("Invalid verb: must have id and display");
    } else if (!VALID_VERB_IDS.includes(statement.verb.id)) {
        errors.push(`Invalid verb id: ${statement.verb.id}`);
    }

    // Validate object
    if (!statement.object.id || !statement.object.definition?.type) {
        errors.push("Invalid object: must have id and definition.type");
    } else if (!VALID_ACTIVITY_TYPES.includes(statement.object.definition.type)) {
        errors.push(`Invalid activity type: ${statement.object.definition.type}`);
    }

    // Validate context
    if (!statement.context.registration) {
        errors.push("Invalid context: must have registration");
    } else if (!isValidUUID(statement.context.registration)) {
        errors.push("Invalid context.registration: must be a valid UUID");
    }

    // Validate timestamp
    const timestamp = new Date(statement.timestamp);
    if (isNaN(timestamp.getTime())) {
        errors.push("Invalid timestamp: must be a valid ISO 8601 date");
    }

    return { valid: errors.length === 0, errors };
}

/**
 * POST /api/xapi/statements
 * Receive and store xAPI Statements.
 * Accepts single statement or batch { statements: [...] }
 */
router.post("/statements", express.json({ limit: "10mb" }), async (req, res) => {
    try {
        const body = req.body;

        // Debug: log incoming request
        console.log("[xapi] POST /statements received, body keys:", Object.keys(body || {}));

        // Determine if single statement or batch
        let statements;
        if (Array.isArray(body.statements)) {
            statements = body.statements;
        } else if (body.id && body.actor && body.verb) {
            statements = [body];
        } else {
            return res.status(400).json({
                error: "Invalid request body",
                details: ["Expected single statement or { statements: [...] }"],
            });
        }

        // Validate batch size
        if (statements.length === 0) {
            return res.status(400).json({
                error: "Empty batch",
                details: ["At least one statement is required"],
            });
        }

        if (statements.length > 100) {
            return res.status(400).json({
                error: "Batch too large",
                details: ["Maximum 100 statements per batch"],
            });
        }

        // Validate all statements
        const validationErrors = [];
        for (let i = 0; i < statements.length; i++) {
            const result = validateStatement(statements[i]);
            if (!result.valid) {
                validationErrors.push({
                    index: i,
                    id: statements[i].id || "unknown",
                    errors: result.errors,
                });
            }
        }

        if (validationErrors.length > 0) {
            console.log("[xapi] Validation failed:", JSON.stringify(validationErrors));
            return res.status(400).json({
                error: "Validation failed",
                details: validationErrors,
            });
        }

        // Add server metadata
        const ingestTime = new Date().toISOString();
        const srcIp = (req.headers["x-forwarded-for"] || req.socket.remoteAddress || "").toString();

        for (const statement of statements) {
            statement._meta = {
                ingestTime,
                srcIp,
            };
        }

        // Store statements
        if (statements.length === 1) {
            const result = await appendStatement(statements[0]);
            if (!result.success) {
                console.error("[xapi] Storage error:", result.error);
                return res.status(500).json({
                    error: "Storage error",
                    details: [result.error],
                });
            }
        } else {
            const result = await appendStatements(statements);
            if (result.failed > 0) {
                console.error("[xapi] Storage errors:", result.errors);
                if (result.success === 0) {
                    return res.status(500).json({
                        error: "Storage error",
                        details: result.errors,
                    });
                }
                // Partial success
                return res.status(207).json({
                    message: "Partial success",
                    total: result.total,
                    success: result.success,
                    failed: result.failed,
                    errors: result.errors,
                });
            }
        }

        // Success
        console.log("[xapi] Stored", statements.length, "statement(s)");
        return res.sendStatus(204);
    } catch (error) {
        console.error("[xapi] Error processing statements:", error);
        return res.status(500).json({
            error: "Internal server error",
            details: [error.message],
        });
    }
});

/**
 * GET /api/xapi/statements
 * Query statements by date range.
 * Query params: startDate, endDate, actorId (optional)
 */
router.get("/statements", async (req, res) => {
    try {
        const { startDate, endDate, actorId } = req.query;

        // Validate required params
        if (!startDate || !endDate) {
            return res.status(400).json({
                error: "Missing required parameters",
                details: ["startDate and endDate are required"],
            });
        }

        const start = new Date(startDate);
        const end = new Date(endDate);

        if (isNaN(start.getTime()) || isNaN(end.getTime())) {
            return res.status(400).json({
                error: "Invalid date format",
                details: ["Dates must be valid ISO 8601 format"],
            });
        }

        if (start > end) {
            return res.status(400).json({
                error: "Invalid date range",
                details: ["startDate must be before or equal to endDate"],
            });
        }

        // Limit date range to prevent huge queries
        const daysDiff = (end - start) / (1000 * 60 * 60 * 24);
        if (daysDiff > 31) {
            return res.status(400).json({
                error: "Date range too large",
                details: ["Maximum 31 days per query"],
            });
        }

        // Set up streaming response
        res.setHeader("Content-Type", "application/x-ndjson");
        res.setHeader("Transfer-Encoding", "chunked");

        let count = 0;
        const maxResults = 10000;

        for await (const statement of readStatementsInRange(start, end)) {
            // Filter by actorId if specified
            if (actorId && statement.actor?.account?.name !== actorId) {
                continue;
            }

            // Check result limit
            if (count >= maxResults) {
                res.write(JSON.stringify({ _truncated: true, count }) + "\n");
                break;
            }

            res.write(JSON.stringify(statement) + "\n");
            count++;
        }

        res.end();
    } catch (error) {
        console.error("[xapi] Error querying statements:", error);
        if (!res.headersSent) {
            return res.status(500).json({
                error: "Internal server error",
                details: [error.message],
            });
        }
        res.end();
    }
});

/**
 * GET /api/xapi/statements/:id
 * Get a single statement by ID.
 * Note: This requires scanning files, so it may be slow.
 */
router.get("/statements/:id", async (req, res) => {
    try {
        const { id } = req.params;

        if (!isValidUUID(id)) {
            return res.status(400).json({
                error: "Invalid statement ID",
                details: ["ID must be a valid UUID"],
            });
        }

        // Search in recent files (last 7 days)
        const end = new Date();
        const start = new Date();
        start.setDate(start.getDate() - 7);

        for await (const statement of readStatementsInRange(start, end)) {
            if (statement.id === id) {
                return res.json(statement);
            }
        }

        return res.status(404).json({
            error: "Statement not found",
            details: [`No statement found with id: ${id}`],
        });
    } catch (error) {
        console.error("[xapi] Error getting statement:", error);
        return res.status(500).json({
            error: "Internal server error",
            details: [error.message],
        });
    }
});

/**
 * GET /api/xapi/stats
 * Get storage statistics for a date.
 */
router.get("/stats", async (req, res) => {
    try {
        const { date } = req.query;
        const targetDate = date ? new Date(date) : new Date();

        if (isNaN(targetDate.getTime())) {
            return res.status(400).json({
                error: "Invalid date format",
                details: ["Date must be valid ISO 8601 format"],
            });
        }

        const stats = await getDateStats(targetDate);

        return res.json({
            date: targetDate.toISOString().split("T")[0],
            ...stats,
        });
    } catch (error) {
        console.error("[xapi] Error getting stats:", error);
        return res.status(500).json({
            error: "Internal server error",
            details: [error.message],
        });
    }
});

export default router;
