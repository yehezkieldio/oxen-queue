import mysql, { type Pool, type PoolOptions, type ResultSetHeader, type RowDataPacket } from 'mysql2/promise'

import type { AddJobOptions, ExtraFieldMap, JobStatus, StoredJob } from './types.js'
import {
    assertIdentifier,
    dedupeHash,
    jitteredRetryDelay,
    parseJson,
    serializeJson,
    toDate,
} from './utils.js'

export interface EnqueueRecord<Body> {
    body: Body
    options: AddJobOptions
}

export interface StorageOptions<Body> {
    mysql: PoolOptions
    table: string
    queueName: string
    extraFields?: ExtraFieldMap<Body>
}

interface JobRow extends RowDataPacket {
    id: number | bigint
    body: string | null
    attempt: number
    started_ts: Date
}

interface CountRow extends RowDataPacket {
    total: number
}

interface SchemaColumnRow extends RowDataPacket {
    COLUMN_NAME: string
}

export class MySqlStorage<Body> {
    readonly pool: Pool
    readonly tableName: string
    readonly tableIdentifier: string
    readonly queueName: string
    readonly extraFields: ExtraFieldMap<Body>

    constructor(options: StorageOptions<Body>) {
        this.pool = mysql.createPool(options.mysql)
        this.tableName = options.table
        this.tableIdentifier = assertIdentifier(options.table)
        this.queueName = options.queueName
        this.extraFields = options.extraFields ?? {}
    }

    async close(): Promise<void> {
        await this.pool.end()
    }

    async addJobs(records: EnqueueRecord<Body>[]): Promise<void> {
        if (records.length === 0) {
            return
        }

        const extraFieldNames = Object.keys(this.extraFields)
        const fields = [
            'body',
            'queue_name',
            'dedupe_key',
            'priority',
            'created_ts',
            'available_ts',
            ...extraFieldNames,
        ]
        const values = records.map(({ body, options }) => {
            const availableAt = toDate(options.startTime)
            const row: unknown[] = [
                serializeJson(body),
                this.queueName,
                options.uniqueKey === undefined ? null : dedupeHash(this.queueName, options.uniqueKey),
                typeof options.priority === 'number' ? options.priority : Date.now(),
                new Date(),
                availableAt,
            ]

            for (const field of extraFieldNames) {
                row.push(serializeJson(this.extraFields[field]?.(body)))
            }

            return row
        })

        const columns = fields.map(assertIdentifier).join(', ')
        const updatePriority =
            'priority = IF(priority > VALUES(priority), VALUES(priority), priority), available_ts = LEAST(available_ts, VALUES(available_ts))'

        await this.query(
            `INSERT INTO ${this.tableIdentifier} (${columns}) VALUES ? ON DUPLICATE KEY UPDATE ${updatePriority}`,
            [values]
        )
    }

    async claimJobs(limit: number): Promise<StoredJob<Body>[]> {
        const batchId = String(Date.now()) + '-' + Math.floor(Math.random() * Number.MAX_SAFE_INTEGER)

        const result = await this.query<ResultSetHeader>(
            `
            UPDATE ${this.tableIdentifier} AS main
            INNER JOIN (
                SELECT id FROM ${this.tableIdentifier} FORCE INDEX (oxen_claim)
                WHERE batch_id IS NULL
                AND status = 'waiting'
                AND queue_name = ?
                AND available_ts <= NOW(3)
                ORDER BY priority ASC, id ASC
                LIMIT ${Math.max(1, Math.floor(limit))}
            ) sub ON sub.id = main.id
            SET batch_id = ?, status = 'processing', started_ts = NOW(3), attempt = attempt + 1
            `,
            [this.queueName, batchId]
        )

        if (result.changedRows === 0) {
            return []
        }

        const rows = await this.query<JobRow[]>(
            `SELECT id, body, attempt, started_ts FROM ${this.tableIdentifier} WHERE batch_id = ? ORDER BY priority ASC, id ASC`,
            [batchId]
        )

        return rows.map(row => ({
            id: BigInt(row.id),
            body: parseJson<Body>(row.body),
            queueName: this.queueName,
            attempt: row.attempt,
            startedAt: row.started_ts,
        }))
    }

    async markSuccess(jobId: bigint, result: unknown): Promise<void> {
        await this.query(
            `
            UPDATE ${this.tableIdentifier}
            SET result = ?, dedupe_key = NULL, status = 'success', running_time_ms = TIMESTAMPDIFF(MICROSECOND, started_ts, NOW(3)) / 1000
            WHERE id = ?
            LIMIT 1
            `,
            [serializeJson(result), jobId.toString()]
        )
    }

    async markError(jobId: bigint, error: unknown): Promise<void> {
        await this.query(
            `
            UPDATE ${this.tableIdentifier}
            SET result = ?, dedupe_key = NULL, status = 'error', running_time_ms = TIMESTAMPDIFF(MICROSECOND, started_ts, NOW(3)) / 1000
            WHERE id = ?
            LIMIT 1
            `,
            [formatError(error), jobId.toString()]
        )
    }

    async retryJob(jobId: bigint, delayMs: number): Promise<void> {
        await this.query(
            `
            UPDATE ${this.tableIdentifier}
            SET status = 'waiting', batch_id = NULL, started_ts = NULL, available_ts = DATE_ADD(NOW(3), INTERVAL ? MICROSECOND), recovered = recovered + 1
            WHERE id = ?
            LIMIT 1
            `,
            [delayMs * 1000, jobId.toString()]
        )
    }

    async recoverStuckJobs(timeoutMs: number): Promise<number> {
        const result = await this.query<ResultSetHeader>(
            `
            UPDATE ${this.tableIdentifier}
            SET status = 'waiting', batch_id = NULL, started_ts = NULL, recovered = recovered + 1
            WHERE status = 'processing'
            AND started_ts < (NOW(3) - INTERVAL ? MICROSECOND)
            AND queue_name = ?
            `,
            [timeoutMs * 1000, this.queueName]
        )

        return result.changedRows
    }

    async markStuckJobs(timeoutMs: number): Promise<number> {
        const result = await this.query<ResultSetHeader>(
            `
            UPDATE ${this.tableIdentifier}
            SET status = 'stuck', dedupe_key = NULL, recovered = recovered + 1
            WHERE status = 'processing'
            AND started_ts < (NOW(3) - INTERVAL ? MICROSECOND)
            AND queue_name = ?
            `,
            [timeoutMs * 1000, this.queueName]
        )

        return result.changedRows
    }

    async countByStatus(status: JobStatus): Promise<number> {
        const rows = await this.query<CountRow[]>(
            `SELECT COUNT(*) AS total FROM ${this.tableIdentifier} WHERE queue_name = ? AND status = ?`,
            [this.queueName, status]
        )
        return rows[0]?.total ?? 0
    }

    async createSchema(extraColumns: string[] = []): Promise<void> {
        const extras = extraColumns.map(name => `${assertIdentifier(name)} JSON DEFAULT NULL`).join(',\n')
        const extraSql = extras ? `,\n${extras}` : ''
        await this.query(`
            CREATE TABLE IF NOT EXISTS ${this.tableIdentifier} (
                id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                batch_id VARCHAR(80) DEFAULT NULL,
                queue_name VARCHAR(200) NOT NULL,
                created_ts DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
                available_ts DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
                started_ts DATETIME(3) DEFAULT NULL,
                body JSON DEFAULT NULL,
                status ENUM('waiting','processing','success','error','stuck') NOT NULL DEFAULT 'waiting',
                result MEDIUMTEXT DEFAULT NULL,
                recovered INT UNSIGNED NOT NULL DEFAULT 0,
                attempt INT UNSIGNED NOT NULL DEFAULT 0,
                running_time_ms INT UNSIGNED DEFAULT NULL,
                dedupe_key CHAR(64) DEFAULT NULL,
                priority BIGINT DEFAULT NULL,
                PRIMARY KEY (id),
                UNIQUE KEY oxen_dedupe (dedupe_key),
                KEY oxen_created (created_ts),
                KEY oxen_status (status),
                KEY oxen_claim (queue_name, batch_id, status, available_ts, priority, id),
                KEY oxen_batch (batch_id, priority, id),
                KEY oxen_started (started_ts, queue_name, status)
                ${extraSql}
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        `)
    }

    async inspectSchema(): Promise<{ exists: boolean; version: 'legacy' | 'v2' | 'unknown'; columns: string[] }> {
        const tableParts = this.tableName.split('.')
        const table = tableParts.at(-1) ?? this.tableName
        const schema = tableParts.length > 1 ? tableParts[0] : undefined
        const params = schema ? [schema, table] : [table]
        const schemaFilter = schema ? 'TABLE_SCHEMA = ? AND TABLE_NAME = ?' : 'TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?'
        const rows = await this.query<SchemaColumnRow[]>(
            `
            SELECT COLUMN_NAME
            FROM information_schema.COLUMNS
            WHERE ${schemaFilter}
            ORDER BY ORDINAL_POSITION
            `,
            params
        )
        const columns = rows.map(row => row.COLUMN_NAME)
        if (columns.length === 0) {
            return { exists: false, version: 'unknown', columns }
        }
        if (columns.includes('queue_name') && columns.includes('dedupe_key') && columns.includes('available_ts')) {
            return { exists: true, version: 'v2', columns }
        }
        if (columns.includes('job_type') && columns.includes('unique_key') && columns.includes('created_ts')) {
            return { exists: true, version: 'legacy', columns }
        }
        return { exists: true, version: 'unknown', columns }
    }

    async migrateLegacyTable(legacyTable: string): Promise<number> {
        const legacyIdentifier = assertIdentifier(legacyTable)
        const result = await this.query<ResultSetHeader>(
            `
            INSERT INTO ${this.tableIdentifier} (
                id,
                batch_id,
                queue_name,
                created_ts,
                available_ts,
                started_ts,
                body,
                status,
                result,
                recovered,
                attempt,
                running_time_ms,
                dedupe_key,
                priority
            )
            SELECT
                id,
                CAST(batch_id AS CHAR),
                job_type,
                COALESCE(created_ts, CURRENT_TIMESTAMP(3)),
                COALESCE(created_ts, CURRENT_TIMESTAMP(3)),
                started_ts,
                CASE
                    WHEN body IS NULL THEN NULL
                    WHEN JSON_VALID(body) THEN body
                    ELSE JSON_QUOTE(body)
                END,
                CASE
                    WHEN status IN ('waiting','processing','success','error','stuck') THEN status
                    ELSE 'error'
                END,
                result,
                recovered,
                CASE WHEN status = 'processing' THEN 1 ELSE 0 END,
                CASE WHEN running_time IS NULL THEN NULL ELSE running_time * 1000 END,
                CASE WHEN unique_key IS NULL THEN NULL ELSE SHA2(CONCAT(job_type, CHAR(0), unique_key), 256) END,
                priority
            FROM ${legacyIdentifier}
            ON DUPLICATE KEY UPDATE id = id
            `
        )
        return result.affectedRows
    }

    async deleteTableForTests(): Promise<void> {
        await this.query(`DROP TABLE IF EXISTS ${this.tableIdentifier}`)
    }

    async selectAllForTests(): Promise<RowDataPacket[]> {
        return this.query<RowDataPacket[]>(`SELECT * FROM ${this.tableIdentifier} ORDER BY id ASC`)
    }

    private async query<T extends ResultSetHeader | RowDataPacket[]>(
        sql: string,
        params?: unknown[]
    ): Promise<T> {
        const retryable = ['ER_LOCK_WAIT_TIMEOUT', 'ER_LOCK_DEADLOCK', 'ETIMEDOUT', 'ECONNREFUSED']
        let retries = 5

        while (retries > 0) {
            try {
                const [result] = await this.pool.query(sql, params)
                return result as T
            } catch (error) {
                retries -= 1
                const message = error instanceof Error ? error.message : String(error)
                if (retries === 0 || !retryable.some(fragment => message.includes(fragment))) {
                    throw error
                }
                await new Promise(resolve => setTimeout(resolve, jitteredRetryDelay()))
            }
        }

        throw new Error('unreachable query retry state')
    }
}

function formatError(error: unknown): string {
    if (error instanceof Error) {
        return error.stack ?? error.message
    }
    return String(error)
}
