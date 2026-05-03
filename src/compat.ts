import { OxenQueue } from './queue.js'
import { errorMessages, OxenQueueError } from './errors.js'
import type { LegacyProcessOptions, LegacyQueueOptions, QueueDebugSnapshot } from './types.js'

export const error_messages = {
    no_job_type: errorMessages.noJobType,
    already_processing: errorMessages.alreadyProcessing,
    work_fn_missing: errorMessages.workFnMissing,
    invalid_concurrency: errorMessages.invalidConcurrency,
    no_mysql_connection: errorMessages.noLegacyMysqlConnection,
}

export class queue {
    private readonly inner: OxenQueue<unknown>

    constructor(options: LegacyQueueOptions) {
        if (!options?.mysql_config) {
            throw new OxenQueueError(error_messages.no_mysql_connection)
        }
        if (!options.job_type) {
            throw new OxenQueueError(error_messages.no_job_type)
        }

        const polling: { minMs?: number; maxMs?: number; backoff?: number } = {}
        if (options.fastest_polling_rate !== undefined) {
            polling.minMs = options.fastest_polling_rate
        }
        if (options.slowest_polling_rate !== undefined) {
            polling.maxMs = options.slowest_polling_rate
        }
        if (options.polling_backoff_rate !== undefined) {
            polling.backoff = options.polling_backoff_rate
        }

        this.inner = new OxenQueue({
            mysql: options.mysql_config,
            queueName: options.job_type,
            table: options.db_table ?? 'oxen_queue',
            polling,
            extraFields: Object.fromEntries(
                (options.extra_fields ?? []).map(field => [
                    field,
                    (body: unknown) =>
                        coerceExtraField(
                            body && typeof body === 'object'
                                ? (body as Record<string, unknown>)[field]
                                : undefined
                        ),
                ])
            ),
        })
        this.onJobSuccess = options.onJobSuccess ?? (async () => {})
        this.onJobError = options.onJobError ?? (async () => {})
    }

    readonly onJobSuccess: NonNullable<LegacyQueueOptions['onJobSuccess']>
    readonly onJobError: NonNullable<LegacyQueueOptions['onJobError']>

    get job_type(): string {
        return this.inner.queueName
    }

    get db_table(): string {
        return this.inner.table
    }

    get fastest_polling_rate(): number {
        return this.inner['polling'].minMs
    }

    async addJob(job: unknown): Promise<void> {
        await this.inner.add(toLegacyJob(job))
    }

    async addJobs(jobs: unknown[]): Promise<void> {
        await this.inner.addMany(jobs.map(toLegacyJob))
    }

    process({ work_fn, concurrency = 3, timeout = 60, recover_stuck_jobs = true }: LegacyProcessOptions): void {
        if (!work_fn) {
            throw new OxenQueueError(error_messages.work_fn_missing)
        }
        if (typeof concurrency !== 'number' || concurrency <= 0) {
            throw new OxenQueueError(error_messages.invalid_concurrency)
        }

        this.inner.start(
            async job => {
                return work_fn(job.body)
            },
            {
                concurrency,
                timeoutMs: Math.floor(timeout) * 1000,
                recoverStuckJobs: recover_stuck_jobs,
            }
        )
    }

    stopProcessing(): void {
        void this.inner.stop({ drain: false })
    }

    async createTable(): Promise<void> {
        await this.inner.createSchema()
    }

    async deleteTable(): Promise<void> {
        await this.inner.deleteTableForTests()
    }

    async selectEntireTable(): Promise<unknown[]> {
        return this.inner.selectAllForTests()
    }

    debug(): QueueDebugSnapshot {
        return this.inner.debug()
    }
}

function toLegacyJob(job: unknown): unknown {
    if (!job || typeof job !== 'object' || !('body' in job)) {
        return job
    }
    const record = job as Record<string, unknown>
    return {
        body: record.body,
        uniqueKey: record.unique_key,
        priority: record.priority,
        startTime: record.start_time,
    }
}

function coerceExtraField(value: unknown): string | number | boolean | null | Date | undefined {
    if (
        value === undefined ||
        value === null ||
        typeof value === 'string' ||
        typeof value === 'number' ||
        typeof value === 'boolean' ||
        value instanceof Date
    ) {
        return value
    }
    return JSON.stringify(value)
}

export default queue
