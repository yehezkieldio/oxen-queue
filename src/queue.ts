import { OxenQueueError, errorMessages } from './errors.js'
import { MySqlStorage } from './storage.js'
import type {
    AddJobInput,
    AddJobOptions,
    ExtraFieldMap,
    InferHandlerResult,
    JobHandler,
    Logger,
    OxenQueueOptions,
    PollingOptions,
    QueueDebugSnapshot,
    WorkerOptions,
} from './types.js'
import { validateQueueOptions, validateWorkerOptions } from './validation.js'
import { OxenWorker } from './worker.js'

const defaultLogger: Logger = {
    error(message, meta) {
        console.error(`Oxen Queue: ${message}`, meta?.error ?? meta ?? '')
    },
}

export class OxenQueue<
    Body = unknown,
    Result = unknown,
    Extra extends ExtraFieldMap<Body> = ExtraFieldMap<Body>,
> {
    readonly queueName: string
    readonly table: string
    readonly storage: MySqlStorage<Body>
    private readonly polling: Required<PollingOptions>
    private readonly logger: Logger
    private activeWorker: OxenWorker<Body, unknown> | undefined

    constructor(private readonly options: OxenQueueOptions<Body, Extra>) {
        validateQueueOptions(options)
        this.queueName = options.queueName
        this.table = options.table ?? 'oxen_queue'
        this.polling = {
            minMs: options.polling?.minMs ?? 100,
            maxMs: options.polling?.maxMs ?? 10_000,
            backoff: options.polling?.backoff ?? 1.1,
        }
        this.logger = options.logger ?? defaultLogger
        this.storage = new MySqlStorage<Body>({
            mysql: options.mysql,
            table: this.table,
            queueName: options.queueName,
            ...(options.extraFields ? { extraFields: options.extraFields } : {}),
        })
    }

    async add(body: Body, options?: AddJobOptions): Promise<void>
    async add(job: AddJobInput<Body>): Promise<void>
    async add(first: Body | AddJobInput<Body>, options: AddJobOptions = {}): Promise<void> {
        await this.addMany([normalizeJob(first, options)])
    }

    async addMany(jobs: AddJobInput<Body>[]): Promise<void> {
        await this.storage.addJobs(jobs.map(job => normalizeJob(job)))
    }

    worker<const Handler extends JobHandler<Body, unknown>>(
        handler: Handler,
        options: WorkerOptions = {}
    ): OxenWorker<Body, InferHandlerResult<Handler>> {
        if (!handler) {
            throw new OxenQueueError(errorMessages.handlerMissing)
        }
        const parsed = validateWorkerOptions(options)
        return new OxenWorker(this.storage, handler, {
            concurrency: parsed.concurrency,
            timeoutMs: parsed.timeoutMs,
            recoverStuckJobs: parsed.recoverStuckJobs,
            stuckJobIntervalMs: parsed.stuckJobIntervalMs,
            pollingMinMs: this.polling.minMs,
            pollingMaxMs: this.polling.maxMs,
            pollingBackoff: this.polling.backoff,
            logger: this.logger,
        }) as OxenWorker<Body, InferHandlerResult<Handler>>
    }

    start<const Handler extends JobHandler<Body, unknown>>(
        handler: Handler,
        options: WorkerOptions = {}
    ): OxenWorker<Body, InferHandlerResult<Handler>> {
        if (this.activeWorker?.status().running) {
            throw new OxenQueueError(errorMessages.alreadyProcessing)
        }
        this.activeWorker = this.worker(handler, options)
        this.activeWorker.start()
        return this.activeWorker as OxenWorker<Body, InferHandlerResult<Handler>>
    }

    async stop(options = { drain: false }): Promise<void> {
        await this.activeWorker?.stop(options)
    }

    async close(): Promise<void> {
        await this.stop()
        await this.storage.close()
    }

    async createSchema(): Promise<void> {
        await this.storage.createSchema(Object.keys(this.options.extraFields ?? {}))
    }

    async inspectSchema(): Promise<{ exists: boolean; version: 'legacy' | 'v2' | 'unknown'; columns: string[] }> {
        return this.storage.inspectSchema()
    }

    async migrateLegacyTable(legacyTable: string): Promise<number> {
        await this.createSchema()
        return this.storage.migrateLegacyTable(legacyTable)
    }

    async deleteTableForTests(): Promise<void> {
        await this.storage.deleteTableForTests()
    }

    async selectAllForTests(): Promise<unknown[]> {
        return this.storage.selectAllForTests()
    }

    debug(): QueueDebugSnapshot {
        const status = this.activeWorker?.status()
        return {
            processing: status?.running ?? false,
            in_process: status?.active ?? 0,
            currently_fetching: status?.fetching ?? false,
            working_job_batch: this.activeWorker?.debugBatch() ?? [],
        }
    }
}

export function defineQueue<
    const Body,
    const Extra extends ExtraFieldMap<Body> = ExtraFieldMap<Body>,
>(options: OxenQueueOptions<Body, Extra>): OxenQueue<Body, unknown, Extra> {
    return new OxenQueue<Body, unknown, Extra>(options)
}

export function defineWorker<const Body, const Handler extends JobHandler<Body, unknown>>(
    queue: OxenQueue<Body>,
    handler: Handler,
    options?: WorkerOptions
) : OxenWorker<Body, InferHandlerResult<Handler>> {
    return queue.worker(handler, options)
}

function normalizeJob<Body>(
    input: Body | AddJobInput<Body>,
    options: AddJobOptions = {}
): { body: Body; options: AddJobOptions } {
    if (
        input &&
        typeof input === 'object' &&
        'body' in input &&
        (Object.keys(input).some(key => ['uniqueKey', 'unique_key', 'priority', 'startTime', 'start_time'].includes(key)) ||
            Object.keys(input).length === 1)
    ) {
        const record = input as {
            body: Body
            uniqueKey?: string | number
            unique_key?: string | number
            priority?: number
            startTime?: Date | string | number
            start_time?: Date | string | number
        }
        return { body: record.body, options: compactOptions(record) }
    }

    return { body: input as Body, options }
}

function compactOptions(record: {
    uniqueKey?: string | number
    unique_key?: string | number
    priority?: number
    startTime?: Date | string | number
    start_time?: Date | string | number
}): AddJobOptions {
    const options: AddJobOptions = {}
    const uniqueKey = record.uniqueKey ?? record.unique_key
    const startTime = record.startTime ?? record.start_time
    if (uniqueKey !== undefined) {
        options.uniqueKey = uniqueKey
    }
    if (record.priority !== undefined) {
        options.priority = record.priority
    }
    if (startTime !== undefined) {
        options.startTime = startTime
    }
    return options
}
