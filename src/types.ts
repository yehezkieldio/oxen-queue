import type { PoolOptions } from 'mysql2/promise'

export type JsonPrimitive = string | number | boolean | null
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue }
export type JobStatus = 'waiting' | 'processing' | 'success' | 'error' | 'stuck'
export type ExtraFieldValue = JsonPrimitive | Date | undefined
export type ExtraFieldMap<Body> = Record<string, (body: Body) => ExtraFieldValue>
export type AnyExtraFieldMap = Record<string, (body: never) => ExtraFieldValue>
export type InferExtraFieldValue<Extractor> = Extractor extends (body: never) => infer Value
    ? Value
    : never
export type ExtraFieldOutput<Extra extends Record<string, (...args: never[]) => unknown>> = {
    readonly [Key in keyof Extra]: Awaited<ReturnType<Extra[Key]>>
}
export type QueueEventName =
    | 'job:claimed'
    | 'job:success'
    | 'job:error'
    | 'job:retry'
    | 'job:stuck'
    | 'worker:started'
    | 'worker:stopped'
export type QueueEventHandlerName<Event extends QueueEventName> =
    `on${Capitalize<Event extends `${infer Head}:${infer Tail}` ? `${Head}${Capitalize<Tail>}` : Event>}`

export interface PollingOptions {
    minMs?: number
    maxMs?: number
    backoff?: number
}

export interface Logger {
    debug?(message: string, meta?: Record<string, unknown>): void
    info?(message: string, meta?: Record<string, unknown>): void
    warn?(message: string, meta?: Record<string, unknown>): void
    error?(message: string, meta?: Record<string, unknown>): void
}

export interface OxenQueueOptions<Body, Extra extends ExtraFieldMap<Body> = ExtraFieldMap<Body>> {
    mysql: PoolOptions
    queueName: string
    table?: string
    extraFields?: Extra
    polling?: PollingOptions
    logger?: Logger
}

export type InferQueueBody<Options> = Options extends OxenQueueOptions<infer Body, ExtraFieldMap<infer Body>>
    ? Body
    : never

export type InferQueueExtraFields<Options> = Options extends { extraFields: infer Extra }
    ? Extra extends Record<string, (...args: never[]) => unknown>
        ? ExtraFieldOutput<Extra>
        : {}
    : {}

export interface AddJobOptions {
    uniqueKey?: string | number
    priority?: number
    startTime?: Date | string | number
}

export type AddJobInput<Body> = Body | ({ body: Body } & AddJobOptions)

export interface StoredJob<Body = unknown> {
    id: bigint
    body: Body
    queueName: string
    attempt: number
    startedAt: Date
}

export interface JobContext<Body> extends StoredJob<Body> {
    signal: AbortSignal
}

export interface RetryInstruction {
    readonly type: 'retry'
    readonly delayMs: number
}

export type JobHandler<Body, Result> = (
    job: JobContext<Body>
) => Result | RetryInstruction | Promise<Result | RetryInstruction>
export type InferHandlerResult<Handler> = Handler extends (...args: never[]) => infer Result
    ? Exclude<Awaited<Result>, RetryInstruction>
    : never

export interface WorkerOptions {
    concurrency?: number
    timeoutMs?: number
    recoverStuckJobs?: boolean
    stuckJobIntervalMs?: number
}

export interface StopOptions {
    drain?: boolean
    timeoutMs?: number
}

export interface WorkerStatus {
    running: boolean
    active: number
    buffered: number
    fetching: boolean
}

export interface QueueDebugSnapshot {
    processing: boolean
    in_process: number
    currently_fetching: boolean
    working_job_batch: unknown[]
}

export interface LegacyQueueOptions {
    mysql_config: PoolOptions
    job_type: string
    db_table?: string
    extra_fields?: string[]
    fastest_polling_rate?: number
    slowest_polling_rate?: number
    polling_backoff_rate?: number
    onJobSuccess?: (event: {
        job_id: bigint
        job_body: unknown
        job_type: string
        job_result: unknown
    }) => void | Promise<void>
    onJobError?: (event: {
        job_id: bigint
        job_body: unknown
        job_type: string
        error: unknown
    }) => void | Promise<void>
}

export interface LegacyProcessOptions {
    work_fn: (jobBody: unknown) => unknown | Promise<unknown>
    concurrency?: number
    timeout?: number
    recover_stuck_jobs?: boolean
}
