import type { JobContext, JobHandler, Logger, StopOptions, StoredJob, WorkerStatus } from './types.js'
import {
    delay,
    isLegacyRetryInstruction,
    isRetryInstruction,
    retryAfter,
    timeoutError,
    withTimeout,
} from './utils.js'
import type { MySqlStorage } from './storage.js'

export interface WorkerRuntimeOptions {
    concurrency: number
    timeoutMs: number
    recoverStuckJobs: boolean
    stuckJobIntervalMs: number
    pollingMinMs: number
    pollingMaxMs: number
    pollingBackoff: number
    logger: Logger
    onJobSuccess?: (event: { job_id: bigint; job_body: unknown; job_type: string; job_result: unknown }) => Promise<void>
    onJobError?: (event: { job_id: bigint; job_body: unknown; job_type: string; error: unknown }) => Promise<void>
}

export class OxenWorker<Body, Result> {
    private running = false
    private fetching = false
    private active = 0
    private buffer: StoredJob<Body>[] = []
    private pollingRate: number
    private loopTimer: ReturnType<typeof setTimeout> | undefined
    private stuckTimer: ReturnType<typeof setInterval> | undefined
    private activeControllers = new Set<AbortController>()

    constructor(
        private readonly storage: MySqlStorage<Body>,
        private readonly handler: JobHandler<Body, Result>,
        private readonly options: WorkerRuntimeOptions
    ) {
        this.pollingRate = options.pollingMinMs
    }

    start(): void {
        if (this.running) {
            return
        }
        this.running = true
        this.schedule(0)
        this.stuckTimer = setInterval(() => {
            void this.checkStuckJobs()
        }, this.options.stuckJobIntervalMs)
    }

    async stop(options: StopOptions = {}): Promise<void> {
        this.running = false
        if (this.loopTimer) {
            clearTimeout(this.loopTimer)
        }
        if (this.stuckTimer) {
            clearInterval(this.stuckTimer)
        }

        if (!options.drain) {
            for (const controller of this.activeControllers) {
                controller.abort(new Error('worker stopped'))
            }
            return
        }

        const started = Date.now()
        while (this.active > 0 || this.fetching) {
            if (options.timeoutMs && Date.now() - started > options.timeoutMs) {
                throw new Error(`Timed out waiting for worker drain after ${options.timeoutMs}ms`)
            }
            await delay(25)
        }
    }

    status(): WorkerStatus {
        return {
            running: this.running,
            active: this.active,
            buffered: this.buffer.length,
            fetching: this.fetching,
        }
    }

    debugBatch(): StoredJob<Body>[] {
        return [...this.buffer]
    }

    private schedule(ms: number): void {
        if (!this.running) {
            return
        }
        this.loopTimer = setTimeout(() => {
            void this.tick()
        }, ms)
    }

    private async tick(): Promise<void> {
        if (!this.running) {
            return
        }

        while (this.running && this.active < this.options.concurrency) {
            const job = await this.nextJob()
            if (!job) {
                break
            }
            this.active += 1
            void this.runJob(job).finally(() => {
                this.active -= 1
                this.schedule(0)
            })
        }

        const nextRate = this.active >= this.options.concurrency ? this.options.pollingMinMs : this.pollingRate
        this.schedule(nextRate)
    }

    private async nextJob(): Promise<StoredJob<Body> | undefined> {
        if (this.buffer.length === 0 && !this.fetching) {
            this.fetching = true
            try {
                this.buffer.push(...(await this.storage.claimJobs(this.options.concurrency)))
            } catch (error) {
                this.options.logger.error?.('There was an error while trying to get the next set of jobs', {
                    error,
                })
            } finally {
                this.fetching = false
            }
        }

        const job = this.buffer.shift()
        if (job) {
            this.pollingRate = this.options.pollingMinMs
        } else {
            this.pollingRate = Math.min(
                this.options.pollingMaxMs,
                Math.max(this.options.pollingMinMs, this.pollingRate * this.options.pollingBackoff)
            )
        }
        return job
    }

    private async runJob(job: StoredJob<Body>): Promise<void> {
        const controller = new AbortController()
        this.activeControllers.add(controller)
        const context: JobContext<Body> = { ...job, signal: controller.signal }

        try {
            const result = await withTimeout(
                Promise.resolve(this.handler(context)),
                this.options.timeoutMs,
                controller,
                () => timeoutError(job.id, this.options.timeoutMs)
            )

            if (isRetryInstruction(result)) {
                await this.storage.retryJob(job.id, result.delayMs)
                return
            }
            if (isLegacyRetryInstruction(result)) {
                await this.storage.retryJob(job.id, retryAfter(result._oxen_queue_retry_seconds).delayMs)
                return
            }

            await this.options.onJobSuccess?.({
                job_id: job.id,
                job_body: job.body,
                job_type: job.queueName,
                job_result: result,
            })
            await this.storage.markSuccess(job.id, result)
        } catch (error) {
            await this.options.onJobError?.({
                job_id: job.id,
                job_body: job.body,
                job_type: job.queueName,
                error,
            })
            await this.storage.markError(job.id, error)
        } finally {
            this.activeControllers.delete(controller)
        }
    }

    private async checkStuckJobs(): Promise<void> {
        try {
            if (this.options.recoverStuckJobs) {
                await this.storage.recoverStuckJobs(this.options.timeoutMs)
            } else {
                await this.storage.markStuckJobs(this.options.timeoutMs)
            }
        } catch (error) {
            this.options.logger.error?.('Unable to check stuck jobs', { error })
        }
    }
}
