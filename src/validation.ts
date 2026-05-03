import { z } from 'zod'

import { errorMessages, OxenQueueError } from './errors.js'
import type { OxenQueueOptions, WorkerOptions } from './types.js'

export function validateQueueOptions<Body>(options: OxenQueueOptions<Body>): void {
    if (!options.mysql) {
        throw new OxenQueueError(errorMessages.noMysqlConnection)
    }
    if (!options.queueName) {
        throw new OxenQueueError(errorMessages.noQueueName)
    }

    const pollingSchema = z
        .object({
            minMs: z.number().positive().optional(),
            maxMs: z.number().positive().optional(),
            backoff: z.number().min(1).optional(),
        })
        .optional()

    pollingSchema.parse(options.polling)
}

export function validateWorkerOptions(options: WorkerOptions): Required<WorkerOptions> {
    const parsed = z
        .object({
            concurrency: z.number().int().positive().default(3),
            timeoutMs: z.number().int().positive().default(60_000),
            recoverStuckJobs: z.boolean().default(true),
            stuckJobIntervalMs: z.number().int().positive().default(60_000),
        })
        .parse(options)

    return parsed
}
