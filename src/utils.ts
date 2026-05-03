import { createHash, randomInt } from 'node:crypto'
import { setTimeout as sleep } from 'node:timers/promises'

import { errorMessages, OxenQueueError } from './errors.js'
import type { RetryInstruction } from './types.js'

export const RETRY_SYMBOL = Symbol.for('oxen-queue.retry')

export interface InternalRetryInstruction extends RetryInstruction {
    readonly [RETRY_SYMBOL]: true
}

export function retryAfter(seconds: number): RetryInstruction
export function retryAfter(options: { ms: number }): RetryInstruction
export function retryAfter(input: number | { ms: number }): RetryInstruction {
    const delayMs = typeof input === 'number' ? input * 1000 : input.ms
    return {
        type: 'retry',
        delayMs: Math.max(0, Math.floor(delayMs)),
        [RETRY_SYMBOL]: true,
    } as InternalRetryInstruction
}

export function isRetryInstruction(value: unknown): value is RetryInstruction {
    if (!value || typeof value !== 'object') {
        return false
    }
    const record = value as Record<PropertyKey, unknown>
    return record[RETRY_SYMBOL] === true || record.type === 'retry'
}

export function isLegacyRetryInstruction(value: unknown): value is {
    _oxen_queue_retry_seconds: number
} {
    return (
        !!value &&
        typeof value === 'object' &&
        typeof (value as { _oxen_queue_retry_seconds?: unknown })._oxen_queue_retry_seconds === 'number'
    )
}

export function assertIdentifier(identifier: string): string {
    if (!/^[A-Za-z0-9_.]+$/.test(identifier)) {
        throw new OxenQueueError(errorMessages.invalidIdentifier)
    }
    return identifier
        .split('.')
        .map(part => `\`${part}\``)
        .join('.')
}

export function dedupeHash(queueName: string, uniqueKey: string | number): string {
    return createHash('sha256').update(`${queueName}\0${String(uniqueKey)}`).digest('hex')
}

export function serializeJson(value: unknown): string | null {
    if (value === undefined) {
        return null
    }
    return JSON.stringify(value)
}

export function parseJson<T>(value: unknown): T {
    if (value === null || value === undefined) {
        return value as T
    }
    if (typeof value === 'string') {
        return JSON.parse(value) as T
    }
    return value as T
}

export async function delay(ms: number, signal?: AbortSignal): Promise<void> {
    await sleep(ms, undefined, { signal })
}

export function jitteredRetryDelay(): number {
    return randomInt(500, 1001)
}

export function toDate(input: Date | string | number | undefined): Date {
    if (input === undefined) {
        return new Date()
    }
    return new Date(input)
}

export function timeoutError(jobId: bigint, timeoutMs: number): Error {
    return new Error(`timeout for job_id ${jobId.toString()} (over ${Math.floor(timeoutMs / 1000)} seconds)`)
}

export async function withTimeout<T>(
    task: Promise<T>,
    timeoutMs: number,
    controller: AbortController,
    createError: () => Error
): Promise<T> {
    let timeout: ReturnType<typeof setTimeout> | undefined
    const timeoutPromise = new Promise<never>((_, reject) => {
        timeout = setTimeout(() => {
            controller.abort(createError())
            reject(createError())
        }, timeoutMs)
    })

    try {
        return await Promise.race([task, timeoutPromise])
    } finally {
        if (timeout) {
            clearTimeout(timeout)
        }
    }
}
