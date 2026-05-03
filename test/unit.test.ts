import { describe, expect, test } from 'bun:test'

import { error_messages, queue as LegacyQueue } from '../src/compat.js'
import { errorMessages, OxenQueueError } from '../src/errors.js'
import { OxenQueue } from '../src/index.js'
import { dedupeHash, isLegacyRetryInstruction, isRetryInstruction, retryAfter } from '../src/utils.js'

describe('v2 public surface', () => {
    test('validates required mysql config', () => {
        expect(() => {
            new OxenQueue({
                mysql: undefined as never,
                queueName: 'jobs',
            })
        }).toThrow(errorMessages.noMysqlConnection)
    })

    test('validates required queue name', () => {
        expect(() => {
            new OxenQueue({
                mysql: {},
                queueName: '',
            })
        }).toThrow(errorMessages.noQueueName)
    })

    test('creates typed retry instructions', () => {
        const retry = retryAfter({ ms: 1500 })
        expect(retry.delayMs).toBe(1500)
        expect(isRetryInstruction(retry)).toBe(true)
    })

    test('dedupe hashes include queue name and stable key', () => {
        expect(dedupeHash('alpha', 'same')).toBe(dedupeHash('alpha', 'same'))
        expect(dedupeHash('alpha', 'same')).not.toBe(dedupeHash('beta', 'same'))
        expect(dedupeHash('alpha', 'same')).toHaveLength(64)
    })
})

describe('legacy compatibility surface', () => {
    test('preserves old constructor errors', () => {
        expect(() => {
            new LegacyQueue({
                mysql_config: undefined as never,
                job_type: 'jobs',
            })
        }).toThrow(error_messages.no_mysql_connection)

        expect(() => {
            new LegacyQueue({
                mysql_config: {},
                job_type: '',
            })
        }).toThrow(error_messages.no_job_type)
    })

    test('preserves old work_fn validation', () => {
        const legacy = new LegacyQueue({
            mysql_config: {},
            job_type: 'jobs',
        })

        expect(() => {
            legacy.process({ work_fn: undefined as never })
        }).toThrow(error_messages.work_fn_missing)
    })

    test('recognizes legacy retry objects', () => {
        expect(isLegacyRetryInstruction({ _oxen_queue_retry_seconds: 3 })).toBe(true)
        expect(isLegacyRetryInstruction({ _oxen_queue_retry_seconds: '3' })).toBe(false)
    })

    test('uses OxenQueueError for compatibility failures', () => {
        expect(() => {
            new LegacyQueue({
                mysql_config: {},
                job_type: '',
            })
        }).toThrow(OxenQueueError)
    })
})
