import { afterEach, beforeEach, describe, expect, test } from 'bun:test'

import { OxenQueue, retryAfter } from '../../src/index.js'
import { delay } from '../../src/utils.js'

const mysql = Object.fromEntries(
    Object.entries({
        host: process.env.OXEN_TEST_HOST,
        user: process.env.OXEN_TEST_USER,
        port: process.env.OXEN_TEST_PORT ? Number(process.env.OXEN_TEST_PORT) : undefined,
        password: process.env.OXEN_TEST_PASSWORD,
        database: process.env.OXEN_TEST_DATABASE,
        timezone: process.env.OXEN_TEST_TIMEZONE,
    }).filter(([, value]) => value !== undefined)
)

const enabled = process.env.OXEN_INTEGRATION === '1'
const maybeDescribe = enabled ? describe : describe.skip

maybeDescribe('mysql integration', () => {
    let queue: OxenQueue<unknown, unknown>

    beforeEach(async () => {
        queue = new OxenQueue({
            mysql,
            queueName: 'anything',
            table: 'oxen_queue_test',
            polling: { minMs: 2, maxMs: 200 },
        })
        await queue.deleteTableForTests()
        await queue.createSchema()
    })

    afterEach(async () => {
        await queue.close()
    })

    test('processes FIFO jobs by default', async () => {
        const jobsIn = Array.from({ length: 10 }, (_, some_id) => ({ some_id, some_msg: `msg${some_id}` }))
        for (const job of jobsIn) {
            await queue.add(job)
        }

        const jobsOut: unknown[] = []
        const worker = queue.start(async job => {
            jobsOut.push(job.body)
            return 'ok'
        })

        await waitUntil(() => jobsOut.length === jobsIn.length)
        await worker.stop()

        expect(jobsOut).toEqual(jobsIn)
    })

    test('respects priority and retry instructions', async () => {
        await queue.addMany([
            { body: 'last', priority: 3 },
            { body: 'first', priority: 1 },
            { body: 'retry', priority: 2 },
        ])

        const jobsOut: unknown[] = []
        let retryAttempts = 0
        const worker = queue.start(async job => {
            jobsOut.push(job.body)
            if (job.body === 'retry' && retryAttempts === 0) {
                retryAttempts += 1
                return retryAfter({ ms: 25 })
            }
            return 'ok'
        }, { concurrency: 1 })

        await waitUntil(() => jobsOut.length === 4)
        await worker.stop()

        expect(jobsOut).toEqual(['first', 'retry', 'last', 'retry'])
    })
})

async function waitUntil(condition: () => boolean | Promise<boolean>): Promise<void> {
    const started = Date.now()
    while (Date.now() - started < 10_000) {
        if (await condition()) {
            return
        }
        await delay(20)
    }
    throw new Error('timed out waiting for condition')
}
