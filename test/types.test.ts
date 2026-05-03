import { expect, test } from 'bun:test'

import {
    defineWorker,
    defineQueue,
    type ExtraFieldOutput,
    type InferHandlerResult,
    type InferQueueBody,
    type InferQueueExtraFields,
    retryAfter,
} from '../src/index.js'
import type { OxenQueueOptions } from '../src/types.js'

type Equal<Left, Right> =
    (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
        ? true
        : false
type Expect<Condition extends true> = Condition

interface EmailJob {
    userId: string
    template: 'welcome' | 'receipt'
    attempt?: number
}

const emailOptions = {
    mysql: {},
    queueName: 'email',
    extraFields: {
        user_id: (body: EmailJob) => body.userId,
        template: (body: EmailJob) => body.template,
    },
} satisfies OxenQueueOptions<EmailJob>

type _BodyInference = Expect<Equal<InferQueueBody<typeof emailOptions>, EmailJob>>
type _ExtraInference = Expect<
    Equal<
        InferQueueExtraFields<typeof emailOptions>,
        {
            readonly user_id: string
            readonly template: 'welcome' | 'receipt'
        }
    >
>
type _ExtraOutput = Expect<
    Equal<
        ExtraFieldOutput<typeof emailOptions.extraFields>,
        {
            readonly user_id: string
            readonly template: 'welcome' | 'receipt'
        }
    >
>

const handler = async (job: { body: EmailJob }) => {
    if (job.body.template === 'receipt') {
        return retryAfter(10)
    }
    return { delivered: true as const, userId: job.body.userId }
}

type _HandlerResult = Expect<
    Equal<InferHandlerResult<typeof handler>, { delivered: true; userId: string }>
>

test('type helpers have no runtime footprint', () => {
    const queue = defineQueue<EmailJob>(emailOptions)
    const worker = defineWorker(queue, async job => {
        return { queued: job.body.template }
    })
    expect(queue.queueName).toBe('email')
    expect(worker.status().running).toBe(false)
})
