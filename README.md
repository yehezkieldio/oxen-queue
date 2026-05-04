# Oxen Queue

A no-frills, resilient worker queue backed by MySQL.

Oxen is for teams that already operate MySQL and want a durable, SQL-queryable queue without adding Kafka, Redis, or a queue UI to their stack. It optimizes for high throughput, persistence, operational debuggability, and multi-process workers rather than sub-second user-facing latency.

This is a private fork of the original and is not published to npm.

## Quick Start

```ts
import { defineQueue, retryAfter } from 'oxen-queue'

type AvatarJob = {
    userId: string
    imageUrl: string
}

const queue = defineQueue<AvatarJob>({
    mysql: {
        host: '127.0.0.1',
        user: 'app',
        password: 'secret',
        database: 'app',
    },
    queueName: 'avatar_renders',
})

await queue.createSchema()

await queue.add(
    { userId: 'user_123', imageUrl: 'https://example.com/avatar.png' },
    { uniqueKey: 'user_123', priority: Date.now() }
)

const worker = queue.start(
    async job => {
        try {
            await renderAvatar(job.body, { signal: job.signal })
            return 'ok'
        } catch (error) {
            return retryAfter(60)
        }
    },
    {
        concurrency: 25,
        timeoutMs: 60_000,
    }
)

process.on('SIGTERM', async () => {
    await worker.stop({ drain: true, timeoutMs: 30_000 })
    await queue.close()
})
```

## API

### `defineQueue(options)` / `new OxenQueue(options)`

```ts
const queue = defineQueue<Body>({
    mysql,
    queueName,
    table,
    extraFields,
    polling,
    logger,
})
```

Options:

- `mysql`: `mysql2/promise` pool options.
- `queueName`: logical queue name. Workers with the same queue name share work.
- `table`: MySQL table name. Defaults to `oxen_queue`.
- `extraFields`: typed extractor map for fields you want to store as queryable columns.
- `polling.minMs`: fastest polling delay. Defaults to `100`.
- `polling.maxMs`: slowest polling delay. Defaults to `10000`.
- `polling.backoff`: empty-queue backoff multiplier. Defaults to `1.1`.
- `logger`: optional structured logger.

### Adding Jobs

```ts
await queue.add(body)
await queue.add(body, { uniqueKey, priority, startTime })

await queue.addMany([
    { body: firstJob, priority: 1 },
    { body: secondJob, startTime: new Date(Date.now() + 60_000) },
])
```

Job options:

- `uniqueKey`: deduplicates waiting/processing jobs in the same queue.
- `priority`: lower numbers run first. Defaults to `Date.now()` for FIFO-like behavior.
- `startTime`: job is not eligible until this time.

### Processing Jobs

```ts
const worker = queue.start(async job => {
    console.log(job.id, job.body, job.attempt)
    return 'ok'
}, {
    concurrency: 10,
    timeoutMs: 30_000,
    recoverStuckJobs: true,
})
```

The handler receives:

- `id`
- `body`
- `queueName`
- `attempt`
- `startedAt`
- `signal`

Timeouts abort `job.signal` and mark the job as failed. User code should pass the signal into APIs that support cancellation.

### Retrying

```ts
import { retryAfter } from 'oxen-queue'

queue.start(async job => {
    const result = await callBackend(job.body)
    if (!result.ok) {
        return retryAfter({ ms: 5_000 })
    }
    return result.value
})
```

### Stopping Workers

```ts
await worker.stop({ drain: true, timeoutMs: 30_000 })
await queue.close()
```

`drain: true` stops fetching new jobs and waits for active jobs. Without draining, active job signals are aborted.

## Schema

`createSchema()` creates a V2 table with durable job history:

```sql
status ENUM('waiting','processing','success','error','stuck')
body JSON
result MEDIUMTEXT
dedupe_key CHAR(64)
attempt INT UNSIGNED
running_time_ms INT UNSIGNED
```

The table remains intentionally queryable. For example:

```sql
SELECT id, created_ts, started_ts, running_time_ms, result
FROM oxen_queue
WHERE queue_name = 'avatar_renders' AND status = 'error'
ORDER BY id DESC
LIMIT 50;
```

## Extra Fields

Extra fields let you project values from the job body into indexed MySQL columns:

```ts
const queue = defineQueue({
    mysql,
    queueName: 'payment_sync',
    extraFields: {
        user_id: body => body.userId,
        payment_method: body => body.paymentMethod,
    },
})
```

`createSchema()` creates these extra fields as JSON columns. For custom column types or indexes, manage the table with your own migration and keep the extractor names aligned.

## Legacy Compatibility

The old API is available as an ESM compatibility subpath:

```js
import Oxen from 'oxen-queue/compat'

const ox = new Oxen({
    mysql_config: { user: 'app', password: 'secret', database: 'app' },
    job_type: 'avatar_renders',
    db_table: 'oxen_queue',
})

await ox.createTable()
await ox.addJob({ body: 'hello', unique_key: 'hello' })

ox.process({
    work_fn: async body => {
        return 'ok'
    },
    concurrency: 3,
    timeout: 60,
})
```

Legacy option names map to V2 names:

| Legacy | V2 |
| --- | --- |
| `mysql_config` | `mysql` |
| `job_type` | `queueName` |
| `db_table` | `table` |
| `extra_fields` | `extraFields` |
| `fastest_polling_rate` | `polling.minMs` |
| `slowest_polling_rate` | `polling.maxMs` |
| `polling_backoff_rate` | `polling.backoff` |
| `work_fn` | typed worker handler |
| `_oxen_queue_retry_seconds` | `retryAfter()` |

CommonJS `require('oxen-queue')` is intentionally not supported in V2.

## Development

```bash
bun install
bun test
bun run typecheck
bun run build
```

Integration tests require a MySQL database:

```bash
OXEN_INTEGRATION=1 \
OXEN_TEST_HOST=127.0.0.1 \
OXEN_TEST_USER=root \
OXEN_TEST_PASSWORD=password \
OXEN_TEST_DATABASE=oxen_test \
bun run test:integration
```