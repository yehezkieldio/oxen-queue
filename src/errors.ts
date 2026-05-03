export const errorMessages = {
    noQueueName:
        'Must specify the queue name e.g. new OxenQueue({ queueName: "weekly_emails", mysql: { ... } })',
    noJobType:
        'Must specify the job type e.g. const ox = new oxen_queue({ job_type: "weekly_emails" ... })',
    alreadyProcessing: 'This queue is already processing',
    handlerMissing: 'Missing handler argument, nothing to do.',
    workFnMissing:
        'Missing work_fn argument, nothing to do! Remember that the process() function takes an object as its single argument.',
    invalidConcurrency: 'The concurrency argument must be a positive number',
    noMysqlConnection:
        'Must supply mysql argument. It should look something like: new OxenQueue({ mysql: { host: "foo.net", password: "secret" } ... }).',
    noLegacyMysqlConnection:
        'Must supply mysql_config argument. It should look something like: const ox = new oxen_queue({ mysql_config: { host : "foo.net", password : "secret"} ... }).',
    invalidIdentifier: 'SQL identifiers may only contain letters, numbers, underscores, and dots.',
} as const

export class OxenQueueError extends Error {
    constructor(message: string) {
        super(message)
        this.name = 'OxenQueueError'
    }
}
