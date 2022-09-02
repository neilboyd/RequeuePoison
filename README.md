# RequeuePoison

Requeue Azure Storage Queue messages from the poison queue back to the main queue.

## Usage

```bash
RequeuePoison Queue={queue name} ConnectionString={connection string} [Max={max number of messages to requeue}] [MaxAge={max age in days of messages to requeue}] [Interval={interval in milliseconds of visibility of requeued messages}]
```

Parameters can also be specified in `appsettings.json`.
