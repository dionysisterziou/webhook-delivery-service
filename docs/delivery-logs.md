# Trace one delivery

All delivery events carry a UTC timestamp, a component, an event name and the
same `delivery_id`. A new outbox message (including a retry) has a new
`message_id`. HTTP responses and delivery outcomes are separate events: the
former is observed by the sender; the latter is logged by the worker only after
the outcome commits to PostgreSQL.

## Run the local fail-once example

Use PowerShell 7 and the project's activated virtual environment in each
terminal. From the repository root, start the dependencies and apply the
existing setup (there is no migration for logging):

```powershell
docker compose up -d postgres rabbitmq
docker compose ps
# Once both are healthy:
python -m alembic upgrade head
python -m webhook_delivery_service.rabbitmq_setup
```

Run each of the following five commands in its **own terminal**. Stop old
instances of the same processes first. The four redirected commands write their
stdout to separate temporary files so a single filter can read all of them.
Without redirection the same JSON lines appear directly in the terminal.

```powershell
python -m uvicorn examples.webhook_receiver:app --host 127.0.0.1 --port 9000
python -u -m webhook_delivery_service.api_runner > "$env:TEMP/webhook-trace-api.log"
python -u -m webhook_delivery_service.dispatcher_runner > "$env:TEMP/webhook-trace-dispatcher.log"
python -u -m webhook_delivery_service.consumer_runner > "$env:TEMP/webhook-trace-consumer.log"
python -u -m webhook_delivery_service.retry_scheduler_runner > "$env:TEMP/webhook-trace-scheduler.log"
```

From a free terminal in the repository, create one delivery. Keep this terminal
open: `$delivery.id` is the ID returned by this specific run.

```powershell
$body = @{
    target_url = "http://127.0.0.1:9000/webhooks/fail-once"
    event_type = "demo.delivery.trace"
    payload = @{ source = "structured-logs-check" }
} | ConvertTo-Json -Depth 3
$delivery = Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:8000/deliveries" -ContentType "application/json" -Body $body
$delivery.id
```

Wait for completion (first retry is normally due in 30–33 seconds):

```powershell
$deadline = (Get-Date).AddSeconds(90)
do {
    $state = Invoke-RestMethod "http://127.0.0.1:8000/deliveries/$($delivery.id)"
    if ($state.status -in @("succeeded", "failed")) { break }
    Start-Sleep -Seconds 1
} while ((Get-Date) -lt $deadline)
$state | Select-Object id, status, attempt_count, next_attempt_at
```

Read only this delivery's JSON records across the four logs:

```powershell
$trace = Get-Content "$env:TEMP/webhook-trace-*.log" |
    Where-Object { $_.StartsWith("{") } |
    ForEach-Object { $_ | ConvertFrom-Json } |
    Where-Object { $_.delivery_id -eq $delivery.id } |
    Sort-Object timestamp
$trace | Format-Table component, event, attempt_count, status_code, status, message_id -AutoSize
```

For this scenario expect final state `succeeded` and two attempts. In the trace:

- `delivery.created`, `outbox.published` and `message.received` share the first
  message ID. `attempt.started` precedes the first `http.response` (503).
- `delivery.retry_scheduled` records attempt 1 and `next_attempt_at`. When due,
  `retry.queued` shows the second message ID, which the dispatcher and consumer
  then log. Attempt count is still 1 until the worker actually starts attempt 2.
- Attempt 2 records `http.response` (200), then `delivery.succeeded`.

The dispatcher logs publication after both broker confirmation and its database
commit; a fast consumer can therefore log receipt before the dispatcher writes
its line. Use the IDs to correlate those events rather than assuming global
ordering. A process crash may leave an incomplete trace; the persisted delivery
state is authoritative.

The same events cover permanent failures (`delivery.failed`), network errors
(`http.request_error` without a status code), ignored duplicate messages
(`delivery.skipped`) and matching API replays (`delivery.replayed`).
Delivery JSON records omit request bodies, destination URLs, raw exceptions
and idempotency keys. The separate **demo receiver** intentionally prints its
test payload; it is not the application's structured log.

Stop the five Python processes with Ctrl+C when finished. Starting a new capture
with the same commands replaces the previous temporary log files.

## Focused check without Docker

```powershell
python -m pytest tests/test_delivery_logs.py -q
```

These component tests use the application code with a mocked database session,
publisher and HTTP transport. The manual example above uses real PostgreSQL,
RabbitMQ and HTTP.
