# Offline Queue Simulator

`offline-queue-simulator` reads one exported, synthetic event timeline and reports what a bounded virtual device queue would deliver, lose, suppress, or leave pending. It does not contact devices, send writes, or replay a real queue. Node.js 22+; no dependencies.

## Quick start

```sh
node bin/offline-queue-simulator.mjs --root examples --input passing.json
node bin/offline-queue-simulator.mjs --root examples --input failing.json
npm run check
```

The passing example exits `0`; the failing reconnect burst exits `1` with `queue-overflow`. Paths are resolved against the real `--root`; a symlink out of that root is refused. Invalid CLI options exit `2` with empty stdout. Unreadable, malformed, partial, or unsupported input exits `2` with an `incomplete` JSON report. Stdout contains only one JSON report.

## Export format

```json
{
  "schemaVersion": "1",
  "complete": true,
  "seed": 7,
  "policy": {
    "capacity": 2,
    "ttlMs": 100,
    "maxAttempts": 2,
    "retryDelayMs": 10,
    "successPercent": 100,
    "duplicate": "suppress",
    "overflow": "reject-new"
  },
  "events": [
    { "at": 0, "kind": "enqueue", "id": "opaque-a" },
    { "at": 1, "kind": "connect" }
  ]
}
```

Events are nondecreasing virtual milliseconds. Kinds are `enqueue`, `connect`, `disconnect`, and `tick`; only enqueues carry an opaque `id`. A non-enqueue event may advance time. The simulator begins offline. Queue order is FIFO. `duplicate: suppress` suppresses every later enqueue of a seen ID, even if the first item expired or was lost; `allow` permits it and flags a second delivery. `overflow: reject-new` loses the arriving item, while `drop-oldest` loses the oldest queued item. At age exactly `ttlMs`, an undelivered item expires. On each connected event, due attempts draw from a deterministic seeded virtual PRNG; a draw below `successPercent` acknowledges the item. A failed attempt schedules another after `retryDelayMs`, up to `maxAttempts`. The export must be complete and must contain at least one enqueue. A still-pending item is unknown, hence incomplete.

## Rules and status

| Rule | Severity | Result |
| --- | --- | --- |
| `queue-overflow`, `item-expired`, `retry-exhausted`, `duplicate-delivered` | error | fail (`1`) |
| `duplicate-suppressed` | info | pass (`0`) when no other issue exists |
| `input-unreadable`, `input-invalid`, `duplicate-key`, `byte-limit`, `record-limit`, `depth-limit`, `time-limit`, `export-incomplete`, `policy-invalid`, `event-invalid`, `no-items`, `queue-pending` | error | incomplete (`2`) |

Reports follow the local v1 report contract: `schemaVersion`, `tool`, `status`, `summary`, `outcomes`, and sorted `findings`. Each outcome has `sourceOrdinal` (zero-based event index), `state`, and `attempts`. Every finding uses logical source role `@export` and a JSON pointer such as `/events/2`, so IDs and payloads never appear in the report. Findings sort by UTF-16 code-unit `(location.file, location.pointer, ruleId)`. This role refers to the exact file supplied via `--input`, not a fabricated file path.

## Bounds and non-goals

Input is strict UTF-8 and at most 1,048,576 bytes. At most 200 events, queue capacity 1–100, attempts 1–10, seed 0–4,294,967,295, acknowledgment percentage 0–100, JSON depth 3 (root is depth 0), and 5,000 ms injected processing time are permitted. Exceeding a bound returns `incomplete`; exact N and N+1 are tested. TTL and retry delay must be safe nonnegative integers (TTL positive). Event IDs are opaque, 1–128 code units, with control/bidi characters refused. Unknown fields and duplicate JSON keys, including escaped spellings, are refused.

This is a bounded model, not a prediction of radio reliability, durable storage behavior, real network timing, provider retry semantics, or device acknowledgments. It performs no external writes and has no `--out` option. The default wall clock only caps local processing time; the event timeline and seed, not wall time, determine outcomes.
