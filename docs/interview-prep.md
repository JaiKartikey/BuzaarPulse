# BazaarPulse: interview preparation

How to explain this project, defend its decisions, and answer the follow-up questions. Every number here was measured on the real system; the commands to reproduce them are in the README.

---

## 1. The pitch

### 30 seconds

> BazaarPulse is a real-time trading platform I built end to end. Market prices stream through a Redis Streams pipeline that turns ticks into candles, quotes and anomaly alerts in a few milliseconds and pushes them to browsers over WebSockets. On top of that is a paper-trading engine with a database-enforced double-entry ledger, and an AI research desk where agents use tools to analyse stocks, run in parallel, and explain sudden price moves on their own. Agents can draft trades, but only a human can approve them. It runs as independently scalable services on Docker and Kubernetes, with CI, monitoring and load tests.

### 2 minutes (the shape of the answer)

1. **Problem.** I wanted one project that forced me to deal with streaming data, money correctness, distributed workers and AI agents together, because real systems mix all of them.
2. **Data path.** Feed → ingestor → partitioned Redis Streams → stream processors (candles, quotes, anomaly detection) → Postgres for history, Redis pub/sub for live fan-out → WebSocket → React.
3. **Money path.** Order → risk checks under a per-user row lock → executor matches against live prices → one transaction writes ledger entries, the fill, the position and an outbox event.
4. **AI path.** A run is a durable job. Workers run a tool-use loop; every step is stored and streamed over SSE. Three patterns: copilot, a parallel multi-agent war room, and an autonomous sentinel triggered by anomalies.
5. **Ops.** One image, five roles; compose and a Helm chart; Prometheus, Grafana and alert rules; CI that tests on real Postgres and Redis and smoke-tests the built images.
6. **One thing I am proud of.** Exactly-once effects on an at-least-once stream, with lease-based partition ownership and fencing, all tested by killing processes mid-batch.

Then stop and let them pick the thread.

---

## 2. Three journeys to walk through on a whiteboard

### A tick, from feed to browser

1. The **ingestor** receives a trade (symbol, price, size, time). It does no computation: it appends the tick to `md.ticks.<partition>`, where the partition is `crc32(symbol) % N`. Same symbol, same partition, so per-symbol order is kept. *(crc32, not Python's `hash()`, because `hash()` is salted per process and every producer and consumer must agree.)*
2. A **stream processor** that owns that partition reads it through a consumer group. For each tick it updates the symbol's in-memory state: the open 1-minute bar, the day's high/low/volume, last price.
3. When a minute closes, the bar is **upserted** into Postgres (`ON CONFLICT DO UPDATE`), into a monthly-partitioned `candles` table.
4. The **anomaly detector** buckets prices into 5-second slots and flags a move that is both above 0.6% and more than 4 standard deviations of that stock's normal 30-second move.
5. In one **Redis MULTI/EXEC transaction** it saves each symbol's checkpoint (including the stream id of the last tick applied), the latest quote, and acknowledges the batch.
6. Quotes are **conflated**: at most one update per symbol every 250 ms goes out on pub/sub.
7. Each **API process** has one Redis subscription and fans quotes out to its WebSocket clients, again keeping only the newest pending quote per symbol per client.

Measured: **p50 3–4 ms, p95 5–9 ms** from the tick's timestamp to fan-out.

### An order, from click to fill

1. The browser sends `POST /api/orders` with an **Idempotency-Key** header (a fresh UUID per click).
2. The order service opens a transaction and takes `SELECT ... FOR UPDATE` on **this user's cash account row**. That row is the per-user mutex.
3. It checks the idempotency key (unique on `(user_id, key)`): if this key was seen, it returns the original order.
4. It gathers the risk context (cash, cash already reserved by open buys, holdings, open sells, equity, latest quote and its age, orders in the last minute) and runs the **pure risk function**. Ten named checks: tradable, quantity/lot, order rate (30/min), live price (≤ 20 s old), tick grid, price band (±10%), price, order size (≤ ₹5 lakh), buying power, concentration (≤ 40% of equity), holdings (no short selling).
5. Accepted orders **reserve cash**. A market buy reserves 1% extra because its fill price is unknown.
6. An **outbox** row is written in the same transaction; a "wake" message tells executors to try the order now rather than at the next tick.
7. An **executor** selects open orders that the current price satisfies, `FOR UPDATE SKIP LOCKED`, and for each one, in one transaction: posts the ledger transaction, inserts the fill, updates the position (weighted average cost; realised P&L on sells), marks the order filled.
8. The outbox relay publishes the events after commit; the user's sockets receive them.

### An agent run

1. `POST /api/agent/runs` inserts a `queued` row and appends a job to the `agent.jobs` stream. The request returns immediately (HTTP 202).
2. An agent worker claims the run with `UPDATE ... WHERE status = 'queued' RETURNING`, so a redelivered job never runs twice.
3. The loop: call the model with the tool list → if it asks for tools, run them **concurrently**, return all results in **one** message → repeat until it answers or hits 8 steps. On the last step tools are withheld, forcing an answer.
4. Every step (model text, tool call, tool result, final) is written to `agent_steps` and to a per-run Redis stream.
5. The browser follows `GET /runs/{id}/events` (Server-Sent Events). On reconnect it sends `Last-Event-ID` and resumes exactly after the last event it saw. After an hour the stream expires and the endpoint replays the steps from Postgres.

---

## 3. Component by component: what, why, and the trade-off

### Redis Streams as the event bus

- **What it gives:** append-only log, consumer groups, per-message acknowledgement, a pending list per consumer, `XAUTOCLAIM` to take over a dead consumer's messages, replay from any id.
- **Why not Kafka:** at this scale one Redis gives all of that *and* the quote cache, rate limiting and pub/sub. Two stateful services (Postgres, Redis) instead of four. The bus interface is the subset Kafka has too, so it can be swapped.
- **Trade-off:** Redis keeps the stream in memory and is single-node here, so it is a single point of failure for the live path and retention is bounded (`MAXLEN ~200k`). Kafka would give replicated, disk-backed, long retention.
- **Streams vs pub/sub:** streams for anything that must not be lost (ticks, jobs, anomalies); pub/sub for UI updates where the next message replaces the last anyway.

### Exactly-once effects

- Delivery is **at-least-once**: a consumer can crash after processing and before acknowledging, so the message is delivered again.
- Two tools make the *effects* exactly-once:
  1. **Dedup by position.** Each symbol's state stores the stream id of the last tick applied. A tick at or below it is skipped.
  2. **Atomic checkpoint + ack.** State and acknowledgement are written in one Redis transaction, so they can never disagree.
- Candles are written to Postgres *before* the checkpoint. If the process dies between the two, replay recomputes the same candle and the upsert writes the same row: idempotent.
- **Tests:** deliver the same batch twice (volume counted once); kill a processor with unacknowledged ticks and start a new one (state restored from checkpoint, the dead consumer's ticks adopted, every tick counted once).

### Partition leases and fencing

- A partition's state lives in one process's memory, so two processors must never own it at once.
- **Lease:** `SET lease.p3 <owner> NX PX 15000`; the owner renews every 5 s with a Lua compare-and-extend. If it dies, the key expires and another processor takes it.
- **Fair share:** each processor registers a heartbeat key, counts live processors and holds at most `ceil(partitions / processors)`, releasing extras.
- **Fencing:** a processor can freeze (GC pause, stalled VM) past its lease and wake up thinking it still owns the partition. The checkpoint transaction `WATCH`es the lease keys and checks the owner; if the lease changed, Redis aborts `EXEC`, and the zombie drops the partition. This is the same problem fencing tokens solve in distributed locks.
- **Measured:** two replicas split partitions `[0]` and `[1]`; after `docker kill` on one, the other held `[0, 1]` and backlog stayed at 0.

### The ledger

- **Double entry:** each transaction is a set of legs; per asset, they sum to zero. Shares are an asset too, so buying moves rupees one way and shares the other.
- **Three layers of enforcement:** the Python `post()` refuses unbalanced legs; a **deferred constraint trigger** re-checks at COMMIT (deferred so legs can be inserted one by one); a `CHECK (user_id IS NULL OR balance >= 0)` forbids customer overdrafts.
- **Append-only:** a trigger rejects `UPDATE` and `DELETE` on entries. Corrections are new, opposite transactions.
- **Savepoints:** the posting runs in a nested transaction, so an overdraft rolls back only that posting and the caller can still record a rejected order in the same transaction.
- **Hot accounts:** the system's market and fee accounts appear in every trade. Locking or caching their balances would serialise all trades. So only customer balances are cached and locked; system balances are derived from entries when needed.
- **Deadlocks:** locks are always taken in sorted account-id order; a test runs 40 opposite-direction transfers concurrently.
- **Money types:** `Decimal` in Python and `NUMERIC` in Postgres. Floats exist only on the analytics path (candles, indicators). Prices snap to the tick grid in the direction that hurts the trader, so rounding never gives anyone a better fill than the market.

### Concurrency on orders

- **Double spending:** two orders read "₹1,00,000 free" at the same moment and both pass. Fix: lock the user's cash row first. Test: eight concurrent orders that each need ~₹3 lakh from a ₹10 lakh account: exactly three are accepted.
- **Idempotency:** a unique constraint on `(user_id, idempotency_key)`, checked *after* taking the lock, so a racing retry waits and then finds the order. Test: eight identical requests at once produce one order.
- **Multiple executors:** `FOR UPDATE SKIP LOCKED` lets each executor take different orders without waiting on each other. Test: two executors, six orders, six fills.
- **Cancel vs fill race:** cancel is `UPDATE ... WHERE status = 'open'`; whichever commits first wins and the other changes nothing.
- **Stale prices:** executors skip ticks older than 20 s, so catching up on a backlog never fills orders at prices the market left behind (a bug I found and fixed).

### Transactional outbox

- **Problem:** "write to the database, then publish an event" can fail between the two steps: either the event is lost or it describes a change that rolled back.
- **Solution:** write the event into an `outbox` table in the same transaction; a relay publishes unpublished rows (`SKIP LOCKED`, so several relays are safe) and marks them sent.
- **Guarantee:** at-least-once; consumers treat events as snapshots or re-fetch state, so a duplicate is harmless.

### WebSocket fan-out and backpressure

- One Redis subscription per API process, not per socket.
- **Slow consumers:** each client holds a dict of the newest pending quote per symbol (conflation) and a bounded queue (200) for events that must not be merged. A send that takes more than 5 s disconnects the client.
- **Auth:** the token is the first WebSocket message, not a URL parameter, because URLs end up in proxy logs.
- **Reconnection:** exponential backoff with jitter, so a server restart does not cause every browser to reconnect at the same instant.
- **Measured:** 300 concurrent sockets, ~7,000 quotes/s delivered, 0 errors.

### Agents

- **Why a hand-written loop and not a framework:** what matters here happens *between* steps: persisting each step, streaming it, budgeting, injecting identity. A framework would hide exactly those points. The loop itself is about 60 lines.
- **Parallel tool calls:** independent lookups run with `asyncio.gather` and all results go back in one message.
- **Step budget:** at most 8 tool turns; the final turn gets no tools so the model must answer with what it has.
- **War room (fan-out / fan-in):** three specialists run concurrently on the same question and do not see each other (no anchoring). `return_exceptions=True` means one failed analyst does not sink the desk; the lead is told the view rests on fewer inputs. If all three fail, the run fails cleanly.
- **Sentinel (autonomous):** triggered by the anomalies stream. Budgeted by a per-symbol cooldown (`SET NX EX 180`) and an hourly global cap (20 briefs). It waits 8 seconds so the move finishes and the headline arrives before it investigates.
- **Offline model:** a deterministic scripted provider lets the whole agent stack run in CI with no key. Every answer it writes is labelled. It is never a silent fallback for a real model.

### Agent safety (prompt injection and authority)

- **Identity is not an argument.** Tools that touch user data take no user id; the runtime injects the authenticated user. The model cannot request another user's portfolio because there is nowhere to put the id.
- **Closed schemas.** Every tool's schema has all fields required and `additionalProperties: false`; inputs are validated before the handler runs (an extra `user_id` field is rejected).
- **Untrusted text.** News results are labelled as third-party; the system prompt says to treat them as information, not instructions.
- **No order tool.** The only write tool drafts a proposal. Approval is an authenticated HTTP call from a person. Even a successful injection can at most produce a draft that a human then sees, risk-checked.
- **Two risk checks:** at draft time (feedback to the agent, a vetted plan for the user) and again at execution (prices and balances moved). Proposals expire after 15 minutes.
- **Atomic decision:** approve/reject is one conditional `UPDATE`, so a double click or an approve racing a reject resolves to exactly one outcome. Orders from an approval use idempotency keys derived from the proposal id, so a retried approval cannot double-place.

### LLM gateway

- One code path for every model call: per-user **daily token budget** in Redis, retries with full-jitter backoff for retryable errors, **failover between real providers** (Claude → Groq), token and cost accounting into Prometheus.
- **Refusals are final:** a model declining is not shopped to the next provider.
- **Claude adapter:** official SDK, strict tool schemas, `cache_control` on the system prompt (kept byte-stable: no timestamps), effort level from config, server-side refusal fallback, and assistant turns replayed *exactly* as returned (reasoning blocks are only valid if returned unchanged).
- **Groq adapter:** translates block-style tool use to Chat Completions (`tool_calls` with JSON-string arguments, one `role: tool` message per result).

### Database design

- **Monthly range partitions on candles:** each index stays small, the planner skips months outside the query window, and retention is `DROP PARTITION`, not a huge `DELETE`. A default partition catches anything outside the created months.
- **Wider candles on read:** only 1-minute bars are stored; 5m/15m/1h are built with `date_bin` and `array_agg(... ORDER BY ts)` for first/last. The live, still-open bar is merged in from Redis.
- **Partial index** on open orders: `WHERE status = 'open'`; the executor only ever asks for those and they are a tiny fraction of the table.
- **Full-text search** on news: a generated `tsvector` column with a GIN index, queried with `websearch_to_tsquery` (free text, never throws on odd input).
- **Dedup key** on news (hash of source, symbol, time, headline) makes re-ingestion idempotent.
- **Schema as SQL:** the migration is hand-written SQL because the important parts (partitioning, deferred triggers, generated columns, partial indexes) have no ORM equivalent. A test checks the ORM models match the migrated schema.

### DevOps

- **One image, many roles:** `bazaarpulse api`, `worker ingestor|processor|executor|agents`, `migrate`. One artifact to build, scan and promote.
- **Image:** multi-stage, dependencies installed from the lockfile in their own layer (fast rebuilds), runtime runs as uid 10001 with no build tools and no pip. ~360 MB backend, ~76 MB web.
- **Compose:** migrations run as a one-shot service; everything else waits for it to *complete successfully*. Each worker scales independently.
- **Kubernetes (Helm):** migration as a `pre-upgrade` hook (a failed migration stops the release and old pods keep serving); readiness (`/readyz`, checks Postgres and Redis) separate from liveness (`/healthz`, checks only the process) so a database outage removes pods from the load balancer instead of restart-looping them; `maxUnavailable: 0`; PDB; HPA for API and agents; ingestor with `replicas: 1` and `Recreate` (two ingestors would publish every tick twice); read-only root filesystem; config checksum annotation so config changes roll pods.
- **Observability:** Prometheus metrics labelled by route *template* (`/api/candles/{symbol}`), never the raw path, to bound cardinality; workers expose `/metrics` on their own port and Prometheus finds replicas through DNS; alert rules on symptoms (no ticks processed for 2 minutes, backlog growing, orders accepted but not filling, 5xx rate, model failure rate, model spend per hour).
- **CI:** backend tests on real Postgres and Redis service containers; frontend lint/types/tests/build; Helm lint plus kubeconform schema validation; images built with layer caching, scanned with Trivy (fails on fixable high/critical CVEs), started together and smoke-tested end to end; pushed to GHCR on `main`.

---

## 4. Numbers to remember

| | |
|---|---|
| Tick → fan-out latency | p50 3–4 ms, p95 5–9 ms |
| Fan-out under load | 300 sockets, ~7,000 quotes/s, 0 errors |
| Place-order latency, 40 concurrent traders, 4 vCPUs shared with the load generator | 1 API process p50 411 ms / p95 2.2 s → 3 processes p50 166 ms / p95 1.5 s |
| Failover | partition taken over within the 15 s lease; backlog 0 |
| Tests | 102 backend (real Postgres + Redis, property-based, concurrency, crash recovery), 8 frontend |
| Quote conflation | 250 ms per symbol |
| Lease | 15 s TTL, renewed every 5 s |
| Risk limits | 30 orders/min/user, ₹5 lakh per order, 40% concentration, ±10% price band, quote ≤ 20 s old |
| Agent limits | 8 tool steps, 400k tokens/user/day, sentinel 180 s cooldown per symbol and 20 briefs/hour |

Say how they were measured: a 4-vCPU shared VM, with the load generator on the same machine. They are good for comparing configurations, not as a capacity claim.

---

## 5. Questions you will be asked, and good answers

### System design

**How would this scale to 10,000 symbols and 100,000 users?**
Ticks: raise `TICK_PARTITIONS` and add processors (they scale up to the partition count; leases rebalance automatically). Move the tick log to Kafka for replicated, disk-backed retention. Fan-out: more API pods; each holds one subscription, so Redis load grows with pods, not users; for very large fan-out, shard pub/sub by symbol so a pod only subscribes to symbols its clients watch. Postgres: candles are already partitioned; add a read replica for chart queries, and TimescaleDB or pre-aggregated tables for wide intervals. Orders: already per-user locked, so they scale with users; executors scale with `SKIP LOCKED`. Agents: separate deployment, HPA on CPU or queue length.

**What is the single point of failure?**
Redis on the live path. If it goes down, quotes and new orders stop; the ledger and history are safe in Postgres. Mitigations: Redis Sentinel or a managed replicated Redis; or Kafka for the log and Redis only for cache.

**Why not process ticks inside the API?**
Then a burst of ticks competes with user requests for the same event loop, and scaling one means scaling both. Separate processes let each tier scale and fail on its own.

**Why partition by symbol?**
Per-symbol state (the open bar, the detector window) must see that symbol's ticks in order and in one place. Hashing the symbol to a partition gives ordering and lets different symbols be processed in parallel.

**How do you know the processors keep up?**
Consumer-group lag (`XINFO GROUPS`) is exported as a metric and shown on the System page; an alert fires if it grows past 5,000 for 3 minutes.

### Databases and money

**Why double entry? Isn't a balance column enough?**
A balance column says what you have, not why. Entries are an immutable history from which any balance can be recomputed and audited. And "sums to zero" is an invariant the database can check, so a whole class of bugs becomes impossible rather than unlikely.

**What isolation level? Why not SERIALIZABLE?**
READ COMMITTED plus explicit row locks on exactly the rows that matter (the user's cash row; open orders with SKIP LOCKED). SERIALIZABLE would also be correct but turns contention into retry storms, and every code path would need retry logic. Targeted locks make the contention points explicit.

**How do you prevent deadlocks?**
Lock in a consistent order (sorted account ids), keep transactions short, and avoid locking hot shared rows (system accounts) at all.

**How does idempotency work end to end?**
The client generates a key per user action; the server stores it with a unique constraint; a duplicate returns the original result. The same idea appears internally: fills use `fill:<order_id>` as the ledger transaction's key, and approvals use `proposal:<id>:<index>` for the orders they create.

### Distributed systems

**At-least-once vs exactly-once?**
Exactly-once *delivery* is not achievable over an unreliable network. Exactly-once *effects* are: deliver at least once, and make processing idempotent (dedupe by stream position, upserts) with the checkpoint and the acknowledgement committed atomically.

**What is fencing and why do you need it?**
A lease can expire while its holder is paused. When the holder resumes, it may still act as owner. Fencing makes the shared store reject writes from a stale owner. Here, the checkpoint transaction watches the lease key, so Redis refuses the zombie's write.

**What happens if the executor crashes mid-fill?**
The fill is one Postgres transaction. Either everything committed (ledger, fill, position, status, outbox) or nothing did and the order is still open for the next price.

### AI and agents

**How do you stop prompt injection from placing trades?**
By not relying on the model to behave. There is no tool that places orders; the model's only write action is drafting a proposal, approval is a separate authenticated call by a person, and both draft and execution go through the same risk engine as manual orders. Identity is injected server-side, tool schemas are closed, and third-party text is labelled.

**How do you control cost?**
Per-user daily token budget enforced in the gateway; step limit per run; the autonomous agent has a cooldown and an hourly cap; prompt caching on the stable system prompt; cost per run is recorded and an alert fires on hourly spend.

**How do you test an agent system without paying for model calls?**
A deterministic offline provider that follows a script per role and builds answers only from tool results. It exercises the real loop, parallel tool calls, persistence, streaming, proposals and the multi-agent desk in CI. Wire-level tests drive the real Anthropic SDK against a mock HTTP transport to check the exact request (strict tools, cache control, no sampling parameters, refusal fallback) and the replay of tool-use turns.

**What would you add for production AI?**
An evaluation set (does the agent call the right tools, are its numbers grounded in tool results, does it refuse when it should), run against a real model in CI; tracing of each run; and per-tool latency budgets.

### DevOps

**Liveness vs readiness?**
Liveness: should this process be restarted? Only checks the process itself. Readiness: should it receive traffic right now? Checks dependencies. If readiness checked nothing and liveness checked the database, a database outage would restart every pod in a loop and make recovery slower.

**How do you deploy schema changes safely?**
Migrations run as a pre-upgrade job; if it fails, the release stops and the old version keeps serving. Changes should be backward compatible (add first, backfill, then remove in a later release) so old and new pods can run against the same schema during a rolling update.

**Why one image for all roles?**
One build, one scan, one tag to promote. Roles cannot drift apart in dependencies.

---

## 6. Bugs I found while building it (good interview stories)

Each one is a real bug from this codebase, how it showed up, and the fix. Pick two.

1. **Nginx 502s after every API redeploy.** During the load test I recreated the API container and every request through nginx returned 502. The logs showed nginx still sending traffic to the old container's IP: `proxy_pass http://api:8000` resolves the name *once, at startup*. Fix: a `resolver` with `valid=10s` and the upstream in a variable, so nginx re-resolves. Proved it by forcing the API onto a new IP and watching nginx find it within 4 seconds.
2. **Vulnerable packages hiding inside pip.** The image scan reported HIGH CVEs in urllib3 and msgpack, which were not in my lockfile. They were pip's own vendored copies in the base image. Nothing installs packages at runtime, so pip was removed from the runtime image. The same scan found 3 critical and 65 high CVEs in the old nginx base image, fixed by moving to a newer base and upgrading OS packages at build time. The scanner also flagged a fake "secret" in a third-party SDK docstring, so image scans are limited to vulnerabilities.
3. **A backlog could fill orders at old prices.** If the executor was down and came back, it would process queued ticks from minutes ago and fill new orders at those prices. Fix: executors ignore ticks older than the maximum quote age.
4. **ORM objects expiring inside a savepoint.** An overdraft rolled back a nested transaction, which expired the SQLAlchemy objects loaded inside it; touching them afterwards tried to lazy-load from an async session and crashed (`MissingGreenlet`). Fix: ledger accounts became immutable value objects read with Core queries, and writes became explicit `INSERT`/`UPDATE` statements.
5. **The WebSocket hub died silently on a Redis drop.** The single subscription loop raised, the task ended, and every browser stayed connected but received nothing. Fix: the hub resubscribes in a loop with a pause and logs the loss. Lesson: a background task that dies quietly is the worst failure mode; workers run under a supervisor that restarts them with backoff for the same reason.
6. **Charts empty after a quiet period.** The candle query only looked back `limit × width`, so after a restart or overnight there was nothing in the window. Fix: always look back at least a week; the time bound still lets Postgres skip old partitions.
7. **Rate limit charged for invalid requests.** The agent endpoint counted a request against the user's limit before validating it, so malformed requests used up the allowance. Fix: validate first, then rate-limit.
8. **A single API process was the order-latency bottleneck.** The load test showed p50 411 ms. Running three processes brought it to 166 ms. The load generator shared the same four cores, so part of the remaining tail is the measurement itself; I say that when I quote the numbers.

---

## 7. Weaknesses to admit before they find them

- Redis is a single point of failure on the live path.
- Fills are simplified: full fills at last price plus fixed slippage, no order book.
- The anomaly detector's thresholds were tuned by eye on simulated data.
- The offline agent model is a script; agent quality is untested without a real model and an evaluation set.
- Benchmarks come from a small shared VM.

Saying these yourself shows you understand the system's limits, which is most of what a senior interviewer is checking.

---

## 8. Glossary

| Term | Meaning here |
|---|---|
| Consumer group | Redis Streams feature: several consumers share a stream; each message goes to one of them and stays pending until acknowledged |
| Conflation | Keeping only the latest value per key while a consumer is busy, instead of queueing every update |
| Fencing | Making a shared store reject writes from a lock holder whose lease has expired |
| Outbox | Writing events in the same transaction as the state change, publishing after commit |
| `SKIP LOCKED` | Postgres option: skip rows another transaction has locked instead of waiting for them |
| Deferred constraint trigger | A check that runs at COMMIT rather than per statement |
| Idempotency key | A client-chosen id that makes retrying a request safe |
| Lease | A lock with an expiry, kept alive by renewing it |
| Backpressure | Stopping a fast producer from overwhelming a slow consumer |
| Fan-out / fan-in | Running several agents in parallel, then combining their results |
| Prompt caching | The provider reuses an identical prompt prefix across calls, so it is cheaper and faster |
