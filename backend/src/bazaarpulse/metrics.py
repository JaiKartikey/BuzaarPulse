"""Prometheus metrics for every process.

Labels are kept low-cardinality on purpose (no symbol, no user id): each
distinct label value is a separate time series that Prometheus has to store.
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

_LATENCY_BUCKETS = (0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10)

# --- Market data pipeline ---------------------------------------------------
TICKS_INGESTED = Counter("bp_ticks_ingested_total", "Ticks published to the tick stream", ["source"])
TICKS_PROCESSED = Counter("bp_ticks_processed_total", "Ticks applied by the stream processor")
TICKS_DUPLICATE = Counter("bp_ticks_duplicate_total", "Redelivered ticks skipped by the processor")
CANDLES_WRITTEN = Counter("bp_candles_written_total", "Closed candles upserted to Postgres")
PIPELINE_LATENCY = Histogram("bp_pipeline_latency_seconds", "Tick timestamp to quote fan-out", buckets=_LATENCY_BUCKETS)
STREAM_LAG = Gauge("bp_stream_lag_messages", "Undelivered messages per consumer group", ["stream", "group"])
ANOMALIES_DETECTED = Counter("bp_anomalies_detected_total", "Anomalies raised", ["kind"])
WS_CONNECTIONS = Gauge("bp_ws_connections", "Open WebSocket connections")
WS_MESSAGES = Counter("bp_ws_messages_sent_total", "WebSocket frames sent", ["type"])

# --- Trading ----------------------------------------------------------------
ORDERS = Counter("bp_orders_total", "Orders by outcome", ["status", "source"])
FILLS = Counter("bp_fills_total", "Fills executed", ["side"])
ORDER_TO_FILL = Histogram("bp_order_to_fill_seconds", "Order accepted to fill, market orders", buckets=_LATENCY_BUCKETS)
OUTBOX_PUBLISHED = Counter("bp_outbox_published_total", "Outbox events relayed")

# --- Agents -----------------------------------------------------------------
AGENT_RUNS = Counter("bp_agent_runs_total", "Agent runs by outcome", ["kind", "status"])
AGENT_RUN_SECONDS = Histogram(
    "bp_agent_run_seconds", "Agent run duration", ["kind"], buckets=(0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300)
)
LLM_REQUESTS = Counter("bp_llm_requests_total", "LLM calls by outcome", ["provider", "outcome"])
LLM_TOKENS = Counter("bp_llm_tokens_total", "LLM tokens", ["provider", "direction"])
LLM_COST = Counter("bp_llm_cost_usd_total", "Estimated LLM spend in USD", ["provider"])
TOOL_CALLS = Counter("bp_agent_tool_calls_total", "Agent tool calls", ["tool", "outcome"])

# --- HTTP -------------------------------------------------------------------
HTTP_REQUESTS = Counter("bp_http_requests_total", "HTTP requests", ["method", "route", "status"])
HTTP_LATENCY = Histogram(
    "bp_http_request_seconds", "HTTP request duration", ["method", "route"], buckets=_LATENCY_BUCKETS
)
