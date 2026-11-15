// Shapes of what the backend returns. Kept in one file so a contract change shows up in one place.

export interface Instrument {
  symbol: string;
  name: string;
  exchange: string;
  sector: string;
  currency: string;
  tick_size: number;
}

export interface Candle {
  t: number; // epoch seconds, start of the bar
  o: number;
  h: number;
  l: number;
  c: number;
  v: number;
}

export interface Quote {
  symbol: string;
  last: number;
  prev_close: number;
  change: number;
  change_pct: number;
  open: number;
  high: number;
  low: number;
  volume: number;
  ts: number;
  bar: Candle | null;
}

export interface Position {
  symbol: string;
  quantity: number;
  avg_cost: number;
  last: number;
  market_value: number;
  unrealized_pnl: number;
  unrealized_pct: number;
  day_change_pct: number;
  weight_pct: number;
}

export interface Portfolio {
  currency: string;
  cash: number;
  reserved_cash: number;
  buying_power: number;
  market_value: number;
  equity: number;
  deposits: number;
  total_pnl: number;
  total_pnl_pct: number;
  unrealized_pnl: number;
  realized_pnl: number;
  fees_paid: number;
  positions: Position[];
}

export interface RiskCheck {
  name: string;
  passed: boolean;
  detail: string;
}

export interface RiskReport {
  passed: boolean;
  reference_price: string | null;
  notional: string;
  estimated_fee: string;
  required_cash: string;
  checks: RiskCheck[];
}

export type Side = "buy" | "sell";
export type OrderType = "market" | "limit" | "stop";

export interface Order {
  id: string;
  symbol: string;
  side: Side;
  type: OrderType;
  quantity: number;
  limit_price: number | null;
  stop_price: number | null;
  status: "open" | "filled" | "cancelled" | "rejected";
  avg_fill_price: number | null;
  reject_reason: string | null;
  source: "user" | "agent";
  created_at: number;
}

export interface Fill {
  id: string;
  symbol: string;
  side: Side;
  quantity: number;
  price: number;
  fee: number;
  ts: number;
}

export interface Indicators {
  bars: number;
  trend?: "up" | "down" | "sideways" | "unknown";
  rsi14?: number | null;
  rsi_state?: string | null;
  vs_vwap_pct?: number | null;
  atr14_pct?: number | null;
  ema9?: number | null;
  ema21?: number | null;
}

export type Signal = "bullish" | "bearish" | "neutral" | null;

export interface AgentRun {
  id: string;
  kind: "copilot" | "war_room" | "sentinel";
  status: "queued" | "running" | "completed" | "failed";
  symbol: string | null;
  input: { message?: string };
  output: {
    text?: string;
    signal?: Signal;
    confidence?: string | null;
    reports?: Record<string, { text: string; signal: Signal; confidence: string | null }>;
  } | null;
  provider: string | null;
  model: string | null;
  input_tokens: number;
  output_tokens: number;
  cost_usd: number;
  error: string | null;
  created_at: number;
}

export interface AgentStep {
  seq: number;
  agent: string;
  type: "status" | "message" | "tool_call" | "tool_result" | "report" | "final" | "error" | "done";
  payload: Record<string, unknown>;
  ts: number;
}

export interface ProposedOrder {
  symbol: string;
  side: Side;
  order_type: "market" | "limit";
  quantity: number;
  limit_price: number | null;
  estimated_value: number;
}

export interface Proposal {
  id: string;
  run_id: string | null;
  status: "pending" | "approved" | "rejected" | "expired" | "executed" | "failed";
  rationale: string;
  orders: ProposedOrder[];
  risk_report: { passed: boolean; orders: Array<RiskReport & { symbol: string; side: Side }> };
  created_at: number;
  expires_at: number;
}

// Items on the live "pulse" feed.
export type PulseItem =
  | { type: "anomaly"; id: string; symbol: string; kind: string; severity: string; move_pct: number; zscore: number; ts: number }
  | { type: "news"; symbol: string | null; headline: string; source: string; sentiment: number | null; ts: number; is_simulated: boolean }
  | { type: "brief"; anomaly_id: string | null; symbol: string; text: string; run_id: string; ts: number };

export interface SystemStats {
  version: string;
  market_provider: string;
  pipeline: { ticks_total?: number; ticks_per_second?: number; latency_p50_ms?: number; latency_p95_ms?: number; symbols?: number };
  stream_lag: Record<string, number | null>;
  websocket_clients: number;
  open_orders: number;
  ledger: { ok: boolean; transactions: number; entries: number };
  llm: { provider: string; model: string; runs_24h: number; input_tokens_24h: number; output_tokens_24h: number; cost_usd_24h: number };
}
