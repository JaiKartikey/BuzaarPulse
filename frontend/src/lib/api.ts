import type {
  AgentRun,
  AgentStep,
  Candle,
  Fill,
  Indicators,
  Instrument,
  Order,
  OrderType,
  Portfolio,
  Proposal,
  PulseItem,
  Quote,
  RiskReport,
  Side,
  SystemStats,
} from "./types";

const TOKEN_KEY = "bazaarpulse.token";

export class ApiError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }
}

let token: string | null = readStoredToken();
let onUnauthorized: () => void = () => {};

function readStoredToken(): string | null {
  try {
    return localStorage.getItem(TOKEN_KEY);
  } catch {
    return null; // storage can be blocked (private mode); the session then lives in memory only
  }
}

export function getToken(): string | null {
  return token;
}

export function setToken(next: string | null): void {
  token = next;
  try {
    if (next) localStorage.setItem(TOKEN_KEY, next);
    else localStorage.removeItem(TOKEN_KEY);
  } catch {
    /* see readStoredToken */
  }
}

export function setUnauthorizedHandler(handler: () => void): void {
  onUnauthorized = handler;
}

async function request<T>(path: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers);
  if (token) headers.set("Authorization", `Bearer ${token}`);
  if (init.body) headers.set("Content-Type", "application/json");
  const response = await fetch(path, { ...init, headers });
  if (response.status === 401 && token) onUnauthorized(); // the token expired: back to the login screen
  if (!response.ok) {
    const body = await response.json().catch(() => null);
    const detail = body?.detail;
    const message = typeof detail === "string" ? detail : Array.isArray(detail) ? detail.map((d) => d.msg).join("; ") : response.statusText;
    throw new ApiError(response.status, message);
  }
  return response.json() as Promise<T>;
}

const post = <T>(path: string, body?: unknown, headers?: Record<string, string>) =>
  request<T>(path, { method: "POST", body: body === undefined ? undefined : JSON.stringify(body), headers });

export interface Session {
  token: string;
  user: { id: string; email: string; display_name: string };
}

export interface OrderInput {
  symbol: string;
  side: Side;
  type: OrderType;
  quantity: number;
  limit_price?: number | null;
  stop_price?: number | null;
}

export const api = {
  demoLogin: () => post<Session>("/api/auth/demo"),
  login: (email: string, password: string) => post<Session>("/api/auth/login", { email, password }),
  register: (email: string, password: string, display_name: string) =>
    post<Session>("/api/auth/register", { email, password, display_name }),
  me: () => request<Session["user"]>("/api/me"),

  instruments: () => request<Instrument[]>("/api/instruments"),
  quotes: () => request<Record<string, Quote>>("/api/quotes"),
  candles: (symbol: string, interval: string, limit = 300) =>
    request<Candle[]>(`/api/candles/${symbol}?interval=${interval}&limit=${limit}`),
  indicators: (symbol: string, interval: string) => request<Indicators>(`/api/indicators/${symbol}?interval=${interval}`),
  anomalies: () => request<Array<Extract<PulseItem, { type: "anomaly" }> & { brief: string | null }>>("/api/anomalies?limit=20"),
  news: () => request<Array<Extract<PulseItem, { type: "news" }>>>("/api/news?limit=15"),

  portfolio: () => request<Portfolio>("/api/portfolio"),
  orders: () => request<Order[]>("/api/orders"),
  fills: () => request<Fill[]>("/api/fills"),
  previewOrder: (order: OrderInput) => post<RiskReport>("/api/orders/preview", order),
  // The key is generated once per click. If the request is retried, the server returns the same order.
  placeOrder: (order: OrderInput, idempotencyKey: string) =>
    post<Order>("/api/orders", order, { "Idempotency-Key": idempotencyKey }),
  cancelOrder: (id: string) => request<Order>(`/api/orders/${id}`, { method: "DELETE" }),

  startRun: (kind: "copilot" | "war_room", message: string, symbol?: string) =>
    post<AgentRun>("/api/agent/runs", { kind, message, symbol }),
  runs: () => request<AgentRun[]>("/api/agent/runs"),
  run: (id: string) => request<AgentRun & { steps: AgentStep[] }>(`/api/agent/runs/${id}`),
  proposals: () => request<Proposal[]>("/api/proposals"),
  decideProposal: (id: string, decision: "approve" | "reject") => post<Proposal>(`/api/proposals/${id}/${decision}`),

  shock: (symbol: string, pct: number) => post<{ symbol: string }>("/api/sim/shock", { symbol, pct }),
  stats: () => request<SystemStats>("/api/system/stats"),
};
