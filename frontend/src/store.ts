import { create } from "zustand";
import { api, getToken, setToken, type Session } from "./lib/api";
import type { ConnectionState, LiveMessage } from "./lib/live";
import type { Fill, Instrument, Order, Portfolio, Proposal, PulseItem, Quote } from "./lib/types";

const PULSE_LIMIT = 60;

export type View = "terminal" | "desk" | "system";

interface AppState {
  user: Session["user"] | null;
  view: View;
  connection: ConnectionState;
  marketProvider: string;
  instruments: Instrument[];
  quotes: Record<string, Quote>;
  selected: string;
  pulse: PulseItem[];
  portfolio: Portfolio | null;
  orders: Order[];
  fills: Fill[];
  proposals: Proposal[];

  signIn: (session: Session) => void;
  signOut: () => void;
  setView: (view: View) => void;
  select: (symbol: string) => void;
  setConnection: (state: ConnectionState) => void;
  bootstrap: () => Promise<void>;
  refreshAccount: () => Promise<void>;
  refreshProposals: () => Promise<void>;
  handleLive: (message: LiveMessage) => void;
}

export const useApp = create<AppState>((set, get) => ({
  user: null,
  view: "terminal",
  connection: "connecting",
  marketProvider: "",
  instruments: [],
  quotes: {},
  selected: "",
  pulse: [],
  portfolio: null,
  orders: [],
  fills: [],
  proposals: [],

  signIn: (session) => {
    setToken(session.token);
    set({ user: session.user });
  },
  signOut: () => {
    setToken(null);
    set({ user: null, portfolio: null, orders: [], fills: [], proposals: [], pulse: [] });
  },
  setView: (view) => set({ view }),
  select: (symbol) => set({ selected: symbol }),
  setConnection: (connection) => set({ connection }),

  /** Everything the app needs before the live feed takes over. */
  bootstrap: async () => {
    if (!getToken()) return;
    const user = await api.me();
    const [instruments, quotes, anomalies, news, stats] = await Promise.all([
      api.instruments(),
      api.quotes(),
      api.anomalies(),
      api.news(),
      api.stats(),
    ]);
    const history: PulseItem[] = [
      ...anomalies.flatMap((a): PulseItem[] => [
        { type: "anomaly", id: a.id, symbol: a.symbol, kind: a.kind, severity: a.severity, move_pct: a.move_pct, zscore: a.zscore, ts: a.ts },
        ...(a.brief ? [{ type: "brief" as const, anomaly_id: a.id, symbol: a.symbol, text: a.brief, run_id: "", ts: a.ts + 1 }] : []),
      ]),
      ...news.map((n): PulseItem => ({ ...n, type: "news" })),
    ].sort((a, b) => b.ts - a.ts);
    set({
      user,
      instruments,
      quotes,
      marketProvider: stats.market_provider,
      pulse: history.slice(0, PULSE_LIMIT),
      selected: get().selected || instruments[0]?.symbol || "",
    });
    await Promise.all([get().refreshAccount(), get().refreshProposals()]);
  },

  refreshAccount: async () => {
    const [portfolio, orders, fills] = await Promise.all([api.portfolio(), api.orders(), api.fills()]);
    set({ portfolio, orders, fills });
  },
  refreshProposals: async () => set({ proposals: await api.proposals() }),

  handleLive: (message) => {
    switch (message.type) {
      case "quotes": {
        // One state update per frame, however many symbols it carries.
        const quotes = { ...get().quotes };
        for (const quote of message.quotes) quotes[quote.symbol] = quote;
        set({ quotes });
        break;
      }
      case "anomaly":
      case "news":
      case "brief":
        set({ pulse: [message, ...get().pulse].slice(0, PULSE_LIMIT) });
        break;
      case "order":
      case "fill":
        void get().refreshAccount();
        break;
      case "proposal":
        void get().refreshProposals();
        break;
    }
  },
}));

/** Live equity: cash plus every position at its latest streamed price, so P&L moves tick by tick. */
export function useLiveAccount() {
  const portfolio = useApp((s) => s.portfolio);
  const quotes = useApp((s) => s.quotes);
  if (!portfolio) return null;
  const positions = portfolio.positions.map((p) => {
    const last = quotes[p.symbol]?.last ?? p.last;
    return {
      ...p,
      last,
      market_value: last * p.quantity,
      unrealized_pnl: (last - p.avg_cost) * p.quantity,
      unrealized_pct: p.avg_cost ? (last / p.avg_cost - 1) * 100 : 0,
      day_change_pct: quotes[p.symbol]?.change_pct ?? p.day_change_pct,
    };
  });
  const marketValue = positions.reduce((sum, p) => sum + p.market_value, 0);
  const equity = portfolio.cash + marketValue;
  return {
    ...portfolio,
    positions,
    market_value: marketValue,
    equity,
    total_pnl: equity - portfolio.deposits,
    total_pnl_pct: portfolio.deposits ? (equity / portfolio.deposits - 1) * 100 : 0,
  };
}
