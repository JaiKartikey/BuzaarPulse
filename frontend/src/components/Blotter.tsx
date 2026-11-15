import { useState } from "react";
import { api } from "../lib/api";
import { clock, direction, money, percent, price, toneClass } from "../lib/format";
import type { Order } from "../lib/types";
import { useApp, useLiveAccount } from "../store";

type Tab = "positions" | "orders" | "fills";

const STATUS_TONE: Record<Order["status"], string> = {
  open: "text-ai",
  filled: "text-up",
  cancelled: "text-muted",
  rejected: "text-down",
};

export function Blotter() {
  const [tab, setTab] = useState<Tab>("positions");
  const account = useLiveAccount();
  const orders = useApp((s) => s.orders);
  const fills = useApp((s) => s.fills);
  const openCount = orders.filter((o) => o.status === "open").length;

  const cancel = async (id: string) => {
    await api.cancelOrder(id).catch(() => {});
    await useApp.getState().refreshAccount();
  };

  const th = "label px-3 py-1.5 text-right first:text-left";
  const td = "num px-3 py-1.5 text-right first:text-left";

  return (
    <section className="panel flex h-56 shrink-0 flex-col">
      <header className="flex gap-1 border-b border-line px-2 py-1.5">
        {(
          [
            ["positions", `Positions ${account?.positions.length ?? 0}`],
            ["orders", `Orders ${openCount ? `· ${openCount} open` : ""}`],
            ["fills", "Fills"],
          ] as const
        ).map(([key, label]) => (
          <button
            key={key}
            onClick={() => setTab(key)}
            className={`rounded px-2.5 py-1 text-xs font-medium ${tab === key ? "bg-raised text-text" : "text-muted hover:text-text"}`}
          >
            {label}
          </button>
        ))}
      </header>

      <div className="min-h-0 flex-1 overflow-auto">
        {tab === "positions" &&
          (account?.positions.length ? (
            <table className="w-full">
              <thead className="sticky top-0 bg-panel">
                <tr>
                  {["Symbol", "Qty", "Avg cost", "Last", "Value", "P&L", "P&L %", "Weight"].map((h) => (
                    <th key={h} className={th}>
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-line/60">
                {account.positions.map((p) => (
                  <tr key={p.symbol} className="cursor-pointer hover:bg-raised" onClick={() => useApp.getState().select(p.symbol)}>
                    <td className={`${td} font-sans font-semibold`}>{p.symbol}</td>
                    <td className={td}>{p.quantity}</td>
                    <td className={td}>{price(p.avg_cost)}</td>
                    <td className={td}>{price(p.last)}</td>
                    <td className={td}>{money(p.market_value)}</td>
                    <td className={`${td} ${toneClass[direction(p.unrealized_pnl)]}`}>{money(p.unrealized_pnl)}</td>
                    <td className={`${td} ${toneClass[direction(p.unrealized_pct)]}`}>{percent(p.unrealized_pct)}</td>
                    <td className={`${td} text-muted`}>{account.equity ? ((p.market_value / account.equity) * 100).toFixed(1) : "0.0"}%</td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <Empty text="No positions yet. Place an order from the ticket, or ask the AI desk to draft one." />
          ))}

        {tab === "orders" &&
          (orders.length ? (
            <table className="w-full">
              <thead className="sticky top-0 bg-panel">
                <tr>
                  {["Time", "Symbol", "Side", "Type", "Qty", "Price", "Status", ""].map((h, i) => (
                    <th key={i} className={th}>
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-line/60">
                {orders.map((o) => (
                  <tr key={o.id} title={o.reject_reason ?? undefined}>
                    <td className={`${td} text-muted`}>{clock(o.created_at)}</td>
                    <td className={`${td} font-sans font-semibold`}>
                      {o.symbol}
                      {o.source === "agent" && <span className="ml-1.5 rounded bg-ai/15 px-1 text-[10px] font-medium text-ai">AI</span>}
                    </td>
                    <td className={`${td} font-sans capitalize ${o.side === "buy" ? "text-up" : "text-down"}`}>{o.side}</td>
                    <td className={`${td} font-sans capitalize text-muted`}>{o.type}</td>
                    <td className={td}>{o.quantity}</td>
                    <td className={td}>{price(o.avg_fill_price ?? o.limit_price ?? o.stop_price)}</td>
                    <td className={`${td} font-sans capitalize ${STATUS_TONE[o.status]}`}>{o.status}</td>
                    <td className={td}>
                      {o.status === "open" && (
                        <button onClick={() => void cancel(o.id)} className="font-sans text-xs text-muted underline-offset-2 hover:text-down hover:underline">
                          Cancel
                        </button>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <Empty text="No orders yet." />
          ))}

        {tab === "fills" &&
          (fills.length ? (
            <table className="w-full">
              <thead className="sticky top-0 bg-panel">
                <tr>
                  {["Time", "Symbol", "Side", "Qty", "Price", "Fee"].map((h) => (
                    <th key={h} className={th}>
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-line/60">
                {fills.map((f) => (
                  <tr key={f.id}>
                    <td className={`${td} text-muted`}>{clock(f.ts)}</td>
                    <td className={`${td} font-sans font-semibold`}>{f.symbol}</td>
                    <td className={`${td} font-sans capitalize ${f.side === "buy" ? "text-up" : "text-down"}`}>{f.side}</td>
                    <td className={td}>{f.quantity}</td>
                    <td className={td}>{price(f.price)}</td>
                    <td className={`${td} text-muted`}>{money(f.fee, { paise: true })}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <Empty text="No fills yet." />
          ))}
      </div>
    </section>
  );
}

function Empty({ text }: { text: string }) {
  return <div className="grid h-full place-items-center px-6 text-center text-muted">{text}</div>;
}
