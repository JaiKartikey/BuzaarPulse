import { CircleCheck, CircleX, LoaderCircle } from "lucide-react";
import { useEffect, useState } from "react";
import { api, ApiError, type OrderInput } from "../lib/api";
import { money, price } from "../lib/format";
import type { OrderType, RiskReport, Side } from "../lib/types";
import { useApp } from "../store";

const CHECK_LABELS: Record<string, string> = {
  instrument: "Tradable",
  quantity: "Quantity",
  order_rate: "Order rate",
  market_data: "Live price",
  tick_size: "Tick size",
  price_band: "Price band",
  price: "Price",
  order_size: "Order size",
  buying_power: "Buying power",
  concentration: "Concentration",
  holdings: "Holdings",
};

/** The ticket for one symbol. The parent re-creates it (via `key`) when the symbol changes, which resets the form. */
function Ticket({ symbol }: { symbol: string }) {
  const last = useApp((s) => s.quotes[symbol]?.last);
  const tick = useApp((s) => s.instruments.find((i) => i.symbol === symbol)?.tick_size ?? 0.05);
  const [side, setSide] = useState<Side>("buy");
  const [type, setType] = useState<OrderType>("market");
  const [quantity, setQuantity] = useState("10");
  const [triggerPrice, setTriggerPrice] = useState("");
  const [preview, setPreview] = useState<{ key: string; report: RiskReport } | null>(null);
  const [busy, setBusy] = useState(false);
  const [notice, setNotice] = useState<{ ok: boolean; text: string } | null>(null);

  const qty = Number.parseInt(quantity, 10);
  const level = Number.parseFloat(triggerPrice);
  const needsPrice = type !== "market";
  const valid = qty > 0 && (!needsPrice || level > 0);
  const orderKey = `${side}|${type}|${quantity}|${triggerPrice}`;
  // A preview computed for different inputs is ignored, so the checks never describe an order the user is no longer looking at.
  const report = valid && preview?.key === orderKey ? preview.report : null;

  const chooseType = (next: OrderType) => {
    setType(next);
    setNotice(null);
    // Pre-fill a sensible price on the tick grid, once, when the user asks for a priced order.
    if (next !== "market" && last) setTriggerPrice((Math.round(last / tick) * tick).toFixed(2));
  };

  // Ask the server's risk engine what it would say, a moment after the user stops typing.
  useEffect(() => {
    if (!valid) return;
    let stale = false;
    const order = build();
    const timer = setTimeout(() => {
      api
        .previewOrder(order)
        .then((result) => !stale && setPreview({ key: orderKey, report: result }))
        .catch(() => {});
    }, 250);
    return () => {
      stale = true;
      clearTimeout(timer);
    };
    // `build` reads the same values that make up orderKey.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [orderKey, valid]);

  function build(): OrderInput {
    return {
      symbol,
      side,
      type,
      quantity: qty,
      limit_price: type === "limit" ? level : null,
      stop_price: type === "stop" ? level : null,
    };
  }

  const submit = async () => {
    setBusy(true);
    setNotice(null);
    try {
      // One key per click: if the network retries this request, the server still creates one order.
      const order = await api.placeOrder(build(), crypto.randomUUID());
      setNotice(
        order.status === "rejected"
          ? { ok: false, text: order.reject_reason ?? "Rejected by the risk engine." }
          : { ok: true, text: `${order.side === "buy" ? "Buy" : "Sell"} ${order.quantity} ${order.symbol} accepted.` },
      );
      await useApp.getState().refreshAccount();
    } catch (e) {
      setNotice({ ok: false, text: e instanceof ApiError ? e.message : "The order could not be sent." });
    } finally {
      setBusy(false);
    }
  };

  const segment = (active: boolean) => `flex-1 rounded px-2 py-1 text-xs font-medium capitalize transition ${active ? "bg-raised text-text" : "text-muted hover:text-text"}`;
  const input = "num w-full rounded-md border border-line bg-ink px-2.5 py-1.5 text-right";

  return (
    <section className="panel flex flex-col">
      <header className="label border-b border-line px-3 py-2.5">Order ticket · {symbol}</header>
      <div className="space-y-3 p-3">
        <div className="grid grid-cols-2 gap-1.5">
          {(["buy", "sell"] as const).map((option) => (
            <button
              key={option}
              onClick={() => setSide(option)}
              className={`rounded-md py-1.5 font-semibold capitalize transition ${
                side === option ? (option === "buy" ? "bg-up text-ink" : "bg-down text-ink") : "border border-line text-muted hover:text-text"
              }`}
            >
              {option}
            </button>
          ))}
        </div>

        <div className="flex rounded-md border border-line p-0.5">
          {(["market", "limit", "stop"] as const).map((option) => (
            <button key={option} onClick={() => chooseType(option)} className={segment(type === option)}>
              {option}
            </button>
          ))}
        </div>

        <div className="grid grid-cols-2 gap-2">
          <label>
            <span className="label">Quantity</span>
            <input className={input} inputMode="numeric" value={quantity} onChange={(e) => setQuantity(e.target.value.replace(/\D/g, ""))} />
          </label>
          <label>
            <span className="label">{type === "stop" ? "Stop price" : "Limit price"}</span>
            <input
              className={`${input} disabled:text-faint`}
              inputMode="decimal"
              disabled={!needsPrice}
              value={needsPrice ? triggerPrice : last ? price(last) : ""}
              onChange={(e) => setTriggerPrice(e.target.value.replace(/[^\d.]/g, ""))}
            />
          </label>
        </div>

        {report && (
          <div className="rounded-md border border-line bg-ink/60 p-2.5">
            <div className="num flex justify-between text-xs">
              <span className="font-sans text-muted">Order value</span>
              <span>{money(Number(report.notional), { paise: true })}</span>
            </div>
            <div className="num flex justify-between text-xs">
              <span className="font-sans text-muted">Estimated fee</span>
              <span>{money(Number(report.estimated_fee), { paise: true })}</span>
            </div>
            <ul className="mt-2 space-y-1 border-t border-line pt-2">
              {report.checks.map((check) => (
                <li key={check.name} className="flex items-start gap-1.5 text-[11px]" title={check.detail}>
                  {check.passed ? <CircleCheck size={13} className="mt-px shrink-0 text-up" /> : <CircleX size={13} className="mt-px shrink-0 text-down" />}
                  <span className={check.passed ? "text-muted" : "text-text"}>
                    {CHECK_LABELS[check.name] ?? check.name}
                    {!check.passed && <span className="text-down"> · {check.detail}</span>}
                  </span>
                </li>
              ))}
            </ul>
          </div>
        )}

        <button
          onClick={() => void submit()}
          disabled={!valid || busy || report?.passed === false}
          className={`flex w-full items-center justify-center gap-2 rounded-md py-2 font-semibold text-ink transition disabled:cursor-not-allowed disabled:opacity-40 ${
            side === "buy" ? "bg-up hover:brightness-110" : "bg-down hover:brightness-110"
          }`}
        >
          {busy && <LoaderCircle size={14} className="animate-spin" />}
          {side === "buy" ? "Buy" : "Sell"} {qty > 0 ? qty : ""} {symbol}
        </button>

        {notice && <p className={`slide-in rounded-md px-2.5 py-1.5 text-xs ${notice.ok ? "bg-up/10 text-up" : "bg-down/10 text-down"}`}>{notice.text}</p>}
      </div>
    </section>
  );
}

export function OrderTicket() {
  const symbol = useApp((s) => s.selected);
  return <Ticket key={symbol} symbol={symbol} />;
}
