import { Blotter } from "../components/Blotter";
import { OrderTicket } from "../components/OrderTicket";
import { PriceChart } from "../components/PriceChart";
import { PulseFeed } from "../components/PulseFeed";
import { Watchlist } from "../components/Watchlist";
import { direction, money, percent, toneClass } from "../lib/format";
import { useApp, useLiveAccount } from "../store";

function AccountStrip() {
  const account = useLiveAccount();
  const cells: Array<[string, string, string?]> = account
    ? [
        ["Equity", money(account.equity)],
        ["Cash", money(account.cash)],
        ["Buying power", money(account.buying_power)],
        ["Invested", money(account.market_value)],
        ["Total P&L", `${money(account.total_pnl)}  ${percent(account.total_pnl_pct)}`, toneClass[direction(account.total_pnl)]],
        ["Fees paid", money(account.fees_paid, { paise: true })],
      ]
    : [];
  return (
    <section className="panel grid shrink-0 grid-cols-3 gap-y-2 px-4 py-2.5 xl:grid-cols-6">
      {cells.map(([label, value, tone]) => (
        <div key={label}>
          <div className="label">{label}</div>
          <div className={`num text-[15px] font-semibold whitespace-pre ${tone ?? ""}`}>{value}</div>
        </div>
      ))}
    </section>
  );
}

export function Terminal() {
  // The shock control drives the simulator, so it is only shown when that is the feed in use.
  const simulated = useApp((s) => s.marketProvider === "simulator");
  return (
    <div className="grid h-full min-h-0 grid-cols-[230px_minmax(0,1fr)_310px] gap-3">
      <Watchlist />
      <div className="flex min-h-0 min-w-0 flex-col gap-3">
        <AccountStrip />
        <PriceChart />
        <Blotter />
      </div>
      <div className="flex min-h-0 flex-col gap-3">
        <OrderTicket />
        <PulseFeed simulated={simulated} />
      </div>
    </div>
  );
}
