import { Bot, Newspaper, TrendingDown, TrendingUp, Zap } from "lucide-react";
import { useState } from "react";
import { api } from "../lib/api";
import { ago, percent } from "../lib/format";
import type { PulseItem } from "../lib/types";
import { useApp } from "../store";

function Item({ item }: { item: PulseItem }) {
  const select = () => item.symbol && useApp.getState().select(item.symbol);
  const time = <span className="num ml-auto shrink-0 text-[10px] text-faint">{ago(item.ts)}</span>;

  if (item.type === "anomaly") {
    const up = item.move_pct > 0;
    const Icon = up ? TrendingUp : TrendingDown;
    return (
      <li className="slide-in cursor-pointer px-3 py-2 hover:bg-raised" onClick={select}>
        <div className="flex items-center gap-1.5">
          <Icon size={13} className={up ? "text-up" : "text-down"} />
          <span className="font-semibold">{item.symbol}</span>
          <span className={`num font-medium ${up ? "text-up" : "text-down"}`}>{percent(item.move_pct)}</span>
          <span className="rounded bg-warn/15 px-1 text-[10px] font-medium text-warn uppercase">{item.severity}</span>
          {time}
        </div>
        <p className="mt-0.5 text-[11px] text-muted">Unusual move, {item.zscore}σ beyond its normal range. An agent is looking into it.</p>
      </li>
    );
  }
  if (item.type === "brief") {
    return (
      <li className="slide-in cursor-pointer border-l-2 border-ai bg-ai/5 px-3 py-2 hover:bg-ai/10" onClick={select}>
        <div className="flex items-center gap-1.5 text-ai">
          <Bot size={13} />
          <span className="text-[11px] font-semibold tracking-wide uppercase">AI brief · {item.symbol}</span>
          {time}
        </div>
        <p className="mt-1 text-xs leading-relaxed">{item.text}</p>
      </li>
    );
  }
  return (
    <li className="slide-in cursor-pointer px-3 py-2 hover:bg-raised" onClick={select}>
      <div className="flex items-center gap-1.5 text-muted">
        <Newspaper size={12} />
        <span className="text-[11px]">
          {item.source}
          {item.is_simulated && " · simulated"}
        </span>
        {time}
      </div>
      <p className="mt-0.5 text-xs leading-snug">{item.headline}</p>
    </li>
  );
}

export function PulseFeed({ simulated }: { simulated: boolean }) {
  const pulse = useApp((s) => s.pulse);
  const symbol = useApp((s) => s.selected);
  const [sent, setSent] = useState("");

  const shock = async (pct: number) => {
    await api.shock(symbol, pct).catch(() => {});
    setSent(`${percent(pct, 0)} shock sent to ${symbol}`);
    setTimeout(() => setSent(""), 4000);
  };

  return (
    <section className="panel flex min-h-0 flex-1 flex-col">
      <header className="flex items-center border-b border-line px-3 py-2.5">
        <span className="label">Pulse</span>
        {simulated && (
          <span className="ml-auto flex items-center gap-1.5" title="Demo control: push the simulated market and watch detection and the AI brief happen">
            <Zap size={12} className="text-warn" />
            <span className="text-[11px] text-muted">Shock {symbol}</span>
            <button onClick={() => void shock(-3)} className="num rounded border border-line px-1.5 text-[11px] text-down hover:border-down">
              −3%
            </button>
            <button onClick={() => void shock(3)} className="num rounded border border-line px-1.5 text-[11px] text-up hover:border-up">
              +3%
            </button>
          </span>
        )}
      </header>
      {sent && <div className="slide-in border-b border-line bg-warn/10 px-3 py-1 text-[11px] text-warn">{sent}. Watch the chart.</div>}
      {pulse.length ? (
        <ul className="min-h-0 flex-1 divide-y divide-line/60 overflow-y-auto">
          {pulse.map((item, index) => (
            <Item key={`${item.type}-${item.ts}-${index}`} item={item} />
          ))}
        </ul>
      ) : (
        <div className="grid flex-1 place-items-center px-6 text-center text-muted">
          Quiet so far. Unusual moves, headlines and AI briefs appear here as they happen.
        </div>
      )}
    </section>
  );
}
