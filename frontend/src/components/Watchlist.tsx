import { memo, useEffect, useRef, useState } from "react";
import { direction, percent, price, toneClass } from "../lib/format";
import type { Instrument } from "../lib/types";
import { useApp } from "../store";

/** One row subscribes to one symbol's quote, so a tick re-renders a single row, not the whole list. */
const Row = memo(function Row({ instrument, active }: { instrument: Instrument; active: boolean }) {
  const quote = useApp((s) => s.quotes[instrument.symbol]);
  const previous = useRef(quote?.last);
  const [flash, setFlash] = useState("");

  useEffect(() => {
    const last = quote?.last;
    if (last != null && previous.current != null && last !== previous.current) {
      setFlash(last > previous.current ? "flash-up" : "flash-down");
      const timer = setTimeout(() => setFlash(""), 700);
      previous.current = last;
      return () => clearTimeout(timer);
    }
    previous.current = last;
  }, [quote?.last]);

  const tone = toneClass[direction(quote?.change_pct)];
  return (
    <button
      onClick={() => useApp.getState().select(instrument.symbol)}
      className={`grid w-full grid-cols-[1fr_auto] items-center gap-x-3 border-l-2 px-3 py-2 text-left transition hover:bg-raised ${
        active ? "border-accent bg-raised" : "border-transparent"
      }`}
    >
      <div className="min-w-0">
        <div className="font-semibold">{instrument.symbol}</div>
        <div className="truncate text-[11px] text-muted">{instrument.sector}</div>
      </div>
      <div className="text-right">
        <div className={`num rounded px-1 font-medium ${flash}`}>{price(quote?.last)}</div>
        <div className={`num px-1 text-[11px] ${tone}`}>{percent(quote?.change_pct)}</div>
      </div>
    </button>
  );
});

export function Watchlist() {
  const instruments = useApp((s) => s.instruments);
  const selected = useApp((s) => s.selected);
  return (
    <section className="panel flex min-h-0 flex-col">
      <header className="label border-b border-line px-3 py-2.5">Watchlist</header>
      <div className="min-h-0 flex-1 divide-y divide-line/60 overflow-y-auto">
        {instruments.map((instrument) => (
          <Row key={instrument.symbol} instrument={instrument} active={instrument.symbol === selected} />
        ))}
      </div>
    </section>
  );
}
