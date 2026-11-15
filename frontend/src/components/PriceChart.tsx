import {
  CandlestickSeries,
  ColorType,
  createChart,
  CrosshairMode,
  HistogramSeries,
  type IChartApi,
  type ISeriesApi,
  type UTCTimestamp,
} from "lightweight-charts";
import { useEffect, useRef, useState } from "react";
import { api } from "../lib/api";
import { compact, direction, percent, price, toneClass } from "../lib/format";
import type { Candle, Indicators } from "../lib/types";
import { useApp } from "../store";

const INTERVALS = { "1m": 60, "5m": 300, "15m": 900, "1h": 3600 } as const;
type Interval = keyof typeof INTERVALS;

const UP = "#2ee59d";
const DOWN = "#ff5d7c";
// The chart library draws in UTC. Shifting timestamps by the local offset makes the axis read in local time.
const TZ_SHIFT = -new Date().getTimezoneOffset() * 60;
const at = (epoch: number) => (epoch + TZ_SHIFT) as UTCTimestamp;

export function PriceChart() {
  const symbol = useApp((s) => s.selected);
  const instrument = useApp((s) => s.instruments.find((i) => i.symbol === symbol));
  const quote = useApp((s) => s.quotes[symbol]);
  const [interval, setInterval_] = useState<Interval>("1m");
  const [indicatorState, setIndicatorState] = useState<{ key: string; data: Indicators } | null>(null);
  const indicatorKey = `${symbol}:${interval}`;
  // Indicators loaded for another symbol or interval are ignored rather than shown stale.
  const indicators = indicatorState?.key === indicatorKey ? indicatorState.data : null;

  const container = useRef<HTMLDivElement>(null);
  const chart = useRef<IChartApi | null>(null);
  const candles = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const volume = useRef<ISeriesApi<"Histogram"> | null>(null);
  const last = useRef<Candle | null>(null); // the right-most candle, kept so live ticks can extend it

  // Create the chart once.
  useEffect(() => {
    if (!container.current) return;
    const instance = createChart(container.current, {
      autoSize: true,
      layout: { background: { type: ColorType.Solid, color: "transparent" }, textColor: "#8793a6", fontFamily: "JetBrains Mono, monospace", fontSize: 11 },
      grid: { vertLines: { color: "#151c29" }, horzLines: { color: "#151c29" } },
      crosshair: { mode: CrosshairMode.Normal },
      rightPriceScale: { borderColor: "#1e2636" },
      timeScale: { borderColor: "#1e2636", timeVisible: true, secondsVisible: false, rightOffset: 4 },
    });
    candles.current = instance.addSeries(CandlestickSeries, {
      upColor: UP, downColor: DOWN, wickUpColor: UP, wickDownColor: DOWN, borderVisible: false,
    });
    volume.current = instance.addSeries(HistogramSeries, { priceFormat: { type: "volume" }, priceScaleId: "volume" });
    // Volume sits in the bottom fifth of the pane, under the candles.
    instance.priceScale("volume").applyOptions({ scaleMargins: { top: 0.82, bottom: 0 } });
    chart.current = instance;
    return () => {
      instance.remove();
      chart.current = null;
    };
  }, []);

  // Load history whenever the symbol or interval changes.
  useEffect(() => {
    if (!symbol) return;
    let stale = false;
    last.current = null;
    api
      .candles(symbol, interval, 400)
      .then((rows) => {
        if (stale || !candles.current || !volume.current) return;
        candles.current.setData(rows.map((c) => ({ time: at(c.t), open: c.o, high: c.h, low: c.l, close: c.c })));
        volume.current.setData(rows.map((c) => ({ time: at(c.t), value: c.v, color: c.c >= c.o ? `${UP}55` : `${DOWN}55` })));
        last.current = rows.at(-1) ?? null;
        chart.current?.timeScale().scrollToRealTime();
      })
      .catch(() => {});
    return () => {
      stale = true;
    };
  }, [symbol, interval]);

  // Extend the last candle (or start a new one) from the live quote.
  useEffect(() => {
    const bar = quote?.bar;
    if (!bar || !candles.current) return;
    const width = INTERVALS[interval];
    const bucket = Math.floor(bar.t / width) * width;
    const current = last.current;
    if (current && bucket < current.t) return; // an out-of-date frame; the chart only moves forward
    const next: Candle =
      current && bucket === current.t
        ? { ...current, h: Math.max(current.h, bar.h), l: Math.min(current.l, bar.l), c: bar.c }
        : { t: bucket, o: bar.o, h: bar.h, l: bar.l, c: bar.c, v: bar.v };
    last.current = next;
    candles.current.update({ time: at(next.t), open: next.o, high: next.h, low: next.l, close: next.c });
  }, [quote?.bar, interval]);

  // Indicator chips refresh on a slow timer; they are computed server-side from stored candles.
  useEffect(() => {
    if (!symbol) return;
    let stale = false;
    const key = `${symbol}:${interval}`;
    const load = () =>
      api
        .indicators(symbol, interval)
        .then((data) => !stale && setIndicatorState({ key, data }))
        .catch(() => {});
    void load();
    const timer = setInterval(load, 10_000);
    return () => {
      stale = true;
      clearInterval(timer);
    };
  }, [symbol, interval]);

  const tone = toneClass[direction(quote?.change_pct)];
  return (
    <section className="panel flex min-h-0 flex-1 flex-col">
      <header className="flex flex-wrap items-end gap-x-5 gap-y-2 border-b border-line px-4 py-3">
        <div>
          <div className="flex items-baseline gap-2">
            <h2 className="text-lg font-semibold tracking-tight">{symbol}</h2>
            <span className="text-muted">{instrument?.name}</span>
          </div>
          <div className="flex items-baseline gap-2.5">
            <span className="num text-2xl font-semibold">{price(quote?.last)}</span>
            <span className={`num font-medium ${tone}`}>
              {quote ? `${quote.change >= 0 ? "+" : "−"}${price(Math.abs(quote.change))}` : ""} ({percent(quote?.change_pct)})
            </span>
          </div>
        </div>
        <dl className="num flex gap-4 text-[11px] text-muted">
          <Stat label="Open" value={price(quote?.open)} />
          <Stat label="High" value={price(quote?.high)} />
          <Stat label="Low" value={price(quote?.low)} />
          <Stat label="Volume" value={compact(quote?.volume)} />
        </dl>
        <div className="ml-auto flex flex-wrap items-center gap-2">
          {indicators && indicators.bars > 0 && (
            <>
              <Chip label="Trend" value={indicators.trend ?? "–"} tone={indicators.trend === "up" ? "up" : indicators.trend === "down" ? "down" : "flat"} />
              {indicators.rsi14 != null && <Chip label="RSI" value={indicators.rsi14.toFixed(0)} tone={indicators.rsi14 >= 70 ? "down" : indicators.rsi14 <= 30 ? "up" : "flat"} />}
              {indicators.vs_vwap_pct != null && <Chip label="vs VWAP" value={percent(indicators.vs_vwap_pct)} tone={direction(indicators.vs_vwap_pct)} />}
            </>
          )}
          <div className="flex rounded-md border border-line p-0.5">
            {(Object.keys(INTERVALS) as Interval[]).map((option) => (
              <button
                key={option}
                onClick={() => setInterval_(option)}
                className={`num rounded px-2 py-0.5 text-xs ${interval === option ? "bg-raised text-text" : "text-muted hover:text-text"}`}
              >
                {option}
              </button>
            ))}
          </div>
        </div>
      </header>
      <div ref={container} className="min-h-0 flex-1" />
    </section>
  );
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <dt className="font-sans text-[10px] tracking-wide text-faint uppercase">{label}</dt>
      <dd className="text-text">{value}</dd>
    </div>
  );
}

function Chip({ label, value, tone }: { label: string; value: string; tone: "up" | "down" | "flat" }) {
  return (
    <span className="rounded-md border border-line px-2 py-1 text-[11px]">
      <span className="text-faint">{label} </span>
      <span className={`num font-medium ${toneClass[tone]}`}>{value}</span>
    </span>
  );
}
