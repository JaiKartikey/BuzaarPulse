import { CircleCheck, CircleX } from "lucide-react";
import { useEffect, useState } from "react";
import { api } from "../lib/api";
import { compact } from "../lib/format";
import type { SystemStats } from "../lib/types";

const HISTORY = 90;

function Sparkline({ values, color }: { values: number[]; color: string }) {
  if (values.length < 2) return <div className="h-10" />;
  const max = Math.max(...values, 1);
  const points = values.map((v, i) => `${(i / (HISTORY - 1)) * 100},${36 - (v / max) * 32}`).join(" ");
  return (
    <svg viewBox="0 0 100 38" preserveAspectRatio="none" className="h-10 w-full" aria-hidden>
      <polyline points={points} fill="none" stroke={color} strokeWidth="1.4" vectorEffect="non-scaling-stroke" strokeLinejoin="round" />
    </svg>
  );
}

function Tile({ label, value, unit, hint, children }: { label: string; value: string; unit?: string; hint: string; children?: React.ReactNode }) {
  return (
    <div className="panel p-3.5">
      <div className="label">{label}</div>
      <div className="mt-1 flex items-baseline gap-1.5">
        <span className="num text-2xl font-semibold">{value}</span>
        {unit && <span className="text-muted">{unit}</span>}
      </div>
      {children}
      <p className="mt-1 text-[11px] leading-snug text-muted">{hint}</p>
    </div>
  );
}

/** One box in the architecture diagram. */
function Node({ x, y, w = 132, title, sub, tone = "#1e2636" }: { x: number; y: number; w?: number; title: string; sub: string; tone?: string }) {
  return (
    <g>
      <rect x={x} y={y} width={w} height={46} rx={8} fill="#0e131c" stroke={tone} strokeWidth={1.2} />
      <text x={x + w / 2} y={y + 19} textAnchor="middle" fill="#e7ecf3" fontSize="11.5" fontWeight="600">
        {title}
      </text>
      <text x={x + w / 2} y={y + 34} textAnchor="middle" fill="#8793a6" fontSize="9.5">
        {sub}
      </text>
    </g>
  );
}

function Edge({ d, label, lx, ly, dashed }: { d: string; label?: string; lx?: number; ly?: number; dashed?: boolean }) {
  return (
    <g>
      <path d={d} fill="none" stroke="#2b3549" strokeWidth={1.3} strokeDasharray={dashed ? "4 3" : undefined} markerEnd="url(#arrow)" />
      {label && (
        <text x={lx} y={ly} textAnchor="middle" fill="#45c8ff" fontSize="9.5" fontFamily="JetBrains Mono, monospace">
          {label}
        </text>
      )}
    </g>
  );
}

function Architecture({ stats }: { stats: SystemStats }) {
  const tps = stats.pipeline.ticks_per_second ?? 0;
  const lag = stats.stream_lag["md.ticks.0"] ?? 0;
  return (
    <svg viewBox="0 0 860 300" className="w-full" role="img" aria-label="How data flows through the system">
      <defs>
        <marker id="arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
          <path d="M0 0 8 4 0 8z" fill="#2b3549" />
        </marker>
      </defs>

      <Node x={10} y={34} title="Market feed" sub={stats.market_provider} />
      <Node x={182} y={34} title="Ingestor" sub="normalise, publish" />
      <Node x={354} y={34} w={150} title="Redis Streams" sub="durable tick log" tone="#8b7bff" />
      <Node x={548} y={34} title="Processor" sub="candles · alerts" />
      <Node x={720} y={34} title="Browser" sub={`${stats.websocket_clients} WebSocket${stats.websocket_clients === 1 ? "" : "s"}`} tone="#2ee59d" />
      <Edge d="M142 57 H180" />
      <Edge d="M314 57 H352" label={`${tps.toFixed(0)}/s`} lx={333} ly={48} />
      <Edge d="M504 57 H546" label={`lag ${lag}`} lx={525} ly={48} />
      <Edge d="M680 57 H718" label="push" lx={699} ly={48} />

      <Node x={354} y={140} w={150} title="Executor" sub={`${stats.open_orders} open orders`} />
      <Node x={548} y={140} title="Postgres" sub="candles · ledger" tone="#8b7bff" />
      <Node x={720} y={140} title="API" sub="REST · SSE · auth" />
      <Edge d="M429 80 V138" label="ticks" lx={449} ly={113} />
      <Edge d="M614 80 V138" />
      <Edge d="M504 163 H546" label="fills" lx={525} ly={154} />
      <Edge d="M786 138 V82" />
      <Edge d="M718 163 H682" />

      <Node x={354} y={236} w={150} title="Agent workers" sub={`${stats.llm.provider} · ${stats.llm.runs_24h} runs today`} tone="#45c8ff" />
      <Node x={548} y={236} title="Tools" sub="quotes · news · risk" />
      <Node x={720} y={236} title="Approval gate" sub="a human decides" tone="#ffb648" />
      <Edge d="M560 80 C 548 104, 526 108, 526 130 V 196 C 526 218, 478 212, 470 234" label="anomalies" lx={468} ly={212} dashed />
      <Edge d="M504 259 H546" />
      <Edge d="M614 234 V188" />
      <Edge d="M680 259 H718" label="plan" lx={699} ly={250} />
      <Edge d="M786 234 V188" label="orders" lx={808} ly={214} />
    </svg>
  );
}

export function System() {
  const [view, setView] = useState<{ stats: SystemStats; throughput: number[]; latency: number[] } | null>(null);
  const [failed, setFailed] = useState(false);

  useEffect(() => {
    let stopped = false;
    const load = () =>
      api
        .stats()
        .then((stats) => {
          if (stopped) return;
          setView((current) => ({
            stats,
            throughput: [...(current?.throughput ?? []), stats.pipeline.ticks_per_second ?? 0].slice(-HISTORY),
            latency: [...(current?.latency ?? []), stats.pipeline.latency_p95_ms ?? 0].slice(-HISTORY),
          }));
          setFailed(false);
        })
        .catch(() => !stopped && setFailed(true));
    void load();
    const timer = setInterval(load, 2000);
    return () => {
      stopped = true;
      clearInterval(timer);
    };
  }, []);

  const stats = view?.stats;
  if (!stats) return <div className="grid h-full place-items-center text-muted">{failed ? "Could not load system stats." : "Loading…"}</div>;

  const { pipeline, ledger, llm } = stats;
  const lag = Object.entries(stats.stream_lag)
    .filter(([name]) => name.startsWith("md.ticks"))
    .reduce((sum, [, value]) => sum + (value ?? 0), 0);

  return (
    <div className="h-full space-y-3 overflow-y-auto pr-1">
      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
        <Tile label="Throughput" value={(pipeline.ticks_per_second ?? 0).toFixed(1)} unit="ticks / s" hint={`${compact(pipeline.ticks_total)} ticks processed since start, across ${pipeline.symbols ?? 0} symbols.`}>
          <Sparkline values={view.throughput} color="#2ee59d" />
        </Tile>
        <Tile label="Pipeline latency (p95)" value={(pipeline.latency_p95_ms ?? 0).toFixed(1)} unit="ms" hint={`From a trade's timestamp to its quote being fanned out. Median ${(pipeline.latency_p50_ms ?? 0).toFixed(1)} ms.`}>
          <Sparkline values={view.latency} color="#45c8ff" />
        </Tile>
        <Tile label="Stream backlog" value={String(lag)} unit="ticks waiting" hint="Ticks published but not yet processed. Staying near zero means the processors are keeping up." />
        <Tile label="Live connections" value={String(stats.websocket_clients)} unit="sockets" hint={`${stats.open_orders} open orders are waiting for their price.`} />
      </div>

      <div className="grid gap-3 xl:grid-cols-[minmax(0,1fr)_340px]">
        <section className="panel p-4">
          <div className="label mb-2">How a tick becomes a decision</div>
          <Architecture stats={stats} />
        </section>

        <div className="space-y-3">
          <section className="panel p-3.5">
            <div className="label">Ledger</div>
            <div className={`mt-1.5 flex items-center gap-2 text-base font-semibold ${ledger.ok ? "text-up" : "text-down"}`}>
              {ledger.ok ? <CircleCheck size={18} /> : <CircleX size={18} />}
              {ledger.ok ? "Books balance" : "Books do not balance"}
            </div>
            <p className="mt-1.5 text-[11px] leading-snug text-muted">
              <span className="num text-text">{ledger.transactions}</span> transactions, <span className="num text-text">{ledger.entries}</span> entries. For every asset, all entries sum to
              exactly zero, and every cached balance equals the sum of its entries. Re-checked on each refresh.
            </p>
          </section>

          <section className="panel p-3.5">
            <div className="label">AI usage · last 24 hours</div>
            <dl className="num mt-2 grid grid-cols-2 gap-y-2 text-sm">
              <dt className="font-sans text-muted">Model</dt>
              <dd className="truncate text-right" title={llm.model}>
                {llm.model}
              </dd>
              <dt className="font-sans text-muted">Runs</dt>
              <dd className="text-right">{llm.runs_24h}</dd>
              <dt className="font-sans text-muted">Tokens</dt>
              <dd className="text-right">{compact(llm.input_tokens_24h + llm.output_tokens_24h)}</dd>
              <dt className="font-sans text-muted">Estimated cost</dt>
              <dd className="text-right">${llm.cost_usd_24h.toFixed(4)}</dd>
            </dl>
            {llm.provider === "mock" && <p className="mt-2 text-[11px] leading-snug text-warn">Running on the offline scripted model. Set an API key to use a real one.</p>}
          </section>
        </div>
      </div>
    </div>
  );
}
