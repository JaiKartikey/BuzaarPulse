import { Bot, ChevronDown, ChevronRight, CircleCheck, CircleX, LoaderCircle, Newspaper, Send, ShieldAlert, TrendingUp, Users, Wrench } from "lucide-react";
import { useEffect, useState, type FormEvent } from "react";
import { api, ApiError } from "../lib/api";
import { ago, money } from "../lib/format";
import type { AgentRun, AgentStep, Proposal, Signal } from "../lib/types";
import { toolActivity, useRun, type ToolActivity } from "../lib/useRun";
import { useApp } from "../store";

type Mode = "copilot" | "war_room";

const SUGGESTIONS: Record<Mode, string[]> = {
  copilot: ["How is the market doing right now?", "How is TCS doing, and is there any news?", "Buy 10 INFY for me", "Rebalance my portfolio", "Backtest RSI on SBIN"],
  war_room: ["Is this a good moment to buy?", "What are the risks right now?"],
};

const SIGNAL_STYLE: Record<string, string> = {
  bullish: "bg-up/15 text-up border-up/40",
  bearish: "bg-down/15 text-down border-down/40",
  neutral: "bg-raised text-muted border-line-strong",
};

function SignalBadge({ signal, confidence, large }: { signal: Signal | undefined; confidence?: string | null; large?: boolean }) {
  if (!signal) return null;
  return (
    <span className={`inline-flex items-center gap-1.5 rounded-md border font-semibold capitalize ${SIGNAL_STYLE[signal]} ${large ? "px-3 py-1 text-sm" : "px-1.5 py-0.5 text-[11px]"}`}>
      {signal}
      {confidence && <span className="font-normal opacity-75">· {confidence} confidence</span>}
    </span>
  );
}

/** Strips the machine-readable verdict line; it is shown as a badge instead. */
const prose = (text: string) => text.replace(/\n?SIGNAL:.*$/is, "").trim();
const signalOf = (text: string): Signal => (/SIGNAL:\s*(bullish|bearish|neutral)/i.exec(text)?.[1]?.toLowerCase() as Signal) ?? null;

function ToolCard({ call }: { call: ToolActivity }) {
  const [open, setOpen] = useState(false);
  const args = Object.entries(call.input)
    .filter(([, v]) => v !== null && !(Array.isArray(v) && typeof v[0] === "object"))
    .map(([k, v]) => `${k}: ${Array.isArray(v) ? v.join(", ") : String(v)}`)
    .join(" · ");
  return (
    <div className="slide-in rounded-md border border-line bg-ink/60">
      <button onClick={() => setOpen(!open)} className="flex w-full items-center gap-2 px-2.5 py-1.5 text-left" aria-expanded={open}>
        {call.ok === null ? (
          <LoaderCircle size={13} className="shrink-0 animate-spin text-ai" />
        ) : call.ok ? (
          <CircleCheck size={13} className="shrink-0 text-up" />
        ) : (
          <CircleX size={13} className="shrink-0 text-down" />
        )}
        <Wrench size={12} className="shrink-0 text-faint" />
        <span className="num text-xs font-medium">{call.name}</span>
        <span className="truncate text-[11px] text-muted">{args}</span>
        {open ? <ChevronDown size={13} className="ml-auto shrink-0 text-faint" /> : <ChevronRight size={13} className="ml-auto shrink-0 text-faint" />}
      </button>
      {open && (
        <pre className="num max-h-56 overflow-auto border-t border-line px-2.5 py-2 text-[11px] leading-relaxed whitespace-pre-wrap text-muted">
          {call.result === null ? "Running…" : typeof call.result === "string" ? call.result : JSON.stringify(call.result, null, 2)}
        </pre>
      )}
    </div>
  );
}

function Paragraphs({ text }: { text: string }) {
  return (
    <div className="space-y-2 leading-relaxed">
      {text.split(/\n{2,}/).map((paragraph, i) => (
        <p key={i}>{paragraph}</p>
      ))}
    </div>
  );
}

const LANES = [
  { agent: "technical", title: "Technical analyst", icon: TrendingUp },
  { agent: "news", title: "News analyst", icon: Newspaper },
  { agent: "risk", title: "Risk officer", icon: ShieldAlert },
] as const;

function WarRoom({ steps, streaming }: { steps: AgentStep[]; streaming: boolean }) {
  const verdict = steps.find((s) => s.agent === "lead" && s.type === "final");
  const verdictText = verdict ? String(verdict.payload.text) : "";
  const reportsIn = steps.filter((s) => s.type === "report").length;
  return (
    <div className="space-y-3">
      <div className="grid gap-3 lg:grid-cols-3">
        {LANES.map(({ agent, title, icon: Icon }) => {
          const report = steps.find((s) => s.agent === agent && (s.type === "report" || s.type === "error"));
          const text = report ? String(report.payload.text) : "";
          return (
            <div key={agent} className="panel flex flex-col p-3">
              <div className="mb-2 flex items-center gap-1.5">
                <Icon size={14} className="text-ai" />
                <span className="font-semibold">{title}</span>
                <span className="ml-auto">{report ? <SignalBadge signal={signalOf(text)} /> : streaming && <LoaderCircle size={13} className="animate-spin text-ai" />}</span>
              </div>
              <div className="space-y-1.5">
                {toolActivity(steps, agent).map((call) => (
                  <ToolCard key={call.id} call={call} />
                ))}
              </div>
              {report && <p className={`slide-in mt-2.5 text-xs leading-relaxed ${report.type === "error" ? "text-down" : ""}`}>{prose(text)}</p>}
            </div>
          );
        })}
      </div>
      <div className={`panel p-4 ${verdict ? "border-accent/50" : ""}`}>
        <div className="mb-2 flex items-center gap-2">
          <Users size={15} className="text-accent" />
          <span className="font-semibold">Desk lead</span>
          {verdict ? (
            <span className="ml-auto">
              <SignalBadge signal={signalOf(verdictText)} confidence={/CONFIDENCE:\s*(\w+)/i.exec(verdictText)?.[1]?.toLowerCase()} large />
            </span>
          ) : (
            <span className="ml-auto text-xs text-muted">{streaming ? `Waiting for the analysts (${reportsIn} of 3 reports in)` : ""}</span>
          )}
        </div>
        {verdict ? <Paragraphs text={prose(verdictText)} /> : <p className="text-muted">The lead reads all three reports, then writes the desk's view.</p>}
      </div>
    </div>
  );
}

function Copilot({ steps, streaming }: { steps: AgentStep[]; streaming: boolean }) {
  const calls = toolActivity(steps);
  const final = steps.find((s) => s.type === "final");
  return (
    <div className="space-y-2.5">
      {calls.length > 0 && (
        <div className="space-y-1.5">
          <div className="label">What the agent looked up</div>
          {calls.map((call) => (
            <ToolCard key={call.id} call={call} />
          ))}
        </div>
      )}
      {final ? (
        <div className="panel slide-in border-ai/40 p-4">
          <div className="mb-2 flex items-center gap-1.5 text-ai">
            <Bot size={15} />
            <span className="text-xs font-semibold tracking-wide uppercase">Copilot</span>
          </div>
          <Paragraphs text={String(final.payload.text)} />
        </div>
      ) : (
        streaming && (
          <div className="flex items-center gap-2 text-muted">
            <LoaderCircle size={14} className="animate-spin text-ai" />
            {calls.length ? "Reading the results…" : "Thinking…"}
          </div>
        )
      )}
    </div>
  );
}

function ProposalCard({ proposal }: { proposal: Proposal }) {
  const [busy, setBusy] = useState("");
  const [error, setError] = useState("");
  const pending = proposal.status === "pending";

  const decide = async (decision: "approve" | "reject") => {
    setBusy(decision);
    setError("");
    try {
      await api.decideProposal(proposal.id, decision);
      await Promise.all([useApp.getState().refreshProposals(), useApp.getState().refreshAccount()]);
    } catch (e) {
      setError(e instanceof ApiError ? e.message : "Could not record the decision.");
      await useApp.getState().refreshProposals();
    } finally {
      setBusy("");
    }
  };

  const tone = { pending: "text-warn", executed: "text-up", approved: "text-up", rejected: "text-muted", expired: "text-muted", failed: "text-down" }[proposal.status];
  return (
    <div className={`rounded-lg border p-3 ${pending ? "border-warn/50 bg-warn/5" : "border-line"}`}>
      <div className="flex items-center gap-2">
        <span className={`text-[11px] font-semibold tracking-wide uppercase ${tone}`}>{pending ? "Needs your approval" : proposal.status}</span>
        <span className="num ml-auto text-[10px] text-faint">{ago(proposal.created_at)}</span>
      </div>
      <ul className="mt-2 space-y-1">
        {proposal.orders.map((order, i) => (
          <li key={i} className="num flex items-center gap-2 text-xs">
            <span className={`font-sans font-semibold uppercase ${order.side === "buy" ? "text-up" : "text-down"}`}>{order.side}</span>
            <span>{order.quantity}</span>
            <span className="font-sans font-semibold">{order.symbol}</span>
            <span className="font-sans text-muted">{order.order_type}</span>
            <span className="ml-auto text-muted">≈ {money(order.estimated_value)}</span>
          </li>
        ))}
      </ul>
      <p className="mt-2 text-xs leading-relaxed text-muted">{proposal.rationale}</p>
      {pending && (
        <>
          <p className="mt-2 flex items-center gap-1.5 text-[11px] text-up">
            <CircleCheck size={12} /> Passed every risk check. Checked again when placed.
          </p>
          <div className="mt-2.5 grid grid-cols-2 gap-2">
            <button onClick={() => void decide("approve")} disabled={Boolean(busy)} className="rounded-md bg-up py-1.5 font-semibold text-ink hover:brightness-110 disabled:opacity-50">
              {busy === "approve" ? "Placing…" : "Approve"}
            </button>
            <button onClick={() => void decide("reject")} disabled={Boolean(busy)} className="rounded-md border border-line-strong py-1.5 font-medium hover:border-down hover:text-down disabled:opacity-50">
              Reject
            </button>
          </div>
        </>
      )}
      {error && <p className="mt-2 text-xs text-down">{error}</p>}
    </div>
  );
}

export function AiDesk() {
  const instruments = useApp((s) => s.instruments);
  const proposals = useApp((s) => s.proposals);
  const selected = useApp((s) => s.selected);
  const [mode, setMode] = useState<Mode>("copilot");
  const [symbol, setSymbol] = useState(selected);
  const [message, setMessage] = useState("");
  const [runs, setRuns] = useState<AgentRun[]>([]);
  const [activeId, setActiveId] = useState<string | null>(null);
  const [error, setError] = useState("");

  const loadRuns = () => api.runs().then(setRuns).catch(() => {});
  useEffect(() => {
    void loadRuns();
  }, []);

  const live = useRun(activeId, () => {
    void loadRuns();
    void useApp.getState().refreshProposals();
  });
  const active = runs.find((r) => r.id === activeId) ?? live.run;
  const done = live.steps.find((s) => s.type === "done");

  const start = async (text: string) => {
    if (!text.trim()) return;
    setError("");
    try {
      const run = await api.startRun(mode, text.trim(), mode === "war_room" ? symbol : undefined);
      setRuns((current) => [run, ...current]);
      setActiveId(run.id);
      setMessage("");
    } catch (e) {
      setError(e instanceof ApiError ? e.message : "Could not start the run.");
    }
  };

  const submit = (event: FormEvent) => {
    event.preventDefault();
    void start(message);
  };

  return (
    <div className="grid h-full min-h-0 grid-cols-[250px_minmax(0,1fr)_320px] gap-3">
      <aside className="panel flex min-h-0 flex-col">
        <header className="label border-b border-line px-3 py-2.5">History</header>
        <ul className="min-h-0 flex-1 divide-y divide-line/60 overflow-y-auto">
          {runs.map((run) => (
            <li key={run.id}>
              <button onClick={() => setActiveId(run.id)} className={`w-full border-l-2 px-3 py-2 text-left hover:bg-raised ${run.id === activeId ? "border-accent bg-raised" : "border-transparent"}`}>
                <div className="flex items-center gap-1.5 text-[11px] text-muted">
                  {run.kind === "war_room" ? <Users size={11} /> : <Bot size={11} />}
                  {run.kind === "war_room" ? `War room · ${run.symbol}` : "Copilot"}
                  <span className="num ml-auto text-faint">{ago(run.created_at)}</span>
                </div>
                <div className="mt-0.5 truncate text-xs">{run.input.message}</div>
              </button>
            </li>
          ))}
          {!runs.length && <li className="px-4 py-6 text-center text-muted">Your questions will be listed here.</li>}
        </ul>
      </aside>

      <section className="flex min-h-0 min-w-0 flex-col gap-3">
        <div className="min-h-0 flex-1 overflow-y-auto pr-1">
          {activeId && active ? (
            <div className="space-y-3">
              <div className="ml-auto w-fit max-w-[80%] rounded-lg rounded-br-sm bg-accent/20 px-3.5 py-2">{active.input.message}</div>
              {active.kind === "war_room" ? <WarRoom steps={live.steps} streaming={live.streaming} /> : <Copilot steps={live.steps} streaming={live.streaming} />}
              {done?.payload.status === "failed" && <p className="rounded-md bg-down/10 px-3 py-2 text-down">The run failed: {String(done.payload.error)}</p>}
              {live.error && <p className="text-down">{live.error}</p>}
              {done && (
                <p className="num text-[11px] text-faint">
                  {String(done.payload.provider)} · {String(done.payload.model)} · {Number(done.payload.llm_calls)} model calls · {Number(done.payload.tool_calls)} tool calls ·{" "}
                  {Number(done.payload.input_tokens) + Number(done.payload.output_tokens)} tokens · ${Number(done.payload.cost_usd).toFixed(4)}
                </p>
              )}
            </div>
          ) : (
            <div className="grid h-full place-items-center">
              <div className="max-w-md text-center">
                <Bot size={30} className="mx-auto text-ai" />
                <h2 className="mt-3 text-lg font-semibold">Ask the desk</h2>
                <p className="mt-1.5 text-muted">
                  The copilot looks things up with tools and shows you every call it makes. The war room puts three analysts on one stock at the same time, then a lead
                  weighs their reports. Agents can draft trades; only you can approve them.
                </p>
              </div>
            </div>
          )}
        </div>

        <form onSubmit={submit} className="panel shrink-0 p-3">
          <div className="mb-2 flex flex-wrap items-center gap-2">
            <div className="flex rounded-md border border-line p-0.5">
              {(
                [
                  ["copilot", "Copilot", Bot],
                  ["war_room", "War room", Users],
                ] as const
              ).map(([key, label, Icon]) => (
                <button type="button" key={key} onClick={() => setMode(key)} className={`flex items-center gap-1.5 rounded px-2.5 py-1 text-xs font-medium ${mode === key ? "bg-raised text-text" : "text-muted hover:text-text"}`}>
                  <Icon size={12} />
                  {label}
                </button>
              ))}
            </div>
            {mode === "war_room" && (
              <select value={symbol} onChange={(e) => setSymbol(e.target.value)} className="rounded-md border border-line bg-ink px-2 py-1 text-xs" aria-label="Stock to review">
                {instruments.map((i) => (
                  <option key={i.symbol} value={i.symbol}>
                    {i.symbol} · {i.name}
                  </option>
                ))}
              </select>
            )}
            <div className="flex flex-wrap gap-1.5">
              {SUGGESTIONS[mode].map((suggestion) => (
                <button type="button" key={suggestion} onClick={() => void start(suggestion)} className="rounded-full border border-line px-2.5 py-0.5 text-[11px] text-muted hover:border-accent hover:text-text">
                  {suggestion}
                </button>
              ))}
            </div>
          </div>
          <div className="flex gap-2">
            <input
              value={message}
              onChange={(e) => setMessage(e.target.value)}
              maxLength={2000}
              placeholder={mode === "copilot" ? "Ask about a stock, your portfolio, or ask it to draft a trade…" : `What should the desk find out about ${symbol}?`}
              className="min-w-0 flex-1 rounded-md border border-line bg-ink px-3 py-2 placeholder:text-faint"
            />
            <button disabled={!message.trim() || live.streaming} className="flex items-center gap-1.5 rounded-md bg-accent px-4 font-semibold text-ink hover:brightness-110 disabled:opacity-40">
              <Send size={14} />
              Ask
            </button>
          </div>
          {error && <p className="mt-2 text-xs text-down">{error}</p>}
        </form>
      </section>

      <aside className="panel flex min-h-0 flex-col">
        <header className="label border-b border-line px-3 py-2.5">Trade proposals</header>
        <div className="min-h-0 flex-1 space-y-2.5 overflow-y-auto p-2.5">
          {proposals.map((proposal) => (
            <ProposalCard key={proposal.id} proposal={proposal} />
          ))}
          {!proposals.length && <p className="px-3 py-6 text-center text-muted">When an agent drafts a trade, it waits here for you to approve or reject it.</p>}
        </div>
      </aside>
    </div>
  );
}
