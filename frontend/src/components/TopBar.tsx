import { Bot, ChartCandlestick, Cpu, LogOut, Zap } from "lucide-react";
import { useApp, type View } from "../store";

const TABS: Array<{ view: View; label: string; icon: typeof Bot }> = [
  { view: "terminal", label: "Terminal", icon: ChartCandlestick },
  { view: "desk", label: "AI Desk", icon: Bot },
  { view: "system", label: "System", icon: Cpu },
];

const STATE = {
  live: { text: "Live", dot: "bg-up live-dot", tone: "text-up" },
  connecting: { text: "Connecting", dot: "bg-warn live-dot", tone: "text-warn" },
  offline: { text: "Reconnecting", dot: "bg-down", tone: "text-down" },
} as const;

export function TopBar() {
  const view = useApp((s) => s.view);
  const connection = useApp((s) => s.connection);
  const user = useApp((s) => s.user);
  const pending = useApp((s) => s.proposals.filter((p) => p.status === "pending").length);
  const state = STATE[connection];

  return (
    <header className="flex h-12 shrink-0 items-center gap-5 border-b border-line bg-panel px-4">
      <div className="flex items-center gap-2">
        <span className="grid h-7 w-7 place-items-center rounded-md bg-up/15 text-up">
          <Zap size={15} />
        </span>
        <span className="font-semibold tracking-tight">BazaarPulse</span>
      </div>

      <nav className="flex gap-1">
        {TABS.map(({ view: tab, label, icon: Icon }) => (
          <button
            key={tab}
            onClick={() => useApp.getState().setView(tab)}
            aria-current={view === tab ? "page" : undefined}
            className={`relative flex items-center gap-1.5 rounded-md px-3 py-1.5 font-medium transition ${
              view === tab ? "bg-raised text-text" : "text-muted hover:text-text"
            }`}
          >
            <Icon size={14} />
            {label}
            {tab === "desk" && pending > 0 && (
              <span className="num ml-0.5 rounded-full bg-warn px-1.5 text-[10px] font-semibold text-ink" title="Trade plans waiting for your approval">
                {pending}
              </span>
            )}
          </button>
        ))}
      </nav>

      <span className="hidden text-xs text-faint lg:block">Paper trading · simulated money · not investment advice</span>

      <div className="ml-auto flex items-center gap-4">
        <span className={`flex items-center gap-1.5 text-xs font-medium ${state.tone}`}>
          <span className={`h-1.5 w-1.5 rounded-full ${state.dot}`} />
          {state.text}
        </span>
        <span className="text-muted">{user?.display_name}</span>
        <button onClick={() => useApp.getState().signOut()} className="text-muted hover:text-text" title="Sign out" aria-label="Sign out">
          <LogOut size={15} />
        </button>
      </div>
    </header>
  );
}
