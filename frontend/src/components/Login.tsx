import { Activity, Bot, LoaderCircle, ShieldCheck, Zap } from "lucide-react";
import { useState, type FormEvent } from "react";
import { api, ApiError, type Session } from "../lib/api";
import { useApp } from "../store";

const FEATURES = [
  { icon: Activity, title: "A live market pipeline", text: "Ticks become candles, quotes and alerts in a few milliseconds." },
  { icon: ShieldCheck, title: "Real broker mechanics", text: "Risk checks, order matching and a double-entry ledger. The money is not real; the accounting is." },
  { icon: Bot, title: "An AI research desk", text: "Agents investigate with tools and draft trades. Nothing executes until you approve it." },
];

export function Login() {
  const [mode, setMode] = useState<"login" | "register">("login");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [name, setName] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  const enter = async (attempt: () => Promise<Session>) => {
    setBusy(true);
    setError("");
    try {
      useApp.getState().signIn(await attempt());
      await useApp.getState().bootstrap();
    } catch (e) {
      setError(e instanceof ApiError ? e.message : "Could not reach the server. Is the backend running?");
    } finally {
      setBusy(false);
    }
  };

  const submit = (event: FormEvent) => {
    event.preventDefault();
    void enter(() => (mode === "login" ? api.login(email, password) : api.register(email, password, name || email.split("@")[0]!)));
  };

  const field = "w-full rounded-md border border-line bg-ink px-3 py-2 text-sm placeholder:text-faint";

  return (
    <div className="grid h-full place-items-center overflow-auto p-6">
      <div className="grid w-full max-w-4xl gap-8 md:grid-cols-[1.15fr_1fr]">
        <section className="flex flex-col justify-center">
          <div className="mb-5 flex items-center gap-2.5">
            <span className="grid h-9 w-9 place-items-center rounded-lg bg-up/15 text-up">
              <Zap size={19} />
            </span>
            <span className="text-xl font-semibold tracking-tight">BazaarPulse</span>
          </div>
          <h1 className="text-3xl leading-tight font-semibold tracking-tight">
            Watch a market think.
            <br />
            <span className="text-muted">Trade it with paper money.</span>
          </h1>
          <ul className="mt-7 space-y-4">
            {FEATURES.map(({ icon: Icon, title, text }) => (
              <li key={title} className="flex gap-3">
                <Icon size={17} className="mt-0.5 shrink-0 text-ai" />
                <div>
                  <div className="font-medium">{title}</div>
                  <div className="text-muted">{text}</div>
                </div>
              </li>
            ))}
          </ul>
        </section>

        <section className="panel p-6">
          <button
            onClick={() => void enter(api.demoLogin)}
            disabled={busy}
            className="flex w-full items-center justify-center gap-2 rounded-md bg-up px-4 py-2.5 font-semibold text-ink transition hover:brightness-110 disabled:opacity-60"
          >
            {busy ? <LoaderCircle size={16} className="animate-spin" /> : <Zap size={16} />}
            Enter the demo
          </button>
          <p className="mt-2 text-center text-xs text-muted">A shared account with ₹10,00,000 of paper money.</p>

          <div className="my-5 flex items-center gap-3 text-xs text-faint">
            <span className="h-px flex-1 bg-line" />
            or use your own account
            <span className="h-px flex-1 bg-line" />
          </div>

          <form onSubmit={submit} className="space-y-3">
            {mode === "register" && (
              <input className={field} placeholder="Display name" value={name} onChange={(e) => setName(e.target.value)} maxLength={80} />
            )}
            <input className={field} type="email" placeholder="Email" value={email} onChange={(e) => setEmail(e.target.value)} required autoComplete="email" />
            <input
              className={field}
              type="password"
              placeholder="Password (8+ characters)"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              required
              minLength={8}
              autoComplete={mode === "login" ? "current-password" : "new-password"}
            />
            <button disabled={busy} className="w-full rounded-md border border-line-strong bg-raised px-4 py-2 font-medium hover:border-accent disabled:opacity-60">
              {mode === "login" ? "Sign in" : "Create account"}
            </button>
          </form>

          {error && <p className="mt-3 rounded-md bg-down/10 px-3 py-2 text-sm text-down">{error}</p>}

          <button onClick={() => setMode(mode === "login" ? "register" : "login")} className="mt-4 w-full text-center text-xs text-muted hover:text-text">
            {mode === "login" ? "New here? Create an account" : "Already have an account? Sign in"}
          </button>
        </section>
      </div>
    </div>
  );
}
