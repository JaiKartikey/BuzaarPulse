import { useEffect, useState } from "react";
import { Login } from "./components/Login";
import { TopBar } from "./components/TopBar";
import { getToken, setUnauthorizedHandler } from "./lib/api";
import { connectLive } from "./lib/live";
import { useApp } from "./store";
import { AiDesk } from "./views/AiDesk";
import { System } from "./views/System";
import { Terminal } from "./views/Terminal";

export function App() {
  const user = useApp((s) => s.user);
  const view = useApp((s) => s.view);
  const [booting, setBooting] = useState(Boolean(getToken()));

  // A returning visitor: validate the stored token and load the starting state.
  useEffect(() => {
    setUnauthorizedHandler(() => useApp.getState().signOut());
    if (!getToken()) return;
    useApp
      .getState()
      .bootstrap()
      .catch(() => useApp.getState().signOut())
      .finally(() => setBooting(false));
  }, []);

  // One live connection for the whole app, for as long as someone is signed in.
  const userId = user?.id;
  useEffect(() => {
    if (!userId) return;
    const { handleLive, setConnection } = useApp.getState();
    return connectLive(getToken, { onMessage: handleLive, onState: setConnection });
  }, [userId]);

  if (booting) return <div className="grid h-full place-items-center text-muted">Loading…</div>;
  if (!user) return <Login />;

  return (
    <div className="flex h-full flex-col">
      <TopBar />
      <main className="min-h-0 flex-1 p-3">
        {view === "terminal" && <Terminal />}
        {view === "desk" && <AiDesk />}
        {view === "system" && <System />}
      </main>
    </div>
  );
}
