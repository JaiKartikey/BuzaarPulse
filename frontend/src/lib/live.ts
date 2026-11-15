// The live WebSocket connection: quotes, the pulse feed and this user's own events.
//
// It reconnects by itself with exponential backoff and jitter. The jitter matters
// at scale: after a server restart every browser reconnects, and without it they
// would all retry at the same instants.

import type { PulseItem, Quote } from "./types";

export type ConnectionState = "connecting" | "live" | "offline";

export type LiveMessage =
  | { type: "ready" | "pong" }
  | { type: "quotes"; quotes: Quote[] }
  | { type: "order" | "fill" | "proposal"; [key: string]: unknown }
  | PulseItem;

export interface LiveHandlers {
  onMessage: (message: LiveMessage) => void;
  onState: (state: ConnectionState) => void;
}

export function connectLive(getToken: () => string | null, handlers: LiveHandlers): () => void {
  let socket: WebSocket | null = null;
  let closed = false;
  let attempt = 0;
  let retryTimer: ReturnType<typeof setTimeout> | undefined;
  let heartbeat: ReturnType<typeof setInterval> | undefined;

  const open = () => {
    if (closed) return;
    handlers.onState("connecting");
    const scheme = location.protocol === "https:" ? "wss" : "ws";
    socket = new WebSocket(`${scheme}://${location.host}/ws`);
    socket.binaryType = "arraybuffer";

    socket.onopen = () => {
      // The token is the first message, never part of the URL.
      socket?.send(JSON.stringify({ op: "auth", token: getToken() }));
      heartbeat = setInterval(() => socket?.readyState === WebSocket.OPEN && socket.send('{"op":"ping"}'), 25_000);
    };
    socket.onmessage = (event) => {
      const text = typeof event.data === "string" ? event.data : new TextDecoder().decode(event.data as ArrayBuffer);
      const message = JSON.parse(text) as LiveMessage;
      if (message.type === "ready") {
        attempt = 0;
        handlers.onState("live");
      }
      handlers.onMessage(message);
    };
    socket.onclose = (event) => {
      clearInterval(heartbeat);
      if (closed) return;
      handlers.onState("offline");
      if (event.code === 4401) return; // rejected token: retrying would not help
      const delay = Math.min(1000 * 2 ** attempt, 15_000) * (0.5 + Math.random() / 2);
      attempt += 1;
      retryTimer = setTimeout(open, delay);
    };
  };

  open();
  return () => {
    closed = true;
    clearTimeout(retryTimer);
    clearInterval(heartbeat);
    socket?.close();
  };
}
