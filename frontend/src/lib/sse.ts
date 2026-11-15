// A Server-Sent Events reader built on fetch.
//
// The browser's built-in EventSource cannot send an Authorization header, which
// would force the token into the URL (and from there into logs). Reading the
// stream with fetch keeps the token in a header. Reconnection is handled here:
// on a dropped connection we reconnect with `Last-Event-ID`, and the server
// resumes right after the last event we saw.

export interface SseEvent {
  id: string;
  data: string;
}

/** Splits a text stream into SSE events. Pure, so it can be unit-tested without a network. */
export function createSseParser(onEvent: (event: SseEvent) => void): (chunk: string) => void {
  let buffer = "";
  return (chunk) => {
    buffer += chunk;
    // An event ends at a blank line. Anything after the last blank line is an incomplete event: keep it.
    let boundary = buffer.indexOf("\n\n");
    while (boundary !== -1) {
      const block = buffer.slice(0, boundary);
      buffer = buffer.slice(boundary + 2);
      let id = "";
      const data: string[] = [];
      for (const line of block.split("\n")) {
        if (line.startsWith(":")) continue; // a comment, used by the server as a keep-alive
        if (line.startsWith("id: ")) id = line.slice(4);
        else if (line.startsWith("data: ")) data.push(line.slice(6));
      }
      if (data.length) onEvent({ id, data: data.join("\n") });
      boundary = buffer.indexOf("\n\n");
    }
  };
}

export interface StreamOptions<T> {
  url: string;
  token: string | null;
  onEvent: (event: T) => void;
  /** Return true when this event is the last one; the stream then closes for good. */
  isFinal: (event: T) => boolean;
  onError?: (error: unknown) => void;
}

/** Follows an event stream until its final event, reconnecting and resuming as needed. Returns a cancel function. */
export function followStream<T>({ url, token, onEvent, isFinal, onError }: StreamOptions<T>): () => void {
  const controller = new AbortController();
  let lastId = "";
  let finished = false;

  const run = async () => {
    for (let attempt = 0; !finished && !controller.signal.aborted; attempt++) {
      try {
        const headers: Record<string, string> = { Accept: "text/event-stream" };
        if (token) headers.Authorization = `Bearer ${token}`;
        if (lastId) headers["Last-Event-ID"] = lastId;
        const response = await fetch(url, { headers, signal: controller.signal });
        if (!response.ok || !response.body) throw new Error(`stream failed with ${response.status}`);
        attempt = 0;
        const parse = createSseParser((event) => {
          lastId = event.id || lastId;
          const payload = JSON.parse(event.data) as T;
          onEvent(payload);
          if (isFinal(payload)) finished = true;
        });
        const reader = response.body.pipeThrough(new TextDecoderStream()).getReader();
        for (;;) {
          const { done, value } = await reader.read();
          if (done || finished) break;
          parse(value);
        }
        if (finished) await reader.cancel().catch(() => {});
      } catch (error) {
        if (controller.signal.aborted) return;
        if (attempt >= 5) {
          onError?.(error);
          return;
        }
      }
      // Back off before reconnecting: 0.5s, 1s, 2s ... capped at 8s.
      if (!finished) await new Promise((resolve) => setTimeout(resolve, Math.min(500 * 2 ** attempt, 8000)));
    }
  };
  void run();
  return () => controller.abort();
}
