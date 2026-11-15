import { useEffect, useState } from "react";
import { api, getToken } from "./api";
import { followStream } from "./sse";
import type { AgentRun, AgentStep } from "./types";

export interface LiveRun {
  steps: AgentStep[];
  run: AgentRun | null;
  streaming: boolean;
  error: string;
}

/**
 * Follows one agent run. The event stream replays the run from its first step,
 * so opening an old run and watching a new one are the same code path.
 */
export function useRun(runId: string | null, onFinished?: () => void): LiveRun {
  // State remembers which run it belongs to. When the id changes, the old state is simply
  // ignored (see the return below) instead of being reset inside the effect.
  const [state, setState] = useState<LiveRun & { runId: string | null }>({ runId: null, steps: [], run: null, streaming: false, error: "" });

  useEffect(() => {
    if (!runId) return;
    const fresh = { runId, steps: [] as AgentStep[], run: null, streaming: true, error: "" };
    const update = (change: (current: LiveRun) => Partial<LiveRun>) =>
      setState((current) => {
        const base = current.runId === runId ? current : fresh;
        return { ...base, ...change(base) };
      });
    const seen = new Set<number>();
    return followStream<AgentStep>({
      url: `/api/agent/runs/${runId}/events`,
      token: getToken(),
      isFinal: (step) => step.type === "done",
      onEvent: (step) => {
        if (seen.has(step.seq)) return; // a reconnect can replay the event we resumed from
        seen.add(step.seq);
        update((current) => ({ steps: [...current.steps, step] }));
        if (step.type === "done") {
          api
            .run(runId)
            .then((run) => update(() => ({ run, streaming: false })))
            .catch(() => update(() => ({ streaming: false })));
          onFinished?.();
        }
      },
      onError: () => update(() => ({ streaming: false, error: "Lost the connection to this run." })),
    });
    // onFinished is intentionally not a dependency: a new callback identity must not restart the stream.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [runId]);

  if (state.runId !== runId) return { steps: [], run: null, streaming: Boolean(runId), error: "" };
  return state;
}

export interface ToolActivity {
  id: string;
  name: string;
  input: Record<string, unknown>;
  ok: boolean | null; // null while the call is still running
  result: unknown;
}

/** Pairs each tool call with its result, in the order the calls were made. */
export function toolActivity(steps: AgentStep[], agent?: string): ToolActivity[] {
  const calls = new Map<string, ToolActivity>();
  for (const step of steps) {
    if (agent && step.agent !== agent) continue;
    const id = String(step.payload.id ?? "");
    if (step.type === "tool_call") {
      calls.set(id, { id, name: String(step.payload.name), input: (step.payload.input ?? {}) as Record<string, unknown>, ok: null, result: null });
    } else if (step.type === "tool_result") {
      const call = calls.get(id);
      if (call) {
        call.ok = Boolean(step.payload.ok);
        call.result = step.payload.result;
      }
    }
  }
  return [...calls.values()];
}
