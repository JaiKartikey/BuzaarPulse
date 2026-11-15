import { describe, expect, it } from "vitest";
import { ago, compact, direction, money, percent, price } from "./format";
import { createSseParser, type SseEvent } from "./sse";
import type { AgentStep } from "./types";
import { toolActivity } from "./useRun";

describe("format", () => {
  it("groups digits the Indian way", () => {
    expect(price(1234567.5)).toBe("12,34,567.50");
    expect(money(1000000)).toBe("₹10,00,000");
    expect(money(-2500.4, { paise: true })).toBe("−₹2,500.40");
    expect(price(null)).toBe("–");
  });

  it("signs percentages and classifies direction", () => {
    expect(percent(1.239)).toBe("+1.24%");
    expect(percent(-0.8)).toBe("−0.80%");
    expect(percent(0)).toBe("0.00%");
    expect([direction(2), direction(-2), direction(0), direction(undefined)]).toEqual(["up", "down", "flat", "flat"]);
  });

  it("abbreviates volume in lakh and crore", () => {
    expect(compact(950)).toBe("950");
    expect(compact(12_500)).toBe("12.5K");
    expect(compact(125_000)).toBe("1.25L");
    expect(compact(34_000_000)).toBe("3.40Cr");
  });

  it("describes elapsed time", () => {
    expect(ago(1000, 1042)).toBe("42s ago");
    expect(ago(1000, 1000 + 5 * 60)).toBe("5m ago");
    expect(ago(1000, 1000 + 3 * 3600)).toBe("3h ago");
  });
});

describe("SSE parser", () => {
  const collect = () => {
    const events: SseEvent[] = [];
    return { events, feed: createSseParser((e) => events.push(e)) };
  };

  it("emits complete events and ignores keep-alive comments", () => {
    const { events, feed } = collect();
    feed('id: 1-0\ndata: {"seq":1}\n\n: keep-alive\n\nid: 2-0\ndata: {"seq":2}\n\n');
    expect(events).toEqual([
      { id: "1-0", data: '{"seq":1}' },
      { id: "2-0", data: '{"seq":2}' },
    ]);
  });

  it("waits for the rest of an event split across network chunks", () => {
    const { events, feed } = collect();
    feed('id: 7-0\ndata: {"se');
    expect(events).toHaveLength(0);
    feed('q":7}\n');
    expect(events).toHaveLength(0);
    feed("\n");
    expect(events).toEqual([{ id: "7-0", data: '{"seq":7}' }]);
  });
});

describe("toolActivity", () => {
  const step = (seq: number, agent: string, type: AgentStep["type"], payload: Record<string, unknown>): AgentStep => ({ seq, agent, type, payload, ts: seq });
  const steps = [
    step(1, "technical", "tool_call", { id: "a", name: "get_quote", input: { symbols: ["TCS"] } }),
    step(2, "news", "tool_call", { id: "b", name: "search_news", input: { symbol: "TCS" } }),
    step(3, "news", "tool_result", { id: "b", name: "search_news", ok: false, result: "Error: boom" }),
    step(4, "technical", "tool_result", { id: "a", name: "get_quote", ok: true, result: { last: 1 } }),
    step(5, "technical", "tool_call", { id: "c", name: "run_backtest", input: {} }),
  ];

  it("pairs calls with results that arrive in any order", () => {
    expect(toolActivity(steps).map((c) => [c.name, c.ok])).toEqual([
      ["get_quote", true],
      ["search_news", false],
      ["run_backtest", null], // still running
    ]);
  });

  it("can be limited to one agent's lane", () => {
    expect(toolActivity(steps, "news").map((c) => c.name)).toEqual(["search_news"]);
  });
});
