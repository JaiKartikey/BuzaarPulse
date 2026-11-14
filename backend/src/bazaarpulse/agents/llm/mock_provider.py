"""A deterministic stand-in for a language model, for offline runs, demos and CI.

It is not intelligent and does not pretend to be: it follows a fixed script per
role (which tools to call, then a templated summary built only from the tool
results it received). What it buys is that the *entire* agent stack (tool
loop, parallel calls, persistence, streaming, proposals, the multi-agent desk)
runs and is testable with no API key and no network, and behaves identically
every time. Every answer it writes is labelled as coming from the offline model.
"""

from __future__ import annotations

import json
import re
from typing import Any

from bazaarpulse.agents.llm.types import LLMResponse, Message, ToolCall, ToolSpec, Usage, content_blocks

LABEL = "(Offline demo model: this text is templated from the tool results, not written by an AI.)"
_TRADE = re.compile(r"\b(buy|sell)\s+(\d+)\s+(?:shares?\s+)?(?:of\s+)?([A-Za-z&]+)", re.I)
_SIGNAL = re.compile(r"SIGNAL:\s*(bullish|bearish|neutral)", re.I)


Results = list[tuple[str, dict[str, Any], dict[str, Any]]]
Plan = tuple[list[tuple[str, dict[str, Any]]], str]


def _tokens(text: str) -> int:
    return max(1, len(text) // 4)


def _fmt(value: Any, digits: int = 2) -> str:
    return f"{value:,.{digits}f}" if isinstance(value, (int, float)) else "n/a"


class MockProvider:
    name = "mock"
    model = "offline-scripted"

    def __init__(self, symbols: dict[str, str]) -> None:
        self.symbols = symbols  # symbol -> company name
        self.calls = 0

    async def aclose(self) -> None:
        return None

    async def complete(
        self, *, system: str, messages: list[Message], tools: list[ToolSpec], max_tokens: int
    ) -> LLMResponse:
        self.calls += 1
        role = system.split("\n", 1)[0].removeprefix("ROLE:").strip()
        offered = {t.name for t in tools}
        question = self._first_user_text(messages)
        results = self._tool_results(messages)
        plan = getattr(self, f"_plan_{role}", self._plan_copilot)
        calls, text = plan(question, results, offered)

        content: list[dict[str, Any]] = [{"type": "text", "text": text}] if text else []
        tool_calls = [ToolCall(f"mock_{self.calls}_{i}", name, args) for i, (name, args) in enumerate(calls)]
        content += [{"type": "tool_use", "id": c.id, "name": c.name, "input": c.input} for c in tool_calls]
        prompt_size = _tokens(system) + sum(
            _tokens(json.dumps(content_blocks(m["content"]), default=str)) for m in messages
        )
        return LLMResponse(
            text=text,
            tool_calls=tool_calls,
            stop_reason="tool_use" if tool_calls else "end_turn",
            content=content,
            usage=Usage(prompt_size, _tokens(text) + 20 * len(tool_calls)),
            model=self.model,
            provider=self.name,
            latency_ms=1.0,
        )

    # --- Reading the conversation ------------------------------------------

    @staticmethod
    def _first_user_text(messages: list[Message]) -> str:
        for message in messages:
            if message["role"] == "user":
                return " ".join(
                    b.get("text", "") for b in content_blocks(message["content"]) if b.get("type") == "text"
                )
        return ""

    @staticmethod
    def _tool_results(messages: list[Message]) -> Results:
        """``(tool name, input, parsed result)`` for every completed call so far, in order."""
        calls: dict[str, tuple[str, dict[str, Any]]] = {}
        out: Results = []
        for message in messages:
            for block in content_blocks(message["content"]):
                if block.get("type") == "tool_use":
                    calls[block["id"]] = (block["name"], block["input"])
                elif block.get("type") == "tool_result" and block["tool_use_id"] in calls:
                    name, args = calls[block["tool_use_id"]]
                    raw = block.get("content")
                    try:
                        parsed = json.loads(raw) if isinstance(raw, str) else {"value": raw}
                    except json.JSONDecodeError:
                        parsed = {"error": str(raw)}
                    out.append((name, args, parsed if isinstance(parsed, dict) else {"value": parsed}))
        return out

    def _mentioned(self, text: str) -> list[str]:
        found: list[tuple[int, str]] = []
        for symbol, name in self.symbols.items():
            for needle in (symbol, name, name.split()[0]):
                match = re.search(rf"\b{re.escape(needle)}\b", text, re.I)
                if match and len(needle) >= 2:
                    found.append((match.start(), symbol))
                    break
        return list(dict.fromkeys(s for _, s in sorted(found)))

    # --- Phrases built from tool results -------------------------------------

    @staticmethod
    def _quote_line(symbol: str, q: dict[str, Any]) -> str:
        if "last" not in q:
            return f"{symbol}: no live quote yet."
        direction = "up" if q["change_pct"] > 0 else "down" if q["change_pct"] < 0 else "flat"
        return (
            f"{symbol} is at {_fmt(q['last'])}, {direction} {abs(q['change_pct']):.2f}% today "
            f"(range {_fmt(q['low'])} to {_fmt(q['high'])})."
        )

    @staticmethod
    def _indicator_line(ind: dict[str, Any]) -> str:
        if not ind.get("bars"):
            return "There is not enough price history yet for indicators."
        parts = [
            f"Trend is {ind.get('trend', 'unknown')} (EMA9 {_fmt(ind.get('ema9'))} vs EMA21 {_fmt(ind.get('ema21'))})"
        ]
        if ind.get("rsi14") is not None:
            parts.append(f"RSI {ind['rsi14']:.0f} ({ind.get('rsi_state')})")
        if ind.get("vs_vwap_pct") is not None:
            parts.append(f"{abs(ind['vs_vwap_pct']):.2f}% {'above' if ind['vs_vwap_pct'] >= 0 else 'below'} VWAP")
        if ind.get("atr14_pct") is not None:
            parts.append(f"ATR {ind['atr14_pct']:.2f}% per bar")
        return ", ".join(parts) + "."

    @staticmethod
    def _news_line(news: dict[str, Any]) -> str:
        items = news.get("items") or []
        if not items:
            return "No recent headlines were found."
        top = items[0]
        tag = " (simulated feed)" if top.get("simulated") else ""
        return f'Latest headline: "{top["headline"]}" from {top["source"]}{tag}.'

    @staticmethod
    def _score_to_signal(score: float) -> str:
        label = "bullish" if score >= 1 else "bearish" if score <= -1 else "neutral"
        confidence = "high" if abs(score) >= 2 else "medium" if abs(score) >= 1 else "low"
        return f"SIGNAL: {label}  CONFIDENCE: {confidence}"

    @staticmethod
    def _technical_score(ind: dict[str, Any]) -> float:
        score = {"up": 1.0, "down": -1.0}.get(ind.get("trend", ""), 0.0)
        rsi = ind.get("rsi14")
        if rsi is not None:
            score += 1.0 if rsi <= 30 else -1.0 if rsi >= 70 else 0.0
        change = ind.get("change_15_bars_pct")
        if change is not None:
            score += 0.5 if change > 0.2 else -0.5 if change < -0.2 else 0.0
        return score

    @staticmethod
    def _quote_for(symbol: str, results: Results) -> dict[str, Any]:
        for name, _, result in results:
            if name == "get_quote":
                found: dict[str, Any] = result.get("quotes", {}).get(symbol, {})
                return found
        return {}

    # --- Scripts, one per role ------------------------------------------------

    def _plan_copilot(self, question: str, results: Results, offered: set[str]) -> Plan:
        symbols = self._mentioned(question)[:3]
        trades = [(m[0].lower(), int(m[1]), s) for m in _TRADE.findall(question) for s in self._mentioned(m[2])[:1]]
        wants_rebalance = bool(re.search(r"\b(rebalanc|trim|reduce|diversif)", question, re.I))
        wants_portfolio = (
            wants_rebalance
            or bool(trades)
            or bool(re.search(r"\b(portfolio|holding|position|my account|p&l|pnl)", question, re.I))
        )
        done = {name for name, _, _ in results}

        if not results:
            calls: list[tuple[str, dict[str, Any]]] = []
            if wants_portfolio and "get_portfolio" in offered:
                calls.append(("get_portfolio", {}))
            if symbols:
                calls.append(("get_quote", {"symbols": symbols}))
                for s in symbols:
                    calls.append(("get_indicators", {"symbol": s, "interval": "1m"}))
                    calls.append(("search_news", {"symbol": s, "query": None}))
            if re.search(r"\bbacktest", question, re.I) and symbols:
                strategy = "rsi_reversion" if re.search(r"\brsi\b", question, re.I) else "sma_cross"
                calls.append(("run_backtest", {"symbol": symbols[0], "interval": "1m", "strategy": strategy}))
            if not calls:
                calls.append(("get_market_overview", {}))
            return [c for c in calls if c[0] in offered], "Let me look that up."

        if (trades or wants_rebalance) and "propose_trade_plan" in offered and "propose_trade_plan" not in done:
            orders = [
                {"symbol": s, "side": side, "order_type": "market", "quantity": qty, "limit_price": None}
                for side, qty, s in trades
            ]
            rationale = "Orders as requested by the user."
            if not orders and wants_rebalance:
                book = next((r for name, _, r in results if name == "get_portfolio"), {})
                heavy = max(book.get("positions") or [], key=lambda p: p.get("weight_pct", 0), default=None)
                if heavy and heavy.get("weight_pct", 0) > 25:
                    target = heavy["quantity"] * 25 / heavy["weight_pct"]
                    quantity = max(1, int(heavy["quantity"] - target))
                    orders = [
                        {
                            "symbol": heavy["symbol"],
                            "side": "sell",
                            "order_type": "market",
                            "quantity": quantity,
                            "limit_price": None,
                        }
                    ]
                    rationale = f"{heavy['symbol']} is {heavy['weight_pct']}% of the account; trimming it towards 25% reduces concentration."
            if orders:
                return [("propose_trade_plan", {"orders": orders, "rationale": rationale})], ""

        lines: list[str] = []
        for name, _, result in results:
            if "error" in result and len(result) == 1:
                lines.append(f"{name} failed: {result['error']}")
            elif name == "get_quote":
                lines += [self._quote_line(s, q) for s, q in result.get("quotes", {}).items()]
            elif name == "get_indicators":
                lines.append(f"{result.get('symbol')}: {self._indicator_line(result)}")
            elif name == "search_news":
                lines.append(self._news_line(result))
            elif name == "get_portfolio":
                held = (
                    ", ".join(f"{p['symbol']} {p['weight_pct']}%" for p in result.get("positions", []))
                    or "no positions"
                )
                lines.append(
                    f"Your account: equity {_fmt(result.get('equity'))}, cash {_fmt(result.get('cash'))}, "
                    f"total P&L {_fmt(result.get('total_pnl'))}. Holdings: {held}."
                )
            elif name == "run_backtest":
                if result.get("error"):
                    lines.append(f"Backtest: {result['error']}.")
                else:
                    lines.append(
                        f"Backtest of {result['strategy']} on {result['bars']} bars: {result['total_return_pct']}% "
                        f"against {result['buy_hold_return_pct']}% for buy and hold, {result['trades']} trades, "
                        f"max drawdown {result['max_drawdown_pct']}%. A short past window, not a forecast."
                    )
            elif name == "get_market_overview":
                gainers = ", ".join(f"{g['symbol']} {g['change_pct']:+.2f}%" for g in result.get("top_gainers", []))
                losers = ", ".join(f"{g['symbol']} {g['change_pct']:+.2f}%" for g in result.get("top_losers", []))
                lines.append(
                    f"Market: {result.get('advancers', 0)} advancing, {result.get('decliners', 0)} declining, "
                    f"average move {result.get('average_change_pct', 0):+.2f}%. Leaders: {gainers or 'n/a'}. Laggards: {losers or 'n/a'}."
                )
            elif name == "propose_trade_plan":
                if result.get("created"):
                    plan = ", ".join(f"{o['side']} {o['quantity']} {o['symbol']}" for o in result["orders"])
                    lines.append(
                        f"I drafted a trade plan ({plan}). It passed the risk checks and is waiting for your approval "
                        f"in the Proposals panel. Nothing is placed until you approve; it expires in {result['expires_in_minutes']} minutes."
                    )
                else:
                    reasons = "; ".join(result.get("failures") or [str(result.get("error", "rejected"))])
                    lines.append(f"The plan did not pass the risk checks, so nothing was drafted: {reasons}")
        if wants_rebalance and "propose_trade_plan" not in done and not trades:
            lines.append("No position is above 25% of the account, so there is nothing to trim.")
        return [], "\n\n".join([*lines, LABEL])

    def _symbol_from(self, question: str) -> str:
        match = re.search(r"Symbol:\s*([A-Z&]+)", question)
        mentioned = self._mentioned(question)
        return match.group(1) if match else (mentioned[0] if mentioned else next(iter(self.symbols), ""))

    def _plan_technical(self, question: str, results: Results, offered: set[str]) -> Plan:
        symbol = self._symbol_from(question)
        if not results:
            return [
                ("get_quote", {"symbols": [symbol]}),
                ("get_indicators", {"symbol": symbol, "interval": "1m"}),
                ("run_backtest", {"symbol": symbol, "interval": "1m", "strategy": "sma_cross"}),
            ], ""
        ind = next((r for n, _, r in results if n == "get_indicators"), {})
        quote = self._quote_for(symbol, results)
        bt = next((r for n, _, r in results if n == "run_backtest"), {})
        text = f"{self._quote_line(symbol, quote)} {self._indicator_line(ind)}"
        if bt.get("total_return_pct") is not None:
            text += f" A trend-following backtest returned {bt['total_return_pct']}% against {bt['buy_hold_return_pct']}% for buy and hold over {bt['bars']} bars, which is too little history to lean on."
        return [], f"{text}\n{self._score_to_signal(self._technical_score(ind))}"

    def _plan_news(self, question: str, results: Results, offered: set[str]) -> Plan:
        symbol = self._symbol_from(question)
        if not results:
            return [("search_news", {"symbol": symbol, "query": None}), ("get_market_overview", {})], ""
        news = next((r for n, _, r in results if n == "search_news"), {})
        market = next((r for n, _, r in results if n == "get_market_overview"), {})
        items = news.get("items") or []
        sentiments = [i["sentiment"] for i in items if i.get("sentiment") is not None]
        score = 2 * sum(sentiments) / len(sentiments) if sentiments else 0.0
        text = self._news_line(news)
        if items and items[0].get("summary"):
            text += f" {items[0]['summary']}"
        text += f" The wider market is {market.get('average_change_pct', 0):+.2f}% on average with {market.get('advancers', 0)} advancers and {market.get('decliners', 0)} decliners."
        return [], f"{text}\n{self._score_to_signal(score)}"

    def _plan_risk(self, question: str, results: Results, offered: set[str]) -> Plan:
        symbol = self._symbol_from(question)
        if not results:
            calls: list[tuple[str, dict[str, Any]]] = [
                ("get_quote", {"symbols": [symbol]}),
                ("get_indicators", {"symbol": symbol, "interval": "1m"}),
            ]
            if "get_portfolio" in offered:
                calls.append(("get_portfolio", {}))
            return calls, ""
        ind = next((r for n, _, r in results if n == "get_indicators"), {})
        book = next((r for n, _, r in results if n == "get_portfolio"), None)
        atr = ind.get("atr14_pct")
        text = f"Volatility: ATR is {_fmt(atr, 3)}% per one-minute bar and the recent range is {_fmt(ind.get('range_low'))} to {_fmt(ind.get('range_high'))}."
        score = 0.0
        if atr is not None and atr > 0.15:
            text += " That is elevated, so position size matters more than direction here."
            score -= 1.0
        if ind.get("trend") == "down":
            score -= 0.5
        if book is not None:
            held = next((p for p in book.get("positions", []) if p["symbol"] == symbol), None)
            if held:
                text += f" You already hold {held['quantity']} shares, {held['weight_pct']}% of the account."
                score -= 0.5 if held["weight_pct"] > 25 else 0.0
            else:
                text += " You hold no position in it."
        text += " The view breaks if the price closes outside that range on heavy volume."
        return [], f"{text}\n{self._score_to_signal(score)}"

    def _plan_lead(self, question: str, results: Results, offered: set[str]) -> Plan:
        votes = [v.lower() for v in _SIGNAL.findall(question)]
        tally = {k: votes.count(k) for k in ("bullish", "bearish", "neutral")}
        score = tally["bullish"] - tally["bearish"]
        agree = "The desk is split." if tally["bullish"] and tally["bearish"] else "The analysts broadly agree."
        if len(votes) < 3:
            agree += f" Only {len(votes)} of 3 analyst reports were usable, so treat this as a partial view."
        text = (
            f"Desk view on {self._symbol_from(question)}: {tally['bullish']} bullish, {tally['bearish']} bearish, "
            f"{tally['neutral']} neutral. {agree} Watch whether the next headlines confirm the price action before "
            f"acting on it.\n{LABEL}"
        )
        return [], f"{text}\n{self._score_to_signal(float(score))}"

    def _plan_sentinel(self, question: str, results: Results, offered: set[str]) -> Plan:
        symbol = self._symbol_from(question)
        if not results:
            return [
                ("get_quote", {"symbols": [symbol]}),
                ("search_news", {"symbol": symbol, "query": None}),
                ("get_indicators", {"symbol": symbol, "interval": "1m"}),
            ], ""
        quote = self._quote_for(symbol, results)
        news = next((r for n, _, r in results if n == "search_news"), {})
        ind = next((r for n, _, r in results if n == "get_indicators"), {})
        move = re.search(r"move_pct[\"']?:\s*(-?[\d.]+)", question)
        described = f"moved {float(move.group(1)):+.2f}% in under a minute" if move else "made an unusual move"
        driver = self._news_line(news) if news.get("items") else "No headline explains it yet."
        watch = (
            f"RSI is {ind['rsi14']:.0f} ({ind.get('rsi_state')}); watch whether {_fmt(quote.get('last'))} holds."
            if ind.get("rsi14") is not None
            else "Watch whether the move holds over the next few minutes."
        )
        return [], f"{symbol} {described}. {self._quote_line(symbol, quote)} {driver} {watch} {LABEL}"
