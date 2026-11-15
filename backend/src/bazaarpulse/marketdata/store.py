"""Read side of market data, shared by the API and the agent tools.

Hot, constantly overwritten data (the latest quote) is read from Redis.
History (candles, news, anomalies) is read from Postgres.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from typing import Any

import orjson
from sqlalchemy import select, text

from bazaarpulse.bus import RedisBus
from bazaarpulse.db import Database
from bazaarpulse.db.models import Anomaly, Instrument
from bazaarpulse.marketdata.processor import STATS_KEY, quote_key

INTERVALS: dict[str, int] = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600}

# Wider bars are built from 1-minute rows at query time. `date_bin` snaps each
# row to its bucket; first/last by time give open/close. The `ts >= :since`
# bound lets Postgres skip every monthly partition older than the window.
_CANDLES_SQL = text(
    """
    SELECT extract(epoch FROM bucket)::bigint AS t, o, h, l, c, v, vw
      FROM (
        SELECT date_bin(CAST(:width AS interval), ts, TIMESTAMPTZ '2001-01-01 00:00:00+00') AS bucket,
               (array_agg(open  ORDER BY ts ASC))[1]  AS o,
               max(high)                              AS h,
               min(low)                               AS l,
               (array_agg(close ORDER BY ts DESC))[1] AS c,
               sum(volume)                            AS v,
               sum(vwap * volume) / nullif(sum(volume), 0) AS vw
          FROM candles
         WHERE symbol = :symbol AND "interval" = '1m' AND ts >= :since
         GROUP BY bucket
         ORDER BY bucket DESC
         LIMIT :limit
      ) recent
     ORDER BY t
    """
)

_NEWS_SQL = """
    SELECT id, symbol, headline, summary, source, url, sentiment, is_simulated, published_at
      FROM news
     WHERE {where}
     ORDER BY published_at DESC
     LIMIT :limit
"""


class MarketStore:
    def __init__(self, bus: RedisBus, db: Database) -> None:
        self.bus = bus
        self.db = db
        self._instruments: list[dict[str, Any]] = []
        self._instruments_at = 0.0

    # --- Instruments --------------------------------------------------------

    async def instruments(self) -> list[dict[str, Any]]:
        if time.monotonic() - self._instruments_at > 60 or not self._instruments:
            async with self.db.session() as session:
                rows = (
                    await session.execute(select(Instrument).where(Instrument.is_active).order_by(Instrument.symbol))
                ).scalars()
                self._instruments = [
                    {
                        "symbol": i.symbol,
                        "name": i.name,
                        "exchange": i.exchange,
                        "sector": i.sector,
                        "currency": i.currency,
                        "tick_size": float(i.tick_size),
                        "lot_size": i.lot_size,
                        "ref_price": float(i.ref_price),
                    }
                    for i in rows
                ]
            self._instruments_at = time.monotonic()
        return self._instruments

    async def symbols(self) -> list[str]:
        return [i["symbol"] for i in await self.instruments()]

    # --- Quotes -------------------------------------------------------------

    async def quotes(self, symbols: list[str] | None = None) -> dict[str, dict[str, Any]]:
        wanted = symbols if symbols is not None else await self.symbols()
        if not wanted:
            return {}
        raw = await self.bus.redis.mget([self.bus.key(quote_key(s)) for s in wanted])
        return {s: orjson.loads(r) for s, r in zip(wanted, raw, strict=True) if r}

    async def quote(self, symbol: str) -> dict[str, Any] | None:
        return (await self.quotes([symbol])).get(symbol)

    # --- Candles ------------------------------------------------------------

    async def candles(self, symbol: str, interval: str = "1m", limit: int = 300) -> list[dict[str, Any]]:
        width = INTERVALS[interval]
        limit = max(1, min(limit, 1000))
        # Reach back at least a week: markets close overnight and on weekends, and a chart should still
        # show the previous sessions. The bound keeps the scan to a couple of monthly partitions.
        since = datetime.now(UTC) - max(timedelta(seconds=width * (limit + 1)), timedelta(days=7))
        async with self.db.session() as session:
            rows = (
                await session.execute(
                    _CANDLES_SQL,
                    {"width": timedelta(seconds=width), "symbol": symbol, "since": since, "limit": limit},
                )
            ).mappings()
            out = [
                {
                    "t": int(r["t"]),
                    "o": float(r["o"]),
                    "h": float(r["h"]),
                    "l": float(r["l"]),
                    "c": float(r["c"]),
                    "v": int(r["v"]),
                    "vw": round(float(r["vw"] or r["c"]), 4),
                }
                for r in rows
            ]
        # The bar still being built lives only in Redis; merge it in so the chart's last candle is live.
        quote = await self.quote(symbol)
        live = (quote or {}).get("bar")
        if live:
            bucket = live["t"] // width * width
            if out and out[-1]["t"] == bucket:
                last = out[-1]
                last["h"], last["l"], last["c"] = max(last["h"], live["h"]), min(last["l"], live["l"]), live["c"]
                last["v"] += live["v"]
            elif not out or bucket > out[-1]["t"]:
                out.append({**live, "t": bucket})
        return out[-limit:]

    # --- News and anomalies -------------------------------------------------

    async def news(
        self, *, symbol: str | None = None, query: str | None = None, limit: int = 20
    ) -> list[dict[str, Any]]:
        where, params = ["true"], {"limit": max(1, min(limit, 100))}
        if symbol:
            where.append("symbol = :symbol")
            params["symbol"] = symbol  # type: ignore[assignment]
        if query:
            # websearch_to_tsquery accepts free text ("rbi rate cut") without raising on odd input.
            where.append("tsv @@ websearch_to_tsquery('english', :query)")
            params["query"] = query  # type: ignore[assignment]
        async with self.db.session() as session:
            rows = (await session.execute(text(_NEWS_SQL.format(where=" AND ".join(where))), params)).mappings()
            return [
                {
                    "id": r["id"],
                    "symbol": r["symbol"],
                    "headline": r["headline"],
                    "summary": r["summary"],
                    "source": r["source"],
                    "url": r["url"],
                    "sentiment": None if r["sentiment"] is None else float(r["sentiment"]),
                    "is_simulated": r["is_simulated"],
                    "ts": r["published_at"].timestamp(),
                }
                for r in rows
            ]

    async def anomalies(self, limit: int = 30, symbol: str | None = None) -> list[dict[str, Any]]:
        stmt = select(Anomaly).order_by(Anomaly.detected_at.desc()).limit(max(1, min(limit, 200)))
        if symbol:
            stmt = stmt.where(Anomaly.symbol == symbol)
        async with self.db.session() as session:
            return [
                {
                    "id": str(a.id),
                    "symbol": a.symbol,
                    "kind": a.kind,
                    "severity": a.severity,
                    "ts": a.detected_at.timestamp(),
                    "brief": a.brief,
                    "run_id": str(a.run_id) if a.run_id else None,
                    **a.details,
                }
                for a in (await session.execute(stmt)).scalars()
            ]

    # --- Market overview ----------------------------------------------------

    async def overview(self) -> dict[str, Any]:
        instruments = {i["symbol"]: i for i in await self.instruments()}
        quotes = await self.quotes(list(instruments))
        ranked = sorted(quotes.values(), key=lambda q: q["change_pct"], reverse=True)
        brief = [{"symbol": q["symbol"], "last": q["last"], "change_pct": q["change_pct"]} for q in ranked]
        sectors: dict[str, list[float]] = {}
        for q in ranked:
            sectors.setdefault(instruments[q["symbol"]]["sector"], []).append(q["change_pct"])
        return {
            "advancers": sum(1 for q in ranked if q["change_pct"] > 0),
            "decliners": sum(1 for q in ranked if q["change_pct"] < 0),
            "average_change_pct": round(sum(q["change_pct"] for q in ranked) / len(ranked), 2) if ranked else 0.0,
            "top_gainers": brief[:3],
            "top_losers": brief[::-1][:3],
            "sectors": sorted(
                ({"sector": s, "change_pct": round(sum(v) / len(v), 2)} for s, v in sectors.items()),
                key=lambda s: s["change_pct"],
                reverse=True,
            ),
        }

    async def pipeline_stats(self) -> dict[str, Any]:
        raw = await self.bus.redis.hgetall(self.bus.key(STATS_KEY))
        stats: dict[str, Any] = {}
        for k, v in raw.items():
            key = k.decode() if isinstance(k, bytes) else str(k)
            value = v.decode() if isinstance(v, bytes) else str(v)
            try:
                stats[key] = float(value) if "." in value else int(value)
            except ValueError:
                stats[key] = value
        return stats
