"""The tradable universe for each market-data provider.

``ref_price`` is only a starting level for the simulator (and a fallback
previous close): the numbers are round figures, not quotes.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True, slots=True)
class InstrumentSpec:
    symbol: str
    name: str
    sector: str
    ref_price: Decimal
    beta: float = 1.0  # sensitivity to the simulated market factor
    exchange: str = "NSE"
    currency: str = "INR"
    tick_size: Decimal = Decimal("0.05")
    provider_symbol: str | None = None


def _nse(symbol: str, name: str, sector: str, price: str, beta: float) -> InstrumentSpec:
    return InstrumentSpec(symbol, name, sector, Decimal(price), beta, provider_symbol=f"{symbol}.NS")


def _us(symbol: str, name: str, sector: str, price: str, beta: float) -> InstrumentSpec:
    return InstrumentSpec(
        symbol, name, sector, Decimal(price), beta,
        exchange="US", currency="USD", tick_size=Decimal("0.01"), provider_symbol=symbol,
    )  # fmt: skip


NSE_UNIVERSE: tuple[InstrumentSpec, ...] = (
    _nse("RELIANCE", "Reliance Industries", "Energy", "2900", 1.0),
    _nse("TCS", "Tata Consultancy Services", "IT", "4100", 0.8),
    _nse("INFY", "Infosys", "IT", "1850", 0.9),
    _nse("HDFCBANK", "HDFC Bank", "Banking", "1700", 1.0),
    _nse("ICICIBANK", "ICICI Bank", "Banking", "1250", 1.1),
    _nse("SBIN", "State Bank of India", "Banking", "820", 1.3),
    _nse("BHARTIARTL", "Bharti Airtel", "Telecom", "1600", 0.8),
    _nse("ITC", "ITC", "FMCG", "470", 0.6),
    _nse("HINDUNILVR", "Hindustan Unilever", "FMCG", "2500", 0.5),
    _nse("LT", "Larsen & Toubro", "Infrastructure", "3600", 1.1),
    _nse("TATAMOTORS", "Tata Motors", "Auto", "950", 1.5),
    _nse("MARUTI", "Maruti Suzuki India", "Auto", "12500", 0.9),
    _nse("SUNPHARMA", "Sun Pharmaceutical", "Pharma", "1800", 0.7),
    _nse("BAJFINANCE", "Bajaj Finance", "Financials", "7000", 1.4),
)

US_UNIVERSE: tuple[InstrumentSpec, ...] = (
    _us("AAPL", "Apple", "Technology", "230", 1.1),
    _us("MSFT", "Microsoft", "Technology", "430", 0.9),
    _us("NVDA", "NVIDIA", "Semiconductors", "140", 1.7),
    _us("GOOGL", "Alphabet", "Technology", "175", 1.0),
    _us("AMZN", "Amazon", "Consumer", "200", 1.2),
    _us("META", "Meta Platforms", "Technology", "580", 1.2),
    _us("TSLA", "Tesla", "Auto", "350", 2.0),
    _us("JPM", "JPMorgan Chase", "Banking", "240", 1.1),
)


def universe_for(provider: str) -> tuple[InstrumentSpec, ...]:
    """Finnhub's free feed streams US trades; the simulator and Yahoo use NSE names."""
    return US_UNIVERSE if provider == "finnhub" else NSE_UNIVERSE
