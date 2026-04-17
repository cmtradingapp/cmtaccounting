"""CRO dashboard metric formulas.

Self-contained — operates on plain dicts returned by `mt5_bridge.MT5Bridge`.
All money values are in account currency (USD on AN100).

The Metabase dashboard we're replicating uses Europe/Nicosia for day boundaries.
Conversions here go Nicosia-local-date → UTC → MT5 SMTTime (which the bridge
handles).
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from typing import Iterable
from zoneinfo import ZoneInfo

NICOSIA = ZoneInfo("Europe/Nicosia")

# MT5 deal action codes (MT5APIConstants.h — IMTDeal::EnDealAction)
ACTION_BUY         = 0
ACTION_SELL        = 1
ACTION_BALANCE     = 2   # deposit / withdrawal
ACTION_CREDIT      = 3
ACTION_CHARGE      = 4
ACTION_CORRECTION  = 5
ACTION_BONUS       = 6
ACTION_COMMISSION  = 7
ACTION_INTEREST    = 11

ENTRY_IN       = 0
ENTRY_OUT      = 1
ENTRY_INOUT    = 2
ENTRY_OUT_BY   = 3


# ── time helpers ───────────────────────────────────────────────────────────
def day_bounds_utc(day: date) -> tuple[datetime, datetime]:
    """Return (start_utc, end_utc) for the given Europe/Nicosia calendar day."""
    start_local = datetime.combine(day, time(0, 0, 0), NICOSIA)
    end_local = start_local + timedelta(days=1)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)


def month_bounds_utc(year: int, month: int) -> tuple[datetime, datetime]:
    start_local = datetime(year, month, 1, tzinfo=NICOSIA)
    if month == 12:
        end_local = datetime(year + 1, 1, 1, tzinfo=NICOSIA)
    else:
        end_local = datetime(year, month + 1, 1, tzinfo=NICOSIA)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)


# ── classification ─────────────────────────────────────────────────────────
def is_trade_deal(d: dict) -> bool:
    return d["action"] in (ACTION_BUY, ACTION_SELL)


def is_balance_deal(d: dict) -> bool:
    return d["action"] == ACTION_BALANCE


def is_bonus_deal(d: dict) -> bool:
    if d["action"] == ACTION_BONUS:
        return True
    # Per plan: "balance transactions whose comment includes 'Bonus'" also count.
    return is_balance_deal(d) and "bonus" in (d.get("comment") or "").lower()


def is_internal_transfer(d: dict) -> bool:
    """Filter out intra-client transfers (marked in comment on this broker)."""
    c = (d.get("comment") or "").lower()
    return "internal" in c or "transfer" in c


# ── per-deal math ──────────────────────────────────────────────────────────
def deal_pnl(d: dict) -> float:
    """Realised P&L including swap + commission — matches plan's formula."""
    return float(d.get("profit", 0.0)) + float(d.get("storage", 0.0)) + float(d.get("commission", 0.0))


def deal_notional_usd(d: dict) -> float:
    """|volume * contract_size * price|  in account currency (approx)."""
    lots = float(d.get("volume_ext", 0)) / 10000.0 or float(d.get("volume", 0)) / 100.0
    price = float(d.get("price", 0.0)) or 1.0
    notional_quote = abs(lots * float(d.get("contract_size", 0.0)) * price)
    rate = float(d.get("rate_profit", 1.0)) or 1.0
    return notional_quote * rate


def position_open_pnl(p: dict) -> float:
    return float(p.get("profit", 0.0)) + float(p.get("storage", 0.0))


def position_abs_exposure(p: dict) -> float:
    lots = float(p.get("volume_ext", 0)) / 10000.0 or float(p.get("volume", 0)) / 100.0
    price = float(p.get("price_current", 0.0)) or float(p.get("price_open", 0.0)) or 1.0
    notional_quote = abs(lots * float(p.get("contract_size", 0.0)) * price)
    rate = float(p.get("rate_profit", 1.0)) or 1.0
    return notional_quote * rate


# ── aggregations ───────────────────────────────────────────────────────────
def net_deposits(deals: Iterable[dict]) -> tuple[float, float, float]:
    """Return (deposits, withdrawals, net) from balance deals.
    `profit` sign on balance-action deals: +ve = deposit, -ve = withdrawal.
    Bonuses and internal transfers excluded.
    """
    dep = wd = 0.0
    for d in deals:
        if not is_balance_deal(d) or is_bonus_deal(d) or is_internal_transfer(d):
            continue
        amt = float(d.get("profit", 0.0))
        if amt >= 0:
            dep += amt
        else:
            wd += amt
    return dep, wd, dep + wd


def closed_pnl(deals: Iterable[dict]) -> float:
    return sum(deal_pnl(d) for d in deals if is_trade_deal(d) and d["entry"] != ENTRY_IN)


def open_pnl(positions: Iterable[dict]) -> float:
    return sum(position_open_pnl(p) for p in positions)


def total_abs_exposure(positions: Iterable[dict]) -> float:
    return sum(position_abs_exposure(p) for p in positions)


def volume_usd(deals: Iterable[dict]) -> float:
    return sum(deal_notional_usd(d) for d in deals if is_trade_deal(d))


def trader_counts(deals: Iterable[dict]) -> tuple[int, int]:
    """
    # Active Traders  = distinct logins with open_time (entry=IN) in period
    # Traders         = distinct logins with ANY trade deal in period (open or close)
    """
    active, any_ = set(), set()
    for d in deals:
        if not is_trade_deal(d):
            continue
        any_.add(d["login"])
        if d["entry"] == ENTRY_IN:
            active.add(d["login"])
    return len(active), len(any_)


def depositor_counts(deals: Iterable[dict]) -> tuple[int, set[int]]:
    """Count + set of distinct logins that deposited (positive balance op, not bonus/transfer)."""
    depositors: set[int] = set()
    for d in deals:
        if not is_balance_deal(d) or is_bonus_deal(d) or is_internal_transfer(d):
            continue
        if float(d.get("profit", 0.0)) > 0:
            depositors.add(int(d["login"]))
    return len(depositors), depositors


# ── Volume Distribution by symbol ──────────────────────────────────────────
@dataclass
class SymbolBucket:
    symbol: str
    n_deals: int = 0
    notional: float = 0.0
    swap: float = 0.0
    commission: float = 0.0
    pnl: float = 0.0
    abs_notional_buy: float = 0.0
    abs_notional_sell: float = 0.0


def volume_by_symbol(deals: Iterable[dict]) -> list[SymbolBucket]:
    buckets: dict[str, SymbolBucket] = {}
    for d in deals:
        if not is_trade_deal(d):
            continue
        b = buckets.setdefault(d["symbol"], SymbolBucket(symbol=d["symbol"]))
        b.n_deals += 1
        b.swap += float(d.get("storage", 0.0))
        b.commission += float(d.get("commission", 0.0))
        b.pnl += deal_pnl(d)
        n = deal_notional_usd(d)
        b.notional += n
        if d["action"] == ACTION_BUY:
            b.abs_notional_buy += n
        else:
            b.abs_notional_sell += n
    return sorted(buckets.values(), key=lambda x: x.notional, reverse=True)


# ── per-group / per-office breakdown ───────────────────────────────────────
@dataclass
class GroupPerf:
    group: str
    n_active_traders: int = 0
    n_traders: int = 0
    n_depositors: int = 0
    deposits: float = 0.0
    withdrawals: float = 0.0
    net_deposits: float = 0.0
    closed_pnl: float = 0.0
    volume: float = 0.0


def perf_by_group(deals: Iterable[dict], user_group_fn) -> list[GroupPerf]:
    """Aggregate deals by `user_group_fn(login)->group_str` (often the MT5 group).
    Skips deals whose login can't be resolved.
    """
    active: dict[str, set[int]] = defaultdict(set)
    any_: dict[str, set[int]] = defaultdict(set)
    deps: dict[str, set[int]] = defaultdict(set)
    buckets: dict[str, GroupPerf] = {}
    for d in deals:
        g = user_group_fn(int(d["login"]))
        if g is None:
            continue
        b = buckets.setdefault(g, GroupPerf(group=g))
        if is_trade_deal(d):
            any_[g].add(d["login"])
            if d["entry"] == ENTRY_IN:
                active[g].add(d["login"])
            b.closed_pnl += deal_pnl(d) if d["entry"] != ENTRY_IN else 0.0
            b.volume += deal_notional_usd(d)
        elif is_balance_deal(d) and not is_bonus_deal(d) and not is_internal_transfer(d):
            amt = float(d.get("profit", 0.0))
            if amt > 0:
                deps[g].add(d["login"])
                b.deposits += amt
            else:
                b.withdrawals += amt
            b.net_deposits = b.deposits + b.withdrawals
    for g, b in buckets.items():
        b.n_active_traders = len(active[g])
        b.n_traders = len(any_[g])
        b.n_depositors = len(deps[g])
    return sorted(buckets.values(), key=lambda x: x.volume, reverse=True)


# ── snapshot wrapper ───────────────────────────────────────────────────────
@dataclass
class Snapshot:
    label: str
    date: str
    group_mask: str

    # counts
    n_deals: int = 0
    n_trade_deals: int = 0
    n_balance_deals: int = 0
    n_positions: int = 0
    n_active_traders: int = 0
    n_traders: int = 0
    n_depositors: int = 0

    # money
    deposits: float = 0.0
    withdrawals: float = 0.0
    net_deposits: float = 0.0
    closed_pnl: float = 0.0
    open_pnl: float = 0.0            # open P&L across all positions (current)
    volume_usd: float = 0.0
    abs_exposure: float = 0.0

    # per-symbol + per-group breakdown
    by_symbol: list[dict] = field(default_factory=list)
    by_group:  list[dict] = field(default_factory=list)


def compute_snapshot(
    bridge,
    day: date,
    group_mask: str = "CMV*",
    *,
    include_positions: bool = True,
) -> Snapshot:
    """Pull deals (+ positions) and compute every card value for `day`.

    `positions` are a live snapshot — not bounded by `day`. Callers who want
    EOD floating for a historical day need to supply a cached value.
    """
    start_utc, end_utc = day_bounds_utc(day)
    deals = bridge.get_deals_by_group(group_mask, start_utc, end_utc)
    positions = bridge.get_positions_by_group(group_mask) if include_positions else []

    # Map login → MT5 group (used for per-group breakdown).
    # We batch-fetch once since 400k users is a lot; fallback to per-login
    # lookup misses for logins not in our group mask.
    login_group: dict[int, str] = {}
    try:
        for u in bridge.get_users(group_mask):
            login_group[int(u["login"])] = u["group"]
    except Exception:
        pass

    def _group_of(login: int) -> str | None:
        g = login_group.get(login)
        if g:
            return g
        # cheap fallback: unknown
        return "Unknown"

    dep, wd, net = net_deposits(deals)
    n_active, n_any = trader_counts(deals)
    n_dep, _ = depositor_counts(deals)

    snap = Snapshot(
        label=day.strftime("%Y-%m-%d"),
        date=day.isoformat(),
        group_mask=group_mask,
        n_deals=len(deals),
        n_trade_deals=sum(1 for d in deals if is_trade_deal(d)),
        n_balance_deals=sum(1 for d in deals if is_balance_deal(d)),
        n_positions=len(positions),
        n_active_traders=n_active,
        n_traders=n_any,
        n_depositors=n_dep,
        deposits=dep,
        withdrawals=wd,
        net_deposits=net,
        closed_pnl=closed_pnl(deals),
        open_pnl=open_pnl(positions),
        volume_usd=volume_usd(deals),
        abs_exposure=total_abs_exposure(positions),
        by_symbol=[b.__dict__ for b in volume_by_symbol(deals)],
        by_group=[b.__dict__ for b in perf_by_group(deals, _group_of)],
    )
    return snap


def compute_month_snapshot(
    bridge, year: int, month: int, group_mask: str = "CMV*"
) -> Snapshot:
    """Same shape as compute_snapshot but over a full calendar month."""
    start_utc, end_utc = month_bounds_utc(year, month)
    deals = bridge.get_deals_by_group(group_mask, start_utc, end_utc)

    dep, wd, net = net_deposits(deals)
    n_active, n_any = trader_counts(deals)
    n_dep, _ = depositor_counts(deals)

    return Snapshot(
        label=f"{year:04d}-{month:02d}",
        date=f"{year:04d}-{month:02d}-01",
        group_mask=group_mask,
        n_deals=len(deals),
        n_trade_deals=sum(1 for d in deals if is_trade_deal(d)),
        n_balance_deals=sum(1 for d in deals if is_balance_deal(d)),
        n_positions=0,
        n_active_traders=n_active,
        n_traders=n_any,
        n_depositors=n_dep,
        deposits=dep,
        withdrawals=wd,
        net_deposits=net,
        closed_pnl=closed_pnl(deals),
        open_pnl=0.0,
        volume_usd=volume_usd(deals),
        abs_exposure=0.0,
        by_symbol=[b.__dict__ for b in volume_by_symbol(deals)],
    )
