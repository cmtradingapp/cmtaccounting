"""MT5 Manager API bridge via pythonnet.

Wraps the .NET assembly `MetaQuotes.MT5ManagerAPI64.dll` so Python can talk to
an MT5 Manager endpoint directly. Windows-only (the native DLL is 64-bit
Windows). All public methods return plain Python dicts / lists of dicts —
no .NET objects leak out.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

import clr  # type: ignore  # from pythonnet

SDK_LIBS = Path(os.environ.get("MT5_SDK_LIBS", r"C:\MetaTrader5SDK\Libs"))
if str(SDK_LIBS) not in sys.path:
    sys.path.insert(0, str(SDK_LIBS))
os.add_dll_directory(str(SDK_LIBS))  # native MT5APIManager64.dll resolver

clr.AddReference(str(SDK_LIBS / "MetaQuotes.MT5CommonAPI64.dll"))
clr.AddReference(str(SDK_LIBS / "MetaQuotes.MT5ManagerAPI64.dll"))

from MetaQuotes.MT5CommonAPI import MTRetCode, SMTTime  # type: ignore  # noqa: E402
from MetaQuotes.MT5ManagerAPI import (  # type: ignore  # noqa: E402
    CIMTManagerAPI,
    SMTManagerAPIFactory,
)
from System import DateTime, DateTimeKind  # type: ignore  # noqa: E402


PUMP_MODE_FULL = CIMTManagerAPI.EnPumpModes.PUMP_MODE_FULL
PUMP_MODE_NONE = CIMTManagerAPI.EnPumpModes.PUMP_MODE_NONE
DEFAULT_PUMP_MODE = PUMP_MODE_NONE  # login 1111 is rejected with PUMP_MODE_FULL
DEFAULT_TIMEOUT_MS = 30000


class MT5Error(RuntimeError):
    """Raised when a Manager API call returns a non-OK code."""


def _check(res, op: str) -> None:
    if res != MTRetCode.MT_RET_OK:
        raise MT5Error(f"{op} failed: {res}")


def _to_mt_time(dt: datetime) -> int:
    """Python datetime → MT5 SMTTime int64 seconds."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    net_dt = DateTime(
        dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, DateTimeKind.Utc
    )
    return SMTTime.FromDateTime(net_dt)


def _from_mt_time(ts: int) -> Optional[str]:
    """MT5 int64 seconds-since-epoch → ISO string (or None on zero)."""
    if not ts:
        return None
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()


class MT5Bridge:
    """One instance per (server, login). Not thread-safe."""

    def __init__(self) -> None:
        self._manager: Optional[CIMTManagerAPI] = None
        self._initialised = False

    # ── lifecycle ──────────────────────────────────────────────────────────
    def connect(self, server: str, login: int, password: str,
                timeout_ms: int = DEFAULT_TIMEOUT_MS,
                pump_mode=DEFAULT_PUMP_MODE) -> None:
        if not self._initialised:
            _check(SMTManagerAPIFactory.Initialize(str(SDK_LIBS)), "Initialize")
            self._initialised = True

        res_ref = MTRetCode.MT_RET_OK_NONE
        mgr, res_ref = SMTManagerAPIFactory.CreateManager(
            SMTManagerAPIFactory.ManagerAPIVersion, res_ref
        )
        if mgr is None or res_ref != MTRetCode.MT_RET_OK:
            SMTManagerAPIFactory.Shutdown()
            self._initialised = False
            raise MT5Error(f"CreateManager failed: {res_ref}")
        self._manager = mgr

        res = mgr.Connect(server, int(login), password, None, pump_mode, timeout_ms)
        if res != MTRetCode.MT_RET_OK:
            mgr.Dispose()
            self._manager = None
            SMTManagerAPIFactory.Shutdown()
            self._initialised = False
            raise MT5Error(f"Connect to {server} login={login} failed: {res}")

    def disconnect(self) -> None:
        if self._manager is not None:
            try:
                self._manager.Disconnect()
            finally:
                self._manager.Dispose()
                self._manager = None
        if self._initialised:
            SMTManagerAPIFactory.Shutdown()
            self._initialised = False

    def __enter__(self) -> "MT5Bridge":
        return self

    def __exit__(self, *_exc) -> None:
        self.disconnect()

    # ── helpers ────────────────────────────────────────────────────────────
    def _mgr(self) -> CIMTManagerAPI:
        if self._manager is None:
            raise MT5Error("Not connected — call connect() first")
        return self._manager

    @staticmethod
    def _user_dict(u) -> dict:
        return {
            "login":         int(u.Login()),
            "group":         str(u.Group()),
            "name":          str(u.Name()),
            "first_name":    str(u.FirstName()),
            "last_name":     str(u.LastName()),
            "country":       str(u.Country()),
            "email":         str(u.EMail()),
            "leverage":      int(u.Leverage()),
            "balance":       float(u.Balance()),
            "credit":        float(u.Credit()),
            "registration":  _from_mt_time(u.Registration()),
            "last_access":   _from_mt_time(u.LastAccess()),
            "balance_prev_day":   float(u.BalancePrevDay()),
            "equity_prev_day":    float(u.EquityPrevDay()),
            "balance_prev_month": float(u.BalancePrevMonth()),
            "equity_prev_month":  float(u.EquityPrevMonth()),
            "comment":       str(u.Comment()),
            "agent":         int(u.Agent()),
            "lead_campaign": str(u.LeadCampaign()),
            "lead_source":   str(u.LeadSource()),
        }

    @staticmethod
    def _deal_dict(d) -> dict:
        return {
            "deal":        int(d.Deal()),
            "external_id": str(d.ExternalID()),
            "login":       int(d.Login()),
            "order":       int(d.Order()),
            "position_id": int(d.PositionID()),
            "action":      int(d.Action()),   # 0=buy 1=sell 2=balance 3=credit ...
            "entry":       int(d.Entry()),    # 0=in 1=out 2=inout 3=out_by
            "reason":      int(d.Reason()),
            "time":        _from_mt_time(d.Time()),
            "time_msc":    int(d.TimeMsc()),
            "symbol":      str(d.Symbol()),
            "price":       float(d.Price()),
            "volume":      int(d.Volume()),
            "volume_ext":  int(d.VolumeExt()),
            "contract_size": float(d.ContractSize()),
            "profit":      float(d.Profit()),
            "profit_raw":  float(d.ProfitRaw()),
            "storage":     float(d.Storage()),
            "commission":  float(d.Commission()),
            "fee":         float(d.Fee()),
            "rate_profit": float(d.RateProfit()),
            "rate_margin": float(d.RateMargin()),
            "comment":     str(d.Comment()),
            "tick_value":  float(d.TickValue()),
            "tick_size":   float(d.TickSize()),
            "market_bid":  float(d.MarketBid()),
            "market_ask":  float(d.MarketAsk()),
            "value":       float(d.Value()),
        }

    @staticmethod
    def _position_dict(p) -> dict:
        return {
            "login":         int(p.Login()),
            "symbol":        str(p.Symbol()),
            "action":        int(p.Action()),   # 0=buy, 1=sell
            "position_id":   int(p.Position()),
            "time_create":   _from_mt_time(p.TimeCreate()),
            "time_update":   _from_mt_time(p.TimeUpdate()),
            "price_open":    float(p.PriceOpen()),
            "price_current": float(p.PriceCurrent()),
            "volume":        int(p.Volume()),
            "volume_ext":    int(p.VolumeExt()),
            "contract_size": float(p.ContractSize()),
            "profit":        float(p.Profit()),
            "storage":       float(p.Storage()),
            "rate_profit":   float(p.RateProfit()),
            "rate_margin":   float(p.RateMargin()),
            "comment":       str(p.Comment()),
            "external_id":   str(p.ExternalID()),
        }

    @staticmethod
    def _account_dict(a) -> dict:
        return {
            "login":      int(a.Login()),
            "balance":    float(a.Balance()),
            "credit":     float(a.Credit()),
            "margin":     float(a.Margin()),
            "margin_free": float(a.MarginFree()),
            "margin_level": float(a.MarginLevel()),
            "profit":     float(a.Profit()),
            "floating":   float(a.Floating()),
            "equity":     float(a.Equity()),
            "storage":    float(a.Storage()),
            "assets":     float(a.Assets()),
            "liabilities": float(a.Liabilities()),
        }

    # ── currency helpers ───────────────────────────────────────────────────
    # Known non-USD ISO codes that appear as group name suffixes on this broker.
    _NON_USD_CURRENCIES = {"ZAR", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD",
                           "NZD", "KES", "NGN", "GHS", "ZMW"}

    def group_currency(self, group_name: str) -> str:
        """Return the deposit currency for a group name.

        Tries `GroupRequest` first (authoritative). Falls back to suffix
        matching (CMV3ZAR -> ZAR, CMV3US -> USD, CMV3USIS -> USD).
        """
        if not group_name:
            return "USD"
        mgr = self._mgr()
        try:
            grp = mgr.GroupCreate()
            try:
                res = mgr.GroupRequest(group_name, grp)
                if res == MTRetCode.MT_RET_OK:
                    c = str(grp.Currency())
                    if c:
                        return c
            finally:
                grp.Dispose()
        except Exception:
            pass
        # Heuristic fallback: check ISO suffix (e.g. CMV3ZAR -> ZAR)
        upper = group_name.upper()
        for code in self._NON_USD_CURRENCIES:
            if upper.endswith(code):
                return code
        return "USD"

    def get_users_by_logins(self, logins: list[int]) -> list[dict]:
        """Batch fetch users for the given list of login IDs."""
        from System import UInt64
        mgr = self._mgr()
        net_arr = [UInt64(l) for l in logins]
        arr = mgr.UserCreateArray()
        try:
            _check(mgr.UserRequestByLogins(net_arr, arr),
                   f"UserRequestByLogins({len(logins)} logins)")
            return [self._user_dict(arr.Next(i)) for i in range(arr.Total())]
        finally:
            arr.Dispose()

    # ── fetch APIs ─────────────────────────────────────────────────────────
    def get_user(self, login: int) -> dict:
        mgr = self._mgr()
        u = mgr.UserCreate()
        try:
            _check(mgr.UserRequest(int(login), u), f"UserRequest({login})")
            return self._user_dict(u)
        finally:
            u.Dispose()

    def get_users(self, group_mask: str = "*") -> list[dict]:
        mgr = self._mgr()
        arr = mgr.UserCreateArray()
        try:
            _check(mgr.UserRequestArray(group_mask, arr), f"UserRequestArray({group_mask!r})")
            return [self._user_dict(arr.Next(i)) for i in range(arr.Total())]
        finally:
            arr.Dispose()

    def get_user_logins(self, group_mask: str = "*") -> list[int]:
        """Just the login IDs. Uses UserRequestArray under the hood because the
        native `UserLogins` out-param signature doesn't marshal through pythonnet."""
        mgr = self._mgr()
        arr = mgr.UserCreateArray()
        try:
            _check(mgr.UserRequestArray(group_mask, arr), f"UserRequestArray({group_mask!r})")
            return [int(arr.Next(i).Login()) for i in range(arr.Total())]
        finally:
            arr.Dispose()

    def get_deals(self, login: int, time_from: datetime, time_to: datetime) -> list[dict]:
        mgr = self._mgr()
        arr = mgr.DealCreateArray()
        try:
            _check(
                mgr.DealRequest(int(login), _to_mt_time(time_from), _to_mt_time(time_to), arr),
                f"DealRequest({login})",
            )
            return [self._deal_dict(arr.Next(i)) for i in range(arr.Total())]
        finally:
            arr.Dispose()

    def get_deals_by_group(self, group_mask: str, time_from: datetime, time_to: datetime) -> list[dict]:
        mgr = self._mgr()
        arr = mgr.DealCreateArray()
        try:
            _check(
                mgr.DealRequestByGroup(group_mask, _to_mt_time(time_from), _to_mt_time(time_to), arr),
                f"DealRequestByGroup({group_mask!r})",
            )
            return [self._deal_dict(arr.Next(i)) for i in range(arr.Total())]
        finally:
            arr.Dispose()

    def get_positions(self, login: int) -> list[dict]:
        mgr = self._mgr()
        arr = mgr.PositionCreateArray()
        try:
            _check(mgr.PositionRequest(int(login), arr), f"PositionRequest({login})")
            return [self._position_dict(arr.Next(i)) for i in range(arr.Total())]
        finally:
            arr.Dispose()

    def get_positions_by_group(self, group_mask: str) -> list[dict]:
        mgr = self._mgr()
        arr = mgr.PositionCreateArray()
        try:
            _check(
                mgr.PositionRequestByGroup(group_mask, arr),
                f"PositionRequestByGroup({group_mask!r})",
            )
            return [self._position_dict(arr.Next(i)) for i in range(arr.Total())]
        finally:
            arr.Dispose()

    def get_account(self, login: int) -> dict:
        mgr = self._mgr()
        a = mgr.UserCreateAccount()
        try:
            _check(mgr.UserAccountRequest(int(login), a), f"UserAccountRequest({login})")
            return self._account_dict(a)
        finally:
            a.Dispose()

    def get_version(self) -> str:
        try:
            return str(SMTManagerAPIFactory.ManagerAPIVersion)
        except Exception:
            return "unknown"
