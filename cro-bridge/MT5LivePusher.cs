// MT5 Manager API -> continuous JSON line pusher.
// Connects once, stays connected; outputs one JSON line per CRO_INTERVAL seconds.
// v3: FX conversion via Mt5MonitorApiBundle (proper bid/ask PositiveToUsd/NegativeToUsd per sign,
//     replacing the mid-rate heuristic). RateProfit is only used as a last-resort fallback.
//
// Two-speed loop:
//   Fast (every CRO_INTERVAL):  positions + today's deals + per-group floating PnL
//   Slow (every ~60s):          monthly deals (MTD totals), user balance/credit/equity, FTD
//
// Env vars: MT5_SERVER, MT5_LOGIN, MT5_PASSWORD, CRO_GROUP, MT5_SDK_LIBS
//           CRO_INTERVAL (seconds, default 5)   -- fast loop cadence
//           CRO_SLOW_EVERY (cycles, default 12) -- run slow loop every N fast cycles

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Threading;
using MetaQuotes.MT5CommonAPI;
using MetaQuotes.MT5ManagerAPI;
using Mt5Monitor.Api;

public class MT5LivePusher
{
    const uint ENTRY_IN       = 0;
    const uint ACTION_BUY     = 0;
    const uint ACTION_SELL    = 1;
    const uint ACTION_BALANCE = 2;

    static string JsonEscape(string s)
    {
        if (s == null) return "";
        var sb = new StringBuilder(s.Length + 4);
        foreach (char c in s)
        {
            switch (c)
            {
                case '\\': sb.Append("\\\\"); break;
                case '"':  sb.Append("\\\""); break;
                case '\b': sb.Append("\\b");  break;
                case '\f': sb.Append("\\f");  break;
                case '\n': sb.Append("\\n");  break;
                case '\r': sb.Append("\\r");  break;
                case '\t': sb.Append("\\t");  break;
                default:
                    if (c < 0x20) sb.AppendFormat("\\u{0:x4}", (int)c);
                    else sb.Append(c);
                    break;
            }
        }
        return sb.ToString();
    }

    // Last-resort fallback when no live FX rate exists (RateProfit is entry-time, stale).
    static double ToUsd(double native, double rate)
    {
        if (rate > 1.5 && rate != 0) return native / rate;
        return native;
    }

    // Sign-aware conversion using proper bid/ask rates from Mt5MonitorApiBundle.
    // Positive amounts use PositiveToUsd (e.g. 1/Ask for USDxxx), negative use NegativeToUsd.
    static double ApplyRate(double native, Mt5UsdConversionRate r)
    {
        if (native == 0.0) return 0.0;
        double rate = native >= 0.0 ? r.PositiveToUsd : r.NegativeToUsd;
        return rate > 0.0 ? native * rate : 0.0;
    }

    // Compute notional USD for a deal.
    // For xxxUSD pairs: lots * contractSize * price.
    // For others (USDxxx, XAU, etc.): lots * contractSize (already base-currency units).
    static double NotionalUsd(double volumeLots, double contractSize, double price, string symbol)
    {
        string su = (symbol ?? "").ToUpperInvariant();
        if (su.StartsWith("USD") && su.Length > 3 && su[3] != 'X') // USDxxx (not USDX index)
            return Math.Abs(volumeLots * contractSize);
        if (su.EndsWith("USD") || su == "XAUUSD" || su == "XAGUSD")
            return Math.Abs(volumeLots * contractSize * price);
        // Fallback: treat as USD-quoted
        return Math.Abs(volumeLots * contractSize * price);
    }

    class SymAgg
    {
        public string symbol;
        public int nDeals;
        public HashSet<ulong> traders = new HashSet<ulong>();
        public double notionalUsd, notionalBuy, notionalSell, swap, commission, pnl;
    }

    struct GrpAgg
    {
        public string group;
        public int    nPositions;
        public double floatingPnl;
        public string currency;
    }

    // Per-deposit-currency closed PnL bucket (all amounts in deposit currency)
    struct CcyAgg
    {
        public double profit, swap, commission, usdTotal;
    }

    static Dictionary<string, string> LoadGroupCurrencies(CIMTManagerAPI mgr, string mask)
    {
        var d = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var arr = mgr.GroupCreateArray();
        if (mgr.GroupRequestArray(mask, arr) == MTRetCode.MT_RET_OK)
            for (uint i = 0; i < arr.Total(); i++)
            {
                var g = arr.Next(i);
                string name = g.Group() ?? "";
                if (!string.IsNullOrEmpty(name))
                    d[name] = g.Currency() ?? "USD";
            }
        arr.Release();
        return d;
    }

    static CIMTManagerAPI Connect(string server, ulong login, string pw)
    {
        MTRetCode cm;
        CIMTManagerAPI mgr = SMTManagerAPIFactory.CreateManager(
            SMTManagerAPIFactory.ManagerAPIVersion, out cm);
        if (mgr == null || cm != MTRetCode.MT_RET_OK)
        {
            Console.Error.WriteLine("[pusher] CreateManager: " + cm);
            return null;
        }
        MTRetCode cr = mgr.Connect(server, login, pw, null,
            CIMTManagerAPI.EnPumpModes.PUMP_MODE_POSITIONS, 30000);
        if (cr != MTRetCode.MT_RET_OK)
        {
            Console.Error.WriteLine("[pusher] Connect: " + cr);
            try { mgr.Disconnect(); } catch { }
            mgr.Dispose();
            return null;
        }
        Console.Error.WriteLine("[pusher] Waiting for position pump...");
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (DateTime.UtcNow < deadline)
        {
            var t = mgr.PositionCreateArray();
            bool ready = mgr.PositionGetByGroup("*", t) == MTRetCode.MT_RET_OK && t.Total() > 0;
            t.Dispose();
            if (ready) break;
            Thread.Sleep(500);
        }
        Console.Error.WriteLine("[pusher] Pump ready.");
        return mgr;
    }

    // ── Login → deposit currency snapshot (populated by slow loop) ───────────
    // Reference assignment is atomic on x86/x64, so no lock needed for reads.
    static volatile Dictionary<ulong, string> _loginCcySnap =
        new Dictionary<ulong, string>();

    // ── Login → group name snapshot (for per-group closed PnL + exclusions) ──
    static volatile Dictionary<ulong, string> _loginGrpSnap =
        new Dictionary<ulong, string>();

    // ── Groups to exclude from PnL totals (internal/hedge accounts) ──────────
    static readonly HashSet<string> _excludeGroups =
        new HashSet<string>(StringComparer.OrdinalIgnoreCase);

    // ── FTD tracking ──────────────────────────────────────────────────────────
    // Seed once at startup with YTD known depositors; then track incrementally.
    static readonly HashSet<ulong> _knownDepositors = new HashSet<ulong>();
    static bool   _knownDepLoaded  = false;
    static string _knownDepDate    = "";   // last date depositors were merged
    static int    _ftdToday        = 0;

    static void SeedKnownDepositors(CIMTManagerAPI mgr, string group, DateTime dayStart,
                                    CultureInfo ci)
    {
        var yearStart = new DateTime(dayStart.Year, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var yesterday = dayStart.AddSeconds(-1);
        if (yesterday < yearStart) { _knownDepLoaded = true; return; }

        Console.Error.WriteLine("[pusher] Seeding FTD known-depositors from YTD deals...");
        var arr = mgr.DealCreateArray();
        var res = mgr.DealRequestByGroup(group,
            SMTTime.FromDateTime(yearStart), SMTTime.FromDateTime(yesterday), arr);
        if (res == MTRetCode.MT_RET_OK)
        {
            for (uint i = 0; i < arr.Total(); i++)
            {
                var d = arr.Next(i);
                if (d.Action() != ACTION_BALANCE) continue;
                string c = (d.Comment() ?? "").ToLowerInvariant();
                if (d.Profit() > 0 && !c.Contains("bonus") && !c.Contains("internal") && !c.Contains("transfer"))
                    _knownDepositors.Add(d.Login());
            }
        }
        arr.Dispose();
        _knownDepLoaded = true;
        Console.Error.WriteLine("[pusher] FTD seed done: " + _knownDepositors.Count + " known depositors YTD.");
    }

    static void UpdateFtd(HashSet<ulong> todayDepositors, string todayStr)
    {
        if (_knownDepDate == todayStr) return; // already ran for today
        _ftdToday = 0;
        foreach (var dep in todayDepositors)
            if (!_knownDepositors.Contains(dep)) _ftdToday++;
        foreach (var dep in todayDepositors)
            _knownDepositors.Add(dep);
        _knownDepDate = todayStr;
    }

    // ── Slow-loop result struct ────────────────────────────────────────────────
    struct SlowData
    {
        public double totalBalance, totalCredit;
        // MTD deal aggregates
        public double mthClosedPnl, mthNetDeps, mthDeps, mthWds, mthCob, mthVol;
        public double mthSwap, mthCommission;
        public int    mthNTraders, mthNActive, mthNDeps;
        // Per-day closed PnL for trend table (date → closed PnL USD)
        public SortedDictionary<string, double> closedPnlByDay;
        // Per-deposit-currency MTD closed PnL
        public Dictionary<string, CcyAgg> mthByCcy;
    }

    static SlowData RunSlowLoop(CIMTManagerAPI mgr, string group,
                                IDictionary<string, Mt5UsdConversionRate> ccyRates,
                                Dictionary<string, string> groupCcyRef,
                                DateTime monthStart, DateTime nowDt,
                                HashSet<ulong> todayDeps, string todayStr,
                                CultureInfo ci)
    {
        var sd = new SlowData();

        // -- account totals: balance + credit (USD) ---------------------------
        // equity (live) = totalBalance + totalCredit + floatingPnl
        // wd_equity      = max(0, equity - totalCredit - cob)
        //                = max(0, totalBalance + floatingPnl - cob)
        // IMTUser has Group() but not Currency() -- look up currency via groupCcy dict.
        var userArr = mgr.UserCreateArray();
        var newLoginCcy = new Dictionary<ulong, string>();
        var newLoginGrp = new Dictionary<ulong, string>();
        if (mgr.UserRequestArray(group, userArr) == MTRetCode.MT_RET_OK)
        {
            newLoginCcy = new Dictionary<ulong, string>((int)userArr.Total());
            newLoginGrp = new Dictionary<ulong, string>((int)userArr.Total());
            for (uint i = 0; i < userArr.Total(); i++)
            {
                var u = userArr.Next(i);
                string grpName = u.Group() ?? "";
                string ccy;
                if (!groupCcyRef.TryGetValue(grpName, out ccy)) ccy = "USD";
                Mt5UsdConversionRate cRate;
                if (ccyRates.TryGetValue(ccy, out cRate))
                {
                    sd.totalBalance += ApplyRate(u.Balance(), cRate);
                    sd.totalCredit  += ApplyRate(u.Credit(),  cRate);
                }
                else
                {
                    sd.totalBalance += u.Balance();
                    sd.totalCredit  += u.Credit();
                }
                newLoginCcy[u.Login()] = ccy;
                newLoginGrp[u.Login()] = grpName;
            }
            _loginCcySnap = newLoginCcy;  // atomic reference swap
            _loginGrpSnap = newLoginGrp;
        }
        userArr.Release();

        // -- MTD deals --------------------------------------------------------
        var mArr = mgr.DealCreateArray();
        var mRes = mgr.DealRequestByGroup(group,
            SMTTime.FromDateTime(monthStart), SMTTime.FromDateTime(nowDt), mArr);
        var byDay    = new SortedDictionary<string, double>(StringComparer.Ordinal);
        var mthByCcy = new Dictionary<string, CcyAgg>(StringComparer.Ordinal);
        // Use the freshly-built loginCcy/Grp mappings (populated above)
        var lcSnap    = newLoginCcy.Count > 0 ? newLoginCcy : _loginCcySnap;
        var lcGrpSnap = newLoginGrp.Count > 0 ? newLoginGrp : _loginGrpSnap;
        if (mRes == MTRetCode.MT_RET_OK)
        {
            var mTraders = new HashSet<ulong>();
            var mActive  = new HashSet<ulong>();
            var mDeps    = new HashSet<ulong>();
            for (uint i = 0; i < mArr.Total(); i++)
            {
                var    d      = mArr.Next(i);
                uint   action = d.Action();
                ulong  mLogin = d.Login();

                // Skip excluded groups (internal/hedge accounts)
                string mGrp; if (!lcGrpSnap.TryGetValue(mLogin, out mGrp)) mGrp = "";
                if (mGrp.Length > 0 && _excludeGroups.Contains(mGrp)) continue;

                if (action == ACTION_BUY || action == ACTION_SELL)
                {
                    mTraders.Add(mLogin);
                    if (d.Entry() == ENTRY_IN)
                        mActive.Add(mLogin);
                    else
                    {
                        // Currency-aware USD conversion using proper bid/ask rates
                        string mCcy; if (!lcSnap.TryGetValue(mLogin, out mCcy)) mCcy = "USD";
                        Mt5UsdConversionRate mRate; bool hasMRate = ccyRates.TryGetValue(mCcy, out mRate);
                        double mProfitUsd = hasMRate ? ApplyRate(d.Profit(),     mRate) : d.Profit();
                        double mSwapUsd   = hasMRate ? ApplyRate(d.Storage(),    mRate) : d.Storage();
                        double mCommUsd   = hasMRate ? ApplyRate(d.Commission(), mRate) : d.Commission();
                        double cpDay      = mProfitUsd + mSwapUsd + mCommUsd;
                        sd.mthClosedPnl  += cpDay;
                        sd.mthSwap       += mSwapUsd;
                        sd.mthCommission += mCommUsd;
                        // per-day breakdown
                        string dayStr = SMTTime.ToDateTime(d.Time()).ToString("yyyy-MM-dd", ci);
                        double ex; byDay.TryGetValue(dayStr, out ex);
                        byDay[dayStr] = ex + cpDay;
                        // per-currency breakdown (native amounts stay in deposit currency)
                        CcyAgg ca; mthByCcy.TryGetValue(mCcy, out ca);
                        ca.profit     += d.Profit();
                        ca.swap       += d.Storage();
                        ca.commission += d.Commission();
                        ca.usdTotal   += cpDay;
                        mthByCcy[mCcy] = ca;
                    }
                    double lots = d.Volume() / 100.0;
                    sd.mthVol += NotionalUsd(lots, d.ContractSize(), d.Price(), d.Symbol());
                }
                else if (action == ACTION_BALANCE)
                {
                    string bCcy; if (!lcSnap.TryGetValue(mLogin, out bCcy)) bCcy = "USD";
                    Mt5UsdConversionRate bRate;
                    double amt = ccyRates.TryGetValue(bCcy, out bRate)
                        ? ApplyRate(d.Profit(), bRate)
                        : ToUsd(d.Profit(), d.RateProfit());
                    string comment = (d.Comment() ?? "").ToLowerInvariant();
                    if (comment.Contains("bonus"))
                    {
                        sd.mthCob += amt;   // COB: bonus balance for WD Equity Z
                        continue;
                    }
                    if (comment.Contains("internal") || comment.Contains("transfer"))
                        continue;
                    if (amt > 0) { sd.mthDeps += amt; mDeps.Add(mLogin); }
                    else           sd.mthWds  += amt;
                }
            }
            sd.mthNTraders = mTraders.Count;
            sd.mthNActive  = mActive.Count;
            sd.mthNDeps    = mDeps.Count;
            sd.mthNetDeps  = sd.mthDeps + sd.mthWds;
        }
        mArr.Dispose();
        sd.closedPnlByDay = byDay;
        sd.mthByCcy       = mthByCcy;

        // -- FTD update -------------------------------------------------------
        if (!_knownDepLoaded)
            SeedKnownDepositors(mgr, group, monthStart.Date, ci);
        UpdateFtd(todayDeps, todayStr);

        return sd;
    }

    public static int Main(string[] args)
    {
        var ci       = CultureInfo.InvariantCulture;
        var server   = Environment.GetEnvironmentVariable("MT5_SERVER")   ?? "";
        var loginStr = Environment.GetEnvironmentVariable("MT5_LOGIN")    ?? "0";
        var pw       = Environment.GetEnvironmentVariable("MT5_PASSWORD") ?? "";
        var group    = Environment.GetEnvironmentVariable("CRO_GROUP")    ?? "CMV*";
        var sdkDir   = Environment.GetEnvironmentVariable("MT5_SDK_LIBS") ?? "Z:/app";
        int interval    = int.Parse(Environment.GetEnvironmentVariable("CRO_INTERVAL")   ?? "5",  ci);
        int slowEvery   = int.Parse(Environment.GetEnvironmentVariable("CRO_SLOW_EVERY") ?? "12", ci);

        if (string.IsNullOrEmpty(server) || string.IsNullOrEmpty(pw))
        {
            Console.Error.WriteLine("[pusher] MT5_SERVER + MT5_PASSWORD required.");
            return 2;
        }
        ulong login = ulong.Parse(loginStr, ci);

        // Parse excluded groups (comma-separated group names to omit from PnL totals)
        {
            var raw = Environment.GetEnvironmentVariable("CRO_EXCLUDE_GROUPS") ?? "";
            foreach (var s in raw.Split(','))
            { var t = s.Trim(); if (t.Length > 0) _excludeGroups.Add(t); }
            if (_excludeGroups.Count > 0)
                Console.Error.WriteLine("[pusher] excluding groups: " +
                    string.Join(", ", _excludeGroups));
        }

        var initRes = SMTManagerAPIFactory.Initialize(sdkDir);
        if (initRes != MTRetCode.MT_RET_OK)
        {
            Console.Error.WriteLine("[pusher] Initialize failed: " + initRes);
            return 3;
        }

        Console.Error.WriteLine("[pusher] group=" + group + "  interval=" + interval +
                                "s  slow_every=" + slowEvery + " cycles");

        CIMTManagerAPI mgr = null;
        while (mgr == null) { mgr = Connect(server, login, pw); if (mgr == null) Thread.Sleep(5000); }

        var groupCcy         = LoadGroupCurrencies(mgr, group);
        var ccyRates         = Mt5UsdRateLoader.LoadLiveRates(mgr);
        int rateRefreshCycle = 0;
        int slowCycle        = slowEvery; // trigger slow loop on first iteration
        Console.Error.WriteLine("[pusher] " + groupCcy.Count + " groups, " + ccyRates.Count + " FX rates.");

        // Slow-loop cached values (populated every ~60s, zero until first slow run)
        var slow = new SlowData();

        while (true)
        {
            try
            {
                if (++rateRefreshCycle >= 60) { ccyRates = Mt5UsdRateLoader.LoadLiveRates(mgr); rateRefreshCycle = 0; }

                DateTime nowDt      = DateTime.UtcNow;
                DateTime dayStartDt = nowDt.Date;
                string   todayStr   = dayStartDt.ToString("yyyy-MM-dd", ci);
                DateTime monthStart = new DateTime(nowDt.Year, nowDt.Month, 1, 0, 0, 0, DateTimeKind.Utc);

                // ── FAST: per-group positions ─────────────────────────────────
                int    nPositions  = 0;
                double floatingPnl = 0.0;
                var    byGroupList = new List<GrpAgg>(groupCcy.Count);

                foreach (var kv in groupCcy)
                {
                    Mt5UsdConversionRate grpRate; bool hasRate = ccyRates.TryGetValue(kv.Value, out grpRate);
                    var posArr = mgr.PositionCreateArray();
                    if (mgr.PositionGetByGroup(kv.Key, posArr) != MTRetCode.MT_RET_OK)
                    {
                        posArr.Dispose();
                        throw new Exception("PositionGetByGroup failed for " + kv.Key);
                    }
                    int    grpPos   = (int)posArr.Total();
                    double grpFloat = 0.0;
                    for (uint i = 0; i < posArr.Total(); i++)
                    {
                        var p = posArr.Next(i);
                        if (hasRate)
                            grpFloat += ApplyRate(p.Profit(), grpRate) + ApplyRate(p.Storage(), grpRate);
                        else
                            grpFloat += ToUsd(p.Profit() + p.Storage(), p.RateProfit());
                    }
                    posArr.Dispose();
                    nPositions  += grpPos;
                    floatingPnl += grpFloat;
                    if (grpPos > 0)
                        byGroupList.Add(new GrpAgg {
                            group = kv.Key, nPositions = grpPos,
                            floatingPnl = grpFloat, currency = kv.Value
                        });
                }

                // ── FAST: today's deals ───────────────────────────────────────
                int    nClosingDeals = 0;
                double closedPnl = 0, volumeUsd = 0, swap = 0, commission = 0;
                double deposits = 0, withdrawals = 0;
                var tradersAny    = new HashSet<ulong>();
                var tradersActive = new HashSet<ulong>();
                var depositors    = new HashSet<ulong>();
                var bySymbol      = new Dictionary<string, SymAgg>();
                var closedByCcy   = new Dictionary<string, CcyAgg>(StringComparer.Ordinal);
                var closedByGrp   = new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase);
                var lcFast        = _loginCcySnap; // snapshot reference — no lock needed
                var lcGrp         = _loginGrpSnap;

                var dealArr = mgr.DealCreateArray();
                if (mgr.DealRequestByGroup(group,
                    SMTTime.FromDateTime(dayStartDt), SMTTime.FromDateTime(nowDt),
                    dealArr) == MTRetCode.MT_RET_OK)
                {
                    for (uint i = 0; i < dealArr.Total(); i++)
                    {
                        var    d      = dealArr.Next(i);
                        uint   action = d.Action();
                        ulong  dLogin = d.Login();

                        // Resolve group name (for exclusion filter + per-group PnL)
                        string dGrp; if (!lcGrp.TryGetValue(dLogin, out dGrp)) dGrp = "";
                        bool excluded = dGrp.Length > 0 && _excludeGroups.Contains(dGrp);

                        if (action == ACTION_BUY || action == ACTION_SELL)
                        {
                            if (d.Entry() != ENTRY_IN)
                            {
                                // Currency-aware USD conversion using proper bid/ask rates
                                string dCcy; if (!lcFast.TryGetValue(dLogin, out dCcy)) dCcy = "USD";
                                Mt5UsdConversionRate dRate; bool hasDRate = ccyRates.TryGetValue(dCcy, out dRate);
                                double dUsd = hasDRate
                                    ? ApplyRate(d.Profit(), dRate) + ApplyRate(d.Storage(), dRate) + ApplyRate(d.Commission(), dRate)
                                    : d.Profit() + d.Storage() + d.Commission();
                                // Always track per-group (shows all groups including excluded ones)
                                double gv; closedByGrp.TryGetValue(dGrp, out gv);
                                closedByGrp[dGrp] = gv + dUsd;
                                if (!excluded)
                                {
                                    closedPnl += dUsd;
                                    nClosingDeals++;
                                    CcyAgg dca; closedByCcy.TryGetValue(dCcy, out dca);
                                    dca.profit     += d.Profit();
                                    dca.swap       += d.Storage();
                                    dca.commission += d.Commission();
                                    dca.usdTotal   += dUsd;
                                    closedByCcy[dCcy] = dca;
                                }
                            }
                            // Skip trader/volume/symbol stats for excluded groups
                            if (excluded) continue;
                            tradersAny.Add(dLogin);
                            if (d.Entry() == ENTRY_IN)
                                tradersActive.Add(dLogin);
                            double lots = d.Volume() / 100.0;
                            double dn   = NotionalUsd(lots, d.ContractSize(), d.Price(), d.Symbol());
                            volumeUsd  += dn;
                            {
                                string sCcy; if (!lcFast.TryGetValue(dLogin, out sCcy)) sCcy = "USD";
                                Mt5UsdConversionRate sRate; bool hasSRate = ccyRates.TryGetValue(sCcy, out sRate);
                                swap       += hasSRate ? ApplyRate(d.Storage(),    sRate) : d.Storage();
                                commission += hasSRate ? ApplyRate(d.Commission(), sRate) : d.Commission();
                            }
                            string sym = d.Symbol();
                            if (!string.IsNullOrEmpty(sym))
                            {
                                string symCcy; if (!lcFast.TryGetValue(dLogin, out symCcy)) symCcy = "USD";
                                Mt5UsdConversionRate symRate; bool hasSymRate = ccyRates.TryGetValue(symCcy, out symRate);
                                SymAgg sa;
                                if (!bySymbol.TryGetValue(sym, out sa))
                                    { sa = new SymAgg { symbol = sym }; bySymbol[sym] = sa; }
                                sa.nDeals++;
                                sa.traders.Add(dLogin);
                                sa.notionalUsd  += dn;
                                if (action == ACTION_BUY) sa.notionalBuy += dn;
                                else                      sa.notionalSell += dn;
                                sa.swap       += hasSymRate ? ApplyRate(d.Storage(),    symRate) : d.Storage();
                                sa.commission += hasSymRate ? ApplyRate(d.Commission(), symRate) : d.Commission();
                                sa.pnl        += hasSymRate
                                    ? ApplyRate(d.Profit(), symRate) + ApplyRate(d.Storage(), symRate) + ApplyRate(d.Commission(), symRate)
                                    : d.Profit() + d.Storage() + d.Commission();
                            }
                        }
                        else if (action == ACTION_BALANCE)
                        {
                            if (excluded) continue;
                            string fCcy; if (!lcFast.TryGetValue(dLogin, out fCcy)) fCcy = "USD";
                            Mt5UsdConversionRate fRate;
                            double amt = ccyRates.TryGetValue(fCcy, out fRate)
                                ? ApplyRate(d.Profit(), fRate)
                                : ToUsd(d.Profit(), d.RateProfit());
                            string comment = (d.Comment() ?? "").ToLowerInvariant();
                            if (comment.Contains("bonus") || comment.Contains("internal") || comment.Contains("transfer"))
                                continue;
                            if (amt > 0) { deposits += amt; depositors.Add(dLogin); }
                            else           withdrawals += amt;
                        }
                    }
                }
                dealArr.Dispose();

                // ── Sanity guard ─────────────────────────────────────────────
                if (nPositions == 0 && tradersAny.Count == 0)
                {
                    Console.Error.WriteLine("[pusher] sanity: 0 positions + 0 traders -- skipping");
                    Thread.Sleep(interval * 1000);
                    continue;
                }

                // ── SLOW loop ─────────────────────────────────────────────────
                if (++slowCycle >= slowEvery)
                {
                    slowCycle = 0;
                    try
                    {
                        slow = RunSlowLoop(mgr, group, ccyRates, groupCcy,
                                           monthStart, nowDt, depositors, todayStr, ci);
                        Console.Error.WriteLine(
                            "[pusher] slow: bal=" + slow.totalBalance.ToString("N0", ci) +
                            " cred=" + slow.totalCredit.ToString("N0", ci) +
                            " mth_closed=" + slow.mthClosedPnl.ToString("N0", ci) +
                            " ftd=" + _ftdToday);
                    }
                    catch (Exception ex)
                    {
                        Console.Error.WriteLine("[pusher] slow loop error (non-fatal): " + ex.Message);
                    }
                }

                // ── Derived values ────────────────────────────────────────────
                // equity (live) = balance + credit + floating (standard MT5 formula)
                double totalEquity = slow.totalBalance + slow.totalCredit + floatingPnl;
                // wd_equity = max(0, equity - credit - cob) = max(0, balance + floating - cob)
                double wdEquity = Math.Max(0.0, slow.totalBalance + floatingPnl - slow.mthCob);

                // ── Emit JSON ─────────────────────────────────────────────────
                string pushedAt = nowDt.ToString("yyyy-MM-ddTHH:mm:ss.fffZ", ci);
                var sb = new StringBuilder(8192);
                sb.Append("{");
                // --- live / fast fields ---
                sb.Append("\"floating_pnl_usd\":").Append(floatingPnl.ToString("G17", ci));
                sb.Append(",\"closed_pnl_usd\":").Append(closedPnl.ToString("G17", ci));
                sb.Append(",\"n_positions\":").Append(nPositions.ToString(ci));
                sb.Append(",\"n_closing_deals\":").Append(nClosingDeals.ToString(ci));
                sb.Append(",\"volume_usd\":").Append(volumeUsd.ToString("G17", ci));
                sb.Append(",\"swap\":").Append(swap.ToString("G17", ci));
                sb.Append(",\"commission\":").Append(commission.ToString("G17", ci));
                sb.Append(",\"net_deposits\":").Append((deposits + withdrawals).ToString("G17", ci));
                sb.Append(",\"deposits\":").Append(deposits.ToString("G17", ci));
                sb.Append(",\"withdrawals\":").Append(withdrawals.ToString("G17", ci));
                sb.Append(",\"n_traders\":").Append(tradersAny.Count.ToString(ci));
                sb.Append(",\"n_active_traders\":").Append(tradersActive.Count.ToString(ci));
                sb.Append(",\"n_depositors\":").Append(depositors.Count.ToString(ci));
                sb.Append(",\"n_ftd\":").Append(_ftdToday.ToString(ci));
                // --- closed_pnl_by_ccy (sorted by abs usdTotal desc) ---
                var ccyList = new List<KeyValuePair<string, CcyAgg>>(closedByCcy);
                ccyList.Sort(delegate(KeyValuePair<string, CcyAgg> a, KeyValuePair<string, CcyAgg> b) {
                    return Math.Abs(b.Value.usdTotal).CompareTo(Math.Abs(a.Value.usdTotal));
                });
                sb.Append(",\"closed_pnl_by_ccy\":[");
                for (int i = 0; i < ccyList.Count; i++)
                {
                    var kv = ccyList[i];
                    if (i > 0) sb.Append(",");
                    sb.Append("{\"ccy\":\"").Append(JsonEscape(kv.Key)).Append("\"");
                    sb.Append(",\"profit\":").Append(kv.Value.profit.ToString("G17", ci));
                    sb.Append(",\"swap\":").Append(kv.Value.swap.ToString("G17", ci));
                    sb.Append(",\"commission\":").Append(kv.Value.commission.ToString("G17", ci));
                    sb.Append(",\"usd_total\":").Append(kv.Value.usdTotal.ToString("G17", ci));
                    sb.Append("}");
                }
                sb.Append("]");
                // --- slow / account fields ---
                sb.Append(",\"balance\":").Append(slow.totalBalance.ToString("G17", ci));
                sb.Append(",\"credit\":").Append(slow.totalCredit.ToString("G17", ci));
                sb.Append(",\"equity\":").Append(totalEquity.ToString("G17", ci));
                sb.Append(",\"wd_equity\":").Append(wdEquity.ToString("G17", ci));
                // --- monthly (MTD) fields ---
                sb.Append(",\"monthly_closed_pnl\":").Append(slow.mthClosedPnl.ToString("G17", ci));
                sb.Append(",\"monthly_net_deposits\":").Append(slow.mthNetDeps.ToString("G17", ci));
                sb.Append(",\"monthly_deposits\":").Append(slow.mthDeps.ToString("G17", ci));
                sb.Append(",\"monthly_withdrawals\":").Append(slow.mthWds.ToString("G17", ci));
                sb.Append(",\"monthly_volume_usd\":").Append(slow.mthVol.ToString("G17", ci));
                sb.Append(",\"monthly_swap\":").Append(slow.mthSwap.ToString("G17", ci));
                sb.Append(",\"monthly_commission\":").Append(slow.mthCommission.ToString("G17", ci));
                sb.Append(",\"monthly_n_traders\":").Append(slow.mthNTraders.ToString(ci));
                sb.Append(",\"monthly_n_active_traders\":").Append(slow.mthNActive.ToString(ci));
                sb.Append(",\"monthly_n_depositors\":").Append(slow.mthNDeps.ToString(ci));
                // --- monthly_by_day (per-day closed PnL for trend table) ---
                sb.Append(",\"monthly_by_day\":[");
                bool bdFirst = true;
                if (slow.closedPnlByDay != null)
                    foreach (var kv in slow.closedPnlByDay)
                    {
                        if (!bdFirst) sb.Append(",");
                        sb.Append("{\"date\":\"").Append(kv.Key).Append("\",\"closed_pnl\":");
                        sb.Append(kv.Value.ToString("G17", ci)).Append("}");
                        bdFirst = false;
                    }
                sb.Append("]");
                // --- monthly_closed_pnl_by_ccy ---
                var mCcyList = new List<KeyValuePair<string, CcyAgg>>(
                    slow.mthByCcy != null ? slow.mthByCcy : new Dictionary<string, CcyAgg>());
                mCcyList.Sort(delegate(KeyValuePair<string, CcyAgg> a, KeyValuePair<string, CcyAgg> b) {
                    return Math.Abs(b.Value.usdTotal).CompareTo(Math.Abs(a.Value.usdTotal));
                });
                sb.Append(",\"monthly_closed_pnl_by_ccy\":[");
                for (int i = 0; i < mCcyList.Count; i++)
                {
                    var kv = mCcyList[i];
                    if (i > 0) sb.Append(",");
                    sb.Append("{\"ccy\":\"").Append(JsonEscape(kv.Key)).Append("\"");
                    sb.Append(",\"profit\":").Append(kv.Value.profit.ToString("G17", ci));
                    sb.Append(",\"swap\":").Append(kv.Value.swap.ToString("G17", ci));
                    sb.Append(",\"commission\":").Append(kv.Value.commission.ToString("G17", ci));
                    sb.Append(",\"usd_total\":").Append(kv.Value.usdTotal.ToString("G17", ci));
                    sb.Append("}");
                }
                sb.Append("]");
                // --- meta ---
                sb.Append(",\"snap_login_count\":").Append(_loginCcySnap.Count.ToString(ci));
                sb.Append(",\"source\":\"AN100\"");
                sb.Append(",\"group_mask\":\"").Append(JsonEscape(group)).Append("\"");
                sb.Append(",\"pushed_at\":\"").Append(pushedAt).Append("\"");

                // --- by_symbol (top 30 by notional) ---
                var symList = new List<SymAgg>(bySymbol.Values);
                symList.Sort(delegate(SymAgg a, SymAgg b) {
                    return Math.Abs(b.notionalUsd).CompareTo(Math.Abs(a.notionalUsd));
                });
                sb.Append(",\"by_symbol\":[");
                int take = Math.Min(30, symList.Count);
                for (int i = 0; i < take; i++)
                {
                    var sa = symList[i];
                    if (i > 0) sb.Append(",");
                    sb.Append("{\"symbol\":\"").Append(JsonEscape(sa.symbol)).Append("\"");
                    sb.Append(",\"n_deals\":").Append(sa.nDeals.ToString(ci));
                    sb.Append(",\"n_traders\":").Append(sa.traders.Count.ToString(ci));
                    sb.Append(",\"notional_usd\":").Append(sa.notionalUsd.ToString("G17", ci));
                    sb.Append(",\"notional_buy\":").Append(sa.notionalBuy.ToString("G17", ci));
                    sb.Append(",\"notional_sell\":").Append(sa.notionalSell.ToString("G17", ci));
                    sb.Append(",\"swap\":").Append(sa.swap.ToString("G17", ci));
                    sb.Append(",\"commission\":").Append(sa.commission.ToString("G17", ci));
                    sb.Append(",\"pnl\":").Append(sa.pnl.ToString("G17", ci));
                    sb.Append("}");
                }
                sb.Append("]");

                // --- by_group (non-empty groups, sorted by abs floating PnL) ---
                byGroupList.Sort(delegate(GrpAgg a, GrpAgg b) {
                    return Math.Abs(b.floatingPnl).CompareTo(Math.Abs(a.floatingPnl));
                });
                sb.Append(",\"by_group\":[");
                for (int i = 0; i < byGroupList.Count; i++)
                {
                    var g = byGroupList[i];
                    if (i > 0) sb.Append(",");
                    double grpClosed; closedByGrp.TryGetValue(g.group, out grpClosed);
                    sb.Append("{\"groupname\":\"").Append(JsonEscape(g.group)).Append("\"");
                    sb.Append(",\"n_accounts\":").Append(g.nPositions.ToString(ci));
                    sb.Append(",\"n_depositors\":0");
                    sb.Append(",\"floating_pnl\":").Append(g.floatingPnl.ToString("G17", ci));
                    sb.Append(",\"closed_pnl\":").Append(grpClosed.ToString("G17", ci));
                    sb.Append(",\"delta_floating\":").Append(g.floatingPnl.ToString("G17", ci));
                    sb.Append(",\"net_deposits\":0,\"equity\":0,\"balance\":0");
                    sb.Append("}");
                }
                sb.Append("]");
                // --- closed_pnl_by_group (all groups with closes today, sorted by abs desc) ---
                var grpPnlList = new List<KeyValuePair<string, double>>(closedByGrp);
                grpPnlList.Sort(delegate(KeyValuePair<string, double> a, KeyValuePair<string, double> b) {
                    return Math.Abs(b.Value).CompareTo(Math.Abs(a.Value));
                });
                sb.Append(",\"closed_pnl_by_group\":[");
                int gTake = Math.Min(100, grpPnlList.Count);
                for (int i = 0; i < gTake; i++)
                {
                    if (i > 0) sb.Append(",");
                    sb.Append("{\"group\":\"").Append(JsonEscape(grpPnlList[i].Key)).Append("\"");
                    sb.Append(",\"closed_pnl\":").Append(grpPnlList[i].Value.ToString("G17", ci));
                    sb.Append("}");
                }
                sb.Append("]}");

                Console.WriteLine(sb.ToString());
                Console.Out.Flush();
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine("[pusher] ERROR: " + ex.Message + " -- reconnecting");
                try { mgr.Disconnect(); } catch { }
                try { mgr.Dispose(); } catch { }
                mgr = null;
                while (mgr == null) { mgr = Connect(server, login, pw); if (mgr == null) Thread.Sleep(5000); }
                groupCcy = LoadGroupCurrencies(mgr, group);
                ccyRates = Mt5UsdRateLoader.LoadLiveRates(mgr);
                rateRefreshCycle = 0;
                slowCycle = slowEvery; // trigger slow loop immediately after reconnect
            }

            Thread.Sleep(interval * 1000);
        }
    }
}
