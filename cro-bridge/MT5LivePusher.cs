// MT5 Manager API -> continuous JSON line pusher.
// Connects once, stays connected; outputs one JSON line per CRO_INTERVAL seconds.
// Designed to run under wine; stdout is piped to cro_live_pusher.py which
// forwards each line to /cro/feed.
//
// JSON fields: same as MT5Bridge.cs (floating_pnl_usd, closed_pnl_usd,
// n_positions, n_closing_deals, volume_usd, swap, commission, net_deposits,
// deposits, withdrawals, n_traders, n_active_traders, n_depositors,
// source, group_mask, pushed_at, by_symbol[])
//
// Env vars: MT5_SERVER, MT5_LOGIN, MT5_PASSWORD, CRO_GROUP, MT5_SDK_LIBS
//           CRO_INTERVAL (seconds, default 5)

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Threading;
using MetaQuotes.MT5CommonAPI;
using MetaQuotes.MT5ManagerAPI;

public class MT5LivePusher
{
    const uint ENTRY_IN      = 0;
    const uint ACTION_BUY    = 0;
    const uint ACTION_SELL   = 1;
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

    // Fallback heuristic for currencies whose TickLast symbol isn't on the broker
    // (AED, CLP, INR). Correct for those since their rates >> 1.5.
    static double ToUsd(double native, double rate)
    {
        if (rate > 1.5 && rate != 0) return native / rate;
        return native;
    }

    class SymAgg
    {
        public string symbol;
        public int nDeals;
        public HashSet<ulong> traders = new HashSet<ulong>();
        public double notionalUsd, notionalBuy, notionalSell, swap, commission, pnl;
    }

    // FX pair table: for each non-USD deposit currency, which symbol to query via TickLast.
    // UsdBase=true  → symbol is USDxxx → usdRate = 1/mid
    // UsdBase=false → symbol is xxxUSD → usdRate = mid
    struct CcyEntry { public string Sym, Ccy; public bool UsdBase; }
    static readonly CcyEntry[] CcyTable = {
        new CcyEntry { Sym="EURUSD", Ccy="EUR", UsdBase=false },
        new CcyEntry { Sym="GBPUSD", Ccy="GBP", UsdBase=false },
        new CcyEntry { Sym="AUDUSD", Ccy="AUD", UsdBase=false },
        new CcyEntry { Sym="NZDUSD", Ccy="NZD", UsdBase=false },
        new CcyEntry { Sym="USDZAR", Ccy="ZAR", UsdBase=true  },
        new CcyEntry { Sym="USDKES", Ccy="KES", UsdBase=true  },
        new CcyEntry { Sym="USDNGN", Ccy="NGN", UsdBase=true  },
        new CcyEntry { Sym="USDMXN", Ccy="MXN", UsdBase=true  },
        new CcyEntry { Sym="USDAED", Ccy="AED", UsdBase=true  },
        new CcyEntry { Sym="USDCLP", Ccy="CLP", UsdBase=true  },
        new CcyEntry { Sym="USDINR", Ccy="INR", UsdBase=true  },
    };

    static Dictionary<string, double> BuildCcyRates(CIMTManagerAPI mgr)
    {
        var d = new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase);
        d["USD"] = 1.0;
        foreach (var c in CcyTable)
        {
            MTTickShort tick;
            if (mgr.TickLast(c.Sym, out tick) == MTRetCode.MT_RET_OK && tick.bid > 0 && tick.ask > 0)
            {
                double mid = (tick.bid + tick.ask) * 0.5;
                d[c.Ccy] = c.UsdBase ? 1.0 / mid : mid;
            }
        }
        return d;
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

    public static int Main(string[] args)
    {
        var ci       = CultureInfo.InvariantCulture;
        var server   = Environment.GetEnvironmentVariable("MT5_SERVER")   ?? "";
        var loginStr = Environment.GetEnvironmentVariable("MT5_LOGIN")    ?? "0";
        var pw       = Environment.GetEnvironmentVariable("MT5_PASSWORD") ?? "";
        var group    = Environment.GetEnvironmentVariable("CRO_GROUP")    ?? "CMV*";
        var sdkDir   = Environment.GetEnvironmentVariable("MT5_SDK_LIBS") ?? "Z:/app";
        int interval = int.Parse(Environment.GetEnvironmentVariable("CRO_INTERVAL") ?? "5", ci);

        if (string.IsNullOrEmpty(server) || string.IsNullOrEmpty(pw))
        {
            Console.Error.WriteLine("[pusher] MT5_SERVER + MT5_PASSWORD required.");
            return 2;
        }
        ulong login = ulong.Parse(loginStr, ci);

        var initRes = SMTManagerAPIFactory.Initialize(sdkDir);
        if (initRes != MTRetCode.MT_RET_OK)
        {
            Console.Error.WriteLine("[pusher] Initialize failed: " + initRes);
            return 3;
        }

        Console.Error.WriteLine("[pusher] group=" + group + "  interval=" + interval + "s");

        CIMTManagerAPI mgr = null;
        while (mgr == null) { mgr = Connect(server, login, pw); if (mgr == null) Thread.Sleep(5000); }

        var groupCcy         = LoadGroupCurrencies(mgr, group);
        var ccyRates         = BuildCcyRates(mgr);
        int rateRefreshCycle = 0;
        Console.Error.WriteLine("[pusher] " + groupCcy.Count + " groups, " + ccyRates.Count + " FX rates.");

        while (true)
        {
            try
            {
                if (++rateRefreshCycle >= 60) { ccyRates = BuildCcyRates(mgr); rateRefreshCycle = 0; }

                // Day bounds (UTC midnight as start-of-day)
                DateTime nowDt      = DateTime.UtcNow;
                DateTime dayStartDt = nowDt.Date;

                // --- floating PnL: per-group with live TickLast rates ---
                int    nPositions  = 0;
                double floatingPnl = 0.0;
                foreach (var kv in groupCcy)
                {
                    double usdRate; bool hasRate = ccyRates.TryGetValue(kv.Value, out usdRate);
                    var posArr = mgr.PositionCreateArray();
                    if (mgr.PositionGetByGroup(kv.Key, posArr) != MTRetCode.MT_RET_OK)
                    {
                        posArr.Dispose();
                        throw new Exception("PositionGetByGroup failed for " + kv.Key);
                    }
                    nPositions += (int)posArr.Total();
                    for (uint i = 0; i < posArr.Total(); i++)
                    {
                        var p = posArr.Next(i);
                        double native = p.Profit() + p.Storage();
                        floatingPnl += hasRate ? native * usdRate : ToUsd(native, p.RateProfit());
                    }
                    posArr.Dispose();
                }

                // --- today's deals ---
                int    nClosingDeals = 0, nDepositors = 0;
                double closedPnl = 0.0, volumeUsd = 0.0, swap = 0.0, commission = 0.0;
                double deposits = 0.0, withdrawals = 0.0;
                var tradersAny    = new HashSet<ulong>();
                var tradersActive = new HashSet<ulong>();
                var depositors    = new HashSet<ulong>();
                var bySymbol      = new Dictionary<string, SymAgg>();

                var dealArr = mgr.DealCreateArray();
                MTRetCode dealRes = mgr.DealRequestByGroup(group,
                    SMTTime.FromDateTime(dayStartDt), SMTTime.FromDateTime(nowDt), dealArr);
                if (dealRes == MTRetCode.MT_RET_OK)
                {
                    for (uint i = 0; i < dealArr.Total(); i++)
                    {
                        var    d      = dealArr.Next(i);
                        uint   action = d.Action();
                        ulong  dLogin = d.Login();
                        double rate   = d.RateProfit();

                        if (action == ACTION_BUY || action == ACTION_SELL)
                        {
                            tradersAny.Add(dLogin);
                            if (d.Entry() == ENTRY_IN)
                                tradersActive.Add(dLogin);
                            else
                            {
                                closedPnl += ToUsd(d.Profit() + d.Storage() + d.Commission(), rate);
                                nClosingDeals++;
                            }
                            double lots = d.Volume() / 100.0;
                            double price = d.Price();
                            string symU = (d.Symbol() ?? "").ToUpperInvariant();
                            double dn;
                            if (symU.EndsWith("USD") || symU.EndsWith("USDC"))
                                dn = Math.Abs(lots * d.ContractSize() * price);
                            else
                                dn = Math.Abs(lots * d.ContractSize());
                            volumeUsd  += dn;
                            swap       += ToUsd(d.Storage(), rate);
                            commission += ToUsd(d.Commission(), rate);

                            string sym = d.Symbol();
                            if (!string.IsNullOrEmpty(sym))
                            {
                                SymAgg sa;
                                if (!bySymbol.TryGetValue(sym, out sa))
                                    { sa = new SymAgg { symbol = sym }; bySymbol[sym] = sa; }
                                sa.nDeals++;
                                sa.traders.Add(dLogin);
                                sa.notionalUsd  += dn;
                                if (action == ACTION_BUY) sa.notionalBuy += dn; else sa.notionalSell += dn;
                                sa.swap       += ToUsd(d.Storage(), rate);
                                sa.commission += ToUsd(d.Commission(), rate);
                                sa.pnl        += ToUsd(d.Profit() + d.Storage() + d.Commission(), rate);
                            }
                        }
                        else if (action == ACTION_BALANCE)
                        {
                            double amt = ToUsd(d.Profit(), rate);
                            string comment = (d.Comment() ?? "").ToLowerInvariant();
                            if (comment.Contains("bonus") || comment.Contains("internal") || comment.Contains("transfer"))
                                continue;
                            if (amt > 0) { deposits += amt; depositors.Add(dLogin); }
                            else         { withdrawals += amt; }
                        }
                    }
                }
                dealArr.Dispose();
                nDepositors = depositors.Count;

                // Sanity: skip emission if pump returned empty (transient MT5 issue)
                if (nPositions == 0 && tradersAny.Count == 0)
                {
                    Console.Error.WriteLine("[pusher] sanity: 0 positions + 0 traders -- skipping");
                    Thread.Sleep(interval * 1000);
                    continue;
                }

                // --- emit JSON ---
                string pushedAt = nowDt.ToString("yyyy-MM-ddTHH:mm:ss.fffZ", ci);
                var sb = new StringBuilder(4096);
                sb.Append("{");
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
                sb.Append(",\"n_depositors\":").Append(nDepositors.ToString(ci));
                sb.Append(",\"source\":\"AN100\"");
                sb.Append(",\"group_mask\":\"").Append(JsonEscape(group)).Append("\"");
                sb.Append(",\"pushed_at\":\"").Append(pushedAt).Append("\"");

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
                ccyRates = BuildCcyRates(mgr);
                rateRefreshCycle = 0;
            }

            Thread.Sleep(interval * 1000);
        }
    }
}
