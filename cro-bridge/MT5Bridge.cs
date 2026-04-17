// MT5 Manager API -> JSON stdout helper.
//
// Compiled with Mono's mcs. Runs under Wine's Wine-Mono runtime, which
// handles the native MT5APIManager64.dll via Wine's PE loader.
//
// Output JSON fields:
//   floating_pnl_usd, n_positions              -- live (open positions)
//   closed_pnl_usd, n_closing_deals             -- today (closing deals)
//   volume_usd, swap, commission                -- today (trade deals)
//   n_traders, n_active_traders                 -- distinct logins today
//   net_deposits, n_depositors                  -- today (balance deals)
//   by_symbol[]                                 -- top 30 symbols today
//   source, group_mask, pushed_at
//
// Env vars:
//   MT5_SERVER, MT5_LOGIN, MT5_PASSWORD, CRO_GROUP
//   CRO_DAY_START, CRO_NOW   (unix seconds, UTC)

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Threading;
using MetaQuotes.MT5CommonAPI;
using MetaQuotes.MT5ManagerAPI;

public class MT5Bridge
{
    const uint ENTRY_IN = 0;
    const uint ACTION_BUY = 0;
    const uint ACTION_SELL = 1;
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
                case '\b': sb.Append("\\b"); break;
                case '\f': sb.Append("\\f"); break;
                case '\n': sb.Append("\\n"); break;
                case '\r': sb.Append("\\r"); break;
                case '\t': sb.Append("\\t"); break;
                default:
                    if (c < 0x20) sb.AppendFormat("\\u{0:x4}", (int)c);
                    else sb.Append(c);
                    break;
            }
        }
        return sb.ToString();
    }

    // Non-USD accounts (ZAR/KES/NGN) have rate_profit > 1.5; profit is in
    // native currency -> divide by rate_profit to get USD.
    // USD accounts have rate_profit <= 1.5 (1.0 or <1 for JPY-quoted pairs)
    // -> profit is already in USD.
    static double ToUsd(double native, double rateProfit)
    {
        if (rateProfit > 1.5 && rateProfit != 0) return native / rateProfit;
        return native;
    }

    // --- per-symbol aggregate ---
    class SymAgg
    {
        public string symbol;
        public int nDeals = 0;
        public HashSet<ulong> traders = new HashSet<ulong>();
        public double notionalUsd = 0;
        public double notionalBuy = 0;
        public double notionalSell = 0;
        public double swap = 0;
        public double commission = 0;
        public double pnl = 0;
    }

    // notional_usd computed inline in the deal loop (IMTDeal isn't a public
    // concrete type in the wrapper; we can't accept it as a parameter).

    public static int Main(string[] args)
    {
        Console.Error.WriteLine("[helper] .NET version: " + Environment.Version);

        var server = Environment.GetEnvironmentVariable("MT5_SERVER") ?? "";
        var loginStr = Environment.GetEnvironmentVariable("MT5_LOGIN") ?? "0";
        var pw = Environment.GetEnvironmentVariable("MT5_PASSWORD") ?? "";
        var group = Environment.GetEnvironmentVariable("CRO_GROUP") ?? "CMV*";
        var dayStartStr = Environment.GetEnvironmentVariable("CRO_DAY_START") ?? "0";
        var nowStr = Environment.GetEnvironmentVariable("CRO_NOW") ?? "0";

        if (string.IsNullOrEmpty(server) || string.IsNullOrEmpty(pw))
        {
            Console.Error.WriteLine("MT5_SERVER + MT5_PASSWORD env vars required.");
            return 2;
        }

        ulong login = ulong.Parse(loginStr, CultureInfo.InvariantCulture);
        long dayStart = long.Parse(dayStartStr, CultureInfo.InvariantCulture);
        long nowUnix = long.Parse(nowStr, CultureInfo.InvariantCulture);
        if (nowUnix == 0) nowUnix = (long)(DateTime.UtcNow - new DateTime(1970, 1, 1)).TotalSeconds;
        if (dayStart == 0) dayStart = nowUnix - 86400;

        string sdkDir = Environment.GetEnvironmentVariable("MT5_SDK_LIBS") ?? @"Z:\app";

        var res = SMTManagerAPIFactory.Initialize(sdkDir);
        if (res != MTRetCode.MT_RET_OK) { Console.Error.WriteLine("Initialize failed: " + res); return 3; }

        MTRetCode cmres = MTRetCode.MT_RET_OK_NONE;
        CIMTManagerAPI mgr = SMTManagerAPIFactory.CreateManager(
            SMTManagerAPIFactory.ManagerAPIVersion, out cmres);
        if (mgr == null || cmres != MTRetCode.MT_RET_OK)
        {
            Console.Error.WriteLine("CreateManager failed: " + cmres);
            SMTManagerAPIFactory.Shutdown();
            return 4;
        }

        var connRes = mgr.Connect(server, login, pw, null,
            CIMTManagerAPI.EnPumpModes.PUMP_MODE_POSITIONS, 30000);
        if (connRes != MTRetCode.MT_RET_OK)
        {
            Console.Error.WriteLine("Connect failed: " + connRes);
            mgr.Dispose();
            SMTManagerAPIFactory.Shutdown();
            return 5;
        }

        Console.Error.WriteLine("[helper] Waiting for position pump...");
        {
            var pumpDeadline = DateTime.UtcNow.AddSeconds(30);
            while (DateTime.UtcNow < pumpDeadline)
            {
                var testArr = mgr.PositionCreateArray();
                bool ready = mgr.PositionGetByGroup("*", testArr) == MTRetCode.MT_RET_OK && testArr.Total() > 0;
                testArr.Dispose();
                if (ready) break;
                Thread.Sleep(500);
            }
        }

        int nPositions = 0, nClosingDeals = 0, nDepositors = 0;
        double floatingPnl = 0.0, closedPnl = 0.0;
        double volumeUsd = 0.0, swap = 0.0, commission = 0.0;
        double deposits = 0.0, withdrawals = 0.0;
        HashSet<ulong> tradersAny = new HashSet<ulong>();
        HashSet<ulong> tradersActive = new HashSet<ulong>();
        HashSet<ulong> depositors = new HashSet<ulong>();
        var bySymbol = new Dictionary<string, SymAgg>();

        int exitOnErr = 0;
        try
        {
            // --- open positions: floating PnL, currency-aware per group ---
            var grpArr = mgr.GroupCreateArray();
            if (mgr.GroupRequestArray(group, grpArr) == MTRetCode.MT_RET_OK)
            {
                var groupIsUsd = new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase);
                for (uint i = 0; i < grpArr.Total(); i++)
                {
                    var g = grpArr.Next(i);
                    string gName = g.Group() ?? "";
                    string gCcy  = g.Currency() ?? "USD";
                    if (!string.IsNullOrEmpty(gName))
                        groupIsUsd[gName] = string.Equals(gCcy, "USD", StringComparison.OrdinalIgnoreCase);
                }
                grpArr.Release();
                foreach (var kv in groupIsUsd)
                {
                    var posArr = mgr.PositionCreateArray();
                    var posRes = mgr.PositionGetByGroup(kv.Key, posArr);
                    if (posRes != MTRetCode.MT_RET_OK)
                    {
                        Console.Error.WriteLine("PositionGetByGroup failed for " + kv.Key + ": " + posRes);
                        posArr.Dispose();
                        exitOnErr = 6;
                        break;
                    }
                    bool isUsd = kv.Value;
                    nPositions += (int)posArr.Total();
                    for (uint i = 0; i < posArr.Total(); i++)
                    {
                        var p = posArr.Next(i);
                        double native = p.Profit() + p.Storage();
                        if (isUsd)
                            floatingPnl += native;
                        else
                        {
                            double r = p.RateProfit();
                            floatingPnl += (r > 0.0) ? native / r : native;
                        }
                    }
                    posArr.Dispose();
                }
            }
            else
            {
                grpArr.Release();
                Console.Error.WriteLine("GroupRequestArray failed");
                exitOnErr = 6;
            }

            // --- today's deals ---
            DateTime dayStartDt = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddSeconds(dayStart);
            DateTime nowDt      = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddSeconds(nowUnix);
            var dealArr = mgr.DealCreateArray();
            var dealRes = mgr.DealRequestByGroup(group,
                SMTTime.FromDateTime(dayStartDt), SMTTime.FromDateTime(nowDt), dealArr);
            if (dealRes != MTRetCode.MT_RET_OK)
            {
                Console.Error.WriteLine("DealRequestByGroup failed: " + dealRes);
                exitOnErr = 7;
            }

            for (uint i = 0; i < dealArr.Total(); i++)
            {
                var d = dealArr.Next(i);
                uint action = d.Action();
                ulong dLogin = d.Login();
                double rate = d.RateProfit();

                if (action == ACTION_BUY || action == ACTION_SELL)
                {
                    // Trade deal
                    tradersAny.Add(dLogin);
                    if (d.Entry() == ENTRY_IN) tradersActive.Add(dLogin);
                    else
                    {
                        // Closing deal contributes to PnL / volume
                        closedPnl += ToUsd(d.Profit() + d.Storage() + d.Commission(), rate);
                        nClosingDeals++;
                    }
                    // Volume in USD -- use symbol-name heuristic.
                    // * Symbol ends with "USD" (EURUSD, XAUUSD, USTECH USD-quoted indices)
                    //     -> price is USD per unit, notional_usd = lots * contract_size * price
                    // * Symbol starts with "USD" (USDJPY, USDCAD, USDZAR)
                    //     -> USD is the base, notional_usd = lots * contract_size
                    // * Cross-pair / commodity: approximate with lots * contract_size
                    //     (conservative -- volumes get under-reported for some crosses).
                    // In MT5: Volume() is lots * 100 (hundredths of a lot).
                    double lots = d.Volume() / 100.0;
                    double price = d.Price();
                    string symU = (d.Symbol() ?? "").ToUpperInvariant();
                    double dn;
                    if (symU.EndsWith("USD") || symU.EndsWith("USDC"))
                        dn = Math.Abs(lots * d.ContractSize() * price);
                    else if (symU.StartsWith("USD"))
                        dn = Math.Abs(lots * d.ContractSize());
                    else
                        dn = Math.Abs(lots * d.ContractSize());  // conservative fallback
                    volumeUsd += dn;
                    swap += ToUsd(d.Storage(), rate);
                    commission += ToUsd(d.Commission(), rate);

                    // By-symbol aggregation
                    string sym = d.Symbol();
                    if (!string.IsNullOrEmpty(sym))
                    {
                        SymAgg sa;
                        if (!bySymbol.TryGetValue(sym, out sa))
                        {
                            sa = new SymAgg { symbol = sym };
                            bySymbol[sym] = sa;
                        }
                        sa.nDeals++;
                        sa.traders.Add(dLogin);
                        sa.notionalUsd += dn;
                        if (action == ACTION_BUY) sa.notionalBuy += dn; else sa.notionalSell += dn;
                        sa.swap += ToUsd(d.Storage(), rate);
                        sa.commission += ToUsd(d.Commission(), rate);
                        sa.pnl += ToUsd(d.Profit() + d.Storage() + d.Commission(), rate);
                    }
                }
                else if (action == ACTION_BALANCE)
                {
                    // Deposit / withdrawal
                    double amt = ToUsd(d.Profit(), rate);
                    string comment = (d.Comment() ?? "").ToLowerInvariant();
                    if (comment.Contains("bonus") || comment.Contains("internal") || comment.Contains("transfer"))
                        continue;
                    if (amt > 0) { deposits += amt; depositors.Add(dLogin); }
                    else         { withdrawals += amt; }
                }
            }
            dealArr.Dispose();
            nDepositors = depositors.Count;
        }
        finally
        {
            try { mgr.Disconnect(); } catch { }
            mgr.Dispose();
            SMTManagerAPIFactory.Shutdown();
        }

        // --- emit JSON ---
        string pushedAt = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ", CultureInfo.InvariantCulture);
        var ci = CultureInfo.InvariantCulture;
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

        // by_symbol top 30 by |notional|
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

        // Sanity: this broker has 22 000+ open positions during trading hours.
        // If both n_positions AND n_traders are zero the query returned empty
        // silently (transient MT5 issue). Skip emission so the last good push
        // is preserved in _CRO_LIVE on the server side.
        if (nPositions == 0 && tradersAny.Count == 0)
        {
            Console.Error.WriteLine("[helper] sanity failed: 0 positions AND 0 traders -- skipping");
            return 8;
        }

        // Don't push a JSON with stale/incomplete values if an MT5 call errored.
        if (exitOnErr != 0)
        {
            Console.Error.WriteLine("[helper] skipping JSON emit due to MT5 API error");
            return exitOnErr;
        }
        Console.WriteLine(sb.ToString());
        return 0;
    }
}
