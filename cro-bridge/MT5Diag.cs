// Diagnostic: breakdown of floating PnL by position reason + group login ranges.
// Helps identify why CMV* total differs from MT5 Manager panel.
//
// Env vars: MT5_SERVER, MT5_LOGIN, MT5_PASSWORD, CRO_GROUP, MT5_SDK_LIBS

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using MetaQuotes.MT5CommonAPI;
using MetaQuotes.MT5ManagerAPI;

public class MT5Diag
{
    static double ToUsd(double native, double rate)
    {
        if (rate > 1.5 && rate != 0) return native / rate;
        return native;
    }

    public static int Main(string[] args)
    {
        var ci       = CultureInfo.InvariantCulture;
        var server   = Environment.GetEnvironmentVariable("MT5_SERVER")   ?? "";
        var loginStr = Environment.GetEnvironmentVariable("MT5_LOGIN")    ?? "0";
        var pw       = Environment.GetEnvironmentVariable("MT5_PASSWORD") ?? "";
        var group    = Environment.GetEnvironmentVariable("CRO_GROUP")    ?? "CMV*";
        var sdkDir   = Environment.GetEnvironmentVariable("MT5_SDK_LIBS") ?? @"C:\MetaTrader5SDK\Libs";

        if (string.IsNullOrEmpty(server) || string.IsNullOrEmpty(pw))
        {
            Console.Error.WriteLine("MT5_SERVER + MT5_PASSWORD required.");
            return 2;
        }
        ulong login = ulong.Parse(loginStr, ci);

        var initRes = SMTManagerAPIFactory.Initialize(sdkDir);
        if (initRes != MTRetCode.MT_RET_OK)
        {
            Console.Error.WriteLine("Initialize failed: " + initRes);
            return 3;
        }

        MTRetCode cm = MTRetCode.MT_RET_OK_NONE;
        CIMTManagerAPI mgr = SMTManagerAPIFactory.CreateManager(
            SMTManagerAPIFactory.ManagerAPIVersion, out cm);
        if (mgr == null || cm != MTRetCode.MT_RET_OK)
        {
            Console.Error.WriteLine("CreateManager: " + cm);
            return 4;
        }

        // PUMP_MODE_POSITIONS so we can also call SummaryGetAll (needs pump cache)
        var cr = mgr.Connect(server, login, pw, null,
            CIMTManagerAPI.EnPumpModes.PUMP_MODE_POSITIONS, 30000);
        if (cr != MTRetCode.MT_RET_OK)
        {
            Console.Error.WriteLine("Connect: " + cr);
            mgr.Dispose(); return 5;
        }

        Console.Error.WriteLine("Waiting for pump...");
        var deadline = DateTime.Now.AddSeconds(30);
        while (DateTime.Now < deadline)
        {
            var t2 = mgr.PositionCreateArray();
            bool ready = mgr.PositionGetByGroup("*", t2) == MTRetCode.MT_RET_OK && t2.Total() > 0;
            t2.Dispose();
            if (ready) break;
            Thread.Sleep(500);
        }
        Console.Error.WriteLine("Connected.");

        // ----------------------------------------------------------------
        // 1. Positions via PositionGetByGroup (pump cache, group-filtered)
        // ----------------------------------------------------------------
        var posArr = mgr.PositionCreateArray();
        mgr.PositionGetByGroup(group, posArr);
        uint nPos = posArr.Total();

        double totalPnl = 0, totalPnlRaw = 0;
        // breakdown by Reason()
        var byReason = new Dictionary<uint, double>();
        var byReasonRaw = new Dictionary<uint, double>();
        var byReasonCount = new Dictionary<uint, int>();
        double profitOnly = 0, storageOnly = 0;

        for (uint i = 0; i < nPos; i++)
        {
            var p = posArr.Next(i);
            double rate   = p.RateProfit();
            double profit = p.Profit();
            double storage= p.Storage();
            double usd    = ToUsd(profit + storage, rate);
            uint   reason = p.Reason();

            totalPnl    += usd;
            totalPnlRaw += profit + storage;
            profitOnly  += ToUsd(profit,  rate);
            storageOnly += ToUsd(storage, rate);

            if (!byReason.ContainsKey(reason))      { byReason[reason] = 0; byReasonRaw[reason] = 0; byReasonCount[reason] = 0; }
            byReason[reason]    += usd;
            byReasonRaw[reason] += profit + storage;
            byReasonCount[reason]++;
        }
        posArr.Dispose();

        Console.WriteLine(string.Format("\n=== PositionGetByGroup(\"{0}\") ===", group));
        Console.WriteLine(string.Format("  Total positions : {0:N0}", nPos));
        Console.WriteLine(string.Format("  PnL (ToUsd)     : {0:N2}", totalPnl));
        Console.WriteLine(string.Format("  PnL (raw, no fx): {0:N2}", totalPnlRaw));
        Console.WriteLine(string.Format("  Profit only     : {0:N2}", profitOnly));
        Console.WriteLine(string.Format("  Storage only    : {0:N2}", storageOnly));
        Console.WriteLine("\n  Breakdown by position Reason:");
        Console.WriteLine(string.Format("  {0,-8}  {1,10}  {2,20}  {3,20}", "Reason", "Count", "PnL USD (ToUsd)", "PnL raw"));
        foreach (var kv in byReason)
        {
            Console.WriteLine(string.Format("  {0,-8}  {1,10:N0}  {2,20:N2}  {3,20:N2}",
                kv.Key, byReasonCount[kv.Key], kv.Value, byReasonRaw[kv.Key]));
        }

        // ----------------------------------------------------------------
        // 2. Positions via PositionGetByGroup("*") — all groups
        // ----------------------------------------------------------------
        var posAll = mgr.PositionCreateArray();
        mgr.PositionGetByGroup("*", posAll);
        uint nAll = posAll.Total();
        double allPnl = 0;
        for (uint i = 0; i < nAll; i++)
        {
            var p = posAll.Next(i);
            allPnl += ToUsd(p.Profit() + p.Storage(), p.RateProfit());
        }
        posAll.Dispose();
        Console.WriteLine(string.Format("\n=== PositionGetByGroup(\"*\") — all groups ==="));
        Console.WriteLine(string.Format("  Total positions : {0:N0}", nAll));
        Console.WriteLine(string.Format("  PnL (ToUsd)     : {0:N2}", allPnl));

        // ----------------------------------------------------------------
        // 3. Summary API (broker-wide, pump cache)
        // ----------------------------------------------------------------
        mgr.SummaryCurrency("USD");
        var sumArr = mgr.SummaryCreateArray();
        MTRetCode sumRes = mgr.SummaryGetAll(sumArr);
        uint nSymbols = sumArr.Total();
        double sumPnlClients = 0, sumPnlCoverage = 0;
        uint   sumPosClients = 0, sumPosCoverage = 0;
        for (uint i = 0; i < nSymbols; i++)
        {
            var s = sumArr.Next(i);
            sumPnlClients  += s.ProfitFullClients();
            sumPnlCoverage += s.ProfitFullCoverage();
            sumPosClients  += s.PositionClients();
            sumPosCoverage += s.PositionCoverage();
        }
        sumArr.Release();
        Console.WriteLine(string.Format("\n=== SummaryGetAll (broker-wide, USD) res={0} ===", sumRes));
        Console.WriteLine(string.Format("  Symbols         : {0:N0}", nSymbols));
        Console.WriteLine(string.Format("  Positions client: {0:N0}   PnL: {1:N2}", sumPosClients, sumPnlClients));
        Console.WriteLine(string.Format("  Positions cover : {0:N0}   PnL: {1:N2}", sumPosCoverage, sumPnlCoverage));

        // ----------------------------------------------------------------
        // 4. PositionRequestByGroup — server snapshot (no pump cache)
        // ----------------------------------------------------------------
        var posReq = mgr.PositionCreateArray();
        MTRetCode reqRes = mgr.PositionRequestByGroup(group, posReq);
        uint nReq = posReq.Total();
        double reqPnl = 0, reqProfit = 0, reqStorage = 0;
        for (uint i = 0; i < nReq; i++)
        {
            var p = posReq.Next(i);
            double rate = p.RateProfit();
            reqPnl     += ToUsd(p.Profit() + p.Storage(), rate);
            reqProfit  += ToUsd(p.Profit(), rate);
            reqStorage += ToUsd(p.Storage(), rate);
        }
        posReq.Dispose();
        Console.WriteLine(string.Format("\n=== PositionRequestByGroup(\"{0}\") — server snapshot ===", group));
        Console.WriteLine(string.Format("  res={0}  positions={1:N0}", reqRes, nReq));
        Console.WriteLine(string.Format("  PnL (ToUsd)  : {0:N2}", reqPnl));
        Console.WriteLine(string.Format("  Profit only  : {0:N2}", reqProfit));
        Console.WriteLine(string.Format("  Storage only : {0:N2}", reqStorage));

        // ----------------------------------------------------------------
        // 5. CMV* group currencies
        // ----------------------------------------------------------------
        var grpArr = mgr.GroupCreateArray();
        MTRetCode grpRes = mgr.GroupRequestArray(group, grpArr);
        Console.WriteLine(string.Format("\n=== GroupRequestArray(\"{0}\") res={1} ===", group, grpRes));
        var currencySet = new Dictionary<string, int>();
        for (uint i = 0; i < grpArr.Total(); i++)
        {
            var g = grpArr.Next(i);
            string cur = g.Currency() ?? "?";
            if (!currencySet.ContainsKey(cur)) currencySet[cur] = 0;
            currencySet[cur]++;
        }
        grpArr.Release();
        foreach (var kv in currencySet)
            Console.WriteLine(string.Format("  {0,-6} : {1} groups", kv.Key, kv.Value));

        // ----------------------------------------------------------------
        // 6. Re-sum positions with "always divide by RateProfit" (no threshold)
        //    For USD accounts trading EURUSD: rate=1.0, divide by 1.0 = same result.
        //    For non-USD accounts: rate != 1.0, divide gives correct USD.
        //    Caveat: USD accounts trading non-USD-profit instruments get wrong result,
        //    but that only matters if this broker has such instruments.
        // ----------------------------------------------------------------
        var posArr2 = mgr.PositionCreateArray();
        mgr.PositionGetByGroup(group, posArr2);
        double alwaysDividePnl = 0;
        double byRateBucket_0_to_1 = 0, byRateBucket_1_to_1_5 = 0, byRateBucket_over_1_5 = 0;
        int cnt0 = 0, cnt1 = 0, cnt2 = 0;
        for (uint i = 0; i < posArr2.Total(); i++)
        {
            var p = posArr2.Next(i);
            double rate = p.RateProfit();
            double native2 = p.Profit() + p.Storage();
            // always divide (with zero guard)
            double usd2 = (rate != 0) ? native2 / rate : native2;
            alwaysDividePnl += usd2;
            // bucket by rate range
            if (rate < 1.0)           { byRateBucket_0_to_1 += usd2; cnt0++; }
            else if (rate < 1.5)      { byRateBucket_1_to_1_5 += usd2; cnt1++; }
            else                       { byRateBucket_over_1_5 += usd2; cnt2++; }
        }
        posArr2.Dispose();
        Console.WriteLine(string.Format("\n=== Alternative: always divide by RateProfit ==="));
        Console.WriteLine(string.Format("  Total (always divide) : {0:N2}", alwaysDividePnl));
        Console.WriteLine(string.Format("  Diff vs heuristic     : {0:N2}", alwaysDividePnl - totalPnl));
        Console.WriteLine(string.Format("  rate < 1.0   ({0,6:N0} pos): {1:N2}", cnt0, byRateBucket_0_to_1));
        Console.WriteLine(string.Format("  1.0<=rate<1.5({0,6:N0} pos): {1:N2}", cnt1, byRateBucket_1_to_1_5));
        Console.WriteLine(string.Format("  rate >= 1.5  ({0,6:N0} pos): {1:N2}", cnt2, byRateBucket_over_1_5));

        // ----------------------------------------------------------------
        // 7. Suspicious positions: rate in [0.9, 1.1] — could be non-USD
        //    accounts trading home-currency pairs (RateProfit=1 because
        //    profit currency == deposit currency, but we treat it as USD)
        // ----------------------------------------------------------------
        var posArr3 = mgr.PositionCreateArray();
        mgr.PositionGetByGroup(group, posArr3);
        Console.WriteLine("\n=== Positions with RateProfit in [0.9, 1.1] (suspicious) ===");
        int suspCount = 0;
        double suspNative = 0;
        var suspLogins = new HashSet<ulong>();
        for (uint i = 0; i < posArr3.Total(); i++)
        {
            var p = posArr3.Next(i);
            double r = p.RateProfit();
            if (r >= 0.9 && r <= 1.1)
            {
                suspCount++;
                suspNative += p.Profit() + p.Storage();
                suspLogins.Add(p.Login());
            }
        }
        posArr3.Dispose();
        Console.WriteLine(string.Format("  Count: {0}  Distinct logins: {1}", suspCount, suspLogins.Count));
        Console.WriteLine(string.Format("  Sum native (treated as USD by heuristic): {0:N2}", suspNative));

        // Fetch user groups for the suspicious logins (up to 50 to limit time)
        var grpFetch = mgr.GroupCreate();
        var usr = mgr.UserCreate();
        var groupCurrencyCache = new Dictionary<string, string>();
        var nonUsdLogins = new List<ulong>();
        int checked2 = 0;
        foreach (ulong suspLogin in suspLogins)
        {
            if (checked2++ >= 200) break;
            if (mgr.UserRequest(suspLogin, usr) == MTRetCode.MT_RET_OK)
            {
                string grpName = usr.Group() ?? "";
                string cur;
                if (!groupCurrencyCache.TryGetValue(grpName, out cur))
                {
                    if (mgr.GroupRequest(grpName, grpFetch) == MTRetCode.MT_RET_OK)
                        cur = grpFetch.Currency() ?? "?";
                    else
                        cur = "?";
                    groupCurrencyCache[grpName] = cur;
                }
                if (cur != "USD") nonUsdLogins.Add(suspLogin);
            }
        }
        grpFetch.Release();
        usr.Release();

        Console.WriteLine(string.Format("  Non-USD accounts among suspicious logins (sample 200): {0}", nonUsdLogins.Count));
        foreach (var kv2 in groupCurrencyCache)
            if (kv2.Value != "USD")
                Console.WriteLine(string.Format("    group={0}  currency={1}", kv2.Key, kv2.Value));

        // Sum native PnL for the non-USD suspicious positions
        if (nonUsdLogins.Count > 0)
        {
            var nonUsdSet = new HashSet<ulong>(nonUsdLogins);
            var posArr4 = mgr.PositionCreateArray();
            mgr.PositionGetByGroup(group, posArr4);
            double nonUsdSuspNative = 0, nonUsdSuspTrue = 0;
            int nonUsdSuspCount = 0;
            for (uint i = 0; i < posArr4.Total(); i++)
            {
                var p = posArr4.Next(i);
                if (!nonUsdSet.Contains(p.Login())) continue;
                double r = p.RateProfit();
                if (r < 0.9 || r > 1.1) continue;
                double native3 = p.Profit() + p.Storage();
                nonUsdSuspNative += native3;
                nonUsdSuspCount++;
            }
            posArr4.Dispose();
            Console.WriteLine(string.Format("  Non-USD accounts: {0} positions, native sum (our USD): {1:N2}", nonUsdSuspCount, nonUsdSuspNative));
            Console.WriteLine(string.Format("  --> This entire native sum is being incorrectly treated as USD!"));
        }

        Console.WriteLine("\n=== Gap analysis ===");
        Console.WriteLine(string.Format("  PumpCache CMV* vs Request CMV*     : {0:N2}", totalPnl - reqPnl));
        Console.WriteLine(string.Format("  AlwaysDivide  vs Request CMV*      : {0:N2}", alwaysDividePnl - reqPnl));
        Console.WriteLine(string.Format("  CMV* (ToUsd) vs Summary clients    : {0:N2}", totalPnl - sumPnlClients));
        Console.WriteLine(string.Format("  AlwaysDivide vs Summary clients    : {0:N2}", alwaysDividePnl - sumPnlClients));

        try { mgr.Disconnect(); } catch { }
        mgr.Dispose();
        SMTManagerAPIFactory.Shutdown();
        return 0;
    }
}
