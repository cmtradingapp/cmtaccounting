// Continuous MT5 stats poller for manual testing.
// Connects, pulls positions + today's deals, prints a one-line summary, disconnects, sleeps.
//
// Env vars: same as MT5Bridge.cs + MT5_INTERVAL (seconds, default 5)

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using MetaQuotes.MT5CommonAPI;
using MetaQuotes.MT5ManagerAPI;

public class MT5Monitor
{
    const uint ENTRY_IN    = 0;
    const uint ACTION_BUY  = 0;
    const uint ACTION_SELL = 1;
    const uint ACTION_BAL  = 2;

    static double ToUsd(double native, double rate)
    {
        if (rate > 1.5 && rate != 0) return native / rate;
        return native;
    }

    public static int Main(string[] args)
    {
        var server   = Environment.GetEnvironmentVariable("MT5_SERVER")   ?? "";
        var loginStr = Environment.GetEnvironmentVariable("MT5_LOGIN")    ?? "0";
        var pw       = Environment.GetEnvironmentVariable("MT5_PASSWORD") ?? "";
        var group    = Environment.GetEnvironmentVariable("CRO_GROUP")    ?? "CMV*";
        var sdkDir   = Environment.GetEnvironmentVariable("MT5_SDK_LIBS") ?? @"C:\MetaTrader5SDK\Libs";
        int interval = int.Parse(Environment.GetEnvironmentVariable("MT5_INTERVAL") ?? "5",
                                 CultureInfo.InvariantCulture);

        if (string.IsNullOrEmpty(server) || string.IsNullOrEmpty(pw))
        {
            Console.Error.WriteLine("MT5_SERVER + MT5_PASSWORD required.");
            return 2;
        }
        ulong login = ulong.Parse(loginStr, CultureInfo.InvariantCulture);

        var initRes = SMTManagerAPIFactory.Initialize(sdkDir);
        if (initRes != MTRetCode.MT_RET_OK)
        {
            Console.Error.WriteLine("Initialize failed: " + initRes);
            return 3;
        }

        Console.WriteLine($"[monitor] group={group}  interval={interval}s  server={server}");
        Console.WriteLine("[monitor] Ctrl+C to stop.\n");

        // ── header ──────────────────────────────────────────────────────────
        Console.WriteLine(
            $"{"Time",-10}  {"Floating PnL (USD)",>22}  {"Δ Float",>14}  " +
            $"{"Closed PnL",>14}  {"Net Dep",>12}  {"Positions",>10}  {"Traders",>8}");
        Console.WriteLine(new string('─', 102));

        double prevFloat = double.NaN;
        int    cycle     = 0;

        while (true)
        {
            cycle++;
            var t0 = DateTime.Now;
            try
            {
                // fresh connect each cycle — avoids stale state
                MTRetCode cm = MTRetCode.MT_RET_OK_NONE;
                CIMTManagerAPI mgr = SMTManagerAPIFactory.CreateManager(
                    SMTManagerAPIFactory.ManagerAPIVersion, out cm);
                if (mgr == null || cm != MTRetCode.MT_RET_OK)
                { Console.Error.WriteLine($"CreateManager: {cm}"); goto Sleep; }

                if (mgr.Connect(server, login, pw, null,
                        CIMTManagerAPI.EnPumpModes.PUMP_MODE_NONE, 15000) != MTRetCode.MT_RET_OK)
                { Console.Error.WriteLine("Connect failed"); mgr.Dispose(); goto Sleep; }

                // ── open positions ───────────────────────────────────────────
                double floatPnl = 0;
                int    nPos     = 0;
                var posArr = mgr.PositionCreateArray();
                if (mgr.PositionRequestByGroup(group, posArr) == MTRetCode.MT_RET_OK)
                {
                    nPos = (int)posArr.Total();
                    for (uint i = 0; i < posArr.Total(); i++)
                    {
                        var p = posArr.Next(i);
                        floatPnl += ToUsd(p.Profit() + p.Storage(), p.RateProfit());
                    }
                }
                posArr.Dispose();

                // ── today's deals ────────────────────────────────────────────
                var dayStart = DateTime.UtcNow.Date;  // 00:00 UTC today
                var nowUtc   = DateTime.UtcNow;
                double closedPnl = 0, netDep = 0;
                var traders = new HashSet<ulong>();
                var dealArr = mgr.DealCreateArray();
                if (mgr.DealRequestByGroup(group,
                        SMTTime.FromDateTime(dayStart),
                        SMTTime.FromDateTime(nowUtc), dealArr) == MTRetCode.MT_RET_OK)
                {
                    for (uint i = 0; i < dealArr.Total(); i++)
                    {
                        var d      = dealArr.Next(i);
                        uint action = d.Action();
                        double rate = d.RateProfit();
                        if (action == ACTION_BUY || action == ACTION_SELL)
                        {
                            traders.Add(d.Login());
                            if (d.Entry() != ENTRY_IN)
                                closedPnl += ToUsd(d.Profit() + d.Storage() + d.Commission(), rate);
                        }
                        else if (action == ACTION_BAL)
                        {
                            string c = (d.Comment() ?? "").ToLowerInvariant();
                            if (!c.Contains("bonus") && !c.Contains("internal") && !c.Contains("transfer"))
                                netDep += ToUsd(d.Profit(), rate);
                        }
                    }
                }
                dealArr.Dispose();

                try { mgr.Disconnect(); } catch { }
                mgr.Dispose();

                // ── delta from previous reading ──────────────────────────────
                string deltaStr = double.IsNaN(prevFloat)
                    ? $"{"—",>14}"
                    : $"{floatPnl - prevFloat,>14:+#,##0.00;-#,##0.00}";
                prevFloat = floatPnl;

                int elapsed = (int)(DateTime.Now - t0).TotalSeconds;
                Console.WriteLine(
                    $"{DateTime.Now:HH:mm:ss}  {floatPnl,>22:N2}  {deltaStr}  " +
                    $"{closedPnl,>14:N2}  {netDep,>12:N2}  {nPos,>10:N0}  {traders.Count,>8:N0}" +
                    $"  ({elapsed}s)");
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[{DateTime.Now:HH:mm:ss}] ERROR: {ex.Message}");
            }

            Sleep:
            Thread.Sleep(interval * 1000);
        }
    }
}
