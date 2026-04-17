// Continuous MT5 stats poller for manual testing.
// Connects once at startup and holds the connection; reconnects only on failure.
//
// Env vars: same as MT5Bridge.cs + MT5_INTERVAL (seconds, default 1)

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

    static CIMTManagerAPI Connect(string server, ulong login, string pw)
    {
        MTRetCode cm = MTRetCode.MT_RET_OK_NONE;
        CIMTManagerAPI mgr = SMTManagerAPIFactory.CreateManager(
            SMTManagerAPIFactory.ManagerAPIVersion, out cm);
        if (mgr == null || cm != MTRetCode.MT_RET_OK)
        {
            Console.Error.WriteLine("[monitor] CreateManager: " + cm);
            return null;
        }
        MTRetCode cr = mgr.Connect(server, login, pw, null,
            CIMTManagerAPI.EnPumpModes.PUMP_MODE_NONE, 15000);
        if (cr != MTRetCode.MT_RET_OK)
        {
            Console.Error.WriteLine("[monitor] Connect: " + cr + " (" + (uint)cr + ")");
            try { mgr.Disconnect(); } catch { }
            mgr.Dispose();
            return null;
        }
        return mgr;
    }

    public static int Main(string[] args)
    {
        var ci       = CultureInfo.InvariantCulture;
        var server   = Environment.GetEnvironmentVariable("MT5_SERVER")   ?? "";
        var loginStr = Environment.GetEnvironmentVariable("MT5_LOGIN")    ?? "0";
        var pw       = Environment.GetEnvironmentVariable("MT5_PASSWORD") ?? "";
        var group    = Environment.GetEnvironmentVariable("CRO_GROUP")    ?? "CMV*";
        var sdkDir   = Environment.GetEnvironmentVariable("MT5_SDK_LIBS") ?? @"C:\MetaTrader5SDK\Libs";
        int interval = int.Parse(Environment.GetEnvironmentVariable("MT5_INTERVAL") ?? "1", ci);

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

        Console.WriteLine("[monitor] group=" + group + "  interval=" + interval + "s  server=" + server);
        Console.WriteLine("[monitor] Ctrl+C to stop.\n");

        // header
        Console.WriteLine(string.Format("{0,-10}  {1,22}  {2,14}  {3,14}  {4,12}  {5,10}  {6,8}",
            "Time", "Floating PnL (USD)", "Delta Float", "Closed PnL", "Net Deposits", "Positions", "Traders"));
        Console.WriteLine(new string('-', 102));

        double prevFloat = double.NaN;

        // Connect once; reconnect only when a query fails
        CIMTManagerAPI mgr = null;
        while (mgr == null)
        {
            mgr = Connect(server, login, pw);
            if (mgr == null) Thread.Sleep(5000);
        }
        Console.Error.WriteLine("[monitor] Connected.");

        while (true)
        {
            var t0 = DateTime.Now;
            try
            {
                // --- floating PnL: CMV* group position scan ---
                // SummaryGetAll() is broker-wide (includes demo accounts = 2x positions/PnL).
                // PositionRequestByGroup filters to real client accounts only, matching
                // what MT5 Manager shows when filtered to the CMV* group.
                double floatPnl = 0;
                int    nPos     = 0;
                var posArr = mgr.PositionCreateArray();
                MTRetCode posRes = mgr.PositionRequestByGroup(group, posArr);
                if (posRes == MTRetCode.MT_RET_OK)
                {
                    nPos = (int)posArr.Total();
                    for (uint i = 0; i < posArr.Total(); i++)
                    {
                        var p = posArr.Next(i);
                        floatPnl += ToUsd(p.Profit() + p.Storage(), p.RateProfit());
                    }
                }
                else
                {
                    Console.Error.WriteLine("[monitor] PositionRequestByGroup: " + posRes + " -- reconnecting");
                    posArr.Dispose();
                    try { mgr.Disconnect(); } catch { }
                    mgr.Dispose(); mgr = null;
                    while (mgr == null) { mgr = Connect(server, login, pw); if (mgr == null) Thread.Sleep(5000); }
                    Thread.Sleep(interval * 1000);
                    continue;
                }
                posArr.Dispose();

                // --- today's deals (CMV* filtered) ---
                DateTime dayStart = DateTime.UtcNow.Date;
                DateTime nowUtc   = DateTime.UtcNow;
                double closedPnl = 0, netDep = 0;
                var traders = new HashSet<ulong>();
                var dealArr = mgr.DealCreateArray();
                MTRetCode dealRes = mgr.DealRequestByGroup(group,
                    SMTTime.FromDateTime(dayStart), SMTTime.FromDateTime(nowUtc), dealArr);
                if (dealRes == MTRetCode.MT_RET_OK)
                {
                    for (uint i = 0; i < dealArr.Total(); i++)
                    {
                        var    d      = dealArr.Next(i);
                        uint   action = d.Action();
                        double rate   = d.RateProfit();
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

                string deltaStr;
                if (double.IsNaN(prevFloat))
                    deltaStr = string.Format("{0,14}", "---");
                else
                    deltaStr = string.Format("{0,14:+#,##0.00;-#,##0.00;0.00}", floatPnl - prevFloat);
                prevFloat = floatPnl;

                int elapsed = (int)(DateTime.Now - t0).TotalSeconds;
                Console.WriteLine(string.Format(
                    "{0,-10}  {1,22:N2}  {2}  {3,14:N2}  {4,12:N2}  {5,10:N0}  {6,8:N0}  ({7}s)",
                    DateTime.Now.ToString("HH:mm:ss"),
                    floatPnl, deltaStr, closedPnl, netDep, nPos, traders.Count, elapsed));
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine("[" + DateTime.Now.ToString("HH:mm:ss") + "] ERROR: " + ex.Message + " -- reconnecting");
                try { mgr.Disconnect(); } catch { }
                try { mgr.Dispose(); } catch { }
                mgr = null;
                while (mgr == null) { mgr = Connect(server, login, pw); if (mgr == null) Thread.Sleep(5000); }
            }

            Thread.Sleep(interval * 1000);
        }
    }
}
