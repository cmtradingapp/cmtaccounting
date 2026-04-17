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

        while (true)
        {
            var t0 = DateTime.Now;
            try
            {
                MTRetCode cm = MTRetCode.MT_RET_OK_NONE;
                CIMTManagerAPI mgr = SMTManagerAPIFactory.CreateManager(
                    SMTManagerAPIFactory.ManagerAPIVersion, out cm);
                if (mgr == null || cm != MTRetCode.MT_RET_OK)
                {
                    Console.Error.WriteLine("CreateManager: " + cm);
                    Thread.Sleep(interval * 1000);
                    continue;
                }

                if (mgr.Connect(server, login, pw, null,
                        CIMTManagerAPI.EnPumpModes.PUMP_MODE_NONE, 15000) != MTRetCode.MT_RET_OK)
                {
                    Console.Error.WriteLine("Connect failed");
                    mgr.Dispose();
                    Thread.Sleep(interval * 1000);
                    continue;
                }

                // Use server-computed Summary for floating PnL — matches MT5 Manager exactly.
                // SummaryCurrency("USD") tells the server to express all values in USD.
                // ProfitFullClients() = Profit + Storage for client positions, already in USD.
                double floatPnl = 0;
                int    nPos     = 0;
                mgr.SummaryCurrency("USD");
                var sumArr = mgr.SummaryCreateArray();
                if (mgr.SummaryGetAll(sumArr) == MTRetCode.MT_RET_OK)
                {
                    for (uint i = 0; i < sumArr.Total(); i++)
                    {
                        var s = sumArr.Next(i);
                        floatPnl += s.ProfitFullClients();
                        nPos     += (int)s.PositionClients();
                    }
                }
                sumArr.Release();

                // today's deals (UTC day boundary)
                DateTime dayStart = DateTime.UtcNow.Date;
                DateTime nowUtc   = DateTime.UtcNow;
                double closedPnl = 0, netDep = 0;
                var traders = new HashSet<ulong>();
                var dealArr = mgr.DealCreateArray();
                if (mgr.DealRequestByGroup(group,
                        SMTTime.FromDateTime(dayStart),
                        SMTTime.FromDateTime(nowUtc), dealArr) == MTRetCode.MT_RET_OK)
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

                try { mgr.Disconnect(); } catch { }
                mgr.Dispose();

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
                Console.Error.WriteLine("[" + DateTime.Now.ToString("HH:mm:ss") + "] ERROR: " + ex.Message);
            }

            Thread.Sleep(interval * 1000);
        }
    }
}
