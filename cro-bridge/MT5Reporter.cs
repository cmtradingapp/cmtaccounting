using System;
using System.Globalization;
using Mt5Monitor.Api;

public static class MT5Reporter
{
    static int Main(string[] args)
    {
        if (args.Length < 2)
        {
            Console.Error.WriteLine("Usage: MT5Reporter.exe <report_type> [<from_date> <to_date>] <format>");
            Console.Error.WriteLine("  report_type: deposit-withdrawal | positions-history | trading-accounts");
            Console.Error.WriteLine("  format: json | csv  (positions-history: json only)");
            return 1;
        }

        string reportType = args[0].ToLowerInvariant();
        var settings = Mt5MonitorSettings.FromEnvironment();
        var ci = CultureInfo.InvariantCulture;

        try
        {
            if (reportType == "trading-accounts")
            {
                string fmt = args.Length > 1 ? args[1].ToLowerInvariant() : "json";
                string output = fmt == "csv"
                    ? Mt5TradingAccountsGenerator.GenerateCsv(settings)
                    : Mt5TradingAccountsGenerator.GenerateJson(settings);
                Console.Write(output);
                return 0;
            }

            if (args.Length < 4)
            {
                Console.Error.WriteLine("Expected: <report_type> <from_date> <to_date> <format>");
                return 1;
            }

            DateTime fromDate = DateTime.ParseExact(args[1], "yyyy-MM-dd", ci);
            DateTime toDate   = DateTime.ParseExact(args[2], "yyyy-MM-dd", ci);
            string format     = args[3].ToLowerInvariant();

            if (reportType == "deposit-withdrawal")
            {
                string output = format == "csv"
                    ? Mt5DepositWithdrawalGenerator.GenerateCsv(settings, fromDate, toDate)
                    : Mt5DepositWithdrawalGenerator.GenerateJson(settings, fromDate, toDate);
                Console.Write(output);
                return 0;
            }

            if (reportType == "positions-history")
            {
                string output = Mt5PositionHistoryGenerator.GenerateJson(settings, fromDate, toDate);
                Console.Write(output);
                return 0;
            }

            Console.Error.WriteLine("Unknown report type: " + reportType);
            return 1;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine("Error: " + ex.Message);
            return 1;
        }
    }
}
