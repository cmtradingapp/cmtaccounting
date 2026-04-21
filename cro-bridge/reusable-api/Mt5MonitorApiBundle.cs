using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Json;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MetaQuotes.MT5CommonAPI;
using MetaQuotes.MT5ManagerAPI;

namespace Mt5Monitor.Api
{
    // Reusable single-file MT5 monitor API bundle.
    // Copy this file into another .NET Framework 4.8 project and add references to:
    // - MetaQuotes.MT5CommonAPI64.dll
    // - MetaQuotes.MT5ManagerAPI64.dll
    public interface IMt5MonitorFeed : IDisposable
    {
        event EventHandler<Mt5MonitorSnapshotEventArgs> SnapshotReceived;
        event EventHandler<Mt5MonitorStatusChangedEventArgs> StatusChanged;

        bool IsRunning { get; }

        void Start(Mt5MonitorSettings settings);
        void Stop();
    }

    public sealed class Mt5MonitorSettings
    {
        private const string LocalSettingsFileName = "monitor.local.env";

        public string Server { get; set; }
        public ulong Login { get; set; }
        public string Password { get; set; }
        public string GroupMask { get; set; }
        public string SdkLibsPath { get; set; }
        public int IntervalSeconds { get; set; }

        public Mt5MonitorSettings()
        {
            Server = string.Empty;
            Login = 0;
            Password = string.Empty;
            GroupMask = "CMV*";
            SdkLibsPath = ResolveDefaultSdkLibsPath();
            IntervalSeconds = 1;
        }

        public static Mt5MonitorSettings FromEnvironment()
        {
            var settings = new Mt5MonitorSettings();
            settings.ApplyLocalSettingsFile();

            string server = Environment.GetEnvironmentVariable("MT5_SERVER");
            if (!string.IsNullOrWhiteSpace(server))
                settings.Server = server;

            string password = Environment.GetEnvironmentVariable("MT5_PASSWORD");
            if (!string.IsNullOrWhiteSpace(password))
                settings.Password = password;

            string loginText = Environment.GetEnvironmentVariable("MT5_LOGIN") ?? "0";
            ulong loginValue;
            if (ulong.TryParse(loginText, NumberStyles.Integer, CultureInfo.InvariantCulture, out loginValue))
            {
                if (loginValue != 0)
                    settings.Login = loginValue;
            }

            string groupMask = Environment.GetEnvironmentVariable("CRO_GROUP");
            if (!string.IsNullOrWhiteSpace(groupMask))
                settings.GroupMask = groupMask;

            string sdkPath = Environment.GetEnvironmentVariable("MT5_SDK_LIBS");
            if (!string.IsNullOrWhiteSpace(sdkPath))
                settings.SdkLibsPath = sdkPath;

            string intervalText = Environment.GetEnvironmentVariable("MT5_INTERVAL") ?? "1";
            int intervalValue;
            if (int.TryParse(intervalText, NumberStyles.Integer, CultureInfo.InvariantCulture, out intervalValue) && intervalValue > 0)
                settings.IntervalSeconds = intervalValue;

            return settings;
        }

        public Mt5MonitorSettings Clone()
        {
            return new Mt5MonitorSettings
            {
                Server = Server,
                Login = Login,
                Password = Password,
                GroupMask = GroupMask,
                SdkLibsPath = SdkLibsPath,
                IntervalSeconds = IntervalSeconds
            };
        }

        public void Validate()
        {
            if (string.IsNullOrWhiteSpace(Server))
                throw new InvalidOperationException("MT5 server is required.");
            if (Login == 0)
                throw new InvalidOperationException("MT5 login is required.");
            if (string.IsNullOrWhiteSpace(Password))
                throw new InvalidOperationException("MT5 password is required.");
            if (string.IsNullOrWhiteSpace(GroupMask))
                throw new InvalidOperationException("Group mask is required.");
            if (IntervalSeconds < 1)
                throw new InvalidOperationException("Interval must be at least 1 second.");
            if (string.IsNullOrWhiteSpace(SdkLibsPath) || !Directory.Exists(SdkLibsPath))
                throw new InvalidOperationException("MT5 SDK libs path was not found: " + SdkLibsPath);

            EnsureSdkFile("MetaQuotes.MT5CommonAPI64.dll");
            EnsureSdkFile("MetaQuotes.MT5ManagerAPI64.dll");
            EnsureSdkFile("MT5APIManager64.dll");
        }

        private void ApplyLocalSettingsFile()
        {
            string settingsPath = FindLocalSettingsFile();
            if (string.IsNullOrWhiteSpace(settingsPath) || !File.Exists(settingsPath))
                return;

            foreach (string rawLine in File.ReadAllLines(settingsPath))
            {
                string line = rawLine.Trim();
                if (line.Length == 0 || line.StartsWith("#", StringComparison.Ordinal))
                    continue;

                int separator = line.IndexOf('=');
                if (separator <= 0)
                    continue;

                string key = line.Substring(0, separator).Trim();
                string value = line.Substring(separator + 1).Trim();
                ApplySetting(key, value);
            }
        }

        private void ApplySetting(string key, string value)
        {
            if (string.IsNullOrWhiteSpace(key))
                return;

            switch (key.Trim().ToUpperInvariant())
            {
                case "MT5_SERVER":
                    if (!string.IsNullOrWhiteSpace(value))
                        Server = value;
                    break;

                case "MT5_LOGIN":
                    ulong loginValue;
                    if (ulong.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out loginValue) && loginValue != 0)
                        Login = loginValue;
                    break;

                case "MT5_PASSWORD":
                    if (!string.IsNullOrWhiteSpace(value))
                        Password = value;
                    break;

                case "CRO_GROUP":
                    if (!string.IsNullOrWhiteSpace(value))
                        GroupMask = value;
                    break;

                case "MT5_SDK_LIBS":
                    if (!string.IsNullOrWhiteSpace(value))
                        SdkLibsPath = value;
                    break;

                case "MT5_INTERVAL":
                    int intervalValue;
                    if (int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out intervalValue) && intervalValue > 0)
                        IntervalSeconds = intervalValue;
                    break;
            }
        }

        private static string FindLocalSettingsFile()
        {
            foreach (string root in EnumerateSearchRoots())
            {
                string candidate = Path.Combine(root, LocalSettingsFileName);
                if (File.Exists(candidate))
                    return candidate;
            }

            return null;
        }

        private void EnsureSdkFile(string fileName)
        {
            string fullPath = Path.Combine(SdkLibsPath, fileName);
            if (!File.Exists(fullPath))
                throw new InvalidOperationException("Required SDK file not found: " + fullPath);
        }

        private static string ResolveDefaultSdkLibsPath()
        {
            string envPath = Environment.GetEnvironmentVariable("MT5_SDK_LIBS");
            if (!string.IsNullOrWhiteSpace(envPath))
                return envPath;

            foreach (string root in EnumerateSearchRoots())
            {
                string mt5Sdk = Path.Combine(root, "MT5SDK", "Libs");
                if (Directory.Exists(mt5Sdk))
                    return Path.GetFullPath(mt5Sdk);

                string bridgeSdk = Path.Combine(root, "MT5Bridge", "sdk-libs");
                if (Directory.Exists(bridgeSdk))
                    return Path.GetFullPath(bridgeSdk);
            }

            return @"C:\MetaTrader5SDK\Libs";
        }

        private static IEnumerable<string> EnumerateSearchRoots()
        {
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            string[] roots =
            {
                Environment.CurrentDirectory,
                AppDomain.CurrentDomain.BaseDirectory
            };

            for (int i = 0; i < roots.Length; i++)
            {
                string current = roots[i];
                if (string.IsNullOrWhiteSpace(current))
                    continue;

                DirectoryInfo dir = new DirectoryInfo(current);
                while (dir != null)
                {
                    string fullName = dir.FullName;
                    if (seen.Add(fullName))
                        yield return fullName;
                    dir = dir.Parent;
                }
            }
        }
    }

    public sealed class Mt5SymbolSummaryRow
    {
        public string Symbol { get; set; }
        public int Digits { get; set; }
        public int ClientPositions { get; set; }
        public int CoveragePositions { get; set; }
        public double ClientBuyVolume { get; set; }
        public double CoverageBuyVolume { get; set; }
        public double ClientBuyPrice { get; set; }
        public double CoverageBuyPrice { get; set; }
        public double ClientSellVolume { get; set; }
        public double CoverageSellVolume { get; set; }
        public double ClientSellPrice { get; set; }
        public double CoverageSellPrice { get; set; }
        public double NetVolume { get; set; }
        public double ClientProfitUsd { get; set; }
        public double CoverageProfitUsd { get; set; }
        public double UncoveredUsd { get; set; }
    }

    public sealed class Mt5SymbolSummaryTotals
    {
        public int ClientPositions { get; set; }
        public int CoveragePositions { get; set; }
        public double ClientBuyVolume { get; set; }
        public double CoverageBuyVolume { get; set; }
        public double ClientSellVolume { get; set; }
        public double CoverageSellVolume { get; set; }
        public double NetVolume { get; set; }
        public double ClientProfitUsd { get; set; }
        public double CoverageProfitUsd { get; set; }
        public double UncoveredUsd { get; set; }
    }

    public sealed class Mt5PositionAuditRow
    {
        public ulong Position { get; set; }
        public ulong Login { get; set; }
        public string Group { get; set; }
        public string GroupCurrency { get; set; }
        public string DepositCurrency { get; set; }
        public string Symbol { get; set; }
        public string Side { get; set; }
        public ulong RawVolume { get; set; }
        public double VolumeLots { get; set; }
        public int Digits { get; set; }
        public int CurrencyDigits { get; set; }
        public double ContractSize { get; set; }
        public double PriceOpen { get; set; }
        public double PriceCurrent { get; set; }
        public double RateProfit { get; set; }
        public double ProfitNative { get; set; }
        public double StorageNative { get; set; }
        public double NativeTotal { get; set; }
        public string ProfitFxSymbol { get; set; }
        public double ProfitFxBid { get; set; }
        public double ProfitFxAsk { get; set; }
        public bool ProfitFxUsdBase { get; set; }
        public double ProfitToUsdRate { get; set; }
        public bool ProfitUsedFallback { get; set; }
        public double ProfitUsd { get; set; }
        public string StorageFxSymbol { get; set; }
        public double StorageFxBid { get; set; }
        public double StorageFxAsk { get; set; }
        public bool StorageFxUsdBase { get; set; }
        public double StorageToUsdRate { get; set; }
        public bool StorageUsedFallback { get; set; }
        public double StorageUsd { get; set; }
        public double FloatingUsd { get; set; }
    }

    public sealed class Mt5MonitorSnapshot
    {
        public Mt5MonitorSnapshot()
        {
            SymbolSummaryRows = new List<Mt5SymbolSummaryRow>();
            SymbolSummaryTotals = new Mt5SymbolSummaryTotals();
            PositionAuditRows = new List<Mt5PositionAuditRow>();
            MissingCurrencyRates = new List<string>();
            ConversionSummary = "FX conversion: live bid/ask FX rates.";
        }

        public DateTime LocalTimestamp { get; set; }
        public DateTime UtcTimestamp { get; set; }
        public double FloatingPnlUsd { get; set; }
        public double? FloatingPnlDeltaUsd { get; set; }
        public double ClosedPnlUsd { get; set; }
        public double NetDepositsUsd { get; set; }
        public int PositionCount { get; set; }
        public int TraderCount { get; set; }
        public IList<Mt5SymbolSummaryRow> SymbolSummaryRows { get; set; }
        public Mt5SymbolSummaryTotals SymbolSummaryTotals { get; set; }
        public IList<Mt5PositionAuditRow> PositionAuditRows { get; set; }
        public IList<string> MissingCurrencyRates { get; set; }
        public int FallbackConversionCount { get; set; }
        public string ConversionSummary { get; set; }
    }

    public sealed class Mt5DailyReportRow
    {
        public DateTime Timestamp { get; set; }
        public ulong Login { get; set; }
        public string Name { get; set; }
        public string Group { get; set; }
        public string Currency { get; set; }
        public int CurrencyDigits { get; set; }
        public double PrevBalance { get; set; }
        public double Deposit { get; set; }
        public double ClosedPnl { get; set; }
        public double EquityPrevDay { get; set; }
        public double Balance { get; set; }
        public double Credit { get; set; }
        public double DailyCredit { get; set; }
        public double DailyBonus { get; set; }
        public double FloatingPnl { get; set; }
        public double Equity { get; set; }
        public double Margin { get; set; }
        public double FreeMargin { get; set; }
    }

    public sealed class Mt5DailyReportSnapshot
    {
        public Mt5DailyReportSnapshot()
        {
            Rows = new List<Mt5DailyReportRow>();
        }

        public DateTime GeneratedAt { get; set; }
        public DateTime RangeFrom { get; set; }
        public DateTime RangeTo { get; set; }
        public IList<Mt5DailyReportRow> Rows { get; set; }
    }

    public sealed class Mt5PositionHistoryRow
    {
        public ulong Login { get; set; }
        public string Name { get; set; }
        public DateTime OpenTime { get; set; }
        public ulong Ticket { get; set; }
        public string Type { get; set; }
        public double Volume { get; set; }
        public string Symbol { get; set; }
        public double OpenPrice { get; set; }
        public double StopLoss { get; set; }
        public double TakeProfit { get; set; }
        public DateTime CloseTime { get; set; }
        public double ClosePrice { get; set; }
        public string Reason { get; set; }
        public double Commission { get; set; }
        public double Fee { get; set; }
        public double Swap { get; set; }
        public double Profit { get; set; }
        public string Currency { get; set; }
        public string Comment { get; set; }
        public int Digits { get; set; }
        public int CurrencyDigits { get; set; }
    }

    public sealed class Mt5PositionHistoryCurrencyTotal
    {
        public string Currency { get; set; }
        public int CurrencyDigits { get; set; }
        public double Commission { get; set; }
        public double Fee { get; set; }
        public double Swap { get; set; }
        public double Profit { get; set; }
    }

    public sealed class Mt5PositionHistorySnapshot
    {
        public Mt5PositionHistorySnapshot()
        {
            Rows = new List<Mt5PositionHistoryRow>();
            CurrencyTotals = new List<Mt5PositionHistoryCurrencyTotal>();
        }

        public DateTime GeneratedAt { get; set; }
        public DateTime RangeFrom { get; set; }
        public DateTime RangeTo { get; set; }
        public IList<Mt5PositionHistoryRow> Rows { get; set; }
        public IList<Mt5PositionHistoryCurrencyTotal> CurrencyTotals { get; set; }
    }

    public sealed class Mt5TradingAccountRow
    {
        public ulong Login { get; set; }
        public string Name { get; set; }
        public string Group { get; set; }
        public string Currency { get; set; }
        public int CurrencyDigits { get; set; }
        public double Balance { get; set; }
        public double Credit { get; set; }
        public double Profit { get; set; }
        public double Equity { get; set; }
        public double Margin { get; set; }
        public double FreeMargin { get; set; }
        public double MarginLevel { get; set; }
        public uint MarginLeverage { get; set; }
    }

    public sealed class Mt5TradingAccountsSnapshot
    {
        public Mt5TradingAccountsSnapshot()
        {
            Rows = new List<Mt5TradingAccountRow>();
        }

        public DateTime GeneratedAt { get; set; }
        public IList<Mt5TradingAccountRow> Rows { get; set; }
    }

    public sealed class Mt5DepositWithdrawalRow
    {
        public ulong Deal { get; set; }
        public ulong Login { get; set; }
        public string Name { get; set; }
        public string Group { get; set; }
        public DateTime Time { get; set; }
        public string Comment { get; set; }
        public double Amount { get; set; }
        public string Currency { get; set; }
        public int CurrencyDigits { get; set; }
    }

    public sealed class Mt5DepositWithdrawalCurrencyTotal
    {
        public string Currency { get; set; }
        public int CurrencyDigits { get; set; }
        public int DealCount { get; set; }
        public double Deposits { get; set; }
        public double Withdrawals { get; set; }
        public double NetAmount { get; set; }
    }

    public sealed class Mt5DepositWithdrawalSnapshot
    {
        public Mt5DepositWithdrawalSnapshot()
        {
            Rows = new List<Mt5DepositWithdrawalRow>();
            CurrencyTotals = new List<Mt5DepositWithdrawalCurrencyTotal>();
        }

        public DateTime GeneratedAt { get; set; }
        public DateTime RangeFrom { get; set; }
        public DateTime RangeTo { get; set; }
        public IList<Mt5DepositWithdrawalRow> Rows { get; set; }
        public IList<Mt5DepositWithdrawalCurrencyTotal> CurrencyTotals { get; set; }
    }

    public sealed class Mt5UsdConversionRate
    {
        public string Currency { get; set; }
        public string FxSymbol { get; set; }
        public double Bid { get; set; }
        public double Ask { get; set; }
        public bool UsdBase { get; set; }
        public double PositiveToUsd { get; set; }
        public double NegativeToUsd { get; set; }
    }

    public enum Mt5WdEquityZComputationMode
    {
        EndOnly = 0,
        DeltaFromStartWhenBothPositive = 1
    }

    public sealed class Mt5WdEquityZRequest
    {
        public Mt5WdEquityZRequest()
        {
            BonusCommentContains = "Bonus Protected Trad";
            ComputationMode = Mt5WdEquityZComputationMode.EndOnly;
            IncludeBonusDealRows = true;
            UseLatestAvailableDailyReportDateWhenMissing = true;
            LatestAvailableDailyReportLookbackDays = 7;
        }

        public DateTime ReportDate { get; set; }
        public DateTime BonusHistoryFrom { get; set; }
        public string BonusCommentContains { get; set; }
        public Mt5WdEquityZComputationMode ComputationMode { get; set; }
        public bool IncludeBonusDealRows { get; set; }
        public bool UseLatestAvailableDailyReportDateWhenMissing { get; set; }
        public int LatestAvailableDailyReportLookbackDays { get; set; }
    }

    public sealed class Mt5WdEquityZProtectedBonusDeal
    {
        public ulong Deal { get; set; }
        public ulong Login { get; set; }
        public string Name { get; set; }
        public string Group { get; set; }
        public DateTime Time { get; set; }
        public string Currency { get; set; }
        public int CurrencyDigits { get; set; }
        public string Comment { get; set; }
        public double Amount { get; set; }
        public double AmountUsd { get; set; }
    }

    public sealed class Mt5WdEquityZInputs
    {
        public Mt5WdEquityZInputs()
        {
            MissingCurrencyRates = new List<string>();
            Assumptions = new List<string>();
            ComputationMode = Mt5WdEquityZComputationMode.EndOnly;
        }

        public DateTime ReportDate { get; set; }
        public Mt5WdEquityZComputationMode ComputationMode { get; set; }
        public double EndEquityUsd { get; set; }
        public double EndCreditsUsd { get; set; }
        public double EndProtectedBonusesUsd { get; set; }
        public double StartEquityUsd { get; set; }
        public double StartCreditsUsd { get; set; }
        public double StartProtectedBonusesUsd { get; set; }
        public IList<string> MissingCurrencyRates { get; set; }
        public IList<string> Assumptions { get; set; }
    }

	    public sealed class Mt5WdEquityZReport
	    {
	        public Mt5WdEquityZReport()
	        {
	            MissingCurrencyRates = new List<string>();
            Assumptions = new List<string>();
            DailyRows = new List<Mt5DailyReportRow>();
            ProtectedBonusDeals = new List<Mt5WdEquityZProtectedBonusDeal>();
        }

        public DateTime GeneratedAt { get; set; }
        public DateTime RequestedReportDate { get; set; }
        public DateTime ReportDate { get; set; }
        public DateTime BonusHistoryFrom { get; set; }
        public string BonusCommentContains { get; set; }
        public Mt5WdEquityZComputationMode ComputationMode { get; set; }
        public bool UsedLatestAvailableDailyReportDateFallback { get; set; }
        public int DailyRowCount { get; set; }
        public int ProtectedBonusDealCount { get; set; }
        public double EndEquityUsd { get; set; }
        public double EndCreditsUsd { get; set; }
        public double EndProtectedBonusesUsd { get; set; }
        public double EndWdEquityUsd { get; set; }
        public double StartEquityUsd { get; set; }
        public double StartCreditsUsd { get; set; }
        public double StartProtectedBonusesUsd { get; set; }
        public double StartWdEquityUsd { get; set; }
        public double WdEquityZUsd { get; set; }
        public string CalculationSummary { get; set; }
        public IList<string> MissingCurrencyRates { get; set; }
        public IList<string> Assumptions { get; set; }
	        public IList<Mt5DailyReportRow> DailyRows { get; set; }
	        public IList<Mt5WdEquityZProtectedBonusDeal> ProtectedBonusDeals { get; set; }
	    }

    public sealed class Mt5LiveWdEquityZRequest
    {
        public Mt5LiveWdEquityZRequest()
        {
            AsOfUtc = DateTime.UtcNow;
            FilterZeroEquityAndBalance = true;
            BonusScopePositiveBalanceOnly = true;
            IncludeCrmBonusLoginRows = false;
            ExcludedGroups = new List<string>();
        }

        public DateTime AsOfUtc { get; set; }
        public bool FilterZeroEquityAndBalance { get; set; }
        public bool BonusScopePositiveBalanceOnly { get; set; }
        public bool IncludeCrmBonusLoginRows { get; set; }
        public IList<string> ExcludedGroups { get; set; }
    }

    public sealed class Mt5LiveWdEquityZCrmBonusLoginTotal
    {
        public ulong Login { get; set; }
        public double NetAmountUsd { get; set; }
        public int TransactionCount { get; set; }
    }

    public sealed class Mt5LiveWdEquityZCrmBonusTotals
    {
        public Mt5LiveWdEquityZCrmBonusTotals()
        {
            LoginTotals = new List<Mt5LiveWdEquityZCrmBonusLoginTotal>();
        }

        public DateTime AsOfUtc { get; set; }
        public int ScopeLoginCount { get; set; }
        public int MatchedLoginCount { get; set; }
        public int MatchedTransactionCount { get; set; }
        public double TotalCumulativeBonusUsd { get; set; }
        public IList<Mt5LiveWdEquityZCrmBonusLoginTotal> LoginTotals { get; set; }
    }

    public sealed class Mt5LiveWdEquityZReport
    {
        public Mt5LiveWdEquityZReport()
        {
            MissingCurrencyRates = new List<string>();
            Assumptions = new List<string>();
            CrmBonusLogins = new List<Mt5LiveWdEquityZCrmBonusLoginTotal>();
            Source = "trading_accounts_live_balance_plus_floating_minus_crm_bonus";
        }

        public DateTime GeneratedAt { get; set; }
        public DateTime AsOfUtc { get; set; }
        public DateTime CrmQueryAsOfUtc { get; set; }
        public string Source { get; set; }
        public bool FilterZeroEquityAndBalance { get; set; }
        public bool BonusScopePositiveBalanceOnly { get; set; }
        public int RawAccountCount { get; set; }
        public int IncludedAccountCount { get; set; }
        public int SkippedZeroEquityCount { get; set; }
        public int SkippedZeroBalanceCount { get; set; }
        public int SkippedExcludedGroupCount { get; set; }
        public int BonusScopeLoginCount { get; set; }
        public int CrmMatchedLoginCount { get; set; }
        public int CrmMatchedTransactionCount { get; set; }
        public double BalanceUsdTotal { get; set; }
        public double FloatingPnlUsdTotal { get; set; }
        public double CumulativeBonusUsd { get; set; }
        public double PreClampWdEquityUsd { get; set; }
        public double WdEquityZUsd { get; set; }
        public string CalculationSummary { get; set; }
        public IList<string> MissingCurrencyRates { get; set; }
        public IList<string> Assumptions { get; set; }
        public IList<Mt5LiveWdEquityZCrmBonusLoginTotal> CrmBonusLogins { get; set; }
    }

    public sealed class Mt5DailyPnlCashRequest
    {
        public Mt5DailyPnlCashRequest()
        {
            BonusCommentContains = "Bonus Protected Trad";
	            NetDepositExcludedCommentContains = new List<string>
	            {
	                "bonus",
	                "cash on balance bonus",
	                "internal",
	                "transfer"
            };
            IncludeBonusDealRows = true;
            IncludeNetDepositDealRows = true;
            UseLatestAvailableDailyReportDateWhenMissing = true;
            LatestAvailableDailyReportLookbackDays = 7;
        }

	        public DateTime ReportDate { get; set; }
	        public DateTime BonusHistoryFrom { get; set; }
	        public string BonusCommentContains { get; set; }
        public IList<string> NetDepositExcludedCommentContains { get; set; }
        public bool IncludeBonusDealRows { get; set; }
        public bool IncludeNetDepositDealRows { get; set; }
        public bool UseLatestAvailableDailyReportDateWhenMissing { get; set; }
        public int LatestAvailableDailyReportLookbackDays { get; set; }
    }

	    public sealed class Mt5DailyPnlCashNetDepositDeal
	    {
	        public ulong Deal { get; set; }
	        public ulong Login { get; set; }
	        public string Name { get; set; }
	        public string Group { get; set; }
	        public DateTime Time { get; set; }
	        public string Currency { get; set; }
	        public int CurrencyDigits { get; set; }
	        public string Comment { get; set; }
	        public double Amount { get; set; }
	        public double AmountUsd { get; set; }
	    }

	    public sealed class Mt5DailyPnlCashInputs
	    {
	        public Mt5DailyPnlCashInputs()
	        {
	            MissingCurrencyRates = new List<string>();
	            Assumptions = new List<string>();
	        }

	        public DateTime ReportDate { get; set; }
	        public double EndEquityUsd { get; set; }
	        public double EndCreditsUsd { get; set; }
	        public double EndProtectedBonusesUsd { get; set; }
	        public double StartEquityUsd { get; set; }
	        public double StartCreditsUsd { get; set; }
	        public double StartProtectedBonusesUsd { get; set; }
	        public double NetDepositsUsd { get; set; }
	        public IList<string> MissingCurrencyRates { get; set; }
	        public IList<string> Assumptions { get; set; }
	    }

	    public sealed class Mt5DailyPnlCashReport
	    {
	        public Mt5DailyPnlCashReport()
	        {
	            MissingCurrencyRates = new List<string>();
	            Assumptions = new List<string>();
	            NetDepositExcludedCommentContains = new List<string>();
	            DailyRows = new List<Mt5DailyReportRow>();
	            ProtectedBonusDeals = new List<Mt5WdEquityZProtectedBonusDeal>();
	            NetDepositDeals = new List<Mt5DailyPnlCashNetDepositDeal>();
	        }

        public DateTime GeneratedAt { get; set; }
        public DateTime RequestedReportDate { get; set; }
        public DateTime ReportDate { get; set; }
        public DateTime BonusHistoryFrom { get; set; }
        public string BonusCommentContains { get; set; }
        public IList<string> NetDepositExcludedCommentContains { get; set; }
        public bool UsedLatestAvailableDailyReportDateFallback { get; set; }
        public int DailyRowCount { get; set; }
	        public int ProtectedBonusDealCount { get; set; }
	        public int NetDepositDealCount { get; set; }
	        public double EndEquityUsd { get; set; }
	        public double EndCreditsUsd { get; set; }
	        public double EndProtectedBonusesUsd { get; set; }
	        public double EndCleanEquityUsd { get; set; }
	        public double EndPositiveCleanEquityUsd { get; set; }
	        public double StartEquityUsd { get; set; }
	        public double StartCreditsUsd { get; set; }
	        public double StartProtectedBonusesUsd { get; set; }
	        public double StartCleanEquityUsd { get; set; }
	        public double StartPositiveCleanEquityUsd { get; set; }
	        public double NetDepositsUsd { get; set; }
	        public double DailyPnlCashUsd { get; set; }
	        public string CalculationSummary { get; set; }
	        public IList<string> MissingCurrencyRates { get; set; }
	        public IList<string> Assumptions { get; set; }
	        public IList<Mt5DailyReportRow> DailyRows { get; set; }
	        public IList<Mt5WdEquityZProtectedBonusDeal> ProtectedBonusDeals { get; set; }
	        public IList<Mt5DailyPnlCashNetDepositDeal> NetDepositDeals { get; set; }
	    }

	    public sealed class Mt5DailyClosedPnlCurrencyBreakdown
	    {
	        public string Currency { get; set; }
        public int CurrencyDigits { get; set; }
        public string FxSymbol { get; set; }
        public double FxBid { get; set; }
        public double FxAsk { get; set; }
        public bool FxUsdBase { get; set; }
        public double PositiveToUsdRate { get; set; }
        public double NegativeToUsdRate { get; set; }
        public bool MissingRate { get; set; }
        public double CommissionNative { get; set; }
        public double FeeNative { get; set; }
        public double SwapNative { get; set; }
        public double ProfitNative { get; set; }
        public double NativeClosedPnl { get; set; }
        public double CommissionUsd { get; set; }
        public double FeeUsd { get; set; }
        public double SwapUsd { get; set; }
        public double ProfitUsd { get; set; }
        public double ClosedPnlUsd { get; set; }
    }

    public sealed class Mt5DailyClosedPnlResult
    {
        public Mt5DailyClosedPnlResult()
        {
            CurrencyBreakdowns = new List<Mt5DailyClosedPnlCurrencyBreakdown>();
            MissingCurrencyRates = new List<string>();
        }

        public DateTime GeneratedAt { get; set; }
        public DateTime? RangeFrom { get; set; }
        public DateTime? RangeTo { get; set; }
        public int SourceRowCount { get; set; }
        public int SourceCurrencyCount { get; set; }
        public double TotalClosedPnlUsd { get; set; }
        public string ConversionSummary { get; set; }
        public IList<string> MissingCurrencyRates { get; set; }
        public IList<Mt5DailyClosedPnlCurrencyBreakdown> CurrencyBreakdowns { get; set; }
    }

    public interface IMt5DailyClosedPnlCalculator
    {
        Mt5DailyClosedPnlResult Calculate(
            Mt5PositionHistorySnapshot snapshot,
            IDictionary<string, Mt5UsdConversionRate> usdRates);

        Mt5DailyClosedPnlResult Calculate(
            IEnumerable<Mt5PositionHistoryRow> rows,
            IDictionary<string, Mt5UsdConversionRate> usdRates);

        Mt5DailyClosedPnlResult Calculate(
            IEnumerable<Mt5PositionHistoryCurrencyTotal> currencyTotals,
            IDictionary<string, Mt5UsdConversionRate> usdRates);
    }

	    public interface IMt5WdEquityZCalculator
	    {
	        Mt5WdEquityZReport Calculate(Mt5WdEquityZInputs inputs);

        Mt5WdEquityZReport Calculate(
            DateTime reportDate,
            IEnumerable<Mt5DailyReportRow> dailyRows,
            IDictionary<string, Mt5UsdConversionRate> usdRates,
            double startProtectedBonusesUsd,
	            double endProtectedBonusesUsd,
	            Mt5WdEquityZComputationMode computationMode);
	    }

    public interface IMt5LiveWdEquityZCalculator
    {
        Mt5LiveWdEquityZReport Calculate(
            IEnumerable<Mt5TradingAccountRow> filteredAccounts,
            IDictionary<string, Mt5UsdConversionRate> usdRates,
            Mt5LiveWdEquityZCrmBonusTotals crmBonusTotals,
            Mt5LiveWdEquityZRequest request);
    }

	    public interface IMt5DailyPnlCashCalculator
	    {
	        Mt5DailyPnlCashReport Calculate(Mt5DailyPnlCashInputs inputs);

	        Mt5DailyPnlCashReport Calculate(
	            DateTime reportDate,
	            IEnumerable<Mt5DailyReportRow> dailyRows,
	            IDictionary<string, Mt5UsdConversionRate> usdRates,
	            double startProtectedBonusesUsd,
	            double endProtectedBonusesUsd,
	            double netDepositsUsd);
	    }

	    public sealed class Mt5DailyReportJsonRow
	    {
        public string Timestamp { get; set; }
        public ulong Login { get; set; }
        public string Name { get; set; }
        public string Group { get; set; }
        public string Currency { get; set; }
        public int CurrencyDigits { get; set; }
        public double PrevBalance { get; set; }
        public double Deposit { get; set; }
        public double ClosedPnl { get; set; }
        public double EquityPrevDay { get; set; }
        public double Balance { get; set; }
        public double Credit { get; set; }
        public double DailyCredit { get; set; }
        public double DailyBonus { get; set; }
        public double FloatingPnl { get; set; }
        public double Equity { get; set; }
        public double Margin { get; set; }
        public double FreeMargin { get; set; }
    }

    public sealed class Mt5DailyReportJsonDocument
    {
        public Mt5DailyReportJsonDocument()
        {
            Rows = new List<Mt5DailyReportJsonRow>();
        }

        public string ReportType { get; set; }
        public string Server { get; set; }
        public ulong Login { get; set; }
        public string GroupMask { get; set; }
        public string GeneratedAt { get; set; }
        public string RangeFrom { get; set; }
        public string RangeTo { get; set; }
        public int RowCount { get; set; }
        public IList<Mt5DailyReportJsonRow> Rows { get; set; }
    }

    public sealed class Mt5PositionHistoryJsonRow
    {
        public ulong Login { get; set; }
        public string Name { get; set; }
        public string OpenTime { get; set; }
        public ulong Ticket { get; set; }
        public string Type { get; set; }
        public double Volume { get; set; }
        public string Symbol { get; set; }
        public double OpenPrice { get; set; }
        public double StopLoss { get; set; }
        public double TakeProfit { get; set; }
        public string CloseTime { get; set; }
        public double ClosePrice { get; set; }
        public string Reason { get; set; }
        public double Commission { get; set; }
        public double Fee { get; set; }
        public double Swap { get; set; }
        public double Profit { get; set; }
        public string Currency { get; set; }
        public string Comment { get; set; }
        public int Digits { get; set; }
        public int CurrencyDigits { get; set; }
    }

    public sealed class Mt5PositionHistoryJsonCurrencyTotal
    {
        public string Currency { get; set; }
        public int CurrencyDigits { get; set; }
        public double Commission { get; set; }
        public double Fee { get; set; }
        public double Swap { get; set; }
        public double Profit { get; set; }
    }

    public sealed class Mt5PositionHistoryJsonDocument
    {
        public Mt5PositionHistoryJsonDocument()
        {
            Rows = new List<Mt5PositionHistoryJsonRow>();
            CurrencyTotals = new List<Mt5PositionHistoryJsonCurrencyTotal>();
        }

        public string ReportType { get; set; }
        public string Server { get; set; }
        public ulong Login { get; set; }
        public string GroupMask { get; set; }
        public string GeneratedAt { get; set; }
        public string RangeFrom { get; set; }
        public string RangeTo { get; set; }
        public int RowCount { get; set; }
        public int CurrencyTotalCount { get; set; }
        public IList<Mt5PositionHistoryJsonRow> Rows { get; set; }
        public IList<Mt5PositionHistoryJsonCurrencyTotal> CurrencyTotals { get; set; }
    }

    public sealed class Mt5TradingAccountJsonRow
    {
        public ulong Login { get; set; }
        public string Name { get; set; }
        public string Group { get; set; }
        public string Currency { get; set; }
        public int CurrencyDigits { get; set; }
        public double Balance { get; set; }
        public double Credit { get; set; }
        public double Profit { get; set; }
        public double Equity { get; set; }
        public double Margin { get; set; }
        public double FreeMargin { get; set; }
        public double MarginLevel { get; set; }
        public uint MarginLeverage { get; set; }
    }

    public sealed class Mt5TradingAccountsJsonDocument
    {
        public Mt5TradingAccountsJsonDocument()
        {
            Rows = new List<Mt5TradingAccountJsonRow>();
        }

        public string ReportType { get; set; }
        public string Server { get; set; }
        public ulong Login { get; set; }
        public string GroupMask { get; set; }
        public string GeneratedAt { get; set; }
        public int RowCount { get; set; }
        public IList<Mt5TradingAccountJsonRow> Rows { get; set; }
    }

    public sealed class Mt5DepositWithdrawalJsonRow
    {
        public ulong Deal { get; set; }
        public ulong Login { get; set; }
        public string Name { get; set; }
        public string Group { get; set; }
        public string Time { get; set; }
        public string Comment { get; set; }
        public double Amount { get; set; }
        public string Currency { get; set; }
        public int CurrencyDigits { get; set; }
    }

    public sealed class Mt5DepositWithdrawalJsonCurrencyTotal
    {
        public string Currency { get; set; }
        public int CurrencyDigits { get; set; }
        public int DealCount { get; set; }
        public double Deposits { get; set; }
        public double Withdrawals { get; set; }
        public double NetAmount { get; set; }
    }

    public sealed class Mt5DepositWithdrawalJsonDocument
    {
        public Mt5DepositWithdrawalJsonDocument()
        {
            Rows = new List<Mt5DepositWithdrawalJsonRow>();
            CurrencyTotals = new List<Mt5DepositWithdrawalJsonCurrencyTotal>();
        }

        public string ReportType { get; set; }
        public string Server { get; set; }
        public ulong Login { get; set; }
        public string GroupMask { get; set; }
        public string GeneratedAt { get; set; }
        public string RangeFrom { get; set; }
        public string RangeTo { get; set; }
        public int RowCount { get; set; }
        public int CurrencyTotalCount { get; set; }
        public IList<Mt5DepositWithdrawalJsonRow> Rows { get; set; }
        public IList<Mt5DepositWithdrawalJsonCurrencyTotal> CurrencyTotals { get; set; }
    }

    public sealed class Mt5MonitorSnapshotEventArgs : EventArgs
    {
        public Mt5MonitorSnapshotEventArgs(Mt5MonitorSnapshot snapshot)
        {
            Snapshot = snapshot;
        }

        public Mt5MonitorSnapshot Snapshot { get; private set; }
    }

    public sealed class Mt5MonitorStatusChangedEventArgs : EventArgs
    {
        public Mt5MonitorStatusChangedEventArgs(string message, bool connected)
        {
            Message = message;
            Connected = connected;
            Timestamp = DateTime.Now;
        }

        public string Message { get; private set; }
        public bool Connected { get; private set; }
        public DateTime Timestamp { get; private set; }
    }

    public static class Mt5MonitorCsvExporter
    {
        public static void ExportSummarySnapshot(string path, Mt5MonitorSnapshot snapshot, Mt5MonitorSettings settings)
        {
            if (string.IsNullOrWhiteSpace(path))
                throw new ArgumentException("Export path is required.", "path");
            if (snapshot == null)
                throw new ArgumentNullException("snapshot");

            string directory = Path.GetDirectoryName(Path.GetFullPath(path));
            if (!string.IsNullOrWhiteSpace(directory))
                Directory.CreateDirectory(directory);

            File.WriteAllText(path, BuildSummarySnapshotCsv(snapshot, settings), new UTF8Encoding(true));
        }

        public static void ExportPositionAuditSnapshot(string path, Mt5MonitorSnapshot snapshot, Mt5MonitorSettings settings)
        {
            if (string.IsNullOrWhiteSpace(path))
                throw new ArgumentException("Export path is required.", "path");
            if (snapshot == null)
                throw new ArgumentNullException("snapshot");

            string directory = Path.GetDirectoryName(Path.GetFullPath(path));
            if (!string.IsNullOrWhiteSpace(directory))
                Directory.CreateDirectory(directory);

            File.WriteAllText(path, BuildPositionAuditCsv(snapshot, settings), new UTF8Encoding(true));
        }

        public static void ExportDailyReportSnapshot(string path, Mt5DailyReportSnapshot snapshot, Mt5MonitorSettings settings)
        {
            if (string.IsNullOrWhiteSpace(path))
                throw new ArgumentException("Export path is required.", "path");
            if (snapshot == null)
                throw new ArgumentNullException("snapshot");

            string directory = Path.GetDirectoryName(Path.GetFullPath(path));
            if (!string.IsNullOrWhiteSpace(directory))
                Directory.CreateDirectory(directory);

            File.WriteAllText(path, BuildDailyReportCsv(snapshot, settings), new UTF8Encoding(true));
        }

        public static void ExportPositionHistorySnapshot(string path, Mt5PositionHistorySnapshot snapshot, Mt5MonitorSettings settings)
        {
            if (string.IsNullOrWhiteSpace(path))
                throw new ArgumentException("Export path is required.", "path");
            if (snapshot == null)
                throw new ArgumentNullException("snapshot");

            string directory = Path.GetDirectoryName(Path.GetFullPath(path));
            if (!string.IsNullOrWhiteSpace(directory))
                Directory.CreateDirectory(directory);

            File.WriteAllText(path, BuildPositionHistoryCsv(snapshot, settings), new UTF8Encoding(true));
        }

        public static void ExportTradingAccountsSnapshot(string path, Mt5TradingAccountsSnapshot snapshot, Mt5MonitorSettings settings)
        {
            if (string.IsNullOrWhiteSpace(path))
                throw new ArgumentException("Export path is required.", "path");
            if (snapshot == null)
                throw new ArgumentNullException("snapshot");

            string directory = Path.GetDirectoryName(Path.GetFullPath(path));
            if (!string.IsNullOrWhiteSpace(directory))
                Directory.CreateDirectory(directory);

            File.WriteAllText(path, BuildTradingAccountsCsv(snapshot, settings), new UTF8Encoding(true));
        }

        public static void ExportDepositWithdrawalSnapshot(string path, Mt5DepositWithdrawalSnapshot snapshot, Mt5MonitorSettings settings)
        {
            if (string.IsNullOrWhiteSpace(path))
                throw new ArgumentException("Export path is required.", "path");
            if (snapshot == null)
                throw new ArgumentNullException("snapshot");

            string directory = Path.GetDirectoryName(Path.GetFullPath(path));
            if (!string.IsNullOrWhiteSpace(directory))
                Directory.CreateDirectory(directory);

            File.WriteAllText(path, BuildDepositWithdrawalCsv(snapshot, settings), new UTF8Encoding(true));
        }

        public static string BuildSummarySnapshotCsv(Mt5MonitorSnapshot snapshot, Mt5MonitorSettings settings)
        {
            if (snapshot == null)
                throw new ArgumentNullException("snapshot");

            var builder = new StringBuilder(4096);

            AppendMetadata(builder, "Report Type", "Manager Summary");
            AppendMetadata(builder, "Local Timestamp", snapshot.LocalTimestamp.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
            AppendMetadata(builder, "UTC Timestamp", snapshot.UtcTimestamp.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
            AppendMetadata(builder, "Server", settings != null ? settings.Server : string.Empty);
            AppendMetadata(builder, "Login", settings != null && settings.Login != 0 ? settings.Login.ToString(CultureInfo.InvariantCulture) : string.Empty);
            AppendMetadata(builder, "Group Mask", settings != null ? settings.GroupMask : string.Empty);
            AppendMetadata(builder, "Floating PnL (USD)", FormatMoney(snapshot.FloatingPnlUsd));
            AppendMetadata(builder, "Floating PnL Delta (USD)", FormatNullableMoney(snapshot.FloatingPnlDeltaUsd));
            AppendMetadata(builder, "Closed PnL (USD)", FormatMoney(snapshot.ClosedPnlUsd));
            AppendMetadata(builder, "Net Deposits (USD)", FormatMoney(snapshot.NetDepositsUsd));
            AppendMetadata(builder, "Positions", snapshot.PositionCount.ToString(CultureInfo.InvariantCulture));
            AppendMetadata(builder, "Traders", snapshot.TraderCount.ToString(CultureInfo.InvariantCulture));
            AppendMetadata(builder, "Conversion Summary", snapshot.ConversionSummary ?? string.Empty);
            AppendMetadata(builder, "Missing Currency Rates", snapshot.MissingCurrencyRates != null ? string.Join(", ", snapshot.MissingCurrencyRates) : string.Empty);
            AppendMetadata(builder, "Fallback Conversion Count", snapshot.FallbackConversionCount.ToString(CultureInfo.InvariantCulture));

            builder.AppendLine();
            AppendRow(
                builder,
                "Symbol",
                "Client Positions",
                "Coverage Positions",
                "Client Buy Volume",
                "Coverage Buy Volume",
                "Client Buy Price",
                "Coverage Buy Price",
                "Client Sell Volume",
                "Coverage Sell Volume",
                "Client Sell Price",
                "Coverage Sell Price",
                "Net Volume",
                "Client Profit (USD)",
                "Coverage Profit (USD)",
                "Uncovered (USD)");

            for (int i = 0; i < snapshot.SymbolSummaryRows.Count; i++)
            {
                Mt5SymbolSummaryRow row = snapshot.SymbolSummaryRows[i];
                AppendRow(
                    builder,
                    row.Symbol,
                    row.ClientPositions.ToString(CultureInfo.InvariantCulture),
                    row.CoveragePositions.ToString(CultureInfo.InvariantCulture),
                    FormatVolume(row.ClientBuyVolume),
                    FormatVolume(row.CoverageBuyVolume),
                    FormatPrice(row.ClientBuyPrice, row.Digits),
                    FormatPrice(row.CoverageBuyPrice, row.Digits),
                    FormatVolume(row.ClientSellVolume),
                    FormatVolume(row.CoverageSellVolume),
                    FormatPrice(row.ClientSellPrice, row.Digits),
                    FormatPrice(row.CoverageSellPrice, row.Digits),
                    FormatNetVolume(row.NetVolume),
                    FormatMoney(row.ClientProfitUsd),
                    FormatMoney(row.CoverageProfitUsd),
                    FormatMoney(row.UncoveredUsd));
            }

            Mt5SymbolSummaryTotals totals = snapshot.SymbolSummaryTotals ?? new Mt5SymbolSummaryTotals();
            AppendRow(
                builder,
                "Summary",
                totals.ClientPositions.ToString(CultureInfo.InvariantCulture),
                totals.CoveragePositions.ToString(CultureInfo.InvariantCulture),
                FormatVolume(totals.ClientBuyVolume),
                FormatVolume(totals.CoverageBuyVolume),
                string.Empty,
                string.Empty,
                FormatVolume(totals.ClientSellVolume),
                FormatVolume(totals.CoverageSellVolume),
                string.Empty,
                string.Empty,
                FormatNetVolume(totals.NetVolume),
                FormatMoney(totals.ClientProfitUsd),
                FormatMoney(totals.CoverageProfitUsd),
                FormatMoney(totals.UncoveredUsd));

            return builder.ToString();
        }

        public static string BuildPositionAuditCsv(Mt5MonitorSnapshot snapshot, Mt5MonitorSettings settings)
        {
            if (snapshot == null)
                throw new ArgumentNullException("snapshot");

            var builder = new StringBuilder(8192);

            AppendMetadata(builder, "Report Type", "Position Conversion Audit");
            AppendMetadata(builder, "Local Timestamp", snapshot.LocalTimestamp.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
            AppendMetadata(builder, "UTC Timestamp", snapshot.UtcTimestamp.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
            AppendMetadata(builder, "Server", settings != null ? settings.Server : string.Empty);
            AppendMetadata(builder, "Login", settings != null && settings.Login != 0 ? settings.Login.ToString(CultureInfo.InvariantCulture) : string.Empty);
            AppendMetadata(builder, "Group Mask", settings != null ? settings.GroupMask : string.Empty);
            AppendMetadata(builder, "Floating PnL (USD)", FormatAuditNumber(snapshot.FloatingPnlUsd));
            AppendMetadata(
                builder,
                "Position Rows",
                snapshot.PositionAuditRows != null ? snapshot.PositionAuditRows.Count.ToString(CultureInfo.InvariantCulture) : "0");
            AppendMetadata(builder, "Conversion Summary", snapshot.ConversionSummary ?? string.Empty);
            AppendMetadata(builder, "Missing Currency Rates", snapshot.MissingCurrencyRates != null ? string.Join(", ", snapshot.MissingCurrencyRates) : string.Empty);
            AppendMetadata(builder, "Fallback Conversion Count", snapshot.FallbackConversionCount.ToString(CultureInfo.InvariantCulture));

            builder.AppendLine();
            AppendRow(
                builder,
                "Position",
                "Login",
                "Group",
                "Group Currency",
                "Deposit Currency",
                "Symbol",
                "Side",
                "Raw Volume",
                "Volume Lots",
                "Digits",
                "Currency Digits",
                "Contract Size",
                "Price Open",
                "Price Current",
                "Rate Profit",
                "Profit Native",
                "Profit FX Symbol",
                "Profit FX Bid",
                "Profit FX Ask",
                "Profit FX USD Base",
                "Profit To USD Rate",
                "Profit Used Fallback",
                "Profit USD",
                "Storage Native",
                "Storage FX Symbol",
                "Storage FX Bid",
                "Storage FX Ask",
                "Storage FX USD Base",
                "Storage To USD Rate",
                "Storage Used Fallback",
                "Storage USD",
                "Native Total",
                "Floating USD");

            if (snapshot.PositionAuditRows != null)
            {
                for (int i = 0; i < snapshot.PositionAuditRows.Count; i++)
                {
                    Mt5PositionAuditRow row = snapshot.PositionAuditRows[i];
                    AppendRow(
                        builder,
                        row.Position.ToString(CultureInfo.InvariantCulture),
                        row.Login.ToString(CultureInfo.InvariantCulture),
                        row.Group,
                        row.GroupCurrency,
                        row.DepositCurrency,
                        row.Symbol,
                        row.Side,
                        row.RawVolume.ToString(CultureInfo.InvariantCulture),
                        FormatAuditNumber(row.VolumeLots),
                        row.Digits.ToString(CultureInfo.InvariantCulture),
                        row.CurrencyDigits.ToString(CultureInfo.InvariantCulture),
                        FormatAuditNumber(row.ContractSize),
                        FormatAuditPrice(row.PriceOpen, row.Digits),
                        FormatAuditPrice(row.PriceCurrent, row.Digits),
                        FormatAuditNumber(row.RateProfit),
                        FormatAuditNumber(row.ProfitNative),
                        row.ProfitFxSymbol,
                        FormatAuditNumber(row.ProfitFxBid),
                        FormatAuditNumber(row.ProfitFxAsk),
                        FormatBoolean(row.ProfitFxUsdBase),
                        FormatAuditNumber(row.ProfitToUsdRate),
                        FormatBoolean(row.ProfitUsedFallback),
                        FormatAuditNumber(row.ProfitUsd),
                        FormatAuditNumber(row.StorageNative),
                        row.StorageFxSymbol,
                        FormatAuditNumber(row.StorageFxBid),
                        FormatAuditNumber(row.StorageFxAsk),
                        FormatBoolean(row.StorageFxUsdBase),
                        FormatAuditNumber(row.StorageToUsdRate),
                        FormatBoolean(row.StorageUsedFallback),
                        FormatAuditNumber(row.StorageUsd),
                        FormatAuditNumber(row.NativeTotal),
                        FormatAuditNumber(row.FloatingUsd));
                }
            }

            return builder.ToString();
        }

        public static string BuildDailyReportCsv(Mt5DailyReportSnapshot snapshot, Mt5MonitorSettings settings)
        {
            if (snapshot == null)
                throw new ArgumentNullException("snapshot");

            var builder = new StringBuilder(4096);
            AppendExcelTabSeparatorDefinition(builder);

            AppendTabDelimitedRow(
                builder,
                "Time",
                "Login",
                "Name",
                "Prev Balance",
                "Deposit",
                "Closed P/L",
                "Balance",
                "Credit",
                "Floating P/L",
                "Equity",
                "Margin",
                "Free Margin",
                "Currency");

            if (snapshot.Rows != null)
            {
                for (int i = 0; i < snapshot.Rows.Count; i++)
                {
                    Mt5DailyReportRow row = snapshot.Rows[i];
                    AppendTabDelimitedRow(
                        builder,
                        row.Timestamp.ToString("yyyy.MM.dd HH:mm:ss", CultureInfo.InvariantCulture),
                        row.Login.ToString(CultureInfo.InvariantCulture),
                        SanitizeTabField(row.Name),
                        FormatDailyMoney(row.PrevBalance, row.CurrencyDigits),
                        FormatDailyMoney(row.Deposit, row.CurrencyDigits),
                        FormatDailyMoney(row.ClosedPnl, row.CurrencyDigits),
                        FormatDailyMoney(row.Balance, row.CurrencyDigits),
                        FormatDailyMoney(row.Credit, row.CurrencyDigits),
                        FormatDailyMoney(row.FloatingPnl, row.CurrencyDigits),
                        FormatDailyMoney(row.Equity, row.CurrencyDigits),
                        FormatDailyMoney(row.Margin, row.CurrencyDigits),
                        FormatDailyMoney(row.FreeMargin, row.CurrencyDigits),
                        row.Currency);
                }
            }

            return builder.ToString();
        }

        public static string BuildPositionHistoryCsv(Mt5PositionHistorySnapshot snapshot, Mt5MonitorSettings settings)
        {
            if (snapshot == null)
                throw new ArgumentNullException("snapshot");

            var builder = new StringBuilder(8192);
            AppendExcelTabSeparatorDefinition(builder);

            AppendTabDelimitedRow(
                builder,
                "Login",
                "Name",
                "Time",
                "Ticket",
                "Type",
                "Volume",
                "Symbol",
                "Price",
                "S / L",
                "T / P",
                "Close Time",
                "Close Price",
                "Reason",
                "Commission",
                "Fee",
                "Swap",
                "Profit",
                "Currency",
                "Comment");

            if (snapshot.Rows != null)
            {
                for (int i = 0; i < snapshot.Rows.Count; i++)
                {
                    Mt5PositionHistoryRow row = snapshot.Rows[i];
                    AppendTabDelimitedRow(
                        builder,
                        row.Login.ToString(CultureInfo.InvariantCulture),
                        SanitizeTabField(row.Name),
                        FormatPositionHistoryTime(row.OpenTime),
                        row.Ticket.ToString(CultureInfo.InvariantCulture),
                        row.Type,
                        FormatPositionHistoryVolume(row.Volume),
                        row.Symbol,
                        FormatPositionHistoryPrice(row.OpenPrice, row.Digits),
                        FormatPositionHistoryPrice(row.StopLoss, row.Digits),
                        FormatPositionHistoryPrice(row.TakeProfit, row.Digits),
                        FormatPositionHistoryTime(row.CloseTime),
                        FormatPositionHistoryPrice(row.ClosePrice, row.Digits),
                        row.Reason,
                        FormatPositionHistoryMoney(row.Commission, row.CurrencyDigits),
                        FormatPositionHistoryMoney(row.Fee, row.CurrencyDigits),
                        FormatPositionHistoryMoney(row.Swap, row.CurrencyDigits),
                        FormatPositionHistoryMoney(row.Profit, row.CurrencyDigits),
                        row.Currency,
                        SanitizeTabField(row.Comment));
                }
            }

            if (snapshot.CurrencyTotals != null && snapshot.CurrencyTotals.Count > 0)
            {
                for (int i = 0; i < snapshot.CurrencyTotals.Count; i++)
                {
                    Mt5PositionHistoryCurrencyTotal total = snapshot.CurrencyTotals[i];
                    AppendTabDelimitedRow(
                        builder,
                        i == 0 ? "Total" : string.Empty,
                        string.Empty,
                        string.Empty,
                        string.Empty,
                        string.Empty,
                        string.Empty,
                        string.Empty,
                        string.Empty,
                        string.Empty,
                        string.Empty,
                        string.Empty,
                        string.Empty,
                        string.Empty,
                        FormatPositionHistoryMoney(total.Commission, total.CurrencyDigits),
                        FormatPositionHistoryMoney(total.Fee, total.CurrencyDigits),
                        FormatPositionHistoryMoney(total.Swap, total.CurrencyDigits),
                        FormatPositionHistoryMoney(total.Profit, total.CurrencyDigits),
                        total.Currency,
                        string.Empty);
                }
            }

            return builder.ToString();
        }

        public static string BuildTradingAccountsCsv(Mt5TradingAccountsSnapshot snapshot, Mt5MonitorSettings settings)
        {
            if (snapshot == null)
                throw new ArgumentNullException("snapshot");

            var builder = new StringBuilder(8192);
            builder.AppendLine("sep=,");

            AppendRow(
                builder,
                "Login",
                "Name",
                "Group",
                "Balance",
                "Credit",
                "Profit",
                "Equity",
                "Margin",
                "Free Margin",
                "Margin Level",
                "Leverage",
                "Currency");

            if (snapshot.Rows != null)
            {
                for (int i = 0; i < snapshot.Rows.Count; i++)
                {
                    Mt5TradingAccountRow row = snapshot.Rows[i];
                    AppendRow(
                        builder,
                        row.Login.ToString(CultureInfo.InvariantCulture),
                        row.Name ?? string.Empty,
                        row.Group,
                        FormatDailyMoney(row.Balance, row.CurrencyDigits),
                        FormatDailyMoney(row.Credit, row.CurrencyDigits),
                        FormatDailyMoney(row.Profit, row.CurrencyDigits),
                        FormatDailyMoney(row.Equity, row.CurrencyDigits),
                        FormatDailyMoney(row.Margin, row.CurrencyDigits),
                        FormatDailyMoney(row.FreeMargin, row.CurrencyDigits),
                        FormatAccountRatio(row.MarginLevel),
                        row.MarginLeverage.ToString(CultureInfo.InvariantCulture),
                        row.Currency);
                }
            }

            return builder.ToString();
        }

        public static string BuildDepositWithdrawalCsv(Mt5DepositWithdrawalSnapshot snapshot, Mt5MonitorSettings settings)
        {
            if (snapshot == null)
                throw new ArgumentNullException("snapshot");

            var builder = new StringBuilder(8192);
            AppendExcelTabSeparatorDefinition(builder);

            AppendTabDelimitedRow(
                builder,
                "Deal",
                "Login",
                "Name",
                "Group",
                "Time",
                "Comment",
                "Amount",
                "Currency");

            if (snapshot.Rows != null)
            {
                for (int i = 0; i < snapshot.Rows.Count; i++)
                {
                    Mt5DepositWithdrawalRow row = snapshot.Rows[i];
                    AppendTabDelimitedRow(
                        builder,
                        row.Deal.ToString(CultureInfo.InvariantCulture),
                        row.Login.ToString(CultureInfo.InvariantCulture),
                        SanitizeTabField(row.Name),
                        row.Group,
                        row.Time == default(DateTime)
                            ? string.Empty
                            : row.Time.ToString("yyyy.MM.dd HH:mm:ss", CultureInfo.InvariantCulture),
                        SanitizeTabField(row.Comment),
                        FormatDailyMoney(row.Amount, row.CurrencyDigits),
                        row.Currency);
                }
            }

            if (snapshot.CurrencyTotals != null && snapshot.CurrencyTotals.Count > 0)
            {
                builder.AppendLine();
                AppendTabDelimitedRow(
                    builder,
                    "Currency",
                    "Deal Count",
                    "Deposits",
                    "Withdrawals",
                    "Net Amount");

                for (int i = 0; i < snapshot.CurrencyTotals.Count; i++)
                {
                    Mt5DepositWithdrawalCurrencyTotal total = snapshot.CurrencyTotals[i];
                    AppendTabDelimitedRow(
                        builder,
                        total.Currency,
                        total.DealCount.ToString(CultureInfo.InvariantCulture),
                        FormatDailyMoney(total.Deposits, total.CurrencyDigits),
                        FormatDailyMoney(total.Withdrawals, total.CurrencyDigits),
                        FormatDailyMoney(total.NetAmount, total.CurrencyDigits));
                }
            }

            return builder.ToString();
        }

        private static void AppendMetadata(StringBuilder builder, string key, string value)
        {
            AppendRow(builder, key, value);
        }

        private static void AppendRow(StringBuilder builder, params string[] values)
        {
            for (int i = 0; i < values.Length; i++)
            {
                if (i > 0)
                    builder.Append(',');
                builder.Append(Escape(values[i] ?? string.Empty));
            }

            builder.AppendLine();
        }

        private static void AppendTabDelimitedRow(StringBuilder builder, params string[] values)
        {
            for (int i = 0; i < values.Length; i++)
            {
                if (i > 0)
                    builder.Append('\t');
                builder.Append(SanitizeTabField(values[i] ?? string.Empty));
            }

            builder.AppendLine();
        }

        private static void AppendExcelTabSeparatorDefinition(StringBuilder builder)
        {
            builder.Append("sep=");
            builder.Append('\t');
            builder.AppendLine();
        }

        private static string Escape(string value)
        {
            if (value.IndexOfAny(new[] { '"', ',', '\r', '\n' }) >= 0)
                return "\"" + value.Replace("\"", "\"\"") + "\"";
            return value;
        }

        private static string SanitizeTabField(string value)
        {
            if (string.IsNullOrEmpty(value))
                return string.Empty;

            return value.Replace('\t', ' ').Replace('\r', ' ').Replace('\n', ' ');
        }

        private static string FormatVolume(double value)
        {
            return value.ToString("0.##", CultureInfo.InvariantCulture);
        }

        private static string FormatPrice(double value, int digits)
        {
            int safeDigits = digits > 0 ? digits : 2;
            if (safeDigits > 5)
                safeDigits = 5;

            return value.ToString("F" + safeDigits.ToString(CultureInfo.InvariantCulture), CultureInfo.InvariantCulture);
        }

        private static string FormatNetVolume(double value)
        {
            return value.ToString("0.##", CultureInfo.InvariantCulture);
        }

        private static string FormatMoney(double value)
        {
            return value.ToString("0.00", CultureInfo.InvariantCulture);
        }

        private static string FormatNullableMoney(double? value)
        {
            return value.HasValue ? value.Value.ToString("0.00", CultureInfo.InvariantCulture) : string.Empty;
        }

        private static string FormatAuditNumber(double value)
        {
            return value.ToString("0.###############", CultureInfo.InvariantCulture);
        }

        private static string FormatAuditPrice(double value, int digits)
        {
            int safeDigits = digits > 0 ? digits : 5;
            if (safeDigits > 8)
                safeDigits = 8;

            return value.ToString("F" + safeDigits.ToString(CultureInfo.InvariantCulture), CultureInfo.InvariantCulture);
        }

        private static string FormatBoolean(bool value)
        {
            return value ? "TRUE" : "FALSE";
        }

        private static string FormatDailyMoney(double value, int digits)
        {
            int safeDigits = digits < 0 ? 0 : digits;
            if (safeDigits > 8)
                safeDigits = 8;

            return value.ToString("F" + safeDigits.ToString(CultureInfo.InvariantCulture), CultureInfo.InvariantCulture);
        }

        private static string FormatPositionHistoryTime(DateTime value)
        {
            if (value == default(DateTime))
                return string.Empty;

            return value.ToString("yyyy.MM.dd HH:mm:ss.fff", CultureInfo.InvariantCulture);
        }

        private static string FormatPositionHistoryVolume(double value)
        {
            return value.ToString("0.##", CultureInfo.InvariantCulture);
        }

        private static string FormatPositionHistoryPrice(double value, int digits)
        {
            int safeDigits = digits > 0 ? digits : 5;
            if (safeDigits > 8)
                safeDigits = 8;

            return value.ToString("F" + safeDigits.ToString(CultureInfo.InvariantCulture), CultureInfo.InvariantCulture);
        }

        private static string FormatPositionHistoryMoney(double value, int digits)
        {
            int safeDigits = digits < 0 ? 0 : digits;
            if (safeDigits > 8)
                safeDigits = 8;

            return value.ToString("F" + safeDigits.ToString(CultureInfo.InvariantCulture), CultureInfo.InvariantCulture);
        }

        private static string FormatAccountRatio(double value)
        {
            return value.ToString("0.##", CultureInfo.InvariantCulture);
        }
    }

    public static class Mt5MonitorJsonExporter
    {
        public static void ExportDailyReportSnapshotJson(string path, Mt5DailyReportSnapshot snapshot, Mt5MonitorSettings settings, bool indented)
        {
            if (string.IsNullOrWhiteSpace(path))
                throw new ArgumentException("Export path is required.", "path");
            if (snapshot == null)
                throw new ArgumentNullException("snapshot");

            string directory = Path.GetDirectoryName(Path.GetFullPath(path));
            if (!string.IsNullOrWhiteSpace(directory))
                Directory.CreateDirectory(directory);

            File.WriteAllText(path, BuildDailyReportJson(snapshot, settings, indented), new UTF8Encoding(true));
        }

        public static void ExportDailyReportSnapshotJson(string path, Mt5DailyReportSnapshot snapshot, Mt5MonitorSettings settings)
        {
            ExportDailyReportSnapshotJson(path, snapshot, settings, true);
        }

        public static void ExportPositionHistorySnapshotJson(string path, Mt5PositionHistorySnapshot snapshot, Mt5MonitorSettings settings, bool indented)
        {
            if (string.IsNullOrWhiteSpace(path))
                throw new ArgumentException("Export path is required.", "path");
            if (snapshot == null)
                throw new ArgumentNullException("snapshot");

            string directory = Path.GetDirectoryName(Path.GetFullPath(path));
            if (!string.IsNullOrWhiteSpace(directory))
                Directory.CreateDirectory(directory);

            File.WriteAllText(path, BuildPositionHistoryJson(snapshot, settings, indented), new UTF8Encoding(true));
        }

        public static void ExportPositionHistorySnapshotJson(string path, Mt5PositionHistorySnapshot snapshot, Mt5MonitorSettings settings)
        {
            ExportPositionHistorySnapshotJson(path, snapshot, settings, true);
        }

        public static void ExportTradingAccountsSnapshotJson(string path, Mt5TradingAccountsSnapshot snapshot, Mt5MonitorSettings settings, bool indented)
        {
            if (string.IsNullOrWhiteSpace(path))
                throw new ArgumentException("Export path is required.", "path");
            if (snapshot == null)
                throw new ArgumentNullException("snapshot");

            string directory = Path.GetDirectoryName(Path.GetFullPath(path));
            if (!string.IsNullOrWhiteSpace(directory))
                Directory.CreateDirectory(directory);

            File.WriteAllText(path, BuildTradingAccountsJson(snapshot, settings, indented), new UTF8Encoding(true));
        }

        public static void ExportTradingAccountsSnapshotJson(string path, Mt5TradingAccountsSnapshot snapshot, Mt5MonitorSettings settings)
        {
            ExportTradingAccountsSnapshotJson(path, snapshot, settings, true);
        }

        public static void ExportDepositWithdrawalSnapshotJson(string path, Mt5DepositWithdrawalSnapshot snapshot, Mt5MonitorSettings settings, bool indented)
        {
            if (string.IsNullOrWhiteSpace(path))
                throw new ArgumentException("Export path is required.", "path");
            if (snapshot == null)
                throw new ArgumentNullException("snapshot");

            string directory = Path.GetDirectoryName(Path.GetFullPath(path));
            if (!string.IsNullOrWhiteSpace(directory))
                Directory.CreateDirectory(directory);

            File.WriteAllText(path, BuildDepositWithdrawalJson(snapshot, settings, indented), new UTF8Encoding(true));
        }

        public static void ExportDepositWithdrawalSnapshotJson(string path, Mt5DepositWithdrawalSnapshot snapshot, Mt5MonitorSettings settings)
        {
            ExportDepositWithdrawalSnapshotJson(path, snapshot, settings, true);
        }

        public static Mt5DailyReportJsonDocument CreateDailyReportDocument(Mt5DailyReportSnapshot snapshot, Mt5MonitorSettings settings)
        {
            if (snapshot == null)
                throw new ArgumentNullException("snapshot");

            IList<Mt5DailyReportJsonRow> rows = snapshot.Rows != null
                ? snapshot.Rows.Select(
                    row => new Mt5DailyReportJsonRow
                    {
                        Timestamp = FormatJsonDateTime(row.Timestamp),
                        Login = row.Login,
                        Name = row.Name,
                        Group = row.Group,
                        Currency = row.Currency,
                        CurrencyDigits = row.CurrencyDigits,
                        PrevBalance = row.PrevBalance,
                        Deposit = row.Deposit,
                        ClosedPnl = row.ClosedPnl,
                        EquityPrevDay = row.EquityPrevDay,
                        Balance = row.Balance,
                        Credit = row.Credit,
                        DailyCredit = row.DailyCredit,
                        DailyBonus = row.DailyBonus,
                        FloatingPnl = row.FloatingPnl,
                        Equity = row.Equity,
                        Margin = row.Margin,
                        FreeMargin = row.FreeMargin
                    }).ToList()
                : new List<Mt5DailyReportJsonRow>();

            return new Mt5DailyReportJsonDocument
            {
                ReportType = "DailyReport",
                Server = settings != null ? settings.Server : string.Empty,
                Login = settings != null ? settings.Login : 0,
                GroupMask = settings != null ? settings.GroupMask : string.Empty,
                GeneratedAt = FormatJsonDateTime(snapshot.GeneratedAt),
                RangeFrom = FormatJsonDateTime(snapshot.RangeFrom),
                RangeTo = FormatJsonDateTime(snapshot.RangeTo),
                RowCount = rows.Count,
                Rows = rows
            };
        }

        public static Mt5PositionHistoryJsonDocument CreatePositionHistoryDocument(Mt5PositionHistorySnapshot snapshot, Mt5MonitorSettings settings)
        {
            if (snapshot == null)
                throw new ArgumentNullException("snapshot");

            IList<Mt5PositionHistoryJsonRow> rows = snapshot.Rows != null
                ? snapshot.Rows.Select(
                    row => new Mt5PositionHistoryJsonRow
                    {
                        Login = row.Login,
                        Name = row.Name,
                        OpenTime = FormatJsonDateTime(row.OpenTime),
                        Ticket = row.Ticket,
                        Type = row.Type,
                        Volume = row.Volume,
                        Symbol = row.Symbol,
                        OpenPrice = row.OpenPrice,
                        StopLoss = row.StopLoss,
                        TakeProfit = row.TakeProfit,
                        CloseTime = FormatJsonDateTime(row.CloseTime),
                        ClosePrice = row.ClosePrice,
                        Reason = row.Reason,
                        Commission = row.Commission,
                        Fee = row.Fee,
                        Swap = row.Swap,
                        Profit = row.Profit,
                        Currency = row.Currency,
                        Comment = row.Comment,
                        Digits = row.Digits,
                        CurrencyDigits = row.CurrencyDigits
                    }).ToList()
                : new List<Mt5PositionHistoryJsonRow>();

            IList<Mt5PositionHistoryJsonCurrencyTotal> totals = snapshot.CurrencyTotals != null
                ? snapshot.CurrencyTotals.Select(
                    total => new Mt5PositionHistoryJsonCurrencyTotal
                    {
                        Currency = total.Currency,
                        CurrencyDigits = total.CurrencyDigits,
                        Commission = total.Commission,
                        Fee = total.Fee,
                        Swap = total.Swap,
                        Profit = total.Profit
                    }).ToList()
                : new List<Mt5PositionHistoryJsonCurrencyTotal>();

            return new Mt5PositionHistoryJsonDocument
            {
                ReportType = "PositionHistory",
                Server = settings != null ? settings.Server : string.Empty,
                Login = settings != null ? settings.Login : 0,
                GroupMask = settings != null ? settings.GroupMask : string.Empty,
                GeneratedAt = FormatJsonDateTime(snapshot.GeneratedAt),
                RangeFrom = FormatJsonDateTime(snapshot.RangeFrom),
                RangeTo = FormatJsonDateTime(snapshot.RangeTo),
                RowCount = rows.Count,
                CurrencyTotalCount = totals.Count,
                Rows = rows,
                CurrencyTotals = totals
            };
        }

        public static Mt5TradingAccountsJsonDocument CreateTradingAccountsDocument(Mt5TradingAccountsSnapshot snapshot, Mt5MonitorSettings settings)
        {
            if (snapshot == null)
                throw new ArgumentNullException("snapshot");

            IList<Mt5TradingAccountJsonRow> rows = snapshot.Rows != null
                ? snapshot.Rows.Select(
                    row => new Mt5TradingAccountJsonRow
                    {
                        Login = row.Login,
                        Name = row.Name,
                        Group = row.Group,
                        Currency = row.Currency,
                        CurrencyDigits = row.CurrencyDigits,
                        Balance = row.Balance,
                        Credit = row.Credit,
                        Profit = row.Profit,
                        Equity = row.Equity,
                        Margin = row.Margin,
                        FreeMargin = row.FreeMargin,
                        MarginLevel = row.MarginLevel,
                        MarginLeverage = row.MarginLeverage
                    }).ToList()
                : new List<Mt5TradingAccountJsonRow>();

            return new Mt5TradingAccountsJsonDocument
            {
                ReportType = "TradingAccounts",
                Server = settings != null ? settings.Server : string.Empty,
                Login = settings != null ? settings.Login : 0,
                GroupMask = settings != null ? settings.GroupMask : string.Empty,
                GeneratedAt = FormatJsonDateTime(snapshot.GeneratedAt),
                RowCount = rows.Count,
                Rows = rows
            };
        }

        public static Mt5DepositWithdrawalJsonDocument CreateDepositWithdrawalDocument(Mt5DepositWithdrawalSnapshot snapshot, Mt5MonitorSettings settings)
        {
            if (snapshot == null)
                throw new ArgumentNullException("snapshot");

            IList<Mt5DepositWithdrawalJsonRow> rows = snapshot.Rows != null
                ? snapshot.Rows.Select(
                    row => new Mt5DepositWithdrawalJsonRow
                    {
                        Deal = row.Deal,
                        Login = row.Login,
                        Name = row.Name,
                        Group = row.Group,
                        Time = FormatJsonDateTime(row.Time),
                        Comment = row.Comment,
                        Amount = row.Amount,
                        Currency = row.Currency,
                        CurrencyDigits = row.CurrencyDigits
                    }).ToList()
                : new List<Mt5DepositWithdrawalJsonRow>();

            IList<Mt5DepositWithdrawalJsonCurrencyTotal> totals = snapshot.CurrencyTotals != null
                ? snapshot.CurrencyTotals.Select(
                    total => new Mt5DepositWithdrawalJsonCurrencyTotal
                    {
                        Currency = total.Currency,
                        CurrencyDigits = total.CurrencyDigits,
                        DealCount = total.DealCount,
                        Deposits = total.Deposits,
                        Withdrawals = total.Withdrawals,
                        NetAmount = total.NetAmount
                    }).ToList()
                : new List<Mt5DepositWithdrawalJsonCurrencyTotal>();

            return new Mt5DepositWithdrawalJsonDocument
            {
                ReportType = "DepositWithdrawal",
                Server = settings != null ? settings.Server : string.Empty,
                Login = settings != null ? settings.Login : 0,
                GroupMask = settings != null ? settings.GroupMask : string.Empty,
                GeneratedAt = FormatJsonDateTime(snapshot.GeneratedAt),
                RangeFrom = FormatJsonDateTime(snapshot.RangeFrom),
                RangeTo = FormatJsonDateTime(snapshot.RangeTo),
                RowCount = rows.Count,
                CurrencyTotalCount = totals.Count,
                Rows = rows,
                CurrencyTotals = totals
            };
        }

        public static string BuildDailyReportJson(Mt5DailyReportSnapshot snapshot, Mt5MonitorSettings settings, bool indented)
        {
            return SerializeJson(CreateDailyReportDocument(snapshot, settings), indented);
        }

        public static string BuildDailyReportJson(Mt5DailyReportSnapshot snapshot, Mt5MonitorSettings settings)
        {
            return BuildDailyReportJson(snapshot, settings, true);
        }

        public static string BuildPositionHistoryJson(Mt5PositionHistorySnapshot snapshot, Mt5MonitorSettings settings, bool indented)
        {
            return SerializeJson(CreatePositionHistoryDocument(snapshot, settings), indented);
        }

        public static string BuildPositionHistoryJson(Mt5PositionHistorySnapshot snapshot, Mt5MonitorSettings settings)
        {
            return BuildPositionHistoryJson(snapshot, settings, true);
        }

        public static string BuildTradingAccountsJson(Mt5TradingAccountsSnapshot snapshot, Mt5MonitorSettings settings, bool indented)
        {
            return SerializeJson(CreateTradingAccountsDocument(snapshot, settings), indented);
        }

        public static string BuildTradingAccountsJson(Mt5TradingAccountsSnapshot snapshot, Mt5MonitorSettings settings)
        {
            return BuildTradingAccountsJson(snapshot, settings, true);
        }

        public static string BuildDepositWithdrawalJson(Mt5DepositWithdrawalSnapshot snapshot, Mt5MonitorSettings settings, bool indented)
        {
            return SerializeJson(CreateDepositWithdrawalDocument(snapshot, settings), indented);
        }

        public static string BuildDepositWithdrawalJson(Mt5DepositWithdrawalSnapshot snapshot, Mt5MonitorSettings settings)
        {
            return BuildDepositWithdrawalJson(snapshot, settings, true);
        }

        private static string SerializeJson<T>(T value, bool indented)
        {
            var serializer = new DataContractJsonSerializer(typeof(T));

            using (var stream = new MemoryStream())
            {
                serializer.WriteObject(stream, value);
                string json = Encoding.UTF8.GetString(stream.ToArray());
                return indented ? PrettyPrintJson(json) : json;
            }
        }

        private static string FormatJsonDateTime(DateTime value)
        {
            return value == DateTime.MinValue
                ? null
                : value.ToString("O", CultureInfo.InvariantCulture);
        }

        private static string PrettyPrintJson(string json)
        {
            if (string.IsNullOrWhiteSpace(json))
                return json;

            var builder = new StringBuilder(json.Length + 256);
            bool inString = false;
            bool escaping = false;
            int depth = 0;

            for (int i = 0; i < json.Length; i++)
            {
                char current = json[i];

                if (escaping)
                {
                    builder.Append(current);
                    escaping = false;
                    continue;
                }

                if (current == '\\')
                {
                    builder.Append(current);
                    if (inString)
                        escaping = true;
                    continue;
                }

                if (current == '"')
                {
                    builder.Append(current);
                    inString = !inString;
                    continue;
                }

                if (inString)
                {
                    builder.Append(current);
                    continue;
                }

                switch (current)
                {
                    case '{':
                    case '[':
                        builder.Append(current);
                        builder.AppendLine();
                        depth++;
                        AppendJsonIndent(builder, depth);
                        break;

                    case '}':
                    case ']':
                        builder.AppendLine();
                        depth = Math.Max(0, depth - 1);
                        AppendJsonIndent(builder, depth);
                        builder.Append(current);
                        break;

                    case ',':
                        builder.Append(current);
                        builder.AppendLine();
                        AppendJsonIndent(builder, depth);
                        break;

                    case ':':
                        builder.Append(": ");
                        break;

                    default:
                        if (!char.IsWhiteSpace(current))
                            builder.Append(current);
                        break;
                }
            }

            return builder.ToString();
        }

        private static void AppendJsonIndent(StringBuilder builder, int depth)
        {
            for (int i = 0; i < depth; i++)
                builder.Append("  ");
        }
    }

    public static class Mt5UsdRateLoader
    {
        public static IDictionary<string, Mt5UsdConversionRate> LoadLiveRates(Mt5MonitorSettings settings)
        {
            return LoadLiveRates(settings, null);
        }

        public static IDictionary<string, Mt5UsdConversionRate> LoadLiveRates(Mt5MonitorSettings settings, Action<string> statusWriter)
        {
            if (settings == null)
                throw new ArgumentNullException("settings");

            Mt5MonitorSettings effective = settings.Clone();
            effective.Validate();

            Action<string> writer = statusWriter ?? (_ => { });
            CIMTManagerAPI manager = null;
            MTRetCode initializeResult = SMTManagerAPIFactory.Initialize(effective.SdkLibsPath);
            if (initializeResult != MTRetCode.MT_RET_OK)
                throw new InvalidOperationException("Initialize failed: " + initializeResult + " (" + (uint)initializeResult + ")");

            try
            {
                manager = Mt5MonitorCollector.Connect(effective.Server, effective.Login, effective.Password, writer);
                if (manager == null)
                    throw new InvalidOperationException("Unable to connect to MT5 to load live USD rates.");

                return LoadLiveRates(manager);
            }
            finally
            {
                Mt5MonitorCollector.Disconnect(manager);
                SMTManagerAPIFactory.Shutdown();
            }
        }

        public static IDictionary<string, Mt5UsdConversionRate> LoadLiveRates(CIMTManagerAPI manager)
        {
            if (manager == null)
                throw new ArgumentNullException("manager");

            var rawRates = Mt5MonitorCollector.BuildCurrencyRates(manager);
            var publicRates = new Dictionary<string, Mt5UsdConversionRate>(StringComparer.OrdinalIgnoreCase);

            foreach (KeyValuePair<string, Mt5MonitorCollector.CurrencyRate> pair in rawRates)
            {
                publicRates[pair.Key] = new Mt5UsdConversionRate
                {
                    Currency = pair.Key,
                    FxSymbol = pair.Value.Symbol,
                    Bid = pair.Value.Bid,
                    Ask = pair.Value.Ask,
                    UsdBase = pair.Value.UsdBase,
                    PositiveToUsd = pair.Value.PositiveToUsd,
                    NegativeToUsd = pair.Value.NegativeToUsd
                };
            }

            EnsureUsdIdentityRate(publicRates);
            return publicRates;
        }

        private static void EnsureUsdIdentityRate(IDictionary<string, Mt5UsdConversionRate> rates)
        {
            if (rates == null)
                throw new ArgumentNullException("rates");

            if (!rates.ContainsKey("USD"))
            {
                rates["USD"] = new Mt5UsdConversionRate
                {
                    Currency = "USD",
                    FxSymbol = "USD",
                    Bid = 1.0,
                    Ask = 1.0,
                    UsdBase = false,
                    PositiveToUsd = 1.0,
                    NegativeToUsd = 1.0
                };
            }
        }
    }

    public sealed class Mt5DailyClosedPnlCalculator : IMt5DailyClosedPnlCalculator
    {
        public Mt5DailyClosedPnlResult Calculate(
            Mt5PositionHistorySnapshot snapshot,
            IDictionary<string, Mt5UsdConversionRate> usdRates)
        {
            if (snapshot == null)
                throw new ArgumentNullException("snapshot");

            Mt5DailyClosedPnlResult result = Calculate(snapshot.CurrencyTotals ?? new List<Mt5PositionHistoryCurrencyTotal>(), usdRates);
            result.GeneratedAt = snapshot.GeneratedAt == default(DateTime) ? DateTime.Now : snapshot.GeneratedAt;
            result.RangeFrom = snapshot.RangeFrom == default(DateTime) ? (DateTime?)null : snapshot.RangeFrom;
            result.RangeTo = snapshot.RangeTo == default(DateTime) ? (DateTime?)null : snapshot.RangeTo;
            result.SourceRowCount = snapshot.Rows != null ? snapshot.Rows.Count : 0;
            return result;
        }

        public Mt5DailyClosedPnlResult Calculate(
            IEnumerable<Mt5PositionHistoryRow> rows,
            IDictionary<string, Mt5UsdConversionRate> usdRates)
        {
            if (rows == null)
                throw new ArgumentNullException("rows");

            List<Mt5PositionHistoryRow> rowList = rows.Where(row => row != null).ToList();
            Mt5DailyClosedPnlResult result = Calculate(BuildCurrencyTotals(rowList), usdRates);
            result.SourceRowCount = rowList.Count;
            return result;
        }

        public Mt5DailyClosedPnlResult Calculate(
            IEnumerable<Mt5PositionHistoryCurrencyTotal> currencyTotals,
            IDictionary<string, Mt5UsdConversionRate> usdRates)
        {
            if (currencyTotals == null)
                throw new ArgumentNullException("currencyTotals");

            List<Mt5PositionHistoryCurrencyTotal> totals = currencyTotals
                .Where(total => total != null)
                .OrderBy(total => total.Currency ?? string.Empty, StringComparer.OrdinalIgnoreCase)
                .ToList();

            Dictionary<string, Mt5UsdConversionRate> normalizedRates = NormalizeRates(usdRates);
            var missingCurrencies = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var breakdowns = new List<Mt5DailyClosedPnlCurrencyBreakdown>(totals.Count);
            double totalClosedPnlUsd = 0.0;

            for (int i = 0; i < totals.Count; i++)
            {
                Mt5PositionHistoryCurrencyTotal total = totals[i];
                string currency = string.IsNullOrWhiteSpace(total.Currency) ? "USD" : total.Currency;
                Mt5UsdConversionRate rate;
                bool hasRate = normalizedRates.TryGetValue(currency, out rate);

                double commissionUsd = ConvertNativeToUsd(total.Commission, currency, normalizedRates, missingCurrencies);
                double feeUsd = ConvertNativeToUsd(total.Fee, currency, normalizedRates, missingCurrencies);
                double swapUsd = ConvertNativeToUsd(total.Swap, currency, normalizedRates, missingCurrencies);
                double profitUsd = ConvertNativeToUsd(total.Profit, currency, normalizedRates, missingCurrencies);

                double nativeClosedPnl = MoneyAdd(total.Commission, total.Fee, total.CurrencyDigits);
                nativeClosedPnl = MoneyAdd(nativeClosedPnl, total.Swap, total.CurrencyDigits);
                nativeClosedPnl = MoneyAdd(nativeClosedPnl, total.Profit, total.CurrencyDigits);

                double closedPnlUsd = commissionUsd + feeUsd + swapUsd + profitUsd;
                totalClosedPnlUsd += closedPnlUsd;

                breakdowns.Add(new Mt5DailyClosedPnlCurrencyBreakdown
                {
                    Currency = currency,
                    CurrencyDigits = total.CurrencyDigits,
                    FxSymbol = hasRate && rate != null ? rate.FxSymbol : string.Empty,
                    FxBid = hasRate && rate != null ? rate.Bid : 0.0,
                    FxAsk = hasRate && rate != null ? rate.Ask : 0.0,
                    FxUsdBase = hasRate && rate != null && rate.UsdBase,
                    PositiveToUsdRate = hasRate && rate != null ? rate.PositiveToUsd : 0.0,
                    NegativeToUsdRate = hasRate && rate != null ? rate.NegativeToUsd : 0.0,
                    MissingRate = !hasRate,
                    CommissionNative = total.Commission,
                    FeeNative = total.Fee,
                    SwapNative = total.Swap,
                    ProfitNative = total.Profit,
                    NativeClosedPnl = nativeClosedPnl,
                    CommissionUsd = commissionUsd,
                    FeeUsd = feeUsd,
                    SwapUsd = swapUsd,
                    ProfitUsd = profitUsd,
                    ClosedPnlUsd = closedPnlUsd
                });
            }

            return new Mt5DailyClosedPnlResult
            {
                GeneratedAt = DateTime.Now,
                SourceCurrencyCount = totals.Count,
                TotalClosedPnlUsd = totalClosedPnlUsd,
                ConversionSummary = BuildClosedPnlConversionSummary(missingCurrencies),
                MissingCurrencyRates = missingCurrencies.OrderBy(code => code, StringComparer.OrdinalIgnoreCase).ToList(),
                CurrencyBreakdowns = breakdowns
            };
        }

        public Mt5DailyClosedPnlResult CalculateUsingLiveRates(
            Mt5PositionHistorySnapshot snapshot,
            Mt5MonitorSettings settings,
            Action<string> statusWriter)
        {
            return Calculate(snapshot, Mt5UsdRateLoader.LoadLiveRates(settings, statusWriter));
        }

        public Mt5DailyClosedPnlResult CalculateUsingLiveRates(
            IEnumerable<Mt5PositionHistoryRow> rows,
            Mt5MonitorSettings settings,
            Action<string> statusWriter)
        {
            return Calculate(rows, Mt5UsdRateLoader.LoadLiveRates(settings, statusWriter));
        }

        public Mt5DailyClosedPnlResult CalculateUsingLiveRates(
            IEnumerable<Mt5PositionHistoryCurrencyTotal> currencyTotals,
            Mt5MonitorSettings settings,
            Action<string> statusWriter)
        {
            return Calculate(currencyTotals, Mt5UsdRateLoader.LoadLiveRates(settings, statusWriter));
        }

        private static Dictionary<string, Mt5UsdConversionRate> NormalizeRates(IDictionary<string, Mt5UsdConversionRate> usdRates)
        {
            var normalized = new Dictionary<string, Mt5UsdConversionRate>(StringComparer.OrdinalIgnoreCase);
            if (usdRates != null)
            {
                foreach (KeyValuePair<string, Mt5UsdConversionRate> pair in usdRates)
                {
                    if (string.IsNullOrWhiteSpace(pair.Key))
                        continue;

                    Mt5UsdConversionRate rate = pair.Value ?? new Mt5UsdConversionRate();
                    normalized[pair.Key] = new Mt5UsdConversionRate
                    {
                        Currency = string.IsNullOrWhiteSpace(rate.Currency) ? pair.Key : rate.Currency,
                        FxSymbol = rate.FxSymbol,
                        Bid = rate.Bid,
                        Ask = rate.Ask,
                        UsdBase = rate.UsdBase,
                        PositiveToUsd = rate.PositiveToUsd,
                        NegativeToUsd = rate.NegativeToUsd
                    };
                }
            }

            if (!normalized.ContainsKey("USD"))
            {
                normalized["USD"] = new Mt5UsdConversionRate
                {
                    Currency = "USD",
                    FxSymbol = "USD",
                    Bid = 1.0,
                    Ask = 1.0,
                    UsdBase = false,
                    PositiveToUsd = 1.0,
                    NegativeToUsd = 1.0
                };
            }

            return normalized;
        }

        private static double ConvertNativeToUsd(
            double nativeAmount,
            string currency,
            IDictionary<string, Mt5UsdConversionRate> usdRates,
            ISet<string> missingCurrencies)
        {
            if (nativeAmount == 0.0)
                return 0.0;

            string effectiveCurrency = string.IsNullOrWhiteSpace(currency) ? "USD" : currency;
            Mt5UsdConversionRate rate;
            if (usdRates != null && usdRates.TryGetValue(effectiveCurrency, out rate) && rate != null)
            {
                double usdRate = nativeAmount >= 0.0 ? rate.PositiveToUsd : rate.NegativeToUsd;
                if (usdRate > 0.0)
                    return nativeAmount * usdRate;
            }

            if (missingCurrencies != null)
                missingCurrencies.Add(effectiveCurrency);

            return 0.0;
        }

        private static List<Mt5PositionHistoryCurrencyTotal> BuildCurrencyTotals(IEnumerable<Mt5PositionHistoryRow> rows)
        {
            var totals = new Dictionary<string, Mt5PositionHistoryCurrencyTotal>(StringComparer.OrdinalIgnoreCase);

            foreach (Mt5PositionHistoryRow row in rows)
            {
                if (row == null)
                    continue;

                string currency = string.IsNullOrWhiteSpace(row.Currency) ? "USD" : row.Currency;
                Mt5PositionHistoryCurrencyTotal total;
                if (!totals.TryGetValue(currency, out total))
                {
                    total = new Mt5PositionHistoryCurrencyTotal
                    {
                        Currency = currency,
                        CurrencyDigits = row.CurrencyDigits
                    };
                    totals[currency] = total;
                }

                total.CurrencyDigits = Math.Max(total.CurrencyDigits, NormalizeCurrencyDigits(row.CurrencyDigits));
                total.Commission = MoneyAdd(total.Commission, row.Commission, total.CurrencyDigits);
                total.Fee = MoneyAdd(total.Fee, row.Fee, total.CurrencyDigits);
                total.Swap = MoneyAdd(total.Swap, row.Swap, total.CurrencyDigits);
                total.Profit = MoneyAdd(total.Profit, row.Profit, total.CurrencyDigits);
            }

            return totals.Values
                .OrderBy(item => item.Currency ?? string.Empty, StringComparer.OrdinalIgnoreCase)
                .ToList();
        }

        private static string BuildClosedPnlConversionSummary(ICollection<string> missingCurrencies)
        {
            if (missingCurrencies == null || missingCurrencies.Count == 0)
                return "Daily closed PnL: converted per currency using supplied USD rate table.";

            return string.Format(
                CultureInfo.InvariantCulture,
                "Daily closed PnL: converted per currency with {0} missing USD rates: {1}.",
                missingCurrencies.Count,
                string.Join(", ", missingCurrencies.OrderBy(code => code, StringComparer.OrdinalIgnoreCase)));
        }

        private static int NormalizeCurrencyDigits(int digits)
        {
            if (digits < 0)
                return 0;
            if (digits > 8)
                return 8;
            return digits;
        }

        private static double MoneyAdd(double left, double right, int digits)
        {
            int safeDigits = NormalizeCurrencyDigits(digits);
            return Math.Round(left + right, safeDigits, MidpointRounding.AwayFromZero);
        }
    }

    public sealed class Mt5WdEquityZCalculator : IMt5WdEquityZCalculator
    {
        public Mt5WdEquityZReport Calculate(Mt5WdEquityZInputs inputs)
        {
            if (inputs == null)
                throw new ArgumentNullException("inputs");

            double endWdEquityUsd = Math.Max(
                0.0,
                inputs.EndEquityUsd - inputs.EndCreditsUsd - inputs.EndProtectedBonusesUsd);

            double startWdEquityUsd = Math.Max(
                0.0,
                inputs.StartEquityUsd - inputs.StartCreditsUsd - inputs.StartProtectedBonusesUsd);

            return new Mt5WdEquityZReport
            {
                GeneratedAt = DateTime.Now,
                ReportDate = inputs.ReportDate == default(DateTime) ? DateTime.Today : inputs.ReportDate.Date,
                ComputationMode = inputs.ComputationMode,
                EndEquityUsd = inputs.EndEquityUsd,
                EndCreditsUsd = inputs.EndCreditsUsd,
                EndProtectedBonusesUsd = inputs.EndProtectedBonusesUsd,
                EndWdEquityUsd = endWdEquityUsd,
                StartEquityUsd = inputs.StartEquityUsd,
                StartCreditsUsd = inputs.StartCreditsUsd,
                StartProtectedBonusesUsd = inputs.StartProtectedBonusesUsd,
                StartWdEquityUsd = startWdEquityUsd,
                WdEquityZUsd = ComputeFinalValue(endWdEquityUsd, startWdEquityUsd, inputs.ComputationMode),
                CalculationSummary = BuildCalculationSummary(inputs.ComputationMode),
                MissingCurrencyRates = NormalizeStrings(inputs.MissingCurrencyRates),
                Assumptions = NormalizeStrings(inputs.Assumptions)
            };
        }

        public Mt5WdEquityZReport Calculate(
            DateTime reportDate,
            IEnumerable<Mt5DailyReportRow> dailyRows,
            IDictionary<string, Mt5UsdConversionRate> usdRates,
            double startProtectedBonusesUsd,
            double endProtectedBonusesUsd,
            Mt5WdEquityZComputationMode computationMode)
        {
            if (dailyRows == null)
                throw new ArgumentNullException("dailyRows");

            List<Mt5DailyReportRow> rows = dailyRows.Where(row => row != null).ToList();
            Dictionary<string, Mt5UsdConversionRate> normalizedRates = NormalizeRates(usdRates);
            var missingCurrencies = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            double endEquityUsd = 0.0;
            double endCreditsUsd = 0.0;
            double startEquityUsd = 0.0;
            double startCreditsUsd = 0.0;

            for (int i = 0; i < rows.Count; i++)
            {
                Mt5DailyReportRow row = rows[i];
                string currency = string.IsNullOrWhiteSpace(row.Currency) ? "USD" : row.Currency;

                endEquityUsd += ConvertNativeToUsd(row.Equity, currency, normalizedRates, missingCurrencies);
                endCreditsUsd += ConvertNativeToUsd(row.Credit, currency, normalizedRates, missingCurrencies);
                startEquityUsd += ConvertNativeToUsd(row.EquityPrevDay, currency, normalizedRates, missingCurrencies);
                startCreditsUsd += ConvertNativeToUsd(row.Credit - row.DailyCredit, currency, normalizedRates, missingCurrencies);
            }

            Mt5WdEquityZReport report = Calculate(
                new Mt5WdEquityZInputs
                {
                    ReportDate = reportDate.Date,
                    ComputationMode = computationMode,
                    EndEquityUsd = endEquityUsd,
                    EndCreditsUsd = endCreditsUsd,
                    EndProtectedBonusesUsd = endProtectedBonusesUsd,
                    StartEquityUsd = startEquityUsd,
                    StartCreditsUsd = startCreditsUsd,
                    StartProtectedBonusesUsd = startProtectedBonusesUsd,
                    MissingCurrencyRates = missingCurrencies.OrderBy(code => code, StringComparer.OrdinalIgnoreCase).ToList(),
                    Assumptions = new List<string>
                    {
                        "Start equity uses EquityPrevDay from MT5 daily rows.",
                        "Start credits are derived as Credit minus DailyCredit because MT5 daily rows do not expose CreditPrevDay.",
                        "Protected bonus totals must be supplied separately from filtered balance deals.",
                        "USD conversion uses the supplied MT5 USD rate table."
                    }
                });

            report.DailyRowCount = rows.Count;
            return report;
        }

        private static double ComputeFinalValue(
            double endWdEquityUsd,
            double startWdEquityUsd,
            Mt5WdEquityZComputationMode computationMode)
        {
            if (computationMode == Mt5WdEquityZComputationMode.DeltaFromStartWhenBothPositive)
                return endWdEquityUsd > 0.0 && startWdEquityUsd > 0.0
                    ? endWdEquityUsd - startWdEquityUsd
                    : endWdEquityUsd;

            return endWdEquityUsd;
        }

        private static string BuildCalculationSummary(Mt5WdEquityZComputationMode computationMode)
        {
            if (computationMode == Mt5WdEquityZComputationMode.DeltaFromStartWhenBothPositive)
                return "End WD Equity Z = max(End Equity - End Credits - Protected Bonuses, 0); Start WD Equity Z = max(Start Equity - Start Credits - Start Protected Bonuses, 0); final WD Equity Z = End WD Equity Z - Start WD Equity Z when both are positive, otherwise End WD Equity Z.";

            return "WD Equity Z = max(End Equity - End Credits - Protected Bonuses, 0).";
        }

        private static Dictionary<string, Mt5UsdConversionRate> NormalizeRates(IDictionary<string, Mt5UsdConversionRate> usdRates)
        {
            var normalized = new Dictionary<string, Mt5UsdConversionRate>(StringComparer.OrdinalIgnoreCase);
            if (usdRates != null)
            {
                foreach (KeyValuePair<string, Mt5UsdConversionRate> pair in usdRates)
                {
                    if (string.IsNullOrWhiteSpace(pair.Key))
                        continue;

                    Mt5UsdConversionRate rate = pair.Value ?? new Mt5UsdConversionRate();
                    normalized[pair.Key] = new Mt5UsdConversionRate
                    {
                        Currency = string.IsNullOrWhiteSpace(rate.Currency) ? pair.Key : rate.Currency,
                        FxSymbol = rate.FxSymbol,
                        Bid = rate.Bid,
                        Ask = rate.Ask,
                        UsdBase = rate.UsdBase,
                        PositiveToUsd = rate.PositiveToUsd,
                        NegativeToUsd = rate.NegativeToUsd
                    };
                }
            }

            if (!normalized.ContainsKey("USD"))
            {
                normalized["USD"] = new Mt5UsdConversionRate
                {
                    Currency = "USD",
                    FxSymbol = "USD",
                    Bid = 1.0,
                    Ask = 1.0,
                    UsdBase = false,
                    PositiveToUsd = 1.0,
                    NegativeToUsd = 1.0
                };
            }

            return normalized;
        }

        private static double ConvertNativeToUsd(
            double nativeAmount,
            string currency,
            IDictionary<string, Mt5UsdConversionRate> usdRates,
            ISet<string> missingCurrencies)
        {
            if (nativeAmount == 0.0)
                return 0.0;

            string effectiveCurrency = string.IsNullOrWhiteSpace(currency) ? "USD" : currency;
            Mt5UsdConversionRate rate;
            if (usdRates != null && usdRates.TryGetValue(effectiveCurrency, out rate) && rate != null)
            {
                double usdRate = nativeAmount >= 0.0 ? rate.PositiveToUsd : rate.NegativeToUsd;
                if (usdRate > 0.0)
                    return nativeAmount * usdRate;
            }

            if (missingCurrencies != null)
                missingCurrencies.Add(effectiveCurrency);

            return 0.0;
        }

	        private static List<string> NormalizeStrings(IEnumerable<string> values)
	        {
	            return values == null
	                ? new List<string>()
                : values
                    .Where(value => !string.IsNullOrWhiteSpace(value))
                    .Distinct(StringComparer.OrdinalIgnoreCase)
	                    .OrderBy(value => value, StringComparer.OrdinalIgnoreCase)
	                    .ToList();
	        }
	    }

    public sealed class Mt5LiveWdEquityZCalculator : IMt5LiveWdEquityZCalculator
    {
        public Mt5LiveWdEquityZReport Calculate(
            IEnumerable<Mt5TradingAccountRow> filteredAccounts,
            IDictionary<string, Mt5UsdConversionRate> usdRates,
            Mt5LiveWdEquityZCrmBonusTotals crmBonusTotals,
            Mt5LiveWdEquityZRequest request)
        {
            if (filteredAccounts == null)
                throw new ArgumentNullException("filteredAccounts");
            if (request == null)
                throw new ArgumentNullException("request");

            List<Mt5TradingAccountRow> rows = filteredAccounts.Where(row => row != null).ToList();
            Dictionary<string, Mt5UsdConversionRate> normalizedRates = NormalizeRates(usdRates);
            var missingCurrencies = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            double balanceUsdTotal = 0.0;
            double floatingPnlUsdTotal = 0.0;
            for (int i = 0; i < rows.Count; i++)
            {
                Mt5TradingAccountRow row = rows[i];
                string currency = string.IsNullOrWhiteSpace(row.Currency) ? "USD" : row.Currency;
                balanceUsdTotal += ConvertNativeToUsd(row.Balance, currency, normalizedRates, missingCurrencies);
                floatingPnlUsdTotal += ConvertNativeToUsd(row.Profit, currency, normalizedRates, missingCurrencies);
            }

            Mt5LiveWdEquityZCrmBonusTotals bonusTotals = crmBonusTotals ?? new Mt5LiveWdEquityZCrmBonusTotals();
            DateTime asOfUtc = NormalizeUtc(request.AsOfUtc);
            double preClampWdEquityUsd = balanceUsdTotal + floatingPnlUsdTotal - bonusTotals.TotalCumulativeBonusUsd;

            var report = new Mt5LiveWdEquityZReport
            {
                GeneratedAt = DateTime.Now,
                AsOfUtc = asOfUtc,
                CrmQueryAsOfUtc = bonusTotals.AsOfUtc == default(DateTime) ? asOfUtc : NormalizeUtc(bonusTotals.AsOfUtc),
                FilterZeroEquityAndBalance = request.FilterZeroEquityAndBalance,
                BonusScopePositiveBalanceOnly = request.BonusScopePositiveBalanceOnly,
                IncludedAccountCount = rows.Count,
                BonusScopeLoginCount = bonusTotals.ScopeLoginCount,
                CrmMatchedLoginCount = bonusTotals.MatchedLoginCount,
                CrmMatchedTransactionCount = bonusTotals.MatchedTransactionCount,
                BalanceUsdTotal = balanceUsdTotal,
                FloatingPnlUsdTotal = floatingPnlUsdTotal,
                CumulativeBonusUsd = bonusTotals.TotalCumulativeBonusUsd,
                PreClampWdEquityUsd = preClampWdEquityUsd,
                WdEquityZUsd = Math.Max(0.0, preClampWdEquityUsd),
                CalculationSummary = BuildCalculationSummary(
                    rows.Count,
                    balanceUsdTotal,
                    floatingPnlUsdTotal,
                    bonusTotals.TotalCumulativeBonusUsd,
                    preClampWdEquityUsd),
                MissingCurrencyRates = NormalizeStrings(missingCurrencies),
                Assumptions = NormalizeStrings(
                    new[]
                    {
                        "Live WD Equity Z = max(sum(Balance USD) + sum(Floating PnL USD) - CRM cumulative bonuses, 0).",
                        "Balance and Floating PnL are converted with the supplied MT5 live USD rate table.",
                        request.FilterZeroEquityAndBalance
                            ? "Accounts with zero equity or zero balance are excluded before summation."
                            : "No zero-equity/zero-balance account pre-filter was applied.",
                        request.BonusScopePositiveBalanceOnly
                            ? "CRM cumulative bonuses are scoped to included accounts whose current balance is greater than zero."
                            : "CRM cumulative bonuses are scoped to all included accounts."
                    }),
                CrmBonusLogins = request.IncludeCrmBonusLoginRows
                    ? bonusTotals.LoginTotals
                        .Where(item => item != null)
                        .OrderBy(item => item.Login)
                        .ToList()
                    : new List<Mt5LiveWdEquityZCrmBonusLoginTotal>()
            };

            return report;
        }

        private static string BuildCalculationSummary(
            int includedAccountCount,
            double balanceUsdTotal,
            double floatingPnlUsdTotal,
            double cumulativeBonusUsd,
            double preClampWdEquityUsd)
        {
            return string.Format(
                CultureInfo.InvariantCulture,
                "WD Equity Z = max(Balance USD {0:N2} + Floating PnL USD {1:N2} - Cumulative Bonus USD {2:N2}, 0) = {3:N2}. Included {4:N0} live trading accounts.",
                balanceUsdTotal,
                floatingPnlUsdTotal,
                cumulativeBonusUsd,
                Math.Max(0.0, preClampWdEquityUsd),
                includedAccountCount);
        }

        private static DateTime NormalizeUtc(DateTime value)
        {
            if (value == default(DateTime))
                return DateTime.UtcNow;

            if (value.Kind == DateTimeKind.Utc)
                return value;

            if (value.Kind == DateTimeKind.Local)
                return value.ToUniversalTime();

            return DateTime.SpecifyKind(value, DateTimeKind.Utc);
        }

        private static Dictionary<string, Mt5UsdConversionRate> NormalizeRates(IDictionary<string, Mt5UsdConversionRate> usdRates)
        {
            var normalized = new Dictionary<string, Mt5UsdConversionRate>(StringComparer.OrdinalIgnoreCase);
            if (usdRates != null)
            {
                foreach (KeyValuePair<string, Mt5UsdConversionRate> pair in usdRates)
                {
                    if (string.IsNullOrWhiteSpace(pair.Key))
                        continue;

                    Mt5UsdConversionRate rate = pair.Value ?? new Mt5UsdConversionRate();
                    normalized[pair.Key] = new Mt5UsdConversionRate
                    {
                        Currency = string.IsNullOrWhiteSpace(rate.Currency) ? pair.Key : rate.Currency,
                        FxSymbol = rate.FxSymbol,
                        Bid = rate.Bid,
                        Ask = rate.Ask,
                        UsdBase = rate.UsdBase,
                        PositiveToUsd = rate.PositiveToUsd,
                        NegativeToUsd = rate.NegativeToUsd
                    };
                }
            }

            if (!normalized.ContainsKey("USD"))
            {
                normalized["USD"] = new Mt5UsdConversionRate
                {
                    Currency = "USD",
                    FxSymbol = "USD",
                    Bid = 1.0,
                    Ask = 1.0,
                    UsdBase = false,
                    PositiveToUsd = 1.0,
                    NegativeToUsd = 1.0
                };
            }

            return normalized;
        }

        private static double ConvertNativeToUsd(
            double nativeAmount,
            string currency,
            IDictionary<string, Mt5UsdConversionRate> usdRates,
            ISet<string> missingCurrencies)
        {
            if (nativeAmount == 0.0)
                return 0.0;

            string effectiveCurrency = string.IsNullOrWhiteSpace(currency) ? "USD" : currency;
            Mt5UsdConversionRate rate;
            if (usdRates != null && usdRates.TryGetValue(effectiveCurrency, out rate) && rate != null)
            {
                double usdRate = nativeAmount >= 0.0 ? rate.PositiveToUsd : rate.NegativeToUsd;
                if (usdRate > 0.0)
                    return nativeAmount * usdRate;
            }

            if (missingCurrencies != null)
                missingCurrencies.Add(effectiveCurrency);

            return 0.0;
        }

        private static List<string> NormalizeStrings(IEnumerable<string> values)
        {
            return values == null
                ? new List<string>()
                : values
                    .Where(value => !string.IsNullOrWhiteSpace(value))
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .OrderBy(value => value, StringComparer.OrdinalIgnoreCase)
                    .ToList();
        }
    }

	    public sealed class Mt5DailyPnlCashCalculator : IMt5DailyPnlCashCalculator
	    {
	        public Mt5DailyPnlCashReport Calculate(Mt5DailyPnlCashInputs inputs)
	        {
	            if (inputs == null)
	                throw new ArgumentNullException("inputs");

	            var wdCalculator = new Mt5WdEquityZCalculator();
	            Mt5WdEquityZReport wdReport = wdCalculator.Calculate(
	                new Mt5WdEquityZInputs
	                {
	                    ReportDate = inputs.ReportDate,
	                    ComputationMode = Mt5WdEquityZComputationMode.EndOnly,
	                    EndEquityUsd = inputs.EndEquityUsd,
	                    EndCreditsUsd = inputs.EndCreditsUsd,
	                    EndProtectedBonusesUsd = inputs.EndProtectedBonusesUsd,
	                    StartEquityUsd = inputs.StartEquityUsd,
	                    StartCreditsUsd = inputs.StartCreditsUsd,
	                    StartProtectedBonusesUsd = inputs.StartProtectedBonusesUsd
	                });

	            return CreateReport(
	                inputs.ReportDate,
	                wdReport,
	                inputs.NetDepositsUsd,
	                inputs.MissingCurrencyRates,
	                inputs.Assumptions);
	        }

	        public Mt5DailyPnlCashReport Calculate(
	            DateTime reportDate,
	            IEnumerable<Mt5DailyReportRow> dailyRows,
	            IDictionary<string, Mt5UsdConversionRate> usdRates,
	            double startProtectedBonusesUsd,
	            double endProtectedBonusesUsd,
	            double netDepositsUsd)
	        {
	            if (dailyRows == null)
	                throw new ArgumentNullException("dailyRows");

	            List<Mt5DailyReportRow> rows = dailyRows.Where(row => row != null).ToList();
	            var wdCalculator = new Mt5WdEquityZCalculator();
	            Mt5WdEquityZReport wdReport = wdCalculator.Calculate(
	                reportDate,
	                rows,
	                usdRates,
	                startProtectedBonusesUsd,
	                endProtectedBonusesUsd,
	                Mt5WdEquityZComputationMode.EndOnly);

	            Mt5DailyPnlCashReport report = CreateReport(
	                reportDate,
	                wdReport,
	                netDepositsUsd,
	                wdReport.MissingCurrencyRates,
	                new List<string>
	                {
	                    "Start equity uses EquityPrevDay from MT5 daily rows.",
	                    "Start credits are derived as Credit minus DailyCredit because MT5 daily rows do not expose CreditPrevDay.",
	                    "Protected bonus totals must be supplied separately from filtered balance deals.",
	                    "Net deposits must be supplied separately from MT5 deposit/withdrawal balance deals.",
	                    "USD conversion uses the supplied MT5 USD rate table."
	                });

	            report.DailyRowCount = rows.Count;
	            return report;
	        }

	        private static Mt5DailyPnlCashReport CreateReport(
	            DateTime reportDate,
	            Mt5WdEquityZReport wdReport,
	            double netDepositsUsd,
	            IEnumerable<string> missingCurrencyRates,
	            IEnumerable<string> assumptions)
	        {
	            if (wdReport == null)
	                throw new ArgumentNullException("wdReport");

	            double endCleanEquityUsd = wdReport.EndEquityUsd - wdReport.EndCreditsUsd - wdReport.EndProtectedBonusesUsd;
	            double startCleanEquityUsd = wdReport.StartEquityUsd - wdReport.StartCreditsUsd - wdReport.StartProtectedBonusesUsd;
	            double endPositiveCleanEquityUsd = wdReport.EndWdEquityUsd;
	            double startPositiveCleanEquityUsd = wdReport.StartWdEquityUsd;

	            return new Mt5DailyPnlCashReport
	            {
	                GeneratedAt = DateTime.Now,
	                ReportDate = reportDate == default(DateTime) ? DateTime.Today : reportDate.Date,
	                EndEquityUsd = wdReport.EndEquityUsd,
	                EndCreditsUsd = wdReport.EndCreditsUsd,
	                EndProtectedBonusesUsd = wdReport.EndProtectedBonusesUsd,
	                EndCleanEquityUsd = endCleanEquityUsd,
	                EndPositiveCleanEquityUsd = endPositiveCleanEquityUsd,
	                StartEquityUsd = wdReport.StartEquityUsd,
	                StartCreditsUsd = wdReport.StartCreditsUsd,
	                StartProtectedBonusesUsd = wdReport.StartProtectedBonusesUsd,
	                StartCleanEquityUsd = startCleanEquityUsd,
	                StartPositiveCleanEquityUsd = startPositiveCleanEquityUsd,
	                NetDepositsUsd = netDepositsUsd,
	                DailyPnlCashUsd = endPositiveCleanEquityUsd - startPositiveCleanEquityUsd - netDepositsUsd,
	                CalculationSummary = BuildCalculationSummary(),
	                MissingCurrencyRates = NormalizeStrings(missingCurrencyRates),
	                Assumptions = NormalizeStrings(assumptions)
	            };
	        }

	        private static string BuildCalculationSummary()
	        {
	            return "Clean Equity = Equity - Credits - Protected Bonuses; Daily PnL Cash = max(End Clean Equity, 0) - max(Start Clean Equity, 0) - Net Deposits. This matches the four-case rule for positive/negative start and end clean equity.";
	        }

	        private static List<string> NormalizeStrings(IEnumerable<string> values)
	        {
	            return values == null
	                ? new List<string>()
	                : values
	                    .Where(value => !string.IsNullOrWhiteSpace(value))
	                    .Distinct(StringComparer.OrdinalIgnoreCase)
	                    .OrderBy(value => value, StringComparer.OrdinalIgnoreCase)
	                    .ToList();
	        }
	    }

    public static class Mt5DailyReportGenerator
    {
        public static Mt5DailyReportSnapshot Generate(Mt5MonitorSettings settings, DateTime reportDate, Action<string> statusWriter)
        {
            return Generate(settings, reportDate, reportDate, statusWriter);
        }

        public static Mt5DailyReportSnapshot Generate(Mt5MonitorSettings settings, DateTime reportDate)
        {
            return Generate(settings, reportDate, reportDate, null);
        }

        public static Mt5DailyReportSnapshot Generate(
            Mt5MonitorSettings settings,
            DateTime fromDate,
            DateTime toDate,
            Action<string> statusWriter)
        {
            if (settings == null)
                throw new ArgumentNullException("settings");

            DateTime normalizedFrom = fromDate.Date;
            DateTime normalizedTo = toDate.Date;
            if (normalizedTo < normalizedFrom)
                throw new InvalidOperationException("Daily report end date must be on or after the start date.");

            settings.Validate();

            Action<string> writer = statusWriter ?? (_ => { });
            writer(string.Format(
                CultureInfo.InvariantCulture,
                "Generating daily report for {0:yyyy-MM-dd} to {1:yyyy-MM-dd}.",
                normalizedFrom,
                normalizedTo));

            MTRetCode initializeResult = SMTManagerAPIFactory.Initialize(settings.SdkLibsPath);
            if (initializeResult != MTRetCode.MT_RET_OK)
                throw new InvalidOperationException("Initialize failed: " + initializeResult);

            CIMTManagerAPI manager = Mt5MonitorCollector.Connect(settings.Server, settings.Login, settings.Password, writer);
            if (manager == null)
            {
                SMTManagerAPIFactory.Shutdown();
                throw new InvalidOperationException("Failed to connect to MT5 Manager API.");
            }

            try
            {
                Dictionary<string, string> groupCurrencies = Mt5MonitorCollector.LoadGroupCurrencies(manager, settings.GroupMask);
                Mt5DailyReportSnapshot snapshot = Mt5MonitorCollector.CollectDailyReport(
                    manager,
                    groupCurrencies,
                    settings.GroupMask,
                    normalizedFrom,
                    normalizedTo,
                    writer);

                writer(string.Format(
                    CultureInfo.InvariantCulture,
                    "Daily report ready with {0} rows.",
                    snapshot.Rows != null ? snapshot.Rows.Count : 0));

                return snapshot;
            }
            finally
            {
                Mt5MonitorCollector.Disconnect(manager);
                SMTManagerAPIFactory.Shutdown();
            }
        }

        public static Mt5DailyReportSnapshot Generate(Mt5MonitorSettings settings, DateTime fromDate, DateTime toDate)
        {
            return Generate(settings, fromDate, toDate, null);
        }

        public static Mt5DailyReportJsonDocument GenerateJsonDocument(
            Mt5MonitorSettings settings,
            DateTime fromDate,
            DateTime toDate,
            Action<string> statusWriter)
        {
            return Mt5MonitorJsonExporter.CreateDailyReportDocument(
                Generate(settings, fromDate, toDate, statusWriter),
                settings);
        }

        public static Mt5DailyReportJsonDocument GenerateJsonDocument(
            Mt5MonitorSettings settings,
            DateTime fromDate,
            DateTime toDate)
        {
            return GenerateJsonDocument(settings, fromDate, toDate, null);
        }

        public static Mt5DailyReportJsonDocument GenerateJsonDocument(
            Mt5MonitorSettings settings,
            DateTime reportDate)
        {
            return GenerateJsonDocument(settings, reportDate, reportDate, null);
        }

        public static string GenerateJson(
            Mt5MonitorSettings settings,
            DateTime fromDate,
            DateTime toDate,
            Action<string> statusWriter,
            bool indented)
        {
            return Mt5MonitorJsonExporter.BuildDailyReportJson(
                Generate(settings, fromDate, toDate, statusWriter),
                settings,
                indented);
        }

        public static string GenerateJson(
            Mt5MonitorSettings settings,
            DateTime fromDate,
            DateTime toDate,
            Action<string> statusWriter)
        {
            return GenerateJson(settings, fromDate, toDate, statusWriter, true);
        }

        public static string GenerateJson(
            Mt5MonitorSettings settings,
            DateTime fromDate,
            DateTime toDate)
        {
            return GenerateJson(settings, fromDate, toDate, null, true);
        }

        public static string GenerateJson(
            Mt5MonitorSettings settings,
            DateTime reportDate)
        {
            return GenerateJson(settings, reportDate, reportDate, null, true);
        }

        public static string GenerateCsv(
            Mt5MonitorSettings settings,
            DateTime fromDate,
            DateTime toDate,
            Action<string> statusWriter)
        {
            return Mt5MonitorCsvExporter.BuildDailyReportCsv(
                Generate(settings, fromDate, toDate, statusWriter),
                settings);
        }

        public static string GenerateCsv(
            Mt5MonitorSettings settings,
            DateTime fromDate,
            DateTime toDate)
        {
            return GenerateCsv(settings, fromDate, toDate, null);
        }

        public static string GenerateCsv(
            Mt5MonitorSettings settings,
            DateTime reportDate)
        {
            return GenerateCsv(settings, reportDate, reportDate, null);
        }
    }

    internal static class Mt5DailyReportAvailabilityResolver
    {
        public static Mt5DailyReportSnapshot CollectResolvedDailyReport(
            CIMTManagerAPI manager,
            Dictionary<string, string> groupCurrencies,
            string groupMask,
            DateTime requestedReportDate,
            bool useLatestAvailableWhenMissing,
            int lookbackDays,
            Action<string> statusWriter,
            out DateTime resolvedReportDate,
            out bool usedFallback)
        {
            if (manager == null)
                throw new ArgumentNullException("manager");

            DateTime requestedDate = requestedReportDate.Date;
            int safeLookbackDays = Math.Max(0, lookbackDays);
            Action<string> writer = statusWriter ?? (_ => { });
            DateTime candidateDate = requestedDate;

            for (int offset = 0; ; offset++)
            {
                try
                {
                    Mt5DailyReportSnapshot snapshot = Mt5MonitorCollector.CollectDailyReport(
                        manager,
                        groupCurrencies,
                        groupMask,
                        candidateDate,
                        candidateDate,
                        writer);

                    resolvedReportDate = candidateDate;
                    usedFallback = candidateDate != requestedDate;
                    if (usedFallback)
                    {
                        writer(string.Format(
                            CultureInfo.InvariantCulture,
                            "No MT5 daily rows were available for {0:yyyy-MM-dd}; using latest available closed day {1:yyyy-MM-dd}.",
                            requestedDate,
                            candidateDate));
                    }

                    return snapshot;
                }
                catch (InvalidOperationException ex)
                {
                    bool canFallback = useLatestAvailableWhenMissing &&
                        offset < safeLookbackDays &&
                        IsDailyReportNotFoundException(ex);

                    if (!canFallback)
                        throw;

                    candidateDate = candidateDate.AddDays(-1);
                }
            }
        }

        private static bool IsDailyReportNotFoundException(InvalidOperationException exception)
        {
            if (exception == null)
                return false;

            return exception.Message != null &&
                exception.Message.IndexOf("DailyRequestByGroup failed: MT_RET_ERR_NOTFOUND", StringComparison.OrdinalIgnoreCase) >= 0;
        }
    }

    internal static class Mt5LiveWdEquityZCrmBonusReader
    {
        private const int LoginInsertChunkSize = 1000;

        private sealed class CrmSqlSettings
        {
            public string Host;
            public int Port;
            public string Database;
            public string User;
            public string Password;

            public static CrmSqlSettings FromEnvironment()
            {
                return new CrmSqlSettings
                {
                    Host = RequireEnvironmentVariable("CRM_HOST"),
                    Port = ParsePort(Environment.GetEnvironmentVariable("CRM_PORT")),
                    Database = RequireEnvironmentVariable("CRM_DB"),
                    User = RequireEnvironmentVariable("CRM_USER"),
                    Password = RequireEnvironmentVariable("CRM_PASS")
                };
            }

            public string BuildConnectionString()
            {
                var builder = new SqlConnectionStringBuilder();
                builder.DataSource = string.Format(
                    CultureInfo.InvariantCulture,
                    "{0},{1}",
                    Host,
                    Port);
                builder.InitialCatalog = Database;
                builder.UserID = User;
                builder.Password = Password;
                builder.Encrypt = true;
                builder.TrustServerCertificate = false;
                builder.ConnectTimeout = 15;
                builder.ApplicationName = "cro-bridge-live-wd-equity";
                return builder.ConnectionString;
            }

            private static string RequireEnvironmentVariable(string name)
            {
                string value = Environment.GetEnvironmentVariable(name);
                if (string.IsNullOrWhiteSpace(value))
                    throw new InvalidOperationException(name + " env var is required for live CRM WD Equity Z.");
                return value;
            }

            private static int ParsePort(string value)
            {
                int parsed;
                return int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out parsed) && parsed > 0
                    ? parsed
                    : 1433;
            }
        }

        public static Mt5LiveWdEquityZCrmBonusTotals Collect(
            IEnumerable<ulong> scopeLogins,
            DateTime asOfUtc,
            bool includeLoginRows,
            Action<string> statusWriter)
        {
            Action<string> writer = statusWriter ?? (_ => { });
            List<ulong> logins = scopeLogins == null
                ? new List<ulong>()
                : scopeLogins
                    .Where(login => login != 0)
                    .Distinct()
                    .OrderBy(login => login)
                    .ToList();

            DateTime normalizedAsOfUtc = NormalizeUtc(asOfUtc);
            var totals = new Mt5LiveWdEquityZCrmBonusTotals
            {
                AsOfUtc = normalizedAsOfUtc,
                ScopeLoginCount = logins.Count
            };

            if (logins.Count == 0)
            {
                writer("CRM cumulative bonus scope is empty; skipping CRM bonus query.");
                return totals;
            }

            writer(string.Format(
                CultureInfo.InvariantCulture,
                "Requesting CRM cumulative bonuses for {0:N0} logins as of {1:yyyy-MM-dd HH:mm:ss}Z.",
                logins.Count,
                normalizedAsOfUtc));

            CrmSqlSettings sqlSettings = CrmSqlSettings.FromEnvironment();
            using (var connection = new SqlConnection(sqlSettings.BuildConnectionString()))
            {
                connection.Open();

                using (var createCommand = connection.CreateCommand())
                {
                    createCommand.CommandTimeout = 120;
                    createCommand.CommandText = "CREATE TABLE #wd_bonus_scope_logins (login BIGINT NOT NULL PRIMARY KEY);";
                    createCommand.ExecuteNonQuery();
                }

                for (int offset = 0; offset < logins.Count; offset += LoginInsertChunkSize)
                {
                    List<ulong> chunk = logins
                        .Skip(offset)
                        .Take(LoginInsertChunkSize)
                        .ToList();

                    var commandText = new StringBuilder(64 + (chunk.Count * 16));
                    commandText.Append("INSERT INTO #wd_bonus_scope_logins (login) VALUES ");

                    using (var insertCommand = connection.CreateCommand())
                    {
                        insertCommand.CommandTimeout = 120;
                        for (int i = 0; i < chunk.Count; i++)
                        {
                            if (i > 0)
                                commandText.Append(", ");

                            string parameterName = "@p" + i.ToString(CultureInfo.InvariantCulture);
                            commandText.Append("(");
                            commandText.Append(parameterName);
                            commandText.Append(")");
                            insertCommand.Parameters.Add(parameterName, SqlDbType.BigInt).Value = unchecked((long)chunk[i]);
                        }

                        insertCommand.CommandText = commandText.ToString();
                        insertCommand.ExecuteNonQuery();
                    }
                }

                using (var queryCommand = connection.CreateCommand())
                {
                    queryCommand.CommandTimeout = 180;
                    queryCommand.CommandText = @"
SELECT
    scoped.login,
    COUNT(*) AS tx_count,
    SUM(
        CASE
            WHEN tx.transactiontype = 'Deposit'
             AND tx.transaction_type_name IN ('Bonus', 'FRF Commission')
                THEN CAST(tx.usdamount AS FLOAT)
            WHEN tx.transactiontype = 'Withdrawal'
             AND tx.transaction_type_name IN ('BonusCancelled', 'FRF Commission Cancelled')
                THEN -CAST(tx.usdamount AS FLOAT)
            ELSE 0
        END
    ) AS net_amount_usd
FROM #wd_bonus_scope_logins scoped
JOIN report.vtiger_mttransactions tx
  ON tx.login = scoped.login
WHERE tx.confirmation_time IS NOT NULL
  AND tx.confirmation_time <= @as_of_utc
  AND tx.transactionapproval = 'Approved'
  AND (tx.deleted IS NULL OR tx.deleted = 0)
  AND (
        (tx.transactiontype = 'Deposit' AND tx.transaction_type_name IN ('Bonus', 'FRF Commission'))
     OR (tx.transactiontype = 'Withdrawal' AND tx.transaction_type_name IN ('BonusCancelled', 'FRF Commission Cancelled'))
  )
GROUP BY scoped.login
ORDER BY scoped.login;";
                    queryCommand.Parameters.Add("@as_of_utc", SqlDbType.DateTime2).Value = normalizedAsOfUtc;

                    using (SqlDataReader reader = queryCommand.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            ulong login = Convert.ToUInt64(reader.GetValue(0), CultureInfo.InvariantCulture);
                            int transactionCount = Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture);
                            double netAmountUsd = reader.IsDBNull(2)
                                ? 0.0
                                : Convert.ToDouble(reader.GetValue(2), CultureInfo.InvariantCulture);

                            totals.MatchedLoginCount++;
                            totals.MatchedTransactionCount += transactionCount;
                            totals.TotalCumulativeBonusUsd += netAmountUsd;

                            if (includeLoginRows)
                            {
                                totals.LoginTotals.Add(new Mt5LiveWdEquityZCrmBonusLoginTotal
                                {
                                    Login = login,
                                    NetAmountUsd = netAmountUsd,
                                    TransactionCount = transactionCount
                                });
                            }
                        }
                    }
                }
            }

            writer(string.Format(
                CultureInfo.InvariantCulture,
                "CRM cumulative bonus query matched {0:N0} logins, {1:N0} transactions, total {2:N2} USD.",
                totals.MatchedLoginCount,
                totals.MatchedTransactionCount,
                totals.TotalCumulativeBonusUsd));

            return totals;
        }

        private static DateTime NormalizeUtc(DateTime value)
        {
            if (value == default(DateTime))
                return DateTime.UtcNow;

            if (value.Kind == DateTimeKind.Utc)
                return value;

            if (value.Kind == DateTimeKind.Local)
                return value.ToUniversalTime();

            return DateTime.SpecifyKind(value, DateTimeKind.Utc);
        }
    }

    public static class Mt5LiveWdEquityZGenerator
    {
        public static Mt5LiveWdEquityZReport Generate(
            Mt5MonitorSettings settings,
            Mt5LiveWdEquityZRequest request,
            Action<string> statusWriter)
        {
            if (settings == null)
                throw new ArgumentNullException("settings");
            if (request == null)
                throw new ArgumentNullException("request");

            settings.Validate();

            Action<string> writer = statusWriter ?? (_ => { });
            DateTime asOfUtc = NormalizeUtc(request.AsOfUtc);
            writer(string.Format(
                CultureInfo.InvariantCulture,
                "Generating live WD Equity Z from Trading Accounts + CRM bonuses as of {0:yyyy-MM-dd HH:mm:ss}Z.",
                asOfUtc));

            MTRetCode initializeResult = SMTManagerAPIFactory.Initialize(settings.SdkLibsPath);
            if (initializeResult != MTRetCode.MT_RET_OK)
                throw new InvalidOperationException("Initialize failed: " + initializeResult);

            CIMTManagerAPI manager = Mt5MonitorCollector.Connect(settings.Server, settings.Login, settings.Password, writer);
            if (manager == null)
            {
                SMTManagerAPIFactory.Shutdown();
                throw new InvalidOperationException("Failed to connect to MT5 Manager API.");
            }

            try
            {
                Dictionary<string, string> groupCurrencies = Mt5MonitorCollector.LoadGroupCurrencies(manager, settings.GroupMask);
                Dictionary<ulong, Mt5LoginContext> loginContexts = Mt5MonitorCollector.LoadLoginContexts(manager, settings.GroupMask, groupCurrencies);
                IDictionary<string, Mt5UsdConversionRate> usdRates = Mt5UsdRateLoader.LoadLiveRates(manager);
                Mt5TradingAccountsSnapshot snapshot = Mt5MonitorCollector.CollectTradingAccounts(
                    manager,
                    groupCurrencies,
                    loginContexts,
                    settings.GroupMask,
                    writer);

                HashSet<string> excludedGroups = NormalizeExcludedGroups(request.ExcludedGroups);
                int rawAccountCount = 0;
                int skippedZeroEquityCount = 0;
                int skippedZeroBalanceCount = 0;
                int skippedExcludedGroupCount = 0;
                var includedAccounts = new List<Mt5TradingAccountRow>();
                var bonusScopeLogins = new HashSet<ulong>();

                IList<Mt5TradingAccountRow> rows = snapshot.Rows ?? new List<Mt5TradingAccountRow>();
                for (int i = 0; i < rows.Count; i++)
                {
                    Mt5TradingAccountRow row = rows[i];
                    if (row == null)
                        continue;

                    rawAccountCount++;

                    if (excludedGroups.Contains(row.Group ?? string.Empty))
                    {
                        skippedExcludedGroupCount++;
                        continue;
                    }

                    if (request.FilterZeroEquityAndBalance && IsEffectivelyZero(row.Equity))
                    {
                        skippedZeroEquityCount++;
                        continue;
                    }

                    if (request.FilterZeroEquityAndBalance && IsEffectivelyZero(row.Balance))
                    {
                        skippedZeroBalanceCount++;
                        continue;
                    }

                    includedAccounts.Add(row);
                    if (!request.BonusScopePositiveBalanceOnly || row.Balance > 0.0)
                        bonusScopeLogins.Add(row.Login);
                }

                Mt5LiveWdEquityZCrmBonusTotals crmBonusTotals = Mt5LiveWdEquityZCrmBonusReader.Collect(
                    bonusScopeLogins,
                    asOfUtc,
                    request.IncludeCrmBonusLoginRows,
                    writer);

                var calculator = new Mt5LiveWdEquityZCalculator();
                Mt5LiveWdEquityZReport report = calculator.Calculate(
                    includedAccounts,
                    usdRates,
                    crmBonusTotals,
                    request);

                report.GeneratedAt = DateTime.Now;
                report.AsOfUtc = asOfUtc;
                report.CrmQueryAsOfUtc = crmBonusTotals.AsOfUtc == default(DateTime)
                    ? asOfUtc
                    : crmBonusTotals.AsOfUtc;
                report.RawAccountCount = rawAccountCount;
                report.IncludedAccountCount = includedAccounts.Count;
                report.SkippedZeroEquityCount = skippedZeroEquityCount;
                report.SkippedZeroBalanceCount = skippedZeroBalanceCount;
                report.SkippedExcludedGroupCount = skippedExcludedGroupCount;
                report.BonusScopeLoginCount = crmBonusTotals.ScopeLoginCount;
                report.CrmMatchedLoginCount = crmBonusTotals.MatchedLoginCount;
                report.CrmMatchedTransactionCount = crmBonusTotals.MatchedTransactionCount;
                report.Source = "trading_accounts_live_balance_plus_floating_minus_crm_bonus";
                report.Assumptions = report.Assumptions
                    .Concat(new[]
                    {
                        "Cumulative bonuses are read live from CRM report.vtiger_mttransactions.",
                        "CRM bonuses include Deposit/Bonus and Deposit/FRF Commission as positive amounts.",
                        "CRM bonus cancellations include Withdrawal/BonusCancelled and Withdrawal/FRF Commission Cancelled as negative amounts.",
                        "Only approved CRM rows with deleted IS NULL or 0 are included.",
                        "No local bonus sync table or manual overrides are applied in this phase."
                    })
                    .Concat(
                        skippedExcludedGroupCount > 0
                            ? new[]
                            {
                                string.Format(
                                    CultureInfo.InvariantCulture,
                                    "Excluded groups removed {0:N0} trading accounts before WD summation.",
                                    skippedExcludedGroupCount)
                            }
                            : Enumerable.Empty<string>())
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .OrderBy(value => value, StringComparer.OrdinalIgnoreCase)
                    .ToList();

                writer(string.Format(
                    CultureInfo.InvariantCulture,
                    "Live WD Equity Z ready. Raw accounts: {0:N0}, included: {1:N0}, CRM scope logins: {2:N0}, CRM txs: {3:N0}, value: {4:N2}.",
                    report.RawAccountCount,
                    report.IncludedAccountCount,
                    report.BonusScopeLoginCount,
                    report.CrmMatchedTransactionCount,
                    report.WdEquityZUsd));

                return report;
            }
            finally
            {
                Mt5MonitorCollector.Disconnect(manager);
                SMTManagerAPIFactory.Shutdown();
            }
        }

        public static Mt5LiveWdEquityZReport Generate(
            Mt5MonitorSettings settings,
            Mt5LiveWdEquityZRequest request)
        {
            return Generate(settings, request, null);
        }

        private static DateTime NormalizeUtc(DateTime value)
        {
            if (value == default(DateTime))
                return DateTime.UtcNow;

            if (value.Kind == DateTimeKind.Utc)
                return value;

            if (value.Kind == DateTimeKind.Local)
                return value.ToUniversalTime();

            return DateTime.SpecifyKind(value, DateTimeKind.Utc);
        }

        private static HashSet<string> NormalizeExcludedGroups(IEnumerable<string> groups)
        {
            return new HashSet<string>(
                groups == null
                    ? Enumerable.Empty<string>()
                    : groups.Where(group => !string.IsNullOrWhiteSpace(group)),
                StringComparer.OrdinalIgnoreCase);
        }

        private static bool IsEffectivelyZero(double value)
        {
            return Math.Abs(value) <= 0.0000001;
        }
    }

    public static class Mt5WdEquityZGenerator
    {
        public static Mt5WdEquityZReport Generate(
            Mt5MonitorSettings settings,
            Mt5WdEquityZRequest request,
            Action<string> statusWriter)
        {
            if (settings == null)
                throw new ArgumentNullException("settings");
            if (request == null)
                throw new ArgumentNullException("request");

            DateTime reportDate = request.ReportDate.Date;
            DateTime bonusHistoryFrom = request.BonusHistoryFrom.Date;
            if (reportDate == default(DateTime))
                throw new InvalidOperationException("ReportDate is required.");
            if (bonusHistoryFrom == default(DateTime))
                throw new InvalidOperationException("BonusHistoryFrom is required to reconstruct protected bonus balances.");
            if (bonusHistoryFrom > reportDate)
                throw new InvalidOperationException("BonusHistoryFrom must be on or before ReportDate.");
            if (string.IsNullOrWhiteSpace(request.BonusCommentContains))
                throw new InvalidOperationException("Bonus comment filter is required.");

            settings.Validate();

            Action<string> writer = statusWriter ?? (_ => { });
            writer(string.Format(
                CultureInfo.InvariantCulture,
                "Generating WD Equity Z for {0:yyyy-MM-dd} using bonus history from {1:yyyy-MM-dd}.",
                reportDate,
                bonusHistoryFrom));

            MTRetCode initializeResult = SMTManagerAPIFactory.Initialize(settings.SdkLibsPath);
            if (initializeResult != MTRetCode.MT_RET_OK)
                throw new InvalidOperationException("Initialize failed: " + initializeResult);

            CIMTManagerAPI manager = Mt5MonitorCollector.Connect(settings.Server, settings.Login, settings.Password, writer);
            if (manager == null)
            {
                SMTManagerAPIFactory.Shutdown();
                throw new InvalidOperationException("Failed to connect to MT5 Manager API.");
            }

            try
            {
                Dictionary<string, string> groupCurrencies = Mt5MonitorCollector.LoadGroupCurrencies(manager, settings.GroupMask);
                Dictionary<ulong, Mt5LoginContext> loginContexts = Mt5MonitorCollector.LoadLoginContexts(manager, settings.GroupMask, groupCurrencies);
                IDictionary<string, Mt5UsdConversionRate> usdRates = Mt5UsdRateLoader.LoadLiveRates(manager);
                DateTime resolvedReportDate;
                bool usedDailyFallback;
                Mt5DailyReportSnapshot dailySnapshot = Mt5DailyReportAvailabilityResolver.CollectResolvedDailyReport(
                    manager,
                    groupCurrencies,
                    settings.GroupMask,
                    reportDate,
                    request.UseLatestAvailableDailyReportDateWhenMissing,
                    request.LatestAvailableDailyReportLookbackDays,
                    writer,
                    out resolvedReportDate,
                    out usedDailyFallback);

                Mt5MonitorCollector.WdEquityZProtectedBonusCollection bonusCollection =
                    Mt5MonitorCollector.CollectWdEquityZProtectedBonuses(
                        manager,
                        groupCurrencies,
                        loginContexts,
                        settings.GroupMask,
                        bonusHistoryFrom,
                        resolvedReportDate,
                        request.BonusCommentContains,
                        request.IncludeBonusDealRows,
                        writer);

                var calculator = new Mt5WdEquityZCalculator();
                Mt5WdEquityZReport report = calculator.Calculate(
                    resolvedReportDate,
                    dailySnapshot.Rows ?? new List<Mt5DailyReportRow>(),
                    usdRates,
                    bonusCollection.StartProtectedBonusesUsd,
                    bonusCollection.EndProtectedBonusesUsd,
                    request.ComputationMode);

                report.GeneratedAt = DateTime.Now;
                report.RequestedReportDate = reportDate;
                report.ReportDate = resolvedReportDate;
                report.BonusHistoryFrom = bonusHistoryFrom;
                report.BonusCommentContains = request.BonusCommentContains;
                report.ComputationMode = request.ComputationMode;
                report.UsedLatestAvailableDailyReportDateFallback = usedDailyFallback;
                report.DailyRows = dailySnapshot.Rows != null
                    ? new List<Mt5DailyReportRow>(dailySnapshot.Rows)
                    : new List<Mt5DailyReportRow>();
                report.DailyRowCount = report.DailyRows.Count;
                report.ProtectedBonusDealCount = bonusCollection.DealCount;
                report.ProtectedBonusDeals = request.IncludeBonusDealRows
                    ? bonusCollection.Deals
                    : new List<Mt5WdEquityZProtectedBonusDeal>();
                report.MissingCurrencyRates = report.MissingCurrencyRates
                    .Concat(bonusCollection.MissingCurrencyRates)
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .OrderBy(code => code, StringComparer.OrdinalIgnoreCase)
                    .ToList();
                report.Assumptions = report.Assumptions
                    .Concat(
                        new[]
                        {
                            string.Format(
                                CultureInfo.InvariantCulture,
                                "Protected bonuses are reconstructed from ActionBalance deals whose comment contains '{0}'.",
                                request.BonusCommentContains),
                            string.Format(
                                CultureInfo.InvariantCulture,
                                "BonusHistoryFrom ({0:yyyy-MM-dd}) must be early enough to capture the full protected-bonus balance you want to subtract.",
                                bonusHistoryFrom)
                        }
                        .Concat(
                            usedDailyFallback
                                ? new[]
                                {
                                    string.Format(
                                        CultureInfo.InvariantCulture,
                                        "Requested ReportDate {0:yyyy-MM-dd} had no MT5 daily rows; used latest available closed day {1:yyyy-MM-dd}.",
                                        reportDate,
                                        resolvedReportDate)
                                }
                                : Enumerable.Empty<string>()))
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .ToList();

                writer(string.Format(
                    CultureInfo.InvariantCulture,
                    "WD Equity Z ready. Daily rows: {0}, protected bonus deals: {1}, value: {2:N2}.",
                    report.DailyRowCount,
                    report.ProtectedBonusDealCount,
                    report.WdEquityZUsd));

                return report;
            }
            finally
            {
                Mt5MonitorCollector.Disconnect(manager);
                SMTManagerAPIFactory.Shutdown();
            }
        }

        public static Mt5WdEquityZReport Generate(
            Mt5MonitorSettings settings,
            Mt5WdEquityZRequest request)
        {
            return Generate(settings, request, null);
        }

        public static Mt5WdEquityZReport Generate(
            Mt5MonitorSettings settings,
            DateTime reportDate,
            DateTime bonusHistoryFrom,
            Action<string> statusWriter)
        {
            return Generate(
                settings,
                new Mt5WdEquityZRequest
                {
                    ReportDate = reportDate,
                    BonusHistoryFrom = bonusHistoryFrom
                },
                statusWriter);
        }

	        public static Mt5WdEquityZReport Generate(
	            Mt5MonitorSettings settings,
	            DateTime reportDate,
	            DateTime bonusHistoryFrom)
	        {
	            return Generate(settings, reportDate, bonusHistoryFrom, null);
	        }
	    }

	    public static class Mt5DailyPnlCashGenerator
	    {
	        public static Mt5DailyPnlCashReport Generate(
	            Mt5MonitorSettings settings,
	            Mt5DailyPnlCashRequest request,
	            Action<string> statusWriter)
	        {
	            if (settings == null)
	                throw new ArgumentNullException("settings");
	            if (request == null)
	                throw new ArgumentNullException("request");

	            DateTime reportDate = request.ReportDate.Date;
	            DateTime bonusHistoryFrom = request.BonusHistoryFrom.Date;
	            if (reportDate == default(DateTime))
	                throw new InvalidOperationException("ReportDate is required.");
	            if (bonusHistoryFrom == default(DateTime))
	                throw new InvalidOperationException("BonusHistoryFrom is required to reconstruct protected bonus balances.");
	            if (bonusHistoryFrom > reportDate)
	                throw new InvalidOperationException("BonusHistoryFrom must be on or before ReportDate.");
	            if (string.IsNullOrWhiteSpace(request.BonusCommentContains))
	                throw new InvalidOperationException("Bonus comment filter is required.");

	            List<string> excludedCommentContains = NormalizeCommentFilters(request.NetDepositExcludedCommentContains);
	            settings.Validate();

	            Action<string> writer = statusWriter ?? (_ => { });
	            writer(string.Format(
	                CultureInfo.InvariantCulture,
	                "Generating Daily PnL Cash for {0:yyyy-MM-dd} using bonus history from {1:yyyy-MM-dd}.",
	                reportDate,
	                bonusHistoryFrom));

	            MTRetCode initializeResult = SMTManagerAPIFactory.Initialize(settings.SdkLibsPath);
	            if (initializeResult != MTRetCode.MT_RET_OK)
	                throw new InvalidOperationException("Initialize failed: " + initializeResult);

	            CIMTManagerAPI manager = Mt5MonitorCollector.Connect(settings.Server, settings.Login, settings.Password, writer);
	            if (manager == null)
	            {
	                SMTManagerAPIFactory.Shutdown();
	                throw new InvalidOperationException("Failed to connect to MT5 Manager API.");
	            }

            try
            {
                Dictionary<string, string> groupCurrencies = Mt5MonitorCollector.LoadGroupCurrencies(manager, settings.GroupMask);
                Dictionary<ulong, Mt5LoginContext> loginContexts = Mt5MonitorCollector.LoadLoginContexts(manager, settings.GroupMask, groupCurrencies);
                IDictionary<string, Mt5UsdConversionRate> usdRates = Mt5UsdRateLoader.LoadLiveRates(manager);
                DateTime resolvedReportDate;
                bool usedDailyFallback;
                Mt5DailyReportSnapshot dailySnapshot = Mt5DailyReportAvailabilityResolver.CollectResolvedDailyReport(
                    manager,
                    groupCurrencies,
                    settings.GroupMask,
                    reportDate,
                    request.UseLatestAvailableDailyReportDateWhenMissing,
                    request.LatestAvailableDailyReportLookbackDays,
                    writer,
                    out resolvedReportDate,
                    out usedDailyFallback);

                Mt5MonitorCollector.WdEquityZProtectedBonusCollection bonusCollection =
                    Mt5MonitorCollector.CollectWdEquityZProtectedBonuses(
                        manager,
                        groupCurrencies,
                        loginContexts,
                        settings.GroupMask,
                        bonusHistoryFrom,
                        resolvedReportDate,
                        request.BonusCommentContains,
                        request.IncludeBonusDealRows,
                        writer);

	                Mt5MonitorCollector.DailyPnlCashNetDepositCollection netDepositCollection =
	                    Mt5MonitorCollector.CollectDailyPnlCashNetDeposits(
                        manager,
                        groupCurrencies,
                        loginContexts,
                        settings.GroupMask,
                        resolvedReportDate,
                        excludedCommentContains,
                        request.IncludeNetDepositDealRows,
                        writer);

                var calculator = new Mt5DailyPnlCashCalculator();
                Mt5DailyPnlCashReport report = calculator.Calculate(
                    resolvedReportDate,
                    dailySnapshot.Rows ?? new List<Mt5DailyReportRow>(),
                    usdRates,
                    bonusCollection.StartProtectedBonusesUsd,
                    bonusCollection.EndProtectedBonusesUsd,
                    netDepositCollection.NetDepositsUsd);

                report.GeneratedAt = DateTime.Now;
                report.RequestedReportDate = reportDate;
                report.ReportDate = resolvedReportDate;
                report.BonusHistoryFrom = bonusHistoryFrom;
                report.BonusCommentContains = request.BonusCommentContains;
                report.NetDepositExcludedCommentContains = excludedCommentContains;
                report.UsedLatestAvailableDailyReportDateFallback = usedDailyFallback;
                report.DailyRows = dailySnapshot.Rows != null
                    ? new List<Mt5DailyReportRow>(dailySnapshot.Rows)
                    : new List<Mt5DailyReportRow>();
	                report.DailyRowCount = report.DailyRows.Count;
	                report.ProtectedBonusDealCount = bonusCollection.DealCount;
	                report.ProtectedBonusDeals = request.IncludeBonusDealRows
	                    ? bonusCollection.Deals
	                    : new List<Mt5WdEquityZProtectedBonusDeal>();
	                report.NetDepositDealCount = netDepositCollection.DealCount;
	                report.NetDepositDeals = request.IncludeNetDepositDealRows
	                    ? netDepositCollection.Deals
	                    : new List<Mt5DailyPnlCashNetDepositDeal>();
	                report.MissingCurrencyRates = report.MissingCurrencyRates
	                    .Concat(bonusCollection.MissingCurrencyRates)
	                    .Concat(netDepositCollection.MissingCurrencyRates)
	                    .Distinct(StringComparer.OrdinalIgnoreCase)
	                    .OrderBy(code => code, StringComparer.OrdinalIgnoreCase)
	                    .ToList();
	                report.Assumptions = report.Assumptions
	                    .Concat(
	                        new[]
	                        {
	                            string.Format(
	                                CultureInfo.InvariantCulture,
	                                "Protected bonuses are reconstructed from ActionBalance deals whose comment contains '{0}'.",
	                                request.BonusCommentContains),
	                            string.Format(
	                                CultureInfo.InvariantCulture,
	                                "BonusHistoryFrom ({0:yyyy-MM-dd}) must be early enough to capture the full protected-bonus balance you want to subtract.",
	                                bonusHistoryFrom),
                            "Net deposits are sourced from MT5 deposit/withdrawal balance deals (DEAL_BALANCE) only; DEAL_CREDIT and DEAL_BONUS are excluded by action.",
                            string.Format(
                                CultureInfo.InvariantCulture,
                                "Net deposit comment exclusions: {0}.",
                                excludedCommentContains.Count > 0
                                    ? string.Join(", ", excludedCommentContains)
                                    : "(none)")
                        }
                        .Concat(
                            usedDailyFallback
                                ? new[]
                                {
                                    string.Format(
                                        CultureInfo.InvariantCulture,
                                        "Requested ReportDate {0:yyyy-MM-dd} had no MT5 daily rows; used latest available closed day {1:yyyy-MM-dd}.",
                                        reportDate,
                                        resolvedReportDate)
                                }
                                : Enumerable.Empty<string>()))
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .ToList();

	                writer(string.Format(
	                    CultureInfo.InvariantCulture,
	                    "Daily PnL Cash ready. Daily rows: {0}, protected bonus deals: {1}, net deposit deals: {2}, value: {3:N2}.",
	                    report.DailyRowCount,
	                    report.ProtectedBonusDealCount,
	                    report.NetDepositDealCount,
	                    report.DailyPnlCashUsd));

	                return report;
	            }
	            finally
	            {
	                Mt5MonitorCollector.Disconnect(manager);
	                SMTManagerAPIFactory.Shutdown();
	            }
	        }

	        public static Mt5DailyPnlCashReport Generate(
	            Mt5MonitorSettings settings,
	            Mt5DailyPnlCashRequest request)
	        {
	            return Generate(settings, request, null);
	        }

	        public static Mt5DailyPnlCashReport Generate(
	            Mt5MonitorSettings settings,
	            DateTime reportDate,
	            DateTime bonusHistoryFrom,
	            Action<string> statusWriter)
	        {
	            return Generate(
	                settings,
	                new Mt5DailyPnlCashRequest
	                {
	                    ReportDate = reportDate,
	                    BonusHistoryFrom = bonusHistoryFrom
	                },
	                statusWriter);
	        }

	        public static Mt5DailyPnlCashReport Generate(
	            Mt5MonitorSettings settings,
	            DateTime reportDate,
	            DateTime bonusHistoryFrom)
	        {
	            return Generate(settings, reportDate, bonusHistoryFrom, null);
	        }

	        private static List<string> NormalizeCommentFilters(IEnumerable<string> values)
	        {
	            return values == null
	                ? new List<string>()
	                : values
	                    .Where(value => !string.IsNullOrWhiteSpace(value))
	                    .Select(value => value.Trim())
	                    .Distinct(StringComparer.OrdinalIgnoreCase)
	                    .OrderBy(value => value, StringComparer.OrdinalIgnoreCase)
	                    .ToList();
	        }
	    }

	    public static class Mt5PositionHistoryGenerator
	    {
        public static Mt5PositionHistorySnapshot Generate(
            Mt5MonitorSettings settings,
            DateTime fromDate,
            DateTime toDate,
            Action<string> statusWriter)
        {
            if (settings == null)
                throw new ArgumentNullException("settings");

            DateTime normalizedFrom = fromDate.Date;
            DateTime normalizedTo = toDate.Date;
            if (normalizedTo < normalizedFrom)
                throw new InvalidOperationException("Positions history end date must be on or after the start date.");

            settings.Validate();

            Action<string> writer = statusWriter ?? (_ => { });
            writer(string.Format(
                CultureInfo.InvariantCulture,
                "Generating positions history for {0:yyyy-MM-dd} to {1:yyyy-MM-dd}.",
                normalizedFrom,
                normalizedTo));

            MTRetCode initializeResult = SMTManagerAPIFactory.Initialize(settings.SdkLibsPath);
            if (initializeResult != MTRetCode.MT_RET_OK)
                throw new InvalidOperationException("Initialize failed: " + initializeResult);

            CIMTManagerAPI manager = Mt5MonitorCollector.Connect(settings.Server, settings.Login, settings.Password, writer);
            if (manager == null)
            {
                SMTManagerAPIFactory.Shutdown();
                throw new InvalidOperationException("Failed to connect to MT5 Manager API.");
            }

            try
            {
                Dictionary<string, string> groupCurrencies = Mt5MonitorCollector.LoadGroupCurrencies(manager, settings.GroupMask);
                Dictionary<ulong, Mt5LoginContext> loginContexts = Mt5MonitorCollector.LoadLoginContexts(manager, settings.GroupMask, groupCurrencies);
                Mt5PositionHistorySnapshot snapshot = Mt5MonitorCollector.CollectPositionHistory(
                    manager,
                    groupCurrencies,
                    loginContexts,
                    settings.GroupMask,
                    normalizedFrom,
                    normalizedTo,
                    writer);

                writer(string.Format(
                    CultureInfo.InvariantCulture,
                    "Positions history ready with {0} rows.",
                    snapshot.Rows != null ? snapshot.Rows.Count : 0));

                return snapshot;
            }
            finally
            {
                Mt5MonitorCollector.Disconnect(manager);
                SMTManagerAPIFactory.Shutdown();
            }
        }

        public static Mt5PositionHistorySnapshot Generate(Mt5MonitorSettings settings, DateTime fromDate, DateTime toDate)
        {
            return Generate(settings, fromDate, toDate, null);
        }

        public static Mt5PositionHistoryJsonDocument GenerateJsonDocument(
            Mt5MonitorSettings settings,
            DateTime fromDate,
            DateTime toDate,
            Action<string> statusWriter)
        {
            return Mt5MonitorJsonExporter.CreatePositionHistoryDocument(
                Generate(settings, fromDate, toDate, statusWriter),
                settings);
        }

        public static Mt5PositionHistoryJsonDocument GenerateJsonDocument(
            Mt5MonitorSettings settings,
            DateTime fromDate,
            DateTime toDate)
        {
            return GenerateJsonDocument(settings, fromDate, toDate, null);
        }

        public static string GenerateJson(
            Mt5MonitorSettings settings,
            DateTime fromDate,
            DateTime toDate,
            Action<string> statusWriter,
            bool indented)
        {
            return Mt5MonitorJsonExporter.BuildPositionHistoryJson(
                Generate(settings, fromDate, toDate, statusWriter),
                settings,
                indented);
        }

        public static string GenerateJson(
            Mt5MonitorSettings settings,
            DateTime fromDate,
            DateTime toDate,
            Action<string> statusWriter)
        {
            return GenerateJson(settings, fromDate, toDate, statusWriter, true);
        }

        public static string GenerateJson(
            Mt5MonitorSettings settings,
            DateTime fromDate,
            DateTime toDate)
        {
            return GenerateJson(settings, fromDate, toDate, null, true);
        }
    }

    public static class Mt5TradingAccountsGenerator
    {
        public static Mt5TradingAccountsSnapshot Generate(
            Mt5MonitorSettings settings,
            Action<string> statusWriter)
        {
            if (settings == null)
                throw new ArgumentNullException("settings");

            settings.Validate();

            Action<string> writer = statusWriter ?? (_ => { });
            writer("Generating current trading accounts snapshot.");

            MTRetCode initializeResult = SMTManagerAPIFactory.Initialize(settings.SdkLibsPath);
            if (initializeResult != MTRetCode.MT_RET_OK)
                throw new InvalidOperationException("Initialize failed: " + initializeResult);

            CIMTManagerAPI manager = Mt5MonitorCollector.Connect(settings.Server, settings.Login, settings.Password, writer);
            if (manager == null)
            {
                SMTManagerAPIFactory.Shutdown();
                throw new InvalidOperationException("Failed to connect to MT5 Manager API.");
            }

            try
            {
                Dictionary<string, string> groupCurrencies = Mt5MonitorCollector.LoadGroupCurrencies(manager, settings.GroupMask);
                Dictionary<ulong, Mt5LoginContext> loginContexts = Mt5MonitorCollector.LoadLoginContexts(manager, settings.GroupMask, groupCurrencies);
                Mt5TradingAccountsSnapshot snapshot = Mt5MonitorCollector.CollectTradingAccounts(
                    manager,
                    groupCurrencies,
                    loginContexts,
                    settings.GroupMask,
                    writer);

                writer(string.Format(
                    CultureInfo.InvariantCulture,
                    "Trading accounts snapshot ready with {0} rows.",
                    snapshot.Rows != null ? snapshot.Rows.Count : 0));

                return snapshot;
            }
            finally
            {
                Mt5MonitorCollector.Disconnect(manager);
                SMTManagerAPIFactory.Shutdown();
            }
        }

        public static Mt5TradingAccountsSnapshot Generate(Mt5MonitorSettings settings)
        {
            return Generate(settings, null);
        }

        public static Mt5TradingAccountsJsonDocument GenerateJsonDocument(
            Mt5MonitorSettings settings,
            Action<string> statusWriter)
        {
            return Mt5MonitorJsonExporter.CreateTradingAccountsDocument(
                Generate(settings, statusWriter),
                settings);
        }

        public static Mt5TradingAccountsJsonDocument GenerateJsonDocument(Mt5MonitorSettings settings)
        {
            return GenerateJsonDocument(settings, null);
        }

        public static string GenerateJson(
            Mt5MonitorSettings settings,
            Action<string> statusWriter,
            bool indented)
        {
            return Mt5MonitorJsonExporter.BuildTradingAccountsJson(
                Generate(settings, statusWriter),
                settings,
                indented);
        }

        public static string GenerateJson(
            Mt5MonitorSettings settings,
            Action<string> statusWriter)
        {
            return GenerateJson(settings, statusWriter, true);
        }

        public static string GenerateJson(Mt5MonitorSettings settings)
        {
            return GenerateJson(settings, null, true);
        }

        public static string GenerateCsv(
            Mt5MonitorSettings settings,
            Action<string> statusWriter)
        {
            return Mt5MonitorCsvExporter.BuildTradingAccountsCsv(
                Generate(settings, statusWriter),
                settings);
        }

        public static string GenerateCsv(Mt5MonitorSettings settings)
        {
            return GenerateCsv(settings, null);
        }
    }

    public static class Mt5DepositWithdrawalGenerator
    {
        public static Mt5DepositWithdrawalSnapshot Generate(
            Mt5MonitorSettings settings,
            DateTime fromDate,
            DateTime toDate,
            Action<string> statusWriter)
        {
            if (settings == null)
                throw new ArgumentNullException("settings");

            DateTime normalizedFrom = fromDate.Date;
            DateTime normalizedTo = toDate.Date;
            if (normalizedTo < normalizedFrom)
                throw new InvalidOperationException("Deposit and withdrawal report end date must be on or after the start date.");

            settings.Validate();

            Action<string> writer = statusWriter ?? (_ => { });
            writer(string.Format(
                CultureInfo.InvariantCulture,
                "Generating deposit and withdrawal report for {0:yyyy-MM-dd} to {1:yyyy-MM-dd}.",
                normalizedFrom,
                normalizedTo));

            MTRetCode initializeResult = SMTManagerAPIFactory.Initialize(settings.SdkLibsPath);
            if (initializeResult != MTRetCode.MT_RET_OK)
                throw new InvalidOperationException("Initialize failed: " + initializeResult);

            CIMTManagerAPI manager = Mt5MonitorCollector.Connect(settings.Server, settings.Login, settings.Password, writer);
            if (manager == null)
            {
                SMTManagerAPIFactory.Shutdown();
                throw new InvalidOperationException("Failed to connect to MT5 Manager API.");
            }

            try
            {
                Dictionary<string, string> groupCurrencies = Mt5MonitorCollector.LoadGroupCurrencies(manager, settings.GroupMask);
                Dictionary<ulong, Mt5LoginContext> loginContexts = Mt5MonitorCollector.LoadLoginContexts(manager, settings.GroupMask, groupCurrencies);
                Mt5DepositWithdrawalSnapshot snapshot = Mt5MonitorCollector.CollectDepositWithdrawals(
                    manager,
                    groupCurrencies,
                    loginContexts,
                    settings.GroupMask,
                    normalizedFrom,
                    normalizedTo,
                    writer);

                writer(string.Format(
                    CultureInfo.InvariantCulture,
                    "Deposit and withdrawal report ready with {0} rows.",
                    snapshot.Rows != null ? snapshot.Rows.Count : 0));

                return snapshot;
            }
            finally
            {
                Mt5MonitorCollector.Disconnect(manager);
                SMTManagerAPIFactory.Shutdown();
            }
        }

        public static Mt5DepositWithdrawalSnapshot Generate(
            Mt5MonitorSettings settings,
            DateTime reportDate,
            Action<string> statusWriter)
        {
            return Generate(settings, reportDate, reportDate, statusWriter);
        }

        public static Mt5DepositWithdrawalSnapshot Generate(Mt5MonitorSettings settings, DateTime reportDate)
        {
            return Generate(settings, reportDate, reportDate, null);
        }

        public static Mt5DepositWithdrawalSnapshot Generate(
            Mt5MonitorSettings settings,
            DateTime fromDate,
            DateTime toDate)
        {
            return Generate(settings, fromDate, toDate, null);
        }

        public static Mt5DepositWithdrawalJsonDocument GenerateJsonDocument(
            Mt5MonitorSettings settings,
            DateTime fromDate,
            DateTime toDate,
            Action<string> statusWriter)
        {
            return Mt5MonitorJsonExporter.CreateDepositWithdrawalDocument(
                Generate(settings, fromDate, toDate, statusWriter),
                settings);
        }

        public static Mt5DepositWithdrawalJsonDocument GenerateJsonDocument(
            Mt5MonitorSettings settings,
            DateTime fromDate,
            DateTime toDate)
        {
            return GenerateJsonDocument(settings, fromDate, toDate, null);
        }

        public static Mt5DepositWithdrawalJsonDocument GenerateJsonDocument(
            Mt5MonitorSettings settings,
            DateTime reportDate)
        {
            return GenerateJsonDocument(settings, reportDate, reportDate, null);
        }

        public static string GenerateJson(
            Mt5MonitorSettings settings,
            DateTime fromDate,
            DateTime toDate,
            Action<string> statusWriter,
            bool indented)
        {
            return Mt5MonitorJsonExporter.BuildDepositWithdrawalJson(
                Generate(settings, fromDate, toDate, statusWriter),
                settings,
                indented);
        }

        public static string GenerateJson(
            Mt5MonitorSettings settings,
            DateTime fromDate,
            DateTime toDate,
            Action<string> statusWriter)
        {
            return GenerateJson(settings, fromDate, toDate, statusWriter, true);
        }

        public static string GenerateJson(
            Mt5MonitorSettings settings,
            DateTime fromDate,
            DateTime toDate)
        {
            return GenerateJson(settings, fromDate, toDate, null, true);
        }

        public static string GenerateJson(
            Mt5MonitorSettings settings,
            DateTime reportDate)
        {
            return GenerateJson(settings, reportDate, reportDate, null, true);
        }

        public static string GenerateCsv(
            Mt5MonitorSettings settings,
            DateTime fromDate,
            DateTime toDate,
            Action<string> statusWriter)
        {
            return Mt5MonitorCsvExporter.BuildDepositWithdrawalCsv(
                Generate(settings, fromDate, toDate, statusWriter),
                settings);
        }

        public static string GenerateCsv(
            Mt5MonitorSettings settings,
            DateTime fromDate,
            DateTime toDate)
        {
            return GenerateCsv(settings, fromDate, toDate, null);
        }

        public static string GenerateCsv(
            Mt5MonitorSettings settings,
            DateTime reportDate)
        {
            return GenerateCsv(settings, reportDate, reportDate, null);
        }
    }

    public sealed class Mt5MonitorService : IMt5MonitorFeed
    {
        private readonly object _sync = new object();
        private CancellationTokenSource _cancellation;
        private Task _worker;
        private double? _previousFloatingPnl;
        private bool _isRunning;

        public event EventHandler<Mt5MonitorSnapshotEventArgs> SnapshotReceived;
        public event EventHandler<Mt5MonitorStatusChangedEventArgs> StatusChanged;

        public bool IsRunning
        {
            get
            {
                lock (_sync)
                {
                    return _isRunning;
                }
            }
        }

        public void Start(Mt5MonitorSettings settings)
        {
            if (settings == null)
                throw new ArgumentNullException("settings");

            Mt5MonitorSettings effective = settings.Clone();
            effective.Validate();

            lock (_sync)
            {
                if (_isRunning)
                    throw new InvalidOperationException("Monitor service is already running.");

                _isRunning = true;
                _previousFloatingPnl = null;
                _cancellation = new CancellationTokenSource();
                CancellationTokenSource cancellation = _cancellation;
                _worker = Task.Factory.StartNew(
                    () => RunLoop(effective, cancellation.Token),
                    cancellation.Token,
                    TaskCreationOptions.LongRunning,
                    TaskScheduler.Default);
            }
        }

        public void Stop()
        {
            CancellationTokenSource cancellation = null;
            Task worker = null;

            lock (_sync)
            {
                if (!_isRunning && _worker == null)
                    return;

                cancellation = _cancellation;
                worker = _worker;
                _isRunning = false;
                _cancellation = null;
                _worker = null;
            }

            if (cancellation != null)
                cancellation.Cancel();

            if (worker != null)
            {
                try
                {
                    worker.Wait(TimeSpan.FromSeconds(5));
                }
                catch (AggregateException)
                {
                }
                catch (OperationCanceledException)
                {
                }
            }

            if (cancellation != null)
                cancellation.Dispose();
        }

        public void Dispose()
        {
            Stop();
        }

        private void RunLoop(Mt5MonitorSettings settings, CancellationToken cancellationToken)
        {
            CIMTManagerAPI manager = null;
            Dictionary<string, string> groupCurrencies = null;
            Dictionary<ulong, Mt5LoginContext> loginContexts = null;
            Dictionary<string, Mt5MonitorCollector.CurrencyRate> currencyRates = null;
            int rateRefreshCycle = 0;

            MTRetCode initializeResult = SMTManagerAPIFactory.Initialize(settings.SdkLibsPath);
            if (initializeResult != MTRetCode.MT_RET_OK)
            {
                PublishStatus("Initialize failed: " + initializeResult, false);
                ResetRunningState();
                return;
            }

            PublishStatus("Monitor starting...", false);

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    if (manager == null)
                    {
                        manager = ConnectWithRetry(settings, cancellationToken);
                        if (manager == null)
                            break;

                        groupCurrencies = Mt5MonitorCollector.LoadGroupCurrencies(manager, settings.GroupMask);
                        loginContexts = Mt5MonitorCollector.LoadLoginContexts(manager, settings.GroupMask, groupCurrencies);
                        currencyRates = Mt5MonitorCollector.BuildCurrencyRates(manager);
                        rateRefreshCycle = 0;
                        PublishStatus(
                            string.Format(
                                "Connected. Loaded {0} groups, {1} users and {2} FX rates.",
                                groupCurrencies.Count,
                                loginContexts.Count,
                                currencyRates.Count),
                            true);
                    }

                    try
                    {
                        if (++rateRefreshCycle >= 60)
                        {
                            currencyRates = Mt5MonitorCollector.BuildCurrencyRates(manager);
                            loginContexts = Mt5MonitorCollector.LoadLoginContexts(manager, settings.GroupMask, groupCurrencies);
                            rateRefreshCycle = 0;
                            PublishStatus("FX rates and login contexts refreshed.", true);
                        }

                        Mt5MonitorSnapshot snapshot = Mt5MonitorCollector.CollectSnapshot(
                            manager,
                            groupCurrencies,
                            loginContexts,
                            currencyRates,
                            settings.GroupMask,
                            _previousFloatingPnl,
                            message => PublishStatus(message, true));

                        _previousFloatingPnl = snapshot.FloatingPnlUsd;
                        PublishSnapshot(snapshot);
                    }
                    catch (Exception ex)
                    {
                        PublishStatus("Monitor error: " + ex.Message + " Reconnecting...", false);
                        Mt5MonitorCollector.Disconnect(manager);
                        manager = null;
                        groupCurrencies = null;
                        loginContexts = null;
                        currencyRates = null;
                        _previousFloatingPnl = null;
                        WaitForInterval(TimeSpan.FromSeconds(2), cancellationToken);
                        continue;
                    }

                    WaitForInterval(TimeSpan.FromSeconds(settings.IntervalSeconds), cancellationToken);
                }
            }
            finally
            {
                Mt5MonitorCollector.Disconnect(manager);
                SMTManagerAPIFactory.Shutdown();
                PublishStatus("Monitor stopped.", false);
                ResetRunningState();
            }
        }

        private CIMTManagerAPI ConnectWithRetry(Mt5MonitorSettings settings, CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                CIMTManagerAPI manager = Mt5MonitorCollector.Connect(
                    settings.Server,
                    settings.Login,
                    settings.Password,
                    message => PublishStatus(message, false));

                if (manager != null)
                    return manager;

                PublishStatus("Retrying connection in 5 seconds...", false);
                WaitForInterval(TimeSpan.FromSeconds(5), cancellationToken);
            }

            return null;
        }

        private void WaitForInterval(TimeSpan delay, CancellationToken cancellationToken)
        {
            try
            {
                Task.Delay(delay, cancellationToken).Wait(cancellationToken);
            }
            catch (AggregateException)
            {
            }
            catch (OperationCanceledException)
            {
            }
        }

        private void PublishSnapshot(Mt5MonitorSnapshot snapshot)
        {
            EventHandler<Mt5MonitorSnapshotEventArgs> handler = SnapshotReceived;
            if (handler != null)
                handler(this, new Mt5MonitorSnapshotEventArgs(snapshot));
        }

        private void PublishStatus(string message, bool connected)
        {
            EventHandler<Mt5MonitorStatusChangedEventArgs> handler = StatusChanged;
            if (handler != null)
                handler(this, new Mt5MonitorStatusChangedEventArgs(message, connected));
        }

        private void ResetRunningState()
        {
            lock (_sync)
            {
                _isRunning = false;
                _cancellation = null;
                _worker = null;
            }
        }
    }

    internal sealed class Mt5LoginContext
    {
        public ulong Login { get; set; }
        public string Name { get; set; }
        public string Group { get; set; }
        public string Currency { get; set; }
    }

    internal static class Mt5MonitorCollector
    {
        private static readonly DateTime UnixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        private const uint EntryIn = 0;
        private const uint EntryOut = 1;
        private const uint EntryInOut = 2;
        private const uint ActionBuy = 0;
        private const uint ActionSell = 1;
        private const uint ActionBalance = 2;
        private const uint ActionBuyCanceled = 13;
        private const uint ActionSellCanceled = 14;
        private const uint PositionBuy = 0;
        private const uint PositionSell = 1;
        private const double VolumeScale = 10000.0;
        private const int PositionHistoryStartYear = 2000;

        private struct CurrencyEntry
        {
            public string Symbol;
            public string Currency;
            public bool UsdBase;
        }

        internal struct CurrencyRate
        {
            public string Symbol;
            public double Bid;
            public double Ask;
            public bool UsdBase;
            public double PositiveToUsd;
            public double NegativeToUsd;
        }

        private sealed class SymbolAggregate
        {
            public string Symbol;
            public int Digits;
            public int ClientPositions;
            public double ClientBuyVolume;
            public double ClientBuyPriceAmount;
            public double ClientSellVolume;
            public double ClientSellPriceAmount;
            public double ClientProfitUsd;
        }

        private sealed class UsdConversionTrace
        {
            public string Currency;
            public string FxSymbol;
            public double FxBid;
            public double FxAsk;
            public bool FxUsdBase;
            public double UsedRate;
            public bool UsedFallback;
            public double UsdAmount;
        }

        private sealed class PositionHistoryAccumulator
        {
            public ulong Login;
            public string Name;
            public ulong Ticket;
            public string Type;
            public double OpenVolume;
            public double CloseVolume;
            public string Symbol;
            public double OpenPrice;
            public double StopLoss;
            public double TakeProfit;
            public DateTime OpenTime;
            public DateTime CloseTime;
            public double ClosePrice;
            public string Reason;
            public double Commission;
            public double Fee;
            public double Swap;
            public double Profit;
            public string Currency;
            public string Comment;
            public int Digits;
            public int CurrencyDigits;
        }

        private sealed class PositionHistoryDealRecord
        {
            public ulong Login;
            public ulong PositionId;
            public ulong Deal;
            public DateTime Time;
            public uint Action;
            public uint Entry;
            public string Symbol;
            public double Volume;
            public double Price;
            public double StopLoss;
            public double TakeProfit;
            public double Commission;
            public double Fee;
            public double Swap;
            public double Profit;
            public uint Reason;
            public string Comment;
            public int Digits;
            public int CurrencyDigits;
        }

	        internal sealed class WdEquityZProtectedBonusCollection
	        {
	            public WdEquityZProtectedBonusCollection()
	            {
	                Deals = new List<Mt5WdEquityZProtectedBonusDeal>();
                MissingCurrencyRates = new List<string>();
            }

            public double StartProtectedBonusesUsd { get; set; }
            public double EndProtectedBonusesUsd { get; set; }
            public int DealCount { get; set; }
	            public IList<Mt5WdEquityZProtectedBonusDeal> Deals { get; set; }
	            public IList<string> MissingCurrencyRates { get; set; }
	        }

	        internal sealed class DailyPnlCashNetDepositCollection
	        {
	            public DailyPnlCashNetDepositCollection()
	            {
	                Deals = new List<Mt5DailyPnlCashNetDepositDeal>();
	                MissingCurrencyRates = new List<string>();
	            }

	            public double NetDepositsUsd { get; set; }
	            public int DealCount { get; set; }
	            public IList<Mt5DailyPnlCashNetDepositDeal> Deals { get; set; }
	            public IList<string> MissingCurrencyRates { get; set; }
	        }

	        private static readonly CurrencyEntry[] CurrencyTable =
	        {
            new CurrencyEntry { Symbol = "EURUSD", Currency = "EUR", UsdBase = false },
            new CurrencyEntry { Symbol = "GBPUSD", Currency = "GBP", UsdBase = false },
            new CurrencyEntry { Symbol = "AUDUSD", Currency = "AUD", UsdBase = false },
            new CurrencyEntry { Symbol = "NZDUSD", Currency = "NZD", UsdBase = false },
            new CurrencyEntry { Symbol = "USDCHF", Currency = "CHF", UsdBase = true },
            new CurrencyEntry { Symbol = "USDCAD", Currency = "CAD", UsdBase = true },
            new CurrencyEntry { Symbol = "USDJPY", Currency = "JPY", UsdBase = true },
            new CurrencyEntry { Symbol = "USDNOK", Currency = "NOK", UsdBase = true },
            new CurrencyEntry { Symbol = "USDSEK", Currency = "SEK", UsdBase = true },
            new CurrencyEntry { Symbol = "USDZAR", Currency = "ZAR", UsdBase = true },
            new CurrencyEntry { Symbol = "USDKES", Currency = "KES", UsdBase = true },
            new CurrencyEntry { Symbol = "USDNGN", Currency = "NGN", UsdBase = true },
            new CurrencyEntry { Symbol = "USDMXN", Currency = "MXN", UsdBase = true },
            new CurrencyEntry { Symbol = "USDAED", Currency = "AED", UsdBase = true },
            new CurrencyEntry { Symbol = "USDCLP", Currency = "CLP", UsdBase = true },
            new CurrencyEntry { Symbol = "USDHUF", Currency = "HUF", UsdBase = true },
            new CurrencyEntry { Symbol = "USDINR", Currency = "INR", UsdBase = true }
        };

        public static double ToUsd(double native, double rate)
        {
            if (rate > 1.5 && rate != 0)
                return native / rate;
            return native;
        }

        public static Dictionary<string, CurrencyRate> BuildCurrencyRates(CIMTManagerAPI manager)
        {
            var rates = new Dictionary<string, CurrencyRate>(StringComparer.OrdinalIgnoreCase);
            rates["USD"] = new CurrencyRate
            {
                Symbol = "USD",
                Bid = 1.0,
                Ask = 1.0,
                UsdBase = false,
                PositiveToUsd = 1.0,
                NegativeToUsd = 1.0
            };

            foreach (CurrencyEntry entry in CurrencyTable)
            {
                MTTickShort tick;
                if (manager.TickLast(entry.Symbol, out tick) == MTRetCode.MT_RET_OK && tick.bid > 0 && tick.ask > 0)
                {
                    rates[entry.Currency] = new CurrencyRate
                    {
                        Symbol = entry.Symbol,
                        Bid = tick.bid,
                        Ask = tick.ask,
                        UsdBase = entry.UsdBase,
                        PositiveToUsd = entry.UsdBase ? 1.0 / tick.ask : tick.bid,
                        NegativeToUsd = entry.UsdBase ? 1.0 / tick.bid : tick.ask
                    };
                }
            }

            return rates;
        }

        public static Dictionary<string, string> LoadGroupCurrencies(CIMTManagerAPI manager, string groupMask)
        {
            var currencies = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            var groups = manager.GroupCreateArray();

            try
            {
                if (manager.GroupRequestArray(groupMask, groups) == MTRetCode.MT_RET_OK)
                {
                    for (uint i = 0; i < groups.Total(); i++)
                    {
                        var group = groups.Next(i);
                        string name = group.Group() ?? string.Empty;
                        if (!string.IsNullOrEmpty(name))
                            currencies[name] = group.Currency() ?? "USD";
                    }
                }
            }
            finally
            {
                groups.Release();
            }

            return currencies;
        }

        public static Dictionary<ulong, Mt5LoginContext> LoadLoginContexts(
            CIMTManagerAPI manager,
            string groupMask,
            Dictionary<string, string> groupCurrencies)
        {
            var loginContexts = new Dictionary<ulong, Mt5LoginContext>();
            var users = manager.UserCreateArray();

            try
            {
                if (manager.UserRequestArray(groupMask, users) == MTRetCode.MT_RET_OK)
                {
                    for (uint i = 0; i < users.Total(); i++)
                    {
                        var user = users.Next(i);
                        string groupName = user.Group() ?? string.Empty;
                        string currency;
                        if (!groupCurrencies.TryGetValue(groupName, out currency))
                            currency = "USD";

                        loginContexts[user.Login()] = new Mt5LoginContext
                        {
                            Login = user.Login(),
                            Name = user.Name() ?? string.Empty,
                            Group = groupName,
                            Currency = currency
                        };
                    }
                }
            }
            finally
            {
                users.Release();
            }

            return loginContexts;
        }

        public static CIMTManagerAPI Connect(string server, ulong login, string password, Action<string> statusWriter)
        {
            MTRetCode createResult = MTRetCode.MT_RET_OK_NONE;
            CIMTManagerAPI manager = SMTManagerAPIFactory.CreateManager(
                SMTManagerAPIFactory.ManagerAPIVersion,
                out createResult);

            if (manager == null || createResult != MTRetCode.MT_RET_OK)
            {
                statusWriter("CreateManager failed: " + createResult);
                return null;
            }

            MTRetCode connectResult = manager.Connect(
                server,
                login,
                password,
                null,
                CIMTManagerAPI.EnPumpModes.PUMP_MODE_POSITIONS,
                30000);

            if (connectResult != MTRetCode.MT_RET_OK)
            {
                statusWriter("Connect failed: " + connectResult + " (" + (uint)connectResult + ")");
                try { manager.Disconnect(); } catch { }
                manager.Dispose();
                return null;
            }

            statusWriter("Waiting for position pump...");
            DateTime deadline = DateTime.Now.AddSeconds(30);
            while (DateTime.Now < deadline)
            {
                var testArray = manager.PositionCreateArray();
                bool ready = false;
                try
                {
                    ready = manager.PositionGetByGroup("*", testArray) == MTRetCode.MT_RET_OK && testArray.Total() > 0;
                }
                finally
                {
                    testArray.Dispose();
                }

                if (ready)
                    break;

                Thread.Sleep(500);
            }

            statusWriter("Connected.");
            return manager;
        }

        public static void Disconnect(CIMTManagerAPI manager)
        {
            if (manager == null)
                return;

            try { manager.Disconnect(); } catch { }
            try { manager.Dispose(); } catch { }
        }

        public static Mt5MonitorSnapshot CollectSnapshot(
            CIMTManagerAPI manager,
            Dictionary<string, string> groupCurrencies,
            Dictionary<ulong, Mt5LoginContext> loginContexts,
            Dictionary<string, CurrencyRate> currencyRates,
            string groupMask,
            double? previousFloatingPnl,
            Action<string> statusWriter)
        {
            double floatingPnl = 0;
            int positionCount = 0;
            var symbolAggregates = new Dictionary<string, SymbolAggregate>(StringComparer.OrdinalIgnoreCase);
            var positionAuditRows = new List<Mt5PositionAuditRow>();
            var missingCurrencies = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            int fallbackConversionCount = 0;

            foreach (KeyValuePair<string, string> group in groupCurrencies)
            {
                string groupCurrency = string.IsNullOrWhiteSpace(group.Value) ? "USD" : group.Value;
                var positions = manager.PositionCreateArray();

                try
                {
                    if (manager.PositionGetByGroup(group.Key, positions) != MTRetCode.MT_RET_OK)
                        throw new InvalidOperationException("PositionGetByGroup failed for group " + group.Key + ".");

                    positionCount += (int)positions.Total();
                    for (uint i = 0; i < positions.Total(); i++)
                    {
                        var position = positions.Next(i);
                        Mt5LoginContext loginContext;
                        if (loginContexts == null || !loginContexts.TryGetValue(position.Login(), out loginContext))
                        {
                            loginContext = new Mt5LoginContext
                            {
                                Login = position.Login(),
                                Group = group.Key,
                                Currency = groupCurrency
                            };
                        }

                        string positionCurrency = string.IsNullOrWhiteSpace(loginContext.Currency)
                            ? groupCurrency
                            : loginContext.Currency;

                        UsdConversionTrace profitTrace = ConvertToUsdTrace(
                            position.Profit(),
                            positionCurrency,
                            position.RateProfit(),
                            currencyRates,
                            missingCurrencies,
                            ref fallbackConversionCount);

                        UsdConversionTrace storageTrace = ConvertToUsdTrace(
                            position.Storage(),
                            positionCurrency,
                            position.RateProfit(),
                            currencyRates,
                            missingCurrencies,
                            ref fallbackConversionCount);

                        double floatingUsd = profitTrace.UsdAmount + storageTrace.UsdAmount;
                        floatingPnl += floatingUsd;

                        string symbol = position.Symbol() ?? string.Empty;
                        if (string.IsNullOrWhiteSpace(symbol))
                            symbol = "(unknown)";

                        SymbolAggregate aggregate;
                        if (!symbolAggregates.TryGetValue(symbol, out aggregate))
                        {
                            aggregate = new SymbolAggregate { Symbol = symbol };
                            symbolAggregates[symbol] = aggregate;
                        }

                        aggregate.ClientPositions++;
                        aggregate.Digits = Math.Max(aggregate.Digits, (int)position.Digits());
                        aggregate.ClientProfitUsd += floatingUsd;

                        double lots = ToDisplayLots(position.Volume());
                        if (position.Action() == PositionBuy)
                        {
                            aggregate.ClientBuyVolume += lots;
                            aggregate.ClientBuyPriceAmount += lots * position.PriceOpen();
                        }
                        else if (position.Action() == PositionSell)
                        {
                            aggregate.ClientSellVolume += lots;
                            aggregate.ClientSellPriceAmount += lots * position.PriceOpen();
                        }

                        positionAuditRows.Add(new Mt5PositionAuditRow
                        {
                            Position = position.Position(),
                            Login = position.Login(),
                            Group = string.IsNullOrWhiteSpace(loginContext.Group) ? group.Key : loginContext.Group,
                            GroupCurrency = groupCurrency,
                            DepositCurrency = positionCurrency,
                            Symbol = symbol,
                            Side = FormatPositionSide(position.Action()),
                            RawVolume = position.Volume(),
                            VolumeLots = lots,
                            Digits = (int)position.Digits(),
                            CurrencyDigits = (int)position.DigitsCurrency(),
                            ContractSize = position.ContractSize(),
                            PriceOpen = position.PriceOpen(),
                            PriceCurrent = position.PriceCurrent(),
                            RateProfit = position.RateProfit(),
                            ProfitNative = position.Profit(),
                            StorageNative = position.Storage(),
                            NativeTotal = position.Profit() + position.Storage(),
                            ProfitFxSymbol = profitTrace.FxSymbol,
                            ProfitFxBid = profitTrace.FxBid,
                            ProfitFxAsk = profitTrace.FxAsk,
                            ProfitFxUsdBase = profitTrace.FxUsdBase,
                            ProfitToUsdRate = profitTrace.UsedRate,
                            ProfitUsedFallback = profitTrace.UsedFallback,
                            ProfitUsd = profitTrace.UsdAmount,
                            StorageFxSymbol = storageTrace.FxSymbol,
                            StorageFxBid = storageTrace.FxBid,
                            StorageFxAsk = storageTrace.FxAsk,
                            StorageFxUsdBase = storageTrace.FxUsdBase,
                            StorageToUsdRate = storageTrace.UsedRate,
                            StorageUsedFallback = storageTrace.UsedFallback,
                            StorageUsd = storageTrace.UsdAmount,
                            FloatingUsd = floatingUsd
                        });
                    }
                }
                finally
                {
                    positions.Dispose();
                }
            }

            DateTime dayStart = DateTime.UtcNow.Date;
            DateTime nowUtc = DateTime.UtcNow;
            double closedPnl = 0;
            double netDeposits = 0;
            var traders = new HashSet<ulong>();
            var deals = manager.DealCreateArray();

            try
            {
                MTRetCode dealResult = manager.DealRequestByGroup(
                    groupMask,
                    SMTTime.FromDateTime(dayStart),
                    SMTTime.FromDateTime(nowUtc),
                    deals);

                if (dealResult == MTRetCode.MT_RET_OK)
                {
                    for (uint i = 0; i < deals.Total(); i++)
                    {
                        var deal = deals.Next(i);
                        uint action = deal.Action();
                        Mt5LoginContext loginContext;
                        string loginCurrency;
                        if (loginContexts == null || !loginContexts.TryGetValue(deal.Login(), out loginContext))
                            loginCurrency = "USD";
                        else
                            loginCurrency = string.IsNullOrWhiteSpace(loginContext.Currency) ? "USD" : loginContext.Currency;

                        if (action == ActionBuy || action == ActionSell)
                        {
                            traders.Add(deal.Login());
                            if (deal.Entry() != EntryIn)
                            {
                                closedPnl += ConvertComponentsToUsd(
                                    loginCurrency,
                                    deal.RateProfit(),
                                    currencyRates,
                                    missingCurrencies,
                                    ref fallbackConversionCount,
                                    deal.Profit(),
                                    deal.Storage(),
                                    deal.Commission());
                            }
                        }
                        else if (action == ActionBalance)
                        {
                            string comment = (deal.Comment() ?? string.Empty).ToLowerInvariant();
                            if (!comment.Contains("bonus") && !comment.Contains("internal") && !comment.Contains("transfer"))
                            {
                                netDeposits += ConvertComponentsToUsd(
                                    loginCurrency,
                                    deal.RateProfit(),
                                    currencyRates,
                                    missingCurrencies,
                                    ref fallbackConversionCount,
                                    deal.Profit());
                            }
                        }
                    }
                }
                else
                {
                    statusWriter("DealRequestByGroup failed: " + dealResult);
                }
            }
            finally
            {
                deals.Dispose();
            }

            List<Mt5SymbolSummaryRow> rows = BuildSymbolRows(symbolAggregates);
            Mt5SymbolSummaryTotals totals = BuildSymbolTotals(rows);

            return new Mt5MonitorSnapshot
            {
                LocalTimestamp = DateTime.Now,
                UtcTimestamp = nowUtc,
                FloatingPnlUsd = floatingPnl,
                FloatingPnlDeltaUsd = previousFloatingPnl.HasValue ? (double?)(floatingPnl - previousFloatingPnl.Value) : null,
                ClosedPnlUsd = closedPnl,
                NetDepositsUsd = netDeposits,
                PositionCount = positionCount,
                TraderCount = traders.Count,
                SymbolSummaryRows = rows,
                SymbolSummaryTotals = totals,
                PositionAuditRows = positionAuditRows,
                MissingCurrencyRates = missingCurrencies.OrderBy(code => code, StringComparer.OrdinalIgnoreCase).ToList(),
                FallbackConversionCount = fallbackConversionCount,
                ConversionSummary = BuildConversionSummary(missingCurrencies, fallbackConversionCount)
            };
        }

        public static Mt5DailyReportSnapshot CollectDailyReport(
            CIMTManagerAPI manager,
            Dictionary<string, string> groupCurrencies,
            string groupMask,
            DateTime fromDate,
            DateTime toDate,
            Action<string> statusWriter)
        {
            if (manager == null)
                throw new ArgumentNullException("manager");
            if (string.IsNullOrWhiteSpace(groupMask))
                throw new InvalidOperationException("Group mask is required.");

            DateTime normalizedFrom = fromDate.Date;
            DateTime normalizedTo = toDate.Date;
            if (normalizedTo < normalizedFrom)
                throw new InvalidOperationException("Daily report end date must be on or after the start date.");

            DateTime rangeStartUtc = CreateUtcBoundary(normalizedFrom, false);
            DateTime rangeEndUtc = CreateUtcBoundary(normalizedTo, true);
            Action<string> writer = statusWriter ?? (_ => { });
            writer(string.Format(
                CultureInfo.InvariantCulture,
                "Requesting MT5 daily rows for {0:yyyy-MM-dd} to {1:yyyy-MM-dd}.",
                normalizedFrom,
                normalizedTo));

            var dailies = manager.DailyCreateArray();
            try
            {
                MTRetCode result = manager.DailyRequestByGroup(
                    groupMask,
                    ToUnixSeconds(rangeStartUtc),
                    ToUnixSeconds(rangeEndUtc),
                    dailies);

                if (result != MTRetCode.MT_RET_OK)
                    throw new InvalidOperationException("DailyRequestByGroup failed: " + result + " (" + (uint)result + ")");

                var rows = new List<Mt5DailyReportRow>((int)dailies.Total());
                for (uint i = 0; i < dailies.Total(); i++)
                {
                    CIMTDaily daily = dailies.Next(i);
                    if (daily == null)
                        continue;

                    int currencyDigits = NormalizeCurrencyDigits((int)daily.CurrencyDigits());
                    string groupName = daily.Group() ?? string.Empty;
                    string currency = ResolveDailyCurrency(daily, groupCurrencies);

                    double closedPnl = MoneyAdd(daily.DailyProfit(), daily.DailyStorage(), currencyDigits);
                    closedPnl = MoneyAdd(closedPnl, daily.DailyCommInstant(), currencyDigits);

                    double floatingPnl = MoneyAdd(daily.Profit(), daily.ProfitStorage(), currencyDigits);

                    rows.Add(new Mt5DailyReportRow
                    {
                        Timestamp = SMTTime.ToDateTime(daily.Datetime()),
                        Login = daily.Login(),
                        Name = daily.Name() ?? string.Empty,
                        Group = groupName,
                        Currency = currency,
                        CurrencyDigits = currencyDigits,
                        PrevBalance = daily.BalancePrevDay(),
                        Deposit = daily.DailyBalance(),
                        ClosedPnl = closedPnl,
                        EquityPrevDay = daily.EquityPrevDay(),
                        Balance = daily.Balance(),
                        Credit = daily.Credit(),
                        DailyCredit = daily.DailyCredit(),
                        DailyBonus = daily.DailyBonus(),
                        FloatingPnl = floatingPnl,
                        Equity = daily.ProfitEquity(),
                        Margin = daily.Margin(),
                        FreeMargin = daily.MarginFree()
                    });
                }

                rows.Sort(CompareDailyRows);

                return new Mt5DailyReportSnapshot
                {
                    GeneratedAt = DateTime.Now,
                    RangeFrom = normalizedFrom,
                    RangeTo = normalizedTo,
                    Rows = rows
                };
            }
            finally
            {
                dailies.Release();
            }
        }

        public static Mt5TradingAccountsSnapshot CollectTradingAccounts(
            CIMTManagerAPI manager,
            Dictionary<string, string> groupCurrencies,
            Dictionary<ulong, Mt5LoginContext> loginContexts,
            string groupMask,
            Action<string> statusWriter)
        {
            if (manager == null)
                throw new ArgumentNullException("manager");
            if (string.IsNullOrWhiteSpace(groupMask))
                throw new InvalidOperationException("Group mask is required.");

            Action<string> writer = statusWriter ?? (_ => { });
            writer("Requesting MT5 trading account rows for current snapshot.");

            var accounts = manager.UserCreateAccountArray();
            try
            {
                MTRetCode result = manager.UserAccountRequestArray(groupMask, accounts);
                if (result != MTRetCode.MT_RET_OK)
                    throw new InvalidOperationException("UserAccountRequestArray failed: " + result + " (" + (uint)result + ")");

                var rows = new List<Mt5TradingAccountRow>((int)accounts.Total());
                for (uint i = 0; i < accounts.Total(); i++)
                {
                    CIMTAccount account = accounts.Next(i);
                    if (account == null)
                        continue;

                    Mt5LoginContext loginContext = ResolveLoginContext(account.Login(), loginContexts);
                    string currency = ResolveLoginCurrency(loginContext, groupCurrencies);
                    int currencyDigits = NormalizeCurrencyDigits((int)account.CurrencyDigits());

                    rows.Add(new Mt5TradingAccountRow
                    {
                        Login = account.Login(),
                        Name = loginContext.Name ?? string.Empty,
                        Group = loginContext.Group ?? string.Empty,
                        Currency = currency,
                        CurrencyDigits = currencyDigits,
                        Balance = account.Balance(),
                        Credit = account.Credit(),
                        Profit = account.Profit(),
                        Equity = account.Equity(),
                        Margin = account.Margin(),
                        FreeMargin = account.MarginFree(),
                        MarginLevel = account.MarginLevel(),
                        MarginLeverage = account.MarginLeverage()
                    });
                }

                rows.Sort(CompareTradingAccountRows);

                return new Mt5TradingAccountsSnapshot
                {
                    GeneratedAt = DateTime.Now,
                    Rows = rows
                };
            }
            finally
            {
                accounts.Release();
            }
        }

	        public static WdEquityZProtectedBonusCollection CollectWdEquityZProtectedBonuses(
	            CIMTManagerAPI manager,
	            Dictionary<string, string> groupCurrencies,
	            Dictionary<ulong, Mt5LoginContext> loginContexts,
            string groupMask,
            DateTime bonusHistoryFrom,
            DateTime reportDate,
            string bonusCommentContains,
            bool includeDealRows,
            Action<string> statusWriter)
        {
            if (manager == null)
                throw new ArgumentNullException("manager");
            if (string.IsNullOrWhiteSpace(groupMask))
                throw new InvalidOperationException("Group mask is required.");
            if (string.IsNullOrWhiteSpace(bonusCommentContains))
                throw new InvalidOperationException("Bonus comment filter is required.");

            DateTime normalizedHistoryFrom = bonusHistoryFrom.Date;
            DateTime normalizedReportDate = reportDate.Date;
            if (normalizedHistoryFrom > normalizedReportDate)
                throw new InvalidOperationException("Bonus history start must be on or before the report date.");

            Dictionary<string, CurrencyRate> currencyRates = BuildCurrencyRates(manager);
            var missingCurrencies = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            int fallbackConversionCount = 0;
            var collection = new WdEquityZProtectedBonusCollection();

            DateTime historyStartUtc = CreateUtcBoundary(normalizedHistoryFrom, false);
            DateTime reportStartUtc = CreateUtcBoundary(normalizedReportDate, false);
            DateTime reportEndUtc = CreateUtcBoundary(normalizedReportDate, true);
            Action<string> writer = statusWriter ?? (_ => { });
            writer(string.Format(
                CultureInfo.InvariantCulture,
                "Requesting protected bonus deals for {0:yyyy-MM-dd} to {1:yyyy-MM-dd}.",
                normalizedHistoryFrom,
                normalizedReportDate));

            var deals = manager.DealCreateArray();
            try
            {
                MTRetCode result = manager.DealRequestByGroup(
                    groupMask,
                    SMTTime.FromDateTime(historyStartUtc),
                    SMTTime.FromDateTime(reportEndUtc),
                    deals);

                if (result != MTRetCode.MT_RET_OK)
                    throw new InvalidOperationException("DealRequestByGroup failed: " + result + " (" + (uint)result + ")");

                for (uint i = 0; i < deals.Total(); i++)
                {
                    CIMTDeal deal = deals.Next(i);
                    if (deal == null || deal.Action() != ActionBalance)
                        continue;

                    string comment = deal.Comment() ?? string.Empty;
                    if (comment.IndexOf(bonusCommentContains, StringComparison.OrdinalIgnoreCase) < 0)
                        continue;

                    DateTime dealTime = SMTTime.ToDateTime(deal.Time());
                    Mt5LoginContext loginContext = ResolveLoginContext(deal.Login(), loginContexts);
                    string currency = ResolveLoginCurrency(loginContext, groupCurrencies);
                    double amountUsd = ConvertComponentsToUsd(
                        currency,
                        deal.RateProfit(),
                        currencyRates,
                        missingCurrencies,
                        ref fallbackConversionCount,
                        deal.Profit());

                    collection.EndProtectedBonusesUsd += amountUsd;
                    if (dealTime < reportStartUtc)
                        collection.StartProtectedBonusesUsd += amountUsd;

                    collection.DealCount++;
                    if (includeDealRows)
                    {
                        collection.Deals.Add(new Mt5WdEquityZProtectedBonusDeal
                        {
                            Deal = deal.Deal(),
                            Login = deal.Login(),
                            Name = loginContext.Name ?? string.Empty,
                            Group = loginContext.Group ?? string.Empty,
                            Time = dealTime,
                            Currency = currency,
                            CurrencyDigits = NormalizeCurrencyDigits((int)deal.DigitsCurrency()),
                            Comment = comment,
                            Amount = deal.Profit(),
                            AmountUsd = amountUsd
                        });
                    }
                }
            }
            finally
            {
                deals.Dispose();
            }

            collection.MissingCurrencyRates = missingCurrencies
                .OrderBy(code => code, StringComparer.OrdinalIgnoreCase)
                .ToList();
            if (fallbackConversionCount > 0)
            {
                writer(string.Format(
                    CultureInfo.InvariantCulture,
                    "Protected bonus deal conversion used {0} fallback conversions.",
                    fallbackConversionCount));
            }

	            return collection;
	        }

	        private static bool ShouldExcludeDailyPnlCashNetDepositComment(
	            string comment,
	            IEnumerable<string> excludedCommentContains)
	        {
	            string value = comment ?? string.Empty;
	            if (excludedCommentContains == null)
	                return false;

	            foreach (string token in excludedCommentContains)
	            {
	                if (string.IsNullOrWhiteSpace(token))
	                    continue;

	                if (value.IndexOf(token, StringComparison.OrdinalIgnoreCase) >= 0)
	                    return true;
	            }

	            return false;
	        }

	        public static DailyPnlCashNetDepositCollection CollectDailyPnlCashNetDeposits(
	            CIMTManagerAPI manager,
	            Dictionary<string, string> groupCurrencies,
	            Dictionary<ulong, Mt5LoginContext> loginContexts,
	            string groupMask,
	            DateTime reportDate,
	            IEnumerable<string> excludedCommentContains,
	            bool includeDealRows,
	            Action<string> statusWriter)
	        {
	            if (manager == null)
	                throw new ArgumentNullException("manager");
	            if (string.IsNullOrWhiteSpace(groupMask))
	                throw new InvalidOperationException("Group mask is required.");

	            DateTime normalizedReportDate = reportDate.Date;
	            if (normalizedReportDate == default(DateTime))
	                throw new InvalidOperationException("ReportDate is required.");

	            List<string> excludedComments = excludedCommentContains == null
	                ? new List<string>()
	                : excludedCommentContains
	                    .Where(value => !string.IsNullOrWhiteSpace(value))
	                    .Select(value => value.Trim())
	                    .Distinct(StringComparer.OrdinalIgnoreCase)
	                    .OrderBy(value => value, StringComparer.OrdinalIgnoreCase)
	                    .ToList();

	            Dictionary<string, CurrencyRate> currencyRates = BuildCurrencyRates(manager);
	            var missingCurrencies = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
	            int fallbackConversionCount = 0;
	            var collection = new DailyPnlCashNetDepositCollection();

	            DateTime reportStartUtc = CreateUtcBoundary(normalizedReportDate, false);
	            DateTime reportEndUtc = CreateUtcBoundary(normalizedReportDate, true);
	            Action<string> writer = statusWriter ?? (_ => { });
	            writer(string.Format(
	                CultureInfo.InvariantCulture,
	                "Requesting Daily PnL Cash net deposit deals for {0:yyyy-MM-dd}.",
	                normalizedReportDate));

	            var deals = manager.DealCreateArray();
	            try
	            {
	                MTRetCode result = manager.DealRequestByGroup(
	                    groupMask,
	                    SMTTime.FromDateTime(reportStartUtc),
	                    SMTTime.FromDateTime(reportEndUtc),
	                    deals);

	                if (result != MTRetCode.MT_RET_OK)
	                    throw new InvalidOperationException("DealRequestByGroup failed: " + result + " (" + (uint)result + ")");

	                for (uint i = 0; i < deals.Total(); i++)
	                {
	                    CIMTDeal deal = deals.Next(i);
	                    if (deal == null || deal.Action() != ActionBalance)
	                        continue;

	                    string comment = deal.Comment() ?? string.Empty;
	                    if (ShouldExcludeDailyPnlCashNetDepositComment(comment, excludedComments))
	                        continue;

	                    DateTime dealTime = SMTTime.ToDateTime(deal.Time());
	                    Mt5LoginContext loginContext = ResolveLoginContext(deal.Login(), loginContexts);
	                    string currency = ResolveLoginCurrency(loginContext, groupCurrencies);
	                    double amountUsd = ConvertComponentsToUsd(
	                        currency,
	                        deal.RateProfit(),
	                        currencyRates,
	                        missingCurrencies,
	                        ref fallbackConversionCount,
	                        deal.Profit());

	                    collection.NetDepositsUsd += amountUsd;
	                    collection.DealCount++;
	                    if (includeDealRows)
	                    {
	                        collection.Deals.Add(new Mt5DailyPnlCashNetDepositDeal
	                        {
	                            Deal = deal.Deal(),
	                            Login = deal.Login(),
	                            Name = loginContext.Name ?? string.Empty,
	                            Group = loginContext.Group ?? string.Empty,
	                            Time = dealTime,
	                            Currency = currency,
	                            CurrencyDigits = NormalizeCurrencyDigits((int)deal.DigitsCurrency()),
	                            Comment = comment,
	                            Amount = deal.Profit(),
	                            AmountUsd = amountUsd
	                        });
	                    }
	                }
	            }
	            finally
	            {
	                deals.Dispose();
	            }

	            collection.MissingCurrencyRates = missingCurrencies
	                .OrderBy(code => code, StringComparer.OrdinalIgnoreCase)
	                .ToList();
	            if (fallbackConversionCount > 0)
	            {
	                writer(string.Format(
	                    CultureInfo.InvariantCulture,
	                    "Daily PnL Cash net deposit conversion used {0} fallback conversions.",
	                    fallbackConversionCount));
	            }

	            return collection;
	        }

        public static Mt5DepositWithdrawalSnapshot CollectDepositWithdrawals(
            CIMTManagerAPI manager,
            Dictionary<string, string> groupCurrencies,
            Dictionary<ulong, Mt5LoginContext> loginContexts,
            string groupMask,
            DateTime fromDate,
            DateTime toDate,
            Action<string> statusWriter)
        {
            if (manager == null)
                throw new ArgumentNullException("manager");
            if (string.IsNullOrWhiteSpace(groupMask))
                throw new InvalidOperationException("Group mask is required.");

            DateTime normalizedFrom = fromDate.Date;
            DateTime normalizedTo = toDate.Date;
            if (normalizedTo < normalizedFrom)
                throw new InvalidOperationException("Deposit and withdrawal report end date must be on or after the start date.");

            DateTime rangeStartUtc = CreateUtcBoundary(normalizedFrom, false);
            DateTime rangeEndUtc = CreateUtcBoundary(normalizedTo, true);
            Action<string> writer = statusWriter ?? (_ => { });
            writer(string.Format(
                CultureInfo.InvariantCulture,
                "Requesting MT5 deposit and withdrawal rows for {0:yyyy-MM-dd} to {1:yyyy-MM-dd}.",
                normalizedFrom,
                normalizedTo));

            var deals = manager.DealCreateArray();
            try
            {
                MTRetCode result = manager.DealRequestByGroup(
                    groupMask,
                    ToUnixSeconds(rangeStartUtc),
                    ToUnixSeconds(rangeEndUtc),
                    deals);

                if (result != MTRetCode.MT_RET_OK)
                    throw new InvalidOperationException("DealRequestByGroup failed: " + result + " (" + (uint)result + ")");

                var rows = new List<Mt5DepositWithdrawalRow>();
                var totalsByCurrency = new Dictionary<string, Mt5DepositWithdrawalCurrencyTotal>(StringComparer.OrdinalIgnoreCase);

                for (uint i = 0; i < deals.Total(); i++)
                {
                    CIMTDeal deal = deals.Next(i);
                    if (deal == null || deal.Action() != ActionBalance)
                        continue;

                    Mt5LoginContext loginContext = ResolveLoginContext(deal.Login(), loginContexts);
                    string currency = ResolveLoginCurrency(loginContext, groupCurrencies);
                    int currencyDigits = NormalizeCurrencyDigits((int)deal.DigitsCurrency());
                    double amount = deal.Profit();

                    rows.Add(new Mt5DepositWithdrawalRow
                    {
                        Deal = deal.Deal(),
                        Login = deal.Login(),
                        Name = loginContext.Name ?? string.Empty,
                        Group = loginContext.Group ?? string.Empty,
                        Time = SMTTime.ToDateTime(deal.Time()),
                        Comment = deal.Comment() ?? string.Empty,
                        Amount = amount,
                        Currency = currency,
                        CurrencyDigits = currencyDigits
                    });

                    Mt5DepositWithdrawalCurrencyTotal total;
                    if (!totalsByCurrency.TryGetValue(currency, out total))
                    {
                        total = new Mt5DepositWithdrawalCurrencyTotal
                        {
                            Currency = currency,
                            CurrencyDigits = currencyDigits
                        };
                        totalsByCurrency[currency] = total;
                    }

                    total.DealCount++;
                    if (amount >= 0)
                        total.Deposits = MoneyAdd(total.Deposits, amount, total.CurrencyDigits);
                    else
                        total.Withdrawals = MoneyAdd(total.Withdrawals, Math.Abs(amount), total.CurrencyDigits);

                    total.NetAmount = Math.Round(
                        total.Deposits - total.Withdrawals,
                        NormalizeCurrencyDigits(total.CurrencyDigits),
                        MidpointRounding.AwayFromZero);
                }

                rows.Sort(CompareDepositWithdrawalRows);
                List<Mt5DepositWithdrawalCurrencyTotal> totals = totalsByCurrency.Values.ToList();
                totals.Sort(CompareDepositWithdrawalCurrencyTotals);

                return new Mt5DepositWithdrawalSnapshot
                {
                    GeneratedAt = DateTime.Now,
                    RangeFrom = normalizedFrom,
                    RangeTo = normalizedTo,
                    Rows = rows,
                    CurrencyTotals = totals
                };
            }
            finally
            {
                deals.Dispose();
            }
        }

	        public static Mt5PositionHistorySnapshot CollectPositionHistory(
	            CIMTManagerAPI manager,
            Dictionary<string, string> groupCurrencies,
            Dictionary<ulong, Mt5LoginContext> loginContexts,
            string groupMask,
            DateTime fromDate,
            DateTime toDate,
            Action<string> statusWriter)
        {
            if (manager == null)
                throw new ArgumentNullException("manager");
            if (string.IsNullOrWhiteSpace(groupMask))
                throw new InvalidOperationException("Group mask is required.");

            DateTime normalizedFrom = fromDate.Date;
            DateTime normalizedTo = toDate.Date;
            if (normalizedTo < normalizedFrom)
                throw new InvalidOperationException("Positions history end date must be on or after the start date.");

            DateTime rangeStartUtc = CreateUtcBoundary(normalizedFrom, false);
            DateTime rangeEndUtc = CreateUtcBoundary(normalizedTo, true);
            Action<string> writer = statusWriter ?? (_ => { });
            writer(string.Format(
                CultureInfo.InvariantCulture,
                "Requesting MT5 positions history rows for {0:yyyy-MM-dd} to {1:yyyy-MM-dd}.",
                normalizedFrom,
                normalizedTo));

            var rangeDeals = manager.DealCreateArray();
            try
            {
                MTRetCode rangeResult = manager.DealRequestByGroup(
                    groupMask,
                    ToUnixSeconds(rangeStartUtc),
                    ToUnixSeconds(rangeEndUtc),
                    rangeDeals);

                if (rangeResult != MTRetCode.MT_RET_OK)
                    throw new InvalidOperationException("DealRequestByGroup failed: " + rangeResult + " (" + (uint)rangeResult + ")");

                var targetPositionIds = new HashSet<ulong>();
                var targetLogins = new HashSet<ulong>();

                for (uint i = 0; i < rangeDeals.Total(); i++)
                {
                    CIMTDeal deal = rangeDeals.Next(i);
                    if (deal == null)
                        continue;

                    if (!ShouldTrackPositionHistoryDeal(deal.Action(), deal.PositionID()))
                        continue;

                    targetPositionIds.Add(deal.PositionID());
                    targetLogins.Add(deal.Login());
                }

                if (targetPositionIds.Count == 0 || targetLogins.Count == 0)
                {
                    return new Mt5PositionHistorySnapshot
                    {
                        GeneratedAt = DateTime.Now,
                        RangeFrom = normalizedFrom,
                        RangeTo = normalizedTo
                    };
                }

                DateTime historyStart = new DateTime(PositionHistoryStartYear, 1, 1);

                writer(string.Format(
                    CultureInfo.InvariantCulture,
                    "Backfilling history for {0} logins and {1} positions from {2:yyyy-MM-dd}.",
                    targetLogins.Count,
                    targetPositionIds.Count,
                    historyStart));

                var historicalDeals = manager.DealCreateArray();
                try
                {
                    ulong[] logins = targetLogins.OrderBy(value => value).ToArray();
                    MTRetCode historyResult = manager.DealRequestByLogins(
                        logins,
                        ToUnixSeconds(CreateUtcBoundary(historyStart, false)),
                        ToUnixSeconds(rangeEndUtc),
                        historicalDeals);

                    if (historyResult != MTRetCode.MT_RET_OK)
                        throw new InvalidOperationException("DealRequestByLogins failed: " + historyResult + " (" + (uint)historyResult + ")");

                    var deals = new List<PositionHistoryDealRecord>((int)historicalDeals.Total());
                    for (uint i = 0; i < historicalDeals.Total(); i++)
                    {
                        CIMTDeal deal = historicalDeals.Next(i);
                        if (deal == null)
                            continue;

                        if (!targetPositionIds.Contains(deal.PositionID()))
                            continue;
                        if (!ShouldTrackPositionHistoryDeal(deal.Action(), deal.PositionID()))
                            continue;

                        deals.Add(new PositionHistoryDealRecord
                        {
                            Login = deal.Login(),
                            PositionId = deal.PositionID(),
                            Deal = deal.Deal(),
                            Time = ConvertTradeTime(deal.TimeMsc(), deal.Time()),
                            Action = deal.Action(),
                            Entry = deal.Entry(),
                            Symbol = deal.Symbol() ?? string.Empty,
                            Volume = ToDisplayLots(deal.Volume()),
                            Price = deal.Price(),
                            StopLoss = deal.PriceSL(),
                            TakeProfit = deal.PriceTP(),
                            Commission = deal.Commission(),
                            Fee = deal.Fee(),
                            Swap = deal.Storage(),
                            Profit = deal.Profit(),
                            Reason = deal.Reason(),
                            Comment = deal.Comment() ?? string.Empty,
                            Digits = NormalizeCurrencyDigits((int)deal.Digits()),
                            CurrencyDigits = NormalizeCurrencyDigits((int)deal.DigitsCurrency())
                        });
                    }

                    deals.Sort(ComparePositionHistoryDeals);

                    var positions = new Dictionary<ulong, PositionHistoryAccumulator>();
                    for (int i = 0; i < deals.Count; i++)
                    {
                        PositionHistoryDealRecord deal = deals[i];
                        PositionHistoryAccumulator accumulator;
                        if (!positions.TryGetValue(deal.PositionId, out accumulator))
                        {
                            accumulator = CreatePositionHistoryAccumulator(deal, loginContexts, groupCurrencies);
                            positions[deal.PositionId] = accumulator;
                        }

                        ApplyPositionHistoryDeal(accumulator, deal);
                    }

                    var rows = new List<Mt5PositionHistoryRow>(positions.Count);
                    var totalsByCurrency = new Dictionary<string, Mt5PositionHistoryCurrencyTotal>(StringComparer.OrdinalIgnoreCase);

                    foreach (PositionHistoryAccumulator accumulator in positions.Values)
                    {
                        if (accumulator.CloseVolume <= 0.0)
                            continue;
                        if (accumulator.CloseTime < rangeStartUtc || accumulator.CloseTime > rangeEndUtc)
                            continue;

                        var row = new Mt5PositionHistoryRow
                        {
                            Login = accumulator.Login,
                            Name = accumulator.Name,
                            OpenTime = accumulator.OpenTime,
                            Ticket = accumulator.Ticket,
                            Type = accumulator.Type,
                            Volume = accumulator.CloseVolume,
                            Symbol = accumulator.Symbol,
                            OpenPrice = accumulator.OpenPrice,
                            StopLoss = accumulator.StopLoss,
                            TakeProfit = accumulator.TakeProfit,
                            CloseTime = accumulator.CloseTime,
                            ClosePrice = accumulator.ClosePrice,
                            Reason = accumulator.Reason,
                            Commission = accumulator.Commission,
                            Fee = accumulator.Fee,
                            Swap = accumulator.Swap,
                            Profit = accumulator.Profit,
                            Currency = accumulator.Currency,
                            Comment = accumulator.Comment,
                            Digits = accumulator.Digits,
                            CurrencyDigits = accumulator.CurrencyDigits
                        };

                        rows.Add(row);

                        Mt5PositionHistoryCurrencyTotal total;
                        if (!totalsByCurrency.TryGetValue(row.Currency ?? string.Empty, out total))
                        {
                            total = new Mt5PositionHistoryCurrencyTotal
                            {
                                Currency = row.Currency ?? string.Empty,
                                CurrencyDigits = row.CurrencyDigits
                            };
                            totalsByCurrency[total.Currency] = total;
                        }

                        total.CurrencyDigits = Math.Max(total.CurrencyDigits, row.CurrencyDigits);
                        total.Commission = MoneyAdd(total.Commission, row.Commission, total.CurrencyDigits);
                        total.Fee = MoneyAdd(total.Fee, row.Fee, total.CurrencyDigits);
                        total.Swap = MoneyAdd(total.Swap, row.Swap, total.CurrencyDigits);
                        total.Profit = MoneyAdd(total.Profit, row.Profit, total.CurrencyDigits);
                    }

                    rows.Sort(ComparePositionHistoryRows);
                    List<Mt5PositionHistoryCurrencyTotal> totals = totalsByCurrency.Values
                        .OrderBy(item => item.Currency, StringComparer.OrdinalIgnoreCase)
                        .ToList();

                    return new Mt5PositionHistorySnapshot
                    {
                        GeneratedAt = DateTime.Now,
                        RangeFrom = normalizedFrom,
                        RangeTo = normalizedTo,
                        Rows = rows,
                        CurrencyTotals = totals
                    };
                }
                finally
                {
                    historicalDeals.Release();
                }
            }
            finally
            {
                rangeDeals.Release();
            }
        }

        private static bool ShouldTrackPositionHistoryDeal(uint action, ulong positionId)
        {
            if (positionId == 0)
                return false;

            return action == ActionBuy ||
                   action == ActionSell ||
                   action == ActionBuyCanceled ||
                   action == ActionSellCanceled;
        }

        private static PositionHistoryAccumulator CreatePositionHistoryAccumulator(
            PositionHistoryDealRecord deal,
            Dictionary<ulong, Mt5LoginContext> loginContexts,
            Dictionary<string, string> groupCurrencies)
        {
            Mt5LoginContext loginContext;
            if (loginContexts == null || !loginContexts.TryGetValue(deal.Login, out loginContext))
            {
                loginContext = new Mt5LoginContext
                {
                    Login = deal.Login,
                    Name = string.Empty,
                    Group = string.Empty,
                    Currency = "USD"
                };
            }

            string currency = loginContext.Currency;
            if (string.IsNullOrWhiteSpace(currency))
            {
                string groupCurrency;
                if (groupCurrencies != null &&
                    !string.IsNullOrWhiteSpace(loginContext.Group) &&
                    groupCurrencies.TryGetValue(loginContext.Group, out groupCurrency) &&
                    !string.IsNullOrWhiteSpace(groupCurrency))
                {
                    currency = groupCurrency;
                }
            }

            if (string.IsNullOrWhiteSpace(currency))
                currency = "USD";

            return new PositionHistoryAccumulator
            {
                Login = deal.Login,
                Name = loginContext.Name ?? string.Empty,
                Ticket = deal.PositionId,
                Currency = currency
            };
        }

        private static void ApplyPositionHistoryDeal(PositionHistoryAccumulator accumulator, PositionHistoryDealRecord deal)
        {
            if (accumulator == null)
                throw new ArgumentNullException("accumulator");

            bool serviceDeal = IsPositionHistoryServiceReason(deal.Reason);
            bool tradeDeal = IsPositionHistoryTradeReason(deal.Reason);

            if (accumulator.OpenVolume <= 0.0 && accumulator.CloseVolume <= 0.0 && string.IsNullOrWhiteSpace(accumulator.Symbol))
            {
                accumulator.Symbol = deal.Symbol;
                accumulator.Type = FormatPositionHistoryType(deal.Action);
                accumulator.Reason = FormatPositionHistoryReason(deal.Reason);
                accumulator.OpenVolume = deal.Volume;
                accumulator.OpenTime = deal.Time;
                accumulator.OpenPrice = deal.Price;
                accumulator.StopLoss = deal.StopLoss;
                accumulator.TakeProfit = deal.TakeProfit;
                accumulator.ClosePrice = 0.0;
                accumulator.CloseTime = default(DateTime);
                accumulator.Profit = deal.Profit;
                accumulator.Swap = deal.Swap;
                accumulator.Commission = deal.Commission;
                accumulator.Fee = deal.Fee;
                accumulator.Digits = deal.Digits;
                accumulator.CurrencyDigits = deal.CurrencyDigits;
                accumulator.Comment = deal.Comment ?? string.Empty;
                return;
            }

            accumulator.Profit = MoneyAdd(accumulator.Profit, deal.Profit, accumulator.CurrencyDigits);
            accumulator.Swap = MoneyAdd(accumulator.Swap, deal.Swap, accumulator.CurrencyDigits);
            accumulator.Commission = MoneyAdd(accumulator.Commission, deal.Commission, accumulator.CurrencyDigits);
            accumulator.Fee = MoneyAdd(accumulator.Fee, deal.Fee, accumulator.CurrencyDigits);

            bool sameSide = IsSamePositionSide(accumulator.Type, deal.Action);
            if (sameSide)
            {
                if (tradeDeal)
                {
                    double combinedVolume = accumulator.OpenVolume + deal.Volume;
                    if (combinedVolume > 0.0)
                    {
                        accumulator.OpenPrice =
                            ((accumulator.OpenVolume * accumulator.OpenPrice) + (deal.Volume * deal.Price)) /
                            combinedVolume;
                        accumulator.OpenVolume = combinedVolume;
                    }

                    if (accumulator.OpenTime == default(DateTime) || deal.Time < accumulator.OpenTime)
                        accumulator.OpenTime = deal.Time;

                    accumulator.StopLoss = deal.StopLoss;
                    accumulator.TakeProfit = deal.TakeProfit;
                }

                if (deal.Entry == EntryOut || deal.Entry == EntryInOut)
                {
                    if (deal.Time > accumulator.CloseTime)
                        accumulator.CloseTime = deal.Time;
                }

                if (!serviceDeal && !string.IsNullOrWhiteSpace(deal.Comment))
                    accumulator.Comment = deal.Comment;
            }
            else
            {
                if (tradeDeal)
                {
                    double combinedCloseVolume = accumulator.CloseVolume + deal.Volume;
                    if (combinedCloseVolume > 0.0)
                    {
                        accumulator.ClosePrice =
                            accumulator.CloseVolume <= 0.0
                                ? deal.Price
                                : ((accumulator.CloseVolume * accumulator.ClosePrice) + (deal.Volume * deal.Price)) /
                                  combinedCloseVolume;
                        accumulator.CloseVolume = combinedCloseVolume;
                    }

                    if (deal.Time > accumulator.CloseTime)
                        accumulator.CloseTime = deal.Time;

                    accumulator.StopLoss = deal.StopLoss;
                    accumulator.TakeProfit = deal.TakeProfit;
                }
            }

            accumulator.Digits = Math.Max(accumulator.Digits, deal.Digits);
            accumulator.CurrencyDigits = Math.Max(accumulator.CurrencyDigits, deal.CurrencyDigits);
            if (string.IsNullOrWhiteSpace(accumulator.Symbol))
                accumulator.Symbol = deal.Symbol;
            if (string.IsNullOrWhiteSpace(accumulator.Reason))
                accumulator.Reason = FormatPositionHistoryReason(deal.Reason);
        }

        private static bool IsSamePositionSide(string positionType, uint action)
        {
            if (string.Equals(positionType, "buy", StringComparison.OrdinalIgnoreCase))
                return action == ActionBuy || action == ActionBuyCanceled;
            if (string.Equals(positionType, "sell", StringComparison.OrdinalIgnoreCase))
                return action == ActionSell || action == ActionSellCanceled;

            return false;
        }

        private static bool IsPositionHistoryTradeReason(uint reason)
        {
            return reason != (uint)CIMTDeal.EnDealReason.DEAL_REASON_ROLLOVER &&
                   reason != (uint)CIMTDeal.EnDealReason.DEAL_REASON_VMARGIN &&
                   reason != (uint)CIMTDeal.EnDealReason.DEAL_REASON_TRANSFER &&
                   reason != (uint)CIMTDeal.EnDealReason.DEAL_REASON_SYNC &&
                   reason != (uint)CIMTDeal.EnDealReason.DEAL_REASON_SPLIT &&
                   reason != (uint)CIMTDeal.EnDealReason.DEAL_REASON_CORPORATE_ACTION;
        }

        private static bool IsPositionHistoryServiceReason(uint reason)
        {
            return reason == (uint)CIMTDeal.EnDealReason.DEAL_REASON_ROLLOVER ||
                   reason == (uint)CIMTDeal.EnDealReason.DEAL_REASON_VMARGIN ||
                   reason == (uint)CIMTDeal.EnDealReason.DEAL_REASON_SETTLEMENT ||
                   reason == (uint)CIMTDeal.EnDealReason.DEAL_REASON_TRANSFER ||
                   reason == (uint)CIMTDeal.EnDealReason.DEAL_REASON_SYNC ||
                   reason == (uint)CIMTDeal.EnDealReason.DEAL_REASON_EXTERNAL_SERVICE ||
                   reason == (uint)CIMTDeal.EnDealReason.DEAL_REASON_SPLIT;
        }

        private static string FormatPositionHistoryType(uint action)
        {
            if (action == ActionBuy || action == ActionBuyCanceled)
                return "buy";
            if (action == ActionSell || action == ActionSellCanceled)
                return "sell";

            return action.ToString(CultureInfo.InvariantCulture);
        }

        private static string FormatPositionHistoryReason(uint reason)
        {
            if (reason == (uint)CIMTDeal.EnDealReason.DEAL_REASON_FIRST)
                return "Client";
            if (reason == (uint)CIMTDeal.EnDealReason.DEAL_REASON_EXPERT)
                return "Expert";
            if (reason == (uint)CIMTDeal.EnDealReason.DEAL_REASON_DEALER)
                return "Dealer";
            if (reason == (uint)CIMTDeal.EnDealReason.DEAL_REASON_SL)
                return "S/L";
            if (reason == (uint)CIMTDeal.EnDealReason.DEAL_REASON_TP)
                return "T/P";
            if (reason == (uint)CIMTDeal.EnDealReason.DEAL_REASON_SO)
                return "S/O";
            if (reason == (uint)CIMTDeal.EnDealReason.DEAL_REASON_ROLLOVER)
                return "Rollover";
            if (reason == (uint)CIMTDeal.EnDealReason.DEAL_REASON_EXTERNAL_CLIENT)
                return "External";
            if (reason == (uint)CIMTDeal.EnDealReason.DEAL_REASON_VMARGIN)
                return "VMargin";
            if (reason == (uint)CIMTDeal.EnDealReason.DEAL_REASON_GATEWAY)
                return "Gateway";
            if (reason == (uint)CIMTDeal.EnDealReason.DEAL_REASON_SIGNAL)
                return "Signal";
            if (reason == (uint)CIMTDeal.EnDealReason.DEAL_REASON_SETTLEMENT)
                return "Settlement";
            if (reason == (uint)CIMTDeal.EnDealReason.DEAL_REASON_TRANSFER)
                return "Transfer";
            if (reason == (uint)CIMTDeal.EnDealReason.DEAL_REASON_SYNC)
                return "Sync";
            if (reason == (uint)CIMTDeal.EnDealReason.DEAL_REASON_EXTERNAL_SERVICE)
                return "External Service";
            if (reason == (uint)CIMTDeal.EnDealReason.DEAL_REASON_MIGRATION)
                return "Migration";
            if (reason == (uint)CIMTDeal.EnDealReason.DEAL_REASON_MOBILE)
                return "Mobile";
            if (reason == (uint)CIMTDeal.EnDealReason.DEAL_REASON_WEB)
                return "Web";
            if (reason == (uint)CIMTDeal.EnDealReason.DEAL_REASON_SPLIT)
                return "Split";
            if (reason == (uint)CIMTDeal.EnDealReason.DEAL_REASON_CORPORATE_ACTION)
                return "Corporate Action";

            return reason.ToString(CultureInfo.InvariantCulture);
        }

        private static int ComparePositionHistoryDeals(PositionHistoryDealRecord left, PositionHistoryDealRecord right)
        {
            int loginCompare = left.Login.CompareTo(right.Login);
            if (loginCompare != 0)
                return loginCompare;

            int positionCompare = left.PositionId.CompareTo(right.PositionId);
            if (positionCompare != 0)
                return positionCompare;

            int timeCompare = DateTime.Compare(left.Time, right.Time);
            if (timeCompare != 0)
                return timeCompare;

            return left.Deal.CompareTo(right.Deal);
        }

        private static int ComparePositionHistoryRows(Mt5PositionHistoryRow left, Mt5PositionHistoryRow right)
        {
            int loginCompare = left.Login.CompareTo(right.Login);
            if (loginCompare != 0)
                return loginCompare;

            int timeCompare = DateTime.Compare(left.OpenTime, right.OpenTime);
            if (timeCompare != 0)
                return timeCompare;

            return left.Ticket.CompareTo(right.Ticket);
        }

        private static DateTime CreateUtcBoundary(DateTime date, bool endOfDay)
        {
            DateTime utcDate = new DateTime(date.Year, date.Month, date.Day, 0, 0, 0, DateTimeKind.Utc);
            return endOfDay ? utcDate.AddDays(1).AddMilliseconds(-1) : utcDate;
        }

        private static long ToUnixSeconds(DateTime utcDateTime)
        {
            DateTime utc = utcDateTime.Kind == DateTimeKind.Utc
                ? utcDateTime
                : DateTime.SpecifyKind(utcDateTime, DateTimeKind.Utc);

            return (long)(utc - UnixEpoch).TotalSeconds;
        }

        private static DateTime ConvertTradeTime(long timeMilliseconds, long fallbackSeconds)
        {
            if (timeMilliseconds > 0)
            {
                try
                {
                    return UnixEpoch.AddMilliseconds(timeMilliseconds);
                }
                catch (ArgumentOutOfRangeException)
                {
                }
            }

            if (fallbackSeconds > 0)
                return SMTTime.ToDateTime(fallbackSeconds);

            return default(DateTime);
        }

        private static double ConvertComponentsToUsd(
            string currency,
            double fallbackRateProfit,
            Dictionary<string, CurrencyRate> currencyRates,
            HashSet<string> missingCurrencies,
            ref int fallbackConversionCount,
            params double[] nativeAmounts)
        {
            double total = 0.0;
            if (nativeAmounts == null || nativeAmounts.Length == 0)
                return total;

            for (int i = 0; i < nativeAmounts.Length; i++)
            {
                total += ConvertToUsdTrace(
                    nativeAmounts[i],
                    currency,
                    fallbackRateProfit,
                    currencyRates,
                    missingCurrencies,
                    ref fallbackConversionCount).UsdAmount;
            }

            return total;
        }

        private static UsdConversionTrace ConvertToUsdTrace(
            double native,
            string currency,
            double fallbackRateProfit,
            Dictionary<string, CurrencyRate> currencyRates,
            HashSet<string> missingCurrencies,
            ref int fallbackConversionCount)
        {
            var trace = new UsdConversionTrace();

            if (string.IsNullOrWhiteSpace(currency))
                currency = "USD";

            trace.Currency = currency;

            CurrencyRate usdRate;
            if (currencyRates.TryGetValue(currency, out usdRate))
            {
                trace.FxSymbol = usdRate.Symbol;
                trace.FxBid = usdRate.Bid;
                trace.FxAsk = usdRate.Ask;
                trace.FxUsdBase = usdRate.UsdBase;
            }

            if (native == 0.0)
                return trace;

            if (currencyRates.TryGetValue(currency, out usdRate))
            {
                double sideAwareRate = native >= 0.0 ? usdRate.PositiveToUsd : usdRate.NegativeToUsd;
                if (sideAwareRate > 0.0)
                {
                    trace.UsedRate = sideAwareRate;
                    trace.UsdAmount = native * sideAwareRate;
                    return trace;
                }
            }

            missingCurrencies.Add(currency);
            fallbackConversionCount++;
            trace.UsedFallback = true;
            trace.UsdAmount = ToUsd(native, fallbackRateProfit);
            trace.UsedRate = native != 0.0 ? trace.UsdAmount / native : 0.0;
            return trace;
        }

        private static double ToDisplayLots(double rawVolume)
        {
            // MetaQuotes' SDK examples convert MT5 extended volume units to display lots by /10000.
            return rawVolume / VolumeScale;
        }

        private static List<Mt5SymbolSummaryRow> BuildSymbolRows(Dictionary<string, SymbolAggregate> symbolAggregates)
        {
            var rows = new List<Mt5SymbolSummaryRow>(symbolAggregates.Count);

            foreach (SymbolAggregate aggregate in symbolAggregates.Values.OrderBy(item => item.Symbol, StringComparer.OrdinalIgnoreCase))
            {
                double buyPrice = aggregate.ClientBuyVolume > 0
                    ? aggregate.ClientBuyPriceAmount / aggregate.ClientBuyVolume
                    : 0.0;
                double sellPrice = aggregate.ClientSellVolume > 0
                    ? aggregate.ClientSellPriceAmount / aggregate.ClientSellVolume
                    : 0.0;

                rows.Add(new Mt5SymbolSummaryRow
                {
                    Symbol = aggregate.Symbol,
                    Digits = aggregate.Digits,
                    ClientPositions = aggregate.ClientPositions,
                    CoveragePositions = 0,
                    ClientBuyVolume = aggregate.ClientBuyVolume,
                    CoverageBuyVolume = 0.0,
                    ClientBuyPrice = buyPrice,
                    CoverageBuyPrice = 0.0,
                    ClientSellVolume = aggregate.ClientSellVolume,
                    CoverageSellVolume = 0.0,
                    ClientSellPrice = sellPrice,
                    CoverageSellPrice = 0.0,
                    NetVolume = aggregate.ClientBuyVolume - aggregate.ClientSellVolume,
                    ClientProfitUsd = aggregate.ClientProfitUsd,
                    CoverageProfitUsd = 0.0,
                    UncoveredUsd = aggregate.ClientProfitUsd
                });
            }

            return rows;
        }

        private static Mt5SymbolSummaryTotals BuildSymbolTotals(IEnumerable<Mt5SymbolSummaryRow> rows)
        {
            var totals = new Mt5SymbolSummaryTotals();

            foreach (Mt5SymbolSummaryRow row in rows)
            {
                totals.ClientPositions += row.ClientPositions;
                totals.CoveragePositions += row.CoveragePositions;
                totals.ClientBuyVolume += row.ClientBuyVolume;
                totals.CoverageBuyVolume += row.CoverageBuyVolume;
                totals.ClientSellVolume += row.ClientSellVolume;
                totals.CoverageSellVolume += row.CoverageSellVolume;
                totals.ClientProfitUsd += row.ClientProfitUsd;
                totals.CoverageProfitUsd += row.CoverageProfitUsd;
                totals.UncoveredUsd += row.UncoveredUsd;
            }

            totals.NetVolume = totals.ClientBuyVolume - totals.ClientSellVolume;
            return totals;
        }

        private static string FormatPositionSide(uint action)
        {
            if (action == PositionBuy)
                return "Buy";
            if (action == PositionSell)
                return "Sell";

            return action.ToString(CultureInfo.InvariantCulture);
        }

        private static string BuildConversionSummary(HashSet<string> missingCurrencies, int fallbackConversionCount)
        {
            if (missingCurrencies == null || missingCurrencies.Count == 0)
                return "FX conversion: live bid/ask FX rates for all seen currencies.";

            return string.Format(
                "FX conversion: live bid/ask FX rates with {0} fallback conversions via RateProfit for: {1}.",
                fallbackConversionCount,
                string.Join(", ", missingCurrencies.OrderBy(code => code, StringComparer.OrdinalIgnoreCase)));
        }

        private static string ResolveDailyCurrency(CIMTDaily daily, Dictionary<string, string> groupCurrencies)
        {
            if (daily == null)
                return "USD";

            string groupName = daily.Group() ?? string.Empty;
            string currency;
            if (groupCurrencies != null &&
                !string.IsNullOrWhiteSpace(groupName) &&
                groupCurrencies.TryGetValue(groupName, out currency) &&
                !string.IsNullOrWhiteSpace(currency))
            {
                return currency;
            }

            string dailyCurrency = daily.Currency();
            if (!string.IsNullOrWhiteSpace(dailyCurrency))
                return dailyCurrency;

            return "USD";
        }

        private static Mt5LoginContext ResolveLoginContext(
            ulong login,
            Dictionary<ulong, Mt5LoginContext> loginContexts)
        {
            Mt5LoginContext context;
            if (loginContexts != null && loginContexts.TryGetValue(login, out context) && context != null)
                return context;

            return new Mt5LoginContext
            {
                Login = login,
                Name = string.Empty,
                Group = string.Empty,
                Currency = "USD"
            };
        }

        private static string ResolveLoginCurrency(
            Mt5LoginContext loginContext,
            Dictionary<string, string> groupCurrencies)
        {
            if (loginContext != null)
            {
                if (!string.IsNullOrWhiteSpace(loginContext.Currency))
                    return loginContext.Currency;

                string groupCurrency;
                if (!string.IsNullOrWhiteSpace(loginContext.Group) &&
                    groupCurrencies != null &&
                    groupCurrencies.TryGetValue(loginContext.Group, out groupCurrency) &&
                    !string.IsNullOrWhiteSpace(groupCurrency))
                {
                    return groupCurrency;
                }
            }

            return "USD";
        }

        private static int CompareDailyRows(Mt5DailyReportRow left, Mt5DailyReportRow right)
        {
            int timeCompare = DateTime.Compare(left.Timestamp, right.Timestamp);
            if (timeCompare != 0)
                return timeCompare;

            return left.Login.CompareTo(right.Login);
        }

        private static int CompareTradingAccountRows(Mt5TradingAccountRow left, Mt5TradingAccountRow right)
        {
            int groupCompare = string.Compare(left.Group, right.Group, StringComparison.OrdinalIgnoreCase);
            if (groupCompare != 0)
                return groupCompare;

            return left.Login.CompareTo(right.Login);
        }

        private static int CompareDepositWithdrawalRows(Mt5DepositWithdrawalRow left, Mt5DepositWithdrawalRow right)
        {
            int timeCompare = DateTime.Compare(left.Time, right.Time);
            if (timeCompare != 0)
                return timeCompare;

            return left.Deal.CompareTo(right.Deal);
        }

        private static int CompareDepositWithdrawalCurrencyTotals(
            Mt5DepositWithdrawalCurrencyTotal left,
            Mt5DepositWithdrawalCurrencyTotal right)
        {
            return string.Compare(left.Currency, right.Currency, StringComparison.OrdinalIgnoreCase);
        }

        private static int NormalizeCurrencyDigits(int digits)
        {
            if (digits < 0)
                return 0;
            if (digits > 8)
                return 8;
            return digits;
        }

        private static double MoneyAdd(double left, double right, int digits)
        {
            int safeDigits = NormalizeCurrencyDigits(digits);
            return Math.Round(left + right, safeDigits, MidpointRounding.AwayFromZero);
        }
    }
}
