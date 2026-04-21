using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Json;
using System.Text;
using System.Threading;
using MetaQuotes.MT5CommonAPI;
using MetaQuotes.MT5ManagerAPI;
using Mt5Monitor.Api;

public sealed class MT5LivePusher
{
    private const uint EntryIn = 0;
    private const uint ActionBuy = 0;
    private const uint ActionSell = 1;
    private const uint ActionBalance = 2;
    private const double VolumeScale = 10000.0;

    private static readonly HashSet<string> ExcludedGroups =
        new HashSet<string>(StringComparer.OrdinalIgnoreCase);

    private static readonly HashSet<ulong> KnownDepositors = new HashSet<ulong>();
    private static bool _knownDepositorsLoaded;
    private static string _knownDepositorsDate = string.Empty;
    private static int _ftdToday;

    private sealed class SymbolAccumulator
    {
        public string Symbol;
        public int DealCount;
        public HashSet<ulong> Traders = new HashSet<ulong>();
        public double NotionalUsd;
        public double NotionalBuyUsd;
        public double NotionalSellUsd;
        public double SwapUsd;
        public double CommissionUsd;
        public double FeeUsd;
        public double PnlUsd;
    }

    private sealed class GroupFloatingAccumulator
    {
        public string GroupName;
        public int PositionCount;
        public double FloatingPnlUsd;
    }

    private sealed class FastStats
    {
        public FastStats()
        {
            ClosedTotalsByCurrency = new Dictionary<string, Mt5PositionHistoryCurrencyTotal>(StringComparer.OrdinalIgnoreCase);
            ClosedTotalsByGroup = new Dictionary<string, Dictionary<string, Mt5PositionHistoryCurrencyTotal>>(StringComparer.OrdinalIgnoreCase);
            Symbols = new Dictionary<string, SymbolAccumulator>(StringComparer.OrdinalIgnoreCase);
            DepositorLogins = new HashSet<ulong>();
            TraderLogins = new HashSet<ulong>();
            ActiveTraderLogins = new HashSet<ulong>();
            GroupFloating = new List<GroupFloatingAccumulator>();
        }

        public double FloatingPnlUsd;
        public int PositionCount;
        public int ClosingDealCount;
        public double VolumeUsd;
        public double DepositsUsd;
        public double WithdrawalsUsd;
        public HashSet<ulong> DepositorLogins;
        public HashSet<ulong> TraderLogins;
        public HashSet<ulong> ActiveTraderLogins;
        public Dictionary<string, Mt5PositionHistoryCurrencyTotal> ClosedTotalsByCurrency;
        public Dictionary<string, Dictionary<string, Mt5PositionHistoryCurrencyTotal>> ClosedTotalsByGroup;
        public Dictionary<string, SymbolAccumulator> Symbols;
        public List<GroupFloatingAccumulator> GroupFloating;
        public Mt5DailyClosedPnlResult ClosedPnl;
        public List<ClosedPnlByGroupPayload> ClosedPnlByGroup;
    }

    private sealed class SlowStats
    {
        public SlowStats()
        {
            ClosedTotalsByCurrency = new Dictionary<string, Mt5PositionHistoryCurrencyTotal>(StringComparer.OrdinalIgnoreCase);
            ClosedTotalsByDay = new Dictionary<string, Dictionary<string, Mt5PositionHistoryCurrencyTotal>>(StringComparer.Ordinal);
            DepositorLogins = new HashSet<ulong>();
            TraderLogins = new HashSet<ulong>();
            ActiveTraderLogins = new HashSet<ulong>();
            MonthlyByDay = new List<MonthlyByDayPayload>();
        }

        public double TotalBalanceUsd;
        public double TotalCreditUsd;
        public double MonthlyDepositsUsd;
        public double MonthlyWithdrawalsUsd;
        public double MonthlyCobUsd;
        public double MonthlyVolumeUsd;
        public HashSet<ulong> DepositorLogins;
        public HashSet<ulong> TraderLogins;
        public HashSet<ulong> ActiveTraderLogins;
	        public Dictionary<string, Mt5PositionHistoryCurrencyTotal> ClosedTotalsByCurrency;
	        public Dictionary<string, Dictionary<string, Mt5PositionHistoryCurrencyTotal>> ClosedTotalsByDay;
	        public Mt5DailyClosedPnlResult ClosedPnl;
	        public List<MonthlyByDayPayload> MonthlyByDay;
	    }

	    private sealed class WdEquityBridgeConfig
	    {
	        public int RefreshSeconds;
	    }

	    private sealed class WdEquityPollingState
	    {
	        public Mt5LiveWdEquityZReport Report;
	        public Mt5DailyPnlCashReport DailyPnlCash;
	        public int AccountCount;
	        public DateTime RefreshedAtUtc;
	        public DateTime NextRefreshUtc;
	        public int RefreshSeconds;
	    }

    [DataContract]
    public sealed class PusherPayload
    {
        [DataMember] public double floating_pnl_usd { get; set; }
        [DataMember] public double closed_pnl_usd { get; set; }
        [DataMember] public int n_positions { get; set; }
        [DataMember] public int n_closing_deals { get; set; }
        [DataMember] public double volume_usd { get; set; }
        [DataMember] public double swap { get; set; }
        [DataMember] public double commission { get; set; }
        [DataMember] public double fee { get; set; }
        [DataMember] public double net_deposits { get; set; }
        [DataMember] public double deposits { get; set; }
        [DataMember] public double withdrawals { get; set; }
        [DataMember] public int n_traders { get; set; }
        [DataMember] public int n_active_traders { get; set; }
        [DataMember] public int n_depositors { get; set; }
        [DataMember] public int n_ftd { get; set; }
        [DataMember] public List<ClosedPnlByCurrencyPayload> closed_pnl_by_ccy { get; set; }
        [DataMember] public double balance { get; set; }
        [DataMember] public double credit { get; set; }
        [DataMember] public double equity { get; set; }
        [DataMember] public double wd_equity { get; set; }
        [DataMember] public double wd_equity_z { get; set; }
        [DataMember] public double wd_equity_legacy { get; set; }
        [DataMember] public double wd_equity_balance_usd { get; set; }
        [DataMember] public double wd_equity_floating_usd { get; set; }
        [DataMember] public double wd_equity_cumulative_bonus_usd { get; set; }
        [DataMember] public double wd_equity_pre_clamp_usd { get; set; }
        [DataMember] public double wd_equity_end { get; set; }
        [DataMember] public double wd_equity_start { get; set; }
        [DataMember] public double wd_equity_end_equity { get; set; }
        [DataMember] public double wd_equity_end_credits { get; set; }
        [DataMember] public double wd_equity_end_bonuses { get; set; }
        [DataMember] public double wd_equity_start_equity { get; set; }
        [DataMember] public double wd_equity_start_credits { get; set; }
        [DataMember] public double wd_equity_start_bonuses { get; set; }
        [DataMember] public int wd_equity_daily_rows { get; set; }
        [DataMember] public int wd_equity_bonus_deals { get; set; }
        [DataMember] public string wd_equity_mode { get; set; }
        [DataMember] public string wd_equity_source { get; set; }
        [DataMember] public string wd_equity_bonus_comment { get; set; }
        [DataMember] public string wd_equity_bonus_history_from { get; set; }
        [DataMember] public string wd_equity_refreshed_at { get; set; }
        [DataMember] public int wd_equity_refresh_seconds { get; set; }
        [DataMember] public int wd_equity_account_count { get; set; }
        [DataMember] public int wd_equity_raw_account_count { get; set; }
        [DataMember] public int wd_equity_skipped_zero_equity_count { get; set; }
        [DataMember] public int wd_equity_skipped_zero_balance_count { get; set; }
        [DataMember] public int wd_equity_skipped_excluded_group_count { get; set; }
        [DataMember] public int wd_equity_bonus_scope_login_count { get; set; }
        [DataMember] public int wd_equity_crm_matched_login_count { get; set; }
        [DataMember] public int wd_equity_crm_transaction_count { get; set; }
        [DataMember] public string wd_equity_crm_query_as_of { get; set; }
        [DataMember] public string wd_equity_missing_currency_rates { get; set; }
        [DataMember] public string wd_equity_summary { get; set; }
        [DataMember] public double daily_pnl_cash_usd { get; set; }
        [DataMember] public double monthly_closed_pnl { get; set; }
        [DataMember] public double monthly_net_deposits { get; set; }
        [DataMember] public double monthly_deposits { get; set; }
        [DataMember] public double monthly_withdrawals { get; set; }
        [DataMember] public double monthly_volume_usd { get; set; }
        [DataMember] public double monthly_swap { get; set; }
        [DataMember] public double monthly_commission { get; set; }
        [DataMember] public double monthly_fee { get; set; }
        [DataMember] public int monthly_n_traders { get; set; }
        [DataMember] public int monthly_n_active_traders { get; set; }
        [DataMember] public int monthly_n_depositors { get; set; }
        [DataMember] public List<MonthlyByDayPayload> monthly_by_day { get; set; }
        [DataMember] public List<ClosedPnlByCurrencyPayload> monthly_closed_pnl_by_ccy { get; set; }
        [DataMember] public int snap_login_count { get; set; }
        [DataMember] public string source { get; set; }
        [DataMember] public string group_mask { get; set; }
        [DataMember] public string pushed_at { get; set; }
        [DataMember] public List<BySymbolPayload> by_symbol { get; set; }
        [DataMember] public List<ByGroupPayload> by_group { get; set; }
        [DataMember] public List<ClosedPnlByGroupPayload> closed_pnl_by_group { get; set; }
        [DataMember] public string daily_closed_pnl_conversion_summary { get; set; }
        [DataMember] public string monthly_closed_pnl_conversion_summary { get; set; }
    }

    [DataContract]
    public sealed class ClosedPnlByCurrencyPayload
    {
        [DataMember] public string ccy { get; set; }
        [DataMember] public double profit { get; set; }
        [DataMember] public double swap { get; set; }
        [DataMember] public double commission { get; set; }
        [DataMember] public double fee { get; set; }
        [DataMember] public double usd_total { get; set; }
    }

    [DataContract]
    public sealed class MonthlyByDayPayload
    {
        [DataMember] public string date { get; set; }
        [DataMember] public double closed_pnl { get; set; }
    }

    [DataContract]
    public sealed class BySymbolPayload
    {
        [DataMember] public string symbol { get; set; }
        [DataMember] public int n_deals { get; set; }
        [DataMember] public int n_traders { get; set; }
        [DataMember] public double notional_usd { get; set; }
        [DataMember] public double notional_buy { get; set; }
        [DataMember] public double notional_sell { get; set; }
        [DataMember] public double swap { get; set; }
        [DataMember] public double commission { get; set; }
        [DataMember] public double fee { get; set; }
        [DataMember] public double pnl { get; set; }
    }

    [DataContract]
    public sealed class ByGroupPayload
    {
        [DataMember] public string groupname { get; set; }
        [DataMember] public int n_accounts { get; set; }
        [DataMember] public int n_depositors { get; set; }
        [DataMember] public double floating_pnl { get; set; }
        [DataMember] public double closed_pnl { get; set; }
        [DataMember] public double delta_floating { get; set; }
        [DataMember] public double net_deposits { get; set; }
        [DataMember] public double equity { get; set; }
        [DataMember] public double balance { get; set; }
    }

    [DataContract]
    public sealed class ClosedPnlByGroupPayload
    {
        [DataMember] public string group { get; set; }
        [DataMember] public double closed_pnl { get; set; }
    }

    public static int Main(string[] args)
    {
        var ci = CultureInfo.InvariantCulture;
        var settings = Mt5MonitorSettings.FromEnvironment();
        int intervalSeconds = ParsePositiveInt(Environment.GetEnvironmentVariable("CRO_INTERVAL"), 5);
        int slowEvery = ParsePositiveInt(Environment.GetEnvironmentVariable("CRO_SLOW_EVERY"), 12);
        WdEquityBridgeConfig wdEquityConfig = LoadWdEquityBridgeConfig();

        if (string.IsNullOrWhiteSpace(settings.Server) || string.IsNullOrWhiteSpace(settings.Password))
        {
            Console.Error.WriteLine("[pusher] MT5_SERVER + MT5_PASSWORD required.");
            return 2;
        }

        ParseExcludedGroups();

        MTRetCode initRes = SMTManagerAPIFactory.Initialize(settings.SdkLibsPath);
        if (initRes != MTRetCode.MT_RET_OK)
        {
            Console.Error.WriteLine("[pusher] Initialize failed: " + initRes);
            return 3;
        }

	        Console.Error.WriteLine(
	            string.Format(
	                ci,
	                "[pusher] group={0} interval={1}s slow_every={2} cycles wd_formula=balance_plus_floating_minus_crm_bonus wd_refresh={3}s",
	                settings.GroupMask,
	                intervalSeconds,
	                slowEvery,
	                wdEquityConfig.RefreshSeconds));

        CIMTManagerAPI manager = null;
        Dictionary<string, string> groupCurrencies = null;
        Dictionary<ulong, Mt5LoginContext> loginContexts = null;
	        IDictionary<string, Mt5UsdConversionRate> usdRates = null;
	        var closedPnlCalculator = new Mt5DailyClosedPnlCalculator();
	        var slow = new SlowStats();
	        WdEquityPollingState wdPolling = null;
	        int slowCycle = slowEvery;
	        int refreshCycle = 60;

        try
        {
            while (true)
            {
                try
                {
                    if (manager == null)
                    {
                        manager = ConnectWithRetry(settings);
                        groupCurrencies = Mt5MonitorCollector.LoadGroupCurrencies(manager, settings.GroupMask);
	                        loginContexts = Mt5MonitorCollector.LoadLoginContexts(manager, settings.GroupMask, groupCurrencies);
	                        usdRates = Mt5UsdRateLoader.LoadLiveRates(manager);
	                        slow = new SlowStats();
	                        wdPolling = null;
	                        slowCycle = slowEvery;
	                        refreshCycle = 60;
                        Console.Error.WriteLine(
                            string.Format(
                                ci,
                                "[pusher] connected. groups={0} logins={1} fx_rates={2}",
                                groupCurrencies.Count,
                                loginContexts.Count,
                                usdRates.Count));
                    }

                    if (--refreshCycle <= 0)
                    {
                        groupCurrencies = Mt5MonitorCollector.LoadGroupCurrencies(manager, settings.GroupMask);
                        loginContexts = Mt5MonitorCollector.LoadLoginContexts(manager, settings.GroupMask, groupCurrencies);
                        usdRates = Mt5UsdRateLoader.LoadLiveRates(manager);
                        refreshCycle = 60;
                        Console.Error.WriteLine("[pusher] refreshed login contexts and FX rates.");
                    }

                    DateTime nowUtc = DateTime.UtcNow;
                    DateTime dayStartUtc = nowUtc.Date;
                    DateTime monthStartUtc = new DateTime(nowUtc.Year, nowUtc.Month, 1, 0, 0, 0, DateTimeKind.Utc);
                    string todayKey = dayStartUtc.ToString("yyyy-MM-dd", ci);

                    FastStats fast = CollectFastStats(
                        manager,
                        settings,
                        groupCurrencies,
                        loginContexts,
                        usdRates,
                        dayStartUtc,
                        nowUtc,
                        closedPnlCalculator,
                        ci);

                    if (fast.PositionCount == 0 && fast.TraderLogins.Count == 0)
                    {
                        Console.Error.WriteLine("[pusher] sanity: 0 positions + 0 traders -- skipping");
	                        Thread.Sleep(intervalSeconds * 1000);
	                        continue;
	                    }

	                    if (IsWdRefreshDue(wdPolling, nowUtc))
	                    {
	                        DateTime wdRefreshStartedUtc = DateTime.UtcNow;
	                        Console.Error.WriteLine("[pusher] wd refresh: polling live Trading Accounts...");
	                        try
	                        {
	                            wdPolling = RefreshWdEquityZ(
	                                manager,
	                                settings,
	                                groupCurrencies,
	                                loginContexts,
	                                usdRates,
	                                wdEquityConfig,
	                                nowUtc,
	                                ci);

	                            TimeSpan wdRefreshDuration = DateTime.UtcNow - wdRefreshStartedUtc;
	                            Console.Error.WriteLine(
	                                string.Format(
	                                    ci,
	                                    "[pusher] wd refresh: raw={0:N0} included={1:N0} balance={2:N0} floating={3:N0} bonuses={4:N0} wdz={5:N0} took={6:N1}s",
	                                    wdPolling.Report != null ? wdPolling.Report.RawAccountCount : 0,
	                                    wdPolling.AccountCount,
	                                    wdPolling.Report != null ? wdPolling.Report.BalanceUsdTotal : 0.0,
	                                    wdPolling.Report != null ? wdPolling.Report.FloatingPnlUsdTotal : 0.0,
	                                    wdPolling.Report != null ? wdPolling.Report.CumulativeBonusUsd : 0.0,
	                                    wdPolling.Report != null ? wdPolling.Report.WdEquityZUsd : 0.0,
	                                    wdRefreshDuration.TotalSeconds));
	                        }
	                        catch (Exception ex)
	                        {
	                            int retrySeconds = Math.Max(1, Math.Min(60, wdEquityConfig.RefreshSeconds));
	                            Console.Error.WriteLine("[pusher] wd refresh failed: " + ex.Message + " -- keeping previous cached value");
	                            if (wdPolling == null)
	                                wdPolling = new WdEquityPollingState();
	                            wdPolling.RefreshSeconds = wdEquityConfig.RefreshSeconds;
	                            wdPolling.NextRefreshUtc = nowUtc.AddSeconds(retrySeconds);
	                        }
	                    }

	                    if (++slowCycle >= slowEvery)
	                    {
                        slowCycle = 0;
                        slow = CollectSlowStats(
                            manager,
                            settings,
                            groupCurrencies,
                            loginContexts,
                            usdRates,
	                            monthStartUtc,
	                            nowUtc,
	                            closedPnlCalculator,
	                            fast.DepositorLogins,
	                            todayKey,
	                            ci);

                        Console.Error.WriteLine(
                            string.Format(
                                ci,
                                "[pusher] slow: bal={0:N0} cred={1:N0} mth_closed={2:N0} wdz={3:N0} ftd={4}",
	                                slow.TotalBalanceUsd,
	                                slow.TotalCreditUsd,
	                                slow.ClosedPnl != null ? slow.ClosedPnl.TotalClosedPnlUsd : 0.0,
	                                wdPolling != null && wdPolling.Report != null ? wdPolling.Report.WdEquityZUsd : 0.0,
	                                _ftdToday));
	                    }

	                    PusherPayload payload = BuildPayload(settings, loginContexts, fast, slow, wdPolling, wdEquityConfig, nowUtc, ci);
                    Console.WriteLine(SerializeJson(payload));
                    Console.Out.Flush();
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine("[pusher] ERROR: " + ex.Message + " -- reconnecting");
                    Mt5MonitorCollector.Disconnect(manager);
                    manager = null;
                    groupCurrencies = null;
                    loginContexts = null;
                    usdRates = null;
                    Thread.Sleep(5000);
                    continue;
                }

                Thread.Sleep(intervalSeconds * 1000);
            }
        }
        finally
        {
            Mt5MonitorCollector.Disconnect(manager);
            SMTManagerAPIFactory.Shutdown();
        }
    }

    private static CIMTManagerAPI ConnectWithRetry(Mt5MonitorSettings settings)
    {
        while (true)
        {
            CIMTManagerAPI manager = Mt5MonitorCollector.Connect(
                settings.Server,
                settings.Login,
                settings.Password,
                message => Console.Error.WriteLine("[pusher] " + message));

            if (manager != null)
                return manager;

            Console.Error.WriteLine("[pusher] retrying connection in 5 seconds...");
            Thread.Sleep(5000);
        }
    }

    private static FastStats CollectFastStats(
        CIMTManagerAPI manager,
        Mt5MonitorSettings settings,
        Dictionary<string, string> groupCurrencies,
        Dictionary<ulong, Mt5LoginContext> loginContexts,
        IDictionary<string, Mt5UsdConversionRate> usdRates,
        DateTime dayStartUtc,
        DateTime nowUtc,
        Mt5DailyClosedPnlCalculator calculator,
        CultureInfo ci)
    {
        var stats = new FastStats();

        foreach (KeyValuePair<string, string> group in groupCurrencies)
        {
            var positions = manager.PositionCreateArray();
            try
            {
                MTRetCode result = manager.PositionGetByGroup(group.Key, positions);
                if (result != MTRetCode.MT_RET_OK)
                    throw new InvalidOperationException("PositionGetByGroup failed for " + group.Key + ": " + result);

                double groupFloatingUsd = 0.0;
                int groupPositionCount = (int)positions.Total();
                stats.PositionCount += groupPositionCount;

                for (uint i = 0; i < positions.Total(); i++)
                {
                    var position = positions.Next(i);
                    string positionCurrency = ResolveLoginCurrency(position.Login(), loginContexts, groupCurrencies, group.Key);
                    double native = position.Profit() + position.Storage();
                    groupFloatingUsd += ConvertNativeToUsd(native, positionCurrency, usdRates, position.RateProfit());
                }

                stats.FloatingPnlUsd += groupFloatingUsd;
                if (groupPositionCount > 0)
                {
                    stats.GroupFloating.Add(
                        new GroupFloatingAccumulator
                        {
                            GroupName = group.Key,
                            PositionCount = groupPositionCount,
                            FloatingPnlUsd = groupFloatingUsd
                        });
                }
            }
            finally
            {
                positions.Dispose();
            }
        }

        var deals = manager.DealCreateArray();
        try
        {
            MTRetCode dealResult = manager.DealRequestByGroup(
                settings.GroupMask,
                SMTTime.FromDateTime(dayStartUtc),
                SMTTime.FromDateTime(nowUtc),
                deals);

            if (dealResult != MTRetCode.MT_RET_OK)
                throw new InvalidOperationException("DealRequestByGroup failed: " + dealResult + " (" + (uint)dealResult + ")");

            for (uint i = 0; i < deals.Total(); i++)
            {
                var deal = deals.Next(i);
                ulong login = deal.Login();
                string groupName = ResolveLoginGroup(login, loginContexts);
                bool excluded = IsExcludedGroup(groupName);
                string currency = ResolveLoginCurrency(login, loginContexts, groupCurrencies, groupName);
                int currencyDigits = NormalizeCurrencyDigits((int)deal.DigitsCurrency());

                if (deal.Action() == ActionBuy || deal.Action() == ActionSell)
                {
                    if (deal.Entry() != EntryIn)
                    {
                        Dictionary<string, Mt5PositionHistoryCurrencyTotal> groupTotals;
                        if (!stats.ClosedTotalsByGroup.TryGetValue(groupName ?? string.Empty, out groupTotals))
                        {
                            groupTotals = new Dictionary<string, Mt5PositionHistoryCurrencyTotal>(StringComparer.OrdinalIgnoreCase);
                            stats.ClosedTotalsByGroup[groupName ?? string.Empty] = groupTotals;
                        }

                        AccumulateClosedComponents(
                            groupTotals,
                            currency,
                            currencyDigits,
                            deal.Profit(),
                            deal.Storage(),
                            deal.Commission(),
                            deal.Fee());

                        if (!excluded)
                        {
                            stats.ClosingDealCount++;
                            AccumulateClosedComponents(
                                stats.ClosedTotalsByCurrency,
                                currency,
                                currencyDigits,
                                deal.Profit(),
                                deal.Storage(),
                                deal.Commission(),
                                deal.Fee());
                        }
                    }

                    if (excluded)
                        continue;

                    stats.TraderLogins.Add(login);
                    if (deal.Entry() == EntryIn)
                        stats.ActiveTraderLogins.Add(login);

                    double lots = ToDisplayLots(deal.Volume());
                    double notionalUsd = NotionalUsd(lots, deal.ContractSize(), deal.Price(), deal.Symbol());
                    stats.VolumeUsd += notionalUsd;

                    SymbolAccumulator symbol;
                    string symbolName = deal.Symbol() ?? string.Empty;
                    if (!stats.Symbols.TryGetValue(symbolName, out symbol))
                    {
                        symbol = new SymbolAccumulator { Symbol = symbolName };
                        stats.Symbols[symbolName] = symbol;
                    }

                    symbol.DealCount++;
                    symbol.Traders.Add(login);
                    symbol.NotionalUsd += notionalUsd;
                    if (deal.Action() == ActionBuy)
                        symbol.NotionalBuyUsd += notionalUsd;
                    else
                        symbol.NotionalSellUsd += notionalUsd;

                    symbol.SwapUsd += ConvertNativeToUsd(deal.Storage(), currency, usdRates, deal.RateProfit());
                    symbol.CommissionUsd += ConvertNativeToUsd(deal.Commission(), currency, usdRates, deal.RateProfit());
                    symbol.FeeUsd += ConvertNativeToUsd(deal.Fee(), currency, usdRates, deal.RateProfit());
                    symbol.PnlUsd += ConvertNativeToUsd(
                        deal.Profit() + deal.Storage() + deal.Commission() + deal.Fee(),
                        currency,
                        usdRates,
                        deal.RateProfit());
                }
                else if (deal.Action() == ActionBalance)
                {
                    if (excluded)
                        continue;
                    if (ShouldSkipBalanceDeal(deal.Comment(), false))
                        continue;

                    double amountUsd = ConvertNativeToUsd(deal.Profit(), currency, usdRates, deal.RateProfit());
                    if (amountUsd > 0)
                    {
                        stats.DepositsUsd += amountUsd;
                        stats.DepositorLogins.Add(login);
                    }
                    else
                    {
                        stats.WithdrawalsUsd += amountUsd;
                    }
                }
            }
        }
        finally
        {
            deals.Dispose();
        }

        stats.ClosedPnl = calculator.Calculate(stats.ClosedTotalsByCurrency.Values, usdRates);
        stats.ClosedPnlByGroup = stats.ClosedTotalsByGroup
            .Select(
                pair => new ClosedPnlByGroupPayload
                {
                    group = pair.Key,
                    closed_pnl = calculator.Calculate(pair.Value.Values, usdRates).TotalClosedPnlUsd
                })
            .Where(item => Math.Abs(item.closed_pnl) > 0.0 || !string.IsNullOrWhiteSpace(item.group))
            .OrderByDescending(item => Math.Abs(item.closed_pnl))
            .Take(100)
            .ToList();

        return stats;
    }

    private static SlowStats CollectSlowStats(
        CIMTManagerAPI manager,
        Mt5MonitorSettings settings,
        Dictionary<string, string> groupCurrencies,
        Dictionary<ulong, Mt5LoginContext> loginContexts,
	        IDictionary<string, Mt5UsdConversionRate> usdRates,
	        DateTime monthStartUtc,
	        DateTime nowUtc,
	        Mt5DailyClosedPnlCalculator calculator,
	        HashSet<ulong> todayDepositors,
	        string todayKey,
	        CultureInfo ci)
    {
        var stats = new SlowStats();

        var users = manager.UserCreateArray();
        try
        {
            MTRetCode result = manager.UserRequestArray(settings.GroupMask, users);
            if (result != MTRetCode.MT_RET_OK)
                throw new InvalidOperationException("UserRequestArray failed: " + result + " (" + (uint)result + ")");

            for (uint i = 0; i < users.Total(); i++)
            {
                var user = users.Next(i);
                string currency = ResolveUserCurrency(user.Group() ?? string.Empty, groupCurrencies);
                stats.TotalBalanceUsd += ConvertNativeToUsd(user.Balance(), currency, usdRates, 1.0);
                stats.TotalCreditUsd += ConvertNativeToUsd(user.Credit(), currency, usdRates, 1.0);
            }
        }
        finally
        {
            users.Release();
        }

        var deals = manager.DealCreateArray();
        try
        {
            MTRetCode result = manager.DealRequestByGroup(
                settings.GroupMask,
                SMTTime.FromDateTime(monthStartUtc),
                SMTTime.FromDateTime(nowUtc),
                deals);

            if (result != MTRetCode.MT_RET_OK)
                throw new InvalidOperationException("DealRequestByGroup failed: " + result + " (" + (uint)result + ")");

            for (uint i = 0; i < deals.Total(); i++)
            {
                var deal = deals.Next(i);
                ulong login = deal.Login();
                string groupName = ResolveLoginGroup(login, loginContexts);
                bool excluded = IsExcludedGroup(groupName);
                string currency = ResolveLoginCurrency(login, loginContexts, groupCurrencies, groupName);
                int currencyDigits = NormalizeCurrencyDigits((int)deal.DigitsCurrency());

                if (deal.Action() == ActionBuy || deal.Action() == ActionSell)
                {
                    if (deal.Entry() != EntryIn)
                    {
                        if (!excluded)
                        {
                            AccumulateClosedComponents(
                                stats.ClosedTotalsByCurrency,
                                currency,
                                currencyDigits,
                                deal.Profit(),
                                deal.Storage(),
                                deal.Commission(),
                                deal.Fee());

                            string dayKey = SMTTime.ToDateTime(deal.Time()).ToString("yyyy-MM-dd", ci);
                            Dictionary<string, Mt5PositionHistoryCurrencyTotal> dayTotals;
                            if (!stats.ClosedTotalsByDay.TryGetValue(dayKey, out dayTotals))
                            {
                                dayTotals = new Dictionary<string, Mt5PositionHistoryCurrencyTotal>(StringComparer.OrdinalIgnoreCase);
                                stats.ClosedTotalsByDay[dayKey] = dayTotals;
                            }

                            AccumulateClosedComponents(
                                dayTotals,
                                currency,
                                currencyDigits,
                                deal.Profit(),
                                deal.Storage(),
                                deal.Commission(),
                                deal.Fee());
                        }
                    }

                    if (excluded)
                        continue;

                    stats.TraderLogins.Add(login);
                    if (deal.Entry() == EntryIn)
                        stats.ActiveTraderLogins.Add(login);

                    double lots = ToDisplayLots(deal.Volume());
                    stats.MonthlyVolumeUsd += NotionalUsd(lots, deal.ContractSize(), deal.Price(), deal.Symbol());
                }
                else if (deal.Action() == ActionBalance)
                {
                    if (excluded)
                        continue;

                    bool isBonus = ShouldSkipBalanceDeal(deal.Comment(), true);
                    double amountUsd = ConvertNativeToUsd(deal.Profit(), currency, usdRates, deal.RateProfit());

                    if (isBonus)
                    {
                        stats.MonthlyCobUsd += amountUsd;
                        continue;
                    }

                    if (ShouldSkipBalanceDeal(deal.Comment(), false))
                        continue;

                    if (amountUsd > 0)
                    {
                        stats.MonthlyDepositsUsd += amountUsd;
                        stats.DepositorLogins.Add(login);
                    }
                    else
                    {
                        stats.MonthlyWithdrawalsUsd += amountUsd;
                    }
                }
            }
        }
        finally
        {
            deals.Dispose();
        }

        if (!_knownDepositorsLoaded)
            SeedKnownDepositors(manager, settings, monthStartUtc.Date, loginContexts, ci);

        UpdateFtd(todayDepositors, todayKey);

        stats.ClosedPnl = calculator.Calculate(stats.ClosedTotalsByCurrency.Values, usdRates);
	        stats.MonthlyByDay = stats.ClosedTotalsByDay
	            .OrderBy(pair => pair.Key, StringComparer.Ordinal)
	            .Select(
                pair => new MonthlyByDayPayload
                {
	                    date = pair.Key,
	                    closed_pnl = calculator.Calculate(pair.Value.Values, usdRates).TotalClosedPnlUsd
	                })
	            .ToList();

	        return stats;
	    }

    private static void SeedKnownDepositors(
        CIMTManagerAPI manager,
        Mt5MonitorSettings settings,
        DateTime dayStartUtc,
        Dictionary<ulong, Mt5LoginContext> loginContexts,
        CultureInfo ci)
    {
        DateTime yearStartUtc = new DateTime(dayStartUtc.Year, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        DateTime yesterdayEndUtc = dayStartUtc.AddSeconds(-1);
        if (yesterdayEndUtc < yearStartUtc)
        {
            _knownDepositorsLoaded = true;
            return;
        }

        Console.Error.WriteLine("[pusher] seeding FTD known-depositors from YTD deals...");

        var deals = manager.DealCreateArray();
        try
        {
            MTRetCode result = manager.DealRequestByGroup(
                settings.GroupMask,
                SMTTime.FromDateTime(yearStartUtc),
                SMTTime.FromDateTime(yesterdayEndUtc),
                deals);

            if (result == MTRetCode.MT_RET_OK)
            {
                for (uint i = 0; i < deals.Total(); i++)
                {
                    var deal = deals.Next(i);
                    if (deal.Action() != ActionBalance)
                        continue;
                    if (ShouldSkipBalanceDeal(deal.Comment(), false))
                        continue;
                    string groupName = ResolveLoginGroup(deal.Login(), loginContexts);
                    if (IsExcludedGroup(groupName))
                        continue;
                    if (deal.Profit() > 0)
                        KnownDepositors.Add(deal.Login());
                }
            }
        }
        finally
        {
            deals.Dispose();
        }

        _knownDepositorsLoaded = true;
        Console.Error.WriteLine(
            string.Format(
                ci,
                "[pusher] FTD seed done: {0} known depositors YTD.",
                KnownDepositors.Count));
    }

    private static void UpdateFtd(HashSet<ulong> todayDepositors, string todayKey)
    {
        if (_knownDepositorsDate == todayKey)
            return;

        _ftdToday = 0;
        foreach (ulong login in todayDepositors)
        {
            if (!KnownDepositors.Contains(login))
                _ftdToday++;
        }

        foreach (ulong login in todayDepositors)
            KnownDepositors.Add(login);

        _knownDepositorsDate = todayKey;
    }

	    private static PusherPayload BuildPayload(
	        Mt5MonitorSettings settings,
	        Dictionary<ulong, Mt5LoginContext> loginContexts,
	        FastStats fast,
	        SlowStats slow,
	        WdEquityPollingState wdPolling,
	        WdEquityBridgeConfig wdEquityConfig,
	        DateTime nowUtc,
	        CultureInfo ci)
	    {
	        Mt5DailyClosedPnlResult dailyClosed = fast.ClosedPnl ?? new Mt5DailyClosedPnlResult();
	        Mt5DailyClosedPnlResult monthlyClosed = slow.ClosedPnl ?? new Mt5DailyClosedPnlResult();
	        Mt5LiveWdEquityZReport wdReport = wdPolling != null && wdPolling.Report != null
	            ? wdPolling.Report
	            : new Mt5LiveWdEquityZReport();

        double dailySwap = dailyClosed.CurrencyBreakdowns.Sum(item => item.SwapUsd);
        double dailyCommission = dailyClosed.CurrencyBreakdowns.Sum(item => item.CommissionUsd);
        double dailyFee = dailyClosed.CurrencyBreakdowns.Sum(item => item.FeeUsd);
        double monthlySwap = monthlyClosed.CurrencyBreakdowns.Sum(item => item.SwapUsd);
        double monthlyCommission = monthlyClosed.CurrencyBreakdowns.Sum(item => item.CommissionUsd);
	        double monthlyFee = monthlyClosed.CurrencyBreakdowns.Sum(item => item.FeeUsd);

	        double totalEquity = slow.TotalBalanceUsd + slow.TotalCreditUsd + fast.FloatingPnlUsd;
	        double legacyWdEquity = Math.Max(0.0, slow.TotalBalanceUsd + fast.FloatingPnlUsd - slow.MonthlyCobUsd);
	        bool hasWdReport = wdPolling != null && wdPolling.Report != null && wdReport.GeneratedAt != default(DateTime);
	        double wdEquity = hasWdReport ? wdReport.WdEquityZUsd : 0.0;

        var bySymbol = fast.Symbols.Values
            .OrderByDescending(item => Math.Abs(item.NotionalUsd))
            .Take(30)
            .Select(
                item => new BySymbolPayload
                {
                    symbol = item.Symbol,
                    n_deals = item.DealCount,
                    n_traders = item.Traders.Count,
                    notional_usd = item.NotionalUsd,
                    notional_buy = item.NotionalBuyUsd,
                    notional_sell = item.NotionalSellUsd,
                    swap = item.SwapUsd,
                    commission = item.CommissionUsd,
                    fee = item.FeeUsd,
                    pnl = item.PnlUsd
                })
            .ToList();

        var byGroup = fast.GroupFloating
            .OrderByDescending(item => Math.Abs(item.FloatingPnlUsd))
            .Select(
                item =>
                {
                    double closedPnl = 0.0;
                    ClosedPnlByGroupPayload groupClosed = fast.ClosedPnlByGroup
                        .FirstOrDefault(group => string.Equals(group.group, item.GroupName, StringComparison.OrdinalIgnoreCase));
                    if (groupClosed != null)
                        closedPnl = groupClosed.closed_pnl;

                    return new ByGroupPayload
                    {
                        groupname = item.GroupName,
                        n_accounts = item.PositionCount,
                        n_depositors = 0,
                        floating_pnl = item.FloatingPnlUsd,
                        closed_pnl = closedPnl,
                        delta_floating = item.FloatingPnlUsd,
                        net_deposits = 0.0,
                        equity = 0.0,
                        balance = 0.0
                    };
                })
            .ToList();

        return new PusherPayload
        {
            floating_pnl_usd = fast.FloatingPnlUsd,
            closed_pnl_usd = dailyClosed.TotalClosedPnlUsd,
            n_positions = fast.PositionCount,
            n_closing_deals = fast.ClosingDealCount,
            volume_usd = fast.VolumeUsd,
            swap = dailySwap,
            commission = dailyCommission,
            fee = dailyFee,
            net_deposits = fast.DepositsUsd + fast.WithdrawalsUsd,
            deposits = fast.DepositsUsd,
            withdrawals = fast.WithdrawalsUsd,
            n_traders = fast.TraderLogins.Count,
            n_active_traders = fast.ActiveTraderLogins.Count,
            n_depositors = fast.DepositorLogins.Count,
            n_ftd = _ftdToday,
            closed_pnl_by_ccy = ToCurrencyPayloads(dailyClosed),
            balance = slow.TotalBalanceUsd,
            credit = slow.TotalCreditUsd,
            equity = totalEquity,
            wd_equity = wdEquity,
            wd_equity_z = wdEquity,
            wd_equity_legacy = legacyWdEquity,
            wd_equity_balance_usd = wdReport.BalanceUsdTotal,
            wd_equity_floating_usd = wdReport.FloatingPnlUsdTotal,
            wd_equity_cumulative_bonus_usd = wdReport.CumulativeBonusUsd,
            wd_equity_pre_clamp_usd = wdReport.PreClampWdEquityUsd,
            wd_equity_end = wdReport.WdEquityZUsd,
            wd_equity_start = 0.0,
            wd_equity_end_equity = wdReport.BalanceUsdTotal + wdReport.FloatingPnlUsdTotal,
            wd_equity_end_credits = 0.0,
            wd_equity_end_bonuses = wdReport.CumulativeBonusUsd,
            wd_equity_start_equity = 0.0,
            wd_equity_start_credits = 0.0,
	            wd_equity_start_bonuses = 0.0,
	            wd_equity_daily_rows = 0,
	            wd_equity_bonus_deals = wdReport.CrmMatchedTransactionCount,
	            wd_equity_mode = "live_raw_crm",
	            wd_equity_source = hasWdReport ? wdReport.Source : "pending_live_refresh",
	            wd_equity_bonus_comment = "CRM approved Bonus/FRF Commission rows net of approved cancellations",
	            wd_equity_bonus_history_from = "all-time",
	            wd_equity_refreshed_at = hasWdReport
	                ? wdPolling.RefreshedAtUtc.ToString("yyyy-MM-ddTHH:mm:ss.fffZ", ci)
	                : string.Empty,
	            wd_equity_refresh_seconds = wdPolling != null && wdPolling.RefreshSeconds > 0
	                ? wdPolling.RefreshSeconds
	                : wdEquityConfig.RefreshSeconds,
	            wd_equity_account_count = wdPolling != null ? wdPolling.AccountCount : 0,
	            wd_equity_raw_account_count = wdReport.RawAccountCount,
	            wd_equity_skipped_zero_equity_count = wdReport.SkippedZeroEquityCount,
	            wd_equity_skipped_zero_balance_count = wdReport.SkippedZeroBalanceCount,
	            wd_equity_skipped_excluded_group_count = wdReport.SkippedExcludedGroupCount,
	            wd_equity_bonus_scope_login_count = wdReport.BonusScopeLoginCount,
	            wd_equity_crm_matched_login_count = wdReport.CrmMatchedLoginCount,
	            wd_equity_crm_transaction_count = wdReport.CrmMatchedTransactionCount,
	            wd_equity_crm_query_as_of = hasWdReport
	                ? wdReport.CrmQueryAsOfUtc.ToString("yyyy-MM-ddTHH:mm:ss.fffZ", ci)
	                : string.Empty,
	            wd_equity_missing_currency_rates = wdReport.MissingCurrencyRates != null
	                ? string.Join(", ", wdReport.MissingCurrencyRates)
	                : string.Empty,
	            wd_equity_summary = hasWdReport
	                ? wdReport.CalculationSummary
	                : "WD Equity Z pending first live Trading Accounts refresh.",
            monthly_closed_pnl = monthlyClosed.TotalClosedPnlUsd,
            monthly_net_deposits = slow.MonthlyDepositsUsd + slow.MonthlyWithdrawalsUsd,
            monthly_deposits = slow.MonthlyDepositsUsd,
            monthly_withdrawals = slow.MonthlyWithdrawalsUsd,
            monthly_volume_usd = slow.MonthlyVolumeUsd,
            monthly_swap = monthlySwap,
            monthly_commission = monthlyCommission,
            monthly_fee = monthlyFee,
            monthly_n_traders = slow.TraderLogins.Count,
            monthly_n_active_traders = slow.ActiveTraderLogins.Count,
            monthly_n_depositors = slow.DepositorLogins.Count,
            monthly_by_day = slow.MonthlyByDay,
            monthly_closed_pnl_by_ccy = ToCurrencyPayloads(monthlyClosed),
            snap_login_count = loginContexts != null ? loginContexts.Count : 0,
            source = "AN100",
            group_mask = settings.GroupMask,
            pushed_at = nowUtc.ToString("yyyy-MM-ddTHH:mm:ss.fffZ", ci),
            by_symbol = bySymbol,
            by_group = byGroup,
            closed_pnl_by_group = fast.ClosedPnlByGroup,
            daily_closed_pnl_conversion_summary = dailyClosed.ConversionSummary,
            monthly_closed_pnl_conversion_summary = monthlyClosed.ConversionSummary
        };
    }

    private static List<ClosedPnlByCurrencyPayload> ToCurrencyPayloads(Mt5DailyClosedPnlResult result)
    {
        if (result == null || result.CurrencyBreakdowns == null)
            return new List<ClosedPnlByCurrencyPayload>();

        return result.CurrencyBreakdowns
            .OrderByDescending(item => Math.Abs(item.ClosedPnlUsd))
            .Select(
                item => new ClosedPnlByCurrencyPayload
                {
                    ccy = item.Currency,
                    profit = item.ProfitNative,
                    swap = item.SwapNative,
                    commission = item.CommissionNative,
                    fee = item.FeeNative,
                    usd_total = item.ClosedPnlUsd
                })
            .ToList();
    }

    private static void AccumulateClosedComponents(
        IDictionary<string, Mt5PositionHistoryCurrencyTotal> totals,
        string currency,
        int currencyDigits,
        double profit,
        double swap,
        double commission,
        double fee)
    {
        string effectiveCurrency = string.IsNullOrWhiteSpace(currency) ? "USD" : currency;
        Mt5PositionHistoryCurrencyTotal total;
        if (!totals.TryGetValue(effectiveCurrency, out total))
        {
            total = new Mt5PositionHistoryCurrencyTotal
            {
                Currency = effectiveCurrency,
                CurrencyDigits = NormalizeCurrencyDigits(currencyDigits)
            };
            totals[effectiveCurrency] = total;
        }

        total.CurrencyDigits = Math.Max(total.CurrencyDigits, NormalizeCurrencyDigits(currencyDigits));
        total.Profit = MoneyAdd(total.Profit, profit, total.CurrencyDigits);
        total.Swap = MoneyAdd(total.Swap, swap, total.CurrencyDigits);
        total.Commission = MoneyAdd(total.Commission, commission, total.CurrencyDigits);
        total.Fee = MoneyAdd(total.Fee, fee, total.CurrencyDigits);
    }

	    private static double ConvertNativeToUsd(
	        double nativeAmount,
	        string currency,
        IDictionary<string, Mt5UsdConversionRate> usdRates,
        double fallbackRateProfit)
    {
        if (nativeAmount == 0.0)
            return 0.0;

        string effectiveCurrency = string.IsNullOrWhiteSpace(currency) ? "USD" : currency;
        Mt5UsdConversionRate rate;
        if (usdRates != null &&
            usdRates.TryGetValue(effectiveCurrency, out rate) &&
            rate != null)
        {
            double usdRate = nativeAmount >= 0.0 ? rate.PositiveToUsd : rate.NegativeToUsd;
            if (usdRate > 0.0)
                return nativeAmount * usdRate;
        }

	        return ToUsd(nativeAmount, fallbackRateProfit);
	    }

	    private static double ConvertNativeToUsdTracked(
	        double nativeAmount,
	        string currency,
	        IDictionary<string, Mt5UsdConversionRate> usdRates,
	        ISet<string> missingCurrencies)
	    {
	        if (nativeAmount == 0.0)
	            return 0.0;

	        string effectiveCurrency = string.IsNullOrWhiteSpace(currency) ? "USD" : currency;
	        Mt5UsdConversionRate rate;
	        if (usdRates != null &&
	            usdRates.TryGetValue(effectiveCurrency, out rate) &&
	            rate != null)
	        {
	            double usdRate = nativeAmount >= 0.0 ? rate.PositiveToUsd : rate.NegativeToUsd;
	            if (usdRate > 0.0)
	                return nativeAmount * usdRate;
	        }

	        if (!string.Equals(effectiveCurrency, "USD", StringComparison.OrdinalIgnoreCase) &&
	            missingCurrencies != null)
	        {
	            missingCurrencies.Add(effectiveCurrency);
	        }

	        return ToUsd(nativeAmount, 1.0);
	    }

	    private static bool ShouldSkipBalanceDeal(string comment, bool onlyBonus)
	    {
        string lowered = (comment ?? string.Empty).ToLowerInvariant();
        bool isBonus = lowered.Contains("bonus");
        if (onlyBonus)
            return isBonus;

        return isBonus || lowered.Contains("internal") || lowered.Contains("transfer");
    }

    private static bool IsExcludedGroup(string groupName)
    {
        return !string.IsNullOrWhiteSpace(groupName) && ExcludedGroups.Contains(groupName);
    }

    private static string ResolveLoginCurrency(
        ulong login,
        Dictionary<ulong, Mt5LoginContext> loginContexts,
        Dictionary<string, string> groupCurrencies,
        string fallbackGroup)
    {
        Mt5LoginContext context;
        if (loginContexts != null && loginContexts.TryGetValue(login, out context))
        {
            if (!string.IsNullOrWhiteSpace(context.Currency))
                return context.Currency;

            if (!string.IsNullOrWhiteSpace(context.Group))
                return ResolveUserCurrency(context.Group, groupCurrencies);
        }

        return ResolveUserCurrency(fallbackGroup, groupCurrencies);
    }

    private static string ResolveLoginGroup(ulong login, Dictionary<ulong, Mt5LoginContext> loginContexts)
    {
        Mt5LoginContext context;
        if (loginContexts != null && loginContexts.TryGetValue(login, out context))
            return context.Group ?? string.Empty;
        return string.Empty;
    }

    private static string ResolveUserCurrency(string groupName, Dictionary<string, string> groupCurrencies)
    {
        string currency;
        if (!string.IsNullOrWhiteSpace(groupName) &&
            groupCurrencies != null &&
            groupCurrencies.TryGetValue(groupName, out currency) &&
            !string.IsNullOrWhiteSpace(currency))
            return currency;

        return "USD";
    }

	    private static WdEquityPollingState RefreshWdEquityZ(
	        CIMTManagerAPI manager,
	        Mt5MonitorSettings settings,
	        Dictionary<string, string> groupCurrencies,
        Dictionary<ulong, Mt5LoginContext> loginContexts,
        IDictionary<string, Mt5UsdConversionRate> usdRates,
        WdEquityBridgeConfig wdEquityConfig,
        DateTime nowUtc,
	        CultureInfo ci)
	    {
	        DateTime reportDate = nowUtc.Date;
	        var silentWriter = new Action<string>(_ => { });

	        var liveRequest = new Mt5LiveWdEquityZRequest
	        {
	            AsOfUtc = nowUtc,
	            FilterZeroEquityAndBalance = true,
	            BonusScopePositiveBalanceOnly = true,
	            IncludeCrmBonusLoginRows = false,
	            ExcludedGroups = ExcludedGroups
	                .OrderBy(value => value, StringComparer.OrdinalIgnoreCase)
	                .ToList()
	        };

	        Mt5LiveWdEquityZReport report = Mt5LiveWdEquityZGenerator.Generate(
	            settings,
	            liveRequest,
	            silentWriter);

	        Mt5MonitorCollector.DailyPnlCashNetDepositCollection netDepositCollection =
	            Mt5MonitorCollector.CollectDailyPnlCashNetDeposits(
	                manager,
	                groupCurrencies,
	                loginContexts,
	                settings.GroupMask,
	                reportDate,
	                null,
	                false,
	                silentWriter);

	        var dailyPnlCashInputs = new Mt5DailyPnlCashInputs
	        {
	            ReportDate = reportDate,
	            EndEquityUsd = report.BalanceUsdTotal + report.FloatingPnlUsdTotal,
	            EndCreditsUsd = 0.0,
	            EndProtectedBonusesUsd = report.CumulativeBonusUsd,
	            StartEquityUsd = 0.0,
	            StartCreditsUsd = 0.0,
	            StartProtectedBonusesUsd = 0.0,
	            NetDepositsUsd = netDepositCollection.NetDepositsUsd,
	            MissingCurrencyRates = report.MissingCurrencyRates
	        };
	        Mt5DailyPnlCashReport dailyPnlCash = new Mt5DailyPnlCashCalculator().Calculate(dailyPnlCashInputs);

	        return new WdEquityPollingState
	        {
	            Report = report,
	            DailyPnlCash = dailyPnlCash,
	            AccountCount = report.IncludedAccountCount,
	            RefreshedAtUtc = nowUtc,
	            NextRefreshUtc = nowUtc.AddSeconds(wdEquityConfig.RefreshSeconds),
	            RefreshSeconds = wdEquityConfig.RefreshSeconds
	        };
	    }

	    private static bool IsWdRefreshDue(WdEquityPollingState state, DateTime nowUtc)
	    {
	        return state == null ||
	            state.Report == null ||
	            state.Report.GeneratedAt == default(DateTime) ||
	            nowUtc >= state.NextRefreshUtc;
	    }

    private static void ParseExcludedGroups()
    {
        ExcludedGroups.Clear();
        string raw = Environment.GetEnvironmentVariable("CRO_EXCLUDE_GROUPS") ?? string.Empty;
        foreach (string token in raw.Split(','))
        {
            string trimmed = token.Trim();
            if (trimmed.Length > 0)
                ExcludedGroups.Add(trimmed);
        }

        if (ExcludedGroups.Count > 0)
        {
            Console.Error.WriteLine("[pusher] excluding groups: " + string.Join(", ", ExcludedGroups));
        }
    }

    private static WdEquityBridgeConfig LoadWdEquityBridgeConfig()
    {
	        return new WdEquityBridgeConfig
	        {
	            RefreshSeconds = ParsePositiveInt(Environment.GetEnvironmentVariable("CRO_WD_REFRESH_SECONDS"), 900)
	        };
	    }

    private static int ParsePositiveInt(string text, int fallback)
    {
        int value;
        if (int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out value) && value > 0)
            return value;
        return fallback;
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

    private static double ToDisplayLots(double rawVolume)
    {
        return rawVolume / VolumeScale;
    }

    private static double ToUsd(double nativeAmount, double fallbackRateProfit)
    {
        if (fallbackRateProfit > 1.5 && fallbackRateProfit != 0.0)
            return nativeAmount / fallbackRateProfit;
        return nativeAmount;
    }

    private static double NotionalUsd(double lots, double contractSize, double price, string symbol)
    {
        string upperSymbol = (symbol ?? string.Empty).ToUpperInvariant();
        if (upperSymbol.StartsWith("USD", StringComparison.Ordinal) &&
            upperSymbol.Length > 3 &&
            upperSymbol[3] != 'X')
            return Math.Abs(lots * contractSize);

        if (upperSymbol.EndsWith("USD", StringComparison.Ordinal) ||
            upperSymbol == "XAUUSD" ||
            upperSymbol == "XAGUSD")
            return Math.Abs(lots * contractSize * price);

        return Math.Abs(lots * contractSize * price);
    }

    private static string SerializeJson(object payload)
    {
        var serializer = new DataContractJsonSerializer(payload.GetType());
        using (var stream = new MemoryStream())
        {
            serializer.WriteObject(stream, payload);
            return Encoding.UTF8.GetString(stream.ToArray());
        }
    }
}
