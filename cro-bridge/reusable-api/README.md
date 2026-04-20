# Reusable API Bundle

`Mt5MonitorApiBundle.cs` is the single-file source of truth for the reusable
MT5 monitor API.

It currently includes:

- settings and local env loading
- live MT5 polling service
- Manager-style symbol summary aggregation
- Manager-style daily report generation via `DailyRequestByGroup`
- WD Equity Z generation from MT5 daily rows plus filtered protected-bonus balance deals
- Daily PnL Cash generation from MT5 daily rows, protected-bonus balances, and filtered deposit/withdrawal deals
- Manager-style positions history generation from closed deal legs
- position-level audit capture
- bid/ask FX conversion with login/group currency handling
- summary, audit, daily report, and positions history export helpers
- JSON document builders and pretty-printed JSON export helpers for daily and positions reports
- raw-data Daily Closed PnL calculator that converts positions-history currency totals into USD

To reuse it in another project:

1. Copy `Mt5MonitorApiBundle.cs`
2. Reference `MetaQuotes.MT5CommonAPI64.dll`
3. Reference `MetaQuotes.MT5ManagerAPI64.dll`
4. Reference `System.Runtime.Serialization`
5. Reference `System.Xml`
6. Target `.NET Framework 4.8 x64`

The existing `Mt5Monitor.Api` project compiles this file directly, so the GUI
and the reusable bundle stay in sync.

Useful entry points:

- `Mt5DailyReportGenerator.Generate(...)` returns the typed in-memory daily snapshot
- `Mt5DailyReportGenerator.GenerateJsonDocument(...)` returns a JSON-friendly daily document object
- `Mt5DailyReportGenerator.GenerateJson(...)` returns a pretty JSON string
- `Mt5PositionHistoryGenerator.Generate(...)` returns the typed in-memory positions snapshot
- `Mt5PositionHistoryGenerator.GenerateJsonDocument(...)` returns a JSON-friendly positions document object
- `Mt5PositionHistoryGenerator.GenerateJson(...)` returns a pretty JSON string
- `Mt5MonitorJsonExporter.BuildDailyReportJson(...)` / `BuildPositionHistoryJson(...)` serialize existing snapshots without recollecting data
- `Mt5DailyClosedPnlCalculator.Calculate(snapshot, usdRates)` calculates Daily Closed PnL from raw positions-history footer totals
- `Mt5DailyClosedPnlCalculator.Calculate(rows, usdRates)` recalculates the same value from raw positions rows in memory
- `Mt5UsdRateLoader.LoadLiveRates(settings)` loads the live bid/ask USD conversion table from MT5
- `Mt5WdEquityZGenerator.Generate(settings, request)` calculates WD Equity Z for a report date using MT5 daily fields plus balance deals whose comment contains a protected-bonus marker
- `Mt5WdEquityZCalculator.Calculate(...)` can also calculate the same metric from already-loaded daily rows and protected-bonus totals
- `Mt5DailyPnlCashGenerator.Generate(settings, request)` calculates Daily PnL Cash for a report date using MT5 daily fields, protected-bonus balances, and filtered deposit/withdrawal balance deals
- `Mt5DailyPnlCashCalculator.Calculate(...)` can also calculate the same metric from already-loaded daily rows, protected-bonus totals, and net deposits

Example:

```csharp
var settings = Mt5MonitorSettings.FromEnvironment();
var positions = Mt5PositionHistoryGenerator.Generate(settings, fromDate, toDate);
var usdRates = Mt5UsdRateLoader.LoadLiveRates(settings);

var calculator = new Mt5DailyClosedPnlCalculator();
Mt5DailyClosedPnlResult closedPnl = calculator.Calculate(positions, usdRates);
```

WD Equity Z example:

```csharp
var settings = Mt5MonitorSettings.FromEnvironment();
var request = new Mt5WdEquityZRequest
{
    ReportDate = new DateTime(2026, 4, 6),
    BonusHistoryFrom = new DateTime(2026, 1, 1),
    BonusCommentContains = "Bonus Protected Trad",
    ComputationMode = Mt5WdEquityZComputationMode.DeltaFromStartWhenBothPositive
};

Mt5WdEquityZReport wdEquityZ = Mt5WdEquityZGenerator.Generate(settings, request);
```

Daily PnL Cash example:

```csharp
var settings = Mt5MonitorSettings.FromEnvironment();
var request = new Mt5DailyPnlCashRequest
{
    ReportDate = new DateTime(2026, 4, 6),
    BonusHistoryFrom = new DateTime(2026, 1, 1),
    BonusCommentContains = "Bonus Protected Trad"
};

Mt5DailyPnlCashReport dailyPnlCash = Mt5DailyPnlCashGenerator.Generate(settings, request);
```

Notes:

- Start equity comes from MT5 `EquityPrevDay`.
- Start credits are derived as `Credit - DailyCredit` because MT5 daily rows do not expose a `CreditPrevDay` field.
- `BonusHistoryFrom` should reach far enough back to capture the protected-bonus balance you want to subtract; otherwise both start and end bonus totals will be understated.
- Daily PnL Cash uses `Clean Equity = Equity - Credits - Protected Bonuses`.
- Daily PnL Cash uses `max(End Clean Equity, 0) - max(Start Clean Equity, 0) - Net Deposits`, which matches the four-case positive/negative rule.
- Daily PnL Cash net deposits are sourced from MT5 `DEAL_BALANCE` rows for the report date, with the default comment exclusions `bonus`, `cash on balance bonus`, `internal`, and `transfer`.
- If MT5 has no daily rows yet for the requested `ReportDate`, the WD Equity Z and Daily PnL Cash generators fall back to the latest available closed day within the configured lookback window and record that in the report assumptions.
