# Reusable API Bundle

`Mt5MonitorApiBundle.cs` is the single-file source of truth for the reusable
MT5 monitor API.

It currently includes:

- settings and local env loading
- live MT5 polling service
- Manager-style symbol summary aggregation
- Manager-style daily report generation via `DailyRequestByGroup`
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

Example:

```csharp
var settings = Mt5MonitorSettings.FromEnvironment();
var positions = Mt5PositionHistoryGenerator.Generate(settings, fromDate, toDate);
var usdRates = Mt5UsdRateLoader.LoadLiveRates(settings);

var calculator = new Mt5DailyClosedPnlCalculator();
Mt5DailyClosedPnlResult closedPnl = calculator.Calculate(positions, usdRates);
```
