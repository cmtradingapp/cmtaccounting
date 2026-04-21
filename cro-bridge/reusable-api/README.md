# Reusable API Bundle

`Mt5MonitorApiBundle.cs` is the single-file source of truth for the reusable
MT5/CRO bridge API.

It currently includes:

- settings and local env loading
- live MT5 polling helpers
- Manager-style symbol summary aggregation
- daily report generation via `DailyRequestByGroup`
- trading accounts snapshot generation via `UserAccountRequestArray`
- deposit / withdrawal report generation from `DEAL_BALANCE` rows
- positions history generation from closed deal legs
- live WD Equity raw/Z generation from Trading Accounts + CRM cumulative bonuses
- historical WD Equity Z generation from MT5 daily rows + protected-bonus balance deals
- Daily PnL Cash generation from MT5 daily rows, protected bonuses, and filtered deposits/withdrawals
- workbook-card bundle generation for the active `/cro` page
- summary/audit/report CSV + JSON exporters
- position-level audit capture
- USD FX conversion helpers

## References

To reuse it in another project:

1. Copy `Mt5MonitorApiBundle.cs`
2. Reference `MetaQuotes.MT5CommonAPI64.dll`
3. Reference `MetaQuotes.MT5ManagerAPI64.dll`
4. Reference `System.Data`
5. Reference `System.Runtime.Serialization`
6. Reference `System.Xml`
7. Target `.NET Framework 4.8 x64`

## Core entry points

- `Mt5DailyReportGenerator.Generate(...)`
- `Mt5PositionHistoryGenerator.Generate(...)`
- `Mt5TradingAccountsGenerator.Generate(...)`
- `Mt5DepositWithdrawalGenerator.Generate(...)`
- `Mt5LiveWdEquityZGenerator.Generate(...)`
- `Mt5LiveWdEquityZAuditGenerator.Generate(...)`
- `Mt5WdEquityZGenerator.Generate(...)`
- `Mt5DailyPnlCashGenerator.Generate(...)`
- `Mt5CroCardsGenerator.Generate(...)`

JSON helpers:

- `Mt5DailyReportGenerator.GenerateJson(...)`
- `Mt5PositionHistoryGenerator.GenerateJson(...)`
- `Mt5TradingAccountsGenerator.GenerateJson(...)`
- `Mt5DepositWithdrawalGenerator.GenerateJson(...)`
- `Mt5LiveWdEquityZAuditGenerator.GenerateJson(...)`
- `Mt5CroCardsGenerator.GenerateJson(...)`

CSV helpers:

- `Mt5DailyReportGenerator.GenerateCsv(...)`
- `Mt5PositionHistoryGenerator.GenerateCsv(...)`
- `Mt5TradingAccountsGenerator.GenerateCsv(...)`
- `Mt5DepositWithdrawalGenerator.GenerateCsv(...)`
- `Mt5LiveWdEquityZAuditGenerator.GenerateCsv(...)`

## CRO workbook cards

The workbook-card API is built around:

- `Mt5CroCardsRequest`
- `Mt5CroCardsBundle`
- `Mt5CroCardsGenerator.Generate(settings, request, statusWriter = null)`
- `Mt5CroCardsGenerator.GenerateJson(settings, request, statusWriter = null, indented = true)`

`Mt5CroCardsRequest` includes:

- `ReportDate`
- `AsOfUtc`
- `GroupMask`
- `Source`
- `IncludeSpecs`

`Mt5CroCardsBundle` includes:

- `Meta`
- `Specs`
- `Daily`
- `Monthly`
- `LiveInputs`

The `Meta` block includes source/group/date/mode/freshness information so the
web UI can distinguish:

- default live fast/slow bundle values
- on-demand historical or filtered snapshots
- sections that remain live-scope-only below the cards

The card registry is defined in-code through `Mt5CroWorkbookCards.Specs`. Each
card spec carries:

- `Id`
- `Label`
- `Section`
- `Timeframe`
- `Kind`
- `Dependencies`
- `Exclusions`
- `FormulaNotes`

Important workbook overrides implemented in the bundle:

- `Daily PnL = delta floating + closed pnl`
- `Monthly PnL = monthly closed pnl + (end floating - month-start floating)`
- `Daily/Monthly PnL Cash` from clean-equity logic
- true retention logic instead of `depositors - ftd`
- raw `WD Equity` plus clamped `WD Equity Z`
- FTD metrics from raw MT5 balance-deal history
- `#New Acc Reg` from MT5 registration timestamps
- `Europe/Nicosia` date boundaries for day/month calculations

## WD Equity

Live WD fields:

- raw: `Balance USD + Floating PnL USD - Cumulative Bonus USD`
- clamped: `max(0, raw)`

CRM cumulative bonuses are read directly from
`report.vtiger_mttransactions` using:

- approved `Deposit + Bonus`
- approved `Deposit + FRF Commission`
- net of approved `Withdrawal + BonusCancelled`
- net of approved `Withdrawal + FRF Commission Cancelled`

## Example

```csharp
var settings = Mt5MonitorSettings.FromEnvironment();

var request = new Mt5CroCardsRequest
{
    ReportDate = new DateTime(2026, 4, 21),
    AsOfUtc = DateTime.UtcNow,
    GroupMask = "CMV*",
    Source = "AN100",
    IncludeSpecs = true
};

Mt5CroCardsBundle cards = Mt5CroCardsGenerator.Generate(settings, request);
string cardsJson = Mt5CroCardsGenerator.GenerateJson(settings, request);
```

## Notes

- `Mt5CroCardsGenerator` is used both by the live pusher and the on-demand report path.
- For live-today scope, the bridge overlays fast-loop values onto the card bundle before pushing it.
- For historical or filtered scope, the on-demand report recomputes cards from MT5 history and CRM only.
- Non-card lower tables are intentionally outside this bundle and may remain live-scope-only in the UI.
