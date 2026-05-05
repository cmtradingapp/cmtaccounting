/* ──────────────────────────────────────────────────────────
   MT5-CRO Dashboard — vanilla JS

   Responsibilities:
     • Poll /cro/metrics with cancel-in-flight (no overlap, no pile-up)
     • Render 3 status sections (today / yesterday / monthly)
     • Flash green/red on value change vs. previous render
     • Right-side formula panel populated when a metric row is clicked
   ────────────────────────────────────────────────────────── */

"use strict";

/* ─────────── Metric definitions ─────────── */

const FILTER_NOTE = "active accounts only "
  + "(NOT (balance=0 AND equity=0) AND group_name NOT ILIKE '%test%')";

// Section definitions. Layout per card:
//   1) The 4 "headline" metrics (primary / composite tier).
//   2) `dividerTop` line.
//   3) Supporting state + derived numbers.
//
// Highlighted-first ordering means WDZ + Daily P&L variants line up at the
// same Y position across all 3 cards. Each entry:
//   key         — payload key under data[section][...]
//   label       — left-hand text in the metric row
//   signed      — colour the value green/red by sign
//   formatter   — 'money' | 'int'
//   primary     — section-coloured left border + larger, bolder value
//   composite   — superset of primary (top/bottom borders, panel padding)
//   dividerTop  — soft gradient line above
//   summary     — plain-English explanation rendered above the SQL formula
//   formula     — SQL/pseudo-SQL string, syntax-highlighted
//   sources     — array of source-table chips
//   componentsOf — related payload keys to surface as sub-values

const SECTIONS = {
  today: [
    // ── HIGHLIGHTED: WDZ → Positive Exposure → Daily P&L → Daily P&L Cash
    { key: "wd_equity_z_usd",       label: "WD Equity Z USD",       formatter: "money", signed: true,  primary: true, hero: true,
      summary:
`Withdrawable cash right now: equity − credit − cumulative bonuses, clamped at zero
per customer (no one can withdraw a negative balance). This is the broker's
"real" customer-cash exposure — what could leave the door if everyone closed
their positions.`,
      formula:
`Σ MAX(raw_wd_login_USD, 0)         -- per-login clamp at 0
  ↑ this is the "withdrawable cash" figure (no customer can withdraw negative)
  raw_wd_login = equity − credit − Σ bonus_deals
  conversion: external_rates`,
      sources: ["accounts_snapshot", "deposits_withdrawals", "external_rates"],
      componentsOf: ["wd_logins", "wd_positive_logins", "wd_negative_logins"] },

    { key: "exposure_positive_usd", label: "Positive Exposure USD", formatter: "money", signed: false, primary: true,
      summary:
`Total long-side broker exposure across every asset — the sum of every position
where the broker is net long. MT5 Manager calls this "Positive (USD)" on the
Exposure tab; the team alias is "Total ABS Exposure".`,
      formula:
`SUM(GREATEST(exposure_snapshot.volume_net, 0))
  ↑ MT5 Manager Exposure tab "Positive (USD)" — gross long exposure
  team alias: "Total ABS Exposure"`,
      sources: ["exposure_snapshot"],
      componentsOf: ["exposure_assets", "exposure_long_assets", "exposure_short_assets"] },

    { key: "daily_pnl_usd",         label: "Daily P&L USD",         formatter: "money", signed: true, composite: true,
      summary:
`Today's P&L so far (positive = clients profitable). Two pieces:
  • Δ Floating — change in unrealized P&L since yesterday's close.
  • Settled P&L — realized P&L on deals that closed today.
Matches the C# bundle's "daily pnl = delta included" formulation.`,
      formula:
`Δ Floating + Settled P&L
  matches dealer's "daily pnl = delta included" formulation
  matches Mt5MonitorApiBundle.cs:8081 (CalculateDailyPnl)`,
      sources: ["accounts_snapshot", "daily_reports", "closed_positions", "internal_rates"],
      componentsOf: ["delta_floating_usd", "settled_pnl_usd"] },

    { key: "daily_pnl_cash_usd",    label: "Daily P&L Cash USD",    formatter: "money", signed: true, composite: true,
      summary:
`Change in withdrawable book today, after subtracting customer deposits/withdrawals.
"Real" cash P&L vs paper P&L. Per-login WDZ clamping (vs. the C# aggregate clamp).`,
      formula:
`Δ WDZ − Net Deposits
  ↑ withdrawable cash change today
  WDZ-consistent (per-login clamping) — see plan
  vs. C# aggregate clamp at Mt5MonitorApiBundle.cs:9032-44 (we do per-login)`,
      sources: ["accounts_snapshot", "deposits_withdrawals", "external_rates"],
      componentsOf: ["delta_wdz_usd", "net_deposits_usd"] },

    // ── SUPPORTING (canonical order — same indices across all 3 cards)
    { key: "total_balance_usd",     label: "Total Balance USD",     formatter: "money", signed: false, dividerTop: true,
      summary:
`Sum of every customer's account balance, USD-converted at the live internal
MT5 rate. Active accounts only.`,
      formula:
`SUM(accounts_snapshot.balance) USD-converted
  via internal_rates (positive_to_usd / negative_to_usd by sign)
WHERE ${FILTER_NOTE}`,
      sources: ["accounts_snapshot", "internal_rates"], componentsOf: [] },

    { key: "total_credit_usd",      label: "Total Credit USD",      formatter: "money", signed: false,
      summary:
`Total credit (broker-issued promotional balance) across all live accounts.
Credit does NOT count toward withdrawable cash — it's why WDZ subtracts it.`,
      formula:
`SUM(accounts_snapshot.credit) USD-converted
  via internal_rates
WHERE ${FILTER_NOTE}`,
      sources: ["accounts_snapshot", "internal_rates"], componentsOf: [] },

    { key: "cumulative_bonus_usd",  label: "Cumulative Bonus USD (COB)", formatter: "money", signed: true,
      summary:
`Sum of every bonus the broker has ever issued to the CMV* book, USD-converted.
Bonuses are DEAL_BALANCE entries with "bonus" in the comment. Subtracted
inside WDZ (so a customer's bonus credit can't be withdrawn).`,
      formula:
`Σ deposits_withdrawals.amount
WHERE action = 2                            -- DEAL_BALANCE
  AND comment ILIKE '%bonus%'
  AND time < today_end_UTC
  conversion: external_rates`,
      sources: ["deposits_withdrawals", "external_rates"], componentsOf: [] },

    { key: "wd_equity_usd",         label: "WD Equity USD",         formatter: "money", signed: true,
      summary:
`Withdrawable equity WITHOUT the per-login clamp — losing accounts contribute
their negative balance, so this can go negative on a heavily losing book.
WDZ is the clamped variant (no negatives).`,
      formula:
`Σ raw_wd_login_USD
  raw_wd_login = equity − credit − Σ bonus_deals
  bonus = deposits_withdrawals.amount WHERE action=2 AND comment ILIKE '%bonus%'
  conversion: external_rates (per Mt5MonitorApiBundle.cs:4498)`,
      sources: ["accounts_snapshot", "deposits_withdrawals", "external_rates"],
      componentsOf: ["wd_logins", "wd_positive_logins", "wd_negative_logins"] },

    { key: "floating_usd",          label: "Floating USD",          formatter: "money", signed: true,
      summary:
`Live unrealized P&L across every open position right now (profit + storage).
Matches the MT5 Manager Profit (USD) total in the Trades tab.`,
      formula:
`SUM(accounts_snapshot.floating)            -- = IMTAccount.Floating() = profit + storage
  conversion: internal_rates
WHERE ${FILTER_NOTE}
  ↑ matches MT5 Manager Profit (USD) total`,
      sources: ["accounts_snapshot", "internal_rates"], componentsOf: [] },

    { key: "delta_floating_usd",    label: "Δ Floating",            formatter: "money", signed: true,
      summary:
`Change in floating P&L since yesterday's end-of-day. The "delta" piece of
Daily P&L (the rest is Settled P&L from today's closed deals).`,
      formula:
`floating_today − Σ daily_reports.(profit + profit_storage)_yest_eod
  ↑ "delta" component of Daily P&L`,
      sources: ["accounts_snapshot", "daily_reports", "internal_rates"], componentsOf: [] },

    { key: "settled_pnl_usd",       label: "Settled P&L USD",       formatter: "money", signed: true,
      summary:
`P&L from deals that have CLOSED today, including swap, commission, and fees
on those positions. Per-position aggregation (matches MT5 Manager Position
History total for today, after the broker-time fix).`,
      formula:
`Σ (cp.profit + cp.storage + cp.commission + cp.fee)        -- per position
WHERE cp.close_time IN [today_start_UTC, today_end_UTC)
  conversion: internal_rates with rate_profit > 1.5 fallback`,
      sources: ["closed_positions", "internal_rates"], componentsOf: [] },

    { key: "net_deposits_usd",      label: "Net Deposits USD",      formatter: "money", signed: true,
      summary:
`Money customers added (positive) or withdrew (negative) today. Excludes
bonuses, internal "fees placeholder" entries, and "spread charge"
adjustments — same exclusions the C# bundle applies.`,
      formula:
`Σ deposits_withdrawals.amount
WHERE action = 2                            -- DEAL_BALANCE
  AND comment NOT ILIKE '%bonus%'           -- per Mt5MonitorApiBundle.cs:8856-62
  AND comment NOT ILIKE '%fees placeholder%'
  AND comment NOT ILIKE '%spread charge%'
  AND time IN [today_start_UTC, today_end_UTC)
  conversion: external_rates`,
      sources: ["deposits_withdrawals", "external_rates"], componentsOf: [] },

    { key: "delta_wdz_usd",         label: "Δ WDZ",                 formatter: "money", signed: true,
      summary:
`Change in withdrawable cash since yesterday's end-of-day. The "real cash"
delta — pair with Net Deposits to get Daily P&L Cash.`,
      formula:
`wd_equity_z_today − wd_equity_z_yest_EOD
  ↑ change in withdrawable cash since yesterday EOD`,
      sources: ["accounts_snapshot", "daily_reports", "external_rates"], componentsOf: [] },

    // ── TODAY-ONLY EXTRAS (live exposure data — only available for the current snapshot)
    { key: "exposure_net_usd",      label: "Net Exposure USD",      formatter: "money", signed: true,
      summary:
`Net broker exposure: long-side minus short-side, summed across every asset.
Tells you which way the broker's book is net-long (positive) or net-short.`,
      formula:
`SUM(exposure_snapshot.volume_net)        -- signed
  source: SDK ExposureGetAll, already USD (ExposureCurrency=USD)`,
      sources: ["exposure_snapshot"],
      componentsOf: ["exposure_assets", "exposure_long_assets", "exposure_short_assets"] },

    { key: "exposure_absolute_usd", label: "Absolute Exposure USD", formatter: "money", signed: false,
      summary:
`Long-side plus the absolute value of short-side: the broker's total open
position size regardless of direction.`,
      formula:
`SUM(ABS(exposure_snapshot.volume_net))
  ↑ mathematical absolute: long + |short|`,
      sources: ["exposure_snapshot"],
      componentsOf: ["exposure_assets", "exposure_long_assets", "exposure_short_assets"] },

    // ── ACTIVITY & COUNTS (mirrors Mt5MonitorApiBundle.cs)
    { key: "n_traders", label: "#Traders", formatter: "int", dividerTop: true, sectionLabel: "Activity",
      summary:
`Distinct logins that closed at least one position today. Synthetic
"Zeroing*" / "*inactivity*" symbols are excluded (per the C# bundle).`,
      formula:
`COUNT(DISTINCT login)
FROM closed_positions
WHERE close_time IN [today_start, today_end)
  AND symbol NOT ILIKE 'Zeroing%'
  AND symbol NOT ILIKE '%inactivity%'`,
      sources: ["closed_positions"], componentsOf: [] },

    { key: "n_active_traders", label: "#Active Traders", formatter: "int",
      summary:
`Distinct logins currently holding open positions in the live book
(positions_snapshot, refreshed each slow cycle).`,
      formula:
`COUNT(DISTINCT login)
FROM positions_snapshot
WHERE symbol NOT ILIKE 'Zeroing%' AND symbol NOT ILIKE '%inactivity%'`,
      sources: ["positions_snapshot"], componentsOf: [] },

    { key: "n_depositors", label: "#Depositors", formatter: "int",
      summary:
`Distinct logins that made at least one positive deposit today.
Excludes bonus / fees-placeholder / spread-charge comments.`,
      formula:
`COUNT(DISTINCT login)
FROM deposits_withdrawals
WHERE action = 2 AND amount > 0 AND time IN [today_start, today_end)
  AND comment NOT LIKE '%bonus%'
  AND comment NOT LIKE '%fees placeholder%'
  AND comment NOT LIKE '%spread charge%'`,
      sources: ["deposits_withdrawals"], componentsOf: [] },

    { key: "n_new_regs", label: "#New Acc Regs", formatter: "int",
      summary:
`Accounts whose IMTUser.Registration timestamp falls within today.
Test groups excluded.`,
      formula:
`COUNT(*) FROM accounts_snapshot
WHERE registration IN [today_start, today_end)
  AND group_name NOT ILIKE '%test%'`,
      sources: ["accounts_snapshot"], componentsOf: [] },

    { key: "n_ftd", label: "#FTD", formatter: "int",
      summary:
`First-Time Depositors: accounts whose FIRST-EVER positive deposit
landed today. Bonus deposits do count toward "first deposit" candidacy
(matches C# CollectFirstValidDepositDates), only fees-placeholder /
spread-charge are filtered out at the candidacy stage.`,
      formula:
`WITH first_dep AS (
  SELECT login, MIN(time) AS first_time
  FROM deposits_withdrawals
  WHERE action = 2 AND amount > 0
    AND comment NOT LIKE '%fees placeholder%'
    AND comment NOT LIKE '%spread charge%'
  GROUP BY login)
SELECT COUNT(*) FROM first_dep
WHERE first_time IN [today_start, today_end)`,
      sources: ["deposits_withdrawals"], componentsOf: [] },

    { key: "ftd_amount_usd", label: "FTD Amount", formatter: "money", signed: false,
      summary:
`Total deposits made TODAY by FTD logins (USD-converted via external_rates
mid-rate). Bonus deposits are excluded from the AMOUNT sum (unlike the
FTD-login set itself), so this represents real cash brought in.`,
      formula:
`SUM(amount → USD via mid-rate)
FROM deposits_withdrawals
WHERE login ∈ today's FTD set
  AND action = 2 AND amount > 0 AND time IN [today_start, today_end)
  AND comment NOT LIKE '%bonus%'
  AND comment NOT LIKE '%fees placeholder%'
  AND comment NOT LIKE '%spread charge%'`,
      sources: ["deposits_withdrawals", "external_rates"], componentsOf: [] },

    { key: "volume_usd", label: "Volume (USD)", formatter: "money", signed: false,
      summary:
`Gross notional turnover of all deal legs today. Both opening and closing
legs counted (matches C# Mt5MonitorApiBundle.cs convention). USD-converted
at ingest using broker-time-exact MarketBid/MarketAsk for symbols where
the symbol IS the USD-cross pair, or RateProfit as fallback —
deal-time-exact, no FX-time-skew.`,
      formula:
`SUM(deals.notional_usd) WHERE time IN [today_start, today_end)
                          AND action IN (0,1)
                          AND symbol NOT ILIKE 'Zeroing%' / '%inactivity%'
  ↑ notional_usd computed at ingest as
    |volume_lots × contract_size × price| → USD`,
      sources: ["deals"], componentsOf: [] },

    { key: "spread_usd", label: "Spread (USD)", formatter: "money", signed: true,
      summary:
`Gross spread cost across all deals today. Per-deal:
volume_lots × contract_size × (MarketAsk − MarketBid), USD-converted
with the same priority logic as notional. Bid/ask captured directly off
the IMTDeal at trade time — exact, not period-averaged.`,
      formula:
`SUM(deals.spread_cost_usd) WHERE time IN [today_start, today_end)
                              AND action IN (0,1)
                              AND symbol NOT ILIKE 'Zeroing%' / '%inactivity%'`,
      sources: ["deals"], componentsOf: [] },
  ],

  yesterday: [
    // ── HIGHLIGHTED: WDZ EOD → Daily P&L → Daily P&L Cash
    { key: "wd_equity_z_usd",       label: "WD Equity Z USD",       formatter: "money", signed: true,  primary: true, hero: true,
      summary:
`Withdrawable cash at yesterday's end-of-day: equity − credit − cumulative
bonuses, clamped at zero per customer. The broker-cash baseline used to
compute today's ΔWDZ.`,
      formula:
`Σ MAX(raw_wd_login_USD, 0) on daily_reports rows for yesterday
  per-login clamp; conversion: external_rates`,
      sources: ["daily_reports", "deposits_withdrawals", "external_rates"],
      componentsOf: ["wd_logins"] },

    // Padding row so the highlighted block has 4 entries, matching Today.
    { key: "exposure_positive_usd", label: "Positive Exposure USD", formatter: "money", signed: false, primary: true, placeholder: true,
      summary:
`Live exposure is a real-time-only snapshot from the SDK ExposureGetAll
call. There's no equivalent figure for yesterday's end-of-day — see
Today's card for the current value.`,
      formula: `(not applicable for the yesterday EOD window)`,
      sources: [], componentsOf: [] },

    { key: "daily_pnl_usd",         label: "Daily P&L USD",         formatter: "money", signed: true, composite: true,
      summary:
`Yesterday's full-day P&L: Δ Floating + Settled P&L over the whole
yesterday window.`,
      formula:
`Δ Floating + Settled P&L          (full yesterday window)`,
      sources: ["daily_reports", "closed_positions", "internal_rates"],
      componentsOf: ["delta_floating_usd", "settled_pnl_usd"] },

    { key: "daily_pnl_cash_usd",    label: "Daily P&L Cash USD",    formatter: "money", signed: true, composite: true,
      summary:
`Yesterday's actual cash gain: the change in withdrawable cash over the day,
minus customer deposits/withdrawals. Per-login WDZ clamping.`,
      formula:
`Δ WDZ − Net Deposits          (full yesterday window)
  per-login WDZ clamping (vs. C# aggregate)`,
      sources: ["daily_reports", "deposits_withdrawals", "external_rates"],
      componentsOf: ["delta_wdz_usd", "net_deposits_usd"] },

    // ── SUPPORTING (canonical order — same indices across all 3 cards)
    { key: "total_balance_usd",     label: "Total Balance USD",     formatter: "money", signed: false, dividerTop: true,
      summary:
`Sum of every active customer's account balance at yesterday's end-of-day.`,
      formula:
`SUM(daily_reports.balance) USD-converted via internal_rates
WHERE datetime IN [yesterday_start_UTC, today_start_UTC)
  AND group_name NOT ILIKE '%test%' AND NOT (balance=0 AND profit_equity=0)`,
      sources: ["daily_reports", "internal_rates"], componentsOf: [] },

    { key: "total_credit_usd",      label: "Total Credit USD",      formatter: "money", signed: false,
      summary:
`Total credit balance across all active accounts at yesterday's EOD.`,
      formula:
`SUM(daily_reports.credit) USD-converted via internal_rates
WHERE datetime IN [yesterday_start_UTC, today_start_UTC)`,
      sources: ["daily_reports", "internal_rates"], componentsOf: [] },

    { key: "cumulative_bonus_usd",  label: "Cumulative Bonus USD (COB)", formatter: "money", signed: true,
      summary:
`Sum of every bonus the broker has issued up to yesterday's end-of-day,
USD-converted. Same definition as Today's COB, just snapshotted earlier.`,
      formula:
`Σ deposits_withdrawals.amount
WHERE action = 2 AND comment ILIKE '%bonus%'
  AND time < today_start_UTC                  -- yesterday EOD cutoff
  conversion: external_rates`,
      sources: ["deposits_withdrawals", "external_rates"], componentsOf: [] },

    { key: "wd_equity_usd",         label: "WD Equity USD",         formatter: "money", signed: true,
      summary:
`Withdrawable equity (no clamp) at yesterday's EOD — losing accounts contribute
negatives so this can be much smaller than WDZ.`,
      formula:
`Σ raw_wd_login_USD on daily_reports rows for yesterday
  raw_wd_login = profit_equity − credit − Σ bonus_deals (time < today_start)
  conversion: external_rates`,
      sources: ["daily_reports", "deposits_withdrawals", "external_rates"],
      componentsOf: ["wd_logins"] },

    { key: "floating_usd",          label: "Floating USD",          formatter: "money", signed: true,
      summary:
`Open-position P&L as of yesterday's end-of-day snapshot (from daily_reports,
not live positions).`,
      formula:
`SUM(daily_reports.profit + daily_reports.profit_storage)
WHERE datetime IN [yesterday_start_UTC, today_start_UTC)
  conversion: internal_rates`,
      sources: ["daily_reports", "internal_rates"], componentsOf: [] },

    { key: "delta_floating_usd",    label: "Δ Floating",            formatter: "money", signed: true,
      summary:
`Change in floating P&L from the day-before-yesterday's EOD to yesterday's EOD.`,
      formula:
`floating_yest_eod − floating_day_before_eod
  both from daily_reports.(profit + profit_storage)`,
      sources: ["daily_reports", "internal_rates"], componentsOf: [] },

    { key: "settled_pnl_usd",       label: "Settled P&L USD",       formatter: "money", signed: true,
      summary:
`P&L from every deal that closed during yesterday (P + Storage + Commission
+ Fee, per position).`,
      formula:
`Σ (profit + storage + commission + fee) on closed_positions
WHERE close_time IN [yesterday_start_UTC, today_start_UTC)
  conversion: internal_rates with rate_profit > 1.5 fallback`,
      sources: ["closed_positions", "internal_rates"], componentsOf: [] },

    { key: "net_deposits_usd",      label: "Net Deposits USD",      formatter: "money", signed: true,
      summary:
`Customer deposits/withdrawals over the full yesterday window. Same exclusions
as today's net deposits (no bonuses, no internal fees).`,
      formula:
`Σ deposits_withdrawals.amount over [yesterday_start, today_start)
  filters: action=2; comment excludes bonus/fees placeholder/spread charge
  conversion: external_rates`,
      sources: ["deposits_withdrawals", "external_rates"], componentsOf: [] },

    { key: "delta_wdz_usd",         label: "Δ WDZ",                 formatter: "money", signed: true,
      summary:
`Change in withdrawable cash over the full yesterday window.`,
      formula:
`wd_equity_z_yest_EOD − wd_equity_z_day_before_EOD
  ↑ change in withdrawable cash over the full yesterday window`,
      sources: ["daily_reports", "deposits_withdrawals", "external_rates"], componentsOf: [] },

    // Padding rows so the supporting block has 11 entries, matching Today.
    { key: "exposure_net_usd",      label: "Net Exposure USD",      formatter: "money", signed: true, placeholder: true,
      summary:
`Live exposure is a real-time-only snapshot from SDK ExposureGetAll. There's
no historical record at yesterday's EOD — see Today's card for the current
value.`,
      formula: `(not applicable for the yesterday EOD window)`,
      sources: [], componentsOf: [] },

    { key: "exposure_absolute_usd", label: "Absolute Exposure USD", formatter: "money", signed: false, placeholder: true,
      summary:
`Live exposure is a real-time-only snapshot — no yesterday EOD record.
See Today's card for the current value.`,
      formula: `(not applicable for the yesterday EOD window)`,
      sources: [], componentsOf: [] },

    // ── ACTIVITY & COUNTS (yesterday window)
    { key: "n_traders", label: "#Traders", formatter: "int", dividerTop: true, sectionLabel: "Activity",
      summary: `Distinct logins that closed at least one position yesterday.`,
      formula:
`COUNT(DISTINCT login)
FROM closed_positions
WHERE close_time IN [yesterday_start, today_start)
  AND symbol NOT ILIKE 'Zeroing%'
  AND symbol NOT ILIKE '%inactivity%'`,
      sources: ["closed_positions"], componentsOf: [] },

    { key: "n_active_traders", label: "#Active Traders", formatter: "int",
      summary:
`Distinct logins that opened OR closed positions during yesterday's
window. Approximates "logins with new opens yesterday" — see plan note.`,
      formula:
`COUNT(DISTINCT login)
FROM closed_positions
WHERE (open_time IN window OR close_time IN window)
  AND symbol NOT ILIKE 'Zeroing%' AND symbol NOT ILIKE '%inactivity%'`,
      sources: ["closed_positions"], componentsOf: [] },

    { key: "n_depositors", label: "#Depositors", formatter: "int",
      summary: `Distinct logins with positive deposits yesterday.`,
      formula:
`COUNT(DISTINCT login)
FROM deposits_withdrawals
WHERE action = 2 AND amount > 0 AND time IN [yesterday, today)
  AND comment NOT LIKE '%bonus%'
  AND comment NOT LIKE '%fees placeholder%'
  AND comment NOT LIKE '%spread charge%'`,
      sources: ["deposits_withdrawals"], componentsOf: [] },

    { key: "n_new_regs", label: "#New Acc Regs", formatter: "int",
      summary: `Accounts registered yesterday (test groups excluded).`,
      formula:
`COUNT(*) FROM accounts_snapshot
WHERE registration IN [yesterday, today)
  AND group_name NOT ILIKE '%test%'`,
      sources: ["accounts_snapshot"], componentsOf: [] },

    { key: "n_ftd", label: "#FTD", formatter: "int",
      summary: `Logins whose first-ever positive deposit landed yesterday.`,
      formula:
`WITH first_dep AS (... per login MIN(time)...)
SELECT COUNT(*) FROM first_dep
WHERE first_time IN [yesterday, today)`,
      sources: ["deposits_withdrawals"], componentsOf: [] },

    { key: "ftd_amount_usd", label: "FTD Amount", formatter: "money", signed: false,
      summary: `Cash deposit total for yesterday's FTD logins (USD).`,
      formula:
`SUM(amount → USD)
FROM deposits_withdrawals JOIN ftd_logins
WHERE deposit window = yesterday, bonus/fees/spread excluded`,
      sources: ["deposits_withdrawals", "external_rates"], componentsOf: [] },

    { key: "volume_usd", label: "Volume (USD)", formatter: "money", signed: false,
      summary: `Gross notional turnover for all deal legs yesterday (USD).`,
      formula:
`SUM(deals.notional_usd) WHERE time IN [yesterday, today)
                          AND action IN (0,1)
                          AND symbol NOT ILIKE 'Zeroing%' / '%inactivity%'`,
      sources: ["deals"], componentsOf: [] },

    { key: "spread_usd", label: "Spread (USD)", formatter: "money", signed: true,
      summary: `Gross spread cost across all deals yesterday (USD).`,
      formula:
`SUM(deals.spread_cost_usd) WHERE time IN [yesterday, today)
                              AND action IN (0,1)
                              AND symbol NOT ILIKE 'Zeroing%' / '%inactivity%'`,
      sources: ["deals"], componentsOf: [] },
  ],

  monthly: [
    // ── HIGHLIGHTED: WDZ baseline → Monthly P&L → Monthly P&L Cash
    { key: "wd_equity_z_month_start_usd", label: "WDZ @ month-start", formatter: "money", signed: true,  primary: true, hero: true,
      summary:
`Withdrawable cash at the close of last month — the baseline against which
every MTD cash metric (ΔWDZ, Monthly P&L Cash) is measured.`,
      formula:
`Σ MAX(raw_wd_login_USD, 0) on daily_reports rows for prev-month-end EOD
  baseline for the MTD WDZ delta`,
      sources: ["daily_reports", "deposits_withdrawals", "external_rates"], componentsOf: [] },

    // Padding row so the highlighted block has 4 entries, matching Today.
    { key: "exposure_positive_usd",      label: "Positive Exposure USD", formatter: "money", signed: false, primary: true, placeholder: true,
      summary:
`Live exposure is a real-time-only snapshot from the SDK ExposureGetAll
call. There's no equivalent figure at the start of the month — see
Today's card for the current value.`,
      formula: `(not applicable for the month-start window)`,
      sources: [], componentsOf: [] },

    { key: "monthly_pnl_usd",            label: "Monthly P&L USD",   formatter: "money", signed: true, composite: true,
      summary:
`Month-to-date P&L: ΔFloating since month-start + sum of all deals
closed during the month. Matches the C# bundle's CalculateMonthlyPnl card.`,
      formula:
`Δ Floating (MTD) + MTD Settled P&L
  matches Mt5MonitorApiBundle.cs:8082 (CalculateMonthlyPnl)`,
      sources: ["accounts_snapshot", "daily_reports", "closed_positions", "internal_rates"],
      componentsOf: ["delta_floating_usd", "settled_pnl_usd"] },

    { key: "monthly_pnl_cash_usd",       label: "Monthly P&L Cash USD", formatter: "money", signed: true, composite: true,
      summary:
`Month-to-date cash P&L: change in withdrawable cash since month-start, minus
net customer deposits over the month.`,
      formula:
`Δ WDZ (MTD) − Net Deposits (MTD)
  WDZ-consistent (per-login clamping)`,
      sources: ["accounts_snapshot", "daily_reports", "deposits_withdrawals", "external_rates"],
      componentsOf: ["delta_wdz_usd", "net_deposits_usd"] },

    // ── SUPPORTING (canonical order — same indices across all 3 cards.
    //    Monthly's snapshots are at MONTH-START, since this card's narrative
    //    is "what happened from month-start to now" and the highlighted
    //    Monthly P&L is anchored on month-start values.)
    { key: "total_balance_month_start_usd", label: "Total Balance USD", formatter: "money", signed: false, dividerTop: true,
      summary:
`Total customer balance at the close of last month — the baseline against
which Monthly P&L is measured. From daily_reports.balance for prev_month_end.`,
      formula:
`SUM(daily_reports.balance) USD-converted via internal_rates
WHERE datetime IN [prev_month_end_start_UTC, month_start_UTC)
  AND group_name NOT ILIKE '%test%' AND NOT (balance=0 AND profit_equity=0)`,
      sources: ["daily_reports", "internal_rates"], componentsOf: [] },

    { key: "total_credit_month_start_usd",  label: "Total Credit USD", formatter: "money", signed: false,
      summary:
`Total credit balance at the close of last month.`,
      formula:
`SUM(daily_reports.credit) USD-converted via internal_rates
WHERE datetime IN [prev_month_end_start_UTC, month_start_UTC)`,
      sources: ["daily_reports", "internal_rates"], componentsOf: [] },

    { key: "cumulative_bonus_usd",          label: "Cumulative Bonus USD (COB)", formatter: "money", signed: true,
      summary:
`Sum of every bonus issued up to the close of last month — the baseline
COB for this month's calculations.`,
      formula:
`Σ deposits_withdrawals.amount
WHERE action = 2 AND comment ILIKE '%bonus%'
  AND time < month_start_UTC                  -- prev-month-end cutoff
  conversion: external_rates`,
      sources: ["deposits_withdrawals", "external_rates"], componentsOf: [] },

    { key: "wd_equity_month_start_usd",     label: "WD Equity USD",   formatter: "money", signed: true,
      summary:
`Withdrawable equity (unclamped) at the close of last month — same definition
as Today's WD Equity USD, just snapshotted at the prev-month-end EOD.`,
      formula:
`Σ raw_wd_login_USD on daily_reports rows for prev-month-end
  raw_wd_login = profit_equity − credit − Σ bonus_deals (time < month_start)
  conversion: external_rates`,
      sources: ["daily_reports", "deposits_withdrawals", "external_rates"], componentsOf: [] },

    { key: "floating_month_start_usd",      label: "Floating USD",    formatter: "money", signed: true,
      summary:
`Floating P&L baseline: open-position P&L at the close of last month.
Subtracted from "Floating @ now" to give Δ Floating (MTD).`,
      formula:
`Σ daily_reports.(profit + profit_storage) on the last day of previous month
  ↑ EOD snapshot used as the month-start baseline`,
      sources: ["daily_reports", "internal_rates"], componentsOf: [] },

    { key: "delta_floating_usd",            label: "Δ Floating",      formatter: "money", signed: true,
      summary:
`Change in floating P&L from month-start to right now.`,
      formula:
`floating_now − floating_prev_month_end_eod`,
      sources: ["accounts_snapshot", "daily_reports", "internal_rates"], componentsOf: [] },

    { key: "settled_pnl_usd",               label: "Settled P&L USD", formatter: "money", signed: true,
      summary:
`Sum of P&L from every deal closed since the start of this month.`,
      formula:
`Σ (profit + storage + commission + fee) on closed_positions
WHERE close_time IN [month_start_UTC, today_end_UTC)
  conversion: internal_rates with rate_profit > 1.5 fallback`,
      sources: ["closed_positions", "internal_rates"], componentsOf: [] },

    { key: "net_deposits_usd",              label: "Net Deposits USD", formatter: "money", signed: true,
      summary:
`Net customer deposits/withdrawals from month-start to now.`,
      formula:
`Σ deposits_withdrawals.amount over [month_start_UTC, today_end_UTC)
  filters: action=2; comment excludes bonus/fees placeholder/spread charge
  conversion: external_rates`,
      sources: ["deposits_withdrawals", "external_rates"], componentsOf: [] },

    { key: "delta_wdz_usd",                 label: "Δ WDZ",           formatter: "money", signed: true,
      summary:
`Change in withdrawable cash over the month so far.`,
      formula:
`wd_equity_z_today − wd_equity_z_prev_month_end_EOD`,
      sources: ["accounts_snapshot", "daily_reports", "external_rates"], componentsOf: [] },

    // Padding rows so the supporting block has 11 entries, matching Today.
    { key: "exposure_net_usd",              label: "Net Exposure USD", formatter: "money", signed: true, placeholder: true,
      summary:
`Live exposure is a real-time-only snapshot from SDK ExposureGetAll. There's
no historical record at month-start — see Today's card for the current value.`,
      formula: `(not applicable for the month-start window)`,
      sources: [], componentsOf: [] },

    { key: "exposure_absolute_usd",         label: "Absolute Exposure USD", formatter: "money", signed: false, placeholder: true,
      summary:
`Live exposure is a real-time-only snapshot — no month-start record.
See Today's card for the current value.`,
      formula: `(not applicable for the month-start window)`,
      sources: [], componentsOf: [] },

    // ── ACTIVITY & COUNTS (MTD window)
    { key: "n_traders", label: "#Traders", formatter: "int", dividerTop: true, sectionLabel: "Activity",
      summary: `Distinct logins that closed at least one position this month-to-date.`,
      formula:
`COUNT(DISTINCT login)
FROM closed_positions
WHERE close_time IN [month_start, today_end)
  AND symbol NOT ILIKE 'Zeroing%'
  AND symbol NOT ILIKE '%inactivity%'`,
      sources: ["closed_positions"], componentsOf: [] },

    { key: "n_active_traders", label: "#Active Traders", formatter: "int",
      summary: `Distinct logins that opened OR closed positions during MTD.`,
      formula:
`COUNT(DISTINCT login)
FROM closed_positions
WHERE (open_time IN window OR close_time IN window)
  AND symbol NOT ILIKE 'Zeroing%' AND symbol NOT ILIKE '%inactivity%'`,
      sources: ["closed_positions"], componentsOf: [] },

    { key: "n_depositors", label: "#Depositors", formatter: "int",
      summary: `Distinct logins with positive deposits MTD.`,
      formula:
`COUNT(DISTINCT login)
FROM deposits_withdrawals
WHERE action = 2 AND amount > 0 AND time IN [month_start, today_end)
  AND comment NOT LIKE '%bonus%'
  AND comment NOT LIKE '%fees placeholder%'
  AND comment NOT LIKE '%spread charge%'`,
      sources: ["deposits_withdrawals"], componentsOf: [] },

    { key: "n_new_regs", label: "#New Acc Regs", formatter: "int",
      summary: `Accounts registered MTD (test groups excluded).`,
      formula:
`COUNT(*) FROM accounts_snapshot
WHERE registration IN [month_start, today_end)
  AND group_name NOT ILIKE '%test%'`,
      sources: ["accounts_snapshot"], componentsOf: [] },

    { key: "n_ftd", label: "#FTD", formatter: "int",
      summary: `Logins whose first-ever positive deposit fell in MTD.`,
      formula:
`WITH first_dep AS (... per login MIN(time)...)
SELECT COUNT(*) FROM first_dep
WHERE first_time IN [month_start, today_end)`,
      sources: ["deposits_withdrawals"], componentsOf: [] },

    { key: "ftd_amount_usd", label: "FTD Amount", formatter: "money", signed: false,
      summary: `Cash deposit total for MTD FTD logins (USD).`,
      formula:
`SUM(amount → USD) FROM deposits_withdrawals JOIN ftd_logins
WHERE deposit window = MTD, bonus/fees/spread excluded`,
      sources: ["deposits_withdrawals", "external_rates"], componentsOf: [] },

    { key: "volume_usd", label: "Volume (USD)", formatter: "money", signed: false,
      summary: `Gross notional turnover MTD (USD). Both legs counted.`,
      formula:
`SUM(deals.notional_usd) WHERE time IN [month_start, today_end)
                          AND action IN (0,1)
                          AND symbol NOT ILIKE 'Zeroing%' / '%inactivity%'`,
      sources: ["deals"], componentsOf: [] },

    { key: "spread_usd", label: "Spread (USD)", formatter: "money", signed: true,
      summary: `Gross spread cost MTD (USD).`,
      formula:
`SUM(deals.spread_cost_usd) WHERE time IN [month_start, today_end)
                              AND action IN (0,1)
                              AND symbol NOT ILIKE 'Zeroing%' / '%inactivity%'`,
      sources: ["deals"], componentsOf: [] },
  ],
};

/* ─────────── Formatters ─────────── */

function formatMoney(v) {
  if (v === null || v === undefined || Number.isNaN(v)) return "—";
  const n = Number(v);
  const abs = Math.abs(n).toLocaleString("en-US", {
    minimumFractionDigits: 2, maximumFractionDigits: 2,
  });
  return (n < 0 ? "-$" : " $") + abs;
}

function formatInt(v) {
  if (v === null || v === undefined || Number.isNaN(v)) return "—";
  return Number(v).toLocaleString("en-US");
}

function formatValue(def, v) {
  return def.formatter === "int" ? formatInt(v) : formatMoney(v);
}

// Payload keys that represent INTEGER counters (login counts, asset counts)
// rather than money. Used by the formula panel's sub-components renderer
// where we only have the key name, not a metric def with a `formatter`.
const INTEGER_COMPONENT_KEYS = new Set([
  "wd_logins", "wd_positive_logins", "wd_negative_logins",
  "exposure_assets", "exposure_long_assets", "exposure_short_assets",
]);

function formatComponent(key, def, val) {
  if (def) return formatValue(def, val);
  if (INTEGER_COMPONENT_KEYS.has(key)) return formatInt(val);
  return formatMoney(val);
}

/* ─────────── Cross-card grouping ─────────── */

// Map each per-section payload key to the logical "group" it belongs to.
// Rows in the same group are visually paired across cards: hovering one
// lights up the others, and a small ▴/▾ indicator marks the high/low value
// of the group across the three cards. Keys not in this map fall back to
// the key itself (e.g. delta_floating_usd, settled_pnl_usd already share
// the same key in all sections).
const KEY_TO_GROUP = {
  "wd_equity_z_usd":              "wdz",
  "wd_equity_z_month_start_usd":  "wdz",

  "exposure_positive_usd":        "positive_exposure",

  "daily_pnl_usd":                "pnl",
  "monthly_pnl_usd":              "pnl",
  "daily_pnl_cash_usd":           "pnl_cash",
  "monthly_pnl_cash_usd":         "pnl_cash",

  "total_balance_usd":            "balance",
  "total_balance_month_start_usd":"balance",

  "total_credit_usd":             "credit",
  "total_credit_month_start_usd": "credit",

  "wd_equity_usd":                "wd_equity",
  "wd_equity_month_start_usd":    "wd_equity",

  "floating_usd":                 "floating",
  "floating_month_start_usd":     "floating",
};

function getGroup(key) {
  return KEY_TO_GROUP[key] || key;
}

// For each group with at least 2 real values across the 3 sections,
// returns { [sectionName]: 'high' | 'low' } for the highest and lowest
// (the middle one is implicit and gets no marker).
function computeRanks(payload) {
  const valuesByGroup = {};
  for (const sectionName of ["today", "yesterday", "monthly"]) {
    const section = payload?.[sectionName];
    if (!section) continue;
    for (const def of (SECTIONS[sectionName] || [])) {
      if (def.placeholder) continue;
      const v = section[def.key];
      if (v === null || v === undefined) continue;
      const n = Number(v);
      if (!Number.isFinite(n)) continue;
      const g = getGroup(def.key);
      if (!valuesByGroup[g]) valuesByGroup[g] = {};
      valuesByGroup[g][sectionName] = n;
    }
  }

  const ranks = {};
  for (const [g, sections] of Object.entries(valuesByGroup)) {
    const entries = Object.entries(sections);
    if (entries.length < 2) continue;                 // need at least 2 to rank
    const sorted = entries.slice().sort((a, b) => b[1] - a[1]);
    const highSection = sorted[0][0];
    const lowSection  = sorted[sorted.length - 1][0];
    if (sorted[0][1] === sorted[sorted.length - 1][1]) continue;  // all equal
    ranks[g] = { [highSection]: "high", [lowSection]: "low" };
  }
  return ranks;
}

/* ─────────── State ─────────── */

const PREV = { today: {}, yesterday: {}, monthly: {} };
let currentInflight = null;          // AbortController for the in-flight fetch
let pollTimer = null;
let lastPayload = null;              // for re-rendering the formula panel
let selected = null;                 // { section, key }

/* ─────────── Render ─────────── */

function classifySign(def, v) {
  if (!def.signed) return "";
  if (v === null || v === undefined || Number.isNaN(v)) return "";
  if (Number(v) > 0) return "pos";
  if (Number(v) < 0) return "neg";
  return "";
}

function renderSection(sectionName, sectionData, ranks) {
  const container = document.querySelector(
    `[data-section-content="${sectionName}"]`
  );
  if (!container) return;

  const defs = SECTIONS[sectionName] || [];
  const prev = PREV[sectionName];
  const frag = document.createDocumentFragment();

  for (const def of defs) {
    // Section heading: when a row carries both `dividerTop` AND
    // `sectionLabel` (e.g. "Activity"), emit a small uppercase label
    // BEFORE the metric row. This visually groups the rows below into a
    // distinct section.
    if (def.dividerTop && def.sectionLabel) {
      const heading = document.createElement("div");
      heading.className = "section-label";
      heading.textContent = def.sectionLabel;
      frag.appendChild(heading);
    }

    // `placeholder: true` rows render as "—" in every card — used to pad
    // Yesterday/Monthly so all cards share the same row count and metrics
    // line up at the same Y position. Treat the value as null and the
    // existing formatter handles the dash.
    const v = def.placeholder
      ? null
      : (sectionData ? sectionData[def.key] : null);
    const previous = prev[def.key];
    const numeric = (v === null || v === undefined) ? null : Number(v);

    const row = document.createElement("div");
    row.className = "metric";
    // Composite metrics are implicitly primary too (Daily P&L, Daily P&L Cash,
    // Monthly P&L, Monthly P&L Cash). Plus the explicitly-flagged primaries
    // (WDZ, Positive Exposure).
    if (def.primary || def.composite) row.classList.add("primary");
    if (def.composite)  row.classList.add("composite");
    if (def.hero)       row.classList.add("hero");
    if (def.dividerTop) row.classList.add("divider-top");
    if (selected && selected.section === sectionName && selected.key === def.key) {
      row.classList.add("selected");
    }
    row.dataset.key = def.key;
    row.dataset.section = sectionName;
    // Cross-card group: rows with the same group light up together on hover
    // and may carry a high/low rank indicator.
    const groupName = getGroup(def.key);
    row.dataset.group = groupName;
    const rank = !def.placeholder && ranks?.[groupName]?.[sectionName];
    if (rank) row.classList.add(`rank-${rank}`);

    const label = document.createElement("span");
    label.className = "metric-label";
    label.textContent = def.label;
    label.title = def.label;     // full label visible if ever truncated by ellipsis
    row.appendChild(label);

    // Value and delta share an auto-sized middle cell. The rank indicator
    // lives in its OWN fixed-width column at the very right of the row so
    // all ▴/▾ ticks align at the same X across rows regardless of value
    // length.
    const valueWrap = document.createElement("span");
    valueWrap.className = "metric-value-wrap";

    // HIGHEST / LOWEST badge — only visible during cross-hover (CSS-driven).
    // The text is set unconditionally; CSS hides it unless the parent has
    // both `.cross-hover` and the matching `.rank-high|.rank-low` class.
    const badge = document.createElement("span");
    badge.className = "metric-badge";
    if (rank === "high") badge.textContent = "HIGHEST";
    else if (rank === "low") badge.textContent = "LOWEST";
    valueWrap.appendChild(badge);

    const value = document.createElement("span");
    value.className = "metric-value";
    const signClass = classifySign(def, numeric);
    if (signClass) value.classList.add(signClass);
    value.textContent = formatValue(def, numeric);
    if (numeric !== null && Number.isFinite(numeric)) {
      value.title = def.formatter === "int"
        ? String(numeric)
        : `${numeric} USD`;
    }

    // Threshold: half a cent for money / half a unit for integer counters.
    // Anything smaller would render as "$0.00" or "0" in the formatted
    // delta — show no indicator instead of "▲ $0.00".
    const deltaThreshold = def.formatter === "int" ? 0.5 : 0.005;
    let delta = null;
    if (
      numeric !== null && previous !== undefined && previous !== null
      && Number.isFinite(previous) && Number.isFinite(numeric)
      && Math.abs(numeric - previous) >= deltaThreshold
    ) {
      delta = numeric - previous;
      value.classList.add(delta > 0 ? "flash-up" : "flash-down");
    }

    valueWrap.appendChild(value);

    const deltaEl = document.createElement("span");
    deltaEl.className = "metric-delta";
    if (delta !== null) {
      const glyph = delta > 0 ? "▲" : "▼";
      let deltaText = formatValue(def, Math.abs(delta));
      if (deltaText.startsWith(" ")) deltaText = deltaText.slice(1);
      deltaEl.textContent = `${glyph} ${deltaText}`;
      deltaEl.classList.add(delta > 0 ? "pos" : "neg");
    }
    valueWrap.appendChild(deltaEl);
    row.appendChild(valueWrap);

    // Rank tick lives in its own fixed-width grid column at the right.
    // Always render the span (even when empty) so the grid track stays
    // populated and rows line up identically.
    const rankEl = document.createElement("span");
    rankEl.className = "metric-rank";
    if (rank) {
      rankEl.classList.add(`rank-${rank}`);
      rankEl.textContent = rank === "high" ? "▴" : "▾";
      rankEl.title = rank === "high"
        ? "Highest of the 3 cards"
        : "Lowest of the 3 cards";
    }
    row.appendChild(rankEl);
    frag.appendChild(row);

    prev[def.key] = numeric;
  }

  container.replaceChildren(frag);
}

/* ─────────── Formula panel ─────────── */

const SQL_KEYWORDS = [
  "SELECT", "FROM", "WHERE", "AND", "OR", "ON", "JOIN", "LEFT", "RIGHT",
  "GROUP", "BY", "ORDER", "AS", "WITH", "CASE", "WHEN", "THEN", "ELSE", "END",
  "SUM", "MAX", "MIN", "COUNT", "AVG", "COALESCE", "GREATEST", "LEAST",
  "ABS", "NULLIF", "IS", "NOT", "NULL", "ILIKE", "LIKE", "IN",
];

const SOURCE_TABLES = [
  "accounts_snapshot", "positions_snapshot", "daily_reports",
  "closed_positions", "deposits_withdrawals", "exposure_snapshot",
  "internal_rates", "external_rates", "sync_watermarks",
];

function escapeHTML(s) {
  return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

function highlightFormula(text) {
  // Process line-by-line so SQL comments stay scoped to one line.
  return text.split("\n").map((line) => {
    const idx = line.indexOf("--");
    let head = idx >= 0 ? line.slice(0, idx) : line;
    const tail = idx >= 0 ? line.slice(idx) : "";

    head = escapeHTML(head)
      .replace(/\b(\d+(?:\.\d+)?)\b/g, '<span class="number">$1</span>')
      .replace(
        new RegExp(`\\b(${SOURCE_TABLES.join("|")})\\b`, "g"),
        '<span class="source">$1</span>'
      )
      .replace(
        new RegExp(`\\b(${SQL_KEYWORDS.join("|")})\\b`, "g"),
        '<span class="keyword">$1</span>'
      );

    return idx >= 0
      ? head + `<span class="comment">${escapeHTML(tail)}</span>`
      : head;
  }).join("\n");
}

function findDef(section, key) {
  return (SECTIONS[section] || []).find((d) => d.key === key) || null;
}

function lookupComponentValue(section, key) {
  if (!lastPayload || !lastPayload[section]) return null;
  return lastPayload[section][key];
}

function findComponentDef(section, key) {
  // Components live in the same section first; fall back to today/yesterday/monthly.
  const local = findDef(section, key);
  if (local) return local;
  for (const sec of ["today", "yesterday", "monthly"]) {
    const d = findDef(sec, key);
    if (d) return d;
  }
  return null;
}

function renderFormulaPanel(section, key) {
  const body = document.getElementById("panel-body");
  if (!body) return;

  const def = findDef(section, key);
  if (!def) {
    body.innerHTML = '<div class="panel-placeholder">Metric not found.</div>';
    return;
  }

  const sectionTag = section === "today" ? "TODAY"
                   : section === "yesterday" ? "YESTERDAY"
                   : "MONTHLY";

  let html = "";
  html += `<div class="metric-title">${escapeHTML(def.label)}</div>`;
  html += `<span class="metric-section-tag">${sectionTag}</span>`;

  // Plain-English summary above the SQL formula.
  if (def.summary) {
    html += `<div class="metric-summary">${escapeHTML(def.summary)}</div>`;
  }

  html += `<h3>SQL formula</h3>`;
  html += `<pre class="formula-block">${highlightFormula(def.formula)}</pre>`;

  if (def.sources && def.sources.length) {
    html += "<h3>Source tables</h3><ul>";
    for (const src of def.sources) {
      html += `<li><code>${escapeHTML(src)}</code></li>`;
    }
    html += "</ul>";
  }

  // Current value of this metric.
  const v = lookupComponentValue(section, def.key);
  html += "<h3>Current value</h3>";
  html += `<ul class="components"><li><span>${escapeHTML(def.label)}</span>`
       + `<span class="comp-value">${escapeHTML(formatValue(def, v))}</span></li></ul>`;

  if (def.componentsOf && def.componentsOf.length) {
    html += "<h3>Sub-components</h3><ul class=\"components\">";
    for (const compKey of def.componentsOf) {
      const compDef = findComponentDef(section, compKey);
      const compVal = lookupComponentValue(section, compKey);
      const compLabel = compDef ? compDef.label : compKey;
      const compFormatted = formatComponent(compKey, compDef, compVal);
      html += `<li><span>${escapeHTML(compLabel)}</span>`
           + `<span class="comp-value">${escapeHTML(compFormatted)}</span></li>`;
    }
    html += "</ul>";
  }

  body.innerHTML = html;
}

function selectMetric(section, key) {
  selected = { section, key };
  // Update .selected class on rows without a full re-render.
  document.querySelectorAll(".metric.selected").forEach((el) => {
    el.classList.remove("selected");
  });
  const row = document.querySelector(
    `[data-section-content="${section}"] .metric[data-key="${key}"]`
  );
  if (row) row.classList.add("selected");
  renderFormulaPanel(section, key);
}

/* ─────────── Connection / status ─────────── */

function setConnected(ok) {
  const el = document.getElementById("conn-status");
  if (!el) return;
  if (ok) {
    el.textContent = "connected";
    el.classList.remove("conn-error");
    el.classList.add("conn-ok");
  } else {
    el.textContent = "disconnected";
    el.classList.remove("conn-ok");
    el.classList.add("conn-error");
  }
}

/* ─────────── Freshness pill ─────────── */

let lastSuccessfulPollAt = null;     // Date.now() of last successful fetch

function ageString(seconds) {
  if (seconds < 60)  return `${seconds}s ago`;
  if (seconds < 3600) {
    const m = Math.floor(seconds / 60);
    const s = seconds % 60;
    return s ? `${m}m ${s}s ago` : `${m}m ago`;
  }
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  return m ? `${h}h ${m}m ago` : `${h}h ago`;
}

function classifyFreshness(ageSec, cadenceMs) {
  if (cadenceMs <= 0) return "paused";
  const cadenceSec = cadenceMs / 1000;
  if (ageSec <= cadenceSec * 1.5) return "live";
  if (ageSec <= cadenceSec * 3)   return "lagging";
  return "stale";
}

function tickFreshness() {
  const pill = document.getElementById("freshness-pill");
  if (!pill) return;
  const text = pill.querySelector(".freshness-text");
  const cadence = Number(document.getElementById("cadence")?.value || 10000);

  let state, label;
  if (lastSuccessfulPollAt === null) {
    state = "paused";
    label = cadence > 0 ? "starting…" : "paused";
  } else {
    const age = Math.max(0, Math.floor((Date.now() - lastSuccessfulPollAt) / 1000));
    state = classifyFreshness(age, cadence);
    if (state === "paused") {
      label = `paused · ${ageString(age)}`;
    } else {
      label = `${state} · ${ageString(age)}`;
    }
  }

  pill.classList.remove("live", "lagging", "stale", "paused");
  pill.classList.add(state);
  if (text) text.textContent = label;
}

function setSubLabels(payload) {
  const map = {
    "today-label":     payload.today_label     ? `live snapshot · ${payload.today_label}` : null,
    "yesterday-label": payload.yesterday_label ? `EOD · ${payload.yesterday_label}` : null,
    "monthly-label":   payload.month_start_label
      ? `cumulative · since ${payload.month_start_label}` : null,
  };
  for (const [id, txt] of Object.entries(map)) {
    if (txt) {
      const el = document.getElementById(id);
      if (el) el.textContent = txt;
    }
  }
}

/* ─────────── Volume Distribution table + donut pie chart ─────────── */

let _volChart = null;

function _cls(v) { return v > 0.005 ? "pos" : v < -0.005 ? "neg" : ""; }

function _buildRow(sym, r, rowClass) {
  return `<tr class="${rowClass}">
    <td><strong>${sym != null ? sym : "TOTAL"}</strong></td>
    <td class="num-col ${_cls(r.daily_pnl_usd)}">${formatMoney(r.daily_pnl_usd)}</td>
    <td class="num-col">${formatMoney(r.abs_notional_usd)}</td>
    <td class="num-col ${_cls(r.monthly_pnl_usd)}">${formatMoney(r.monthly_pnl_usd)}</td>
    <td class="num-col">${(r.buy_lots  || 0).toFixed(2)}</td>
    <td class="num-col">${(r.sell_lots || 0).toFixed(2)}</td>
    <td class="num-col ${_cls(-(r.net_lots || 0))}">${(r.net_lots || 0).toFixed(2)}</td>
    <td class="num-col ${_cls(r.notional_usd)}">${formatMoney(r.notional_usd)}</td>
    <td class="num-col ${_cls(r.swaps_usd)}">${formatMoney(r.swaps_usd)}</td>
    <td class="num-col">${formatMoney(r.commission_usd)}</td>
    <td class="num-col ${_cls(r.total_floating_usd)}">${formatMoney(r.total_floating_usd)}</td>
  </tr>`;
}

function renderVolumeDistribution(rows) {
  const tbody = document.getElementById("vol-tbody");
  const meta  = document.getElementById("vol-meta");
  if (!tbody) return;
  if (!rows || !rows.length) {
    tbody.innerHTML = '<tr><td colspan="11" class="vol-empty">No data</td></tr>';
    if (meta) meta.textContent = "";
    return;
  }
  if (meta) meta.textContent = `${rows.length} symbols`;

  // Aggregate totals
  const zero = {daily_pnl_usd:0, abs_notional_usd:0, monthly_pnl_usd:0,
                buy_lots:0, sell_lots:0, net_lots:0, notional_usd:0,
                swaps_usd:0, commission_usd:0, total_floating_usd:0};
  const T = rows.reduce((acc, r) => {
    for (const k of Object.keys(zero)) acc[k] += (r[k] || 0);
    return acc;
  }, {...zero});

  tbody.innerHTML =
    _buildRow(null, T, "row-total") +
    rows.map(r => _buildRow(
      r.symbol, r,
      (r.net_lots || 0) > 0.001 ? "row-long" :
      (r.net_lots || 0) < -0.001 ? "row-short" : ""
    )).join("");

  renderVolumePie(rows, T.abs_notional_usd);
}

const _VOL_PALETTE = ["#4f8ef7","#10c891","#f59e0b","#e94560","#a78bfa",
                      "#fb923c","#34d399","#60a5fa","#f472b6","#94a3b8"];

function _volPiePlugin(totalFmtRef) {
  return {
    id: "vol-centre",
    afterDraw(chart) {
      const { ctx, chartArea: { left, top, right, bottom } } = chart;
      const cx = (left + right) / 2, cy = (top + bottom) / 2;
      ctx.save();
      ctx.textAlign = "center"; ctx.textBaseline = "middle";
      ctx.fillStyle = "#d4a853"; ctx.font = "bold 15px -apple-system,sans-serif";
      ctx.fillText(totalFmtRef.v, cx, cy - 9);
      ctx.fillStyle = "#5a607a"; ctx.font = "10px -apple-system,sans-serif";
      ctx.fillText("Total", cx, cy + 10);
      ctx.restore();
    },
  };
}

// Mutable ref so the centre-text plugin can read the latest total without
// re-registering on every data update.
const _volTotalFmt = { v: "" };

function renderVolumePie(rows, totalAbs) {
  const canvas = document.getElementById("vol-pie");
  if (!canvas || typeof Chart === "undefined") return;

  const TOP    = 9;
  const sorted = [...rows].sort((a, b) => b.abs_notional_usd - a.abs_notional_usd);
  const top    = sorted.slice(0, TOP);
  const other  = sorted.slice(TOP).reduce((s, r) => s + (r.abs_notional_usd || 0), 0);
  const labels = [...top.map(r => r.symbol), ...(other > 0 ? ["Other"] : [])];
  const data   = [...top.map(r => r.abs_notional_usd), ...(other > 0 ? [other] : [])];
  const colors = [..._VOL_PALETTE.slice(0, TOP), "#5a607a"];

  _volTotalFmt.v = formatMoney(totalAbs);

  if (_volChart) {
    // Data-only update — no animation, no flicker.
    _volChart.data.labels                     = labels;
    _volChart.data.datasets[0].data           = data;
    _volChart.data.datasets[0].backgroundColor = colors;
    _volChart.update("none");  // "none" mode = skip animation entirely
    return;
  }

  // First render — create chart once with no animation.
  _volChart = new Chart(canvas.getContext("2d"), {
    type: "doughnut",
    data: { labels, datasets: [{ data, backgroundColor: colors,
                                  borderWidth: 1, borderColor: "rgba(0,0,0,.25)" }] },
    options: {
      animation: false,    // no animation on initial draw or updates
      cutout: "65%",
      plugins: {
        legend: {
          position: "right",
          labels: { color: "#a8aebc", boxWidth: 12, padding: 8, font: { size: 11 } },
        },
        tooltip: {
          callbacks: {
            label: ctx => {
              const pct = totalAbs > 0 ? ((ctx.raw / totalAbs) * 100).toFixed(1) : "0.0";
              return ` ${ctx.label}: ${pct}%`;
            },
          },
        },
      },
    },
    plugins: [_volPiePlugin(_volTotalFmt)],
  });
}

/* ─────────── Stale-while-revalidate cache (localStorage) ───────────
   We save every successful payload so the next page load can paint
   immediately from the last-known-good data while a fresh fetch runs
   in the background. The freshness pill picks up the cache's savedAt
   timestamp and naturally reads "stale" until the in-flight request
   lands and replaces it. */

const CACHE_KEY = "cro-metrics-cache:v1";

function loadCachedPayload() {
  try {
    const raw = localStorage.getItem(CACHE_KEY);
    if (!raw) return null;
    const obj = JSON.parse(raw);
    if (!obj || !obj.payload) return null;
    return obj;       // { savedAt, payload }
  } catch (e) {
    return null;
  }
}

function saveCachedPayload(payload) {
  try {
    localStorage.setItem(
      CACHE_KEY,
      JSON.stringify({ savedAt: Date.now(), payload }),
    );
  } catch (e) {
    /* quota exceeded / storage disabled — not fatal */
  }
}

/* ─────────── Polling ─────────── */

async function fetchMetrics({ manual = false } = {}) {
  // Cancel-in-flight: if a previous request is still pending, abort it.
  if (currentInflight) currentInflight.abort();
  const ctrl = new AbortController();
  currentInflight = ctrl;

  const btn = document.getElementById("manual-refresh");
  if (manual && btn) btn.classList.add("spinning");

  try {
    const resp = await fetch("/cro/metrics", { signal: ctrl.signal, cache: "no-store" });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();
    lastPayload = data;
    saveCachedPayload(data);

    const ranks = computeRanks(data);
    renderSection("today",     data.today,     ranks);
    renderSection("yesterday", data.yesterday, ranks);
    renderSection("monthly",   data.monthly,   ranks);
    renderVolumeDistribution(data.volume_distribution || []);
    setSubLabels(data);
    lastSuccessfulPollAt = Date.now();
    tickFreshness();
    setConnected(true);

    // If a metric is currently selected, refresh its panel content so
    // the "Current value" and sub-component values stay in sync.
    if (selected) renderFormulaPanel(selected.section, selected.key);
  } catch (err) {
    if (err && err.name === "AbortError") return;
    console.error("fetchMetrics failed:", err);
    setConnected(false);
  } finally {
    if (currentInflight === ctrl) currentInflight = null;
    if (manual && btn) btn.classList.remove("spinning");
  }
}

function applyCadence() {
  if (pollTimer !== null) {
    clearInterval(pollTimer);
    pollTimer = null;
  }
  const sel = document.getElementById("cadence");
  const ms = sel ? Number(sel.value) : 10000;
  if (!ms || ms <= 0) return;       // paused
  pollTimer = setInterval(() => fetchMetrics(), ms);
}

/* ─────────── Wire-up ─────────── */

function setCrossHover(group) {
  document.querySelectorAll(".metric.cross-hover").forEach((el) => {
    el.classList.remove("cross-hover");
  });
  if (!group) return;
  document.querySelectorAll(`.metric[data-group="${group}"]`).forEach((el) => {
    el.classList.add("cross-hover");
  });
}

function init() {
  // Click + cross-card hover delegation for metric rows.
  document.querySelectorAll(".metric-list").forEach((list) => {
    list.addEventListener("click", (ev) => {
      const row = ev.target.closest(".metric");
      if (!row) return;
      const section = row.dataset.section;
      const key = row.dataset.key;
      if (section && key) selectMetric(section, key);
    });
    list.addEventListener("mouseover", (ev) => {
      const row = ev.target.closest(".metric");
      if (!row) return;
      setCrossHover(row.dataset.group);
    });
    list.addEventListener("mouseleave", () => setCrossHover(null));
  });

  const cadence = document.getElementById("cadence");
  if (cadence) {
    cadence.addEventListener("change", () => {
      applyCadence();
      tickFreshness();          // immediately reflect "paused" / "live" change
    });
  }
  const refreshBtn = document.getElementById("manual-refresh");
  if (refreshBtn) {
    refreshBtn.addEventListener("click", () => fetchMetrics({ manual: true }));
  }

  // Wire up the Volume Distribution collapsible toggle.
  const volToggle = document.getElementById("vol-toggle");
  if (volToggle) {
    volToggle.addEventListener("click", () => {
      const sec  = document.getElementById("vol-section");
      const body = document.getElementById("vol-body");
      sec.classList.toggle("collapsed");
      const expanded = !sec.classList.contains("collapsed");
      volToggle.setAttribute("aria-expanded", String(expanded));
      body.hidden = !expanded;
    });
  }

  // Stale-while-revalidate: paint the last-known-good payload from
  // localStorage instantly so the page is never blank on load. The
  // freshness pill picks up the cache's savedAt and shows "Xs ago"
  // until the in-flight fetch below replaces the data.
  const cached = loadCachedPayload();
  if (cached && cached.payload) {
    lastPayload = cached.payload;
    const ranks = computeRanks(cached.payload);
    renderSection("today",     cached.payload.today,     ranks);
    renderSection("yesterday", cached.payload.yesterday, ranks);
    renderSection("monthly",   cached.payload.monthly,   ranks);
    renderVolumeDistribution(cached.payload.volume_distribution || []);
    setSubLabels(cached.payload);
    lastSuccessfulPollAt = cached.savedAt;
    tickFreshness();
  }

  fetchMetrics();
  applyCadence();

  // Update the freshness pill every second so "X ago" advances even between
  // polls. Independent of the polling cycle.
  setInterval(tickFreshness, 1000);
  tickFreshness();
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", init);
} else {
  init();
}
