# CRO "All in One" Dashboard Replication — Data Source Questions

We're replicating the CRO "All in One" panel. We know MetaTrader 5 Manager and CRM are involved. Please answer as many as possible — even partial answers help.

---

## A. General Architecture & Data Sources

1. Which **databases/systems** does this dashboard pull from? (e.g., CRM/Antelope vtiger, MetaTrader 5 Manager API, Praxis PSP, Dealio replica, a separate data warehouse, etc.)
2. Is there a **central data warehouse or ETL pipeline** that pre-aggregates the data, or does the dashboard query each source directly at render time?
3. What is the **refresh rate** of the data shown? Real-time, hourly, daily snapshot, or on-demand?
4. Are there any **materialized views, staging tables, or scheduled jobs** that prepare the data for this dashboard? If yes, what are their names and schedules?
5. Which **database server(s)/connection strings** does the dashboard connect to? (e.g., `cmtmainserver.database.windows.net`, the Dealio replica, an internal OLAP cube, etc.)
6. Is there a **schema diagram or ER diagram** available for the tables this dashboard reads from?

## B. Top-Level Filters & Dimensions

7. What are **all the filter controls** at the top of the panel? (Date range, agent, desk, office, department, campaign, etc.)
8. When filtering by **date range**, which timestamp field is used? (e.g., transaction confirmation_time, created_at, MT5 deal time, etc.)
9. Does the date filter apply to **registration date, deposit date, trade date, or login date** — or different fields for different metrics?
10. When filtering by **agent/operator**, which field links clients to agents? (e.g., `assigned_to` in vtiger_account, a separate assignment table, etc.)
11. Is there a concept of **"agent hierarchy"** — do team leaders see their whole team's data, or only their own clients?
12. What is **"CRO"** in "CRO All in One"? Is it a specific role, a department, or an acronym for something like "Client Retention Officer" or "Conversion Rate Optimization"?

## C. Top Stat Cards — Financial Metrics

13. **"Monthly Pro Cash (Q)"** — What exactly does this metric measure? Is it profit from cash transactions? What does "(Q)" mean — quarterly? What table/query produces it?
14. **"CRO Pro"** (showing -$63,610) — What is this? Agent P&L? Client P&L attributed to the agent? Net deposits minus costs? What formula/query?
15. **"Info Net Deposits"** — Is this `SUM(deposits) - SUM(withdrawals)` for the period? Which table — CRM vtiger_mttransactions, Praxis transactions, or MT5 deals?
16. **The large dollar amounts** ($220,812, -$952,336, $1,461,473) — Are these in USD? If clients deposit in other currencies, how is FX conversion handled? What exchange rate source and date?
17. **"889" / "2,944"** — What do these counts represent? Number of trades? Number of clients? Number of transactions?
18. **"49" / "509"** — Are these FTD (first-time deposit) counts? Active trader counts? Something else?
19. **"59,138" / "1,274,652"** — These look like trading volumes. Are they in lots, USD notional, or units? From MT5 Manager deals table?
20. **"19.3M" / "2.3B"** — What are these very large numbers? Total trading volume in USD? Total exposure? P&L in cents?
21. **"4.6k"** — Count of what? Total clients? Active clients in the period? Registered accounts?

## D. Client / Account Metrics

22. What is the definition of an **"active client"** in this dashboard? (e.g., logged in during period, placed a trade, has open positions, has deposited)
23. How is **"FTD" (First Time Deposit)** defined and calculated? Is it the first-ever deposit for an account, or the first deposit in the period? Which table stores the FTD flag?
24. What table maps **clients to agents**? Is it `vtiger_account.assigned_to`, `vtiger_trading_accounts`, or something else?
25. Are **demo accounts** excluded from all metrics, or are they included in some counts?
26. How are **"Funded" vs "Unfunded" clients** distinguished? Is there a `countdeposits > 0` flag, a separate field, or derived from transaction history?
27. What is the definition of **"Cida Dealership Instrum…"** (partially visible in the screenshot)? Is this "Client Instruments" — the number of distinct instruments traded?

## E. Trading Metrics (MetaTrader 5)

28. Which **MT5 Manager API** calls or database tables provide trading data? (`MT5Deal`, `MT5Position`, `MT5Trade`, or a replicated table?)
29. Is the MT5 data replicated to a **SQL database** (e.g., Dealio replica at `cmtrading-replicadb.dealio.ai:5106`), or is it queried live via the MT5 Manager API?
30. How is **trading volume** calculated — sum of `lot * contract_size` in USD? Or sum of deal amounts?
31. How is **client P&L** calculated — sum of `profit` field from closed MT5 deals? Does it include swaps, commissions, and fees?
32. How is **company P&L / "CRO Pro"** calculated? Is it the negative of client P&L (when client loses, company gains)?
33. What is the relationship between **MT5 login** and **CRM account ID**? Is there a mapping table? (e.g., `vtiger_trading_accounts.login` ↔ `vtiger_account.accountid`)
34. Are **MT4 trades** also included, or is this MT5-only?
35. How are **B-book vs A-book** positions handled in the P&L calculation? Are hedged/routed trades excluded from company P&L?
36. Is there a **spread cost** metric shown in the daily table? How is it calculated — difference between ask and bid at execution time, or a fixed spread per instrument?

## F. Deposit & Withdrawal Metrics

37. Which table provides **deposit and withdrawal data** — CRM `vtiger_mttransactions`, Praxis `praxis_transactions`, MT5 deals (balance operations), or all three?
38. How are **pending vs approved vs rejected** transactions filtered? Only `transactionapproval = 'Approved'`?
39. How are **internal transfers** (between trading accounts of the same client) handled — excluded from net deposits?
40. How are **bonuses/credits** handled — are they counted as deposits?
41. What is the difference between **"Monthly Pro Cash"** and **"Net Deposits"** — are they the same metric with different filters, or different calculations?
42. How are **chargebacks** and **reversed transactions** handled in the deposit/withdrawal calculations?

## G. Daily Performance Tables

43. The two **"Daily Performance"** tables visible — what are the columns? (Please provide exact column names and their meanings)
44. Is each row in the daily table a **client**, a **trade**, a **day**, or a **client-day aggregate**?
45. What does **"with spreads"** mean in the table title? Is there a version without spreads?
46. What are the **spread-related columns** — bid-ask spread in pips, spread cost in USD, spread per instrument?
47. Are there any **calculated columns** that combine data from multiple sources (e.g., CRM client info + MT5 trade data + deposit data in the same row)?
48. Is there a **drill-down** from the summary cards to the detail tables? (e.g., clicking a stat filters the table)

## H. Data Freshness, Caching & Performance

49. How often is the **MT5 trade data** synced/replicated? (real-time, every minute, every hour, daily?)
50. How often is the **CRM data** synced? (real-time, nightly ETL, etc.)
51. Are there any **pre-computed aggregation tables** (daily/weekly/monthly summaries) that the dashboard reads from instead of computing on the fly?
52. What is the **typical query execution time** for rendering this dashboard? (under 1s, 5-10s, 30s+?)
53. Are there any **known performance bottlenecks**? (We're currently seeing `vtiger_account` lock/slow queries on Azure SQL)

## I. Access & Permissions

54. Who can see this dashboard? Is it **role-based** (admin, manager, agent) with different data visibility?
55. Do agents see **only their own clients' data**, or do some roles see team/office/company-wide data?
56. Are there any **API endpoints** we can call to get this data programmatically, or must we query the databases directly?

## J. Implementation Details

57. What **technology stack** was used to build this dashboard? (Tableau, Power BI, custom web app, Grafana, etc.)
58. Can you share the **SQL queries or stored procedures** that power each metric card?
59. Can you share the **database view definitions** (if materialized views are used)?
60. Are there any **third-party data feeds** integrated (e.g., market data for spread calculations, FX rate providers)?
61. Is there any **data from the Praxis PSP** shown in this dashboard, or is everything from CRM + MT5?
62. Are there any **REST APIs or WebSocket feeds** from MT5 Manager that we should integrate with?

---

## Priority — what we need first to start building:

**Critical (blocks everything):**
- Questions 1-2 (data sources)
- Questions 13-21 (metric definitions)
- Questions 28-29 (MT5 data access method)
- Questions 37-38 (deposit source)
- Questions 43-47 (daily table schema)
- Question 58 (the actual SQL)

**Important but can iterate:**
- Questions 7-12 (filters)
- Questions 22-27 (client definitions)
- Questions 49-53 (performance/caching)
