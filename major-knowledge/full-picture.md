# The Full Picture — Data, People, Files, Purpose

---

```
WHO PROVIDES IT          WHAT IT IS                    WHICH FILES (today)                    WHAT IT DOES IN THE SYSTEM
════════════════         ══════════════                ═══════════════════════                ══════════════════════════

GIL                      CRM Transactions              CRM Transactions                       THE MASTER LIST.
(CRM owner)              Every deposit and             Additional info.xlsx                   Every column in the Lifecycle
                         withdrawal the company                                               Report comes from here.
                         knows about.                                                         The matching engine joins
                                                                                              PSP files against this.
                                                        ┌─────────────────────────────────────────────────────────────┐
                                                        │ Key columns used:                                           │
                                                        │  psp_transaction_id  ← the join key vs PSP files           │
                                                        │  transactionid       ← fallback join key                    │
                                                        │  amount / usdamount  ← the amounts we compare              │
                                                        │  login               ← client's MT4 account number         │
                                                        │  payment_processor   ← tells us which PSP to expect        │
                                                        │  payment_method      ← credit card / wire / crypto etc.    │
                                                        │  transactiontype     ← Deposit or Withdraw                  │
                                                        │  currency_id         ← numeric FK → ISO currency code      │
                                                        │  first_name/last_name, comment, bank_name, ewalletid...    │
                                                        └─────────────────────────────────────────────────────────────┘

────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────

DESPINA                  PSP Statements                PSPs/ folder (~20 files):              MATCH CONFIRMATION ONLY.
(Accounting,             What the payment              Nuvei.xlsx                             The engine checks: does
manages PSP &            processors actually           Zotapay.csv                            this CRM transaction ID
bank relationships)      processed and settled.        Korapay Pay-ins.csv                    appear in any PSP file?
                                                        Korapay Payouts.csv                    Yes → Matched.
                         Downloaded manually            SolidPayments.csv                      No  → Unmatched CRM.
                         today. Should come             EFTpay 1.csv / EFTpay 2.csv
                         via Praxis API.                Finrax all.xlsx                        Also provides bank_amount
                                                        Ozow Deposits/Refunds.csv              for the discrepancy check.
                                                        Neteller group/processing CSVs
                                                        Skrill Processing.csv
                                                        TrustPayments.csv
                                                        VP Deposits/Refunds.csv
                                                        Directa24.csv
                                                        Inatec.csv
                                                        Swiffy.csv
                                                        + Payabl, LetKnow, others

DESPINA                  Bank Statements               Banks/ folder (4 banks):               SAME AS PSP — match
(manages bank            Wire transfers that            Standard Jan 2023 Zar.pdf              confirmation for clients
relationships)           bypassed PSP gateways.         Standard all.csv                       who deposited/withdrew
                                                        Nedbank Client Funds.csv               via bank wire instead
                         Downloaded manually.           NedbankBlackstone.csv                  of a PSP gateway.
                         No API confirmed yet.          ABSA USD January.xls
                                                        SD BANK GROUP USD.pdf
                                                        Dixipay EUR B2C account.pdf
                                                        Corp banks 1.23.xlsx

────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────

ELISE                    Client Equity Report          platform/Unrealised.xlsx               OPENING BALANCES ONLY.
(Report rules,           Snapshot of every             (or equityFile.csv from uploads)       Not used in matching.
knows the               client account's                                                       Populates the
lifecycle format)        balance and equity             Key columns:                           "1. Opening Balance"
                         at end of period.              Login, Currency, Real Equity           row per account in the
                                                                                               CCY and USD lifecycle
                         Currently: Tableau             ← Rules for generating this            tabs of the Excel output.
                         export from MT4 data.          from MT4 raw data are
                         Future: generated              known to Elis and will be
                         from ETL DB directly.          documented for automation.

────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────

IBRAHIM / LEONIDAS       MT4 Platform Data             Not in MRS dataset yet.                NOT USED YET.
(MT4 / trading           Actual trades, account                                               Future: trades_open,
platform owners)         balances, deal numbers,        Need to establish:                     trades_closed, and
                         equity from the trading         - DB access or API                    account equity will
                         platform itself.               - Which fields are available           come from here instead
                                                        - Link between MT4 deal no             of from Tableau/CRM.
                                                          and CRM mtorder_id

────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────

EXTERNAL API             FX Rates                      Live API calls:                        EUR/USD CONVERSION ONLY.
(Frankfurter /           Daily exchange rates           Frankfurter (28 fiat pairs)            Not used in matching.
ECB, CoinGecko)          for currency conversion.       CoinGecko (5 crypto pairs)             Used in:
                                                                                               1. FX widget in the UI
                         Already fully automated.       Legacy files (not used):               2. EUR/USD totals in
                         No human involved.             platform/Rates.xlsx                       the Balances export
                                                        platform/Rates 2.xlsx
                                                        platform/Rates 3.xlsx

────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────

ALEH                     The Reconciliation            web-gui/server.py                      PRODUCES THE OUTPUT.
(MRS development)        Engine                                                               Reads all inputs above,
                                                        Joins CRM ↔ PSP on                    runs the match, and
                                                        psp_transaction_id /                   generates:
                                                        transactionid                          - Lifecycle Report.xlsx
                                                                                                 (7 tabs)
                                                        Normalises keys:                       - Balances.xlsx
                                                        strip whitespace,                      - Issues Report.xlsx
                                                        remove .0 suffix,
                                                        strip leading zeros,                   Delivered today:
                                                        uppercase                               manually downloaded.
                                                                                               Future: auto-deposited
                                                        Detects PSP column names               to OneDrive on schedule.
                                                        via overlap-based
                                                        heuristics (~20 PSPs)

────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────

DESPINA /                Lifecycle Report              Life Cycle Report YYYY-MM-DD.xlsx      THE END PRODUCT.
Accounting team          The final output.             Balances YYYY-MM-DD.xlsx               What accounting uses
(consumers)                                            Issues YYYY-MM-DD.xlsx                 for financial reporting.
                         Currently stored in
                         OneDrive manually.            7 tabs in Lifecycle Report:
                                                        MT4-Transactions
                                                        MT4 CCY Life Cycle     ← uses equity
                                                        MT4 USD Life Cycle     ← uses equity + FX
                                                        PM USD Life Cycle      ← uses FX
                                                        PM CCY Life Cycle
                                                        PM-Transactions
                                                        Mapping Rules
```

---

## In one sentence per person

| Person | Their role in this system |
|---|---|
| **Gil** | Provides the master list of all client transactions — the CRM file is the foundation of everything |
| **Despina** | Provides the PSP statements and bank statements that confirm each transaction actually happened |
| **Elise** | Provides the equity snapshot (opening balances) and knows the rules for generating it from MT4 data |
| **Ibrahim / Leonidas** | Own MT4 — the trading platform data isn't in the system yet but will be needed for trades and equity in the ETL world |
| **Aleh** | Builds and runs the matching engine; produces the Lifecycle Report |
| **The lead** | Building the central ETL database that will replace all manual file exchanges above with direct DB reads |

---

## What changes when the ETL DB exists

Every arrow that currently says "manual download / manual export" becomes a scheduled DB read. The matching logic moves from Python heuristics to a SQL JOIN on already-clean data. Aleh's system stops ingesting files and starts querying tables. The output is auto-generated and auto-delivered to OneDrive. Nobody touches it manually.