"""Tests for cmtaccounting/recon-app/cro_metrics.py — the new Activity & Counts metrics.

Each test connects to a real Postgres (the SQL is Postgres-specific —
CTEs, ILIKE, ::int casts), creates a minimal schema, seeds focused
fixture data, asserts the metric, and rolls back via savepoint so tests
stay isolated.

Default test DB: localhost:5432 cro_db_test (the WSL Postgres
provisioned by MT5-CRO-Backend/setup-wsl-postgres.ps1).
Override via CRO_METRICS_TEST_DB_URL env var. If the DB is unreachable
the entire module is skipped with a clear message.

Run: pytest cmtaccounting/tests/test_cro_metrics.py -v
"""
from __future__ import annotations

import os
import sys
import pytest

# Make recon-app importable so we can pull in cro_metrics.
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO_ROOT, "recon-app"))

try:
    import psycopg2
    import psycopg2.extras
except ImportError:                                           # pragma: no cover
    pytest.skip("psycopg2 not installed", allow_module_level=True)

import cro_metrics


_DEFAULT_DSN = os.environ.get(
    "CRO_METRICS_TEST_DB_URL",
    "host=localhost port=5432 dbname=cro_db_test "
    "user=cro password=swmlryv9r3haLdXrCftlfNFf0WJY",
)


# ── Schema helpers ──────────────────────────────────────────────────────

# Minimal subset of MT5-CRO-Backend/app/models.py — only the columns the
# six new metric queries actually read. Non-partitioned variants of the
# real `closed_positions` and `daily_reports` tables (partitioning isn't
# semantically relevant for these unit tests).
_SCHEMA = """
DROP TABLE IF EXISTS accounts_snapshot CASCADE;
DROP TABLE IF EXISTS deposits_withdrawals CASCADE;
DROP TABLE IF EXISTS closed_positions CASCADE;
DROP TABLE IF EXISTS positions_snapshot CASCADE;
DROP TABLE IF EXISTS external_rates CASCADE;
DROP TABLE IF EXISTS exposure_snapshot CASCADE;
DROP TABLE IF EXISTS deals CASCADE;
DROP TABLE IF EXISTS internal_rates CASCADE;

CREATE TABLE accounts_snapshot (
    login        BIGINT PRIMARY KEY,
    group_name   TEXT NOT NULL DEFAULT '',
    registration BIGINT NOT NULL DEFAULT 0,
    balance      DOUBLE PRECISION NOT NULL DEFAULT 0,
    equity       DOUBLE PRECISION NOT NULL DEFAULT 0,
    credit       DOUBLE PRECISION NOT NULL DEFAULT 0,
    floating     DOUBLE PRECISION NOT NULL DEFAULT 0,
    currency     TEXT NOT NULL DEFAULT 'USD'
);

CREATE TABLE deposits_withdrawals (
    ticket   BIGINT PRIMARY KEY,
    login    BIGINT NOT NULL,
    action   SMALLINT NOT NULL,
    time     BIGINT NOT NULL,
    amount   DOUBLE PRECISION NOT NULL,
    currency TEXT NOT NULL DEFAULT 'USD',
    comment  TEXT NOT NULL DEFAULT ''
);

CREATE TABLE closed_positions (
    ticket      BIGINT PRIMARY KEY,
    login       BIGINT NOT NULL,
    symbol      TEXT NOT NULL,
    open_time   BIGINT NOT NULL,
    close_time  BIGINT NOT NULL,
    profit      DOUBLE PRECISION NOT NULL DEFAULT 0,
    storage     DOUBLE PRECISION NOT NULL DEFAULT 0,
    commission  DOUBLE PRECISION NOT NULL DEFAULT 0,
    fee         DOUBLE PRECISION NOT NULL DEFAULT 0,
    currency    TEXT NOT NULL DEFAULT 'USD',
    rate_profit DOUBLE PRECISION NOT NULL DEFAULT 0
);

CREATE TABLE positions_snapshot (
    position_id   BIGINT PRIMARY KEY,
    login         BIGINT NOT NULL,
    symbol        TEXT NOT NULL,
    action        SMALLINT NOT NULL DEFAULT 0,   -- 0=BUY 1=SELL
    volume_ext    BIGINT NOT NULL DEFAULT 0,
    contract_size DOUBLE PRECISION NOT NULL DEFAULT 1,
    price_current DOUBLE PRECISION NOT NULL DEFAULT 0,
    profit        DOUBLE PRECISION NOT NULL DEFAULT 0,
    storage       DOUBLE PRECISION NOT NULL DEFAULT 0
);

CREATE TABLE external_rates (
    currency TEXT PRIMARY KEY,
    bid      DOUBLE PRECISION NOT NULL,
    ask      DOUBLE PRECISION NOT NULL,
    usd_base BOOLEAN NOT NULL DEFAULT FALSE
);

-- Minimal internal_rates stub (cro_metrics only reads currency, bid, ask, usd_base)
CREATE TABLE internal_rates (
    currency TEXT PRIMARY KEY,
    bid      DOUBLE PRECISION NOT NULL DEFAULT 0,
    ask      DOUBLE PRECISION NOT NULL DEFAULT 0,
    usd_base BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE exposure_snapshot (
    symbol          TEXT PRIMARY KEY,
    volume_clients  DOUBLE PRECISION NOT NULL DEFAULT 0,
    volume_coverage DOUBLE PRECISION NOT NULL DEFAULT 0,
    volume_net      DOUBLE PRECISION NOT NULL DEFAULT 0
);

-- `deals` mirrors the relevant subset of MT5-CRO-Backend's `Deal` model.
-- Non-partitioned variant for tests; partitioning is irrelevant for SUM queries.
CREATE TABLE deals (
    ticket           BIGINT NOT NULL,
    time             BIGINT NOT NULL,
    login            BIGINT NOT NULL DEFAULT 0,
    action           SMALLINT NOT NULL DEFAULT 0,
    entry            SMALLINT NOT NULL DEFAULT 0,
    symbol           TEXT NOT NULL DEFAULT '',
    notional_usd     DOUBLE PRECISION NOT NULL DEFAULT 0,
    spread_cost_usd  DOUBLE PRECISION NOT NULL DEFAULT 0,
    PRIMARY KEY (ticket, time)
);
"""


@pytest.fixture(scope="module")
def conn():
    """Module-scoped connection. Creates schema once, drops on teardown."""
    try:
        c = psycopg2.connect(_DEFAULT_DSN, connect_timeout=3)
    except psycopg2.OperationalError as e:
        pytest.skip(
            f"test Postgres unavailable at {_DEFAULT_DSN.split()[0]}: {e}\n"
            "Provision via MT5-CRO-Backend/setup-wsl-postgres.ps1 or set "
            "CRO_METRICS_TEST_DB_URL."
        )
    c.autocommit = False
    with c.cursor() as cur:
        cur.execute(_SCHEMA)
    c.commit()
    yield c
    # Final cleanup — drop everything we created.
    with c.cursor() as cur:
        cur.execute(
            "DROP TABLE IF EXISTS accounts_snapshot, deposits_withdrawals, "
            "closed_positions, positions_snapshot, external_rates, deals, "
            "exposure_snapshot, internal_rates CASCADE"
        )
    c.commit()
    c.close()


@pytest.fixture
def cur(conn):
    """Per-test RealDictCursor inside a SAVEPOINT that rolls back after."""
    with conn.cursor() as plain_cur:
        plain_cur.execute("SAVEPOINT test_isolation")
    real_cur = psycopg2.extras.RealDictCursor(conn)
    yield real_cur
    real_cur.close()
    with conn.cursor() as plain_cur:
        plain_cur.execute("ROLLBACK TO SAVEPOINT test_isolation")
        plain_cur.execute("RELEASE SAVEPOINT test_isolation")


# ── Time anchors used across tests ──────────────────────────────────────

# All timestamps below are made-up epoch ints. The numbers are chosen to
# place each fixture row clearly inside or outside specific windows.
DAY = 86400
TODAY_START      = 1_746_230_400   # arbitrary fixed epoch (a Saturday UTC)
YEST_START       = TODAY_START - DAY
DAY_BEFORE_START = TODAY_START - 2 * DAY
LAST_MONTH_TIME  = TODAY_START - 30 * DAY
TODAY_END        = TODAY_START + DAY


def _seed_dw(cur, ticket, login, action, time, amount,
             currency="USD", comment=""):
    cur.execute(
        "INSERT INTO deposits_withdrawals (ticket, login, action, time, amount, "
        "currency, comment) VALUES (%s,%s,%s,%s,%s,%s,%s)",
        (ticket, login, action, time, amount, currency, comment),
    )


def _seed_acct(cur, login, registration, group_name="CMV\\real"):
    cur.execute(
        "INSERT INTO accounts_snapshot (login, group_name, registration) "
        "VALUES (%s,%s,%s)",
        (login, group_name, registration),
    )


def _seed_closed(cur, ticket, login, symbol, open_time, close_time):
    cur.execute(
        "INSERT INTO closed_positions (ticket, login, symbol, open_time, close_time) "
        "VALUES (%s,%s,%s,%s,%s)",
        (ticket, login, symbol, open_time, close_time),
    )


def _seed_position(cur, position_id, login, symbol="EURUSD",
                   action=0, volume_ext=100_000_000, contract_size=1.0,
                   price_current=1.0, profit=0.0, storage=0.0):
    cur.execute(
        "INSERT INTO positions_snapshot"
        " (position_id, login, symbol, action, volume_ext,"
        "  contract_size, price_current, profit, storage)"
        " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (position_id, login, symbol, action, volume_ext,
         contract_size, price_current, profit, storage),
    )


def _seed_deal(cur, ticket, time, login=100, action=0, entry=1,
               symbol="EURUSD", notional_usd=0.0, spread_cost_usd=0.0):
    cur.execute(
        "INSERT INTO deals (ticket, time, login, action, entry, symbol, "
        "notional_usd, spread_cost_usd) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
        (ticket, time, login, action, entry, symbol, notional_usd, spread_cost_usd),
    )


def _seed_external_rate(cur, currency, bid, ask, usd_base=False):
    cur.execute(
        "INSERT INTO external_rates (currency, bid, ask, usd_base) "
        "VALUES (%s,%s,%s,%s) ON CONFLICT (currency) DO UPDATE SET "
        "bid=EXCLUDED.bid, ask=EXCLUDED.ask, usd_base=EXCLUDED.usd_base",
        (currency, bid, ask, usd_base),
    )


# ── #Traders tests ──────────────────────────────────────────────────────

class TestNTraders:

    def test_distinct_logins(self, cur):
        _seed_closed(cur, 1, login=100, symbol="EURUSD",
                     open_time=YEST_START, close_time=TODAY_START + 1000)
        _seed_closed(cur, 2, login=100, symbol="GBPUSD",
                     open_time=YEST_START, close_time=TODAY_START + 2000)
        _seed_closed(cur, 3, login=200, symbol="EURUSD",
                     open_time=YEST_START, close_time=TODAY_START + 3000)
        assert cro_metrics.n_traders(cur, TODAY_START, TODAY_END) == 2

    def test_excludes_zeroing_symbols(self, cur):
        _seed_closed(cur, 1, login=100, symbol="Zeroing_EURUSD",
                     open_time=YEST_START, close_time=TODAY_START + 1000)
        _seed_closed(cur, 2, login=200, symbol="EURUSD",
                     open_time=YEST_START, close_time=TODAY_START + 1000)
        assert cro_metrics.n_traders(cur, TODAY_START, TODAY_END) == 1

    def test_excludes_inactivity_symbols(self, cur):
        _seed_closed(cur, 1, login=100, symbol="inactivity-fee",
                     open_time=YEST_START, close_time=TODAY_START + 1000)
        _seed_closed(cur, 2, login=200, symbol="EURUSD",
                     open_time=YEST_START, close_time=TODAY_START + 1000)
        assert cro_metrics.n_traders(cur, TODAY_START, TODAY_END) == 1

    def test_period_boundary_excludes_to_ts(self, cur):
        # close_time == to_ts is excluded (half-open interval [from, to))
        _seed_closed(cur, 1, login=100, symbol="EURUSD",
                     open_time=YEST_START, close_time=TODAY_END)
        assert cro_metrics.n_traders(cur, TODAY_START, TODAY_END) == 0

    def test_empty_result_returns_zero(self, cur):
        assert cro_metrics.n_traders(cur, TODAY_START, TODAY_END) == 0


# ── #Active Traders tests ───────────────────────────────────────────────

class TestNActiveTraders:

    def test_live_distinct_logins(self, cur):
        _seed_position(cur, 1, login=100)
        _seed_position(cur, 2, login=100)            # same login, different positions
        _seed_position(cur, 3, login=200)
        assert cro_metrics.n_active_traders_live(cur) == 2

    def test_live_excludes_zeroing(self, cur):
        _seed_position(cur, 1, login=100, symbol="Zeroing_EURUSD")
        _seed_position(cur, 2, login=200, symbol="EURUSD")
        assert cro_metrics.n_active_traders_live(cur) == 1

    def test_period_union_open_or_close(self, cur):
        # opened in window, closes later: counts (via open_time)
        _seed_closed(cur, 1, login=100, symbol="EURUSD",
                     open_time=TODAY_START + 1000, close_time=TODAY_END + 500)
        # opened earlier, closed in window: counts (via close_time)
        _seed_closed(cur, 2, login=200, symbol="EURUSD",
                     open_time=DAY_BEFORE_START, close_time=TODAY_START + 2000)
        # entirely outside the window: doesn't count
        _seed_closed(cur, 3, login=300, symbol="EURUSD",
                     open_time=DAY_BEFORE_START, close_time=YEST_START + 100)
        assert cro_metrics.n_active_traders_period(cur, TODAY_START, TODAY_END) == 2


# ── #Depositors tests ───────────────────────────────────────────────────

class TestNDepositors:

    def test_distinct_logins_with_positive_deposits(self, cur):
        _seed_dw(cur, 1, login=100, action=2, time=TODAY_START + 100, amount=500)
        _seed_dw(cur, 2, login=100, action=2, time=TODAY_START + 200, amount=300)
        _seed_dw(cur, 3, login=200, action=2, time=TODAY_START + 300, amount=1000)
        assert cro_metrics.n_depositors(cur, TODAY_START, TODAY_END) == 2

    def test_excludes_bonus(self, cur):
        _seed_dw(cur, 1, login=100, action=2, time=TODAY_START + 100,
                 amount=50, comment="welcome bonus")
        _seed_dw(cur, 2, login=200, action=2, time=TODAY_START + 100,
                 amount=500, comment="real deposit")
        assert cro_metrics.n_depositors(cur, TODAY_START, TODAY_END) == 1

    def test_excludes_fees_placeholder(self, cur):
        _seed_dw(cur, 1, login=100, action=2, time=TODAY_START + 100,
                 amount=50, comment="fees placeholder")
        _seed_dw(cur, 2, login=200, action=2, time=TODAY_START + 100,
                 amount=500, comment="real deposit")
        assert cro_metrics.n_depositors(cur, TODAY_START, TODAY_END) == 1

    def test_excludes_spread_charge(self, cur):
        _seed_dw(cur, 1, login=100, action=2, time=TODAY_START + 100,
                 amount=50, comment="spread charge")
        _seed_dw(cur, 2, login=200, action=2, time=TODAY_START + 100,
                 amount=500, comment="real deposit")
        assert cro_metrics.n_depositors(cur, TODAY_START, TODAY_END) == 1

    def test_only_positive_amounts(self, cur):
        # withdrawals (amount<0 with action=2) are not "depositors"
        _seed_dw(cur, 1, login=100, action=2, time=TODAY_START + 100, amount=-200)
        _seed_dw(cur, 2, login=200, action=2, time=TODAY_START + 200, amount=300)
        assert cro_metrics.n_depositors(cur, TODAY_START, TODAY_END) == 1


# ── #New Acc Regs tests ─────────────────────────────────────────────────

class TestNNewRegistrations:

    def test_in_period(self, cur):
        _seed_acct(cur, 100, registration=TODAY_START + 100)
        _seed_acct(cur, 200, registration=TODAY_START + 200)
        _seed_acct(cur, 300, registration=YEST_START + 100)   # outside
        assert cro_metrics.n_new_registrations(cur, TODAY_START, TODAY_END) == 2

    def test_excludes_test_groups(self, cur):
        _seed_acct(cur, 100, registration=TODAY_START + 100, group_name="CMV\\real")
        _seed_acct(cur, 200, registration=TODAY_START + 100, group_name="CMV\\test")
        _seed_acct(cur, 300, registration=TODAY_START + 100, group_name="testbed")
        assert cro_metrics.n_new_registrations(cur, TODAY_START, TODAY_END) == 1


# ── #FTD tests ──────────────────────────────────────────────────────────

class TestNFTD:

    def test_first_deposit_in_period(self, cur):
        # login 100's first-ever deposit is today → FTD
        _seed_dw(cur, 1, login=100, action=2, time=TODAY_START + 100, amount=500)
        # login 200's first-ever deposit was last month → not today's FTD
        _seed_dw(cur, 2, login=200, action=2, time=LAST_MONTH_TIME, amount=500)
        _seed_dw(cur, 3, login=200, action=2, time=TODAY_START + 200, amount=500)
        assert cro_metrics.n_ftd(cur, TODAY_START, TODAY_END) == 1

    def test_uses_min_time_per_login(self, cur):
        # second-deposit row in period must NOT count if first was earlier
        _seed_dw(cur, 1, login=100, action=2, time=YEST_START + 100, amount=500)
        _seed_dw(cur, 2, login=100, action=2, time=TODAY_START + 100, amount=200)
        assert cro_metrics.n_ftd(cur, TODAY_START, TODAY_END) == 0

    def test_does_not_exclude_bonus_for_candidacy(self, cur):
        # C# CollectFirstValidDepositDates does NOT filter bonus comments
        # — bonus deposits CAN be the first-deposit anchor.
        _seed_dw(cur, 1, login=100, action=2, time=TODAY_START + 100,
                 amount=50, comment="welcome bonus")
        assert cro_metrics.n_ftd(cur, TODAY_START, TODAY_END) == 1

    def test_excludes_fees_placeholder(self, cur):
        # but fees-placeholder rows are not deposits at all
        _seed_dw(cur, 1, login=100, action=2, time=TODAY_START + 100,
                 amount=50, comment="fees placeholder")
        assert cro_metrics.n_ftd(cur, TODAY_START, TODAY_END) == 0


# ── FTD Amount tests ────────────────────────────────────────────────────

class TestFTDAmount:

    def test_sum_period_deposits_for_ftd_logins(self, cur):
        # login 100 is a today-FTD with two today deposits (500 + 200)
        _seed_dw(cur, 1, login=100, action=2, time=TODAY_START + 100, amount=500)
        _seed_dw(cur, 2, login=100, action=2, time=TODAY_START + 200, amount=200)
        # login 200 is NOT a today-FTD (first was yesterday)
        _seed_dw(cur, 3, login=200, action=2, time=YEST_START + 100, amount=1000)
        _seed_dw(cur, 4, login=200, action=2, time=TODAY_START + 100, amount=300)
        assert cro_metrics.ftd_amount_usd(cur, TODAY_START, TODAY_END) == pytest.approx(700.0)

    def test_excludes_bonus_for_amount(self, cur):
        # C# excludes bonus from the AMOUNT sum (cash-only), even though
        # bonus deposits CAN make a login an FTD.
        _seed_dw(cur, 1, login=100, action=2, time=TODAY_START + 100,
                 amount=50, comment="welcome bonus")          # makes login 100 an FTD
        _seed_dw(cur, 2, login=100, action=2, time=TODAY_START + 200,
                 amount=400, comment="real deposit")
        # FTD amount = 400 (bonus excluded), not 450
        assert cro_metrics.ftd_amount_usd(cur, TODAY_START, TODAY_END) == pytest.approx(400.0)

    def test_currency_conversion_via_external_rates(self, cur):
        # EUR deposit converted to USD via mid-rate
        _seed_external_rate(cur, "EUR", bid=1.10, ask=1.12, usd_base=False)
        _seed_dw(cur, 1, login=100, action=2, time=TODAY_START + 100,
                 amount=100, currency="EUR")
        # Mid-rate for foreign-base (usd_base=False) = (bid+ask)/2 = 1.11
        # Result = 100 * 1.11 = 111.0
        assert cro_metrics.ftd_amount_usd(cur, TODAY_START, TODAY_END) == pytest.approx(111.0)


# ── Integration: collect_all_metrics shape ──────────────────────────────

class TestCollectAllMetricsShape:

    def test_includes_new_keys(self, cur):
        # Empty DB → all queries return 0/0.0, but the JSON shape must
        # include the new keys under each of today/yesterday/monthly.
        # collect_all_metrics also needs daily_reports / internal_rates
        # / exposure_snapshot to exist (other queries hit them) — create
        # the missing tables for this integration test only.
        # exposure_snapshot is now in _SCHEMA so it already exists.
        # daily_reports / internal_rates are not in _SCHEMA — create them here.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS daily_reports (
                login BIGINT, datetime BIGINT, currency TEXT DEFAULT 'USD',
                balance DOUBLE PRECISION DEFAULT 0,
                credit DOUBLE PRECISION DEFAULT 0,
                profit DOUBLE PRECISION DEFAULT 0,
                profit_storage DOUBLE PRECISION DEFAULT 0,
                profit_equity DOUBLE PRECISION DEFAULT 0,
                group_name TEXT DEFAULT '');
            CREATE TABLE IF NOT EXISTS internal_rates (
                currency TEXT PRIMARY KEY, bid DOUBLE PRECISION DEFAULT 0,
                ask DOUBLE PRECISION DEFAULT 0, usd_base BOOLEAN DEFAULT FALSE);
        """)
        data = cro_metrics.collect_all_metrics(cur)
        for section in ("today", "yesterday", "monthly"):
            assert section in data
            for k in ("n_traders", "n_active_traders", "n_depositors",
                      "n_new_regs", "n_ftd", "ftd_amount_usd",
                      "volume_usd", "spread_usd"):
                assert k in data[section], f"{section}.{k} missing"
                # Empty DB → all counts/sums are 0
                assert data[section][k] == 0 or data[section][k] == 0.0
        # volume_distribution is a top-level key (blends live + daily + MTD)
        assert "volume_distribution" in data
        assert isinstance(data["volume_distribution"], list)
        assert "volume_distribution" not in data["today"]
        assert "volume_distribution" not in data["yesterday"]
        assert "volume_distribution" not in data["monthly"]


# ── Exposure by symbol tests ────────────────────────────────────────────

class TestVolumeDistribution:
    """Tests for the 11-column volume_distribution query.
    volume_ext / 1e8 = lots; positions_snapshot columns match the expanded _SCHEMA."""

    def _pos(self, cur, pid, login, symbol, action=0,
             volume_ext=100_000_000, contract_size=1.0,
             price=1.0, profit=0.0, storage=0.0):
        cur.execute(
            "INSERT INTO positions_snapshot"
            " (position_id, login, symbol, action, volume_ext,"
            "  contract_size, price_current, profit, storage)"
            " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (pid, login, symbol, action, volume_ext,
             contract_size, price, profit, storage),
        )

    def _closed(self, cur, ticket, login, symbol, close_time,
                profit=0.0, commission=0.0, storage=0.0, fee=0.0):
        cur.execute(
            "INSERT INTO closed_positions"
            " (ticket, login, symbol, open_time, close_time,"
            "  profit, storage, commission, fee)"
            " VALUES (%s,%s,%s,0,%s,%s,%s,%s,%s)",
            (ticket, login, symbol, close_time, profit, storage, commission, fee),
        )

    def test_buy_sell_lots(self, cur):
        self._pos(cur, 1, 100, "EURUSD", action=0, volume_ext=100_000_000)  # 1.0 lot buy
        self._pos(cur, 2, 101, "EURUSD", action=1, volume_ext=50_000_000)   # 0.5 lot sell
        rows = cro_metrics.volume_distribution(cur, TODAY_START, TODAY_END, YEST_START)
        r = next(x for x in rows if x["symbol"] == "EURUSD")
        assert r["buy_lots"]  == pytest.approx(1.0)
        assert r["sell_lots"] == pytest.approx(0.5)

    def test_net_lots_broker_perspective(self, cur):
        # Net = sell - buy (broker's perspective)
        self._pos(cur, 1, 100, "XAUUSD", action=0, volume_ext=200_000_000)  # 2 lots buy
        self._pos(cur, 2, 101, "XAUUSD", action=1, volume_ext=300_000_000)  # 3 lots sell
        rows = cro_metrics.volume_distribution(cur, TODAY_START, TODAY_END, YEST_START)
        r = next(x for x in rows if x["symbol"] == "XAUUSD")
        assert r["net_lots"] == pytest.approx(1.0)   # sell(3) - buy(2) = 1 → broker net long

    def test_floating_pnl_and_swaps(self, cur):
        self._pos(cur, 1, 100, "USDJPY", profit=500.0, storage=-50.0)
        rows = cro_metrics.volume_distribution(cur, TODAY_START, TODAY_END, YEST_START)
        r = next(x for x in rows if x["symbol"] == "USDJPY")
        assert r["floating_pnl_usd"]   == pytest.approx(500.0)
        assert r["swaps_usd"]          == pytest.approx(-50.0)
        assert r["total_floating_usd"] == pytest.approx(450.0)

    def test_daily_pnl_from_closed_positions(self, cur):
        self._closed(cur, 1, 100, "GBPUSD", close_time=TODAY_START + 100,
                     profit=200.0, commission=-5.0)
        # Today-window closed PnL = 200 - 5 = 195
        rows = cro_metrics.volume_distribution(cur, TODAY_START, TODAY_END, YEST_START)
        r = next((x for x in rows if x["symbol"] == "GBPUSD"), None)
        assert r is not None
        assert r["daily_pnl_usd"] == pytest.approx(195.0)

    def test_monthly_pnl_includes_outside_today(self, cur):
        # A closed position from yesterday counts in monthly but not daily
        self._closed(cur, 1, 100, "EURUSD", close_time=YEST_START + 100, profit=100.0)
        rows = cro_metrics.volume_distribution(cur, TODAY_START, TODAY_END, YEST_START)
        r = next((x for x in rows if x["symbol"] == "EURUSD"), None)
        assert r is not None
        assert r["monthly_pnl_usd"] == pytest.approx(100.0)
        assert r["daily_pnl_usd"]   == pytest.approx(0.0)

    def test_floating_pnl_and_swaps_fx_converted(self, cur):
        """floating_pnl and swaps_usd must be converted using the same fx factor
        as the notional — they are in the symbol's native profit currency."""
        cur.execute(
            "INSERT INTO internal_rates (currency, bid, ask, usd_base)"
            " VALUES ('JPY', 156.0, 158.0, TRUE) ON CONFLICT (currency)"
            " DO UPDATE SET bid=EXCLUDED.bid, ask=EXCLUDED.ask, usd_base=EXCLUDED.usd_base",
        )
        # USDJPY: profit = 157,000 JPY, storage = -31,400 JPY
        # fx = 2/(156+158) = 1/157; 157000/157 ≈ 1000 USD; -31400/157 ≈ -200 USD
        self._pos(cur, 1, 100, "USDJPY",
                  volume_ext=100_000_000, contract_size=100_000.0, price=157.0,
                  profit=157_000.0, storage=-31_400.0)
        rows = cro_metrics.volume_distribution(cur, TODAY_START, TODAY_END, YEST_START)
        r = next(x for x in rows if x["symbol"] == "USDJPY")
        assert r["floating_pnl_usd"]   == pytest.approx(1000.0, rel=0.01)
        assert r["swaps_usd"]          == pytest.approx(-200.0, rel=0.01)
        assert r["total_floating_usd"] == pytest.approx(800.0,  rel=0.01)

    def test_fx_conversion_via_internal_rates(self, cur):
        # USDJPY quote_ccy = "JPY"; seed IR with mid ~157
        cur.execute(
            "INSERT INTO internal_rates (currency, bid, ask, usd_base)"
            " VALUES ('JPY', 156.0, 158.0, TRUE) ON CONFLICT (currency)"
            " DO UPDATE SET bid=EXCLUDED.bid, ask=EXCLUDED.ask, usd_base=EXCLUDED.usd_base",
        )
        # 1 lot (volume_ext=1e8), contract_size=100000, price=157 → gross_native=15.7M JPY
        # fx = 2/(156+158) = 0.00637; abs_notional_usd ≈ 15.7M × 0.00637 ≈ 100k USD
        self._pos(cur, 1, 100, "USDJPY",
                  volume_ext=100_000_000, contract_size=100_000.0, price=157.0)
        rows = cro_metrics.volume_distribution(cur, TODAY_START, TODAY_END, YEST_START)
        r = next(x for x in rows if x["symbol"] == "USDJPY")
        assert r["abs_notional_usd"] == pytest.approx(100_000, rel=0.01)

    def test_empty_returns_empty(self, cur):
        rows = cro_metrics.volume_distribution(cur, TODAY_START, TODAY_END, YEST_START)
        assert rows == []

    def test_all_keys_present(self, cur):
        self._pos(cur, 1, 100, "BTCUSD")
        rows = cro_metrics.volume_distribution(cur, TODAY_START, TODAY_END, YEST_START)
        assert len(rows) == 1
        r = rows[0]
        for k in ("symbol", "buy_lots", "sell_lots", "net_lots",
                  "abs_notional_usd", "notional_usd", "floating_pnl_usd",
                  "swaps_usd", "total_floating_usd",
                  "daily_pnl_usd", "monthly_pnl_usd", "commission_usd"):
            assert k in r, f"missing key: {k}"


# ── Volume USD tests ────────────────────────────────────────────────────

class TestVolumeUsd:

    def test_sums_notional_usd_in_period(self, cur):
        _seed_deal(cur, ticket=1, time=TODAY_START + 100, action=0, entry=0,
                   notional_usd=500_000.0)
        _seed_deal(cur, ticket=2, time=TODAY_START + 200, action=0, entry=1,
                   notional_usd=500_000.0)
        # outside window: ignored
        _seed_deal(cur, ticket=3, time=YEST_START + 100, action=1, entry=1,
                   notional_usd=999_999.0)
        result = cro_metrics.volume_usd(cur, TODAY_START, TODAY_END)
        assert result == pytest.approx(1_000_000.0)

    def test_action_filter_only_buy_sell(self, cur):
        # action=2 (DEAL_BALANCE) → must be filtered out even if notional_usd is set
        _seed_deal(cur, ticket=1, time=TODAY_START + 100, action=2, entry=0,
                   notional_usd=999_999.0)
        _seed_deal(cur, ticket=2, time=TODAY_START + 200, action=0, entry=1,
                   notional_usd=100_000.0)
        result = cro_metrics.volume_usd(cur, TODAY_START, TODAY_END)
        assert result == pytest.approx(100_000.0)

    def test_excludes_zeroing_symbol(self, cur):
        _seed_deal(cur, ticket=1, time=TODAY_START + 100, symbol="Zeroing_USD",
                   action=0, notional_usd=999_999.0)
        _seed_deal(cur, ticket=2, time=TODAY_START + 200, symbol="EURUSD",
                   action=0, notional_usd=100_000.0)
        result = cro_metrics.volume_usd(cur, TODAY_START, TODAY_END)
        assert result == pytest.approx(100_000.0)

    def test_excludes_inactivity_symbol(self, cur):
        _seed_deal(cur, ticket=1, time=TODAY_START + 100, symbol="inactivity-fee",
                   action=0, notional_usd=999_999.0)
        _seed_deal(cur, ticket=2, time=TODAY_START + 200, symbol="EURUSD",
                   action=0, notional_usd=100_000.0)
        result = cro_metrics.volume_usd(cur, TODAY_START, TODAY_END)
        assert result == pytest.approx(100_000.0)

    def test_period_boundary_excludes_to_ts(self, cur):
        _seed_deal(cur, ticket=1, time=TODAY_END, action=0, notional_usd=1_000_000.0)
        result = cro_metrics.volume_usd(cur, TODAY_START, TODAY_END)
        assert result == 0.0

    def test_empty_period_returns_zero(self, cur):
        result = cro_metrics.volume_usd(cur, TODAY_START, TODAY_END)
        assert result == 0.0

    def test_both_legs_counted(self, cur):
        # One position, opening leg + closing leg both contribute
        _seed_deal(cur, ticket=1, time=TODAY_START + 100, action=0, entry=0,
                   notional_usd=500_000.0)
        _seed_deal(cur, ticket=2, time=TODAY_START + 200, action=0, entry=1,
                   notional_usd=505_000.0)
        result = cro_metrics.volume_usd(cur, TODAY_START, TODAY_END)
        assert result == pytest.approx(1_005_000.0)


# ── Spread USD tests ────────────────────────────────────────────────────

class TestSpreadUsd:

    def test_sums_spread_cost_usd(self, cur):
        _seed_deal(cur, ticket=1, time=TODAY_START + 100, action=0,
                   spread_cost_usd=15.0)
        _seed_deal(cur, ticket=2, time=TODAY_START + 200, action=1,
                   spread_cost_usd=23.5)
        result = cro_metrics.spread_usd(cur, TODAY_START, TODAY_END)
        assert result == pytest.approx(38.5)

    def test_action_filter_only_buy_sell(self, cur):
        _seed_deal(cur, ticket=1, time=TODAY_START + 100, action=2,
                   spread_cost_usd=999.0)
        _seed_deal(cur, ticket=2, time=TODAY_START + 200, action=0,
                   spread_cost_usd=12.0)
        result = cro_metrics.spread_usd(cur, TODAY_START, TODAY_END)
        assert result == pytest.approx(12.0)

    def test_excludes_zeroing_inactivity(self, cur):
        _seed_deal(cur, ticket=1, time=TODAY_START + 100, symbol="Zeroing_OUT",
                   action=0, spread_cost_usd=99.0)
        _seed_deal(cur, ticket=2, time=TODAY_START + 200, symbol="market-inactivity",
                   action=0, spread_cost_usd=99.0)
        _seed_deal(cur, ticket=3, time=TODAY_START + 300, symbol="EURUSD",
                   action=0, spread_cost_usd=10.0)
        result = cro_metrics.spread_usd(cur, TODAY_START, TODAY_END)
        assert result == pytest.approx(10.0)

    def test_period_boundary(self, cur):
        _seed_deal(cur, ticket=1, time=YEST_START - 1000, action=0,
                   spread_cost_usd=99.0)
        _seed_deal(cur, ticket=2, time=YEST_START + 100, action=0,
                   spread_cost_usd=10.0)
        result = cro_metrics.spread_usd(cur, YEST_START, TODAY_START)
        assert result == pytest.approx(10.0)

    def test_empty_period(self, cur):
        assert cro_metrics.spread_usd(cur, TODAY_START, TODAY_END) == 0.0
