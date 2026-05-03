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
    position_id BIGINT PRIMARY KEY,
    login       BIGINT NOT NULL,
    symbol      TEXT NOT NULL
);

CREATE TABLE external_rates (
    currency TEXT PRIMARY KEY,
    bid      DOUBLE PRECISION NOT NULL,
    ask      DOUBLE PRECISION NOT NULL,
    usd_base BOOLEAN NOT NULL DEFAULT FALSE
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
            "closed_positions, positions_snapshot, external_rates CASCADE"
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


def _seed_position(cur, position_id, login, symbol="EURUSD"):
    cur.execute(
        "INSERT INTO positions_snapshot (position_id, login, symbol) "
        "VALUES (%s,%s,%s)",
        (position_id, login, symbol),
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
            CREATE TABLE IF NOT EXISTS exposure_snapshot (
                volume_net DOUBLE PRECISION DEFAULT 0);
        """)
        data = cro_metrics.collect_all_metrics(cur)
        for section in ("today", "yesterday", "monthly"):
            assert section in data
            for k in ("n_traders", "n_active_traders", "n_depositors",
                      "n_new_regs", "n_ftd", "ftd_amount_usd"):
                assert k in data[section], f"{section}.{k} missing"
                # Empty DB → all counts/sums are 0
                assert data[section][k] == 0 or data[section][k] == 0.0
