"""Deep-dive comparison for XAUUSD Daily PnL vs Dealio.

Dealio screenshot (today):
  XAUUSD: Daily PnL=70,571.63  ABS Notional=15,375,601  Monthly PnL=-290,495.75
           Buy Vol=229.29  Sell Vol=195.44  Net Vol=22.93 (approx)
           Swaps=-1,455,590

Run: python debug_xauusd.py
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "recon-app"))

import psycopg2, psycopg2.extras
from datetime import datetime, timedelta, timezone

DSN = "postgresql://cro:bTiBZzbU2gtAfA5BfPdR5PFcpLqcteu@213.199.45.213:5432/cro_db"

def main():
    conn = psycopg2.connect(DSN)
    conn.set_session(readonly=True, autocommit=True)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    now_utc      = datetime.now(timezone.utc)
    today_start  = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    month_start  = now_utc.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    ts_today     = int(today_start.timestamp())
    ts_end       = ts_today + 86400
    ts_month     = int(month_start.timestamp())

    print(f"As of: {now_utc.strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"today_start={ts_today}  today_end={ts_end}  month_start={ts_month}")
    print()

    # 1. Live positions for XAUUSD
    cur.execute("""
        SELECT
            SUM(CASE WHEN action=0 THEN volume_ext/100000000.0 ELSE 0 END) AS buy_lots,
            SUM(CASE WHEN action=1 THEN volume_ext/100000000.0 ELSE 0 END) AS sell_lots,
            SUM(profit) AS floating_profit_raw,
            SUM(storage) AS storage_raw,
            COUNT(*) AS position_count,
            MIN(price_current) AS price_min, MAX(price_current) AS price_max
        FROM positions_snapshot
        WHERE symbol = 'XAUUSD'
    """)
    pos = cur.fetchone() or {}
    buy  = float(pos.get("buy_lots") or 0)
    sell = float(pos.get("sell_lots") or 0)
    print("=== LIVE POSITIONS (positions_snapshot) ===")
    print(f"  Buy lots : {buy:>12.2f}")
    print(f"  Sell lots: {sell:>12.2f}")
    print(f"  Net lots (broker=sell-buy): {sell-buy:>12.2f}")
    print(f"  Floating profit (raw, sum): {float(pos.get('floating_profit_raw') or 0):>14,.2f}")
    print(f"  Storage   (raw, sum):       {float(pos.get('storage_raw') or 0):>14,.2f}")
    print(f"  Positions: {pos.get('position_count')}  price range: {pos.get('price_min')}-{pos.get('price_max')}")
    print()

    # 2. Closed positions today
    cur.execute("""
        SELECT
            COUNT(*) AS n_trades,
            SUM(profit) AS profit,
            SUM(storage) AS storage,
            SUM(commission) AS commission,
            SUM(fee) AS fee,
            SUM(profit + storage + commission + fee) AS total_pnl,
            MIN(close_time) AS first_close, MAX(close_time) AS last_close
        FROM closed_positions
        WHERE symbol = 'XAUUSD'
          AND close_time >= %(t)s AND close_time < %(te)s
    """, {"t": ts_today, "te": ts_end})
    cl_today = cur.fetchone() or {}
    print("=== CLOSED POSITIONS TODAY (closed_positions) ===")
    print(f"  Trades   : {cl_today.get('n_trades')}")
    print(f"  Profit   : {float(cl_today.get('profit') or 0):>14,.2f}")
    print(f"  Storage  : {float(cl_today.get('storage') or 0):>14,.2f}")
    print(f"  Commission: {float(cl_today.get('commission') or 0):>14,.2f}")
    print(f"  Fee      : {float(cl_today.get('fee') or 0):>14,.2f}")
    print(f"  TOTAL PnL (client): {float(cl_today.get('total_pnl') or 0):>14,.2f}")
    print(f"  TOTAL PnL (broker): {-float(cl_today.get('total_pnl') or 0):>14,.2f}")
    if cl_today.get('first_close'):
        print(f"  Time range: {cl_today.get('first_close')} -> {cl_today.get('last_close')}")
    print()

    # 3. Closed positions MTD
    cur.execute("""
        SELECT
            COUNT(*) AS n_trades,
            SUM(profit + storage + commission + fee) AS total_pnl
        FROM closed_positions
        WHERE symbol = 'XAUUSD'
          AND close_time >= %(m)s AND close_time < %(te)s
    """, {"m": ts_month, "te": ts_end})
    cl_mtd = cur.fetchone() or {}
    print("=== CLOSED POSITIONS MTD ===")
    print(f"  Trades   : {cl_mtd.get('n_trades')}")
    print(f"  Total PnL (client): {float(cl_mtd.get('total_pnl') or 0):>14,.2f}")
    print(f"  Total PnL (broker): {-float(cl_mtd.get('total_pnl') or 0):>14,.2f}")
    print()

    # 4. Sample of individual closed trades today (largest abs profit)
    cur.execute("""
        SELECT login, ticket, open_time, close_time,
               volume_ext/100000000.0 AS lots,
               open_price, close_price,
               profit, storage, commission, fee,
               profit+storage+commission+fee AS total
        FROM closed_positions
        WHERE symbol = 'XAUUSD'
          AND close_time >= %(t)s AND close_time < %(te)s
        ORDER BY ABS(profit + storage + commission + fee) DESC
        LIMIT 15
    """, {"t": ts_today, "te": ts_end})
    trades = cur.fetchall() or []
    print("=== TOP 15 CLOSED TRADES TODAY (by |PnL|) ===")
    print(f"  {'ticket':>12}  {'lots':>8}  {'open':>9}  {'close':>9}  {'profit':>10}  {'comm':>8}  {'total':>10}")
    for t in trades:
        print(f"  {t['ticket']:>12}  {float(t['lots']):>8.2f}  "
              f"{float(t['open_price']):>9.2f}  {float(t['close_price']):>9.2f}  "
              f"{float(t['profit']):>10,.2f}  {float(t['commission']):>8,.2f}  "
              f"{float(t['total']):>10,.2f}")
    print()

    # 5. Check currency of closed_positions for XAUUSD (should be USD)
    cur.execute("""
        SELECT DISTINCT currency, COUNT(*) AS n
        FROM closed_positions
        WHERE symbol = 'XAUUSD'
          AND close_time >= %(t)s AND close_time < %(te)s
        GROUP BY currency
    """, {"t": ts_today, "te": ts_end})
    ccys = cur.fetchall() or []
    print("=== CURRENCIES in closed_positions today ===")
    for r in ccys:
        print(f"  currency='{r['currency']}'  count={r['n']}")
    print()

    # 6. Dealio comparison
    our_daily_client  = float(cl_today.get("total_pnl") or 0)
    our_daily_broker  = -our_daily_client
    our_mtd_client    = float(cl_mtd.get("total_pnl") or 0)
    our_mtd_broker    = -our_mtd_client
    dealio_daily      = 70_571.63
    dealio_monthly    = -290_495.75

    print("=== COMPARISON vs DEALIO ===")
    print(f"  Dealio Daily PnL (broker):  {dealio_daily:>14,.2f}")
    print(f"  Ours Daily PnL (broker):    {our_daily_broker:>14,.2f}")
    print(f"  Gap (settled only):         {dealio_daily - our_daily_broker:>14,.2f}  <- delta floating not yet in our table")
    print()
    print(f"  Dealio Monthly PnL:         {dealio_monthly:>14,.2f}")
    print(f"  Ours Monthly PnL (broker):  {our_mtd_broker:>14,.2f}")
    print(f"  Gap (MTD):                  {dealio_monthly - our_mtd_broker:>14,.2f}")
    print()

    # 7. All symbols total for today
    cur.execute("""
        SELECT SUM(profit + storage + commission + fee) AS total
        FROM closed_positions
        WHERE close_time >= %(t)s AND close_time < %(te)s
          AND symbol NOT ILIKE 'Zeroing%%'
          AND symbol NOT ILIKE '%%inactivity%%'
    """, {"t": ts_today, "te": ts_end})
    r = cur.fetchone() or {}
    total_settled = float(r.get("total") or 0)
    print("=== ALL SYMBOLS TOTAL SETTLED TODAY ===")
    print(f"  Client: {total_settled:>14,.2f}")
    print(f"  Broker: {-total_settled:>14,.2f}")
    print(f"  Dealio total: 185,403.20")
    print(f"  Gap from Dealio: {185403.20 - (-total_settled):>14,.2f}")

    # 8. Full volume_distribution() output for XAUUSD
    from cro_metrics import volume_distribution
    rows = volume_distribution(cur, ts_today, ts_end, ts_month)
    xau = next((r for r in rows if r["symbol"] == "XAUUSD"), None)
    print()
    print("=== volume_distribution() for XAUUSD (what dashboard shows) ===")
    if xau:
        for k, v in xau.items():
            if isinstance(v, float):
                print(f"  {k:<25}: {v:>16,.2f}")
            else:
                print(f"  {k:<25}: {v}")
    print()

    # Dealio reference
    print("=== DEALIO REFERENCE (XAUUSD) ===")
    print(f"  Daily PnL             :     70,571.63")
    print(f"  ABS Notional ($)      : 15,375,601.00")
    print(f"  Monthly PnL           :   -290,495.75")
    print(f"  Buy Volume            :        229.29")
    print(f"  Sell Volume           :        195.44")
    print(f"  Net Volume            :  ~22.93 (approx, broker short)")
    print(f"  Notional ($)          : -15,375,601 (approx)")
    print(f"  Swaps ($)             : -1,455,590.00")
    print(f"  Commission            :          0.00")
    print(f"  Total Floating PnL    : ~-20,857,031 (approx)")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
