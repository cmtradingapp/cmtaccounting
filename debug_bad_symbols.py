"""Inspect backfill profit values and implied prices for GERMANY30 and GBPUSD."""
import psycopg2, psycopg2.extras
DSN = "postgresql://cro:bTiBZzbU2gtAfA5BfPdR5PFcpLqcteu@213.199.45.213:5432/cro_db"
conn = psycopg2.connect(DSN)
conn.set_session(readonly=True, autocommit=True)
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

for sym in ("GERMANY30", "GBPUSD", "GBPJPY", "EURGBP", "XAUUSD"):
    print(f"\n=== {sym} ===")

    # Current positions_snapshot sample
    cur.execute("""
        SELECT action, price_open, price_current, volume_ext/1e8 AS lots,
               contract_size, profit, storage
        FROM positions_snapshot WHERE symbol = %s LIMIT 3
    """, (sym,))
    rows = cur.fetchall() or []
    for r in rows:
        print(f"  Current: action={r['action']} open={r['price_open']:.5f} "
              f"current={r['price_current']:.5f} lots={float(r['lots']):.2f} "
              f"profit={float(r['profit']):.2f}")

    # SOD backfill sample + implied price
    cur.execute("""
        SELECT ps.position_id, ps.login, ps.profit AS sod_profit, ps.storage AS sod_storage,
               snap.action, snap.price_open, snap.volume_ext/1e8 AS lots, snap.contract_size,
               snap.rate_profit
        FROM positions_sod ps
        JOIN positions_snapshot snap ON snap.position_id = ps.position_id
        WHERE ps.snapshot_date = '2026-05-01' AND ps.symbol = %s
        LIMIT 5
    """, (sym,))
    rows = cur.fetchall() or []
    for r in rows:
        lots     = float(r['lots'] or 0)
        contract = float(r['contract_size'] or 1)
        rate     = float(r['rate_profit'] or 1) or 1
        open_px  = float(r['price_open'] or 0)
        sod_p    = float(r['sod_profit'] or 0)
        direction = 1.0 if int(r['action'] or 0) == 0 else -1.0
        # reverse-engineer the implied price
        if lots * contract * rate != 0:
            implied_px = open_px + sod_p / (lots * contract * direction * rate)
        else:
            implied_px = 0
        print(f"  SOD: action={r['action']} open={open_px:.5f} "
              f"sod_profit={sod_p:.2f}  implied_may1_price={implied_px:.5f}")

    # Net SOD floating
    cur.execute("""
        SELECT SUM(profit + storage) AS raw_total, COUNT(*) AS n
        FROM positions_sod WHERE snapshot_date = '2026-05-01' AND symbol = %s
    """, (sym,))
    r = cur.fetchone() or {}
    print(f"  SOD total raw profit+storage: {float(r.get('raw_total') or 0):>14,.2f}  n={r.get('n')}")

cur.close(); conn.close()
