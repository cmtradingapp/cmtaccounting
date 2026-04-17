"""Scp the recon-app/cro_queries.py over and run it inside the recon-app
container to validate the SQL against the live Dealio replica.
"""
from __future__ import annotations

from pathlib import Path
import paramiko


def _server_creds():
    here = Path(__file__).resolve()
    for parent in here.parents:
        f = parent / "putty-creds.txt"
        if f.exists():
            kv = {}
            for line in f.read_text().splitlines():
                if ":" in line:
                    k, v = line.split(":", 1)
                    kv[k.strip().lower()] = v.strip()
            return kv["host"], kv["username"], kv["password"]
    raise SystemExit("putty-creds.txt not found in repo tree")


HOST, USER, PASS = _server_creds()


def run_remote(client: paramiko.SSHClient, cmd: str, timeout: int = 300) -> int:
    _, stdout, stderr = client.exec_command(cmd, timeout=timeout)
    out = stdout.read().decode("utf-8", errors="replace")
    err = stderr.read().decode("utf-8", errors="replace")
    if out: print(out)
    if err: print("--stderr--"); print(err)
    return stdout.channel.recv_exit_status()


def main() -> int:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(HOST, username=USER, password=PASS, timeout=15)

    sftp = client.open_sftp()
    sftp.put(
        "c:/Projects/cmtaccounting/recon-app/cro_queries.py",
        "/root/recon-app/cro_queries.py",
    )
    sftp.close()
    print("[OK] Uploaded cro_queries.py")

    remote_py = r'''
from datetime import date, timedelta
import cro_queries as q
d = date.today()
for attempt in range(10):
    try:
        snap = q.day_snapshot(d)
        if snap["n_accounts"] or snap["n_deals"]:
            break
    except Exception as e:
        print("retry day_snapshot:", e)
    d -= timedelta(days=1)
print("=== DAY", d, "===")
for k in ("label","n_accounts","n_deals","n_active_traders","n_traders","n_depositors","n_ftd","n_retention_depositors","pnl","closed_pnl","delta_floating","net_deposits","volume_usd","equity","floating_pnl","balance","credit","wd_equity"):
    print(f"  {k:25s} {snap[k]!r}")
print("by_group top 5:")
for r in q.perf_by_group(d, d)[:5]:
    print(" ", r)
print("by_symbol top 5:")
for r in q.volume_by_symbol(d, d)[:5]:
    print(" ", r)
series = q.daily_series(d - timedelta(days=7), d)
print("last 7 days:")
for r in series[:7]:
    print(" ", r["date"], "pnl=", f"{r['pnl']:.0f}", "net_dep=", f"{r['net_deposits']:.0f}", "eq=", f"{r['equity']:.0f}")
'''
    cmd = "docker exec -i recon-app-recon-1 python -c " + "'" + remote_py.replace("'", "'\\''") + "'"
    return run_remote(client, cmd)


if __name__ == "__main__":
    raise SystemExit(main())
