"""Upload cro-bridge/ directory to /root/cro-bridge/ on the server."""
from __future__ import annotations

import os
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
    raise SystemExit("putty-creds.txt not found")


def main() -> int:
    host, user, pw = _server_creds()
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    c.connect(host, username=user, password=pw, timeout=15)

    src_root = Path(r"c:/Projects/cmtaccounting/cro-bridge")
    dest_root = "/root/cro-bridge"

    # Ensure target dir exists
    c.exec_command(f"mkdir -p {dest_root}/sdk-libs")

    sftp = c.open_sftp()
    existing_dirs = set()
    for local_path in src_root.rglob("*"):
        if local_path.is_dir():
            continue
        rel = local_path.relative_to(src_root).as_posix()
        remote = f"{dest_root}/{rel}"
        rdir = remote.rsplit("/", 1)[0]
        if rdir not in existing_dirs:
            try:
                sftp.stat(rdir)
            except IOError:
                try:
                    sftp.mkdir(rdir)
                except Exception:
                    pass
            existing_dirs.add(rdir)
        sftp.put(str(local_path), remote)
        size = local_path.stat().st_size
        print(f"  {rel}  ({size:,} B)")
    sftp.close()

    # List uploaded
    _, so, _ = c.exec_command(f"ls -la {dest_root}/ {dest_root}/sdk-libs/")
    print()
    print(so.read().decode())
    c.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
