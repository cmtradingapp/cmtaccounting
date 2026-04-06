"""
Script 1: Index all files in the AGREEMENTS folder.
Output: scripts/output/agreements_index.json
Run: python scripts/index_agreements.py
"""
import os
import json
from pathlib import Path
from collections import defaultdict

AGREEMENTS_DIR = Path(__file__).parent.parent / "relevant-data" / "AGREEMENTS"
OUTPUT_FILE    = Path(__file__).parent / "output" / "agreements_index.json"


def main():
    if not AGREEMENTS_DIR.exists():
        print(f"ERROR: Folder not found: {AGREEMENTS_DIR}")
        return

    index = []
    ext_counts = defaultdict(int)
    psp_folders = set()

    for root, dirs, files in os.walk(AGREEMENTS_DIR):
        # Skip hidden/system folders
        dirs[:] = [d for d in dirs if not d.startswith(".")]
        root_path = Path(root)
        rel_root  = root_path.relative_to(AGREEMENTS_DIR)

        # Determine top-level PSP folder name
        parts = rel_root.parts
        psp_folder = parts[0] if parts else "(root)"

        for fname in files:
            if fname.startswith("."):
                continue
            fpath = root_path / fname
            ext   = fname.rsplit(".", 1)[-1].lower() if "." in fname else "(none)"
            size_kb = round(fpath.stat().st_size / 1024, 1)

            entry = {
                "psp_folder": psp_folder,
                "subfolder":  str(rel_root) if parts else "",
                "filename":   fname,
                "ext":        ext,
                "size_kb":    size_kb,
                "path":       str(fpath),
            }
            index.append(entry)
            ext_counts[ext] += 1
            psp_folders.add(psp_folder)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(index, f, indent=2, ensure_ascii=False)

    print(f"\n{'='*60}")
    print(f"AGREEMENTS INDEX — {len(index)} files across {len(psp_folders)} PSP folders")
    print(f"{'='*60}")
    print(f"\nFiles by extension:")
    for ext, count in sorted(ext_counts.items(), key=lambda x: -x[1]):
        print(f"  .{ext:<12} {count:>4}")

    print(f"\nPSP folders ({len(psp_folders)}):")
    for folder in sorted(psp_folders):
        folder_files = [e for e in index if e["psp_folder"] == folder]
        exts = defaultdict(int)
        for e in folder_files:
            exts[e["ext"]] += 1
        ext_summary = ", ".join(f"{v}×{k}" for k, v in sorted(exts.items(), key=lambda x: -x[1]))
        print(f"  {folder:<30} {len(folder_files):>3} files  [{ext_summary}]")

    print(f"\nIndex written to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
