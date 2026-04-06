"""
Extract all unique currency pairs from the historical Rates files.
Scans every Rates*.xlsx in the MRS directory tree, reads the
BASE CURRENCY and CURRENCY columns, and deduplicates.
"""
import os
import pandas as pd
from collections import Counter

MRS_ROOT = r"C:\Projects\cmtaccounting\relevant-data\MRS"

pairs = Counter()
files_scanned = 0
errors = []

for root, dirs, files in os.walk(MRS_ROOT):
    for f in files:
        lower = f.lower()
        if 'rate' in lower and (lower.endswith('.xlsx') or lower.endswith('.xls') or lower.endswith('.csv')):
            path = os.path.join(root, f)
            try:
                df = pd.read_excel(path, nrows=0) if lower.endswith(('.xlsx', '.xls')) else pd.read_csv(path, nrows=0)
                cols = [c.upper().strip() for c in df.columns]
                
                # Re-read with data if it looks like a rates file
                if 'CURRENCY' in cols or 'BASE CURRENCY' in cols:
                    df = pd.read_excel(path) if lower.endswith(('.xlsx', '.xls')) else pd.read_csv(path)
                    
                    if 'BASE CURRENCY' in cols and 'CURRENCY' in cols:
                        for _, row in df[['BASE CURRENCY', 'CURRENCY']].drop_duplicates().iterrows():
                            pair = f"{row['BASE CURRENCY']}/{row['CURRENCY']}"
                            pairs[pair] += 1
                    elif 'CURRENCY' in cols:
                        for c in df['CURRENCY'].dropna().unique():
                            pairs[f"USD/{c}"] += 1
                
                files_scanned += 1
                print(f"  ✓ {f}")
            except Exception as e:
                errors.append((f, str(e)))

print(f"\n{'='*50}")
print(f"Files scanned: {files_scanned}")
print(f"Errors: {len(errors)}")
print(f"\nUnique currency pairs found ({len(pairs)}):")
print(f"{'='*50}")
for pair, count in pairs.most_common():
    print(f"  {pair:12s}  (appeared in {count} rows)")

if errors:
    print(f"\nErrors:")
    for fn, err in errors:
        print(f"  ✗ {fn}: {err}")
