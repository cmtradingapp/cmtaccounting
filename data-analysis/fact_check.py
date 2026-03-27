import pandas as pd

path = r"C:\Projects\MethodosReconciliationSystem\Data\Reconciliation-Relevant\Life cycle report\2023\1. January\List.xlsx"
df = pd.read_excel(path)

total = len(df)
timing = int(df["IsTiming"].sum()) if "IsTiming" in df.columns else 0
non_timing = total - timing
matched = int(df["Match No"].notna().sum())
unmatched = int(df["Match No"].isna().sum())
unrecon = df["Unrecon. Fees"].sum() if "Unrecon. Fees" in df.columns else 0
unique_groups = df["Match No"].nunique()
matched_by = df["Matched By"].dropna().unique()

print("=== REAL January 2023 Lifecycle Report ===")
print(f"Total transactions:     {total}")
print(f"Matched (has Match No): {matched}")
print(f"Unmatched (no Match No):{unmatched}")
print(f"IsTiming = 1 (orphans): {timing}")
print(f"IsTiming = 0:           {non_timing}")
print(f"Unrecon. Fees total:    {unrecon}")
print(f"Unique Match groups:    {unique_groups}")
print(f"Matched By:             {matched_by}")
