"""
Investigate the actual data structure of the input files and the output file
to understand how the old system performed the matching.
"""
import pandas as pd

print("=" * 70)
print("1. BANK/PSP INPUT: TrustPayments.csv")
print("=" * 70)
bank = pd.read_csv(r"C:\Projects\MethodosReconciliationSystem\Data\Reconciliation-Relevant\MRS\2023\01. Jan. 2023\PSPs\TrustPayments.csv", nrows=3)
print(f"Columns: {bank.columns.tolist()}")
print(bank.head(3).to_string())

print("\n" + "=" * 70)
print("2. PLATFORM INPUT: CRM Transactions Additional info.xlsx")
print("=" * 70)
crm = pd.read_excel(r"C:\Projects\MethodosReconciliationSystem\Data\Reconciliation-Relevant\MRS\2023\01. Jan. 2023\platform\CRM Transactions Additional info.xlsx", nrows=3)
print(f"Columns: {crm.columns.tolist()}")
print(crm.head(3).to_string())

print("\n" + "=" * 70)
print("3. PLATFORM INPUT: Deposit and Withdrawal Report.csv")
print("=" * 70)
dw = pd.read_csv(r"C:\Projects\MethodosReconciliationSystem\Data\Reconciliation-Relevant\MRS\2023\01. Jan. 2023\platform\Deposit and Withdrawal Report.csv", nrows=3)
print(f"Columns: {dw.columns.tolist()}")
print(dw.head(3).to_string())

print("\n" + "=" * 70)
print("4. OUTPUT: List.xlsx (first 3 rows)")
print("=" * 70)
output = pd.read_excel(r"C:\Projects\MethodosReconciliationSystem\Data\Reconciliation-Relevant\Life cycle report\2023\1. January\List.xlsx", nrows=3)
print(f"Columns ({len(output.columns)}): {output.columns.tolist()}")
print(output.head(3).to_string())

print("\n" + "=" * 70)
print("5. KEY QUESTION: Where does 'Deal No' come from?")
print("=" * 70)
output_full = pd.read_excel(r"C:\Projects\MethodosReconciliationSystem\Data\Reconciliation-Relevant\Life cycle report\2023\1. January\List.xlsx")
# Check if Deal No values exist in the CRM file
crm_full = pd.read_excel(r"C:\Projects\MethodosReconciliationSystem\Data\Reconciliation-Relevant\MRS\2023\01. Jan. 2023\platform\CRM Transactions Additional info.xlsx")
dw_full = pd.read_csv(r"C:\Projects\MethodosReconciliationSystem\Data\Reconciliation-Relevant\MRS\2023\01. Jan. 2023\platform\Deposit and Withdrawal Report.csv")

output_deals = set(output_full["Deal No"].dropna().astype(str))
print(f"Output Deal No count: {len(output_deals)}")

# Check overlap with CRM
for col in crm_full.columns:
    crm_vals = set(crm_full[col].dropna().astype(str))
    overlap = len(output_deals & crm_vals)
    if overlap > 10:
        print(f"  CRM column '{col}' overlaps with {overlap} Deal Nos")

# Check overlap with D&W Report
for col in dw_full.columns:
    dw_vals = set(dw_full[col].dropna().astype(str))
    overlap = len(output_deals & dw_vals)
    if overlap > 10:
        print(f"  D&W column '{col}' overlaps with {overlap} Deal Nos")

print("\n" + "=" * 70)
print("6. KEY QUESTION: Where does 'Reference' come from?")
print("=" * 70)
output_refs = set(output_full["Reference"].dropna().astype(str))
print(f"Output Reference count: {len(output_refs)}")

bank_full = pd.read_csv(r"C:\Projects\MethodosReconciliationSystem\Data\Reconciliation-Relevant\MRS\2023\01. Jan. 2023\PSPs\TrustPayments.csv")
for col in bank_full.columns:
    bank_vals = set(bank_full[col].dropna().astype(str))
    overlap = len(output_refs & bank_vals)
    if overlap > 10:
        print(f"  TrustPayments column '{col}' overlaps with {overlap} References")

# Also check CRM
for col in crm_full.columns:
    crm_vals = set(crm_full[col].dropna().astype(str))
    overlap = len(output_refs & crm_vals)
    if overlap > 10:
        print(f"  CRM column '{col}' overlaps with {overlap} References")

for col in dw_full.columns:
    dw_vals = set(dw_full[col].dropna().astype(str))
    overlap = len(output_refs & dw_vals)
    if overlap > 10:
        print(f"  D&W column '{col}' overlaps with {overlap} References")
