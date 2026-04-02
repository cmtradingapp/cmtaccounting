"""SQL queries for the reconciliation engine.

All key normalization happens at transform time, so joins are simple equality checks.
These queries run against the clean layer tables.
"""

# Primary CRM ↔ PSP match on psp_transaction_id = reference_id
PRIMARY_PSP_MATCH = """
SELECT
    c.id AS crm_id,
    p.id AS psp_tx_id,
    c.amount AS crm_amount,
    p.amount AS psp_amount,
    c.currency AS crm_currency,
    p.currency AS psp_currency,
    p.psp_name
FROM clean_crm_transactions c
INNER JOIN clean_psp_transactions p
    ON c.psp_transaction_id = p.reference_id
WHERE c.psp_transaction_id IS NOT NULL
  AND p.reference_id IS NOT NULL
"""

# Fallback CRM ↔ PSP match on transactionid = reference_id
# Only for CRM rows NOT already matched in the primary pass
FALLBACK_PSP_MATCH = """
SELECT
    c.id AS crm_id,
    p.id AS psp_tx_id,
    c.amount AS crm_amount,
    p.amount AS psp_amount,
    c.currency AS crm_currency,
    p.currency AS psp_currency,
    p.psp_name
FROM clean_crm_transactions c
INNER JOIN clean_psp_transactions p
    ON c.transactionid = p.reference_id
WHERE c.transactionid IS NOT NULL
  AND p.reference_id IS NOT NULL
  AND c.id NOT IN ({matched_crm_ids})
  AND p.id NOT IN ({matched_psp_ids})
"""

# CRM ↔ Bank match on psp_transaction_id = client_id
# Only for CRM rows NOT matched by PSP passes
BANK_MATCH = """
SELECT
    c.id AS crm_id,
    b.id AS bank_tx_id,
    c.amount AS crm_amount,
    b.amount AS bank_amount,
    c.currency AS crm_currency,
    b.currency AS bank_currency
FROM clean_crm_transactions c
INNER JOIN clean_bank_transactions b
    ON c.psp_transaction_id = b.client_id
WHERE c.psp_transaction_id IS NOT NULL
  AND b.client_id IS NOT NULL
  AND c.id NOT IN ({matched_crm_ids})
"""

# All CRM rows with PSP-type transactions (deposits/withdrawals that should match PSPs)
CRM_PSP_ROWS = """
SELECT c.id
FROM clean_crm_transactions c
WHERE LOWER(c.transaction_type) IN ('deposit', 'withdraw')
"""

# Unmatched PSP rows (bank-side only)
UNMATCHED_PSP = """
SELECT p.id AS psp_tx_id, p.psp_name, p.reference_id, p.amount, p.currency
FROM clean_psp_transactions p
WHERE p.id NOT IN ({matched_psp_ids})
"""

# Summary stats
COUNT_CRM = "SELECT COUNT(*) FROM clean_crm_transactions"
COUNT_PSP = "SELECT COUNT(*) FROM clean_psp_transactions"
COUNT_BANK = "SELECT COUNT(*) FROM clean_bank_transactions"
