"""Shared normalization functions extracted from the heuristic prototype (server.py).

These are pure functions with no Flask or DB dependency. They handle:
- Join key normalization (whitespace, float suffixes, leading zeros, case)
- Currency FK resolution (integer → ISO 4217)
- Payment processor → 2-letter PM code mapping
- TRX Type classification (payment_method + transactiontype → category)
"""

import re
import pandas as pd


# ── Currency FK → ISO code ──────────────────────────────────────────────────
CURRENCY_ID_MAP = {
    1:  'USD',  2:  'EUR',  3:  'GBP',  4:  'AED',  5:  'ZAR',
    6:  'NGN',  7:  'IDR',  8:  'MXN',  9:  'COP',  10: 'TZS',
    11: 'USDT', 12: 'KES',  13: 'UGX',  14: 'GHS',  15: 'ZMW',
    16: 'BWP',  17: 'PHP',  18: 'MYR',  19: 'VND',
}


def resolve_currency(series):
    """Convert CRM currency_id integers to ISO currency codes."""
    def _conv(v):
        if pd.isna(v):
            return None
        try:
            return CURRENCY_ID_MAP.get(int(float(v)), str(v))
        except (ValueError, TypeError):
            return str(v)
    return series.apply(_conv)


# ── Join key normalization ──────────────────────────────────────────────────

def normalize_key(series):
    """Sanitize join keys for robust matching.

    Handles: trailing whitespace, float .0 suffixes (39120162.0 -> 39120162),
    leading zeros (0012345 -> 12345), case differences, and nulls.
    """
    s = series.astype(str).str.strip()
    s = s.str.replace(r'\.0+$', '', regex=True)
    s = s.str.replace(r'^0+(\d)', r'\1', regex=True)
    s = s.str.upper()
    s = s.replace({'NAN': None, 'NONE': None, 'NAT': None, '': None})
    return s


def normalize_key_scalar(value):
    """Normalize a single join key value (scalar version of normalize_key)."""
    if value is None or pd.isna(value):
        return None
    s = str(value).strip()
    s = re.sub(r'\.0+$', '', s)
    s = re.sub(r'^0+(\d)', r'\1', s)
    s = s.upper()
    if s in ('NAN', 'NONE', 'NAT', ''):
        return None
    return s


# ── Payment processor → PM code ────────────────────────────────────────────

_PROCESSOR_TO_PM_CODE = {
    'zotapaymg':              'ZP',
    'zotapay':                'ZP',
    'safecharges2s3dv2':      'SC',
    'safecharges2s3dv2_ver2': 'SC',
    'safecharge':             'SC',
    'safecharges2s':          'SC',
    'korapayapm':             'KP',
    'korapayhpp':             'KP',
    'korapay':                'KP',
    'solidpayments3dsv2':     'SLP',
    'solidpayments':          'SLP',
    'finrax':                 'FRX',
    'ozow':                   'OZ',
    'eftpay':                 'EFT',
    'virtualpays2s':          'VP',
    'virtualpay':             'VP',
    'skrill':                 'SKR',
    'neteller':               'NT',
    'directa24rest':          'DRC',
    'directa24':              'DRC',
    'inatec':                 'INA',
    'powercash':              'INA',
    'swiffyeft':              'SW',
    'swiffy':                 'SW',
    'astropay':               'ASP',
    'letknow':                'LKP',
    'letknowpay':             'LKP',
    'trustpayments':          'TP',
    'nuvei':                  'SC',
}


def map_pm_code(payment_processor):
    """Map CRM payment_processor string to 2-letter PM code."""
    if not payment_processor or str(payment_processor).lower() in ('nan', 'none', ''):
        return None
    key = re.sub(r'[^a-z0-9]', '', str(payment_processor).lower())
    return _PROCESSOR_TO_PM_CODE.get(key)


# ── TRX Type classification ────────────────────────────────────────────────

_PM_TEXT_TO_TRX_TYPE = {
    'transfer':          '4. Transfer',
    'internal transfer': '4. Transfer',
    'bonus':             '5. Bonuses',
    'frf commission':    '5. Bonuses',
    'processing fees':   '5. Fees/Charges',
    'commission':        '5. Realised Commissions',
    'ib commission':     '5. IB Payment',
    'adjustment':        '4. Transfer',
    'fee compensation':  '5. Fee Compensation',
    'chargeback':        '5. Fee Compensation',
}

_PSP_PAYMENT_METHODS = {
    'credit card', 'electronic payment', 'cryptowallet',
    'cash', 'wire transfer',
}

_PM_TEXT_TO_CODE = {
    'transfer':          'TRF',
    'internal transfer': 'TRF',
    'processing fees':   'PRF',
    'commission':        'PRF',
    'frf commission':    'ADJ',
    'ib commission':     'ADJ',
    'bonus':             'BN',
    'adjustment':        'ADJ',
    'fee compensation':  'ADJ',
    'chargeback':        'ADJ',
    'cash':              'ADJ',
    'wire transfer':     'BT',
}


def map_trx_type(payment_method, transactiontype, is_timing=False):
    """Derive the MT4 TRX Type from CRM payment_method and transactiontype."""
    pm = str(payment_method).strip().lower() if payment_method else ''
    tt = str(transactiontype).strip().lower() if transactiontype else ''

    if is_timing:
        return '3. Timing Deposit' if 'deposit' in tt else '3. Timing Withdrawal'

    if pm in _PM_TEXT_TO_TRX_TYPE:
        return _PM_TEXT_TO_TRX_TYPE[pm]

    if pm in _PSP_PAYMENT_METHODS or not pm:
        if 'deposit' in tt:
            return '2. DP'
        if 'withdraw' in tt:
            return '2. WD'

    if 'deposit' in tt:
        return '2. DP'
    if 'withdraw' in tt:
        return '2. WD'
    return '4. Transfer'


def map_non_psp_pm_code(payment_method):
    """Map non-PSP payment method text to a PM code (TRF, BN, PRF, etc.)."""
    if not payment_method:
        return None
    pm = str(payment_method).strip().lower()
    return _PM_TEXT_TO_CODE.get(pm)


def clean_amount(value):
    """Parse an amount string to float, handling commas and whitespace."""
    if value is None or pd.isna(value):
        return None
    s = str(value).strip().replace(',', '').replace(' ', '')
    try:
        return float(s)
    except (ValueError, TypeError):
        return None
