"""
Document text extraction + OpenRouter AI analysis for PSP agreements.

The FEES system schema this module targets:
  psp_agreements  — one row per PSP
  psp_fee_rules   — many rows per PSP, each a specific fee line
  psp_fee_tiers   — volume bands for tiered-rate rules
"""
import io
import os
import json
import requests

OPENROUTER_KEY   = os.environ.get("OPENROUTER_API_KEY", "")
OPENROUTER_MODEL = os.environ.get("OPENROUTER_MODEL", "google/gemini-2.5-flash")

# Models that support json_schema structured outputs (strict validation).
# All others fall back to json_object mode (valid JSON guaranteed, schema not enforced by API).
_SCHEMA_CAPABLE_MODELS = {
    "openai/gpt-4o",
    "openai/gpt-4o-mini",
    "openai/gpt-4.1",
    "openai/gpt-4.1-mini",
    "openai/o3-mini",
}

# Head+tail strategy: 4 k of agreement header + 12 k of tail (where fee schedules live)
HEAD_CHARS = 4_000
TAIL_CHARS = 12_000

# ── Canonical value lists — MUST match app.py exactly ─────────────────────
FEE_TYPES = [
    "Deposit", "Withdrawal", "Settlement", "Chargeback", "Refund",
    "Rolling Reserve", "Holdback", "Setup", "Registration", "Minimum Monthly",
]
PAYMENT_METHODS = [
    "Credit Cards", "Bank Wire", "Mobile Money", "Electronic Payment",
    "Crypto", "MOMO", "E-Wallet",
]

# ── System prompt ──────────────────────────────────────────────────────────
SYSTEM_PROMPT = """\
You are a specialist financial-document parser for a PSP (Payment Service Provider) \
reconciliation system called FEES. Your sole job is to extract structured data from \
PSP merchant agreements and return it as a single, valid JSON object that exactly \
matches the schema shown below. No markdown fences, no explanation, no extra keys.

═══════════════════════════════════════════════════════════
  OUTPUT JSON SCHEMA  (use null for any unknown field)
═══════════════════════════════════════════════════════════
{
  "agreement": {
    "psp_name"         : "<SHORT UPPERCASE NAME — e.g. NUVEI, BITOLO>",
    "provider_name"    : "<Full legal company name of the PSP — e.g. NUVEI LIMITED>",
    "agreement_entity" : "<CMT legal entity that signed — see rules below>",
    "agreement_date"   : "<YYYY-MM-DD or null>",
    "addendum_date"    : "<YYYY-MM-DD or null — most recent addendum/amendment date>",
    "auto_settlement"  : <true | false>,
    "settlement_bank"  : "<Bank name + account identifier, or null>"
  },
  "fee_rules": [
    {
      "payment_method" : "<one value from the PAYMENT METHODS list, or null>",
      "fee_type"       : "<one value from the FEE TYPES list>",
      "country"        : "<country name as written in agreement, or 'GLOBAL'>",
      "sub_provider"   : "<card network / mobile operator / sub-brand, or null>",
      "fee_kind"       : "<'percentage' | 'fixed' | 'fixed_plus_pct' | 'tiered'>",
      "pct_rate"       : <decimal fraction or null — e.g. 3.5 % → 0.035>,
      "fixed_amount"   : <number or null>,
      "fixed_currency" : "<ISO-4217 code or null — e.g. USD, EUR, NGN>",
      "description"    : "<verbatim or paraphrased fee label from the document, or null>",
      "tiers"          : [
        { "volume_from": <number>, "volume_to": <number or null>, "pct_rate": <decimal fraction> }
      ]
    }
  ]
}

═══════════════════════════════════════════════════════════
  FIELD-BY-FIELD RULES
═══════════════════════════════════════════════════════════

── agreement.psp_name ──────────────────────────────────
• Short commercial name (all caps), NOT the legal entity name.
  ✓ "NUVEI"   ✓ "BITOLO"   ✓ "DIRECTA24"   ✗ "NUVEI LIMITED"

── agreement.provider_name ─────────────────────────────
• Full legal name of the PSP company as it appears in the agreement.
  ✓ "NUVEI LIMITED"   ✓ "FLYBRIDGE LIMITED"   ✓ "BLAVEN TECHNOLOGIES LLP"

── agreement.agreement_entity ──────────────────────────
• The CMT Group legal entity that signed (counterparty).
• Common values (normalise spelling to these exactly):
    "CMT PROCESSING LTD"    ← formerly "CMT PROCESSING LIMITED"
    "GCMT GROUP LTD"        ← also appears as "GCMT GROUP LIMITED"
• If neither is clear, use null.

── agreement.agreement_date / addendum_date ────────────
• Convert any date format to YYYY-MM-DD:
    "27.10.2025" → "2025-10-27"
    "28/06/2022" → "2022-06-28"
    "January 15, 2024" → "2024-01-15"
    "n/a", "N/A", "n.a." → null

── agreement.auto_settlement ───────────────────────────
• true  → document mentions "automatic settlement", "auto settlement", "YES" in settlement column,
          or says the PSP remits funds automatically on a fixed schedule.
• false → manual settlement, "NO", or no mention.

── agreement.settlement_bank ───────────────────────────
• Capture the full string including bank name and account/IBAN reference.
  e.g. "Alpha CY77009005150005151060573155"
  or   "Absa 242586-USD-1046-11"

── fee_rules[].payment_method ──────────────────────────
ALLOWED VALUES (map document terms to these exactly):
  "Credit Cards"        ← CREDIT CARDS, CARD, VISA, MASTERCARD, AMEX
  "Bank Wire"           ← BANK WIRE, WIRE TRANSFER, WIRE, BANK TRANSFER
  "Mobile Money"        ← MOBILE MONEY, MOMO, MOBILE PAYMENTS
  "Electronic Payment"  ← ELECTRONIC PAYMENT, E-WALLET (general), NETELLER, SKRILL,
                          PAYSAFE, PAYPAL, ONLINE BANKING
  "Crypto"              ← CRYPTO, CRYPTOCURRENCY, CRYPTOWALLET, BITCOIN, USDT
  "MOMO"                ← use only when document explicitly labels it MOMO
  "E-Wallet"            ← use only when document labels it E-WALLET distinctly from Electronic Payment
  null                  ← when a fee applies to all methods, or method is unspecified

── fee_rules[].fee_type ────────────────────────────────
ALLOWED VALUES (map document terms to these exactly):
  "Deposit"          ← DEPOSIT FEE, ACQUIRING FEE, PROCESSING FEE (incoming), DEPOSIT AND WITHDRAWAL FEE*
  "Withdrawal"       ← WITHDRAWAL FEE, CASHOUT FEE, PAYOUT FEE, DEPOSIT AND WITHDRAWAL FEE*
  "Settlement"       ← SETTLEMENT FEE, SETTLEMENT CHARGE
  "Chargeback"       ← CHARGEBACK FEE, DISPUTE FEE
  "Refund"           ← REFUND FEE, REVERSAL FEE
  "Rolling Reserve"  ← ROLLING RESERVE, RESERVE RATE, HOLDBACK RATE (as reserve, not one-off)
  "Holdback"         ← HOLDBACK (one-off amount held back on termination), HOLDBACK AMOUNT
  "Setup"            ← SETUP FEE, SET UP FEE, ONBOARDING FEE, INTEGRATION FEE
  "Registration"     ← REGISTRATION FEE, ANNUAL CARD SCHEME FEE, CARD BRAND FEE
  "Minimum Monthly"  ← MINIMUM FEE, MINIMUM MONTHLY FEE, MONTHLY MINIMUM
  * "DEPOSIT AND WITHDRAWAL FEE" — create TWO separate rules, one Deposit and one Withdrawal,
    each with the same rate, unless rates differ.

── fee_rules[].country ─────────────────────────────────
• Use the country name exactly as written in the document.
• If the fee applies globally / to all countries → "GLOBAL"
• Multiple countries in one row → create one rule per country.
• "ALL" or "ALL COUNTRIES" or no country specified → "GLOBAL"

── fee_rules[].sub_provider ────────────────────────────
• Card networks: "Visa", "Mastercard", "Amex", "UnionPay"
• Mobile operators: "MTN", "Airtel", "Vodacom", "M-Pesa", "Orange"
• If the fee applies to all sub-providers → null

── fee_rules[].fee_kind and rate fields ─────────────────
This is the most critical section. Apply the following priority order:

  STEP 1 — Look for explicit % sign:
    "3.5%"   → fee_kind="percentage", pct_rate=0.035
    "3,5%"   → European comma decimal; same result: pct_rate=0.035
    "0,5%"   → pct_rate=0.005
    "1,1%"   → pct_rate=0.011

  STEP 2 — Decimal fraction < 1.0 with no % and no currency code:
    These are stored as decimal fractions in the source system:
    "0.013"  → fee_kind="percentage", pct_rate=0.013   (= 1.3%)
    "0.02"   → fee_kind="percentage", pct_rate=0.02    (= 2%)
    "0.03"   → fee_kind="percentage", pct_rate=0.03    (= 3%)
    "0.29"   — AMBIGUOUS. Check fee_type:
               • If fee_type is Deposit/Withdrawal/Settlement → likely percentage, pct_rate=0.29
               • If combined with "per transaction" language  → likely fixed, fixed_amount=0.29, fixed_currency="USD"

  STEP 3 — Integer or large decimal with no % and no currency code:
    These are usually fixed amounts in USD (unless context implies percentage):
    "0.8"    with "per transaction" → fixed, fixed_amount=0.8, fixed_currency="USD"
    "1"      as settlement fee → fee_kind="percentage", pct_rate=0.01  (= 1%, common for settlement)
    "1"      as a per-transaction fee → fixed, fixed_amount=1, fixed_currency="USD"
    "5"      as margin → fixed, fixed_amount=5, fixed_currency="USD" (or EUR if specified)
    "18"     as chargeback exceeded fee → fixed, fixed_amount=18, fixed_currency="USD"
    "100"    as dormancy/inactivity → fixed, fixed_amount=100, fixed_currency="USD"
    "500"    for Setup/Registration → fixed, fixed_amount=500, fixed_currency="USD"
    Use the fee_type and surrounding description text as context to disambiguate.

  STEP 4 — Combined fixed + percentage:
    "100 NGN + 1%"   → fee_kind="fixed_plus_pct", fixed_amount=100, fixed_currency="NGN", pct_rate=0.01
    "8 GHS + 1%"     → fee_kind="fixed_plus_pct", fixed_amount=8,   fixed_currency="GHS", pct_rate=0.01
    "€0.30 + 2.9%"   → fee_kind="fixed_plus_pct", fixed_amount=0.30, fixed_currency="EUR", pct_rate=0.029
    "USD 0.50 + 3%"  → fee_kind="fixed_plus_pct", fixed_amount=0.50, fixed_currency="USD", pct_rate=0.03

  STEP 5 — Tiered / volume-based:
    Document shows a table with volume bands and different rates per band.
    fee_kind="tiered", pct_rate=null, fixed_amount=null
    Populate tiers array with { volume_from, volume_to, pct_rate } for each band.
    volume_to = null for the last (open-ended) tier.

  NEVER invent a rate. If genuinely ambiguous, choose the most contextually likely
  interpretation and note it in the description field.

── fee_rules[].description ─────────────────────────────
• Copy the exact fee label or description from the document.
• Keep it concise (≤ 120 chars). Include any qualifiers (e.g. "on approved transactions",
  "per merchant ID", "max 30 USD", "where applicable").

═══════════════════════════════════════════════════════════
  WORKED EXAMPLE (from a real agreement)
═══════════════════════════════════════════════════════════
Source row: NUVEI | CREDIT CARDS | DEPOSIT FEE | GLOBAL | ACQUIRING FEE | 0.01 | YES | Alpha CY77...

Output rule:
{
  "payment_method": "Credit Cards",
  "fee_type": "Deposit",
  "country": "GLOBAL",
  "sub_provider": null,
  "fee_kind": "percentage",
  "pct_rate": 0.01,
  "fixed_amount": null,
  "fixed_currency": null,
  "description": "Acquiring fee - on approved transactions",
  "tiers": []
}

Source row: BITOLO | BANK WIRE | WITHDRAWAL FEE | NIGERIA | 100 NGN + 1%

Output rule:
{
  "payment_method": "Bank Wire",
  "fee_type": "Withdrawal",
  "country": "Nigeria",
  "sub_provider": null,
  "fee_kind": "fixed_plus_pct",
  "pct_rate": 0.01,
  "fixed_amount": 100,
  "fixed_currency": "NGN",
  "description": "Withdrawal fee",
  "tiers": []
}

Source row: NUVEI | CREDIT CARDS | SET UP FEE | GLOBAL | PER MERCHANT ID | 500

Output rule:
{
  "payment_method": "Credit Cards",
  "fee_type": "Setup",
  "country": "GLOBAL",
  "sub_provider": null,
  "fee_kind": "fixed",
  "pct_rate": null,
  "fixed_amount": 500,
  "fixed_currency": "USD",
  "description": "Per merchant ID",
  "tiers": []
}

═══════════════════════════════════════════════════════════
  EXTRACTION STRATEGY
═══════════════════════════════════════════════════════════
• Scan the ENTIRE document. Fee schedules are frequently in appendices,
  schedules, or exhibit sections near the END of the document.
• Look for: tables, bullet lists, sections titled "Fees", "Pricing",
  "Schedule", "Charges", "Commercial Terms", "Fee Schedule", "Exhibit".
• Extract EVERY distinct fee line as a separate entry in fee_rules.
• If a fee applies to multiple countries listed separately, create one
  rule per country.
• Skip non-fee items: KYC requirements, legal boilerplate, liability
  clauses, privacy policy, data protection terms.
• If no fee schedule is found, return fee_rules as an empty array [].
"""

# ── JSON schema shown to model in the user turn ───────────────────────────
_JSON_EXAMPLE = json.dumps({
    "agreement": {
        "psp_name": "EXAMPLE",
        "provider_name": "Example Ltd",
        "agreement_entity": "CMT PROCESSING LTD",
        "agreement_date": "2024-01-15",
        "addendum_date": None,
        "auto_settlement": True,
        "settlement_bank": "Alpha CY77009005150005151060573155"
    },
    "fee_rules": [
        {
            "payment_method": "Credit Cards",
            "fee_type": "Deposit",
            "country": "GLOBAL",
            "sub_provider": None,
            "fee_kind": "percentage",
            "pct_rate": 0.035,
            "fixed_amount": None,
            "fixed_currency": None,
            "description": "Processing fee on approved transactions",
            "tiers": []
        }
    ]
}, indent=2)


# ── JSON Schema (used when model supports structured outputs) ──────────────
# This is the strict contract the API enforces at the token level when using
# response_format={"type":"json_schema"}.  When that mode is unavailable the
# schema is still shown to the model in the user prompt as a reference.
_OUTPUT_SCHEMA = {
    "name":   "psp_agreement_extraction",
    "strict": True,
    "schema": {
        "type": "object",
        "required": ["agreement", "fee_rules"],
        "additionalProperties": False,
        "properties": {
            "agreement": {
                "type": "object",
                "required": [
                    "psp_name", "provider_name", "agreement_entity",
                    "agreement_date", "addendum_date",
                    "auto_settlement", "settlement_bank",
                ],
                "additionalProperties": False,
                "properties": {
                    "psp_name":          {"type": "string"},
                    "provider_name":     {"type": ["string", "null"]},
                    "agreement_entity":  {"type": ["string", "null"]},
                    "agreement_date":    {"type": ["string", "null"]},
                    "addendum_date":     {"type": ["string", "null"]},
                    "auto_settlement":   {"type": "boolean"},
                    "settlement_bank":   {"type": ["string", "null"]},
                },
            },
            "fee_rules": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": [
                        "payment_method", "fee_type", "country", "sub_provider",
                        "fee_kind", "pct_rate", "fixed_amount", "fixed_currency",
                        "description", "tiers",
                    ],
                    "additionalProperties": False,
                    "properties": {
                        "payment_method":  {"type": ["string", "null"], "enum": PAYMENT_METHODS + [None]},
                        "fee_type":        {"type": "string", "enum": FEE_TYPES},
                        "country":         {"type": "string"},
                        "sub_provider":    {"type": ["string", "null"]},
                        "fee_kind":        {"type": "string", "enum": ["percentage", "fixed", "fixed_plus_pct", "tiered"]},
                        "pct_rate":        {"type": ["number", "null"]},
                        "fixed_amount":    {"type": ["number", "null"]},
                        "fixed_currency":  {"type": ["string", "null"]},
                        "description":     {"type": ["string", "null"]},
                        "tiers": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "required": ["volume_from", "volume_to", "pct_rate"],
                                "additionalProperties": False,
                                "properties": {
                                    "volume_from": {"type": "number"},
                                    "volume_to":   {"type": ["number", "null"]},
                                    "pct_rate":    {"type": "number"},
                                },
                            },
                        },
                    },
                },
            },
        },
    },
}


def _response_format() -> dict:
    """Return the appropriate response_format dict for the configured model."""
    if OPENROUTER_MODEL in _SCHEMA_CAPABLE_MODELS:
        return {"type": "json_schema", "json_schema": _OUTPUT_SCHEMA}
    # Fallback: guarantees valid JSON syntax but doesn't enforce schema shape.
    return {"type": "json_object"}


# ── Text extraction ────────────────────────────────────────────────────────

def extract_text(file_bytes: bytes, filename: str) -> str:
    """Extract plain text from a PDF or DOCX file."""
    ext = filename.lower().rsplit(".", 1)[-1]

    if ext == "pdf":
        import pdfplumber
        text_parts = []
        with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
            for page in pdf.pages:
                t = page.extract_text()
                if t:
                    text_parts.append(t)
        return "\n".join(text_parts)

    if ext in ("docx", "doc"):
        from docx import Document
        doc = Document(io.BytesIO(file_bytes))
        parts = []
        for para in doc.paragraphs:
            if para.text.strip():
                parts.append(para.text)
        # Tables are critical — fee schedules are often formatted as tables
        for table in doc.tables:
            for row in table.rows:
                cells = [c.text.strip() for c in row.cells if c.text.strip()]
                if cells:
                    parts.append(" | ".join(cells))
        return "\n".join(parts)

    raise ValueError(f"Unsupported file type: .{ext}  (only PDF and DOCX are supported)")


def _smart_truncate(text: str) -> str:
    """
    Keep the first HEAD_CHARS (agreement header: PSP name, entity, dates) +
    the last TAIL_CHARS (fee schedules almost always appear at the end).
    This approach is more reliable than a simple prefix truncation.
    """
    total = HEAD_CHARS + TAIL_CHARS
    if len(text) <= total:
        return text
    head = text[:HEAD_CHARS]
    tail = text[-TAIL_CHARS:]
    skipped = len(text) - total
    return (
        head
        + f"\n\n[... {skipped:,} characters of legal boilerplate omitted ...]\n\n"
        + tail
    )


# ── Post-processing normalisers ────────────────────────────────────────────

def _normalise_pct(value) -> float | None:
    """Ensure pct_rate values > 1 are divided by 100 (model sometimes returns 3.5 instead of 0.035)."""
    if isinstance(value, (int, float)) and value > 1:
        return round(value / 100, 8)
    return value


def _normalise_rule(rule: dict) -> dict:
    rule.setdefault("tiers", [])
    rule["pct_rate"] = _normalise_pct(rule.get("pct_rate"))
    for tier in rule.get("tiers", []):
        tier["pct_rate"] = _normalise_pct(tier.get("pct_rate")) or 0
    # Clamp pct_rate: > 0.99 is almost certainly wrong (model forgot to divide)
    if isinstance(rule.get("pct_rate"), float) and rule["pct_rate"] > 0.99:
        rule["pct_rate"] = round(rule["pct_rate"] / 100, 8)
    return rule


# ── Main API call ──────────────────────────────────────────────────────────

def analyze_agreement(text: str, system_prompt: str = None) -> dict:
    """
    Send extracted agreement text to OpenRouter and return a normalised dict:
      { "agreement": {...}, "fee_rules": [...] }

    Raises RuntimeError / requests.HTTPError on failure.
    """
    if not OPENROUTER_KEY:
        raise RuntimeError(
            "OPENROUTER_API_KEY is not set. "
            "Add it to recon-app/.env: OPENROUTER_API_KEY=sk-or-..."
        )

    active_prompt = system_prompt or SYSTEM_PROMPT
    trimmed = _smart_truncate(text)

    user_msg = (
        "Parse the following PSP agreement document.\n\n"
        "Return a JSON object that EXACTLY matches this structure:\n"
        f"{_JSON_EXAMPLE}\n\n"
        "ALLOWED fee_type values (use ONLY these):\n"
        f"{FEE_TYPES}\n\n"
        "ALLOWED payment_method values (use ONLY these, or null):\n"
        f"{PAYMENT_METHODS}\n\n"
        "DOCUMENT TEXT FOLLOWS:\n"
        "─" * 60 + "\n"
        f"{trimmed}\n"
        "─" * 60 + "\n\n"
        "Return ONLY the JSON object. No markdown, no commentary."
    )

    response = requests.post(
        "https://openrouter.ai/api/v1/chat/completions",
        headers={
            "Authorization":  f"Bearer {OPENROUTER_KEY}",
            "Content-Type":   "application/json",
            "HTTP-Referer":   "https://cmtaccounting.internal",
            "X-Title":        "CMT PSP Fee Parser",
        },
        json={
            "model":   OPENROUTER_MODEL,
            "messages": [
                {"role": "system", "content": active_prompt},
                {"role": "user",   "content": user_msg},
            ],
            "response_format": _response_format(),
            "temperature": 0.0,   # deterministic — we want no creativity here
            "max_tokens":  4096,
        },
        timeout=120,
    )
    response.raise_for_status()

    raw     = response.json()["choices"][0]["message"]["content"]
    result  = json.loads(raw)

    result.setdefault("agreement", {})
    result.setdefault("fee_rules", [])
    result["fee_rules"] = [_normalise_rule(r) for r in result["fee_rules"]]

    return result


# ── Amendment parsing ──────────────────────────────────────────────────────

AMENDMENT_SYSTEM_PROMPT = """\
You are a specialist financial-document parser. You are parsing an AMENDMENT to an \
existing PSP (Payment Service Provider) merchant agreement, NOT a full agreement. \
Your job is to extract ONLY the rules that are explicitly NEW, CHANGED, or REMOVED \
by this amendment, and return them as a single valid JSON object. \
No markdown fences, no explanation, no extra keys.

═══════════════════════════════════════════════════════════
  CRITICAL RULE — WHAT TO INCLUDE vs. EXCLUDE
═══════════════════════════════════════════════════════════
INCLUDE a rule ONLY if the amendment document explicitly:
  • States it is a NEW fee/market/payment method being added
  • States it REPLACES, SUPERSEDES, or CHANGES an existing fee
  • States an existing fee is REMOVED, CANCELLED, or DELETED

EXCLUDE (do NOT include) any rule that the amendment says:
  • "shall remain in place"
  • "remain unchanged"
  • "except as expressly provided … all other terms remain in full effect"
  • Is mentioned only in boilerplate, definitions, or legal clauses

═══════════════════════════════════════════════════════════
  OUTPUT JSON SCHEMA
═══════════════════════════════════════════════════════════
{
  "amendment": {
    "addendum_date": "<YYYY-MM-DD — the execution/effective date of this amendment, or null>",
    "notes":         "<one-sentence plain-English summary of what changed>"
  },
  "rule_changes": [
    {
      "action":       "<'add' | 'replace' | 'remove'>",
      "match_on": {
        "payment_method": "<value from PAYMENT METHODS list or null>",
        "country":        "<country name exactly as in amendment, or 'GLOBAL'>",
        "fee_type":       "<value from FEE TYPES list>"
      },
      "payment_method":  "<value from PAYMENT METHODS list, or null>",
      "fee_type":        "<value from FEE TYPES list>",
      "country":         "<country name exactly as written, or 'GLOBAL'>",
      "sub_provider":    "<card network / mobile operator, or null>",
      "fee_kind":        "<'percentage' | 'fixed' | 'fixed_plus_pct' | 'tiered'>",
      "pct_rate":        <decimal fraction or null — e.g. 2.5% → 0.025>,
      "fixed_amount":    <number or null>,
      "fixed_currency":  "<ISO-4217 code or null>",
      "description":     "<verbatim fee label or null>",
      "tiers":           []
    }
  ]
}

═══════════════════════════════════════════════════════════
  ACTION FIELD RULES
═══════════════════════════════════════════════════════════
"add"     — This is a NEW rule not previously covered (new country, new payment method,
            new fee type). match_on may be the same as the new rule values.
"replace" — An EXISTING rule with the same country + payment_method + fee_type is being
            given NEW rates. Set match_on to identify the old rule to overwrite.
"remove"  — An EXISTING rule is being explicitly deleted or cancelled.
            Set match_on to identify it. The fee fields (pct_rate, etc.) may be null.

═══════════════════════════════════════════════════════════
  PAYMENT METHODS (map document terms to these exactly)
═══════════════════════════════════════════════════════════
  "Credit Cards"        ← CREDIT CARDS, CARD, VISA, MASTERCARD, AMEX
  "Bank Wire"           ← BANK WIRE, WIRE TRANSFER, BANK TRANSFER, EFT, SPEI, PSE, PIX
  "Mobile Money"        ← MOBILE MONEY, MOMO, MOBILE PAYMENTS, MPESA, OPAY
  "Electronic Payment"  ← ELECTRONIC PAYMENT, E-WALLET (general), NETELLER, SKRILL
  "Crypto"              ← CRYPTO, CRYPTOCURRENCY, BITCOIN, USDT
  "MOMO"                ← use only when document explicitly labels it MOMO
  "E-Wallet"            ← use only when document labels it E-WALLET distinctly
  null                  ← when method is unspecified or applies to all

═══════════════════════════════════════════════════════════
  FEE TYPES (map document terms to these exactly)
═══════════════════════════════════════════════════════════
  "Deposit"          ← DEPOSIT FEE, ACQUIRING FEE, PROCESSING FEE, PAYINS
  "Withdrawal"       ← WITHDRAWAL FEE, CASHOUT FEE, PAYOUT FEE, PAYOUTS
  "Settlement"       ← SETTLEMENT FEE
  "Chargeback"       ← CHARGEBACK FEE, DISPUTE FEE
  "Refund"           ← REFUND FEE, REVERSAL FEE
  "Rolling Reserve"  ← ROLLING RESERVE, RESERVE RATE
  "Holdback"         ← HOLDBACK
  "Setup"            ← SETUP FEE, ONBOARDING FEE
  "Registration"     ← REGISTRATION FEE, ANNUAL CARD SCHEME FEE
  "Minimum Monthly"  ← MINIMUM FEE, MINIMUM MONTHLY FEE

  "PAYINS" in the document → fee_type = "Deposit"
  "PAYOUTS" in the document → fee_type = "Withdrawal"
  If a fee applies to both deposit AND withdrawal → create TWO rules.

═══════════════════════════════════════════════════════════
  RATE PARSING RULES
═══════════════════════════════════════════════════════════
  "2.5%"              → fee_kind="percentage", pct_rate=0.025
  "1.40% + 0.60 USD"  → fee_kind="fixed_plus_pct", pct_rate=0.014, fixed_amount=0.60, fixed_currency="USD"
  "3.10% + 0.50 USD"  → fee_kind="fixed_plus_pct", pct_rate=0.031, fixed_amount=0.50, fixed_currency="USD"
  Volume tiers (e.g. "0-300,000 USD: 5.70% / 300,001-600,000 USD: 5.60%"):
    → fee_kind="tiered", populate tiers array, pct_rate=null
  For tiered rules: volume_to=null for the last (open-ended) tier.
  NEVER invent a rate. If ambiguous, note it in description.

  Minimum fee language like "minimum fee of 0.20 USD" — capture in description field,
  do NOT create a separate fee rule for it.

═══════════════════════════════════════════════════════════
  AMENDMENT DATE
═══════════════════════════════════════════════════════════
  Look for: "Executed on", "Effective Date", "dated", signature block date.
  Convert any format to YYYY-MM-DD. If multiple dates, use the execution/signing date.

═══════════════════════════════════════════════════════════
  WORKED EXAMPLE (Blaven amendment, Feb 2025)
═══════════════════════════════════════════════════════════
The amendment adds PAYINS + PAYOUTS for Nigeria, Kenya, Ghana, South Africa, Brazil,
Mexico (tiered), Colombia (tiered), Ecuador (tiered), Peru.

Example output for two of those rules:
{
  "action": "add",
  "match_on": { "payment_method": "Mobile Money", "country": "Kenya", "fee_type": "Deposit" },
  "payment_method": "Mobile Money",
  "fee_type": "Deposit",
  "country": "Kenya",
  "sub_provider": null,
  "fee_kind": "percentage",
  "pct_rate": 0.03,
  "fixed_amount": null,
  "fixed_currency": null,
  "description": "PAYINS: Mobile Money - 3.00% (Flat Fee)",
  "tiers": []
}
{
  "action": "add",
  "match_on": { "payment_method": "Mobile Money", "country": "Kenya", "fee_type": "Withdrawal" },
  "payment_method": "Mobile Money",
  "fee_type": "Withdrawal",
  "country": "Kenya",
  "sub_provider": null,
  "fee_kind": "fixed_plus_pct",
  "pct_rate": 0.031,
  "fixed_amount": 0.50,
  "fixed_currency": "USD",
  "description": "PAYOUTS: 3.10% + 0.50 USD (Flat Fee)",
  "tiers": []
}
"""

# Amendment output schema (used when model supports structured outputs)
_AMENDMENT_SCHEMA = {
    "name":   "psp_amendment_extraction",
    "strict": True,
    "schema": {
        "type": "object",
        "required": ["amendment", "rule_changes"],
        "additionalProperties": False,
        "properties": {
            "amendment": {
                "type": "object",
                "required": ["addendum_date", "notes"],
                "additionalProperties": False,
                "properties": {
                    "addendum_date": {"type": ["string", "null"]},
                    "notes":         {"type": ["string", "null"]},
                },
            },
            "rule_changes": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": [
                        "action", "match_on",
                        "payment_method", "fee_type", "country", "sub_provider",
                        "fee_kind", "pct_rate", "fixed_amount", "fixed_currency",
                        "description", "tiers",
                    ],
                    "additionalProperties": False,
                    "properties": {
                        "action": {"type": "string", "enum": ["add", "replace", "remove"]},
                        "match_on": {
                            "type": "object",
                            "required": ["payment_method", "country", "fee_type"],
                            "additionalProperties": False,
                            "properties": {
                                "payment_method": {"type": ["string", "null"]},
                                "country":        {"type": "string"},
                                "fee_type":       {"type": "string"},
                            },
                        },
                        "payment_method":  {"type": ["string", "null"], "enum": PAYMENT_METHODS + [None]},
                        "fee_type":        {"type": "string", "enum": FEE_TYPES},
                        "country":         {"type": "string"},
                        "sub_provider":    {"type": ["string", "null"]},
                        "fee_kind":        {"type": "string", "enum": ["percentage", "fixed", "fixed_plus_pct", "tiered"]},
                        "pct_rate":        {"type": ["number", "null"]},
                        "fixed_amount":    {"type": ["number", "null"]},
                        "fixed_currency":  {"type": ["string", "null"]},
                        "description":     {"type": ["string", "null"]},
                        "tiers": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "required": ["volume_from", "volume_to", "pct_rate"],
                                "additionalProperties": False,
                                "properties": {
                                    "volume_from": {"type": "number"},
                                    "volume_to":   {"type": ["number", "null"]},
                                    "pct_rate":    {"type": "number"},
                                },
                            },
                        },
                    },
                },
            },
        },
    },
}


def _amendment_response_format() -> dict:
    if OPENROUTER_MODEL in _SCHEMA_CAPABLE_MODELS:
        return {"type": "json_schema", "json_schema": _AMENDMENT_SCHEMA}
    return {"type": "json_object"}


def analyze_amendment(text: str, system_prompt: str = None) -> dict:
    """
    Parse an amendment document and return:
      { "amendment": { "addendum_date": ..., "notes": ... },
        "rule_changes": [ { "action", "match_on", ...fee fields... }, ... ] }
    """
    if not OPENROUTER_KEY:
        raise RuntimeError(
            "OPENROUTER_API_KEY is not set. "
            "Add it to recon-app/.env: OPENROUTER_API_KEY=sk-or-..."
        )

    active_prompt = system_prompt or AMENDMENT_SYSTEM_PROMPT
    trimmed = _smart_truncate(text)

    user_msg = (
        "Parse the following PSP AMENDMENT document.\n\n"
        "Return a JSON object with keys 'amendment' and 'rule_changes'.\n\n"
        "ALLOWED fee_type values (use ONLY these):\n"
        f"{FEE_TYPES}\n\n"
        "ALLOWED payment_method values (use ONLY these, or null):\n"
        f"{PAYMENT_METHODS}\n\n"
        "DOCUMENT TEXT FOLLOWS:\n"
        + "─" * 60 + "\n"
        + trimmed + "\n"
        + "─" * 60 + "\n\n"
        "Return ONLY the JSON object. No markdown, no commentary."
    )

    response = requests.post(
        "https://openrouter.ai/api/v1/chat/completions",
        headers={
            "Authorization":  f"Bearer {OPENROUTER_KEY}",
            "Content-Type":   "application/json",
            "HTTP-Referer":   "https://cmtaccounting.internal",
            "X-Title":        "CMT PSP Amendment Parser",
        },
        json={
            "model":   OPENROUTER_MODEL,
            "messages": [
                {"role": "system", "content": active_prompt},
                {"role": "user",   "content": user_msg},
            ],
            "response_format": _amendment_response_format(),
            "temperature": 0.0,
            "max_tokens":  4096,
        },
        timeout=120,
    )
    response.raise_for_status()

    raw    = response.json()["choices"][0]["message"]["content"]
    result = json.loads(raw)

    result.setdefault("amendment", {})
    result.setdefault("rule_changes", [])
    result["rule_changes"] = [_normalise_rule(r) for r in result["rule_changes"]]

    return result
