"""Praxis API loader — fetches PSP transactions via the Praxis payment gateway API.

Praxis is the payment orchestration layer connected to all ~20 PSPs we work with.
Instead of downloading PSP files manually, this loader pulls transactions directly
from the Praxis API and feeds them through the same transformer pipeline used by
the file-based loader.

Architecture
------------
The key difference from file_loader.py is that extract() is replaced by an API call.
Once we have a DataFrame of raw API rows, the existing transformer.transform() and
DB insert logic is identical — the rest of the pipeline doesn't care where the data
came from.

source_file values will be "praxis_api/{psp_name}/{date_from}_{date_to}" so the
audit trail is clear in the raw layer.

Configuration
-------------
Set these in environment variables or config.py before use:
    PRAXIS_API_URL      Base URL of the Praxis API (e.g. https://api.praxis.com/v1)
    PRAXIS_API_KEY      API key or Bearer token
    PRAXIS_MERCHANT_ID  Your merchant account ID (if required by their API)

TODO — Pending confirmation from Despina / technical team:
    - Exact base URL and authentication method (API key header vs OAuth)
    - Endpoint path for transaction listing
    - Available filter parameters (gateway/processor, date range, status)
    - Pagination mechanism (cursor, offset, or page number)
    - Whether Praxis exposes settlement-level or transaction-level detail
    - How Praxis identifies each PSP (gateway name or processor code)
    - Whether all ~20 PSPs are reachable via one endpoint or separate ones
"""

import os
import json
import time
from datetime import date, datetime, timezone
from typing import Optional

import pandas as pd
import requests

from db.engine import get_session
from db.models import RawPSPTransaction, CleanPSPTransaction, PSPSchemaRegistry
from transformers.registry import registry
from transformers.psp.generic import GenericPSPTransformer


# ── Configuration ────────────────────────────────────────────────────────────

PRAXIS_API_URL = os.environ.get("PRAXIS_API_URL", "")
PRAXIS_API_KEY = os.environ.get("PRAXIS_API_KEY", "")
PRAXIS_MERCHANT_ID = os.environ.get("PRAXIS_MERCHANT_ID", "")

# How many rows to request per API page (adjust based on Praxis limits)
PAGE_SIZE = 500

# Seconds to wait between paginated requests (avoid rate limiting)
REQUEST_DELAY = 0.25

# Maximum retries for a failed request before giving up
MAX_RETRIES = 3


# ── Praxis gateway name → our PSP transformer name ───────────────────────────
# Praxis will refer to each processor by its own internal gateway name.
# This maps those names to the canonical psp_name used in our transformer registry.
#
# TODO: populate these once the Praxis API response format is confirmed.
# The keys should match whatever value Praxis uses to identify the processor
# (e.g. in a "gateway" or "processor" field in their transaction response).

GATEWAY_TO_PSP_NAME = {
    # "SafeCharge":       "nuvei",
    # "Nuvei":            "nuvei",
    # "KoraPay":          "korapay",
    # "Zotapay":          "zotapay",
    # "SolidPayments":    "solidpayments",
    # "Finrax":           "finrax",
    # "Ozow":             "ozow",
    # "EFTPay":           "eftpay",
    # "VirtualPay":       "virtualpay",
    # "Skrill":           "skrill",
    # "Neteller":         "neteller",
    # "Directa24":        "directa24",
    # "AstroPay":         "astropay",
    # "LetKnow":          "letknow",
    # "TrustPayments":    "trustpayments",
    # "Swiffy":           "swiffy",
    # "Inatec":           "inatec",
}


class PraxisAPIError(Exception):
    """Raised when the Praxis API returns an error or unexpected response."""
    pass


class PraxisClient:
    """HTTP client for the Praxis transaction API.

    All endpoint paths and parameter names are marked TODO — fill them in
    once the API spec is obtained from Despina / technical team.
    """

    def __init__(self, base_url: str = None, api_key: str = None,
                 merchant_id: str = None):
        self.base_url = (base_url or PRAXIS_API_URL).rstrip("/")
        self.api_key = api_key or PRAXIS_API_KEY
        self.merchant_id = merchant_id or PRAXIS_MERCHANT_ID

        if not self.base_url:
            raise PraxisAPIError(
                "PRAXIS_API_URL is not set. "
                "Set the environment variable or pass base_url to PraxisClient()."
            )
        if not self.api_key:
            raise PraxisAPIError(
                "PRAXIS_API_KEY is not set. "
                "Set the environment variable or pass api_key to PraxisClient()."
            )

    def _headers(self) -> dict:
        """Build authentication headers.

        TODO: confirm whether Praxis uses Bearer token, X-API-Key, or HMAC.
        """
        return {
            "Authorization": f"Bearer {self.api_key}",
            # TODO: add any other required headers (e.g. merchant ID, version)
            # "X-Merchant-ID": self.merchant_id,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _get(self, path: str, params: dict) -> dict:
        """Make a GET request with retry logic."""
        url = f"{self.base_url}/{path.lstrip('/')}"

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = requests.get(url, headers=self._headers(), params=params, timeout=30)
                resp.raise_for_status()
                return resp.json()
            except requests.HTTPError as e:
                if resp.status_code == 429:
                    # Rate limited — wait and retry
                    wait = 2 ** attempt
                    print(f"  Rate limited by Praxis API, waiting {wait}s...")
                    time.sleep(wait)
                elif attempt == MAX_RETRIES:
                    raise PraxisAPIError(f"Praxis API error: {e}") from e
                else:
                    time.sleep(REQUEST_DELAY * attempt)
            except requests.RequestException as e:
                if attempt == MAX_RETRIES:
                    raise PraxisAPIError(f"Praxis API request failed: {e}") from e
                time.sleep(REQUEST_DELAY * attempt)

    def list_gateways(self) -> list[str]:
        """Fetch the list of available PSP gateways from Praxis.

        TODO: confirm endpoint path and response structure.
        Returns a list of gateway name strings.
        """
        # TODO: replace with real endpoint
        # resp = self._get("/gateways", {"merchant_id": self.merchant_id})
        # return [g["name"] for g in resp.get("gateways", [])]
        raise NotImplementedError(
            "TODO: implement list_gateways() once Praxis API spec is confirmed"
        )

    def fetch_transactions(
        self,
        date_from: date,
        date_to: date,
        gateway: str = None,
        status: str = None,
    ) -> list[dict]:
        """Fetch all transactions for a date range, handling pagination.

        Args:
            date_from: Start date (inclusive)
            date_to:   End date (inclusive)
            gateway:   Optional PSP gateway name to filter (e.g. "SafeCharge")
            status:    Optional status filter (e.g. "approved", "settled")

        Returns:
            List of raw transaction dicts as returned by the API.

        TODO: fill in the real endpoint path and parameter names.
        The structure below is a typical REST pagination pattern —
        adjust offset/cursor/page logic to match Praxis's actual mechanism.
        """
        all_rows = []
        offset = 0

        while True:
            # TODO: replace parameter names with actual Praxis API field names
            params = {
                "date_from": date_from.isoformat(),  # TODO: confirm date format
                "date_to": date_to.isoformat(),
                "limit": PAGE_SIZE,
                "offset": offset,
            }
            if gateway:
                params["gateway"] = gateway      # TODO: confirm param name
            if status:
                params["status"] = status        # TODO: confirm param name
            if self.merchant_id:
                params["merchant_id"] = self.merchant_id  # TODO: confirm param name

            # TODO: replace with real endpoint path
            resp = self._get("/transactions", params)

            # TODO: adjust these keys to match Praxis response structure
            rows = resp.get("transactions", resp.get("data", []))
            if not rows:
                break

            all_rows.extend(rows)

            # TODO: adjust pagination logic (cursor vs offset vs page+per_page)
            total = resp.get("total", resp.get("total_count", None))
            if total is not None and len(all_rows) >= total:
                break
            if len(rows) < PAGE_SIZE:
                break

            offset += PAGE_SIZE
            time.sleep(REQUEST_DELAY)

        return all_rows


def _response_to_dataframe(rows: list[dict]) -> pd.DataFrame:
    """Convert raw Praxis API response rows to a DataFrame.

    The column names here will be whatever Praxis returns in their JSON.
    The transformer's transform() will then map these to our standard schema.

    TODO: once the real API response is known, verify that the DataFrame
    columns match what the transformers expect (or add a Praxis-specific
    column mapping step here if Praxis normalises the format before delivery).
    """
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def _extract_gateway_name(row: dict) -> Optional[str]:
    """Extract the gateway/processor name from a Praxis transaction row.

    TODO: confirm the field name Praxis uses to identify the PSP gateway.
    Common candidates: "gateway", "processor", "payment_method", "provider".
    """
    # TODO: replace with actual field name from Praxis response
    return (row.get("gateway")
            or row.get("processor")
            or row.get("provider")
            or row.get("payment_method"))


def _load_generic_transformers_from_db():
    """Ensure generic transformers from the schema registry are registered."""
    session = get_session()
    try:
        rows = session.query(PSPSchemaRegistry).all()
        for row in rows:
            if registry.get_psp(row.psp_name) is None:
                registry.register_psp(GenericPSPTransformer.from_registry_row(row))
    finally:
        session.close()


def _insert_psp_rows(psp_name: str, clean_df: pd.DataFrame,
                     source_tag: str, raw_rows: list[dict]) -> int:
    """Insert raw + clean rows into the DB. Mirrors file_loader.load_psp_file()."""
    if clean_df is None or clean_df.empty:
        return 0

    session = get_session()
    count = 0

    try:
        for i, row in clean_df.iterrows():
            raw_data = raw_rows[i] if i < len(raw_rows) else row.to_dict()
            raw = RawPSPTransaction(
                psp_name=psp_name,
                source_file=source_tag,
                row_number=i,
                raw_data=raw_data,
            )
            session.add(raw)
            session.flush()

            ref_id = row.get("reference_id")
            if ref_id is None or pd.isna(ref_id):
                continue

            clean = CleanPSPTransaction(
                psp_name=psp_name,
                reference_id=str(ref_id),
                amount=row.get("amount"),
                currency=row.get("currency"),
                fee=row.get("fee"),
                status=row.get("status"),
                date=row.get("date") if pd.notna(row.get("date")) else None,
                source_file=source_tag,
                raw_id=raw.id,
            )
            session.add(clean)
            count += 1

        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

    return count


def load_via_praxis(
    date_from: date,
    date_to: date,
    gateway: str = None,
    client: PraxisClient = None,
) -> dict:
    """Fetch PSP transactions from Praxis API and load into the DB.

    If gateway is specified, only that PSP is fetched.
    If gateway is None, fetches all transactions and groups them by gateway name.

    Args:
        date_from: Start date for the fetch (inclusive)
        date_to:   End date for the fetch (inclusive)
        gateway:   Optional — fetch only this gateway (e.g. "SafeCharge")
        client:    Optional PraxisClient instance (creates one from env vars if None)

    Returns:
        Dict with stats: {psp_name: row_count, ...}
    """
    if client is None:
        client = PraxisClient()

    _load_generic_transformers_from_db()
    registry.auto_discover()

    raw_rows = client.fetch_transactions(
        date_from=date_from,
        date_to=date_to,
        gateway=gateway,
    )

    if not raw_rows:
        print(f"  Praxis API: no transactions for {date_from} → {date_to}")
        return {}

    print(f"  Praxis API: {len(raw_rows)} raw rows fetched")

    # Group rows by gateway so each PSP goes through its own transformer
    grouped: dict[str, list[dict]] = {}
    ungrouped = []

    for row in raw_rows:
        gw = _extract_gateway_name(row)
        if gw:
            grouped.setdefault(gw, []).append(row)
        else:
            ungrouped.append(row)

    if ungrouped:
        print(f"  WARNING: {len(ungrouped)} rows had no gateway field — skipped")

    stats = {}

    for gw_name, gw_rows in grouped.items():
        # Resolve gateway name → our PSP name
        psp_name = GATEWAY_TO_PSP_NAME.get(gw_name)
        if psp_name is None:
            # Try case-insensitive match as fallback
            psp_name = GATEWAY_TO_PSP_NAME.get(gw_name.lower())
        if psp_name is None:
            print(f"  WARNING: unknown gateway '{gw_name}' ({len(gw_rows)} rows) "
                  f"— add to GATEWAY_TO_PSP_NAME mapping")
            continue

        transformer = registry.get_psp(psp_name)
        if transformer is None:
            print(f"  WARNING: no transformer registered for '{psp_name}' — skipping")
            continue

        df = _response_to_dataframe(gw_rows)
        clean_df = transformer.transform(df)

        source_tag = f"praxis_api/{psp_name}/{date_from}_{date_to}"
        count = _insert_psp_rows(psp_name, clean_df, source_tag, gw_rows)

        stats[psp_name] = count
        print(f"  Praxis [{psp_name}] via gateway '{gw_name}': {count} rows")

    return stats


def load_all_gateways_via_praxis(
    date_from: date,
    date_to: date,
    client: PraxisClient = None,
) -> dict:
    """Convenience wrapper — fetches all gateways for the given date range.

    Equivalent to load_via_praxis() with no gateway filter.
    """
    return load_via_praxis(date_from=date_from, date_to=date_to, client=client)
