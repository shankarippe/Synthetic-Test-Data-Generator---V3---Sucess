"""
core/t24_data_library.py
------------------------
Temenos T24 / Transact — Precise Domain Data Library

This module provides EXACT T24-format values for every common field.
Instead of random strings, every value matches real T24 conventions:
  - Field naming patterns (e.g., CURRENCY → 3-char ISO, CUSTOMER.NO → CUST######)
  - T24 mnemonics (CATEGORY, ARRANGEMENT.ID, PRODUCT.LINE patterns)
  - Real banking codes (BIC/SWIFT, IBAN prefixes, SORT.CODE format)
  - T24-specific status values (LIVE, INACT, PEND, etc.)
  - T24 record ID formats (Company!RecordId)

Usage:
    from core.t24_data_library import T24DataLibrary
    lib = T24DataLibrary()
    val = lib.resolve("CUSTOMER", "CURRENCY")          # → "USD"
    val = lib.resolve("ACCOUNT", "ACCOUNT.OFFICER")   # → "100123"
"""

from __future__ import annotations

import random
import string
from datetime import date, timedelta
from typing import Any


# ---------------------------------------------------------------------------
# T24 / Temenos-standard value pools
# ---------------------------------------------------------------------------

# ISO 4217 currency codes — T24 uses 3-char codes
T24_CURRENCIES = [
    "USD", "EUR", "GBP", "JPY", "CHF", "AUD", "CAD", "SGD", "HKD", "NZD",
    "SEK", "NOK", "DKK", "ZAR", "AED", "SAR", "QAR", "KWD", "BHD", "OMR",
    "INR", "PKR", "BDT", "LKR", "NPR", "MYR", "THB", "IDR", "PHP", "VND",
    "CNY", "KRW", "NGN", "KES", "GHS", "TZS", "UGX", "ZMW", "MWK", "RWF",
]

# T24 COMPANY codes (mnemonic format)
T24_COMPANIES = ["GB0010001", "US0010001", "SG0010001", "AE0010001", "NG0010001"]

# T24 CATEGORY codes — standard Temenos product categories
T24_CATEGORIES = {
    "deposit": ["1000", "1001", "1002", "1003", "1100", "1101", "1200", "1201"],
    "loan": ["2000", "2001", "2002", "2100", "2200", "2300", "2400", "2500"],
    "trade": ["4000", "4001", "4002", "4100", "4200"],
    "fx": ["5000", "5001", "5100", "5200"],
    "treasury": ["6000", "6001", "6100"],
    "nostro": ["1000"],
    "default": ["1000", "1001", "2000", "2001", "4000"],
}

# T24 ACCOUNT.OFFICER codes (numeric, 6-digit)
T24_ACCOUNT_OFFICERS = [f"{i:06d}" for i in range(100001, 100050)]

# T24 SECTOR / INDUSTRY codes
T24_SECTORS = [
    "1000", "1001", "1100", "1101", "1200", "1201", "1300", "1400",
    "2000", "2001", "2100", "3000", "3001", "4000", "5000", "9000",
]

# T24 NATIONALITY / COUNTRY codes (ISO 3166-1 alpha-2)
T24_COUNTRIES = [
    "GB", "US", "DE", "FR", "CH", "SG", "AE", "SA", "QA", "KW",
    "IN", "CN", "JP", "AU", "CA", "NG", "KE", "ZA", "GH", "TZ",
    "MY", "TH", "ID", "PH", "VN", "BH", "OM", "EG", "MA", "TN",
]

# T24 LANGUAGE codes
T24_LANGUAGES = ["1", "2", "3"]  # 1=English, 2=French, 3=Arabic

# T24 CUSTOMER.STATUS values
T24_CUSTOMER_STATUSES = ["LIVE", "INACT", "PEND"]

# T24 RECORD.STATUS values (used across many tables)
T24_RECORD_STATUSES = ["LIVE", "INACT", "PEND", "HISTORY", "REVERSED"]

# T24 ONLINE.ACTUAL.BAL / account types
T24_ACCOUNT_TYPES = ["CURRENT", "SAVINGS", "NOSTRO", "INTERNAL", "LOAN", "OVERDRAFT"]

# T24 ARRANGEMENT status values
T24_ARRANGEMENT_STATUSES = ["ACTIVE", "SUSPENDED", "MATURED", "CLOSED", "DEFAULTED"]

# T24 TRANSACTION.CODE values
T24_TRANSACTION_CODES = [
    "AC",  # Account Credit
    "DR",  # Account Debit
    "IN",  # Interest Credit
    "CH",  # Charges
    "FE",  # Fees
    "RP",  # Repayment
    "DS",  # Disbursement
    "CL",  # Collateral
    "OD",  # Overdraft
    "FX",  # Foreign Exchange
]

# T24 PAYMENT order types
T24_PAYMENT_TYPES = ["OUR", "BEN", "SHA"]

# T24 CHANNEL values
T24_CHANNELS = ["INTERNET", "MOBILE", "BRANCH", "ATM", "API", "TELLER", "IVR"]

# T24 COLLATERAL.TYPE
T24_COLLATERAL_TYPES = [
    "CASH", "PROPERTY", "SHARES", "BONDS", "VEHICLE", "GOLD",
    "GUARANTEE", "LC", "INSURANCE", "LAND",
]

# T24 PRODUCT.LINE codes
T24_PRODUCT_LINES = [
    "DEPOSITS", "LOANS", "TRADE", "TREASURY", "FOREX", "PAYMENTS",
    "CARDS", "MORTGAGES", "LEASING", "ISLAMIC",
]

# T24 INTEREST.KEY values
T24_INTEREST_KEYS = [
    "LIBOR1M", "LIBOR3M", "LIBOR6M", "LIBOR12M",
    "SOFR", "EURIBOR3M", "EURIBOR6M",
    "PRIME", "BASERATE", "FIXED",
]

# T24 CHARGE.CODE values
T24_CHARGE_CODES = [
    "ACHARGE", "BCHARGE", "CCHARGE", "SERVICE", "ADMIN",
    "PROCESS", "COMMISSION", "STAMP", "DUTY",
]

# T24 CONDITION.GROUP
T24_CONDITION_GROUPS = ["STANDARD", "PREMIUM", "CORPORATE", "RETAIL", "SME", "WHOLESALE"]

# T24 CONTRACT types
T24_CONTRACT_TYPES = ["FIXED", "VARIABLE", "REVOLVING", "TERM", "DEMAND"]

# T24 CUSTOMER type / CUSTOMER.SEGMENT
T24_CUSTOMER_TYPES = [
    "INDIVIDUAL", "CORPORATE", "BANK", "GOVERNMENT",
    "TRUST", "NGO", "FUND", "INSURANCE",
]

# T24 YES/NO flag (T24 uses these exactly)
T24_YES_NO = ["YES", "NO"]
T24_Y_N = ["Y", "N"]

# T24 LIMIT types
T24_LIMIT_TYPES = ["OD", "TR", "MN", "SC", "ST", "MG", "LN", "CC"]

# T24 PAYMENT order status
T24_PAYMENT_STATUSES = [
    "PAID", "CANCEL", "HOLD", "PEND", "PARTP", "REVERSED",
]

# T24 DEBIT.CREDIT.IND
T24_DR_CR = ["D", "C"]

# T24 NOSTRO.ACCOUNT format (COMPANY-CURRENCY-SEQ)
def t24_nostro_account(company: str = "GB0010001", currency: str = "USD") -> str:
    return f"{company}-{currency}-{random.randint(1,99):02d}"

# T24 BIC / SWIFT codes
T24_BIC_CODES = [
    "MIDLGB22", "BARCGB22", "HSBCGB2L", "NATXGB21", "LOYDGB2L",
    "DEUTDEDB", "BNPAFRPP", "UBSWCHZH", "CITITGR2", "CITIUS33",
    "CHASUS33", "BOFAUS3N", "WFBIUS6S", "RBOSGB2L", "SCBLSGSG",
    "OCBCSGSG", "UOBBSGSG", "DBSSSGSG", "MASHB2B2", "NBADAEAA",
]

# T24 SORT.CODE format (UK): 6 digits grouped as XX-XX-XX
def t24_sort_code() -> str:
    return f"{random.randint(10,99)}-{random.randint(10,99)}-{random.randint(10,99)}"

# T24 ACCOUNT.NO format — typically numeric, padded to 10 digits
def t24_account_no() -> str:
    return f"{random.randint(1000000000, 9999999999)}"

# T24 CUSTOMER.NO — typically 6-digit numeric
def t24_customer_no() -> str:
    return str(random.randint(100001, 999999))

# T24 ARRANGEMENT.ID — 8-char alphanumeric
def t24_arrangement_id() -> str:
    return "AA" + "".join(random.choices(string.digits, k=10))

# T24 transaction reference — typically format: YYYYMMDD.HHMMSS.NNNNNN
def t24_txn_reference(table_prefix: str = "FT") -> str:
    today = date.today()
    return f"{table_prefix}{today.strftime('%y%m%d')}{random.randint(1,999999):06d}"

# T24 IBAN format (simplified, not checksum-valid but format-correct)
def t24_iban(country: str = "GB") -> str:
    check = random.randint(10, 99)
    bank = "MIDL"
    sort = f"{random.randint(100000, 999999)}"
    acct = f"{random.randint(10000000, 99999999)}"
    return f"{country}{check}{bank}{sort}{acct}"[:34]


# ---------------------------------------------------------------------------
# Column name → T24 value resolver
# ---------------------------------------------------------------------------

class T24DataLibrary:
    """
    Maps column names (using T24 naming conventions) to precise value sets.
    
    T24 uses dot-notation for field names: CUSTOMER.TYPE, ACCOUNT.OFFICER
    In SQL these become underscores: customer_type, account_officer
    
    Resolution priority:
      1. Exact column name match (normalised, dots→underscores, lowercase)
      2. Suffix match (e.g., anything ending in _currency → currency codes)
      3. Substring match (e.g., anything containing _status_ → status codes)
      4. Table-context match (e.g., if table is CUSTOMER, _type → customer types)
    """

    def __init__(self):
        self._build_exact_map()
        self._build_suffix_map()
        self._build_substring_map()

    def _build_exact_map(self):
        """Exact column name → value list."""
        self._exact: dict[str, list | None] = {
            # Currency fields
            "currency": T24_CURRENCIES,
            "ccy": T24_CURRENCIES,
            "currency_code": T24_CURRENCIES,
            "local_ccy": T24_CURRENCIES,
            "fcy": T24_CURRENCIES,
            "fcy_amount": None,  # numeric
            "settlement_currency": T24_CURRENCIES,
            "deal_currency": T24_CURRENCIES,
            "counter_currency": T24_CURRENCIES,
            "base_currency": T24_CURRENCIES,
            "report_currency": T24_CURRENCIES,

            # Country
            "country": T24_COUNTRIES,
            "country_code": T24_COUNTRIES,
            "nationality": T24_COUNTRIES,
            "residence": T24_COUNTRIES,
            "domicile": T24_COUNTRIES,
            "country_of_birth": T24_COUNTRIES,
            "registered_country": T24_COUNTRIES,

            # Customer
            "customer_type": T24_CUSTOMER_TYPES,
            "customer_status": T24_CUSTOMER_STATUSES,
            "cust_type": T24_CUSTOMER_TYPES,
            "segment": T24_CONDITION_GROUPS,
            "customer_segment": T24_CONDITION_GROUPS,
            "sector": T24_SECTORS,
            "industry": T24_SECTORS,
            "language": T24_LANGUAGES,

            # Account
            "account_type": T24_ACCOUNT_TYPES,
            "acct_type": T24_ACCOUNT_TYPES,
            "account_officer": T24_ACCOUNT_OFFICERS,
            "account_mgr": T24_ACCOUNT_OFFICERS,
            "relationship_mgr": T24_ACCOUNT_OFFICERS,
            "rm_code": T24_ACCOUNT_OFFICERS,

            # Status fields
            "record_status": T24_RECORD_STATUSES,
            "status": T24_RECORD_STATUSES,
            "account_status": ["LIVE", "INACT", "CLSD", "BLCK", "DORM"],
            "arrangement_status": T24_ARRANGEMENT_STATUSES,
            "loan_status": T24_ARRANGEMENT_STATUSES,
            "deposit_status": ["ACTIVE", "MATURED", "WITHDRAWN", "ROLLED"],

            # Transaction
            "transaction_code": T24_TRANSACTION_CODES,
            "txn_code": T24_TRANSACTION_CODES,
            "transaction_type": T24_TRANSACTION_CODES,
            "debit_credit_ind": T24_DR_CR,
            "dr_cr_indicator": T24_DR_CR,
            "dr_cr": T24_DR_CR,
            "narrative": None,  # free text — skip

            # Payment
            "payment_type": T24_PAYMENT_TYPES,
            "charge_type": T24_PAYMENT_TYPES,
            "payment_status": T24_PAYMENT_STATUSES,
            "channel": T24_CHANNELS,
            "delivery_channel": T24_CHANNELS,
            "booking_channel": T24_CHANNELS,

            # Product
            "product_line": T24_PRODUCT_LINES,
            "product_type": T24_PRODUCT_LINES,
            "category": T24_CATEGORIES["default"],
            "product_category": T24_CATEGORIES["default"],

            # Interest / rate
            "interest_key": T24_INTEREST_KEYS,
            "rate_type": ["FIXED", "VARIABLE", "FLOATING"],
            "rate_basis": ["A", "M", "D"],  # Annual/Monthly/Daily
            "compound_type": ["SIMPLE", "COMPOUND"],
            "interest_type": ["FIXED", "FLOATING", "ZERO"],

            # Contract
            "contract_type": T24_CONTRACT_TYPES,
            "loan_type": T24_CONTRACT_TYPES,
            "collateral_type": T24_COLLATERAL_TYPES,

            # Condition / group
            "condition_group": T24_CONDITION_GROUPS,
            "limit_type": T24_LIMIT_TYPES,
            "charge_code": T24_CHARGE_CODES,

            # BIC / Bank
            "bic_code": T24_BIC_CODES,
            "swift_code": T24_BIC_CODES,
            "bank_code": T24_BIC_CODES,

            # Flags
            "online_accrual": T24_Y_N,
            "accrual_flag": T24_Y_N,
            "netting_flag": T24_Y_N,
            "maturity_alert": T24_Y_N,
            "auto_renewal": T24_YES_NO,
            "tax_applicable": T24_YES_NO,
            "dormant": T24_YES_NO,
            "joint_account": T24_YES_NO,
            "iban_required": T24_YES_NO,
            "block_indicator": T24_Y_N,

            # Company
            "company_code": T24_COMPANIES,
            "co_code": T24_COMPANIES,
            "source_co_code": T24_COMPANIES,
            "dept_code": [f"{i:04d}" for i in range(1000, 1020)],

            # T24 operational
            "curr_no": None,   # version counter — handled by heuristic
            "m": None,
            "s": None,
        }

    def _build_suffix_map(self):
        """Column name suffix → value list (case-insensitive ends-with)."""
        self._suffixes: dict[str, list | None] = {
            "_currency": T24_CURRENCIES,
            "_ccy": T24_CURRENCIES,
            "_country": T24_COUNTRIES,
            "_status": T24_RECORD_STATUSES,
            "_type": T24_CUSTOMER_TYPES,   # broad default; overridden by exact
            "_channel": T24_CHANNELS,
            "_indicator": T24_Y_N,
            "_flag": T24_Y_N,
            "_code": None,   # too generic — skip
            "_language": T24_LANGUAGES,
            "_sector": T24_SECTORS,
            "_officer": T24_ACCOUNT_OFFICERS,
            "_category": T24_CATEGORIES["default"],
            "_line": T24_PRODUCT_LINES,
        }

    def _build_substring_map(self):
        """Column name contains substring → value list."""
        self._substrings: dict[str, list | None] = {
            "currency": T24_CURRENCIES,
            "country": T24_COUNTRIES,
            "channel": T24_CHANNELS,
            "product": T24_PRODUCT_LINES,
            "officer": T24_ACCOUNT_OFFICERS,
            "status": T24_RECORD_STATUSES,
            "swift": T24_BIC_CODES,
            "bic": T24_BIC_CODES,
            "sector": T24_SECTORS,
            "collateral": T24_COLLATERAL_TYPES,
            "interest_key": T24_INTEREST_KEYS,
        }

    # ------------------------------------------------------------------
    # Staging prefix stripper
    # ------------------------------------------------------------------

    # Any of these prefixes can appear before the real T24 app name.
    # e.g.  tstg_aa_product  →  aa_product  →  T24 app: AA.PRODUCT
    #        stg_ac_charge   →  ac_charge   →  T24 app: AC.CHARGE
    #        dm_ft_payment   →  ft_payment  →  T24 app: FT
    #        w1_customer     →  customer    →  T24 app: CUSTOMER
    _STAGING_PREFIXES = (
        "tstg_", "stg_", "dm_", "w1_", "w2_", "w3_",
        "dw_", "edw_", "raw_", "land_", "src_", "fact_", "dim_","fact_","dwh_",
    )

    # T24 application module → keywords in table name after prefix stripped
    # Maps the first segment (e.g. "aa" from "aa_product") to T24 module
    _T24_MODULE_MAP = {
        "aa":   "arrangement",   # AA — Arrangement Architecture (loans, deposits)
        "ac":   "accounting",    # AC — Core Accounting
        "de":   "deposit",       # DE — Deposits
        "ft":   "payment",       # FT — Funds Transfer / Payments
        "fx":   "forex",         # FX — Foreign Exchange
        "lc":   "trade",         # LC — Letters of Credit
        "li":   "limit",         # LI — Limits
        "sc":   "securities",    # SC — Securities
        "sw":   "swift",         # SW — SWIFT Messaging
        "eb":   "ebanking",      # EB — E-Banking
        "st":   "standing",      # ST — Standing Orders
        "mm":   "money_market",  # MM — Money Market
        "md":   "market_data",   # MD — Market Data
        "re":   "retail",        # RE — Retail
        "cr":   "cards",         # CR — Cards
        "am":   "asset",         # AM — Asset Management
        "pf":   "portfolio",     # PF — Portfolio
        "cust": "customer",      # CUST — Customer
        "acct": "account",       # ACCT — Account
    }

    def _strip_staging_prefix(self, table: str) -> str:
        """
        Strip any known staging/warehouse prefix from the table name.

        Examples:
            tstg_aa_product_details  →  aa_product_details
            stg_customer             →  customer
            dm_ft_payment            →  ft_payment
            w1_ac_charge_request     →  ac_charge_request
        """
        tbl = table.lower()
        for pfx in self._STAGING_PREFIXES:
            if tbl.startswith(pfx):
                return tbl[len(pfx):]
        return tbl

    def _detect_t24_module(self, stripped_table: str) -> str | None:
        """
        Given a table name with staging prefix already removed,
        detect which T24 module it belongs to from the first segment.

        Examples:
            aa_product_details  → "arrangement"
            ft_payment          → "payment"
            customer            → "customer"
        """
        parts = stripped_table.split("_")
        first = parts[0] if parts else ""
        return self._T24_MODULE_MAP.get(first)

    # ------------------------------------------------------------------
    # Public resolution API
    # ------------------------------------------------------------------

    def resolve(self, table: str, col_name: str) -> list | None:
        """
        Return a list of domain values for this (table, col_name) pair,
        or None if no T24 match (fall through to type dispatch).

        Staging prefixes (tstg_, stg_, dm_, w1_, etc.) are stripped
        automatically before matching, so tstg_aa_product is treated
        exactly the same as aa_product (T24 AA module — Arrangements).
        """
        col_lower = col_name.lower().replace(".", "_").replace("-", "_")

        # 1. Exact column name match
        if col_lower in self._exact:
            return self._exact[col_lower]

        # 2. Suffix match
        for suffix, vals in self._suffixes.items():
            if col_lower.endswith(suffix):
                return vals

        # 3. Substring match
        for substr, vals in self._substrings.items():
            if substr in col_lower:
                return vals

        # 4. Table-context enrichment (staging prefix stripped here)
        stripped = self._strip_staging_prefix(table)
        return self._table_context_resolve(stripped, col_lower)

    def _table_context_resolve(self, stripped_table: str, col_lower: str) -> list | None:
        """
        Use stripped table name + T24 module context to infer values.

        'stripped_table' has already had the staging prefix removed, so:
            tstg_aa_arrangement  →  aa_arrangement  (module: arrangement)
            tstg_ft_payment      →  ft_payment      (module: payment)
            tstg_customer        →  customer        (module: customer)
        """
        tbl = stripped_table.lower()

        # Detect T24 module from the first segment (aa_, ft_, ac_, etc.)
        t24_module = self._detect_t24_module(tbl)

        # ── AA module — Arrangement Architecture (loans, deposits, savings) ──
        if t24_module == "arrangement" or any(
            k in tbl for k in ("arrangement", "aa_arr", "aa_account", "aa_product")
        ):
            if "type" in col_lower:
                return T24_CONTRACT_TYPES
            if "status" in col_lower:
                return T24_ARRANGEMENT_STATUSES
            if "category" in col_lower:
                return T24_CATEGORIES["loan"]
            if "product" in col_lower:
                return ["LOAN", "MORTGAGE", "OD", "TERM", "REVOLVING", "CALL", "SAVINGS"]
            if "condition" in col_lower or "group" in col_lower:
                return T24_CONDITION_GROUPS

        # ── DE / Deposit module ───────────────────────────────────────────────
        if t24_module == "deposit" or any(k in tbl for k in ("deposit", "saving", "aa_account")):
            if "type" in col_lower:
                return T24_ACCOUNT_TYPES
            if "status" in col_lower:
                return ["LIVE", "INACT", "CLSD", "DORM"]
            if "category" in col_lower:
                return T24_CATEGORIES["deposit"]
            if "product" in col_lower:
                return ["CALL", "NOTICE", "FIXED", "CURRENT", "SAVINGS"]
            if "renewal" in col_lower or "rollover" in col_lower:
                return T24_YES_NO

        # ── AC module — Accounting ────────────────────────────────────────────
        if t24_module == "accounting" or "ac_" in tbl:
            if "type" in col_lower:
                return ["DEBIT", "CREDIT", "TRANSFER", "CHARGE", "INTEREST"]
            if "status" in col_lower:
                return ["LIVE", "PEND", "REVERSED", "CANCELLED"]
            if "category" in col_lower:
                return T24_CATEGORIES["default"]

        # ── FT module — Funds Transfer / Payments ─────────────────────────────
        if t24_module == "payment" or any(k in tbl for k in ("payment", "transfer", "remit", "ft_")):
            if "status" in col_lower:
                return T24_PAYMENT_STATUSES
            if "type" in col_lower:
                return ["SWIFT", "SEPA", "ACH", "RTGS", "CHAPS", "FASTER", "INTERNAL"]
            if "priority" in col_lower:
                return ["NORMAL", "URGENT", "EXPRESS"]
            if "charge" in col_lower:
                return T24_PAYMENT_TYPES  # OUR/BEN/SHA

        # ── FX module — Foreign Exchange ──────────────────────────────────────
        if t24_module == "forex" or any(k in tbl for k in ("forex", "fx_", "_fx", "treasury", "swap")):
            if "type" in col_lower:
                return ["SPOT", "FORWARD", "SWAP", "NDF", "OPTION", "DEPOSIT"]
            if "status" in col_lower:
                return ["LIVE", "MATCHED", "SETTLED", "CANCELLED", "CONFIRMED"]
            if "deal" in col_lower and "type" in col_lower:
                return ["BUY", "SELL"]

        # ── LC / Trade Finance module ─────────────────────────────────────────
        if t24_module == "trade" or any(k in tbl for k in ("lc_", "_lc", "guarantee", "collection", "trade")):
            if "type" in col_lower:
                return ["IMPORT", "EXPORT", "STANBY", "GUARANTEE", "CLEAN"]
            if "status" in col_lower:
                return ["ISSUED", "AMENDED", "SETTLED", "CANCELLED", "EXPIRED", "CONFIRMED"]

        # ── LI module — Limits ────────────────────────────────────────────────
        if t24_module == "limit" or "limit" in tbl:
            if "type" in col_lower:
                return T24_LIMIT_TYPES
            if "status" in col_lower:
                return ["LIVE", "INACT", "EXPIRED", "BREACHED"]
            if "liability" in col_lower or "category" in col_lower:
                return ["CLEAN", "SECURED", "UNSECURED", "DOCUMENTARY"]

        # ── CUSTOMER / CUST tables ────────────────────────────────────────────
        if t24_module == "customer" or any(k in tbl for k in ("customer", "client", "party", "cust_")):
            if "type" in col_lower:
                return T24_CUSTOMER_TYPES
            if "status" in col_lower:
                return T24_CUSTOMER_STATUSES
            if "segment" in col_lower or "group" in col_lower:
                return T24_CONDITION_GROUPS
            if "class" in col_lower:
                return ["RETAIL", "CORPORATE", "PREMIUM", "PRIVATE"]
            if "risk" in col_lower:
                return ["LOW", "MEDIUM", "HIGH", "VERY_HIGH"]

        # ── ACCOUNT / ACCT tables ─────────────────────────────────────────────
        if t24_module == "account" or any(k in tbl for k in ("account", "acct_")):
            if "type" in col_lower:
                return T24_ACCOUNT_TYPES
            if "status" in col_lower:
                return ["LIVE", "INACT", "CLSD", "BLCK", "DORM"]
            if "category" in col_lower:
                return T24_CATEGORIES["deposit"]

        # ── SC — Securities ───────────────────────────────────────────────────
        if t24_module == "securities" or "securit" in tbl:
            if "type" in col_lower:
                return ["EQUITY", "BOND", "FUND", "ETF", "DERIVATIVE", "WARRANT"]
            if "status" in col_lower:
                return ["ACTIVE", "SUSPENDED", "DELISTED", "MATURED"]

        # ── Generic fallback using raw table keywords ─────────────────────────
        if any(k in tbl for k in ("loan", "lending", "credit", "facilit")):
            if "type" in col_lower:
                return T24_CONTRACT_TYPES
            if "status" in col_lower:
                return T24_ARRANGEMENT_STATUSES

        return None

    # ------------------------------------------------------------------
    # T24 ID generators — called by DataGenerator for PK columns
    # ------------------------------------------------------------------

    def generate_pk_value(self, table: str, col: str, counter: int) -> str:
        """
        Generate a T24-format PK value based on table semantics.
        Staging prefix (tstg_, stg_, etc.) is stripped first so
        tstg_aa_arrangement gets the same AA-format ID as aa_arrangement.
        """
        # Strip staging prefix before semantic matching
        tbl = self._strip_staging_prefix(table)
        t24_module = self._detect_t24_module(tbl)

        if "customer" in tbl or t24_module == "customer":
            return str(100000 + counter)

        if ("account" in tbl or t24_module == "account") and "arrangement" not in tbl:
            return f"{random.randint(10,99)}-{random.randint(100000,999999)}-{random.randint(10,99)}"

        if "arrangement" in tbl or t24_module == "arrangement" or tbl.startswith("aa_"):
            return f"AA{date.today().strftime('%y%m%d')}{counter:06d}"

        if any(k in tbl for k in ("stmt", "statement", "entry", "txn", "transaction")):
            return t24_txn_reference()

        if any(k in tbl for k in ("company", "co_")):
            return random.choice(T24_COMPANIES)

        if any(k in tbl for k in ("limit", "collateral")):
            return f"LI{date.today().year}{counter:08d}"

        if any(k in tbl for k in ("payment", "transfer")):
            return t24_txn_reference("FT")

        if "product" in tbl:
            return f"PROD{counter:06d}"

        # Default T24 ID: TABLE_PREFIX + counter
        prefix = "".join(c for c in tbl.upper() if c.isalpha())[:4]
        return f"{prefix}{counter:08d}"


# Singleton instance
_library = None

def get_t24_library() -> T24DataLibrary:
    global _library
    if _library is None:
        _library = T24DataLibrary()
    return _library