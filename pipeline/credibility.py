import os
from pathlib import Path

import yaml
from pyspark.sql import functions as F


SA_PROVINCES = {
    "EASTERN CAPE",
    "FREE STATE",
    "GAUTENG",
    "KWAZULU-NATAL",
    "LIMPOPO",
    "MPUMALANGA",
    "NORTH WEST",
    "NORTHERN CAPE",
    "WESTERN CAPE",
}

TRANSACTION_TYPES = {"CREDIT", "DEBIT", "FEE", "REVERSAL"}
TRANSACTION_CHANNELS = {"POS", "APP", "ATM", "EFT", "USSD", "INTERNAL"}
ACCOUNT_TYPES = {"SAVINGS", "TRANSACTIONAL", "CREDIT"}
ACCOUNT_STATUSES = {"ACTIVE", "DORMANT", "CLOSED", "SUSPENDED"}
PRODUCT_TIERS = {"BASIC", "STANDARD", "PREMIUM"}
DIGITAL_CHANNELS = {"APP", "USSD", "WEB"}
GENDERS = {"M", "F", "NB", "UNKNOWN"}
KYC_STATUSES = {"VERIFIED", "PENDING", "FAILED"}

DEFAULT_CREDIBILITY_RULES = "/data/config/credibility_rules.yaml"
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DOMAIN_VALUES = {
    "transactions.transaction_type": TRANSACTION_TYPES,
    "transactions.channel": TRANSACTION_CHANNELS,
    "transactions.currency": {"ZAR"},
    "transactions.province": SA_PROVINCES,
    "accounts.account_type": ACCOUNT_TYPES,
    "accounts.account_status": ACCOUNT_STATUSES,
    "accounts.product_tier": PRODUCT_TIERS,
    "accounts.digital_channel": DIGITAL_CHANNELS,
    "customers.gender": GENDERS,
    "customers.province": SA_PROVINCES,
    "customers.kyc_status": KYC_STATUSES,
}


def trimmed(col):
    return F.trim(col.cast("string"))


def is_blank(col):
    value = trimmed(col)
    return value.isNull() | (value == "")


def normalized(col):
    return F.upper(trimmed(col))


def invalid_domain(col, allowed_values):
    return ~is_blank(col) & ~normalized(col).isin(*sorted(allowed_values))


def count_if(predicate):
    return F.sum(F.when(predicate, F.lit(1)).otherwise(F.lit(0))).cast("long")


def advisory_rule_catalog():
    return {
        "INVALID_DOMAIN": "Value outside a documented code set.",
        "TEMPORAL_ANOMALY": "Date relationship is impossible or suspicious.",
        "REFERENTIAL_GAP": "Foreign key does not resolve across raw sources.",
        "NUMERIC_OUTLIER": "Numeric field is outside an expected operating range.",
        "PROFILE_INCOMPLETE": "Customer or account profile is missing expected attributes.",
        "LOCATION_MISMATCH": "Transaction geography differs from linked customer geography.",
        "DOMAIN_DRIFT": "Known domain column contains new values or high-cardinality unknown value growth.",
        "GEO_COORDINATE_ANOMALY": "Coordinate is missing, malformed, out of bounds, swapped, or inconsistent with coarse geography.",
    }


def _existing_path(*candidates):
    for candidate in candidates:
        if candidate and Path(candidate).exists():
            return str(candidate)
    return str(candidates[0])


def load_credibility_rules(config=None):
    configured = ((config or {}).get("credibility") or {}).get("rules_path")
    path = _existing_path(
        os.environ.get("CREDIBILITY_RULES_CONFIG", configured or DEFAULT_CREDIBILITY_RULES),
        PROJECT_ROOT / "config" / "credibility_rules.yaml",
    )
    if not Path(path).exists():
        return {"profile": {}, "domains": {}, "advisory_rules": {}}
    with open(path, "r", encoding="utf-8") as handle:
        rules = yaml.safe_load(handle) or {}
    rules["_rules_path"] = path
    return rules


def _normalise_values(values):
    result = set()
    for value in values or []:
        text = str(value).strip().upper()
        if text:
            result.add(text)
    return result


def domain_values(field, rules_or_settings=None, fallback=None):
    fallback_values = fallback if fallback is not None else DEFAULT_DOMAIN_VALUES.get(field, set())
    domains = (rules_or_settings or {}).get("domains") or {}
    config = domains.get(field) or {}
    values = config.get("known_values") or config.get("allowed_values")
    if values is None:
        return _normalise_values(fallback_values)
    return _normalise_values(values)


def configured_domains(rules=None):
    rules = rules or {}
    configured = rules.get("domains") or {}
    fields = set(DEFAULT_DOMAIN_VALUES) | set(configured)
    result = {}
    for field in sorted(fields):
        policy = configured.get(field) or {}
        result[field] = {
            "known_values": sorted(domain_values(field, rules)),
            "normalization": policy.get("normalization", "upper_trim"),
            "unknown_policy": policy.get("unknown_policy", "audit_only"),
            "high_cardinality_threshold": int(policy.get("high_cardinality_threshold", 50)),
        }
    return result


def domain_drift_settings(rules=None):
    rules = rules or {}
    profile = rules.get("profile") or {}
    domain_drift = profile.get("domain_drift") or {}
    geo = rules.get("geo") or {}
    return {
        "domain_drift_enabled": bool(domain_drift.get("enabled", True)),
        "domain_drift_unknown_threshold": int(domain_drift.get("high_cardinality_unknown_threshold", 50)),
        "domain_drift_unknown_ratio": float(domain_drift.get("high_cardinality_unknown_ratio", 0.25)),
        "domain_drift_top_k": int(domain_drift.get("top_k_unknown_values", profile.get("top_k", 5))),
        "enable_top_k": bool(profile.get("enable_top_k", True)),
        "enable_silver_provenance": bool(profile.get("enable_silver_provenance", False)),
        "geo_province_coordinate_check_enabled": bool(geo.get("enable_province_coordinate_check", False)),
        "domains": configured_domains(rules),
    }
