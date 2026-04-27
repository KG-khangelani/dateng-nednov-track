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
    }
