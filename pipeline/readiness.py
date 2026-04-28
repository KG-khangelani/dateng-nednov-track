from pyspark.sql import functions as F

from pipeline.common import normalise_currency
from pipeline.credibility import is_blank, normalized


SA_LAT_MIN = -35.0
SA_LAT_MAX = -22.0
SA_LON_MIN = 16.0
SA_LON_MAX = 33.5

PROVINCE_BOUNDS = {
    "EASTERN CAPE": (-34.5, -30.0, 22.0, 30.5),
    "FREE STATE": (-30.8, -26.5, 24.0, 29.8),
    "GAUTENG": (-26.9, -25.1, 27.0, 29.2),
    "KWAZULU-NATAL": (-31.3, -26.5, 28.5, 33.2),
    "LIMPOPO": (-25.7, -22.0, 26.0, 32.2),
    "MPUMALANGA": (-27.5, -24.3, 28.0, 32.4),
    "NORTH WEST": (-28.2, -24.5, 22.4, 28.5),
    "NORTHERN CAPE": (-32.8, -25.0, 16.0, 26.2),
    "WESTERN CAPE": (-34.9, -30.2, 17.5, 24.5),
}


def _metric(count, total):
    count = int(count or 0)
    total = int(total or 0)
    return {
        "count": count,
        "pct": round(count * 100.0 / total, 2) if total else 0.0,
    }


def currency_normalized(col):
    return normalized(col)


def currency_iso_candidate(col):
    value = currency_normalized(col)
    return F.when(value.rlike(r"^[A-Z]{3}$"), value)


def currency_numeric_candidate(col):
    value = F.trim(col.cast("string"))
    return F.when(value.rlike(r"^[0-9]{3}$"), value)


def currency_conversion_required(col):
    clean = normalise_currency(col)
    return ~is_blank(col) & clean.isNotNull() & (clean != F.lit("ZAR"))


def currency_conversion_ready(col):
    return currency_conversion_required(col) & (
        currency_iso_candidate(col).isNotNull() | currency_numeric_candidate(col).isNotNull()
    )


def currency_unknown(col):
    return currency_conversion_required(col) & currency_iso_candidate(col).isNull() & currency_numeric_candidate(col).isNull()


def coordinate_text(col):
    return F.trim(col.cast("string"))


def coordinate_parts(col):
    return F.split(coordinate_text(col), r"\s*,\s*")


def coordinate_latitude(col):
    parts = coordinate_parts(col)
    return F.when(~is_blank(col) & (F.size(parts) == 2), parts.getItem(0).cast("double"))


def coordinate_longitude(col):
    parts = coordinate_parts(col)
    return F.when(~is_blank(col) & (F.size(parts) == 2), parts.getItem(1).cast("double"))


def geo_parse_failed(coord_col, lat_col, lon_col):
    return ~is_blank(coord_col) & (
        (F.size(coordinate_parts(coord_col)) != 2) | lat_col.isNull() | lon_col.isNull()
    )


def geo_invalid_world_bounds(lat_col, lon_col):
    return lat_col.isNotNull() & lon_col.isNotNull() & (
        (lat_col < -90.0) | (lat_col > 90.0) | (lon_col < -180.0) | (lon_col > 180.0)
    )


def geo_zero_zero(lat_col, lon_col):
    return lat_col.isNotNull() & lon_col.isNotNull() & (lat_col == 0.0) & (lon_col == 0.0)


def geo_possible_swap(lat_col, lon_col):
    return lat_col.isNotNull() & lon_col.isNotNull() & (
        lat_col.between(SA_LON_MIN, SA_LON_MAX) & lon_col.between(SA_LAT_MIN, SA_LAT_MAX)
    )


def geo_sa_sign_mismatch(lat_col, lon_col):
    return lat_col.isNotNull() & lon_col.isNotNull() & ~geo_invalid_world_bounds(lat_col, lon_col) & ~geo_zero_zero(lat_col, lon_col) & (
        (lat_col >= 0.0) | (lon_col <= 0.0)
    )


def geo_outside_sa_bounds(lat_col, lon_col):
    return lat_col.isNotNull() & lon_col.isNotNull() & ~geo_invalid_world_bounds(lat_col, lon_col) & ~geo_zero_zero(lat_col, lon_col) & (
        (lat_col < SA_LAT_MIN) | (lat_col > SA_LAT_MAX) | (lon_col < SA_LON_MIN) | (lon_col > SA_LON_MAX)
    )


def geo_province_mismatch(province_col, lat_col, lon_col):
    province = normalized(province_col)
    expr = F.lit(False)
    in_sa = ~geo_outside_sa_bounds(lat_col, lon_col) & ~geo_invalid_world_bounds(lat_col, lon_col) & ~geo_zero_zero(lat_col, lon_col)
    for name, (lat_min, lat_max, lon_min, lon_max) in PROVINCE_BOUNDS.items():
        in_province = lat_col.between(lat_min, lat_max) & lon_col.between(lon_min, lon_max)
        expr = expr | ((province == F.lit(name)) & in_sa & ~in_province)
    return expr


def geo_anomaly_flags(coord_col, province_col, lat_col, lon_col):
    flags = F.concat_ws(
        "|",
        F.when(geo_parse_failed(coord_col, lat_col, lon_col), F.lit("PARSE_FAILED")),
        F.when(geo_invalid_world_bounds(lat_col, lon_col), F.lit("INVALID_WORLD_BOUNDS")),
        F.when(geo_zero_zero(lat_col, lon_col), F.lit("ZERO_ZERO")),
        F.when(geo_possible_swap(lat_col, lon_col), F.lit("POSSIBLE_LAT_LON_SWAP")),
        F.when(geo_sa_sign_mismatch(lat_col, lon_col), F.lit("SA_SIGN_MISMATCH")),
        F.when(geo_outside_sa_bounds(lat_col, lon_col), F.lit("OUTSIDE_SA_BOUNDS")),
        F.when(geo_province_mismatch(province_col, lat_col, lon_col), F.lit("PROVINCE_COORDINATE_MISMATCH")),
    )
    return F.when(flags == "", F.lit(None).cast("string")).otherwise(flags)


def currency_readiness_summary(metrics, total):
    return {
        "policy": {
            "conversion_performed": False,
            "gold_currency_contract": "ZAR",
            "note": "This pipeline preserves non-ZAR evidence for downstream governed FX conversion rather than applying rates.",
        },
        "raw_currency_present": _metric(metrics.get("currency_present_count", 0), total),
        "zar_or_known_alias": _metric(metrics.get("currency_zar_like_count", 0), total),
        "non_zar_like": _metric(metrics.get("currency_non_zar_like_count", 0), total),
        "unknown_currency": _metric(metrics.get("currency_unknown_count", 0), total),
        "iso_code_candidate": _metric(metrics.get("currency_iso_candidate_count", 0), total),
        "numeric_code_candidate": _metric(metrics.get("currency_numeric_candidate_count", 0), total),
        "conversion_required": _metric(metrics.get("currency_conversion_required_count", 0), total),
        "conversion_ready": _metric(metrics.get("currency_conversion_ready_count", 0), total),
    }


def geo_quality_summary(metrics, total):
    return {
        "privacy": {
            "exact_coordinate_examples_emitted": False,
            "note": "Only aggregate counts are emitted in audit JSON.",
        },
        "coordinate_present": _metric(metrics.get("geo_coordinate_present_count", 0), total),
        "coordinate_missing": _metric(metrics.get("geo_coordinate_missing_count", 0), total),
        "parse_failed": _metric(metrics.get("geo_coordinate_parse_failed_count", 0), total),
        "invalid_world_bounds": _metric(metrics.get("geo_invalid_world_bounds_count", 0), total),
        "zero_zero": _metric(metrics.get("geo_zero_zero_count", 0), total),
        "possible_lat_lon_swap": _metric(metrics.get("geo_possible_swap_count", 0), total),
        "south_africa_sign_mismatch": _metric(metrics.get("geo_sa_sign_mismatch_count", 0), total),
        "outside_south_africa_bounds": _metric(metrics.get("geo_outside_sa_bounds_count", 0), total),
        "province_coordinate_mismatch": _metric(metrics.get("geo_province_mismatch_count", 0), total),
    }
