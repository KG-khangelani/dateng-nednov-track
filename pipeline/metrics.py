METRICS = {
    "source_record_counts": {},
    "dq_issues": {},
    "gold_layer_record_counts": {},
    "raw_profile": {},
}


def set_source_count(name, value):
    METRICS["source_record_counts"][name] = int(value)


def set_gold_count(name, value):
    METRICS["gold_layer_record_counts"][name] = int(value)


def set_raw_profile_section(name, value):
    METRICS["raw_profile"][name] = value


def add_issue(issue_type, records_affected, denominator, handling_action, records_in_output):
    records = int(records_affected or 0)
    if records <= 0:
        return
    denominator = int(denominator or 0)
    percentage = round((records * 100.0 / denominator), 2) if denominator else 0.0
    existing = METRICS["dq_issues"].get(issue_type)
    if existing:
        existing["records_affected"] += records
        existing["records_in_output"] += int(records_in_output or 0)
        existing["percentage_of_total"] = round(
            existing["records_affected"] * 100.0 / denominator, 2
        ) if denominator else 0.0
    else:
        METRICS["dq_issues"][issue_type] = {
            "issue_type": issue_type,
            "records_affected": records,
            "percentage_of_total": percentage,
            "handling_action": handling_action,
            "records_in_output": int(records_in_output or 0),
        }


def issue_list():
    return list(METRICS["dq_issues"].values())
