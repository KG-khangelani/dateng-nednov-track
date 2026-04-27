import argparse
import json
from pathlib import Path
import sys

import duckdb


def _sql_quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def active_delta_files(table_path):
    table_path = Path(table_path)
    log_dir = table_path / "_delta_log"
    active = set()
    for log_file in sorted(log_dir.glob("*.json")):
        with open(log_file, "r", encoding="utf-8") as handle:
            for line in handle:
                action = json.loads(line)
                if "add" in action:
                    active.add(action["add"]["path"])
                elif "remove" in action:
                    active.discard(action["remove"]["path"])
    if not active:
        raise FileNotFoundError(f"No active Delta files found for {table_path}")
    return [table_path / path for path in sorted(active)]


def table_scan(gold_path, table):
    table_path = Path(gold_path) / table
    files = ", ".join(_sql_quote(path) for path in active_delta_files(table_path))
    return f"parquet_scan([{files}])"


def run_query(connection, title, sql):
    print(f"\n## {title}")
    result = connection.execute(sql).fetchall()
    columns = [description[0] for description in connection.description]
    print("\t".join(columns))
    for row in result:
        print("\t".join("" if value is None else str(value) for value in row))
    return result


def main():
    parser = argparse.ArgumentParser(description="Validate Gold Delta outputs with DuckDB Python.")
    parser.add_argument("--gold-path", default="/data/output/gold")
    args = parser.parse_args()

    con = duckdb.connect()
    fact = table_scan(args.gold_path, "fact_transactions")
    accounts = table_scan(args.gold_path, "dim_accounts")
    customers = table_scan(args.gold_path, "dim_customers")

    run_query(
        con,
        "Query 1: Transaction Volume by Type",
        f"""
        SELECT
            transaction_type,
            COUNT(*) AS record_count,
            SUM(amount) AS total_amount,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
        FROM {fact}
        GROUP BY transaction_type
        ORDER BY transaction_type
        """,
    )

    unlinked = run_query(
        con,
        "Query 2: Zero Unlinked Accounts",
        f"""
        SELECT COUNT(*) AS unlinked_accounts
        FROM {accounts} AS a
        LEFT JOIN {customers} AS c
          ON a.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
        """,
    )[0][0]

    province_rows = run_query(
        con,
        "Query 3: Province Distribution",
        f"""
        SELECT
            c.province,
            COUNT(DISTINCT a.account_id) AS account_count
        FROM {accounts} AS a
        JOIN {customers} AS c
          ON a.customer_id = c.customer_id
        GROUP BY c.province
        ORDER BY c.province
        """,
    )

    if unlinked != 0:
        print(f"\nFAIL: expected 0 unlinked accounts, got {unlinked}", file=sys.stderr)
        return 1
    if len(province_rows) != 9:
        print(f"\nFAIL: expected 9 province rows, got {len(province_rows)}", file=sys.stderr)
        return 1

    print("\nPASS: Gold validation queries executed successfully using DuckDB Python parquet_scan.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
