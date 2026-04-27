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


def create_delta_view(connection, gold_path, table, view_name):
    scan = table_scan(gold_path, table)
    connection.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM {scan}")


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
    create_delta_view(con, args.gold_path, "fact_transactions", "fact_transactions")
    create_delta_view(con, args.gold_path, "dim_accounts", "dim_accounts")
    create_delta_view(con, args.gold_path, "dim_customers", "dim_customers")

    row_counts = run_query(
        con,
        "Gold Table Row Counts",
        """
        SELECT 'fact_transactions' AS table_name, COUNT(*) AS row_count FROM fact_transactions
        UNION ALL
        SELECT 'dim_accounts' AS table_name, COUNT(*) AS row_count FROM dim_accounts
        UNION ALL
        SELECT 'dim_customers' AS table_name, COUNT(*) AS row_count FROM dim_customers
        ORDER BY table_name
        """,
    )

    type_rows = run_query(
        con,
        "Query 1: Transaction Volume by Type",
        """
        SELECT
            transaction_type,
            COUNT(*) AS record_count,
            SUM(amount) AS total_amount,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
        FROM fact_transactions
        GROUP BY transaction_type
        ORDER BY transaction_type
        """,
    )

    unlinked = run_query(
        con,
        "Query 2: Zero Unlinked Accounts",
        """
        SELECT COUNT(*) AS unlinked_accounts
        FROM dim_accounts AS a
        LEFT JOIN dim_customers AS c
          ON a.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
        """,
    )[0][0]

    province_rows = run_query(
        con,
        "Query 3: Province Distribution",
        """
        SELECT
            c.province,
            COUNT(DISTINCT a.account_id) AS account_count
        FROM dim_accounts AS a
        JOIN dim_customers AS c
          ON a.customer_id = c.customer_id
        GROUP BY c.province
        ORDER BY c.province
        """,
    )

    empty_tables = [table_name for table_name, row_count in row_counts if row_count == 0]
    if empty_tables:
        print(f"\nFAIL: empty Gold table(s): {', '.join(empty_tables)}", file=sys.stderr)
        return 1
    if len(type_rows) != 4:
        print(f"\nFAIL: expected 4 transaction type rows, got {len(type_rows)}", file=sys.stderr)
        return 1
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
