#!/usr/bin/env python3
"""
Seed IFRS 9 glossary terms, metrics, and dictionary entries via the QueryWise API.

Usage:
    python backend/scripts/seed_ifrs9_metadata.py [--base-url http://localhost:8000]

Prerequisites:
    1. docker compose up -d
    2. Connection created to sample-db and introspected via UI/API
"""

import argparse
import sys

import httpx

API_PREFIX = "/api/v1"


# ---------------------------------------------------------------------------
# Glossary terms
# ---------------------------------------------------------------------------
GLOSSARY_TERMS = [
    {
        "term": "EAD",
        "definition": "Exposure at Default - the total amount a bank is exposed to at the time of a borrower's default. This is the gross carrying amount for on-balance sheet items.",
        "sql_expression": "exposures.ead",
        "related_tables": ["exposures"],
        "related_columns": ["exposures.ead"],
        "examples": ["SELECT SUM(ead) FROM exposures WHERE reporting_date = '2024-12-31'"],
    },
    {
        "term": "PD",
        "definition": "Probability of Default - the likelihood that a borrower will default on their obligations within a given time horizon (12 months for Stage 1, lifetime for Stage 2/3).",
        "sql_expression": "ecl_provisions.pd",
        "related_tables": ["ecl_provisions"],
        "related_columns": ["ecl_provisions.pd"],
        "examples": ["SELECT AVG(pd) FROM ecl_provisions WHERE stage = 1"],
    },
    {
        "term": "LGD",
        "definition": "Loss Given Default - the percentage of exposure that is lost if a borrower defaults, after accounting for recoveries and collateral.",
        "sql_expression": "ecl_provisions.lgd",
        "related_tables": ["ecl_provisions", "collateral"],
        "related_columns": ["ecl_provisions.lgd"],
        "examples": ["SELECT AVG(lgd) FROM ecl_provisions WHERE stage = 3"],
    },
    {
        "term": "ECL",
        "definition": "Expected Credit Loss - the probability-weighted estimate of credit losses. Calculated as PD x LGD x EAD. Under IFRS 9, Stage 1 uses 12-month ECL while Stage 2 and 3 use lifetime ECL.",
        "sql_expression": "ecl_provisions.ecl_lifetime",
        "related_tables": ["ecl_provisions", "exposures"],
        "related_columns": ["ecl_provisions.ecl_12m", "ecl_provisions.ecl_lifetime"],
        "examples": [
            "SELECT SUM(ecl_lifetime) FROM ecl_provisions",
            "SELECT stage, SUM(ecl_lifetime) FROM ecl_provisions GROUP BY stage",
        ],
    },
    {
        "term": "Stage 1",
        "definition": "Performing loans with no significant increase in credit risk since origination. Only 12-month ECL is recognised as a provision.",
        "sql_expression": "exposures.stage = 1",
        "related_tables": ["exposures", "ecl_provisions"],
        "related_columns": ["exposures.stage"],
        "examples": ["SELECT * FROM exposures WHERE stage = 1"],
    },
    {
        "term": "Stage 2",
        "definition": "Loans with a Significant Increase in Credit Risk (SICR) since origination but not yet credit-impaired. Lifetime ECL is recognised.",
        "sql_expression": "exposures.stage = 2",
        "related_tables": ["exposures", "ecl_provisions", "staging_history"],
        "related_columns": ["exposures.stage"],
        "examples": ["SELECT * FROM exposures WHERE stage = 2"],
    },
    {
        "term": "Stage 3",
        "definition": "Credit-impaired (defaulted) loans. Lifetime ECL is recognised and interest revenue is calculated on the net carrying amount.",
        "sql_expression": "exposures.stage = 3",
        "related_tables": ["exposures", "ecl_provisions"],
        "related_columns": ["exposures.stage"],
        "examples": ["SELECT * FROM exposures WHERE stage = 3"],
    },
    {
        "term": "SICR",
        "definition": "Significant Increase in Credit Risk - the trigger for moving a loan from Stage 1 to Stage 2 under IFRS 9. Assessed using quantitative and qualitative criteria.",
        "sql_expression": "staging_history.to_stage = 2",
        "related_tables": ["staging_history", "exposures"],
        "related_columns": ["staging_history.to_stage", "staging_history.reason"],
        "examples": [
            "SELECT * FROM staging_history WHERE to_stage = 2 AND reason = 'downgrade'"
        ],
    },
    {
        "term": "Coverage Ratio",
        "definition": "The ratio of ECL provisions to total exposure (EAD). Indicates the level of provisioning relative to the outstanding loan book. Higher ratios indicate more conservative provisioning.",
        "sql_expression": "SUM(ecl_provisions.ecl_lifetime) / SUM(exposures.ead)",
        "related_tables": ["ecl_provisions", "exposures"],
        "related_columns": ["ecl_provisions.ecl_lifetime", "exposures.ead"],
        "examples": [],
    },
    {
        "term": "NPL",
        "definition": "Non-Performing Loan - loans classified as Stage 3 (credit-impaired) under IFRS 9. These are loans where the borrower has defaulted or is unlikely to pay.",
        "sql_expression": "exposures.stage = 3",
        "related_tables": ["exposures", "counterparties"],
        "related_columns": ["exposures.stage", "counterparties.is_defaulted"],
        "examples": [
            "SELECT COUNT(*) FROM exposures WHERE stage = 3",
            "SELECT SUM(ead) FROM exposures WHERE stage = 3",
        ],
    },
]

# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------
METRICS = [
    {
        "metric_name": "total_ecl",
        "display_name": "Total ECL",
        "description": "Total Expected Credit Loss across the entire portfolio",
        "sql_expression": "SUM(ecl_provisions.ecl_lifetime)",
        "aggregation_type": "sum",
        "related_tables": ["ecl_provisions"],
        "dimensions": ["stage", "facility_type", "segment", "currency"],
    },
    {
        "metric_name": "total_ead",
        "display_name": "Total EAD",
        "description": "Total Exposure at Default across the entire portfolio",
        "sql_expression": "SUM(exposures.ead)",
        "aggregation_type": "sum",
        "related_tables": ["exposures"],
        "dimensions": ["stage", "facility_type", "segment", "currency"],
    },
    {
        "metric_name": "coverage_ratio",
        "display_name": "Coverage Ratio",
        "description": "ECL as a percentage of total EAD — indicates provisioning adequacy",
        "sql_expression": "SUM(ecl_provisions.ecl_lifetime) / NULLIF(SUM(exposures.ead), 0)",
        "aggregation_type": "ratio",
        "related_tables": ["ecl_provisions", "exposures"],
        "dimensions": ["stage", "facility_type", "segment"],
    },
    {
        "metric_name": "stage1_exposure",
        "display_name": "Stage 1 Exposure",
        "description": "Total EAD for performing loans (Stage 1 — 12-month ECL)",
        "sql_expression": "SUM(exposures.ead) FILTER (WHERE exposures.stage = 1)",
        "aggregation_type": "sum",
        "related_tables": ["exposures"],
        "dimensions": ["facility_type", "segment", "currency"],
        "filters": {"stage": 1},
    },
    {
        "metric_name": "stage2_exposure",
        "display_name": "Stage 2 Exposure",
        "description": "Total EAD for SICR loans (Stage 2 — lifetime ECL)",
        "sql_expression": "SUM(exposures.ead) FILTER (WHERE exposures.stage = 2)",
        "aggregation_type": "sum",
        "related_tables": ["exposures"],
        "dimensions": ["facility_type", "segment", "currency"],
        "filters": {"stage": 2},
    },
    {
        "metric_name": "stage3_exposure",
        "display_name": "Stage 3 Exposure",
        "description": "Total EAD for credit-impaired loans (Stage 3 — lifetime ECL)",
        "sql_expression": "SUM(exposures.ead) FILTER (WHERE exposures.stage = 3)",
        "aggregation_type": "sum",
        "related_tables": ["exposures"],
        "dimensions": ["facility_type", "segment", "currency"],
        "filters": {"stage": 3},
    },
    {
        "metric_name": "average_pd",
        "display_name": "Average PD",
        "description": "Weighted average Probability of Default across the portfolio",
        "sql_expression": "AVG(ecl_provisions.pd)",
        "aggregation_type": "avg",
        "related_tables": ["ecl_provisions"],
        "dimensions": ["stage", "facility_type", "segment"],
    },
    {
        "metric_name": "npl_ratio",
        "display_name": "NPL Ratio",
        "description": "Non-Performing Loan ratio — Stage 3 EAD as a percentage of total EAD",
        "sql_expression": "SUM(exposures.ead) FILTER (WHERE exposures.stage = 3) / NULLIF(SUM(exposures.ead), 0)",
        "aggregation_type": "ratio",
        "related_tables": ["exposures"],
        "dimensions": ["facility_type", "segment", "currency"],
    },
]

# ---------------------------------------------------------------------------
# Dictionary entries — keyed by (table_name, column_name)
# ---------------------------------------------------------------------------
DICTIONARY_ENTRIES: dict[tuple[str, str], list[dict]] = {
    ("exposures", "stage"): [
        {"raw_value": "1", "display_value": "Stage 1 - Performing", "description": "No significant increase in credit risk; 12-month ECL", "sort_order": 1},
        {"raw_value": "2", "display_value": "Stage 2 - SICR", "description": "Significant increase in credit risk; lifetime ECL", "sort_order": 2},
        {"raw_value": "3", "display_value": "Stage 3 - Credit-Impaired", "description": "Credit-impaired / defaulted; lifetime ECL", "sort_order": 3},
    ],
    ("facilities", "facility_type"): [
        {"raw_value": "mortgage", "display_value": "Mortgage Loan", "description": "Residential or commercial mortgage", "sort_order": 1},
        {"raw_value": "corporate_loan", "display_value": "Corporate Loan", "description": "Term loan to a corporate entity", "sort_order": 2},
        {"raw_value": "consumer_loan", "display_value": "Consumer Loan", "description": "Unsecured personal loan", "sort_order": 3},
        {"raw_value": "credit_card", "display_value": "Credit Card", "description": "Revolving credit card facility", "sort_order": 4},
        {"raw_value": "overdraft", "display_value": "Overdraft", "description": "Overdraft facility on current account", "sort_order": 5},
    ],
    ("counterparties", "segment"): [
        {"raw_value": "retail", "display_value": "Retail Banking", "description": "Individual consumers and households", "sort_order": 1},
        {"raw_value": "corporate", "display_value": "Corporate Banking", "description": "Large corporate entities", "sort_order": 2},
        {"raw_value": "sme", "display_value": "SME Banking", "description": "Small and medium enterprises", "sort_order": 3},
    ],
    ("collateral", "collateral_type"): [
        {"raw_value": "property", "display_value": "Real Estate Property", "description": "Residential or commercial real estate", "sort_order": 1},
        {"raw_value": "cash", "display_value": "Cash Deposit", "description": "Cash held as security", "sort_order": 2},
        {"raw_value": "guarantee", "display_value": "Bank Guarantee", "description": "Third-party bank guarantee", "sort_order": 3},
        {"raw_value": "securities", "display_value": "Securities", "description": "Bonds, equities, or other financial instruments", "sort_order": 4},
    ],
    ("staging_history", "reason"): [
        {"raw_value": "origination", "display_value": "New Origination", "description": "Initial recognition at Stage 1", "sort_order": 1},
        {"raw_value": "upgrade", "display_value": "Credit Improvement", "description": "Upgrade due to improved credit quality", "sort_order": 2},
        {"raw_value": "downgrade", "display_value": "Credit Deterioration", "description": "Downgrade due to SICR or default triggers", "sort_order": 3},
        {"raw_value": "cure", "display_value": "Return to Performing", "description": "Recovery from impaired status", "sort_order": 4},
        {"raw_value": "default", "display_value": "Default Event", "description": "Borrower entered default", "sort_order": 5},
    ],
    ("counterparties", "credit_rating"): [
        {"raw_value": "AAA", "display_value": "AAA - Prime", "description": "Highest credit quality; minimal default risk", "sort_order": 1},
        {"raw_value": "AA", "display_value": "AA - High Grade", "description": "Very high credit quality; very low default risk", "sort_order": 2},
        {"raw_value": "A", "display_value": "A - Upper Medium", "description": "High credit quality; low default risk", "sort_order": 3},
        {"raw_value": "BBB", "display_value": "BBB - Lower Medium", "description": "Good credit quality; moderate default risk (investment grade floor)", "sort_order": 4},
        {"raw_value": "BB", "display_value": "BB - Speculative", "description": "Speculative; substantial credit risk (sub-investment grade)", "sort_order": 5},
        {"raw_value": "B", "display_value": "B - Highly Speculative", "description": "Highly speculative; high default risk", "sort_order": 6},
        {"raw_value": "CCC", "display_value": "CCC - Substantial Risk", "description": "Very high credit risk; near default", "sort_order": 7},
    ],
    ("counterparties", "is_defaulted"): [
        {"raw_value": "true", "display_value": "Defaulted", "description": "Counterparty has defaulted on obligations", "sort_order": 1},
        {"raw_value": "false", "display_value": "Performing", "description": "Counterparty is current on obligations", "sort_order": 2},
    ],
    ("facilities", "currency"): [
        {"raw_value": "EUR", "display_value": "Euro", "description": "European single currency", "sort_order": 1},
        {"raw_value": "USD", "display_value": "US Dollar", "description": "United States dollar", "sort_order": 2},
        {"raw_value": "GBP", "display_value": "British Pound", "description": "British pound sterling", "sort_order": 3},
    ],
    ("facilities", "is_revolving"): [
        {"raw_value": "true", "display_value": "Revolving", "description": "Revolving credit facility (e.g. credit card, overdraft) — can be drawn and repaid repeatedly", "sort_order": 1},
        {"raw_value": "false", "display_value": "Term / Amortising", "description": "Term facility with scheduled repayment (e.g. mortgage, term loan)", "sort_order": 2},
    ],
    ("ecl_provisions", "stage"): [
        {"raw_value": "1", "display_value": "Stage 1 - Performing", "description": "No significant increase in credit risk; 12-month ECL", "sort_order": 1},
        {"raw_value": "2", "display_value": "Stage 2 - SICR", "description": "Significant increase in credit risk; lifetime ECL", "sort_order": 2},
        {"raw_value": "3", "display_value": "Stage 3 - Credit-Impaired", "description": "Credit-impaired / defaulted; lifetime ECL", "sort_order": 3},
    ],
    ("staging_history", "from_stage"): [
        {"raw_value": "1", "display_value": "Stage 1 - Performing", "description": "No significant increase in credit risk", "sort_order": 1},
        {"raw_value": "2", "display_value": "Stage 2 - SICR", "description": "Significant increase in credit risk", "sort_order": 2},
        {"raw_value": "3", "display_value": "Stage 3 - Credit-Impaired", "description": "Credit-impaired / defaulted", "sort_order": 3},
    ],
    ("staging_history", "to_stage"): [
        {"raw_value": "1", "display_value": "Stage 1 - Performing", "description": "No significant increase in credit risk", "sort_order": 1},
        {"raw_value": "2", "display_value": "Stage 2 - SICR", "description": "Significant increase in credit risk", "sort_order": 2},
        {"raw_value": "3", "display_value": "Stage 3 - Credit-Impaired", "description": "Credit-impaired / defaulted", "sort_order": 3},
    ],
}


def get_connection_id(client: httpx.Client) -> str:
    """Find the first active connection."""
    resp = client.get(f"{API_PREFIX}/connections")
    resp.raise_for_status()
    connections = resp.json()
    if not connections:
        print("ERROR: No connections found. Create a connection and introspect first.")
        sys.exit(1)
    conn = connections[0]
    print(f"  Using connection: {conn['name']} ({conn['id']})")
    return conn["id"]


def get_tables(client: httpx.Client, connection_id: str) -> list[dict]:
    """Get all tables for a connection."""
    resp = client.get(f"{API_PREFIX}/connections/{connection_id}/tables")
    resp.raise_for_status()
    return resp.json()


def get_table_detail(client: httpx.Client, table_id: str) -> dict:
    """Get table detail with columns."""
    resp = client.get(f"{API_PREFIX}/tables/{table_id}")
    resp.raise_for_status()
    return resp.json()


def seed_glossary(client: httpx.Client, connection_id: str) -> None:
    """Seed glossary terms."""
    print("\n--- Seeding Glossary Terms ---")
    for term_data in GLOSSARY_TERMS:
        resp = client.post(
            f"{API_PREFIX}/connections/{connection_id}/glossary",
            json=term_data,
        )
        if resp.status_code == 201:
            print(f"  + {term_data['term']}")
        else:
            print(f"  ! {term_data['term']} — {resp.status_code}: {resp.text}")


def seed_metrics(client: httpx.Client, connection_id: str) -> None:
    """Seed metric definitions."""
    print("\n--- Seeding Metrics ---")
    for metric_data in METRICS:
        resp = client.post(
            f"{API_PREFIX}/connections/{connection_id}/metrics",
            json=metric_data,
        )
        if resp.status_code == 201:
            print(f"  + {metric_data['display_name']}")
        else:
            print(f"  ! {metric_data['display_name']} — {resp.status_code}: {resp.text}")


def seed_dictionary(client: httpx.Client, connection_id: str) -> None:
    """Seed dictionary entries by looking up column IDs."""
    print("\n--- Seeding Dictionary Entries ---")

    # Build column lookup: (table_name, column_name) -> column_id
    tables = get_tables(client, connection_id)
    column_map: dict[tuple[str, str], str] = {}

    for table_summary in tables:
        detail = get_table_detail(client, table_summary["id"])
        for col in detail["columns"]:
            column_map[(detail["table_name"], col["column_name"])] = col["id"]

    for (table_name, column_name), entries in DICTIONARY_ENTRIES.items():
        col_id = column_map.get((table_name, column_name))
        if not col_id:
            print(f"  ! Column {table_name}.{column_name} not found — skipping")
            continue

        print(f"  {table_name}.{column_name}:")
        for entry_data in entries:
            resp = client.post(
                f"{API_PREFIX}/columns/{col_id}/dictionary",
                json=entry_data,
            )
            if resp.status_code == 201:
                print(f"    + {entry_data['raw_value']} -> {entry_data['display_value']}")
            else:
                print(f"    ! {entry_data['raw_value']} — {resp.status_code}: {resp.text}")


def main():
    parser = argparse.ArgumentParser(description="Seed IFRS 9 metadata into QueryWise")
    parser.add_argument(
        "--base-url",
        default="http://localhost:8000",
        help="QueryWise backend URL (default: http://localhost:8000)",
    )
    args = parser.parse_args()

    base_url = args.base_url.rstrip("/")
    print(f"Seeding IFRS 9 metadata into {base_url}")

    with httpx.Client(base_url=base_url, timeout=30) as client:
        # Verify backend is reachable
        try:
            health = client.get(f"{API_PREFIX}/health")
            health.raise_for_status()
        except httpx.ConnectError:
            print(f"ERROR: Cannot connect to {base_url}. Is the backend running?")
            sys.exit(1)

        connection_id = get_connection_id(client)
        seed_glossary(client, connection_id)
        seed_metrics(client, connection_id)
        seed_dictionary(client, connection_id)

    print("\nDone! Verify in the UI:")
    print("  - Glossary: http://localhost:5173/glossary")
    print("  - Metrics:  http://localhost:5173/metrics")
    print("  - Dictionary: click any table, then the Dictionary tab")


if __name__ == "__main__":
    main()
