# -*- coding: utf-8 -*-
"""
Created on Mon Mar 30 12:10:39 2026

@author: hhpop
"""

# Databricks notebook / PySpark version of your NIV summing tool

import pandas as pd
import re
from urllib.parse import unquote
from pyspark.sql import functions as F

# =========================
# CONFIGURATION
# =========================

# Replace/add URLs as needed
INPUT_URLS = [
    "https://travel.state.gov/content/dam/visas/Statistics/Non-Immigrant-Statistics/MonthlyNIVIssuances/Excel/FY2025/SEPTEMBER%202025%20-%20NIV%20Issuances%20by%20Nationality%20and%20Visa%20Class.xlsx",
    "https://travel.state.gov/content/dam/visas/Statistics/Non-Immigrant-Statistics/MonthlyNIVIssuances/Excel/FY2025/AUGUST%202025%20-%20NIV%20Issuances%20by%20Nationality%20and%20Visa%20Class.xlsx",
    "https://travel.state.gov/content/dam/visas/Statistics/Non-Immigrant-Statistics/MonthlyNIVIssuances/Excel/FY2025/JULY%202025%20-%20NIV%20Issuances%20by%20Nationality%20and%20Visa%20Class.xlsx",
    "https://travel.state.gov/content/dam/visas/Statistics/Non-Immigrant-Statistics/MonthlyNIVIssuances/Excel/FY2025/JUNE%202025%20-%20NIV%20Issuances%20by%20Nationality%20and%20Visa%20Class.xlsx",
]

# Databricks output table
OUTPUT_TABLE = "default.niv_issuances_summary"

# Countries to keep
TARGET_COUNTRIES = [
    "afghanistan",
    "burma",
    "cameroon",
    "el salvador",
    "ethiopia",
    "haiti",
    "honduras",
    "lebanon",
    "nepal",
    "nicaragua",
    "somalia",
    "south sudan",
    "sudan",
    "syria",
    "ukraine",
    "venezuela",
    "yemen"
]

MONTHS = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12
}

MONTH_NAMES = {v: k.title() for k, v in MONTHS.items()}
COUNTRY_COLUMNS = [country.title() for country in TARGET_COUNTRIES]

FISCAL_MONTH_ORDER = [
    "October", "November", "December",
    "January", "February", "March",
    "April", "May", "June",
    "July", "August", "September"
]


# =========================
# FUNCTIONS
# =========================

def extract_month_year_from_url(url: str):
    """
    Extract month and year from the URL filename, e.g.
    'SEPTEMBER 2025 - NIV Issuances by Nationality and Visa Class.xlsx'
    """
    filename = unquote(url.split("/")[-1]).lower()

    month_num = None
    for month_name, num in MONTHS.items():
        if month_name in filename:
            month_num = num
            break

    year_match = re.search(r"20\d{2}", filename)

    if month_num and year_match:
        calendar_year = int(year_match.group())
        return calendar_year, month_num

    return None, None


def get_fiscal_year(calendar_year, month_num):
    """
    October–December belong to next fiscal year.
    Example:
      Oct 2024 -> FY 2025
      Sep 2025 -> FY 2025
      Oct 2025 -> FY 2026
    """
    if month_num >= 10:
        return calendar_year + 1
    return calendar_year


def get_fiscal_month_sort(month_name):
    return FISCAL_MONTH_ORDER.index(month_name)


def process_url(url: str):
    """
    Read the Excel file directly from the website URL.
    Uses pandas because these monthly Excel files are small and this is the
    simplest/most reliable way in Databricks without extra Spark Excel packages.
    """
    df = pd.read_excel(url)

    # Column A = nationality, Column C = issuances
    col_a = df.columns[0]
    col_c = df.columns[2]

    df = df[[col_a, col_c]].copy()
    df.columns = ["Nationality", "Issuances"]

    df = df.dropna(subset=["Nationality", "Issuances"])

    df["Nationality"] = df["Nationality"].astype(str).str.strip().str.lower()

    # Fix typo if present
    df["Nationality"] = df["Nationality"].replace({
        "ethiopoa": "ethiopia"
    })

    df["Issuances"] = pd.to_numeric(df["Issuances"], errors="coerce").fillna(0)

    # Keep only target countries
    df = df[df["Nationality"].isin(TARGET_COUNTRIES)]

    grouped = df.groupby("Nationality", as_index=False)["Issuances"].sum()

    return dict(zip(grouped["Nationality"], grouped["Issuances"]))


# =========================
# MAIN
# =========================

results = []

for url in INPUT_URLS:
    calendar_year, month_num = extract_month_year_from_url(url)
    if calendar_year is None or month_num is None:
        print(f"Skipping URL (could not parse month/year): {url}")
        continue

    data = process_url(url)

    fiscal_year = get_fiscal_year(calendar_year, month_num)
    month_name = MONTH_NAMES[month_num]

    row = {
        "Fiscal Year": fiscal_year,
        "Month": month_name
    }

    for country in TARGET_COUNTRIES:
        pretty_name = country.title()
        row[pretty_name] = float(data.get(country, 0))

    row["Total"] = sum(row[country] for country in COUNTRY_COLUMNS)
    results.append(row)

if not results:
    raise ValueError("No valid monthly data was loaded from the provided URLs.")

# Build pandas output first
df_out = pd.DataFrame(results)

# Sort by fiscal year and fiscal month order
df_out["month_sort"] = df_out["Month"].apply(get_fiscal_month_sort)
df_out = df_out.sort_values(["Fiscal Year", "month_sort"]).drop(columns=["month_sort"])

# Reorder columns
ordered_columns = ["Fiscal Year", "Month"] + COUNTRY_COLUMNS + ["Total"]
df_out = df_out[ordered_columns]

# Add fiscal-year total row after each FY group
final_rows = []

for fy, group in df_out.groupby("Fiscal Year", sort=True):
    group = group.copy()
    final_rows.append(group)

    total_row = {"Fiscal Year": fy, "Month": "Total"}
    for country in COUNTRY_COLUMNS:
        total_row[country] = group[country].sum()

    total_row["Total"] = group["Total"].sum()
    final_rows.append(pd.DataFrame([total_row]))

final_df = pd.concat(final_rows, ignore_index=True)

# Convert to Spark DataFrame
spark_df = spark.createDataFrame(final_df)

# Optional: display in Databricks notebook
display(spark_df)

# Save as a managed Delta table in Databricks
spark_df.write.format("delta").mode("overwrite").saveAsTable(OUTPUT_TABLE)

print(f"Done! Table written to Databricks as: {OUTPUT_TABLE}")