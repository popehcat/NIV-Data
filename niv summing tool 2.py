# -*- coding: utf-8 -*-
"""
Created on Thu Mar 26 17:03:49 2026

@author: hhpop
"""

import os
import pandas as pd
import re

# =========================
# CONFIGURATION (EDIT HERE)
# =========================
INPUT_FOLDER = r"C:\Python\NIV\Input"
OUTPUT_FOLDER = r"C:\Python\NIV\Output"
OUTPUT_FILE = "niv_issuances_summary.xlsx"

# Countries to keep (lowercase internally)
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

# Month mapping
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

# Nice output names for countries
COUNTRY_COLUMNS = [country.title() for country in TARGET_COUNTRIES]

# Fiscal year month order
FISCAL_MONTH_ORDER = [
    "October", "November", "December",
    "January", "February", "March",
    "April", "May", "June",
    "July", "August", "September"
]


# =========================
# FUNCTIONS
# =========================

def extract_month_year(filename):
    filename_lower = filename.lower()

    month_num = None
    for month_name, num in MONTHS.items():
        if month_name in filename_lower:
            month_num = num
            break

    year_match = re.search(r'20\d{2}', filename_lower)

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


def process_file(filepath):
    df = pd.read_excel(filepath)

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

    # Make sure issuances are numeric
    df["Issuances"] = pd.to_numeric(df["Issuances"], errors="coerce").fillna(0)

    # Keep only target countries
    df = df[df["Nationality"].isin(TARGET_COUNTRIES)]

    grouped = df.groupby("Nationality")["Issuances"].sum()

    return grouped.to_dict()


# =========================
# MAIN
# =========================

def main():
    results = []

    files = [
        f for f in os.listdir(INPUT_FOLDER)
        if f.lower().endswith((".xlsx", ".xls"))
        and ("niv issuances" in f.lower() or "niv issurances" in f.lower())
    ]

    # Keep only one file per calendar year/month, newest modified
    file_map = {}

    for filename in files:
        calendar_year, month_num = extract_month_year(filename)
        if calendar_year is None or month_num is None:
            continue

        full_path = os.path.join(INPUT_FOLDER, filename)
        key = (calendar_year, month_num)

        if key not in file_map:
            file_map[key] = full_path
        else:
            if os.path.getmtime(full_path) > os.path.getmtime(file_map[key]):
                file_map[key] = full_path

    # Build monthly rows
    for (calendar_year, month_num), filepath in file_map.items():
        data = process_file(filepath)

        fiscal_year = get_fiscal_year(calendar_year, month_num)
        month_name = MONTH_NAMES[month_num]

        row = {
            "Fiscal Year": fiscal_year,
            "Month": month_name
        }

        for country in TARGET_COUNTRIES:
            pretty_name = country.title()
            row[pretty_name] = data.get(country, 0)

        row["Total"] = sum(row[country] for country in COUNTRY_COLUMNS)

        results.append(row)

    if not results:
        print("No matching files found.")
        return

    df_out = pd.DataFrame(results)

    # Sort by fiscal year and fiscal month order
    df_out["month_sort"] = df_out["Month"].apply(get_fiscal_month_sort)
    df_out = df_out.sort_values(["Fiscal Year", "month_sort"]).drop(columns=["month_sort"])

    # Reorder columns
    ordered_columns = ["Fiscal Year", "Month"] + COUNTRY_COLUMNS + ["Total"]
    df_out = df_out[ordered_columns]

    # Add fiscal-year total row after each September / end of each FY group
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

    # Create output folder if needed
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    output_path = os.path.join(OUTPUT_FOLDER, OUTPUT_FILE)

    # Write to Excel
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        final_df.to_excel(writer, index=False, sheet_name="Summary")

        ws = writer.book["Summary"]

        # Auto-size columns
        for col in ws.columns:
            max_len = max(len(str(cell.value)) if cell.value is not None else 0 for cell in col)
            ws.column_dimensions[col[0].column_letter].width = max_len + 2

    print(f"Done! File saved to: {output_path}")


# =========================
# RUN
# =========================

if __name__ == "__main__":
    main()