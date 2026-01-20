"""
Parse LA28 schedule from PDF or JSON into the database.

PDF parsing is the primary method, copied from read.py.
"""
import json
from io import BytesIO
from pathlib import Path

import requests
import pdfplumber
DEFAULT_URL = "https://la28.org/content/dam/latwentyeight/competition-schedule-imagery/uploaded-nov-12-2025/LA28OlympicGamesCompetitionScheduleByEventV2Final.pdf"


def read_pdf_tables(url: str) -> list[list[str]]:
    """Download PDF and extract tables from all pages."""
    pdf_bytes = requests.get(url).content
    all_tables = []
    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            table = page.extract_table()
            if table:
                all_tables.append(table)
    return all_tables


def read_pdf_rows(url: str) -> list[dict[str, str]]:
    """Extract raw session rows from PDF tables."""
    tables = read_pdf_tables(url)
    header = tables[0][1]
    sessions = []
    for table in tables:
        for row in table[2:]:
            sessions.append({header[i]: row[i] for i in range(len(header))})
    return sessions


def pdf_fo_json(url: str, dst: str | Path):
    """
    Load schedule directly from LA28 PDF into database.

    This is the primary loading method.
    """
    rows = read_pdf_rows(url)
    for row in rows:
        if row["Sport"] == "Archery" and row["Venue"] == "N/A":
            row["Venue"] = "Carson Stadium"
    with Path(dst).open("w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    pdf_fo_json(DEFAULT_URL, "resources/la28-schedule.json")
