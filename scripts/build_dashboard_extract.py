"""
Build the small aggregated extract that the Streamlit dashboard reads.

The full DuckDB table holds ~3M CN8-level rows per month. The dashboard only
needs monthly totals by partner and by product chapter, which fit in a few MB
and can be committed so the app deploys on Streamlit Community Cloud without
the pipeline.

Usage:
    uv run --group dashboard python scripts/build_dashboard_extract.py
    uv run --group dashboard python scripts/build_dashboard_extract.py --months 12
    # Read the processed Parquet files instead (no DuckDB lock needed while a run is going)
    uv run --group dashboard python scripts/build_dashboard_extract.py --from-parquet

Outputs (in dashboard/extract/):
    partners.parquet   period, reporter, partner, flow, value_eur
    chapters.parquet   period, reporter, scope, flow, chapter, value_eur
    countries.csv      code, name, iso3, is_eu   (Eurostat names, ISO3 for the map)
    chapters.csv       chapter, short_name, name, section
"""

from __future__ import annotations

import argparse
import csv
import io
from pathlib import Path

import duckdb
import httpx
import pycountry
from rich.console import Console

from comext_pipeline.utils.schema import get_settings

console = Console()

OUT_DIR = Path(__file__).resolve().parent.parent / "dashboard" / "extract"

METADATA_URL = (
    "https://ec.europa.eu/eurostat/api/dissemination/files/"
    "?file=comext/COMEXT_METADATA/CLASSIFICATIONS_AND_RELATIONS/CLASSIFICATIONS/ENGLISH/{name}.txt"
)

EU27 = [
    "AT",
    "BE",
    "BG",
    "CY",
    "CZ",
    "DE",
    "DK",
    "EE",
    "ES",
    "FI",
    "FR",
    "GR",
    "HR",
    "HU",
    "IE",
    "IT",
    "LT",
    "LU",
    "LV",
    "MT",
    "NL",
    "PL",
    "PT",
    "RO",
    "SE",
    "SI",
    "SK",
]

# Chart-friendly chapter labels. The official names are long, and their first
# clause can mislead (chapter 84 starts with "Nuclear reactors" but is machinery).
SHORT_NAMES = {
    "01": "Live animals",
    "02": "Meat",
    "03": "Fish and seafood",
    "04": "Dairy, eggs, honey",
    "05": "Other animal products",
    "06": "Live plants, flowers",
    "07": "Vegetables",
    "08": "Fruit and nuts",
    "09": "Coffee, tea, spices",
    "10": "Cereals",
    "11": "Milling products",
    "12": "Oil seeds",
    "13": "Gums, resins",
    "14": "Other vegetable products",
    "15": "Fats and oils",
    "16": "Meat and fish preparations",
    "17": "Sugar",
    "18": "Cocoa",
    "19": "Cereal preparations",
    "20": "Vegetable and fruit preparations",
    "21": "Misc. food preparations",
    "22": "Beverages and spirits",
    "23": "Food waste, animal feed",
    "24": "Tobacco",
    "25": "Salt, stone, cement",
    "26": "Ores",
    "27": "Mineral fuels and oils",
    "28": "Inorganic chemicals",
    "29": "Organic chemicals",
    "30": "Pharmaceuticals",
    "31": "Fertilisers",
    "32": "Dyes, paints, inks",
    "33": "Cosmetics, essential oils",
    "34": "Soaps, detergents",
    "35": "Proteins, glues",
    "36": "Explosives",
    "37": "Photographic goods",
    "38": "Misc. chemicals",
    "39": "Plastics",
    "40": "Rubber",
    "41": "Hides and leather",
    "42": "Leather goods",
    "43": "Furs",
    "44": "Wood",
    "45": "Cork",
    "46": "Basketwork",
    "47": "Wood pulp",
    "48": "Paper",
    "49": "Printed matter",
    "50": "Silk",
    "51": "Wool",
    "52": "Cotton",
    "53": "Other plant fibres",
    "54": "Man-made filaments",
    "55": "Man-made staple fibres",
    "56": "Wadding, ropes",
    "57": "Carpets",
    "58": "Special fabrics",
    "59": "Technical textiles",
    "60": "Knitted fabrics",
    "61": "Knitted apparel",
    "62": "Woven apparel",
    "63": "Other textile articles",
    "64": "Footwear",
    "65": "Headgear",
    "66": "Umbrellas",
    "67": "Feathers, artificial flowers",
    "68": "Stone and plaster articles",
    "69": "Ceramics",
    "70": "Glass",
    "71": "Precious metals and stones",
    "72": "Iron and steel",
    "73": "Iron and steel articles",
    "74": "Copper",
    "75": "Nickel",
    "76": "Aluminium",
    "78": "Lead",
    "79": "Zinc",
    "80": "Tin",
    "81": "Other base metals",
    "82": "Tools, cutlery",
    "83": "Misc. metal articles",
    "84": "Machinery",
    "85": "Electrical equipment",
    "86": "Railway equipment",
    "87": "Vehicles",
    "88": "Aircraft and spacecraft",
    "89": "Ships and boats",
    "90": "Optical and medical instruments",
    "91": "Clocks and watches",
    "92": "Musical instruments",
    "93": "Arms and ammunition",
    "94": "Furniture, lighting",
    "95": "Toys, sports goods",
    "96": "Misc. manufactured articles",
    "97": "Art and antiques",
    "99": "Other products",
}

# The 21 HS sections, by chapter range (CN_SECTIONS.txt carries the names).
SECTION_BOUNDS = [1, 6, 15, 16, 25, 28, 39, 41, 44, 47, 50, 64, 68, 71, 72, 84, 86, 90, 93, 94, 97]


def _fetch_codelist(name: str) -> list[list[str]]:
    """Download one Eurostat tab-separated code list.

    Most lists are (code, valid_from, valid_to, label); CN_SECTIONS is (code, label).
    """
    response = httpx.get(METADATA_URL.format(name=name), timeout=60, follow_redirects=True)
    response.raise_for_status()
    rows = csv.reader(io.StringIO(response.text), delimiter="\t")
    return [[cell.strip() for cell in row] for row in rows if len(row) >= 2]


def _current(rows: list[list[str]]) -> dict[str, str]:
    """Keep the label of each code's currently valid entry."""
    return {r[0]: r[3] for r in rows if len(r) >= 4 and r[2].endswith("2500")}


def _sentence_case(text: str) -> str:
    """CN chapter names are upper case; make them readable."""
    text = text.lower()
    return text[:1].upper() + text[1:]


def _section_of(chapter: int) -> int:
    """Return the 1-based HS section number for a chapter (0 for chapters 98-99)."""
    if chapter > 97:
        return 0
    section = 0
    for i, start in enumerate(SECTION_BOUNDS, start=1):
        if chapter >= start:
            section = i
    return section


def write_countries(path: Path) -> None:
    """Partner names from Eurostat, ISO3 from pycountry for the choropleth."""
    names = _current(_fetch_codelist("PARTNERS_ISO"))
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["code", "name", "iso3", "is_eu"])
        for code, name in sorted(names.items()):
            # Eurostat-only codes (XK, XS, QP, ...) have no ISO3 and stay off the map.
            country = pycountry.countries.get(alpha_2=code)
            iso3 = country.alpha_3 if country else ""
            writer.writerow([code, name, iso3, code in EU27])


def write_chapters(path: Path) -> None:
    """Two-digit CN chapter names and the HS section each belongs to."""
    cn = _current(_fetch_codelist("CN"))
    sections = {code: label for code, label, *_ in _fetch_codelist("CN_SECTIONS")}
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["chapter", "short_name", "name", "section"])
        for code, label in sorted(cn.items()):
            if len(code) == 2 and code.isdigit() and code != "00":
                section = sections.get(f"{_section_of(int(code)):02d}", "")
                writer.writerow(
                    [
                        code,
                        SHORT_NAMES.get(code, _sentence_case(label)),
                        _sentence_case(label),
                        section.split(" (chapters")[0],
                    ]
                )


def write_aggregates(source: str, months: int) -> None:
    """Aggregate the last `months` periods of `source` and write Parquet files.

    `source` is a DuckDB database path, or a Parquet glob when it ends in .parquet.
    """
    eu_list = ", ".join(f"'{c}'" for c in EU27)
    if source.endswith(".parquet"):
        con = duckdb.connect()
        table = f"read_parquet('{source}')"
    else:
        con = duckdb.connect(source, read_only=True)
        table = "trade_flows"
    periods = [
        row[0]
        for row in con.execute(
            f"SELECT DISTINCT period FROM {table} ORDER BY period DESC LIMIT ?", [months]
        ).fetchall()
    ]
    period_list = ", ".join(f"'{p}'" for p in periods)
    # CN8 rows only: the 'TOTAL' product rows repeat the same trade as a sum.
    base = f"""
        SELECT * FROM {table}
        WHERE period IN ({period_list}) AND product_code <> 'TOTAL'
    """
    con.execute(f"""
        COPY (
            SELECT period, reporter_code AS reporter, partner_code AS partner, flow,
                   SUM(value_eur) AS value_eur
            FROM ({base})
            GROUP BY ALL ORDER BY ALL
        ) TO '{OUT_DIR / "partners.parquet"}' (FORMAT parquet, COMPRESSION zstd)
    """)
    con.execute(f"""
        COPY (
            SELECT period, reporter_code AS reporter,
                   CASE WHEN partner_code IN ({eu_list}) THEN 'intra' ELSE 'extra' END AS scope,
                   flow, LEFT(product_code, 2) AS chapter,
                   SUM(value_eur) AS value_eur
            FROM ({base})
            GROUP BY ALL ORDER BY ALL
        ) TO '{OUT_DIR / "chapters.parquet"}' (FORMAT parquet, COMPRESSION zstd)
    """)
    con.close()
    console.print(f"Aggregated {len(periods)} months: {min(periods)} → {max(periods)}")


def main() -> None:
    """Build every file of the dashboard extract."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[1])
    parser.add_argument("--months", type=int, default=12, help="Most recent months to keep")
    parser.add_argument(
        "--from-parquet",
        action="store_true",
        help="Read the processed monthly Parquet files instead of the DuckDB table",
    )
    args = parser.parse_args()

    settings = get_settings()
    source = (
        f"{Path(settings.comext_processed_dir).resolve()}/*.parquet"
        if args.from_parquet
        else settings.comext_db_path
    )
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    write_aggregates(source, args.months)
    write_countries(OUT_DIR / "countries.csv")
    write_chapters(OUT_DIR / "chapters.csv")
    for f in sorted(OUT_DIR.iterdir()):
        console.print(f"  {f.name:<18} {f.stat().st_size / 1e6:6.2f} MB")


if __name__ == "__main__":
    main()
