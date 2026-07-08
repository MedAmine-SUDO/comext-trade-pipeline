"""
Local file-system abstraction for raw downloads and processed Parquet outputs.

Provides:
- Deterministic path resolution for every period
- 7z extraction via py7zr (pure Python) with fallback to system `7z`
- Manifest persistence (JSON) for tracking downloaded files between runs
- Retention policy: purge raw directories older than N months
"""

import json
import shutil
import subprocess
from datetime import date
from pathlib import Path
from typing import Any, cast

import py7zr
from dagster import ConfigurableResource, get_dagster_logger
from pydantic import Field

logger = get_dagster_logger(__name__)

_MANIFEST_FILENAME = "manifest.json"


class FileStoreResource(ConfigurableResource):
    """Local file-system abstraction for raw downloads and processed Parquet outputs."""

    raw_dir: str = Field(default="./data/raw")
    processed_dir: str = Field(default="./data/processed")

    # ── Path helpers ─────────────────────────────────────────────────────

    def raw_period_dir(self, period: str) -> Path:
        """Return (and create) the directory for a given period's raw files."""
        p = Path(self.raw_dir) / period
        p.mkdir(parents=True, exist_ok=True)
        return p

    def processed_path(self, period: str) -> Path:
        """Return the Parquet output path for a given period, creating the processed dir if needed."""
        p = Path(self.processed_dir)
        p.mkdir(parents=True, exist_ok=True)
        return p / f"{period}.parquet"

    # ── Extraction ───────────────────────────────────────────────────────

    def extract_archive(self, archive_path: Path, dest_dir: Path) -> list[Path]:
        """
        Extract a .7z archive to `dest_dir`.

        Uses py7zr (pure Python) first; falls back to system ``7z`` CLI if
        py7zr fails and ``7z`` is on PATH. Extraction is done via a
        temporary directory so partial failures never leave corrupt files.
        """
        dest_dir.mkdir(parents=True, exist_ok=True)
        # Extract to temp dir so partial failures don't leave corrupt files
        tmp_dir = dest_dir / ".tmp_extract"
        tmp_dir.mkdir(parents=True, exist_ok=True)
        try:
            self._extract_py7zr(archive_path, tmp_dir)
        except Exception as exc:
            shutil.rmtree(tmp_dir, ignore_errors=True)
            if not shutil.which("7z"):
                raise RuntimeError(
                    f"py7zr failed ({exc}) and system `7z` CLI is not installed. "
                    f"Install it: apt-get install p7zip-full  |  brew install p7zip"
                ) from exc
            logger.warning(
                "py7zr extraction failed for %s (%s); trying system 7z", archive_path.name, exc
            )
            tmp_dir.mkdir(parents=True, exist_ok=True)
            self._extract_system_7z(archive_path, tmp_dir)

        # Move extracted files into dest_dir, then remove temp dir
        extracted: list[Path] = []
        for p in tmp_dir.iterdir():
            dest = dest_dir / p.name
            shutil.move(str(p), str(dest))
            extracted.append(dest)
        shutil.rmtree(tmp_dir, ignore_errors=True)
        logger.info("Extracted %d file(s) from %s", len(extracted), archive_path.name)
        return extracted

    def _extract_py7zr(self, archive_path: Path, dest_dir: Path) -> None:
        with py7zr.SevenZipFile(archive_path, mode="r") as z:
            z.extractall(path=dest_dir)
        logger.debug("Extracted (py7zr) to %s", dest_dir)

    def _extract_system_7z(self, archive_path: Path, dest_dir: Path) -> None:
        subprocess.run(
            ["7z", "e", str(archive_path), f"-o{dest_dir}", "-y"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True,
        )
        logger.debug("Extracted (system 7z) to %s", dest_dir)

    # ── Manifest ─────────────────────────────────────────────────────────

    def _manifest_path(self) -> Path:
        p = Path(self.raw_dir)
        p.mkdir(parents=True, exist_ok=True)
        return p / _MANIFEST_FILENAME

    def load_manifest(self) -> dict[str, dict[str, Any]]:
        """Load the persisted manifest JSON. Returns an empty dict if no manifest exists."""
        mp = self._manifest_path()
        if not mp.exists():
            return {}
        with open(mp) as f:
            return cast(dict[str, dict[str, Any]], json.load(f))

    def save_manifest(self, manifest: dict[str, dict[str, Any]]) -> None:
        """Persist the full manifest dict to disk as JSON (atomic write via .tmp)."""
        mp = self._manifest_path()
        tmp = mp.with_suffix(".tmp")
        with open(tmp, "w") as f:
            json.dump(manifest, f, indent=2, sort_keys=True)
        tmp.rename(mp)
        logger.debug("Manifest saved: %d entries", len(manifest))

    def update_manifest_entry(
        self,
        period: str,
        filename: str,
        url: str,
        last_modified: str,
        size_bytes: int,
    ) -> None:
        """Upsert a single period entry into the manifest and persist."""
        manifest = self.load_manifest()
        manifest[period] = {
            "filename": filename,
            "url": url,
            "last_modified": last_modified,
            "size_bytes": size_bytes,
        }
        self.save_manifest(manifest)

    def get_manifest_last_modified(self, period: str) -> str | None:
        """Return the stored Last-Modified value for *period*, or None."""
        manifest = self.load_manifest()
        return manifest.get(period, {}).get("last_modified")

    # ── Retention ────────────────────────────────────────────────────────

    def purge_raw_periods(self, older_than_months: int) -> list[str]:
        """
        Delete raw directories for periods older than `older_than_months`.

        Returns a list of removed period identifiers.
        """
        if older_than_months <= 0:
            logger.debug("Retention disabled (older_than_months=%d)", older_than_months)
            return []

        cutoff = date.today()
        # Subtract months by computing a date 1st-of-month N months back
        total_months = cutoff.year * 12 + cutoff.month - 1 - older_than_months
        cutoff = date(total_months // 12, total_months % 12 + 1, 1)

        raw_path = Path(self.raw_dir)
        if not raw_path.exists():
            return []

        removed: list[str] = []
        for entry in sorted(raw_path.iterdir()):
            if not entry.is_dir():
                continue
            period = entry.name
            if len(period) != 6 or not period.isdigit():
                continue
            try:
                period_year = int(period[:4])
                period_month = int(period[4:])
            except ValueError:
                continue
            period_date = date(period_year, period_month, 1)
            if period_date < cutoff:
                shutil.rmtree(entry, ignore_errors=True)
                removed.append(period)
                logger.info("Purged raw directory: %s", period)

        if removed:
            logger.info("Retention purge complete: %d period(s) removed", len(removed))
        else:
            logger.debug("Retention: no periods older than %d month(s) found", older_than_months)
        return removed
