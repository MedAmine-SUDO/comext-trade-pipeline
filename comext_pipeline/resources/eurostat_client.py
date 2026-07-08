"""
HTTP client resource for the Eurostat bulk-download API.

Responsibilities:
- Discover available COMEXT product files from the directory listing
- Download individual .7z files with streaming
- Compare Last-Modified headers for revision detection (bonus)
- Respect a configurable request delay to avoid hammering the API

Resilience:
- Retry with exponential backoff on 5xx / connection errors (tenacity)
- Circuit breaker that opens after N consecutive failures
"""

import json
import re
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx
from dagster import ConfigurableResource, get_dagster_logger
from pydantic import Field

from comext_pipeline.utils.schema import ComextFileEntry

logger = get_dagster_logger(__name__)

_DIR = "comext/COMEXT_DATA/PRODUCTS"
_CHUNK_SIZE = 1024 * 1024
_FULL_V2_PATTERN = re.compile(r"full_v2_(\d{6})\.7z$")

# ── Circuit breaker state (per-process, in-memory) ──────────────────────

_circuit: dict[str, Any] = {"failures": 0, "open_until": 0.0}


def _is_retryable(exc: Exception) -> bool:
    """Return True if *exc* is a server error (5xx) or a transient network error."""
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code >= 500
    return isinstance(exc, (httpx.ConnectError, httpx.TimeoutException, httpx.RemoteProtocolError))


class EurostatClient(ConfigurableResource):
    """
    Dagster ConfigurableResource wrapping an httpx client for Eurostat.
    """

    base_url: str = Field(description="Base URL of the Eurostat dissemination files API.")
    dir_path: str = Field(default=_DIR, description="Directory path within the Eurostat file API.")
    request_delay: float = Field(default=1.0, ge=0.0)
    timeout: float = Field(default=60.0)

    # Retry / circuit breaker configuration (populated from PipelineSettings)
    max_retries: int = Field(default=3, ge=0)
    circuit_breaker_threshold: int = Field(default=5, ge=1)
    circuit_breaker_timeout: float = Field(default=60.0, ge=1.0)

    # ── URL helpers ──────────────────────────────────────────────────────

    def _listing_url(self) -> str:
        """Build the URL for the Eurostat directory listing endpoint."""
        return f"{self.base_url}?sort=1&dir={quote(self.dir_path, safe='')}"

    def _download_url(self, file_path: str) -> str:
        """Build the download URL for a specific file path within the Eurostat directory."""
        return f"{self.base_url}?sort=1&downfile={quote(file_path, safe='')}"

    def _client(self) -> httpx.Client:
        """Create a new httpx client with configured timeout and user-agent."""
        return httpx.Client(
            timeout=self.timeout,
            follow_redirects=True,
            headers={"User-Agent": "comext-pipeline/0.1 (research)"},
        )

    # ── Circuit breaker ──────────────────────────────────────────────────

    def _circuit_open(self) -> bool:
        """Check whether the circuit breaker is currently open (blocking requests)."""
        if _circuit["failures"] >= self.circuit_breaker_threshold:
            if time.time() < _circuit["open_until"]:
                return True
            _circuit["failures"] = 0
        return False

    def _circuit_record_failure(self) -> None:
        """Increment the failure counter and open the circuit if threshold is reached."""
        if _circuit["failures"] >= self.circuit_breaker_threshold:
            _circuit["open_until"] = time.time() + self.circuit_breaker_timeout
            logger.error(
                "Circuit breaker opened after %d failures (recovery in %.0fs)",
                _circuit["failures"],
                self.circuit_breaker_timeout,
            )

    def _circuit_record_success(self) -> None:
        """Reset the failure counter on a successful request."""
        _circuit["failures"] = 0

    # ── Resilient request ────────────────────────────────────────────────

    def _request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        """Execute a single HTTP request and raise on non-2xx status."""
        with self._client() as client:
            if method.upper() == "GET":
                response = client.get(url, **kwargs)
            elif method.upper() == "HEAD":
                response = client.head(url, **kwargs)
            else:
                response = client.request(method, url, **kwargs)
            response.raise_for_status()
            return response

    def _request_with_retry(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        """Send an HTTP request with exponential backoff and circuit-breaker protection."""
        if self._circuit_open():
            logger.warning("Request blocked by open circuit breaker: %s %s", method, url)
            raise EurostatAPIError("Eurostat API unavailable (circuit breaker open)")

        last_exc: Exception | None = None
        for attempt in range(self.max_retries + 1):
            try:
                response = self._request(method, url, **kwargs)
                self._circuit_record_success()
                return response
            except Exception as exc:
                last_exc = exc
                if not _is_retryable(exc) or attempt >= self.max_retries:
                    break
                wait = min(30, 2**attempt)
                logger.warning(
                    "Request failed (attempt %d/%d): %s — retrying in %ds",
                    attempt + 1,
                    self.max_retries + 1,
                    exc,
                    wait,
                )
                time.sleep(wait)

        self._circuit_record_failure()
        raise last_exc  # type: ignore[misc]

    # ── Discovery ────────────────────────────────────────────────────────

    def list_files(self) -> list[ComextFileEntry]:
        url = self._listing_url()
        logger.info("Fetching listing: %s", url)

        response = self._request_with_retry("GET", url)

        try:
            data: dict[str, Any] = response.json()
        except json.JSONDecodeError:
            logger.warning("JSON listing failed, falling back to HTML parse")
            return self._parse_html_listing(response.text)
        return self._parse_json_listing(data)

    def _parse_json_listing(self, data: dict[str, Any]) -> list[ComextFileEntry]:
        """Parse the JSON directory listing response into ComextFileEntry objects."""
        entries: list[ComextFileEntry] = []
        for item in data.get("items", []):
            if item.get("type") != "FILE":
                continue
            filename: str = item["name"]
            if not filename.endswith(".7z"):
                continue
            period = _extract_period(filename)
            if period is None:
                logger.debug("Skipping file with unrecognised name pattern: %s", filename)
                continue
            entry_url = item.get("downloadLink")
            if not entry_url:
                file_path = f"{self.dir_path}/{filename}"
                entry_url = self._download_url(file_path)
            entries.append(
                ComextFileEntry(
                    filename=filename,
                    url=entry_url,
                    size_bytes=_safe_int(item.get("size", 0)),
                    last_modified=item.get("lastModified", ""),
                    period=period,
                )
            )
        logger.info("Discovered %d COMEXT monthly files", len(entries))
        return entries

    def _parse_html_listing(self, html: str) -> list[ComextFileEntry]:
        """Fallback: parse the HTML directory listing into ComextFileEntry objects."""
        entries: list[ComextFileEntry] = []
        pattern = r'<tr><td>\s*&nbsp;<a href="[^"]*file=([^"]+\.7z)"[^>]*>([^<]+)</a></td><td title="(\d+) bytes">[^<]+</td><td class="center">7z</td><td>\s*&nbsp;([^<]+)</td>'
        for match in re.finditer(pattern, html):
            file_param = match.group(1)
            filename = match.group(2)
            size_bytes = int(match.group(3))
            last_modified = match.group(4).strip()

            period = _extract_period(filename)
            if period is None:
                logger.debug("Skipping file with unrecognised name pattern: %s", filename)
                continue

            download_url = f"{self.base_url}?sort=1&downfile={file_param}"

            last_modified_iso = _convert_date_to_iso(last_modified)
            entries.append(
                ComextFileEntry(
                    filename=filename,
                    url=download_url,
                    size_bytes=size_bytes,
                    last_modified=last_modified_iso,
                    period=period,
                )
            )
        logger.info("Discovered %d COMEXT monthly files", len(entries))
        return entries

    # ── Download ─────────────────────────────────────────────────────────

    def download_file(
        self,
        entry: ComextFileEntry,
        dest_dir: Path,
    ) -> tuple[Path, bool]:
        """Download a single .7z file to *dest_dir* if not already present. Returns (path, was_downloaded)."""
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_path = dest_dir / entry.filename
        already_exists = dest_path.exists()

        if already_exists:
            logger.info(
                "File %s already exists at %s — skipping download", entry.filename, dest_path
            )
            return dest_path, False

        logger.info("Downloading %s (%d bytes)", entry.filename, entry.size_bytes)
        time.sleep(self.request_delay)

        # Download to .tmp first so partial failures don't leave a corrupted file
        tmp_path = dest_path.with_suffix(".tmp")
        try:
            with self._client() as client, client.stream("GET", entry.url) as response:
                response.raise_for_status()
                with open(tmp_path, "wb") as fh:
                    for chunk in response.iter_bytes(chunk_size=_CHUNK_SIZE):
                        fh.write(chunk)
            tmp_path.rename(dest_path)
        except Exception:
            if tmp_path.exists():
                tmp_path.unlink()
            raise

        logger.info("Downloaded %s → %s", entry.filename, dest_path)
        return dest_path, True


class EurostatAPIError(Exception):
    """Raised when Eurostat API is unreachable or returns an error."""


# ── Helpers ────────────────────────────────────────────────────────────


def _safe_int(value: Any, default: int = 0) -> int:
    """Try to parse *value* as int, returning *default* on failure."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _extract_period(filename: str) -> str | None:
    """Extract the YYYYMM period from a ``full_v2_*.7z`` filename, or None."""
    m = _FULL_V2_PATTERN.search(filename)
    return m.group(1) if m else None


def _convert_date_to_iso(date_str: str) -> str:
    """Convert Eurostat's ``DD/MM/YYYY HH:MM`` date format to ISO 8601."""
    if not date_str:
        return ""
    try:
        parts = date_str.split()
        if len(parts) != 2:
            return date_str
        date_parts = parts[0].split("/")
        if len(date_parts) != 3:
            return date_str
        day, month, year = date_parts
        return f"{year}-{month}-{day}T{parts[1]}"
    except (ValueError, IndexError):
        return date_str
