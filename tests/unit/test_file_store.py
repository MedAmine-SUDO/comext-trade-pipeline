"""
Unit tests for resources/file_store.py: path helpers, manifest persistence, and
7z archive extraction.
"""

import json
from pathlib import Path

import pytest

from comext_pipeline.resources.file_store import FileStoreResource


@pytest.fixture
def store(tmp_path: Path) -> FileStoreResource:
    return FileStoreResource(
        raw_dir=str(tmp_path / "raw"),
        processed_dir=str(tmp_path / "processed"),
    )


class TestPaths:
    def test_raw_period_dir_creates_and_returns(self, store: FileStoreResource, tmp_path: Path):
        d = store.raw_period_dir("202401")
        assert d.exists()
        assert d.name == "202401"
        assert d.parent.name == "raw"

    def test_processed_path(self, store: FileStoreResource, tmp_path: Path):
        p = store.processed_path("202401")
        assert p.name == "202401.parquet"
        assert p.parent.name == "processed"
        assert p.parent.exists()


class TestManifest:
    def test_load_empty_manifest(self, store: FileStoreResource):
        assert store.load_manifest() == {}

    def test_save_and_load_manifest(self, store: FileStoreResource):
        manifest = {"202401": {"filename": "test.7z", "url": "http://example.com"}}
        store.save_manifest(manifest)
        loaded = store.load_manifest()
        assert loaded == manifest

    def test_update_manifest_entry(self, store: FileStoreResource):
        store.update_manifest_entry(
            period="202401",
            filename="test.7z",
            url="http://example.com",
            last_modified="2026-01-06T13:54:24",
            size_bytes=12345,
        )
        manifest = store.load_manifest()
        assert manifest["202401"]["filename"] == "test.7z"
        assert manifest["202401"]["last_modified"] == "2026-01-06T13:54:24"

    def test_update_manifest_entry_twice_overwrites(self, store: FileStoreResource):
        store.update_manifest_entry(
            period="202401",
            filename="old.7z",
            url="http://old.com",
            last_modified="2020-01-01",
            size_bytes=100,
        )
        store.update_manifest_entry(
            period="202401",
            filename="new.7z",
            url="http://new.com",
            last_modified="2026-01-06",
            size_bytes=200,
        )
        manifest = store.load_manifest()
        assert manifest["202401"]["filename"] == "new.7z"

    def test_get_manifest_last_modified_returns_none_when_missing(
        self, store: FileStoreResource
    ):
        assert store.get_manifest_last_modified("999999") is None

    def test_get_manifest_last_modified_returns_value(self, store: FileStoreResource):
        store.update_manifest_entry(
            period="202401",
            filename="test.7z",
            url="http://example.com",
            last_modified="2026-01-06T13:54:24",
            size_bytes=12345,
        )
        assert store.get_manifest_last_modified("202401") == "2026-01-06T13:54:24"

    def test_manifest_file_is_valid_json(self, store: FileStoreResource):
        store.save_manifest({"a": {"b": 1}})
        mp = Path(store.raw_dir) / "manifest.json"
        assert mp.exists()
        with open(mp) as f:
            assert json.load(f) == {"a": {"b": 1}}


class TestExtraction:
    def test_extract_archive_with_py7zr(self, store: FileStoreResource, tmp_path: Path):
        import py7zr

        archive = tmp_path / "test.7z"
        content_file = tmp_path / "data.txt"
        content_file.write_text("hello world")

        with py7zr.SevenZipFile(archive, "w") as z:
            z.write(content_file, "data.txt")

        dest = tmp_path / "out"
        extracted = store.extract_archive(archive, dest)
        assert len(extracted) == 1
        assert (dest / "data.txt").exists()
        assert (dest / "data.txt").read_text() == "hello world"

    def test_extract_cleanup_on_py7zr_failure(self, store: FileStoreResource, tmp_path: Path, mocker):
        import py7zr

        archive = tmp_path / "test.7z"
        content_file = tmp_path / "data.txt"
        content_file.write_text("hello world")

        with py7zr.SevenZipFile(archive, "w") as z:
            z.write(content_file, "data.txt")

        # Mock py7zr to raise mid-extraction, and make system 7z unavailable
        mock_7z = mocker.patch("py7zr.SevenZipFile")
        mock_7z.return_value.__enter__.return_value.extractall.side_effect = RuntimeError("extraction failed")
        mocker.patch("comext_pipeline.resources.file_store.shutil.which", return_value=None)

        dest = tmp_path / "out"
        # System 7z not available → should raise
        with pytest.raises(RuntimeError, match="py7zr failed|system"):
            store.extract_archive(archive, dest)

        # Temp dir should be cleaned up
        assert not (dest / ".tmp_extract").exists()
        # Dest dir should be empty (no partial files)
        assert not any(dest.iterdir()) if dest.exists() else True
