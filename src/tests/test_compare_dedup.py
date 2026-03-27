"""Tests for the dedup comparison script."""

import json
import sys
import tempfile
from pathlib import Path

import pytest

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.tests.fixtures.compare_dedup import (
    compare_file,
    current_dedup_strategy,
    extract_message_id,
    extract_request_id,
    extract_tokens,
    load_jsonl,
    reference_dedup_strategy,
    run_comparison,
)


class TestExtractFunctions:
    """Test extraction helper functions."""

    def test_extract_message_id_direct(self) -> None:
        """Test extraction from top-level message_id."""
        data = {"message_id": "msg_123"}
        assert extract_message_id(data) == "msg_123"

    def test_extract_message_id_nested(self) -> None:
        """Test extraction from nested message.id."""
        data = {"message": {"id": "msg_456"}}
        assert extract_message_id(data) == "msg_456"

    def test_extract_message_id_missing(self) -> None:
        """Test extraction when message_id is missing."""
        data = {"some": "data"}
        assert extract_message_id(data) is None

    def test_extract_request_id_direct(self) -> None:
        """Test extraction from request_id."""
        data = {"request_id": "req_123"}
        assert extract_request_id(data) == "req_123"

    def test_extract_request_id_camelcase(self) -> None:
        """Test extraction from requestId (camelCase)."""
        data = {"requestId": "req_456"}
        assert extract_request_id(data) == "req_456"

    def test_extract_request_id_missing(self) -> None:
        """Test extraction when request_id is missing."""
        data = {"message_id": "msg_123"}
        assert extract_request_id(data) is None


class TestExtractTokens:
    """Test token extraction."""

    def test_extract_tokens_from_usage(self) -> None:
        """Test extraction from usage field."""
        data = {"usage": {"input_tokens": 100, "output_tokens": 50}}
        tokens = extract_tokens(data)
        assert tokens["input_tokens"] == 100
        assert tokens["output_tokens"] == 50

    def test_extract_tokens_from_message_usage(self) -> None:
        """Test extraction from message.usage field."""
        data = {"message": {"usage": {"input_tokens": 200, "output_tokens": 75}}}
        tokens = extract_tokens(data)
        assert tokens["input_tokens"] == 200
        assert tokens["output_tokens"] == 75

    def test_extract_tokens_with_cache(self) -> None:
        """Test extraction with cache tokens."""
        data = {
            "usage": {
                "input_tokens": 100,
                "output_tokens": 50,
                "cache_creation_input_tokens": 10,
                "cache_read_input_tokens": 5,
            }
        }
        tokens = extract_tokens(data)
        assert tokens["cache_creation_tokens"] == 10
        assert tokens["cache_read_tokens"] == 5

    def test_extract_tokens_missing(self) -> None:
        """Test extraction with no tokens."""
        data = {}
        tokens = extract_tokens(data)
        assert tokens["input_tokens"] == 0
        assert tokens["output_tokens"] == 0
        assert tokens["cache_creation_tokens"] == 0
        assert tokens["cache_read_tokens"] == 0


class TestLoadJsonl:
    """Test JSONL loading."""

    def test_load_jsonl_valid(self) -> None:
        """Test loading valid JSONL."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            f.write('{"id": 1}\n{"id": 2}\n{"id": 3}\n')
            temp_path = Path(f.name)

        try:
            entries = load_jsonl(temp_path)
            assert len(entries) == 3
            assert entries[0]["id"] == 1
            assert entries[1]["id"] == 2
            assert entries[2]["id"] == 3
        finally:
            temp_path.unlink()

    def test_load_jsonl_with_empty_lines(self) -> None:
        """Test loading JSONL with empty lines."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            f.write('{"id": 1}\n\n{"id": 2}\n  \n{"id": 3}\n')
            temp_path = Path(f.name)

        try:
            entries = load_jsonl(temp_path)
            assert len(entries) == 3
        finally:
            temp_path.unlink()

    def test_load_jsonl_invalid_json(self) -> None:
        """Test loading JSONL with invalid JSON."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            f.write('{"id": 1}\ninvalid\n{"id": 3}\n')
            temp_path = Path(f.name)

        try:
            entries = load_jsonl(temp_path)
            assert len(entries) == 2
            assert entries[0]["id"] == 1
            assert entries[1]["id"] == 3
        finally:
            temp_path.unlink()


class TestCurrentDedupStrategy:
    """Test current deduplication strategy."""

    def test_with_request_id(self) -> None:
        """Test dedup with request_id present."""
        entries = [
            {"message_id": "msg_1", "request_id": "req_1", "usage": {"input_tokens": 100, "output_tokens": 50}},
            {"message_id": "msg_1", "request_id": "req_1", "usage": {"input_tokens": 150, "output_tokens": 75}},
        ]
        _, stats = current_dedup_strategy(entries)
        assert stats.unique_entries == 1
        assert stats.entries_deduped == 1

    def test_without_request_id(self) -> None:
        """Test dedup without request_id (returns None hash, no dedup)."""
        entries = [
            {"message_id": "msg_1", "usage": {"input_tokens": 100, "output_tokens": 50}},
            {"message_id": "msg_1", "usage": {"input_tokens": 150, "output_tokens": 75}},
        ]
        _, stats = current_dedup_strategy(entries)
        # Without request_id, hash is None and entry is NOT deduplicated
        assert stats.unique_entries == 2
        assert stats.entries_deduped == 0

    def test_mixed(self) -> None:
        """Test with mixed entries (some have request_id, some don't)."""
        entries = [
            {"message_id": "msg_1", "request_id": "req_1", "usage": {"input_tokens": 100, "output_tokens": 50}},
            {"message_id": "msg_1", "request_id": "req_1", "usage": {"input_tokens": 150, "output_tokens": 75}},
            {"message_id": "msg_2", "usage": {"input_tokens": 200, "output_tokens": 100}},
            {"message_id": "msg_2", "usage": {"input_tokens": 250, "output_tokens": 125}},
        ]
        _, stats = current_dedup_strategy(entries)
        # First two are deduped (same msg_id + req_id)
        # Last two are NOT deduped (missing request_id)
        assert stats.unique_entries == 3
        assert stats.entries_deduped == 1


class TestReferenceDedupStrategy:
    """Test reference deduplication strategy."""

    def test_with_request_id(self) -> None:
        """Test dedup with request_id present."""
        entries = [
            {"message_id": "msg_1", "request_id": "req_1", "usage": {"input_tokens": 100, "output_tokens": 50}},
            {"message_id": "msg_1", "request_id": "req_2", "usage": {"input_tokens": 150, "output_tokens": 75}},
        ]
        _, stats = reference_dedup_strategy(entries)
        # Same message_id, so only first is kept
        assert stats.unique_entries == 1
        assert stats.entries_deduped == 1

    def test_without_request_id(self) -> None:
        """Test dedup without request_id."""
        entries = [
            {"message_id": "msg_1", "usage": {"input_tokens": 100, "output_tokens": 50}},
            {"message_id": "msg_1", "usage": {"input_tokens": 150, "output_tokens": 75}},
        ]
        _, stats = reference_dedup_strategy(entries)
        # Same message_id, so only first is kept
        assert stats.unique_entries == 1
        assert stats.entries_deduped == 1

    def test_different_message_ids(self) -> None:
        """Test with different message_ids."""
        entries = [
            {"message_id": "msg_1", "usage": {"input_tokens": 100, "output_tokens": 50}},
            {"message_id": "msg_2", "usage": {"input_tokens": 150, "output_tokens": 75}},
        ]
        _, stats = reference_dedup_strategy(entries)
        assert stats.unique_entries == 2
        assert stats.entries_deduped == 0


class TestCompareFile:
    """Test file comparison."""

    def test_compare_legacy_file(self) -> None:
        """Test comparing a legacy file (has request_id)."""
        fixtures_dir = Path(__file__).parent / "fixtures" / "logs"
        legacy_file = fixtures_dir / "legacy" / "small.jsonl"

        if not legacy_file.exists():
            pytest.skip("Fixtures not found")

        result = compare_file(legacy_file)
        # Legacy files have ratio 1.0
        assert result["ratio"]["tokens"] == 1.0
        assert result["ratio"]["cost"] == 1.0
        # Both strategies should produce same results
        assert result["current"]["unique"] == result["reference"]["unique"]

    def test_compare_modern_main_file(self) -> None:
        """Test comparing a modern-main file (missing request_id)."""
        fixtures_dir = Path(__file__).parent / "fixtures" / "logs"
        modern_file = fixtures_dir / "modern-main" / "small.jsonl"

        if not modern_file.exists():
            pytest.skip("Fixtures not found")

        result = compare_file(modern_file)
        # Modern files should have ratio > 1.0 (over-counting)
        assert result["ratio"]["tokens"] > 1.0
        assert result["current"]["unique"] > result["reference"]["unique"]


class TestRunComparison:
    """Test running comparison on all fixtures."""

    def test_run_comparison(self) -> None:
        """Test running comparison on all fixture categories."""
        fixtures_dir = Path(__file__).parent / "fixtures" / "logs"

        if not fixtures_dir.exists():
            pytest.skip("Fixtures not found")

        results = run_comparison(fixtures_dir)

        # Should have results for all categories
        assert "legacy" in results
        assert "modern-main" in results
        assert "modern-subagents" in results
        assert "mixed" in results

        # Legacy should have ratio 1.0
        assert results["legacy"]["small"]["ratio"]["tokens"] == 1.0

        # Modern categories should have ratio > 1.0
        assert results["modern-main"]["small"]["ratio"]["tokens"] > 1.0
        assert results["modern-subagents"]["small"]["ratio"]["tokens"] > 1.0
        assert results["mixed"]["small"]["ratio"]["tokens"] > 1.0
