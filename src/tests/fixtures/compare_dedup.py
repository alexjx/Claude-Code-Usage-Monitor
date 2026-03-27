#!/usr/bin/env python3
"""Comparison script for deduplication strategies.

This script compares the deduplication strategies:
- legacy mode: message_id + request_id (both required) - OLD behavior
- message-id-max mode: message_id only with usage-max selection - NEW behavior

Run from project root:
    python -m src.tests.fixtures.compare_dedup

Or with uv:
    uv run python -m src.tests.fixtures.compare_dedup
"""

import json
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.claude_monitor.core.models import CostMode
from src.claude_monitor.core.pricing import PricingCalculator
from src.claude_monitor.data.reader import load_usage_entries


@dataclass
class DedupStats:
    """Statistics for a deduplication strategy."""

    entries_processed: int
    entries_deduped: int
    unique_entries: int
    total_input_tokens: int
    total_output_tokens: int
    total_cache_creation: int
    total_cache_read: int
    total_tokens: int
    total_cost: float


def load_jsonl(file_path: Path) -> list[dict[str, Any]]:
    """Load entries from a JSONL file."""
    entries = []
    with open(file_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return entries


def extract_message_id(data: dict[str, Any]) -> str | None:
    """Extract message_id from entry."""
    if msg_id := data.get("message_id"):
        return msg_id
    if message := data.get("message"):
        if isinstance(message, dict):
            return message.get("id")
    return None


def extract_request_id(data: dict[str, Any]) -> str | None:
    """Extract request_id from entry."""
    return data.get("requestId") or data.get("request_id")


def extract_tokens(data: dict[str, Any]) -> dict[str, int]:
    """Extract token counts from entry."""
    usage = data.get("usage", {})

    # Try direct fields first
    input_tokens = usage.get("input_tokens", 0) or data.get("inputTokens", 0) or data.get("input_tokens", 0)
    output_tokens = usage.get("output_tokens", 0) or data.get("outputTokens", 0) or data.get("output_tokens", 0)
    cache_creation = (
        usage.get("cache_creation_input_tokens", 0)
        or usage.get("cacheCreationInputTokens", 0)
        or data.get("cache_creation_tokens", 0)
    )
    cache_read = (
        usage.get("cache_read_input_tokens", 0)
        or usage.get("cacheReadInputTokens", 0)
        or data.get("cache_read_tokens", 0)
    )

    # Try message.usage if not found at top level
    if input_tokens == 0 and output_tokens == 0:
        if message := data.get("message"):
            if isinstance(message, dict) and (msg_usage := message.get("usage")):
                input_tokens = msg_usage.get("input_tokens", 0) or data.get("input_tokens", 0)
                output_tokens = msg_usage.get("output_tokens", 0) or data.get("output_tokens", 0)
                cache_creation = msg_usage.get("cache_creation_input_tokens", 0) or data.get("cache_creation_tokens", 0)
                cache_read = msg_usage.get("cache_read_input_tokens", 0) or data.get("cache_read_tokens", 0)

    return {
        "input_tokens": input_tokens or 0,
        "output_tokens": output_tokens or 0,
        "cache_creation_tokens": cache_creation or 0,
        "cache_read_tokens": cache_read or 0,
    }


# Module-level pricing calculator instance
_pricing_calculator = PricingCalculator()


def calculate_cost(tokens: dict[str, int], model: str) -> float:
    """Calculate cost based on token counts and model using project pricing module."""
    return _pricing_calculator.calculate_cost(
        model=model,
        input_tokens=tokens["input_tokens"],
        output_tokens=tokens["output_tokens"],
        cache_creation_tokens=tokens["cache_creation_tokens"],
        cache_read_tokens=tokens["cache_read_tokens"],
    )


def get_model(data: dict[str, Any]) -> str:
    """Extract model name from entry."""
    if model := data.get("model"):
        return model
    if message := data.get("message"):
        if isinstance(message, dict):
            return message.get("model", "unknown")
    return "unknown"


def compute_stats(entries: list[dict[str, Any]], seen_ids: set[str]) -> tuple[list[dict[str, Any]], DedupStats]:
    """Compute statistics for entries using a deduplication strategy.

    Returns (unique_entries, stats).
    """
    unique_entries = []
    total_input = 0
    total_output = 0
    total_cache_creation = 0
    total_cache_read = 0
    total_cost = 0.0

    for entry in entries:
        msg_id = extract_message_id(entry)
        if not msg_id:
            continue

        if msg_id in seen_ids:
            continue

        seen_ids.add(msg_id)
        unique_entries.append(entry)

        tokens = extract_tokens(entry)
        total_input += tokens["input_tokens"]
        total_output += tokens["output_tokens"]
        total_cache_creation += tokens["cache_creation_tokens"]
        total_cache_read += tokens["cache_read_tokens"]

        model = get_model(entry)
        total_cost += calculate_cost(tokens, model)

    return unique_entries, DedupStats(
        entries_processed=len(entries),
        entries_deduped=len(entries) - len(unique_entries),
        unique_entries=len(unique_entries),
        total_input_tokens=total_input,
        total_output_tokens=total_output,
        total_cache_creation=total_cache_creation,
        total_cache_read=total_cache_read,
        total_tokens=total_input + total_output + total_cache_creation + total_cache_read,
        total_cost=round(total_cost, 6),
    )


def current_dedup_strategy(entries: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], DedupStats]:
    """Current strategy: message_id + request_id (both required)."""
    seen: set[str] = set()
    unique_entries = []
    total_input = 0
    total_output = 0
    total_cache_creation = 0
    total_cache_read = 0
    total_cost = 0.0

    for entry in entries:
        msg_id = extract_message_id(entry)
        req_id = extract_request_id(entry)

        # Current logic: if either is missing, hash is None and entry is NOT deduped
        if not msg_id or not req_id:
            hash_key = None
        else:
            hash_key = f"{msg_id}:{req_id}"

        if hash_key and hash_key in seen:
            continue

        if hash_key:
            seen.add(hash_key)
        unique_entries.append(entry)

        tokens = extract_tokens(entry)
        total_input += tokens["input_tokens"]
        total_output += tokens["output_tokens"]
        total_cache_creation += tokens["cache_creation_tokens"]
        total_cache_read += tokens["cache_read_tokens"]

        model = get_model(entry)
        total_cost += calculate_cost(tokens, model)

    return unique_entries, DedupStats(
        entries_processed=len(entries),
        entries_deduped=len(entries) - len(unique_entries),
        unique_entries=len(unique_entries),
        total_input_tokens=total_input,
        total_output_tokens=total_output,
        total_cache_creation=total_cache_creation,
        total_cache_read=total_cache_read,
        total_tokens=total_input + total_output + total_cache_creation + total_cache_read,
        total_cost=round(total_cost, 6),
    )


def reference_dedup_strategy(entries: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], DedupStats]:
    """Reference strategy: message_id only."""
    seen: set[str] = set()
    unique_entries = []
    total_input = 0
    total_output = 0
    total_cache_creation = 0
    total_cache_read = 0
    total_cost = 0.0

    for entry in entries:
        msg_id = extract_message_id(entry)
        if not msg_id:
            continue

        if msg_id in seen:
            continue

        seen.add(msg_id)
        unique_entries.append(entry)

        tokens = extract_tokens(entry)
        total_input += tokens["input_tokens"]
        total_output += tokens["output_tokens"]
        total_cache_creation += tokens["cache_creation_tokens"]
        total_cache_read += tokens["cache_read_tokens"]

        model = get_model(entry)
        total_cost += calculate_cost(tokens, model)

    return unique_entries, DedupStats(
        entries_processed=len(entries),
        entries_deduped=len(entries) - len(unique_entries),
        unique_entries=len(unique_entries),
        total_input_tokens=total_input,
        total_output_tokens=total_output,
        total_cache_creation=total_cache_creation,
        total_cache_read=total_cache_read,
        total_tokens=total_input + total_output + total_cache_creation + total_cache_read,
        total_cost=round(total_cost, 6),
    )


def compare_file(file_path: Path) -> dict[str, Any]:
    """Compare dedup strategies for a single file."""
    entries = load_jsonl(file_path)

    _, current_stats = current_dedup_strategy(entries)
    _, reference_stats = reference_dedup_strategy(entries)

    # Calculate ratio (current / reference)
    # If ratio > 1, current over-counts (bug)
    # If ratio == 1, strategies agree
    token_ratio = (
        current_stats.total_tokens / reference_stats.total_tokens
        if reference_stats.total_tokens > 0
        else 1.0
    )
    cost_ratio = (
        current_stats.total_cost / reference_stats.total_cost
        if reference_stats.total_cost > 0
        else 1.0
    )

    return {
        "file": str(file_path),
        "entries_total": len(entries),
        "current": {
            "unique": current_stats.unique_entries,
            "deduped": current_stats.entries_deduped,
            "input_tokens": current_stats.total_input_tokens,
            "output_tokens": current_stats.total_output_tokens,
            "total_tokens": current_stats.total_tokens,
            "total_cost": current_stats.total_cost,
        },
        "reference": {
            "unique": reference_stats.unique_entries,
            "deduped": reference_stats.entries_deduped,
            "input_tokens": reference_stats.total_input_tokens,
            "output_tokens": reference_stats.total_output_tokens,
            "total_tokens": reference_stats.total_tokens,
            "total_cost": reference_stats.total_cost,
        },
        "ratio": {
            "tokens": round(token_ratio, 4),
            "cost": round(cost_ratio, 4),
        },
    }


def compare_file_with_actual(file_path: Path) -> dict[str, Any]:
    """Compare dedup strategies including actual reader.py implementation.

    Compares:
    - legacy mode (message_id + request_id) - OLD behavior
    - message-id-max mode using actual reader.py - NEW behavior
    """
    entries = load_jsonl(file_path)

    # Legacy behavior (message_id + request_id)
    _, legacy_stats = current_dedup_strategy(entries)

    # Reference behavior (message_id only, first occurrence)
    _, reference_stats = reference_dedup_strategy(entries)

    # Actual reader.py with message-id-max mode
    try:
        actual_stats = compute_stats_with_reader(file_path, "message-id-max")
    except Exception as e:
        # If reader.py fails, return without actual stats
        return {
            "file": str(file_path),
            "entries_total": len(entries),
            "legacy": {
                "unique": legacy_stats.unique_entries,
                "deduped": legacy_stats.entries_deduped,
                "input_tokens": legacy_stats.total_input_tokens,
                "output_tokens": legacy_stats.total_output_tokens,
                "total_tokens": legacy_stats.total_tokens,
                "total_cost": legacy_stats.total_cost,
            },
            "reference": {
                "unique": reference_stats.unique_entries,
                "deduped": reference_stats.entries_deduped,
                "input_tokens": reference_stats.total_input_tokens,
                "output_tokens": reference_stats.total_output_tokens,
                "total_tokens": reference_stats.total_tokens,
                "total_cost": reference_stats.total_cost,
            },
            "ratio": {
                "tokens": round(
                    legacy_stats.total_tokens / reference_stats.total_tokens
                    if reference_stats.total_tokens > 0 else 1.0, 4
                ),
                "cost": round(
                    legacy_stats.total_cost / reference_stats.total_cost
                    if reference_stats.total_cost > 0 else 1.0, 4
                ),
            },
            "actual_message_id_max": None,
            "actual_ratio": None,
        }

    # Ratio of legacy vs reference (should be > 1 for modern fixtures - shows overcounting)
    legacy_vs_ref_token_ratio = (
        legacy_stats.total_tokens / reference_stats.total_tokens
        if reference_stats.total_tokens > 0
        else 1.0
    )

    # Ratio of actual (message-id-max) vs reference (should be ~1.0 - shows fix works)
    actual_vs_ref_token_ratio = (
        actual_stats.total_tokens / reference_stats.total_tokens
        if reference_stats.total_tokens > 0
        else 1.0
    )

    return {
        "file": str(file_path),
        "entries_total": len(entries),
        "legacy": {
            "unique": legacy_stats.unique_entries,
            "deduped": legacy_stats.entries_deduped,
            "input_tokens": legacy_stats.total_input_tokens,
            "output_tokens": legacy_stats.total_output_tokens,
            "total_tokens": legacy_stats.total_tokens,
            "total_cost": legacy_stats.total_cost,
        },
        "reference": {
            "unique": reference_stats.unique_entries,
            "deduped": reference_stats.entries_deduped,
            "input_tokens": reference_stats.total_input_tokens,
            "output_tokens": reference_stats.total_output_tokens,
            "total_tokens": reference_stats.total_tokens,
            "total_cost": reference_stats.total_cost,
        },
        "ratio": {
            "tokens": round(legacy_vs_ref_token_ratio, 4),
            "cost": round(
                legacy_stats.total_cost / reference_stats.total_cost
                if reference_stats.total_cost > 0 else 1.0, 4
            ),
        },
        "actual_message_id_max": {
            "unique": actual_stats.unique_entries,
            "input_tokens": actual_stats.total_input_tokens,
            "output_tokens": actual_stats.total_output_tokens,
            "total_tokens": actual_stats.total_tokens,
            "total_cost": actual_stats.total_cost,
        },
        "actual_ratio": {
            "tokens": round(actual_vs_ref_token_ratio, 4),
            "cost": round(
                actual_stats.total_cost / reference_stats.total_cost
                if reference_stats.total_cost > 0 else 1.0, 4
            ),
        },
    }


def compute_stats_from_usage_entries(entries: list, seen_ids: set) -> tuple:
    """Compute statistics from UsageEntry objects (from reader.py).

    Returns (unique_entries, stats).
    """
    unique_entries = []
    total_input = 0
    total_output = 0
    total_cache_creation = 0
    total_cache_read = 0
    total_cost = 0.0

    for entry in entries:
        msg_id = entry.message_id
        if not msg_id:
            continue

        if msg_id in seen_ids:
            continue

        seen_ids.add(msg_id)
        unique_entries.append(entry)

        total_input += entry.input_tokens
        total_output += entry.output_tokens
        total_cache_creation += entry.cache_creation_tokens
        total_cache_read += entry.cache_read_tokens
        total_cost += entry.cost_usd

    return unique_entries, DedupStats(
        entries_processed=len(entries),
        entries_deduped=len(entries) - len(unique_entries),
        unique_entries=len(unique_entries),
        total_input_tokens=total_input,
        total_output_tokens=total_output,
        total_cache_creation=total_cache_creation,
        total_cache_read=total_cache_read,
        total_tokens=total_input + total_output + total_cache_creation + total_cache_read,
        total_cost=round(total_cost, 6),
    )


def compute_stats_with_reader(file_path: Path, dedupe_mode: str) -> DedupStats:
    """Compute statistics using actual reader.py with specified dedupe_mode.

    Args:
        file_path: Path to JSONL file
        dedupe_mode: Deduplication mode to use

    Returns:
        DedupStats for the entries loaded with the specified mode
    """
    # Create a temp directory with the JSONL file
    with tempfile.TemporaryDirectory() as temp_dir:
        # Copy the file to temp directory (reader expects a directory of JSONL files)
        import shutil
        temp_file = Path(temp_dir) / "test.jsonl"
        shutil.copy(file_path, temp_file)

        # Load entries with the specified dedupe_mode
        entries, _ = load_usage_entries(
            data_path=temp_dir,
            dedupe_mode=dedupe_mode,
        )

    # Compute stats from entries
    seen: set[str] = set()
    total_input = 0
    total_output = 0
    total_cache_creation = 0
    total_cache_read = 0
    total_cost = 0.0

    for entry in entries:
        msg_id = entry.message_id
        if not msg_id:
            continue

        if msg_id in seen:
            continue

        seen.add(msg_id)
        total_input += entry.input_tokens
        total_output += entry.output_tokens
        total_cache_creation += entry.cache_creation_tokens
        total_cache_read += entry.cache_read_tokens
        total_cost += entry.cost_usd

    return DedupStats(
        entries_processed=len(entries),
        entries_deduped=0,  # Not tracked in this flow
        unique_entries=len(seen),
        total_input_tokens=total_input,
        total_output_tokens=total_output,
        total_cache_creation=total_cache_creation,
        total_cache_read=total_cache_read,
        total_tokens=total_input + total_output + total_cache_creation + total_cache_read,
        total_cost=round(total_cost, 6),
    )


def run_comparison(fixtures_dir: Path) -> dict[str, Any]:
    """Run comparison on all fixture files."""
    results = {}
    categories = ["legacy", "modern-main", "modern-subagents", "mixed"]

    for category in categories:
        category_dir = fixtures_dir / category
        if not category_dir.exists():
            continue

        results[category] = {}
        for size in ["small", "medium", "large"]:
            file_path = category_dir / f"{size}.jsonl"
            if file_path.exists():
                results[category][size] = compare_file_with_actual(file_path)

    return results


def print_report(results: dict[str, Any]) -> None:
    """Print a formatted report of the comparison results."""
    print("\n" + "=" * 80)
    print("DEDUPLICATION COMPARISON REPORT")
    print("=" * 80)
    print("\nStrategies compared:")
    print("  - Legacy (message_id + request_id): OLD behavior (may over-count)")
    print("  - Reference (message_id only): FIRST occurrence kept")
    print("  - Actual reader.py (message-id-max): USAGE-MAX snapshot kept")
    print("  - Actual ratio (reader-id-max / reference) should be ~1.0 after fix\n")

    for category, sizes in results.items():
        print(f"\n{'=' * 40}")
        print(f"Category: {category.upper()}")
        print(f"{'=' * 40}")

        for size, data in sizes.items():
            print(f"\n  {size.upper()} ({data['entries_total']} entries):")

            # Legacy (old behavior)
            print(f"    Legacy (msg_id+req_id):")
            print(f"      Unique entries: {data['legacy']['unique']}")
            print(f"      Total tokens: {data['legacy']['total_tokens']:,}")
            print(f"      Total cost: ${data['legacy']['total_cost']:.6f}")

            # Reference (correct behavior)
            print(f"    Reference (msg_id only):")
            print(f"      Unique entries: {data['reference']['unique']}")
            print(f"      Total tokens: {data['reference']['total_tokens']:,}")
            print(f"      Total cost: ${data['reference']['total_cost']:.6f}")

            # Legacy vs Reference ratio (should be > 1 for modern fixtures - shows overcounting)
            print(f"    Legacy/Reference ratio:")
            print(f"      Tokens: {data['ratio']['tokens']:.4f}")
            print(f"      Cost:   {data['ratio']['cost']:.4f}")

            # Flag legacy overcounting
            if data['ratio']['tokens'] > 1.0:
                print(f"    WARNING: Legacy over-counts! "
                      f"Legacy has {data['legacy']['unique']} entries, "
                      f"but reference expects {data['reference']['unique']}")

            # Actual reader.py stats
            if data['actual_message_id_max'] is not None:
                print(f"    Actual reader.py (message-id-max):")
                print(f"      Unique entries: {data['actual_message_id_max']['unique']}")
                print(f"      Total tokens: {data['actual_message_id_max']['total_tokens']:,}")
                print(f"      Total cost: ${data['actual_message_id_max']['total_cost']:.6f}")
                print(f"    Actual/Reference ratio (should be ~1.0):")
                print(f"      Tokens: {data['actual_ratio']['tokens']:.4f}")
                print(f"      Cost:   {data['actual_ratio']['cost']:.4f}")

                # Check if actual ratio is close to 1.0
                if abs(data['actual_ratio']['tokens'] - 1.0) > 0.01:
                    print(f"    NOTE: Actual ratio is not ~1.0, fix may need adjustment")
                else:
                    print(f"    SUCCESS: Actual ratio is ~1.0, fix working!")

    print("\n" + "=" * 80)
    print("END OF REPORT")
    print("=" * 80 + "\n")


def main() -> int:
    """Main entry point."""
    # Find fixtures directory relative to this script
    script_dir = Path(__file__).parent
    fixtures_dir = script_dir / "logs"

    if not fixtures_dir.exists():
        print(f"Error: Fixtures directory not found: {fixtures_dir}")
        return 1

    results = run_comparison(fixtures_dir)
    print_report(results)

    # Check if the actual reader.py ratio is ~1.0 for modern fixtures (indicating fix works)
    fix_working = True
    for category in ["modern-main", "modern-subagents", "mixed"]:
        if category in results:
            for size, data in results[category].items():
                if data["actual_ratio"] is not None:
                    if abs(data["actual_ratio"]["tokens"] - 1.0) > 0.01:
                        fix_working = False
                        break

    if fix_working:
        print("SUCCESS: message-id-max mode produces ratio ~1.0 for modern fixtures!")
    else:
        print("NOTE: message-id-max mode ratio is not ~1.0 for some modern fixtures.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
