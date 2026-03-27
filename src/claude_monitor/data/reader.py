"""Simplified data reader for Claude Monitor.

Combines functionality from file_reader, filter, mapper, and processor
into a single cohesive module.
"""

import json
import logging
from datetime import datetime, timedelta
from datetime import timezone as tz
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from claude_monitor.core.data_processors import (
    DataConverter,
    TimestampProcessor,
    TokenExtractor,
)
from claude_monitor.core.models import CostMode, UsageEntry
from claude_monitor.core.pricing import PricingCalculator
from claude_monitor.error_handling import report_file_error
from claude_monitor.utils.time_utils import TimezoneHandler

FIELD_COST_USD = "cost_usd"
FIELD_MODEL = "model"
TOKEN_INPUT = "input_tokens"
TOKEN_OUTPUT = "output_tokens"

logger = logging.getLogger(__name__)


def load_usage_entries(
    data_path: Optional[str] = None,
    hours_back: Optional[int] = None,
    mode: CostMode = CostMode.AUTO,
    include_raw: bool = False,
    dedupe_mode: str = "message-id-max",
) -> Tuple[List[UsageEntry], Optional[List[Dict[str, Any]]]]:
    """Load and convert JSONL files to UsageEntry objects.

    Args:
        data_path: Path to Claude data directory (defaults to ~/.claude/projects)
        hours_back: Only include entries from last N hours
        mode: Cost calculation mode
        include_raw: Whether to return raw JSON data alongside entries
        dedupe_mode: Deduplication mode - 'message-id-max' keeps usage-max snapshot
                     for same message.id; 'legacy' uses message_id+request_id

    Returns:
        Tuple of (usage_entries, raw_data) where raw_data is None unless include_raw=True
    """
    data_path = Path(data_path if data_path else "~/.claude/projects").expanduser()
    timezone_handler = TimezoneHandler()
    pricing_calculator = PricingCalculator()

    cutoff_time = None
    if hours_back:
        cutoff_time = datetime.now(tz.utc) - timedelta(hours=hours_back)

    jsonl_files = _find_jsonl_files(data_path)
    if not jsonl_files:
        logger.warning("No JSONL files found in %s", data_path)
        return [], None

    all_entries: List[UsageEntry] = []
    raw_entries: Optional[List[Dict[str, Any]]] = [] if include_raw else None
    processed_hashes: Set[str] = set()

    # For message-id-max mode, we skip early deduplication and do it post-processing
    skip_dedup = dedupe_mode == "message-id-max"

    for file_path in jsonl_files:
        entries, raw_data = _process_single_file(
            file_path,
            mode,
            cutoff_time,
            processed_hashes,
            include_raw,
            timezone_handler,
            pricing_calculator,
            skip_dedup=skip_dedup,
        )
        all_entries.extend(entries)
        if include_raw and raw_data:
            raw_entries.extend(raw_data)

    all_entries.sort(key=lambda e: e.timestamp)

    # Apply message-id-max deduplication after collection
    if dedupe_mode == "message-id-max":
        all_entries = _apply_usage_max_dedup(all_entries)

    logger.info(f"Processed {len(all_entries)} entries from {len(jsonl_files)} files")

    return all_entries, raw_entries


def load_all_raw_entries(data_path: Optional[str] = None) -> List[Dict[str, Any]]:
    """Load all raw JSONL entries without processing.

    Args:
        data_path: Path to Claude data directory

    Returns:
        List of raw JSON dictionaries
    """
    data_path = Path(data_path if data_path else "~/.claude/projects").expanduser()
    jsonl_files = _find_jsonl_files(data_path)

    all_raw_entries: List[Dict[str, Any]] = []
    for file_path in jsonl_files:
        try:
            with open(file_path, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        all_raw_entries.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        except Exception as e:
            logger.exception(f"Error loading raw entries from {file_path}: {e}")

    return all_raw_entries


def _find_jsonl_files(data_path: Path) -> List[Path]:
    """Find all .jsonl files in the data directory."""
    if not data_path.exists():
        logger.warning("Data path does not exist: %s", data_path)
        return []
    return list(data_path.rglob("*.jsonl"))


def _process_single_file(
    file_path: Path,
    mode: CostMode,
    cutoff_time: Optional[datetime],
    processed_hashes: Set[str],
    include_raw: bool,
    timezone_handler: TimezoneHandler,
    pricing_calculator: PricingCalculator,
    skip_dedup: bool = False,
) -> Tuple[List[UsageEntry], Optional[List[Dict[str, Any]]]]:
    """Process a single JSONL file."""
    entries: List[UsageEntry] = []
    raw_data: Optional[List[Dict[str, Any]]] = [] if include_raw else None

    try:
        entries_read = 0
        entries_filtered = 0
        entries_mapped = 0

        with open(file_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                try:
                    data = json.loads(line)
                    entries_read += 1

                    if not skip_dedup and not _should_process_entry(
                        data, cutoff_time, processed_hashes, timezone_handler
                    ):
                        entries_filtered += 1
                        continue

                    entry = _map_to_usage_entry(
                        data, mode, timezone_handler, pricing_calculator
                    )
                    if entry:
                        entries_mapped += 1
                        entries.append(entry)
                        if not skip_dedup:
                            _update_processed_hashes(data, processed_hashes)

                    if include_raw:
                        raw_data.append(data)

                except json.JSONDecodeError as e:
                    logger.debug(f"Failed to parse JSON line in {file_path}: {e}")
                    continue

        logger.debug(
            f"File {file_path.name}: {entries_read} read, "
            f"{entries_filtered} filtered out, {entries_mapped} successfully mapped"
        )

    except Exception as e:
        logger.warning("Failed to read file %s: %s", file_path, e)
        report_file_error(
            exception=e,
            file_path=str(file_path),
            operation="read",
            additional_context={"file_exists": file_path.exists()},
        )
        return [], None

    return entries, raw_data


def _should_process_entry(
    data: Dict[str, Any],
    cutoff_time: Optional[datetime],
    processed_hashes: Set[str],
    timezone_handler: TimezoneHandler,
) -> bool:
    """Check if entry should be processed based on time and uniqueness."""
    if cutoff_time:
        timestamp_str = data.get("timestamp")
        if timestamp_str:
            processor = TimestampProcessor(timezone_handler)
            timestamp = processor.parse_timestamp(timestamp_str)
            if timestamp and timestamp < cutoff_time:
                return False

    unique_hash = _create_unique_hash(data)
    return not (unique_hash and unique_hash in processed_hashes)


def _create_unique_hash(data: Dict[str, Any]) -> Optional[str]:
    """Create unique hash for deduplication."""
    message_id = data.get("message_id") or (
        data.get("message", {}).get("id")
        if isinstance(data.get("message"), dict)
        else None
    )
    request_id = data.get("requestId") or data.get("request_id")

    return f"{message_id}:{request_id}" if message_id and request_id else None


def _update_processed_hashes(data: Dict[str, Any], processed_hashes: Set[str]) -> None:
    """Update the processed hashes set with current entry's hash."""
    unique_hash = _create_unique_hash(data)
    if unique_hash:
        processed_hashes.add(unique_hash)


def _map_to_usage_entry(
    data: Dict[str, Any],
    mode: CostMode,
    timezone_handler: TimezoneHandler,
    pricing_calculator: PricingCalculator,
) -> Optional[UsageEntry]:
    """Map raw data to UsageEntry with proper cost calculation."""
    try:
        timestamp_processor = TimestampProcessor(timezone_handler)
        timestamp = timestamp_processor.parse_timestamp(data.get("timestamp", ""))
        if not timestamp:
            return None

        token_data = TokenExtractor.extract_tokens(data)
        if not any(v for k, v in token_data.items() if k != "total_tokens"):
            return None

        model = DataConverter.extract_model_name(data, default="unknown")

        entry_data: Dict[str, Any] = {
            FIELD_MODEL: model,
            TOKEN_INPUT: token_data["input_tokens"],
            TOKEN_OUTPUT: token_data["output_tokens"],
            "cache_creation_tokens": token_data.get("cache_creation_tokens", 0),
            "cache_read_tokens": token_data.get("cache_read_tokens", 0),
            FIELD_COST_USD: data.get("cost") or data.get(FIELD_COST_USD),
        }
        cost_usd = pricing_calculator.calculate_cost_for_entry(entry_data, mode)

        message = data.get("message", {})
        message_id = data.get("message_id") or message.get("id") or ""
        request_id = data.get("request_id") or data.get("requestId") or "unknown"
        session_id = data.get("sessionId", "") or ""

        return UsageEntry(
            timestamp=timestamp,
            input_tokens=token_data["input_tokens"],
            output_tokens=token_data["output_tokens"],
            cache_creation_tokens=token_data.get("cache_creation_tokens", 0),
            cache_read_tokens=token_data.get("cache_read_tokens", 0),
            cost_usd=cost_usd,
            model=model,
            message_id=message_id,
            request_id=request_id,
            session_id=session_id,
        )

    except (KeyError, ValueError, TypeError, AttributeError) as e:
        logger.debug(f"Failed to map entry: {type(e).__name__}: {e}")
        return None


class UsageEntryMapper:
    """Compatibility wrapper for legacy UsageEntryMapper interface.

    This class provides backward compatibility for tests that expect
    the old UsageEntryMapper interface, wrapping the new functional
    approach in _map_to_usage_entry.
    """

    def __init__(
        self, pricing_calculator: PricingCalculator, timezone_handler: TimezoneHandler
    ):
        """Initialize with required components."""
        self.pricing_calculator = pricing_calculator
        self.timezone_handler = timezone_handler

    def map(self, data: Dict[str, Any], mode: CostMode) -> Optional[UsageEntry]:
        """Map raw data to UsageEntry - compatibility interface."""
        return _map_to_usage_entry(
            data, mode, self.timezone_handler, self.pricing_calculator
        )

    def _has_valid_tokens(self, tokens: Dict[str, int]) -> bool:
        """Check if tokens are valid (for test compatibility)."""
        return any(v > 0 for v in tokens.values())

    def _extract_timestamp(self, data: Dict[str, Any]) -> Optional[datetime]:
        """Extract timestamp (for test compatibility)."""
        if "timestamp" not in data:
            return None
        processor = TimestampProcessor(self.timezone_handler)
        return processor.parse_timestamp(data["timestamp"])

    def _extract_model(self, data: Dict[str, Any]) -> str:
        """Extract model name (for test compatibility)."""
        return DataConverter.extract_model_name(data, default="unknown")

    def _extract_metadata(self, data: Dict[str, Any]) -> Dict[str, str]:
        """Extract metadata (for test compatibility)."""
        message = data.get("message", {})
        return {
            "message_id": data.get("message_id") or message.get("id", ""),
            "request_id": data.get("request_id") or data.get("requestId", "unknown"),
        }


def _create_tiered_dedup_key(data: Dict[str, Any]) -> Optional[str]:
    """Create tiered dedup key for message-id-max mode.

    Tiered strategy:
    1. sessionId + message.id + role + agent_id + isSidechain (primary)
    2. sessionId + event_uuid (fallback)
    3. sessionId + parentUuid + sourceToolAssistantUUID + tool_use_id (tool result chain)

    Returns None if no message.id is found.
    """
    message_id = data.get("message_id") or (
        data.get("message", {}).get("id")
        if isinstance(data.get("message"), dict)
        else None
    )

    if not message_id:
        return None

    # Get optional fields with empty string defaults
    session_id = data.get("sessionId", "") or ""
    role = data.get("role", "") or ""
    agent_id = data.get("agent_id", "") or data.get("agentId", "") or ""
    is_sidechain = str(data.get("isSidechain", "") or data.get("is_sidechain", "")).lower()

    # Tier 1: sessionId + message.id + role + agent_id + isSidechain
    if session_id and role:
        return f"{session_id}:{message_id}:{role}:{agent_id}:{is_sidechain}"

    # Tier 2: sessionId + event_uuid
    event_uuid = data.get("event_uuid", "") or data.get("eventUuid", "") or ""
    if session_id and event_uuid:
        return f"{session_id}:{event_uuid}"

    # Tier 3: sessionId + parentUuid + sourceToolAssistantUUID + tool_use_id
    parent_uuid = data.get("parentUuid", "") or data.get("parent_uuid", "") or ""
    source_tool_assistant_uuid = (
        data.get("sourceToolAssistantUUID", "") or data.get("source_tool_assistant_uuid", "") or ""
    )
    tool_use_id = data.get("tool_use_id", "") or data.get("toolUseId", "") or ""
    if session_id and parent_uuid and source_tool_assistant_uuid and tool_use_id:
        return f"{session_id}:{parent_uuid}:{source_tool_assistant_uuid}:{tool_use_id}"

    # Fallback: message_id only (when no sessionId)
    if not session_id:
        return message_id

    # Last resort: session_id + message_id
    return f"{session_id}:{message_id}"


def _get_entry_usage_total(entry: UsageEntry) -> int:
    """Get total usage for an entry (for usage-max selection)."""
    return (
        entry.input_tokens
        + entry.output_tokens
        + entry.cache_creation_tokens
        + entry.cache_read_tokens
    )


def _apply_usage_max_dedup(entries: List[UsageEntry]) -> List[UsageEntry]:
    """Apply usage-max deduplication for message-id-max mode.

    Groups entries by tiered dedup key, keeping only the entry with maximum
    total usage for each key. Entries without message_id are NOT deduplicated
    (maintains backward compatibility). Logs warnings for non-monotonic usage patterns.

    Args:
        entries: List of usage entries to deduplicate

    Returns:
        Deduplicated list of entries with usage-max for each key
    """
    if not entries:
        return entries

    # Separate entries with and without message_id
    # Entries without message_id are NOT deduplicated (backward compatibility)
    entries_with_id: List[UsageEntry] = []
    entries_without_id: List[UsageEntry] = []

    for entry in entries:
        if entry.message_id:
            entries_with_id.append(entry)
        else:
            entries_without_id.append(entry)

    # Group entries with message_id by the tiered key
    # This ensures entries from different sessions are not mixed up
    groups: Dict[str, List[UsageEntry]] = {}
    for entry in entries_with_id:
        # Build dict for tiered dedup key function
        data_for_key: Dict[str, Any] = {
            "message_id": entry.message_id,
            "sessionId": entry.session_id,
        }
        key = _create_tiered_dedup_key(data_for_key) or entry.message_id
        if key not in groups:
            groups[key] = []
        groups[key].append(entry)

    # For each group, keep only the entry with max usage
    result = list(entries_without_id)  # Entries without ID are kept as-is

    for key, group_entries in groups.items():
        if len(group_entries) == 1:
            result.append(group_entries[0])
        else:
            # Find entry with max usage
            max_entry = max(group_entries, key=_get_entry_usage_total)
            result.append(max_entry)

            # Check for non-monotonic usage (warn if segments have significantly
            # different usage patterns that might indicate over-counting)
            usage_values = [_get_entry_usage_total(e) for e in group_entries]
            max_usage = max(usage_values)
            min_usage = min(usage_values)

            if max_usage != min_usage and max_usage > 0:
                logger.warning(
                    f"Non-monotonic usage detected for message_id={key}: "
                    f"segments have usage values {usage_values}, keeping max={max_usage}"
                )

    return result
