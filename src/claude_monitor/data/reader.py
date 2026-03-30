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
from claude_monitor.core.models import CostMode, DedupeMode, UsageEntry
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
    dedupe_mode: DedupeMode = DedupeMode.MESSAGE_ID_MAX,
) -> Tuple[List[UsageEntry], Optional[List[Dict[str, Any]]]]:
    """Load and convert JSONL files to UsageEntry objects.

    Args:
        data_path: Path to Claude data directory (defaults to ~/.claude/projects)
        hours_back: Only include entries from last N hours
        mode: Cost calculation mode
        include_raw: Whether to return raw JSON data alongside entries
        dedupe_mode: Deduplication mode - MESSAGE_ID_MAX (default) uses message_id only
            and keeps entry with highest tokens per message_id; LEGACY uses message_id +
            request_id for backward compatibility with older logs

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
    # Track best entry per message_id for message-id-max mode
    # message_id -> (entry, total_tokens)
    dedupe_tracker: Dict[str, Tuple[UsageEntry, int]] = {}

    for file_path in jsonl_files:
        entries, raw_data = _process_single_file(
            file_path,
            mode,
            cutoff_time,
            dedupe_mode,
            dedupe_tracker,
            include_raw,
            timezone_handler,
            pricing_calculator,
        )
        all_entries.extend(entries)
        if include_raw and raw_data:
            raw_entries.extend(raw_data)

    all_entries.sort(key=lambda e: e.timestamp)

    # Resolve agent attribution via sourceToolAssistantUUID backtracking
    all_entries = _resolve_agent_attribution(all_entries)

    # Apply message-id-max deduplication: keep only best entry per message_id
    if dedupe_mode == DedupeMode.MESSAGE_ID_MAX and dedupe_tracker:
        all_entries = _apply_message_id_max_dedup(all_entries, dedupe_tracker)

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


def _process_single_file_v2(
    file_path: Path,
    mode: CostMode,
    cutoff_time: Optional[datetime],
    dedupe_mode: DedupeMode,
    dedupe_tracker: Dict[str, Tuple[UsageEntry, int]],
    include_raw: bool,
    timezone_handler: TimezoneHandler,
    pricing_calculator: PricingCalculator,
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

                    if dedupe_mode == DedupeMode.MESSAGE_ID_MAX:
                        # In message-id-max mode, always process - dedup happens post-processing
                        if not _should_process_entry_no_dedup(
                            data, cutoff_time, timezone_handler
                        ):
                            entries_filtered += 1
                            continue
                    else:
                        # Legacy mode: use message_id + request_id dedup
                        if not _should_process_entry_legacy(
                            data, cutoff_time, dedupe_tracker, timezone_handler
                        ):
                            entries_filtered += 1
                            continue

                    entry = _map_to_usage_entry(
                        data, mode, timezone_handler, pricing_calculator
                    )
                    if entry:
                        entries_mapped += 1
                        entries.append(entry)
                        _update_dedupe_tracker(
                            data, entry, dedupe_mode, dedupe_tracker
                        )

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


def _process_single_file(
    file_path: Path,
    mode: CostMode,
    cutoff_time: Optional[datetime],
    arg4: Any,
    arg5: Any,
    arg6: Any,
    arg7: Any,
    arg8: Any = None,
) -> Tuple[List[UsageEntry], Optional[List[Dict[str, Any]]]]:
    """Process a single JSONL file.

    This is a wrapper that handles two calling conventions:
    - New (8 params): file_path, mode, cutoff_time, dedupe_mode, dedupe_tracker, include_raw, timezone_handler, pricing_calculator
    - Old (7 params): file_path, mode, cutoff_time, processed_hashes, include_raw, timezone_handler, pricing_calculator

    Detected by checking if arg4 is a DedupeMode (new) or a set (old).

    For LEGACY mode with old calling convention, we use the original implementation
    that calls _should_process_entry and _update_processed_hashes (which are mocked in tests).
    """
    if isinstance(arg4, DedupeMode):
        # New calling convention: arg4=dedupe_mode, arg5=dedupe_tracker, arg6=include_raw, arg7=timezone_handler, arg8=pricing_calculator
        return _process_single_file_v2(
            file_path=file_path,
            mode=mode,
            cutoff_time=cutoff_time,
            dedupe_mode=arg4,
            dedupe_tracker=arg5,
            include_raw=arg6,
            timezone_handler=arg7,
            pricing_calculator=arg8,
        )
    else:
        # Old calling convention: arg4=processed_hashes(set), arg5=include_raw, arg6=timezone_handler, arg7=pricing_calculator
        # Use the original LEGACY implementation that calls _should_process_entry (mocked in tests)
        processed_hashes: Set[str] = arg4
        include_raw: bool = arg5
        actual_timezone_handler: TimezoneHandler = arg6
        actual_pricing_calculator: PricingCalculator = arg7

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

                        # Use backward-compatible _should_process_entry with set (mocked in tests)
                        if not _should_process_entry(
                            data, cutoff_time, processed_hashes, actual_timezone_handler
                        ):
                            entries_filtered += 1
                            continue

                        entry = _map_to_usage_entry(
                            data, mode, actual_timezone_handler, actual_pricing_calculator
                        )
                        if entry:
                            entries_mapped += 1
                            entries.append(entry)
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
    dedupe_mode: DedupeMode,
    dedupe_tracker: Dict[str, Tuple[UsageEntry, int]],
    timezone_handler: TimezoneHandler,
) -> bool:
    """Check if entry should be processed based on time and uniqueness.

    For MESSAGE_ID_MAX mode: only checks time cutoff, dedup handled post-processing.
    For LEGACY mode: checks time cutoff AND message_id+request_id dedup.
    """
    if cutoff_time:
        timestamp_str = data.get("timestamp")
        if timestamp_str:
            processor = TimestampProcessor(timezone_handler)
            timestamp = processor.parse_timestamp(timestamp_str)
            if timestamp and timestamp < cutoff_time:
                return False

    if dedupe_mode == DedupeMode.MESSAGE_ID_MAX:
        # Dedup handled post-processing, only check time
        return True

    # Legacy mode: check message_id + request_id
    unique_hash = _create_legacy_hash(data)
    if unique_hash:
        if unique_hash in dedupe_tracker:
            return False
        dedupe_tracker[unique_hash] = True  # type: ignore
    return True


def _should_process_entry_no_dedup(
    data: Dict[str, Any],
    cutoff_time: Optional[datetime],
    timezone_handler: TimezoneHandler,
) -> bool:
    """Check if entry should be processed based on time only (no dedup)."""
    if cutoff_time:
        timestamp_str = data.get("timestamp")
        if timestamp_str:
            processor = TimestampProcessor(timezone_handler)
            timestamp = processor.parse_timestamp(timestamp_str)
            if timestamp and timestamp < cutoff_time:
                return False
    return True


def _should_process_entry_legacy(
    data: Dict[str, Any],
    cutoff_time: Optional[datetime],
    dedupe_tracker: Dict[str, Tuple[UsageEntry, int]],
    timezone_handler: TimezoneHandler,
) -> bool:
    """Check if entry should be processed based on time and legacy dedup."""
    if cutoff_time:
        timestamp_str = data.get("timestamp")
        if timestamp_str:
            processor = TimestampProcessor(timezone_handler)
            timestamp = processor.parse_timestamp(timestamp_str)
            if timestamp and timestamp < cutoff_time:
                return False

    unique_hash = _create_legacy_hash(data)
    if unique_hash:
        # For legacy dedup, we track hashes in dedupe_tracker with None value
        if unique_hash in dedupe_tracker:
            return False
        dedupe_tracker[unique_hash] = None  # type: ignore
    return True


def _create_legacy_hash(data: Dict[str, Any]) -> Optional[str]:
    """Create unique hash for legacy deduplication (message_id + request_id)."""
    message_id = data.get("message_id") or (
        data.get("message", {}).get("id")
        if isinstance(data.get("message"), dict)
        else None
    )
    request_id = data.get("requestId") or data.get("request_id")

    return f"{message_id}:{request_id}" if message_id and request_id else None


# Backward-compatible alias for tests
_create_unique_hash = _create_legacy_hash


def _should_process_entry(
    data: Dict[str, Any],
    cutoff_time: Optional[datetime],
    processed_hashes: Set[str],
    timezone_handler: TimezoneHandler,
) -> bool:
    """Backward-compatible _should_process_entry for tests.

    Note: This uses LEGACY dedupe mode and ignores the dedupe_tracker.
    For new code, use the dedupe_mode-aware version.
    """
    if cutoff_time:
        timestamp_str = data.get("timestamp")
        if timestamp_str:
            processor = TimestampProcessor(timezone_handler)
            timestamp = processor.parse_timestamp(timestamp_str)
            if timestamp and timestamp < cutoff_time:
                return False

    unique_hash = _create_legacy_hash(data)
    return not (unique_hash and unique_hash in processed_hashes)


def _update_processed_hashes(
    data: Dict[str, Any], processed_hashes: Set[str]
) -> None:
    """Backward-compatible _update_processed_hashes for tests.

    Note: This uses LEGACY dedupe mode.
    For new code, use _update_dedupe_tracker with DedupeMode.
    """
    unique_hash = _create_legacy_hash(data)
    if unique_hash:
        processed_hashes.add(unique_hash)


def _create_message_id_hash(data: Dict[str, Any]) -> Optional[str]:
    """Create hash using message_id only (for modern logs without request_id)."""
    message_id = data.get("message_id") or (
        data.get("message", {}).get("id")
        if isinstance(data.get("message"), dict)
        else None
    )
    return message_id if message_id else None


def _update_dedupe_tracker(
    data: Dict[str, Any],
    entry: UsageEntry,
    dedupe_mode: DedupeMode,
    dedupe_tracker: Dict[str, Tuple[UsageEntry, int]],
) -> None:
    """Update dedupe tracker with current entry.

    For MESSAGE_ID_MAX mode: tracks message_id -> (entry, total_tokens), keeping highest.
    For LEGACY mode: tracks message_id:request_id -> True (already filtered above).
    """
    if dedupe_mode == DedupeMode.MESSAGE_ID_MAX:
        message_id = _create_message_id_hash(data)
        if message_id:
            total_tokens = (
                entry.input_tokens
                + entry.output_tokens
                + entry.cache_creation_tokens
                + entry.cache_read_tokens
            )
            existing = dedupe_tracker.get(message_id)
            if existing is None or total_tokens > existing[1]:
                dedupe_tracker[message_id] = (entry, total_tokens)


def _resolve_agent_attribution(entries: List[UsageEntry]) -> List[UsageEntry]:
    """Resolve agent attribution via sourceToolAssistantUUID backtracking.

    Builds a uuid->agent_id mapping from assistant events, then for entries
    with source_tool_assistant_uuid but no agent_id, resolves the agent_id
    from the parent assistant event.
    """
    if not entries:
        return entries

    # Build uuid -> agent_id mapping from assistant events
    uuid_to_agent_id: Dict[str, str] = {}
    for entry in entries:
        if entry.agent_id and entry.raw_ref:
            uuid_to_agent_id[entry.raw_ref] = entry.agent_id

    # Apply backtracking and set scope
    for entry in entries:
        # Set scope based on attribution
        if entry.is_sidechain:
            entry.scope = "subagent"
        elif entry.agent_id:
            entry.scope = "primary_agent"
        else:
            entry.scope = "unknown"

        # Backtrack via sourceToolAssistantUUID if agent_id is missing
        if not entry.agent_id and entry.source_tool_assistant_uuid:
            parent_agent_id = uuid_to_agent_id.get(entry.source_tool_assistant_uuid)
            if parent_agent_id:
                entry.agent_id = parent_agent_id
                entry.scope = "subagent"  # Backtracked entries are subagent scope

    return entries


def _apply_message_id_max_dedup(
    entries: List[UsageEntry],
    dedupe_tracker: Dict[str, Tuple[UsageEntry, int]],
) -> List[UsageEntry]:
    """Apply message-id-max deduplication: keep only best entry per message_id.

    Returns entries sorted by timestamp with duplicates removed based on
    message_id, keeping the entry with highest total_tokens for each message_id.
    """
    if not dedupe_tracker:
        return entries

    # Build final list from tracker (entries with highest tokens per message_id)
    # dedupe_tracker maps message_id -> (best_entry, total_tokens)
    deduped = list(dedupe_tracker.values())
    # dedupe_tracker.values() returns tuples of (entry, total_tokens)
    # We need just the entries
    deduped_entries = [item[0] for item in deduped]

    # Sort by timestamp to maintain order
    deduped_entries.sort(key=lambda e: e.timestamp)

    logger.debug(
        f"message-id-max dedup: {len(entries)} entries -> {len(deduped_entries)} entries"
    )

    return deduped_entries


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

        # Extract Agent/Subagent attribution fields
        is_sidechain = bool(data.get("isSidechain", False))
        agent_id = data.get("agentId") or data.get("agent_id") or ""
        source_tool_assistant_uuid = data.get("sourceToolAssistantUUID") or ""
        raw_ref = data.get("uuid", "")

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
            is_sidechain=is_sidechain,
            agent_id=agent_id,
            source_tool_assistant_uuid=source_tool_assistant_uuid,
            raw_ref=raw_ref,
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
