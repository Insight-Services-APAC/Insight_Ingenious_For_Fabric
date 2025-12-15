"""Parameter resolution for exports."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo


@dataclass
class Params:
    """Parameters for pattern resolution.

    All date/time fields are datetime for maximum flexibility.
    Users can format to date-only using strftime if needed.

    Attributes:
        run_date: Logical date for the export. Defaults to today but can be
            overridden for backfills.
        process_date: Actual time the pipeline ran. Always "now" in the
            configured timezone. Should be set by the caller.
        timezone: Timezone string (e.g., "Australia/Sydney"). Used as fallback
            if process_date is not explicitly set.
    """

    run_date: Optional[datetime] = None
    process_date: Optional[datetime] = None
    period_start_date: Optional[datetime] = None
    period_end_date: Optional[datetime] = None
    export_name: Optional[str] = None
    run_id: Optional[str] = None
    watermark: Optional[str] = None
    part: Optional[int] = None
    timezone: Optional[str] = None


def resolve_params(pattern: str, params: Params) -> str:
    """Resolve {name} and {name:format} placeholders in pattern.

    Datetime parameters support strftime format specifiers.
    Without format specifier, datetimes use ISO format (YYYY-MM-DD HH:MM:SS).

    Args:
        pattern: String with placeholders to resolve
        params: Params dataclass with values

    Returns:
        Pattern with placeholders replaced

    Examples:
        {run_date} → 2025-12-12 14:30:22
        {run_date:%Y-%m-%d} → 2025-12-12 (date only)
        {run_date:%Y%m%d} → 20251212
        {process_date:%H%M%S} → 143022
    """
    ISO_DATETIME = "%Y-%m-%d %H:%M:%S"

    # Determine process_date: use provided value or current time in configured timezone
    if params.process_date:
        process_date = params.process_date
    elif params.timezone:
        tz = ZoneInfo(params.timezone)
        process_date = datetime.now(tz)
    else:
        process_date = datetime.now()

    # Date values (support format specifiers)
    date_values: dict[str, datetime | None] = {
        "run_date": params.run_date,
        "process_date": process_date,
        "period_start_date": params.period_start_date,
        "period_end_date": params.period_end_date,
    }

    # Step 1: Replace {name:format} patterns
    def replace_formatted(match: re.Match) -> str:
        name, fmt = match.group(1), match.group(2)
        val = date_values.get(name)
        if val is not None:
            return val.strftime(fmt)
        return match.group(0)

    result = re.sub(r"\{(\w+):([^}]+)\}", replace_formatted, pattern)

    # Step 2: Replace {name} datetime patterns with ISO default
    def replace_date_default(match: re.Match) -> str:
        name = match.group(1)
        val = date_values.get(name)
        if val:
            return val.strftime(ISO_DATETIME)
        return match.group(0)

    result = re.sub(
        r"\{(run_date|process_date|period_start_date|period_end_date)\}",
        replace_date_default,
        result,
    )

    # Step 3: Simple string replacements
    if params.export_name:
        result = result.replace("{export_name}", params.export_name)
    if params.run_id:
        result = result.replace("{run_id}", params.run_id[:8])
    if params.watermark:
        result = result.replace("{watermark}", params.watermark)
    if params.part is not None:
        result = result.replace("{part}", str(params.part))

    return result
