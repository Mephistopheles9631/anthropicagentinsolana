from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass(slots=True)
class FilteredEvent:
    program_name: str
    program_id: str
    idl_label: str | None
    idl_instruction: str | None
    idl_event: str | None
    slot: int | None
    signature: str | None
    source: str
    raw_json: str
    ingested_at: datetime

    @staticmethod
    def now_utc() -> datetime:
        return datetime.now(timezone.utc)
