from __future__ import annotations

from dataclasses import dataclass

from .config import Settings
from .idl_registry import IdlRegistry, validate_tracked_programs


@dataclass(slots=True)
class ValidationReport:
    missing_program_ids: list[str]
    known_program_ids: list[str]

    @property
    def is_valid(self) -> bool:
        return not self.missing_program_ids


def build_validation_report(settings: Settings) -> ValidationReport:
    registry = IdlRegistry.from_directory(settings.idl_dir, settings.idl_program_id_map)
    missing_program_ids = validate_tracked_programs(settings.tracked_program_lookup, registry)
    return ValidationReport(
        missing_program_ids=missing_program_ids,
        known_program_ids=sorted(registry.program_ids),
    )
