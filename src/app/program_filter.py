from __future__ import annotations

from collections.abc import Iterable


PROGRAM_KEY_CANDIDATES = {
    "program",
    "program_id",
    "programid",
    "owner",
    "account_owner",
    "executing_program",
}


def deep_string_values(payload: object) -> Iterable[str]:
    if isinstance(payload, str):
        yield payload
        return

    if isinstance(payload, dict):
        for key, value in payload.items():
            if isinstance(key, str) and key.lower() in PROGRAM_KEY_CANDIDATES and isinstance(value, str):
                yield value
            yield from deep_string_values(value)
        return

    if isinstance(payload, list):
        for value in payload:
            yield from deep_string_values(value)


def find_tracked_program(payload: dict, tracked_programs: dict[str, set[str]]) -> tuple[str, str] | None:
    if not tracked_programs:
        return None

    tracked_by_id = {
        program_id: name for name, program_ids in tracked_programs.items() for program_id in program_ids
    }

    for candidate in deep_string_values(payload):
        if candidate in tracked_by_id:
            return tracked_by_id[candidate], candidate

    return None


def extract_slot(payload: dict) -> int | None:
    for key in ("slot", "current_slot", "block_slot"):
        value = payload.get(key)
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
    return None


def extract_signature(payload: dict) -> str | None:
    for key in ("signature", "tx_signature", "transaction_signature"):
        value = payload.get(key)
        if isinstance(value, str) and value:
            return value
    return None
