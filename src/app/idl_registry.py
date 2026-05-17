from __future__ import annotations

import base64
import binascii
import json
from dataclasses import dataclass
from pathlib import Path


DISCRIMINATOR_KEYS = {
    "discriminator",
    "instruction_discriminator",
    "event_discriminator",
    "ix_discriminator",
    "ix_data",
    "instruction_data",
    "data",
}

INSTRUCTION_NAME_KEYS = {
    "instruction",
    "instruction_name",
    "ix_name",
    "name",
}

EVENT_NAME_KEYS = {
    "event",
    "event_name",
    "log_name",
}


@dataclass(slots=True)
class IdlProgram:
    file_name: str
    label: str
    program_id: str
    instruction_by_discriminator: dict[bytes, str]
    event_by_discriminator: dict[bytes, str]
    instruction_names: set[str]
    event_names: set[str]


@dataclass(slots=True)
class DecodedIdlLabel:
    idl_label: str | None
    instruction_name: str | None
    event_name: str | None


class IdlRegistry:
    def __init__(self, by_program_id: dict[str, IdlProgram]) -> None:
        self._by_program_id = by_program_id

    @property
    def program_ids(self) -> set[str]:
        return set(self._by_program_id.keys())

    @classmethod
    def from_directory(cls, idl_dir: str, program_id_map: dict[str, str] | None = None) -> "IdlRegistry":
        root = Path(idl_dir)
        if not root.exists() or not root.is_dir():
            return cls({})

        program_id_map = program_id_map or {}
        by_program_id: dict[str, IdlProgram] = {}

        for idl_file in sorted(root.glob("*.json")):
            payload = _load_json(idl_file)
            if payload is None:
                continue

            program_id = _resolve_program_id(payload, idl_file.name, program_id_map)
            if not program_id:
                continue

            label = _program_label(payload, idl_file)
            instruction_names, instruction_disc = _extract_idl_entries(payload.get("instructions"), "instruction")
            event_names, event_disc = _extract_idl_entries(payload.get("events"), "event")

            by_program_id[program_id] = IdlProgram(
                file_name=idl_file.name,
                label=label,
                program_id=program_id,
                instruction_by_discriminator=instruction_disc,
                event_by_discriminator=event_disc,
                instruction_names=instruction_names,
                event_names=event_names,
            )

        return cls(by_program_id)

    def decode(self, program_id: str, payload: dict) -> DecodedIdlLabel:
        idl = self._by_program_id.get(program_id)
        if idl is None:
            return DecodedIdlLabel(idl_label=None, instruction_name=None, event_name=None)

        instruction_name = _resolve_name(payload, INSTRUCTION_NAME_KEYS, idl.instruction_names)
        event_name = _resolve_name(payload, EVENT_NAME_KEYS, idl.event_names)

        if instruction_name is None or event_name is None:
            for discriminator in _extract_discriminators(payload):
                if instruction_name is None:
                    instruction_name = idl.instruction_by_discriminator.get(discriminator)
                if event_name is None:
                    event_name = idl.event_by_discriminator.get(discriminator)
                if instruction_name is not None and event_name is not None:
                    break

        return DecodedIdlLabel(
            idl_label=idl.label,
            instruction_name=instruction_name,
            event_name=event_name,
        )


def validate_tracked_programs(tracked_program_lookup: dict[str, str], registry: IdlRegistry) -> list[str]:
    return sorted(program_id for program_id in tracked_program_lookup if program_id not in registry.program_ids)


def _load_json(path: Path) -> dict | None:
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _resolve_program_id(payload: dict, file_name: str, program_id_map: dict[str, str]) -> str | None:
    for key in ("address", "programId"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    metadata = payload.get("metadata")
    if isinstance(metadata, dict):
        for key in ("address", "programId"):
            value = metadata.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()

    mapped = program_id_map.get(file_name)
    if isinstance(mapped, str) and mapped.strip():
        return mapped.strip()
    return None


def _program_label(payload: dict, idl_file: Path) -> str:
    metadata = payload.get("metadata")
    if isinstance(metadata, dict):
        name = metadata.get("name")
        if isinstance(name, str) and name.strip():
            return name.strip()

    name = payload.get("name")
    if isinstance(name, str) and name.strip():
        return name.strip()

    return idl_file.stem


def _extract_idl_entries(entries: object, kind: str) -> tuple[set[str], dict[bytes, str]]:
    names: set[str] = set()
    by_discriminator: dict[bytes, str] = {}

    if not isinstance(entries, list):
        return names, by_discriminator

    for entry in entries:
        if not isinstance(entry, dict):
            continue

        name = entry.get("name")
        if isinstance(name, str) and name:
            names.add(name)

            discriminator = entry.get("discriminator")
            bytes_disc = _normalize_discriminator(discriminator)
            if bytes_disc is not None:
                by_discriminator[bytes_disc] = name

        # Meteora-style IDL stores event/instruction shape under fieldType.
        if kind == "event" and isinstance(name, str) and name:
            names.add(name)

    return names, by_discriminator


def _resolve_name(payload: object, keys: set[str], allowed: set[str]) -> str | None:
    if not allowed:
        return None

    for key, value in _deep_key_values(payload):
        if key in keys and isinstance(value, str) and value in allowed:
            return value
    return None


def _extract_discriminators(payload: object) -> list[bytes]:
    out: list[bytes] = []
    for key, value in _deep_key_values(payload):
        if key not in DISCRIMINATOR_KEYS:
            continue
        normalized = _normalize_discriminator(value)
        if normalized is not None:
            out.append(normalized)

    return out


def _normalize_discriminator(value: object) -> bytes | None:
    if isinstance(value, list) and value and all(isinstance(item, int) for item in value):
        try:
            raw = bytes(value)
        except ValueError:
            return None
        return raw[:8] if len(raw) >= 8 else None

    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None

        if stripped.startswith("0x"):
            stripped = stripped[2:]

        try:
            raw = binascii.unhexlify(stripped)
            if len(raw) >= 8:
                return raw[:8]
        except (binascii.Error, ValueError):
            pass

        try:
            raw = base64.b64decode(stripped, validate=True)
            if len(raw) >= 8:
                return raw[:8]
        except (binascii.Error, ValueError):
            return None

    return None


def _deep_key_values(payload: object):
    if isinstance(payload, dict):
        for key, value in payload.items():
            key_lower = key.lower() if isinstance(key, str) else ""
            yield key_lower, value
            yield from _deep_key_values(value)
    elif isinstance(payload, list):
        for value in payload:
            yield from _deep_key_values(value)
