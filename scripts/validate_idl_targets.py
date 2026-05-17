#!/usr/bin/env python3
from __future__ import annotations

import sys

from app.config import Settings
from app.validation import build_validation_report


def main() -> int:
    settings = Settings()
    report = build_validation_report(settings)

    print("Known IDL program IDs:")
    if report.known_program_ids:
        for program_id in report.known_program_ids:
            print(f"- {program_id}")
    else:
        print("- (none found)")

    if report.is_valid:
        print("\nValidation passed: all configured program IDs are represented in local IDLs.")
        return 0

    print("\nValidation failed: missing configured program IDs in local IDLs:")
    for program_id in report.missing_program_ids:
        print(f"- {program_id}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
