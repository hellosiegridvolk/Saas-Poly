"""Preflight — fail-fast config drift detector. See ADR 0004.

Behaviour:
  1. Read `PARAMS_PRESET` from env (no preset → exit 0 with a noop status).
  2. Load the preset file.
  3. Compare each RISK_CRITICAL_KEYS value between preset and live env.
  4. Drift → exit 2 with a JSON drift report on stdout.
  5. No drift → exit 0 with the resolved fingerprint.

This script runs at every boot, BEFORE agents are constructed. Operators
can also run it standalone:

    python -m poly_terminal.scripts.preflight
    echo "exit=$?"
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

from poly_terminal.config.fingerprint import compute_fingerprint, drift_against
from poly_terminal.config.preset_loader import PresetError, load_preset

_DEFAULT_PRESETS_DIR = (
    Path(__file__).resolve().parents[1] / "config" / "presets"
)


def _emit(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, sort_keys=True, indent=2))


def main(argv: list[str] | None = None) -> int:
    _ = argv  # placeholder for future CLI flags
    preset_name = os.environ.get("PARAMS_PRESET", "").strip()
    if not preset_name:
        _emit({"status": "ok", "preset": None, "reason": "no PARAMS_PRESET set"})
        return 0

    try:
        expected = load_preset(preset_name, presets_dir=_DEFAULT_PRESETS_DIR)
    except PresetError as exc:
        _emit({"status": "fail", "preset": preset_name, "error": str(exc)})
        return 2

    drift = drift_against(expected, dict(os.environ))
    if drift:
        _emit(
            {
                "status": "fail",
                "preset": preset_name,
                "drift": drift,
                "hint": (
                    "Resolve drift by aligning .env with the preset, or by "
                    "selecting a different PARAMS_PRESET. Risk-critical keys "
                    "must not be tuned via .env edits — see docs/adrs/0004."
                ),
            }
        )
        return 2

    _emit(
        {
            "status": "ok",
            "preset": preset_name,
            "fingerprint": compute_fingerprint(dict(os.environ)),
        }
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
