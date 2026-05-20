"""Preset overlay loader — port of v2 `config/params_loader.py`, simplified.

v3 uses a flat `Settings` class (not nested-section YAML), so a preset is just
a mapping of env-var-name → string. The loader writes those env vars to
`os.environ` only when unset, then `Settings()` picks them up via the normal
pydantic-settings precedence.

Schema:

    version: 1
    overrides:
      MAX_POSITION_USD: "10"
      STRATEGY_COPY_TRADE: true
      ...

Precedence (highest wins): shell env > .env > preset > Settings defaults.
The preset NEVER overwrites a non-empty env var. Empty-string env vars are
treated as unset (mirrors v2 phase-11 fix — bare `KEY=` lines in .env
templates would otherwise veto the preset).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Final

import yaml

PRESET_SCHEMA_VERSION: Final[int] = 1
_ALLOWED_TOP_KEYS: Final[frozenset[str]] = frozenset({"version", "overrides"})


class PresetError(ValueError):
    """Raised when a preset is missing or malformed."""


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.is_file():
        msg = f"preset not found at {path}"
        raise PresetError(msg)
    raw = yaml.safe_load(path.read_text())
    if raw is None:
        msg = f"empty preset file: {path}"
        raise PresetError(msg)
    if not isinstance(raw, dict):
        msg = f"preset must be a YAML mapping at the top level, got {type(raw).__name__}"
        raise PresetError(msg)
    extra = set(raw) - _ALLOWED_TOP_KEYS
    if extra:
        msg = f"unexpected top-level keys in preset: {sorted(extra)}"
        raise PresetError(msg)
    if raw.get("version") != PRESET_SCHEMA_VERSION:
        msg = f"preset version {raw.get('version')!r} != schema {PRESET_SCHEMA_VERSION}"
        raise PresetError(msg)
    overrides = raw.get("overrides", {})
    if not isinstance(overrides, dict):
        msg = "preset 'overrides' must be a mapping"
        raise PresetError(msg)
    return overrides


def load_preset(
    name: str,
    presets_dir: str | Path = "src/poly_terminal/config/presets",
) -> dict[str, str]:
    """Read `<presets_dir>/<name>.yaml` and return the overrides as str→str."""
    path = Path(presets_dir) / f"{name}.yaml"
    raw_overrides = _read_yaml(path)
    return {str(k): str(v).lower() if isinstance(v, bool) else str(v)
            for k, v in raw_overrides.items()}


def apply_preset_to_env(
    name: str | None = None,
    presets_dir: str | Path = "src/poly_terminal/config/presets",
    env: dict[str, str] | None = None,
) -> int:
    """Apply preset values to `env` (default: `os.environ`).

    Behaviour:
      * `name=None` → read `env['PARAMS_PRESET']`; missing → no-op (returns 0).
      * Existing non-empty values in `env` are preserved.
      * Empty-string values in `env` are treated as unset and get overwritten.

    Returns the number of env vars actually written.
    """
    target = env if env is not None else os.environ
    preset_name = name if name is not None else target.get("PARAMS_PRESET")
    if not preset_name:
        return 0
    overrides = load_preset(preset_name, presets_dir=presets_dir)
    written = 0
    for key, value in overrides.items():
        existing = target.get(key)
        if existing is not None and existing != "":
            continue
        target[key] = value
        written += 1
    return written
