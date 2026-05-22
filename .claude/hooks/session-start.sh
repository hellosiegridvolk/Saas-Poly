#!/bin/bash
# SessionStart hook: surfaces TODO.md to Claude at the start of every session.
# Silent on failure (missing file is fine — e.g. on the very first run).
set -uo pipefail

TODO_FILE="${CLAUDE_PROJECT_DIR:-$PWD}/TODO.md"

if [ -f "$TODO_FILE" ]; then
  printf '## TODO.md (Saas-Poly progress tracker)\n\n'
  cat "$TODO_FILE"
fi

exit 0
