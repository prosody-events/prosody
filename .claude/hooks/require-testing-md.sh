#!/bin/bash
# PreToolUse hook (Edit|Write): blocks test-file edits until TESTING.md has
# been read in full this session. The PostToolUse Read hook
# (mark-testing-md-read.sh) drops the per-session marker that clears the gate.
# Without jq the hook cannot read the file path, so it blocks every edit.
set -u
if ! command -v jq >/dev/null 2>&1; then
  echo "The TESTING.md gate needs jq. Install jq (make bootstrap), then retry the edit." >&2
  exit 2
fi
input=$(cat)
file=$(printf '%s' "$input" | jq -r '.tool_input.file_path // empty')
[ -n "$file" ] || exit 0
printf '%s' "$file" | grep -qE '(^|/)tests?\.rs$|/tests/.*\.rs$|(^|/)test_[a-z_]+\.rs$' || exit 0
sid=$(printf '%s' "$input" | jq -r '.session_id // "global"')
marker="${TMPDIR:-/tmp}/claude-testing-md-read-$sid"
[ -f "$marker" ] && exit 0
printf '%s' '{"hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"deny","permissionDecisionReason":"Test-file edit blocked: TESTING.md has not been read in this session. Read TESTING.md at the repo root with the Read tool. Do not set offset or limit. Then retry this exact edit. The gate clears for the rest of the session once TESTING.md is read."}}'
