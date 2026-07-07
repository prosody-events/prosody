#!/bin/bash
# PostToolUse hook (Read): records that TESTING.md was read this session,
# clearing the test-file edit gate in require-testing-md.sh.
set -u
command -v jq >/dev/null 2>&1 || exit 0
input=$(cat)
file=$(printf '%s' "$input" | jq -r '.tool_input.file_path // empty')
case "$file" in
  */TESTING.md)
    sid=$(printf '%s' "$input" | jq -r '.session_id // "global"')
    touch "${TMPDIR:-/tmp}/claude-testing-md-read-$sid"
    ;;
esac
exit 0
