#!/bin/bash
# PostToolUse hook (Read): records that the project-root TESTING.md was read
# in full this session. This clears the test-file edit gate in
# require-testing-md.sh. A Read with an offset or a limit does not count.
# Without jq this hook does nothing, because require-testing-md.sh then
# blocks every edit.
set -u
command -v jq >/dev/null 2>&1 || exit 0
input=$(cat)
file=$(printf '%s' "$input" | jq -r '.tool_input.file_path // empty')
[ "$file" = "${CLAUDE_PROJECT_DIR:-}/TESTING.md" ] || exit 0
printf '%s' "$input" | jq -e '.tool_input.offset == null and .tool_input.limit == null' >/dev/null || exit 0
sid=$(printf '%s' "$input" | jq -r '.session_id // "global"')
touch "${TMPDIR:-/tmp}/claude-testing-md-read-$sid"
