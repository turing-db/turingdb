#!/usr/bin/env bash
#
# sanitizer_agent.sh — Run Claude on the sanitizer nightly's failures.
#
# Runs /sanitizer-triage with Opus 5.5 at xhigh effort. The agent commits each simple fix on
# a stacked sanitizers/* branch and files a Linear ticket for each other finding. This script
# then checks the stack and bundles it into $SANITIZER_OUT/fixes.bundle for the job that
# opens the PRs.
#
# Environment:
#   ANTHROPIC_API_KEY — Claude API key
#   LINEAR_API_KEY    — Linear API key, sent to Linear's MCP server
#   GH_TOKEN          — read-only GitHub token
#   SANITIZER_OUT     — directory for the agent's summary, manifest and transcript; only the
#                       summary, manifest and bundle may be uploaded, the transcript holds
#                       Linear data
#   SANITIZER_RUN_URL — URL of the workflow run
#   GITHUB_OUTPUT     — receives has_prs=true|false (default: /dev/null)

set -euo pipefail

: "${ANTHROPIC_API_KEY:?}" "${LINEAR_API_KEY:?}" "${SANITIZER_OUT:?}" "${SANITIZER_RUN_URL:?}"
GITHUB_OUTPUT="${GITHUB_OUTPUT:-/dev/null}"
GITHUB_STEP_SUMMARY="${GITHUB_STEP_SUMMARY:-/dev/null}"

mkdir -p "$SANITIZER_OUT"
base=$(git rev-parse HEAD)

# The self-hosted runners keep .git between jobs, so an earlier night's branches are still there.
git for-each-ref --format='%(refname:short)' refs/heads/sanitizers/ | xargs -r git branch -D

mcp_config="$SANITIZER_OUT/mcp.json"
cat > "$mcp_config" << 'EOF'
{
  "mcpServers": {
    "linear-server": {
      "type": "http",
      "url": "https://mcp.linear.app/mcp",
      "headers": { "Authorization": "Bearer ${LINEAR_API_KEY}" }
    }
  }
}
EOF

# The prompt goes first: --allowedTools takes every word after it until the next flag.
agent_status=0
timeout 170m claude -p "/sanitizer-triage build/Testing/Temporary/LastTest.log $SANITIZER_OUT $SANITIZER_RUN_URL" \
    --allowedTools Bash Read Write Edit Glob Grep TodoWrite mcp__linear-server \
    --model claude-opus-5-5 \
    --effort xhigh \
    --permission-mode dontAsk \
    --add-dir "$SANITIZER_OUT" \
    --mcp-config "$mcp_config" \
    --strict-mcp-config \
    --settings '{"attribution": {"commit": "", "pr": ""}}' \
    --output-format stream-json \
    --verbose \
    < /dev/null > "$SANITIZER_OUT/transcript.jsonl" || agent_status=$?

if [ "$agent_status" -ne 0 ]; then
    echo "::error::The sanitizer agent exited with status $agent_status; its committed fixes are still bundled"
fi

if [ -s "$SANITIZER_OUT/summary.md" ]; then
    cat "$SANITIZER_OUT/summary.md"
    {
        echo "## Sanitizer agent"
        echo ""
        cat "$SANITIZER_OUT/summary.md"
    } >> "$GITHUB_STEP_SUMMARY"
fi

manifest="$SANITIZER_OUT/prs.tsv"
if [ ! -s "$manifest" ]; then
    echo "has_prs=false" >> "$GITHUB_OUTPUT"
    exit 0
fi

parent=$base
branches=()
line=0
while IFS=$'\t' read -r branch reviewer title || [ -n "$branch" ]; do
    line=$((line + 1))

    if ! [[ "$branch" =~ ^sanitizers/[A-Za-z0-9._-]+$ ]] || ! git rev-parse --verify --quiet "refs/heads/$branch" > /dev/null; then
        echo "::error::prs.tsv line $line: no branch named '$branch' under sanitizers/"
        exit 1
    elif ! [[ "$reviewer" =~ ^(-|[A-Za-z0-9-]+)$ ]] || [ -z "$title" ] || [ ! -s "$SANITIZER_OUT/pr-$line.md" ]; then
        echo "::error::prs.tsv line $line: needs a reviewer login or '-', a title, and pr-$line.md"
        exit 1
    elif ! git merge-base --is-ancestor "$parent" "$branch" || [ "$(git rev-list --count "$parent..$branch")" -eq 0 ]; then
        echo "::error::$branch does not stack on $parent"
        exit 1
    elif git diff --name-only "$parent" "$branch" | grep -q '^\.github/'; then
        echo "::error::$branch changes .github/"
        exit 1
    fi

    branches+=("$branch")
    parent=$branch
done < "$manifest"

git bundle create "$SANITIZER_OUT/fixes.bundle" "^$base" "${branches[@]}"
echo "has_prs=true" >> "$GITHUB_OUTPUT"
