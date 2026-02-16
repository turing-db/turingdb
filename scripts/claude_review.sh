#!/usr/bin/env bash
#
# claude_review.sh — Run Claude Code to review C++ changes.
#
# Usage:
#   USE_CLAUDE_CODE=1 ./scripts/claude_review.sh  # local: uses your Claude Code login
#   ./scripts/claude_review.sh                    # CI: requires ANTHROPIC_API_KEY
#   BASE_REF=develop ./scripts/claude_review.sh   # diff against a different branch
#
# Environment variables (all have sensible defaults for local use):
#   BASE_REF          — base branch to diff against (default: main)
#   USE_CLAUDE_CODE   — set to 1 to use your local Claude Code login
#                       instead of ANTHROPIC_API_KEY (default: 0)
#   ANTHROPIC_API_KEY — required unless USE_CLAUDE_CODE=1
#   GITHUB_OUTPUT     — GitHub Actions output file (default: /dev/null)
#
# In CI the script writes machine-readable outputs to $GITHUB_OUTPUT.
# Locally it just prints the review to stdout.

set -euo pipefail

BASE_REF="${BASE_REF:-main}"
USE_CLAUDE_CODE="${USE_CLAUDE_CODE:-0}"
GITHUB_OUTPUT="${GITHUB_OUTPUT:-/dev/null}"

if [ "$USE_CLAUDE_CODE" != "1" ] && [ -z "${ANTHROPIC_API_KEY:-}" ]; then
    echo "Error: ANTHROPIC_API_KEY must be set (or use USE_CLAUDE_CODE=1)." >&2
    exit 1
fi

# ── Helper ──────────────────────────────────────────────────────────
gh_output() { echo "$1" >> "$GITHUB_OUTPUT"; }

# ── 1. Generate C++ diff ────────────────────────────────────────────
diff_output=$(git diff "origin/${BASE_REF}..HEAD" -- '*.cpp' '*.h' '*.hpp') || true

if [ -z "$diff_output" ]; then
    gh_output "skip=true"
    gh_output "has_violations=false"
    gh_output "review_body="
    echo "No C++ file changes found."
    exit 0
fi

echo "$diff_output" | head -c 153600 > /tmp/cpp_diff.patch
gh_output "skip=false"

# ── 2. Read style guides from the base branch ──────────────────────
git show "origin/${BASE_REF}:CODING_STYLE.md" > /tmp/CODING_STYLE.md 2>/dev/null \
    || echo "No CODING_STYLE.md on base branch." > /tmp/CODING_STYLE.md

git show "origin/${BASE_REF}:REVIEW.md" > /tmp/REVIEW.md 2>/dev/null \
    || echo "No REVIEW.md on base branch." > /tmp/REVIEW.md

# ── 3. Build review prompt ──────────────────────────────────────────
cat > /tmp/review_prompt.txt << 'PROMPT_HEADER'
You are an expert C++ code reviewer. Review the following diff against the
coding style guide and the review checklist provided below.

RULES:
- Only flag lines that were ADDED in the diff (lines starting with +).
- Only flag CLEAR violations — do not speculate or flag ambiguous cases.
- For each violation, cite the specific rule from CODING_STYLE.md or REVIEW.md.
- If there are NO violations, output exactly the word NO_VIOLATIONS and nothing else.
- If there ARE violations, output a markdown table with these columns:
  | File | Line | Rule | Violation |
- Do NOT include any other text before or after the table (no preamble, no summary).

PROMPT_HEADER

{
    echo ""
    echo "## CODING_STYLE.md"
    echo '```'
    cat /tmp/CODING_STYLE.md
    echo '```'
    echo ""
    echo "## REVIEW.md"
    echo '```'
    cat /tmp/REVIEW.md
    echo '```'
    echo ""
    echo "## Diff to review"
    echo '```diff'
    cat /tmp/cpp_diff.patch
    echo '```'
} >> /tmp/review_prompt.txt

# ── 4. Run Claude review ──────────────────────────────────────────
claude_args=(-p --model claude-opus-4-6)
if [ "$USE_CLAUDE_CODE" != "1" ]; then
    claude_args+=(--api-key "$ANTHROPIC_API_KEY")
fi
(cd /tmp && claude "${claude_args[@]}" < /tmp/review_prompt.txt > /tmp/claude_review_output.txt)

# ── 5. Parse Claude output ─────────────────────────────────────────
output=$(cat /tmp/claude_review_output.txt)

if echo "$output" | grep -q "NO_VIOLATIONS"; then
    gh_output "has_violations=false"
    gh_output "review_body="
    echo "No violations found."
else
    encoded=$(echo "$output" | base64 -w0 2>/dev/null || echo "$output" | base64)
    gh_output "has_violations=true"
    gh_output "review_body=$encoded"
    echo "Violations found:"
    echo ""
    echo "$output"
fi
