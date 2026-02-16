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

# ── 2. Read style guides from HEAD (the branch being reviewed) ─────
git show "HEAD:CODING_STYLE.md" > /tmp/CODING_STYLE.md 2>/dev/null \
    || echo "No CODING_STYLE.md found." > /tmp/CODING_STYLE.md

git show "HEAD:REVIEW.md" > /tmp/REVIEW.md 2>/dev/null \
    || echo "No REVIEW.md found." > /tmp/REVIEW.md

# ── 3. Build review prompt ──────────────────────────────────────────
cat > /tmp/review_prompt.txt << 'PROMPT_HEADER'
You are an expert C++ code reviewer. Review the following diff against the
coding style guide and the review checklist provided below.

RULES:
- Only flag lines that were ADDED in the diff (lines starting with +).
- For each violation, cite the specific rule from CODING_STYLE.md or REVIEW.md.
- Also flag consistency issues: e.g. switch statements where cases mix
different termination styles (some use break, others return). These are
valid findings even if they are not purely formatting-related.
- The bracket initialization rule for member variables applies to types that
  would otherwise be left uninitialized (primitives, pointers, enums). Class
  and struct types with default constructors (e.g. std::string, std::vector)
  are self-initializing and do NOT require explicit {} initialization.

VERIFICATION:
- Before reporting any violation, re-read the exact diff lines to confirm it.
- Pay special attention to multi-line constructs (constructors, function
signatures, initializer lists). A brace '{' on the NEXT line in the diff
is on a separate line — do not confuse consecutive diff lines with being
on the same source line.

COMMON FALSE POSITIVES TO AVOID:
- Template methods MUST be defined in headers (C++ language requirement) — do
not flag them as "big implementation code in headers."
- One-liner getters, setters, and accessors in headers are NOT "big
implementation code."
- Pointers, bools, enums, and other POD/trivial types are NOT "non-trivial
member variables" — classes with only these members do not need explicit
constructors/destructors in a cpp file.
- Multi-line function arguments: both parenthesis-aligned and flat/uniform
indentation are acceptable, as long as each argument is on its own line
and consistently indented below the others. Do NOT flag flat indentation
as a violation of the alignment rule.

OUTPUT FORMAT (follow these three phases in order):

1. <analysis> — Analyze the diff. Reason freely, identify candidate
   violations, and discard false positives.

2. <review> — Re-examine every candidate from the analysis. For each one,
   verify that (a) the line was actually ADDED in the diff (starts with +),
   and (b) the cited rule exists in CODING_STYLE.md or REVIEW.md.
   Only drop a candidate if it is clearly a false positive — i.e. the rule
   does not actually apply to the flagged code. When in doubt, keep it.

3. <answer> — Output only the surviving violations:
   - If none survive, write exactly: <answer>NO_VIOLATIONS</answer>
   - Otherwise, write a markdown table:
     <answer>
     | File | Line | Rule | Violation |
     |------|------|------|-----------|
     | ...  | ...  | ...  | ...       |
     </answer>

Do NOT include any text outside of these three tags.

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

# ── 5. Extract <answer> and parse ────────────────────────────────────
raw=$(cat /tmp/claude_review_output.txt)
answer=$(sed -n '/<answer>/,/<\/answer>/p' /tmp/claude_review_output.txt \
    | sed '1s/.*<answer>//; $s/<\/answer>.*//')

if [ -z "$answer" ] || echo "$answer" | grep -q "NO_VIOLATIONS"; then
    gh_output "has_violations=false"
    gh_output "review_body="
    echo "No violations found."
else
    encoded=$(echo "$answer" | base64 -w0 2>/dev/null || echo "$answer" | base64)
    gh_output "has_violations=true"
    gh_output "review_body=$encoded"
    echo "Violations found:"
    echo ""
    echo "$answer"
fi
