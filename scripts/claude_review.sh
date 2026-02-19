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
#   REVIEW_PASSES     — number of independent review passes (default: 3)
#   REVIEW_THRESHOLD  — minimum passes a violation must appear in (default: 2)
#   REVIEW_MODEL      — model for review passes (default: claude-sonnet-4-6)
#
# The script runs REVIEW_PASSES independent reviews in parallel and only
# reports violations found in at least REVIEW_THRESHOLD passes (consensus
# voting).  This eliminates flaky/non-deterministic findings and makes the
# review converge much faster across iterations.
#
# In CI the script writes machine-readable outputs to $GITHUB_OUTPUT.
# Locally it just prints the review to stdout.

set -euo pipefail

BASE_REF="${BASE_REF:-main}"
USE_CLAUDE_CODE="${USE_CLAUDE_CODE:-0}"
GITHUB_OUTPUT="${GITHUB_OUTPUT:-/dev/null}"
REVIEW_PASSES="${REVIEW_PASSES:-3}"
REVIEW_THRESHOLD="${REVIEW_THRESHOLD:-2}"
REVIEW_MODEL="${REVIEW_MODEL:-claude-sonnet-4-6}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ "$USE_CLAUDE_CODE" != "1" ] && [ -z "${ANTHROPIC_API_KEY:-}" ]; then
    echo "Error: ANTHROPIC_API_KEY must be set (or use USE_CLAUDE_CODE=1)." >&2
    exit 1
fi

# ── Helper ──────────────────────────────────────────────────────────
gh_output() { echo "$1" >> "$GITHUB_OUTPUT"; }

# ── 1. Generate C++ diff ────────────────────────────────────────────
# Use origin/ prefix for branch names; bare ref for commit hashes
if git rev-parse --verify "origin/${BASE_REF}" >/dev/null 2>&1; then
    DIFF_BASE="origin/${BASE_REF}"
else
    DIFF_BASE="${BASE_REF}"
fi
diff_output=$(git diff "${DIFF_BASE}..HEAD" -- '*.cpp' '*.h' '*.hpp') || true

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
- Constructor brace placement: a constructor whose opening brace '{' appears
on its own line is correctly formatted, even when the constructor is defined
inline inside a class body. Do not confuse an inline constructor definition
with a regular method — the constructor brace rule always applies.
- Constructors and destructors have DIFFERENT brace rules. Constructors put
the opening brace on the NEXT line. Destructors put it on the SAME line,
like any other method: `MyClass::~MyClass() {`. Do NOT confuse a correctly
formatted destructor with a constructor violation or vice versa.
- Const correctness for pointers: before flagging a non-const pointer variable,
verify that ALL functions it is passed to accept a const pointer. If any
callee takes a non-const pointer (e.g. `void foo(Expr* e)`), the variable
CANNOT be declared const. Only flag when the variable is truly never passed
to a non-const parameter and is never modified.
- std::function parameters: `std::function` is a standard library callable type
and MAY be passed by (const) reference, just like other STL types. Do NOT flag
`const std::function<...>&` as a violation of the pointer-passing rule.
- Small status/result structs by reference: small, lightweight status or result
structs (e.g. status codes, error wrappers, expected-style results) MAY be
passed by const reference. Do NOT flag these as pointer-passing violations.
- Break after return in switch cases: a `break;` statement after a `return;`
inside a switch case is NOT unnecessary clutter — it is intentional for
coherence and consistency across all cases. Do NOT flag it.
- Returning small result/status types: small, lightweight result or status types
(status codes, error wrappers, expected-style results) MAY be returned by
value. Do NOT flag these as "returning non-trivial objects."

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

# ── 4. Run Claude review (multi-pass consensus) ─────────────────────
claude_args=(-p --model "$REVIEW_MODEL")

echo "Running $REVIEW_PASSES review passes (model: $REVIEW_MODEL, threshold: $REVIEW_THRESHOLD)..."

# Ensure output token limit is high enough for large diffs
export CLAUDE_CODE_MAX_OUTPUT_TOKENS="${CLAUDE_CODE_MAX_OUTPUT_TOKENS:-128000}"

# Clean up previous pass files
rm -f /tmp/claude_review_pass_*.txt /tmp/violations_pass_*.txt

# Unset Claude Code session variables so child processes start clean
unset CLAUDECODE CLAUDE_CODE_ENTRYPOINT CLAUDE_CODE CLAUDE_CODE_SESSION CLAUDE_CONVERSATION_ID 2>/dev/null || true

# Launch all passes in parallel
pids=()
for i in $(seq 1 "$REVIEW_PASSES"); do
    (cd /tmp && claude "${claude_args[@]}" < /tmp/review_prompt.txt \
        > "/tmp/claude_review_pass_${i}.txt" 2>&1) &
    pids+=($!)
done

# Wait for all passes and track failures
pass_failures=0
for pid in "${pids[@]}"; do
    if ! wait "$pid"; then
        ((pass_failures++))
    fi
done

if [ "$pass_failures" -eq "$REVIEW_PASSES" ]; then
    echo "Error: all $REVIEW_PASSES review passes failed." >&2
    exit 1
fi

# ── 5. Extract <answer> from each pass ──────────────────────────────
for i in $(seq 1 "$REVIEW_PASSES"); do
    outfile="/tmp/claude_review_pass_${i}.txt"
    if [ ! -s "$outfile" ]; then
        echo "NO_VIOLATIONS" > "/tmp/violations_pass_${i}.txt"
        continue
    fi
    sed -n '/<answer>/,/<\/answer>/p' "$outfile" \
        | sed '1s/.*<answer>//; $s/<\/answer>.*//' \
        > "/tmp/violations_pass_${i}.txt"
    # If extraction produced nothing, treat as no violations
    if [ ! -s "/tmp/violations_pass_${i}.txt" ]; then
        echo "NO_VIOLATIONS" > "/tmp/violations_pass_${i}.txt"
    fi
done

# Print per-pass summary
for i in $(seq 1 "$REVIEW_PASSES"); do
    vfile="/tmp/violations_pass_${i}.txt"
    if grep -q "NO_VIOLATIONS" "$vfile"; then
        echo "  Pass $i: no violations"
    else
        count=$(grep -c '^|' "$vfile" | head -1)
        # Subtract header and separator rows
        count=$((count > 2 ? count - 2 : 0))
        echo "  Pass $i: $count violation(s)"
    fi
done

# ── 6. Consensus voting ─────────────────────────────────────────────
consensus=$(python3 "$SCRIPT_DIR/review_consensus.py" \
    --threshold "$REVIEW_THRESHOLD" \
    /tmp/violations_pass_*.txt)

if [ -z "$consensus" ] || echo "$consensus" | grep -q "NO_VIOLATIONS"; then
    gh_output "has_violations=false"
    gh_output "review_body="
    echo ""
    echo "No violations survived consensus ($REVIEW_PASSES passes, threshold >= $REVIEW_THRESHOLD)."
else
    encoded=$(echo "$consensus" | base64 -w0 2>/dev/null || echo "$consensus" | base64)
    gh_output "has_violations=true"
    gh_output "review_body=$encoded"
    echo ""
    echo "Consensus violations ($REVIEW_PASSES passes, threshold >= $REVIEW_THRESHOLD):"
    echo ""
    echo "$consensus"
fi
