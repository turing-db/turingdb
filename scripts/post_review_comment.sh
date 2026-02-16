#!/usr/bin/env bash
#
# post_review_comment.sh — Post (or clean up) a Claude review comment on a PR.
#
# Required env vars:
#   GH_TOKEN          — GitHub token (used by `gh`)
#   GITHUB_REPOSITORY — owner/repo slug
#   PR_NUMBER         — pull request number
#   HEAD_REF          — head branch name (for links)
#   HAS_VIOLATIONS    — "true" / "false"
#   REVIEW_BODY       — base64-encoded review markdown (only when violations)

set -euo pipefail

# ── 1. Delete previous Claude review comments ──────────────────────
gh api "repos/${GITHUB_REPOSITORY}/issues/${PR_NUMBER}/comments" \
    --paginate --jq '.[] | select(.body | contains("<!-- claude-code-review -->")) | .id' \
    | while read -r comment_id; do
        echo "Deleting previous Claude review comment $comment_id"
        gh api -X DELETE "repos/${GITHUB_REPOSITORY}/issues/comments/${comment_id}"
      done

# ── 2. Post new comment (only when there are violations) ───────────
if [ "$HAS_VIOLATIONS" = "true" ]; then
    review_body=$(echo "$REVIEW_BODY" | base64 -d)

    cat > /tmp/comment_body.md << 'EOF'
<!-- claude-code-review -->
## Claude Code Review

EOF

    echo "$review_body" >> /tmp/comment_body.md

    cat >> /tmp/comment_body.md << EOF

---
*Checked against [CODING_STYLE.md](../blob/${HEAD_REF}/CODING_STYLE.md) and [REVIEW.md](../blob/${HEAD_REF}/REVIEW.md).*
EOF

    gh pr comment "$PR_NUMBER" --body-file /tmp/comment_body.md

    echo "::error::Claude Code review found style violations. Please fix them before merging."
    exit 1
fi
