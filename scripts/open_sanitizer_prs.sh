#!/usr/bin/env bash
#
# open_sanitizer_prs.sh — Push the sanitizer agent's fix branches and open one PR per branch.
#
# Reads what sanitizer_agent.sh left in $SANITIZER_OUT: fixes.bundle, prs.tsv (branch,
# reviewer login or '-', title; bottom of the stack first) and pr-<k>.md (the body of line k).
# Each PR targets the previous branch; the first targets main.
#
# Environment:
#   SANITIZER_OUT     — directory holding the bundle and the manifest
#   GH_TOKEN          — token that opens the PRs and requests the reviews
#   DISPATCH_TOKEN    — token allowed to dispatch ci_build.yml
#   GITHUB_REPOSITORY — owner/repo

set -euo pipefail

: "${SANITIZER_OUT:?}" "${GH_TOKEN:?}" "${DISPATCH_TOKEN:?}" "${GITHUB_REPOSITORY:?}"

git fetch "$SANITIZER_OUT/fixes.bundle" '+refs/heads/sanitizers/*:refs/heads/sanitizers/*'

base=main
line=0
top_number=
while IFS=$'\t' read -r branch reviewer title || [ -n "$branch" ]; do
    line=$((line + 1))

    git push origin "refs/heads/$branch:refs/heads/$branch"

    url=$(gh pr create --repo "$GITHUB_REPOSITORY" \
                       --base "$base" \
                       --head "$branch" \
                       --title "$title" \
                       --body-file "$SANITIZER_OUT/pr-$line.md")
    number=${url##*/}
    echo "Opened $url"

    if [ "$reviewer" != "-" ]; then
        gh api -X POST "repos/$GITHUB_REPOSITORY/pulls/$number/requested_reviewers" -f "reviewers[]=$reviewer" > /dev/null \
            || echo "::warning::Could not request a review from $reviewer on #$number"
    fi

    base=$branch
    top_number=$number
done < "$SANITIZER_OUT/prs.tsv"

# ci_build.yml runs on PRs into main, and only the bottom PR targets main. Unless GitHub
# treats the PRs as a stack, run CI on the top branch, whose tree holds every fix.
if [ "$line" -gt 1 ]; then
    stack=$(gh api "repos/$GITHUB_REPOSITORY/pulls/$top_number" -q '.stack // empty')
    if [ -z "$stack" ]; then
        GH_TOKEN="$DISPATCH_TOKEN" gh workflow run ci_build.yml --repo "$GITHUB_REPOSITORY" --ref "$base"
    fi
fi
