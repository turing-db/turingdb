Vendored, unmodified, from https://github.com/mattpocock/skills
(`skills/engineering/pr/`), MIT. Fetched 2026-10-13.

Re-sync:

    gh api repos/mattpocock/skills/contents/skills/engineering/pr/SKILL.md --jq .content | base64 -d > ~/.claude/skills/pr/SKILL.md

The full `mattpocock-skills` plugin carries 27 skills. Only `pr` is vendored here, because
every installed skill's description costs context in every session. To take all of them
instead: `/plugin marketplace add mattpocock/skills`, then install `mattpocock-skills`, and
delete this directory.
