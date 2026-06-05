#!/usr/bin/env python3
"""Render Mermaid flowchart (stdin) to ASCII art (stdout) via beautiful-mermaid.

Requires Node.js and beautiful-mermaid installed where node can find it:
    npm install beautiful-mermaid          # local, or
    npm install -g beautiful-mermaid       # global
"""

import json
import os
import subprocess
import sys
import tempfile


def main():
    mermaid = sys.stdin.read()

    # Embed the mermaid source as a JSON string literal (valid JS) so no
    # escaping issues arise regardless of what the diagram contains.
    node_script = (
        "import { renderMermaidASCII } from 'beautiful-mermaid';\n"
        f"process.stdout.write(renderMermaidASCII({json.dumps(mermaid)}) + '\\n');\n"
    )

    # The temp file must live next to node_modules so ESM resolution can find
    # beautiful-mermaid — Node walks up from the importing file's location.
    script_dir = os.path.dirname(os.path.abspath(__file__))
    with tempfile.NamedTemporaryFile(mode='w', suffix='.mjs', dir=script_dir, delete=False) as f:
        f.write(node_script)
        tmp = f.name

    try:
        result = subprocess.run(['node', tmp], capture_output=True, text=True)
    finally:
        os.unlink(tmp)

    if result.returncode != 0:
        sys.stderr.write(result.stderr)
        sys.exit(result.returncode)

    sys.stdout.write(result.stdout)


if __name__ == '__main__':
    main()
