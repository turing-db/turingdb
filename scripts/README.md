# TuringDB Code Review Script

A static C++ code style checker based on `CODING_STYLE.md`. Runs without AI model calls, designed for GitHub Actions integration.

## Installation

The code review tool is a uv-managed Python package located in `scripts/codereview/`.

```bash
# No installation required - uv handles dependencies automatically
# Just run from the repository root
```

## Usage

```bash
# Recommended: Run using uv from the repository root
uv run --project scripts/codereview codereview --all
uv run --project scripts/codereview codereview --diff origin/main
uv run --project scripts/codereview codereview file1.cpp file2.h

# Legacy: The old script still works (calls the new package internally)
python3 scripts/code_review.py --all
python3 scripts/code_review.py --diff origin/main
python3 scripts/code_review.py file1.cpp file2.h

# Output formats
codereview --format text file.cpp    # Human-readable (default)
codereview --format github file.cpp  # GitHub Actions annotations
codereview --format json file.cpp    # Machine-readable JSON

# Skip specific checks
codereview --skip-check "Const" --all
codereview --skip-check "Code Breathing" --all

# Run only specific checks
codereview --only-check "Formatting" --only-check "Namespaces" --all

# Use clang-tidy for const checking (requires clang-tidy and compile_commands.json)
codereview --clang-tidy-const --build-dir build --all
```

## Project Structure

```
scripts/codereview/
├── pyproject.toml              # uv project configuration
└── src/codereview/
    ├── __init__.py
    ├── __main__.py             # Entry point for python -m codereview
    ├── cli.py                  # Command-line interface
    ├── checker.py              # StyleChecker orchestrator
    ├── models.py               # Violation dataclass
    ├── constants.py            # Shared constants (SKIP_DIRS, STL_CONTAINERS, etc.)
    ├── output.py               # Output formatters (text, github, json)
    ├── utils.py                # File discovery utilities
    ├── clang_tidy.py           # Optional clang-tidy integration
    └── checks/                 # Individual check modules
        ├── __init__.py         # Check registry (@register_check)
        ├── base.py             # BaseCheck ABC, CheckContext
        ├── consecutive_blanks.py
        ├── using_namespace.py
        ├── include_order.py
        ├── pointer_member_init.py
        ├── stl_return_by_value.py
        ├── nontrivial_return_by_value.py
        ├── missing_const.py
        ├── constructor_brackets.py
        ├── destructor_brackets.py
        ├── bracket_positioning.py
        └── code_density.py
```

## Exit Codes

- `0` - No errors (warnings are allowed)
- `1` - One or more errors found

## Style Checks

### 1. Consecutive Blank Lines
**Severity:** error

No more than one consecutive blank line allowed anywhere in the code.

```cpp
// Bad
void foo() {
}


void bar() {  // Two blank lines above
}

// Good
void foo() {
}

void bar() {
}
```

### 2. Using Namespace
**Severity:** error

- `using namespace std` is never allowed (anywhere)
- `using namespace X` is forbidden in header files (.h, .hpp)
- `using namespace db` is allowed in .cpp files

```cpp
// Bad (in any file)
using namespace std;

// Bad (in header files)
using namespace db;

// Good (in .cpp files only)
using namespace db;
```

### 3. Pointer Member Initialization
**Severity:** error

All pointer class members must be initialized with `{nullptr}` using brace initialization style.

```cpp
// Good
class MyClass {
private:
    MyData* _data {nullptr};
    const Node* _parent {nullptr};
};

// Bad - no initialization
class MyClass {
private:
    MyData* _data;
};

// Bad - using = style
class MyClass {
private:
    MyData* _data = nullptr;
};

// Bad - empty braces
class MyClass {
private:
    MyData* _data {};
};
```

**Note:** Smart pointers (`std::unique_ptr`, `std::shared_ptr`) are not flagged as they default-construct to null.

### 4. Include Order
**Severity:** error

Includes must follow this order:
1. Current class header (for .cpp files) - followed by blank line
2. Standard library headers (`<vector>`, `<stdlib.h>`, etc.)
3. External library headers (`<spdlog/spdlog.h>`, etc.)
4. Project headers (`"MyClass.h"`)

```cpp
// Good - Graph.cpp
#include "Graph.h"

#include <string>
#include <vector>

#include <spdlog/spdlog.h>

#include "Node.h"
#include "Edge.h"

// Bad - Graph.cpp
#include <vector>           // Should be after Graph.h
#include "Graph.h"          // Should be first
#include "Node.h"
#include <spdlog/spdlog.h>  // Should be before project includes
```

### 5. STL Return by Value
**Severity:** error

Functions must not return STL containers or strings by value. Use output parameters (reference or pointer) instead.

```cpp
// Bad - returning by value
std::vector<int> getValues();
std::map<int, std::string> getMapping();
std::string getName();

// Good - returning by const reference
const std::vector<int>& getValues();
const std::string& getName();

// Good - using output parameter
void getValues(std::vector<int>& output);
void getName(std::string& output);
```

**Detected types:** vector, map, unordered_map, set, unordered_set, list, deque, array, string, wstring, and other STL containers.

### 6. Non-Trivial Class Return by Value
**Severity:** error

Classes that contain STL data structure members must not be returned by value.

```cpp
// Non-trivial class (has STL member)
class DataContainer {
private:
    std::vector<int> _data;
};

// Bad - returning non-trivial class by value
DataContainer getData();

// Good - returning by pointer
DataContainer* getData();

// Good - using output parameter
void getData(DataContainer& output);
```

**Excluded types:** Classes with names containing "Result", "Error", "Status", "Optional", "Expected", "Outcome", "Response" are considered result types and are allowed to be returned by value.

### 7. Constructor Brackets
**Severity:** error

Constructor opening bracket `{` must be on its own line.

```cpp
// Good
MyClass::MyClass()
    : _member(0)
{
}

// Bad
MyClass::MyClass()
    : _member(0) {
}
```

### 8. Destructor Brackets
**Severity:** error

Destructor opening bracket `{` must be on the same line.

```cpp
// Good
MyClass::~MyClass() {
}

// Bad
MyClass::~MyClass()
{
}
```

### 9. Control Flow Brackets
**Severity:** error

Opening bracket `{` must be on the same line for `if`, `for`, `while`, `else`, `switch`.

```cpp
// Good
if (condition) {
    doSomething();
}

for (int i = 0; i < n; i++) {
    process(i);
}

// Bad
if (condition)
{
    doSomething();
}
```

### 10. Code Density (Breathing)
**Severity:** warning

Flags function bodies with 15+ consecutive non-blank lines. Suggests adding blank lines to separate logical sections.

```cpp
// Triggers warning - too dense
void denseFunction() {
    int a = 1;
    int b = 2;
    int c = 3;
    // ... 15+ more lines without a blank line
}

// Good - has visual breaks
void betterFunction() {
    int a = 1;
    int b = 2;

    process(a, b);

    int result = finalize();
}
```

### 11. Missing Const
**Severity:** warning

Detects common cases where `const` should be used but is missing:

**Non-const reference parameters for STL containers:**
```cpp
// Bad - should be const& if data is not modified
void processData(std::vector<int>& data);
void processMap(std::map<int, std::string>& mapping);

// Good - const reference
void processData(const std::vector<int>& data);

// Good - output parameter (not flagged due to naming)
void getData(std::vector<int>& result);
void getValues(std::vector<int>& output);
```

**Local variables that are never modified:**
```cpp
// Bad - value is never changed
void example() {
    int value = 42;      // Warning: could be const
    printf("%d", value);
}

// Good - explicit const
void example() {
    const int value = 42;
    printf("%d", value);
}

// Good - variable is modified
void example() {
    int counter = 0;
    counter++;  // Modified, no warning
}
```

**Excluded:** Loop variables (i, j, k, etc.), iterators, and output parameters named `result`, `output`, `out`, or `ret`.

## Skipped Files

The following are automatically skipped:
- Files in `external/` directory
- Files in `third_party/` directory
- Files in `build/` directory
- Files in `test/` directory
- Files in `googletest/` directory
- Non-C++ files (only `.cpp`, `.h`, `.hpp`, `.cc`, `.cxx` are checked)
- Unit test files (`*Test.cpp`, `*Test.h`)

## GitHub Actions Integration

The script is integrated via `.github/workflows/code_review.yml`. The workflow:

1. Runs the style checker on changed files
2. Creates annotations in the GitHub Actions UI (visible in "Files changed" tab)
3. Posts a PR comment with a summary table of all violations
4. Fails the check if any errors are found (warnings are allowed)

**What happens on a PR:**
- If violations are found, a comment is posted with a table listing all issues
- Errors block the PR from merging (if branch protection is enabled)
- Warnings are informational and don't block merging

**Permissions required:**
- `contents: read` - to checkout code
- `pull-requests: write` - to post comments

## Output Formats

**Text format (default):**
```
src/Graph.cpp:42: error: [Formatting] Found 2 consecutive blank lines (max 1 allowed)
src/Node.h:10: error: [Namespaces] 'using namespace std' is never allowed

Total: 3 error(s), 0 warning(s)
```

**GitHub format (`--format github`):**

Creates annotations visible in GitHub Actions UI:
```
::error file=src/Graph.cpp,line=42::[Formatting] Found 2 consecutive blank lines (max 1 allowed)
::error file=src/Node.h,line=10::[Namespaces] 'using namespace std' is never allowed
```

**JSON format (`--format json`):**

Machine-readable output for programmatic processing:
```json
[
  {
    "filepath": "src/Graph.cpp",
    "line": 42,
    "severity": "error",
    "rule": "Formatting",
    "message": "Found 2 consecutive blank lines (max 1 allowed)"
  },
  {
    "filepath": "src/Node.h",
    "line": 10,
    "severity": "error",
    "rule": "Namespaces",
    "message": "'using namespace std' is never allowed"
  }
]
```

## Adding New Checks

To add a new check, create a new file in `scripts/codereview/src/codereview/checks/`:

```python
# scripts/codereview/src/codereview/checks/my_check.py
"""Check for something specific."""

from . import register_check
from .base import BaseCheck, CheckContext


@register_check
class MyCheck(BaseCheck):
    """Description of what this check does."""

    name = "MyRule"           # Rule name shown in violations
    severity = "error"        # Default severity: "error" or "warning"

    def run(self, context: CheckContext) -> None:
        """Execute the check logic."""
        for i, line in enumerate(context.lines, start=1):
            if self._has_problem(line):
                self.add_violation(
                    i,
                    "Description of the problem",
                )

    def _has_problem(self, line: str) -> bool:
        """Helper method to detect the problem."""
        # Your logic here
        return False
```

Then register the check by importing it in `checks/__init__.py`:

```python
# Add to the imports at the bottom of checks/__init__.py
from . import (
    # ... existing imports ...
    my_check,
)
```

### CheckContext Properties

The `context` object provides:
- `context.filepath` - Full path to the file being checked
- `context.lines` - List of lines in the file (strings)
- `context.is_header` - True if the file is a header (.h, .hpp)
- `context.filename` - Just the filename (e.g., "Graph.cpp")
- `context.stem` - Filename without extension (e.g., "Graph")
- `context.path` - pathlib.Path object for the file

### add_violation() Parameters

```python
self.add_violation(
    line,                    # Line number (1-indexed)
    message,                 # Description of the violation
    severity=None,           # Override default severity
    rule=None,               # Override default rule name
)
```
