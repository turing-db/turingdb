# TuringDB Code Review Script

A static C++ code style checker based on `CODING_STYLE.md`. Runs without AI model calls, designed for GitHub Actions integration.

## Usage

```bash
# Check specific files
python3 code_review.py file1.cpp file2.h

# Check all files changed compared to a base branch
python3 code_review.py --diff origin/main

# Check the entire codebase
python3 code_review.py --all

# Output in GitHub Actions annotation format
python3 code_review.py --format github file.cpp

# Combine options
python3 code_review.py --diff origin/main --format github
python3 code_review.py --all --format text
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
- Non-C++ files (only `.cpp`, `.h`, `.hpp`, `.cc`, `.cxx` are checked)

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

To add a new check:

1. Add a method `check_<name>(self)` to the `StyleChecker` class
2. Call it from `check_all()`
3. Use `self.add_violation(line, severity, rule, message)` to report issues

```python
def check_something(self):
    """Check for something."""
    for i, line in enumerate(self.lines, start=1):
        if problem_detected(line):
            self.add_violation(
                i,
                "error",  # or "warning"
                "RuleName",
                "Description of the problem",
            )
```
