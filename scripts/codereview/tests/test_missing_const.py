"""Tests for the missing_const checker."""

import pytest
from codereview.checks.missing_const import MissingConstCheck
from codereview.checks.base import CheckContext
from codereview.models import Violation


def run_check(code: str) -> list[Violation]:
    """Run the const checker on code and return violations."""
    # Remove leading newline but keep the structure
    if code.startswith('\n'):
        code = code[1:]
    lines = code.rstrip().split('\n')
    context = CheckContext(filepath="test.cpp", lines=lines, is_header=False)
    checker = MissingConstCheck()
    return checker.check(context)


def get_warnings_on_lines(code: str) -> set[int]:
    """Return line numbers that have warnings."""
    violations = run_check(code)
    return {v.line for v in violations}


# =============================================================================
# Parameter checks - should warn
# =============================================================================

class TestParametersShouldWarn:
    """Test cases where parameter const warnings should be raised."""

    def test_vector_ref_only_iterated(self):
        """Non-const vector ref that is only iterated should warn."""
        code = """
void processData(std::vector<int>& data) {
    for (int x : data) {
        printf("%d", x);
    }
}
"""
        warnings = get_warnings_on_lines(code)
        assert 1 in warnings, "Should warn on line 1 for 'data' parameter"

    def test_map_ref_only_iterated(self):
        """Non-const map ref that is only iterated should warn."""
        code = """
void processMap(std::map<int, std::string>& mapping) {
    for (auto& [k, v] : mapping) {
        printf("%d: %s", k, v.c_str());
    }
}
"""
        warnings = get_warnings_on_lines(code)
        assert 1 in warnings, "Should warn on line 1 for 'mapping' parameter"

    def test_unordered_map_only_find(self):
        """Unordered_map that only uses find() should warn (find is const)."""
        code = """
void findInUnorderedMap(std::unordered_map<int, int>& map) {
    auto it = map.find(42);
    if (it != map.end()) {
        printf("%d", it->second);
    }
}
"""
        warnings = get_warnings_on_lines(code)
        assert 1 in warnings, "Should warn on line 1 for 'map' parameter"


# =============================================================================
# Parameter checks - should NOT warn
# =============================================================================

class TestParametersShouldNotWarn:
    """Test cases where parameter const warnings should NOT be raised."""

    def test_const_reference_parameter(self):
        """Already const reference should not warn."""
        code = """
void readData(const std::vector<int>& data) {
    for (int x : data) {
        printf("%d", x);
    }
}
"""
        warnings = get_warnings_on_lines(code)
        assert 1 not in warnings, "Should not warn on const& parameter"

    def test_output_parameter_by_name(self):
        """Parameters named 'result', 'output', etc. should not warn."""
        code = """
void getData(std::vector<int>& result) {
    result.push_back(1);
}
"""
        warnings = get_warnings_on_lines(code)
        assert 1 not in warnings, "Should not warn on output parameter"

    def test_modified_via_push_back(self):
        """Parameter modified via push_back should not warn."""
        code = """
void appendData(std::vector<int>& data) {
    data.push_back(100);
}
"""
        warnings = get_warnings_on_lines(code)
        assert 1 not in warnings, "Should not warn on modified parameter"

    def test_map_subscript_assignment(self):
        """Map modified via subscript assignment should not warn."""
        code = """
void modifyMap(std::map<int, std::string>& mapping) {
    mapping[1] = "one";
}
"""
        warnings = get_warnings_on_lines(code)
        assert 1 not in warnings, "Should not warn on modified map"

    def test_passed_to_another_function(self):
        """Parameter passed to another function should not warn (conservative)."""
        code = """
void passToFunction(std::vector<int>& data) {
    processVector(data);
}
"""
        warnings = get_warnings_on_lines(code)
        assert 1 not in warnings, "Should not warn when passed to function"


# =============================================================================
# False positive: return type matching parameter pattern
# =============================================================================

class TestReturnTypeFalsePositives:
    """Test that return types are not mistaken for parameters."""

    def test_return_vector_ref(self):
        """Return type std::vector<T>& should not be flagged as parameter."""
        code = """
std::vector<int>& getVec(int id) {
    static std::vector<int> v;
    return v;
}
"""
        warnings = get_warnings_on_lines(code)
        assert 1 not in warnings, "Return type should not be flagged"

    def test_return_map_ref(self):
        """Return type std::map<K,V>& should not be flagged as parameter."""
        code = """
std::map<int, std::string>& getMap(int id) {
    static std::map<int, std::string> m;
    return m;
}
"""
        warnings = get_warnings_on_lines(code)
        assert 1 not in warnings, "Return type should not be flagged"


# =============================================================================
# Local variable checks - should warn
# =============================================================================

class TestLocalVariablesShouldWarn:
    """Test cases where local variable const warnings should be raised."""

    def test_int_never_modified(self):
        """Local int never modified should warn."""
        code = """
void test() {
    int value = 42;
    printf("%d", value);
}
"""
        warnings = get_warnings_on_lines(code)
        assert 2 in warnings, "Should warn on unmodified local int"

    def test_double_never_modified(self):
        """Local double never modified should warn."""
        code = """
void test() {
    double pi = 3.14159;
    printf("%f", pi);
}
"""
        warnings = get_warnings_on_lines(code)
        assert 2 in warnings, "Should warn on unmodified local double"

    def test_only_const_methods_called(self):
        """Local object with only const methods called should warn."""
        code = """
void test() {
    auto obj = createObject();
    int size = obj.size();
    printf("%d", size);
}
"""
        warnings = get_warnings_on_lines(code)
        assert 2 in warnings, "obj should warn (only const methods)"
        assert 3 in warnings, "size should warn (never modified)"


# =============================================================================
# Local variable checks - should NOT warn
# =============================================================================

class TestLocalVariablesShouldNotWarn:
    """Test cases where local variable const warnings should NOT be raised."""

    def test_modified_via_increment(self):
        """Variable modified via ++ should not warn."""
        code = """
void test() {
    int counter = 0;
    counter++;
    printf("%d", counter);
}
"""
        warnings = get_warnings_on_lines(code)
        assert 2 not in warnings, "Should not warn on incremented variable"

    def test_modified_via_compound_assignment(self):
        """Variable modified via += should not warn."""
        code = """
void test() {
    int sum = 10;
    sum += 5;
    printf("%d", sum);
}
"""
        warnings = get_warnings_on_lines(code)
        assert 2 not in warnings, "Should not warn on compound assignment"

    def test_loop_variable(self):
        """Loop variables should not warn."""
        code = """
void test() {
    for (int i = 0; i < 10; i++) {
        printf("%d", i);
    }
}
"""
        warnings = get_warnings_on_lines(code)
        assert 2 not in warnings, "Loop variable should not warn"

    def test_passed_to_function(self):
        """Variable passed to a function should not warn (may be modified)."""
        code = """
void test() {
    auto data = createData();
    processData(data);
}
"""
        warnings = get_warnings_on_lines(code)
        assert 2 not in warnings, "Variable passed to function should not warn"

    def test_address_taken(self):
        """Variable whose address is taken should not warn."""
        code = """
void test() {
    int value = 42;
    modifyByPointer(&value);
    printf("%d", value);
}
"""
        warnings = get_warnings_on_lines(code)
        assert 2 not in warnings, "Variable with address taken should not warn"

    def test_std_move_used(self):
        """Variable that is moved should not warn."""
        code = """
void test() {
    auto movable = createMovable();
    consumeMovable(std::move(movable));
}
"""
        warnings = get_warnings_on_lines(code)
        assert 2 not in warnings, "Moved variable should not warn"


# =============================================================================
# RAII object exclusions
# =============================================================================

class TestRAIIExclusions:
    """Test that RAII objects are not flagged."""

    def test_lock_guard(self):
        """lock_guard should not warn (RAII)."""
        code = """
void test() {
    std::lock_guard<std::mutex> lock(mutex);
    doProtectedWork();
}
"""
        warnings = get_warnings_on_lines(code)
        assert 2 not in warnings, "lock_guard should not warn"

    def test_unique_ptr(self):
        """unique_ptr should not warn."""
        code = """
void test() {
    auto ptr = std::make_unique<Widget>();
    useWidget(std::move(ptr));
}
"""
        warnings = get_warnings_on_lines(code)
        assert 2 not in warnings, "unique_ptr should not warn"

    def test_shared_ptr(self):
        """shared_ptr should not warn."""
        code = """
void test() {
    auto sptr = std::make_shared<Widget>();
    useSharedWidget(sptr);
}
"""
        warnings = get_warnings_on_lines(code)
        assert 2 not in warnings, "shared_ptr should not warn"


# =============================================================================
# const-safe functions (printf, fmt::print)
# =============================================================================

class TestConstSafeFunctions:
    """Test that const-safe function calls don't prevent warnings."""

    def test_only_used_in_printf(self):
        """Variable only used in printf should warn (printf takes by value)."""
        code = """
void test() {
    int printValue = 123;
    printf("%d", printValue);
}
"""
        warnings = get_warnings_on_lines(code)
        assert 2 in warnings, "Variable only in printf should warn"

    def test_only_used_in_fmt_print(self):
        """Variable only used in fmt::print should warn."""
        code = """
void test() {
    std::string message = "hello";
    fmt::print("{}", message);
}
"""
        warnings = get_warnings_on_lines(code)
        assert 2 in warnings, "Variable only in fmt::print should warn"


# =============================================================================
# Word boundary tests
# =============================================================================

class TestWordBoundaries:
    """Test that patterns don't match substrings incorrectly."""

    def test_map_vs_unordered_map(self):
        """'map' should not match inside 'unordered_map'."""
        # unordered_map should be recognized as its own container type
        code = """
void test(std::unordered_map<int, int>& umap) {
    umap[1] = 100;
}
"""
        warnings = get_warnings_on_lines(code)
        assert 1 not in warnings, "Modified unordered_map should not warn"


# =============================================================================
# Integration test with the full test file
# =============================================================================

class TestIntegrationWithTestFile:
    """Test running the checker on the full test file."""

    def test_expected_warning_count(self):
        """Verify the test file produces the expected number of warnings."""
        import os
        from pathlib import Path
        # Find the test file relative to the codereview package root
        # tests/ is under scripts/codereview/, test_const.cpp is under scripts/
        tests_dir = Path(__file__).parent  # tests/
        codereview_dir = tests_dir.parent  # scripts/codereview/
        scripts_dir = codereview_dir.parent  # scripts/
        test_file_path = scripts_dir / 'test_const.cpp'
        if not test_file_path.exists():
            pytest.skip(f"test_const.cpp not found at {test_file_path}")

        with open(test_file_path, 'r') as f:
            lines = f.read().split('\n')

        context = CheckContext(filepath="test_const.cpp", lines=lines, is_header=False)
        checker = MissingConstCheck()
        violations = checker.check(context)

        # Expected 11 warnings based on test file documentation
        assert len(violations) == 11, (
            f"Expected 11 warnings, got {len(violations)}: "
            f"{[(v.line, v.message) for v in violations]}"
        )
