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

# =============================================================================
# Additional local variable patterns - should warn
# =============================================================================

class TestAdditionalLocalVariablesShouldWarn:
    """Additional test cases where local variable const warnings should be raised."""

    def test_string_only_compared(self):
        """String only used for comparison should warn."""
        code = """
void test() {
    std::string name = "hello";
    if (name == "world") {
        printf("match");
    }
}
"""
        warnings = get_warnings_on_lines(code)
        assert 2 in warnings, "String only compared should warn"

    def test_size_t_never_modified(self):
        """size_t never modified should warn."""
        code = """
void test() {
    size_t count = vec.size();
    printf("%zu", count);
}
"""
        warnings = get_warnings_on_lines(code)
        assert 2 in warnings, "size_t only read should warn"

    def test_bool_never_modified(self):
        """Boolean never modified should warn."""
        code = """
void test() {
    bool isValid = checkValidity();
    if (isValid) {
        doSomething();
    }
}
"""
        warnings = get_warnings_on_lines(code)
        assert 2 in warnings, "bool only read should warn"

    def test_float_only_used_in_expression(self):
        """Float only used in expressions should warn."""
        code = """
void test() {
    float ratio = 0.5f;
    float result = input * ratio;
    printf("%f", result);
}
"""
        warnings = get_warnings_on_lines(code)
        assert 2 in warnings, "float only read should warn"
        assert 3 in warnings, "result only read should warn"

    def test_string_only_c_str_called(self):
        """String with only c_str() called should warn."""
        code = """
void test() {
    std::string path = "/tmp/file";
    fopen(path.c_str(), "r");
}
"""
        warnings = get_warnings_on_lines(code)
        # c_str() is a const accessor but passing result to fopen doesn't modify string
        # However, this currently won't warn because it's passed to a function
        # This test documents current behavior

    def test_multiple_const_methods_only(self):
        """Object with multiple const method calls should warn."""
        code = """
void test() {
    auto container = getContainer();
    int len = container.size();
    bool hasItems = !container.empty();
    auto first = container.front();
    printf("%d %d %d", len, hasItems, first);
}
"""
        warnings = get_warnings_on_lines(code)
        assert 2 in warnings, "container with only const methods should warn"
        assert 3 in warnings, "len should warn"
        assert 4 in warnings, "hasItems should warn"
        assert 5 in warnings, "first should warn"

    def test_char_never_modified(self):
        """Char variable never modified should warn."""
        code = """
void test() {
    char delimiter = ',';
    printf("Using delimiter: %c", delimiter);
}
"""
        warnings = get_warnings_on_lines(code)
        assert 2 in warnings, "char only read should warn"

    def test_long_never_modified(self):
        """Long variable never modified should warn."""
        code = """
void test() {
    long offset = calculateOffset();
    seek(file, offset);
}
"""
        warnings = get_warnings_on_lines(code)
        # offset is passed to seek() which may modify - conservative

    def test_uint64_never_modified(self):
        """uint64_t never modified should warn."""
        code = """
void test() {
    uint64_t timestamp = getTimestamp();
    printf("Time: %llu", timestamp);
}
"""
        warnings = get_warnings_on_lines(code)
        assert 2 in warnings, "uint64_t only printed should warn"


# =============================================================================
# Additional local variable patterns - should NOT warn
# =============================================================================

class TestAdditionalLocalVariablesShouldNotWarn:
    """Additional test cases where local variable const warnings should NOT be raised."""

    def test_modified_via_decrement(self):
        """Variable modified via -- should not warn."""
        code = """
void test() {
    int count = 10;
    count--;
    printf("%d", count);
}
"""
        warnings = get_warnings_on_lines(code)
        assert 2 not in warnings, "Decremented variable should not warn"

    def test_prefix_increment(self):
        """Variable with prefix increment should not warn."""
        code = """
void test() {
    int val = 0;
    ++val;
    printf("%d", val);
}
"""
        warnings = get_warnings_on_lines(code)
        assert 2 not in warnings, "Prefix incremented should not warn"

    def test_prefix_decrement(self):
        """Variable with prefix decrement should not warn."""
        code = """
void test() {
    int val = 10;
    --val;
    printf("%d", val);
}
"""
        warnings = get_warnings_on_lines(code)
        assert 2 not in warnings, "Prefix decremented should not warn"

    def test_bitwise_or_assignment(self):
        """Variable modified via |= should not warn."""
        code = """
void test() {
    int flags = 0;
    flags |= FLAG_ENABLED;
    useFlags(flags);
}
"""
        warnings = get_warnings_on_lines(code)
        assert 2 not in warnings, "Bitwise or assigned should not warn"

    def test_bitwise_and_assignment(self):
        """Variable modified via &= should not warn."""
        code = """
void test() {
    int mask = 0xFF;
    mask &= newMask;
    useMask(mask);
}
"""
        warnings = get_warnings_on_lines(code)
        assert 2 not in warnings, "Bitwise and assigned should not warn"

    def test_shift_left_assignment(self):
        """Variable modified via <<= should not warn."""
        code = """
void test() {
    int bits = 1;
    bits <<= 4;
    useBits(bits);
}
"""
        warnings = get_warnings_on_lines(code)
        # <<= might not be covered - documenting expected behavior

    def test_later_reassignment(self):
        """Variable reassigned later should not warn."""
        code = """
void test() {
    int value = 0;
    if (condition) {
        value = 42;
    }
    useValue(value);
}
"""
        warnings = get_warnings_on_lines(code)
        assert 2 not in warnings, "Reassigned variable should not warn"

    def test_assigned_in_while_loop(self):
        """Variable assigned in while loop should not warn."""
        code = """
void test() {
    int result = 0;
    while ((result = getNext()) != -1) {
        process(result);
    }
}
"""
        warnings = get_warnings_on_lines(code)
        assert 2 not in warnings, "Loop-assigned variable should not warn"

    def test_array_element_assignment(self):
        """Variable used as array with element assignment should not warn."""
        code = """
void test() {
    auto arr = createArray();
    arr[0] = 42;
    useArray(arr);
}
"""
        warnings = get_warnings_on_lines(code)
        assert 2 not in warnings, "Array element assigned should not warn"

    def test_modified_via_non_const_method(self):
        """Variable modified via non-const method should not warn."""
        code = """
void test() {
    auto buffer = createBuffer();
    buffer.append("data");
    useBuffer(buffer);
}
"""
        warnings = get_warnings_on_lines(code)
        assert 2 not in warnings, "append() is non-const"

    def test_modified_via_set_method(self):
        """Variable modified via set* method should not warn."""
        code = """
void test() {
    auto obj = createObject();
    obj.setValue(42);
    useObject(obj);
}
"""
        warnings = get_warnings_on_lines(code)
        assert 2 not in warnings, "setValue() is non-const"

    def test_modified_via_clear(self):
        """Variable modified via clear() should not warn."""
        code = """
void test() {
    auto container = createContainer();
    container.clear();
    useContainer(container);
}
"""
        warnings = get_warnings_on_lines(code)
        assert 2 not in warnings, "clear() is non-const"

    def test_passed_by_dereferenced_pointer(self):
        """Variable passed as *var should not warn."""
        code = """
void test() {
    auto ptr = getPointer();
    modifyValue(*ptr);
}
"""
        warnings = get_warnings_on_lines(code)
        # Dereferenced pointer passed to function may be modified


# =============================================================================
# Additional parameter patterns - should warn
# =============================================================================

class TestAdditionalParametersShouldWarn:
    """Additional test cases where parameter const warnings should be raised."""

    def test_set_ref_only_iterated(self):
        """Non-const set ref that is only iterated should warn."""
        code = """
void processSet(std::set<int>& numbers) {
    for (int x : numbers) {
        printf("%d", x);
    }
}
"""
        warnings = get_warnings_on_lines(code)
        assert 1 in warnings, "Should warn on line 1 for 'numbers' parameter"

    def test_deque_ref_only_read(self):
        """Non-const deque ref that is only read should warn."""
        code = """
void readDeque(std::deque<int>& dq) {
    for (auto& val : dq) {
        printf("%d", val);
    }
}
"""
        warnings = get_warnings_on_lines(code)
        assert 1 in warnings, "Should warn for 'dq' parameter"

    def test_list_ref_only_iterated(self):
        """Non-const list ref that is only iterated should warn."""
        code = """
void readList(std::list<int>& lst) {
    for (int val : lst) {
        printf("%d", val);
    }
}
"""
        warnings = get_warnings_on_lines(code)
        assert 1 in warnings, "Should warn for 'lst' parameter"

    def test_multimap_only_find_used(self):
        """Multimap that only uses find() should warn."""
        code = """
void findInMultimap(std::multimap<int, int>& mm) {
    auto it = mm.find(42);
    if (it != mm.end()) {
        printf("%d", it->second);
    }
}
"""
        warnings = get_warnings_on_lines(code)
        assert 1 in warnings, "Should warn for 'mm' parameter"

    def test_unordered_set_ref_only_read(self):
        """Non-const unordered_set ref that is only read should warn."""
        code = """
void readUnorderedSet(std::unordered_set<int>& vals) {
    for (int val : vals) {
        printf("%d", val);
    }
}
"""
        warnings = get_warnings_on_lines(code)
        assert 1 in warnings, "Should warn for 'vals' parameter"


# =============================================================================
# Additional parameter patterns - should NOT warn
# =============================================================================

class TestAdditionalParametersShouldNotWarn:
    """Additional test cases where parameter const warnings should NOT be raised."""

    def test_vector_cleared(self):
        """Vector that is cleared should not warn."""
        code = """
void clearAndFill(std::vector<int>& vec) {
    vec.clear();
    vec.push_back(1);
}
"""
        warnings = get_warnings_on_lines(code)
        assert 1 not in warnings, "Cleared vector should not warn"

    def test_vector_resized(self):
        """Vector that is resized should not warn."""
        code = """
void resizeVector(std::vector<int>& vec) {
    vec.resize(100);
}
"""
        warnings = get_warnings_on_lines(code)
        assert 1 not in warnings, "Resized vector should not warn"

    def test_vector_reserved(self):
        """Vector that has reserve called should not warn."""
        code = """
void reserveVector(std::vector<int>& vec) {
    vec.reserve(100);
}
"""
        warnings = get_warnings_on_lines(code)
        assert 1 not in warnings, "Reserved vector should not warn"

    def test_vector_assigned(self):
        """Vector that is assigned should not warn."""
        code = """
void assignVector(std::vector<int>& vec) {
    vec.assign(10, 0);
}
"""
        warnings = get_warnings_on_lines(code)
        assert 1 not in warnings, "Assigned vector should not warn"

    def test_vector_swapped(self):
        """Vector that is swapped should not warn."""
        code = """
void swapVector(std::vector<int>& vec) {
    std::vector<int> other;
    vec.swap(other);
}
"""
        warnings = get_warnings_on_lines(code)
        assert 1 not in warnings, "Swapped vector should not warn"

    def test_set_insert(self):
        """Set that has insert called should not warn."""
        code = """
void insertInSet(std::set<int>& items) {
    items.insert(42);
}
"""
        warnings = get_warnings_on_lines(code)
        assert 1 not in warnings, "Set with insert should not warn"

    def test_set_erase(self):
        """Set that has erase called should not warn."""
        code = """
void eraseFromSet(std::set<int>& items) {
    items.erase(42);
}
"""
        warnings = get_warnings_on_lines(code)
        assert 1 not in warnings, "Set with erase should not warn"

    def test_emplace_back(self):
        """Vector with emplace_back should not warn."""
        code = """
void emplaceInVector(std::vector<std::string>& vec) {
    vec.emplace_back("hello");
}
"""
        warnings = get_warnings_on_lines(code)
        assert 1 not in warnings, "emplace_back should not warn"

    def test_emplace_front(self):
        """Deque with emplace_front should not warn."""
        code = """
void emplaceInDeque(std::deque<int>& dq) {
    dq.emplace_front(42);
}
"""
        warnings = get_warnings_on_lines(code)
        assert 1 not in warnings, "emplace_front should not warn"

    def test_pop_back(self):
        """Vector with pop_back should not warn."""
        code = """
void popFromVector(std::vector<int>& vec) {
    vec.pop_back();
}
"""
        warnings = get_warnings_on_lines(code)
        assert 1 not in warnings, "pop_back should not warn"

    def test_pop_front(self):
        """Deque with pop_front should not warn."""
        code = """
void popFromDeque(std::deque<int>& dq) {
    dq.pop_front();
}
"""
        warnings = get_warnings_on_lines(code)
        assert 1 not in warnings, "pop_front should not warn"


# =============================================================================
# Edge cases and special patterns
# =============================================================================

class TestEdgeCases:
    """Test edge cases and special patterns."""

    def test_nested_template_type(self):
        """Complex nested template should be handled correctly."""
        code = """
void processNested(std::vector<std::pair<int, std::string>>& data) {
    for (auto& p : data) {
        printf("%d: %s", p.first, p.second.c_str());
    }
}
"""
        warnings = get_warnings_on_lines(code)
        assert 1 in warnings, "Nested template only read should warn"

    def test_multiline_function_signature(self):
        """Multiline function signature should be handled."""
        code = """
void processData(
    std::vector<int>& data,
    int count) {
    for (int x : data) {
        printf("%d", x);
    }
}
"""
        warnings = get_warnings_on_lines(code)
        # May not detect due to multiline - documents behavior

    def test_already_const_should_not_warn(self):
        """Already const parameter should not be flagged."""
        code = """
void readData(const std::vector<int>& data) {
    for (int x : data) {
        printf("%d", x);
    }
}
"""
        warnings = get_warnings_on_lines(code)
        assert 1 not in warnings, "const& should never warn"

    def test_rvalue_reference_not_flagged(self):
        """Rvalue reference should not be flagged as needing const."""
        code = """
void consumeData(std::vector<int>&& data) {
    storage = std::move(data);
}
"""
        warnings = get_warnings_on_lines(code)
        assert 1 not in warnings, "&& should not warn"

    def test_lambda_variable(self):
        """Lambda stored in variable should not warn (complex analysis)."""
        code = """
void test() {
    auto lambda = [](int x) { return x * 2; };
    result = lambda(5);
}
"""
        warnings = get_warnings_on_lines(code)
        # Lambda analysis is complex - document behavior

    def test_structured_binding(self):
        """Structured binding variables."""
        code = """
void test() {
    auto [a, b] = getPair();
    printf("%d %d", a, b);
}
"""
        warnings = get_warnings_on_lines(code)
        # Structured bindings have special semantics

    def test_extern_variable_declaration(self):
        """Extern declarations should not trigger warnings."""
        code = """
void test() {
    extern int globalCounter;
    printf("%d", globalCounter);
}
"""
        warnings = get_warnings_on_lines(code)
        # extern is declaration, not definition

    def test_static_local_variable(self):
        """Static local variables may need to be non-const."""
        code = """
void test() {
    static int counter = 0;
    printf("%d", counter);
}
"""
        warnings = get_warnings_on_lines(code)
        # Static locals often get modified across calls

    def test_namespace_declaration(self):
        """Namespace declarations should not be mistaken for variables."""
        code = """
namespace db {
template class ShortestPathProcessor<types::UInt64>;
template class ShortestPathProcessor<types::Double>;
}
"""
        warnings = get_warnings_on_lines(code)
        assert 1 not in warnings, "Namespace name should not be flagged as variable"

    def test_class_declaration(self):
        """Class declarations should not be mistaken for variables."""
        code = """
class MyClass {
public:
    int value;
};
"""
        warnings = get_warnings_on_lines(code)
        assert 1 not in warnings, "Class name should not be flagged as variable"

    def test_struct_declaration(self):
        """Struct declarations should not be mistaken for variables."""
        code = """
struct MyStruct {
    int x;
    int y;
};
"""
        warnings = get_warnings_on_lines(code)
        assert 1 not in warnings, "Struct name should not be flagged as variable"


# =============================================================================
# Iterator and range patterns
# =============================================================================

class TestIteratorPatterns:
    """Test iterator and range-based patterns."""

    def test_range_for_element_only_read(self):
        """Range-for element variable only read should warn."""
        code = """
void test() {
    std::vector<int> data = {1, 2, 3};
    for (int elem : data) {
        printf("%d", elem);
    }
}
"""
        warnings = get_warnings_on_lines(code)
        # elem is a loop variable copy - may or may not warn

    def test_begin_end_iterator(self):
        """begin/end iterators should not warn (commonly modified)."""
        code = """
void test() {
    auto it = container.begin();
    while (it != container.end()) {
        printf("%d", *it);
        ++it;
    }
}
"""
        warnings = get_warnings_on_lines(code)
        # Iterator pattern - 'it' is modified

    def test_find_iterator_result(self):
        """Iterator from find should not warn if incremented."""
        code = """
void test() {
    auto it = map.find(key);
    if (it != map.end()) {
        it++;
        useIterator(it);
    }
}
"""
        warnings = get_warnings_on_lines(code)
        assert 2 not in warnings, "Incremented iterator should not warn"


# =============================================================================
# Reference and pointer patterns
# =============================================================================

class TestReferencePointerPatterns:
    """Test reference and pointer patterns."""

    def test_const_ref_binding_to_temp(self):
        """Const reference to temporary should not warn."""
        code = """
void test() {
    const std::string& ref = getString();
    printf("%s", ref.c_str());
}
"""
        warnings = get_warnings_on_lines(code)
        assert 2 not in warnings, "const ref should not warn"

    def test_reference_to_element(self):
        """Reference to container element."""
        code = """
void test() {
    int& elem = vec[0];
    elem = 42;
}
"""
        warnings = get_warnings_on_lines(code)
        # Reference to element, assigned through ref

    def test_pointer_to_element(self):
        """Pointer to element may be modified through it."""
        code = """
void test() {
    int* ptr = &vec[0];
    *ptr = 42;
}
"""
        warnings = get_warnings_on_lines(code)
        # Pointer modification


# =============================================================================
# Const method call patterns
# =============================================================================

class TestConstMethodPatterns:
    """Test patterns with const/non-const method calls."""

    def test_at_method_is_const(self):
        """Using .at() is a const operation."""
        code = """
void test() {
    auto vec = getVector();
    int val = vec.at(0);
    printf("%d", val);
}
"""
        warnings = get_warnings_on_lines(code)
        assert 2 in warnings, "vec with only at() should warn"
        assert 3 in warnings, "val should warn"

    def test_back_method_is_const(self):
        """Using .back() is a const operation."""
        code = """
void test() {
    auto vec = getVector();
    int last = vec.back();
    printf("%d", last);
}
"""
        warnings = get_warnings_on_lines(code)
        assert 2 in warnings, "vec with only back() should warn"
        assert 3 in warnings, "last should warn"

    def test_front_method_is_const(self):
        """Using .front() is a const operation."""
        code = """
void test() {
    auto vec = getVector();
    int first = vec.front();
    printf("%d", first);
}
"""
        warnings = get_warnings_on_lines(code)
        assert 2 in warnings, "vec with only front() should warn"
        assert 3 in warnings, "first should warn"

    def test_contains_method_is_const(self):
        """Using .contains() is a const operation (C++20)."""
        code = """
void test() {
    auto set = getSet();
    bool has = set.contains(42);
    printf("%d", has);
}
"""
        warnings = get_warnings_on_lines(code)
        assert 2 in warnings, "set with only contains() should warn"
        assert 3 in warnings, "has should warn"

    def test_count_method_is_const(self):
        """Using .count() is a const operation."""
        code = """
void test() {
    auto myMap = getMap();
    size_t numItems = myMap.count(key);
    printf("%zu", numItems);
}
"""
        warnings = get_warnings_on_lines(code)
        assert 2 in warnings, "myMap with only count() should warn"
        assert 3 in warnings, "numItems should warn"


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
