// Test file for const checking
// Run: uv run --project scripts/codereview codereview scripts/test_const.cpp
//
// This file contains test cases for the const checker, including:
// - Parameter const checking
// - Local variable const checking
// - False positive prevention patterns
//
// Expected warnings (11 total):
//   Line 33:  Parameter 'data' - non-const vector ref only iterated
//   Line 39:  Parameter 'mapping' - non-const map ref only iterated
//   Line 118: Parameter 'map' - unordered_map only uses find() which is const
//   Line 136: Local 'value' - never modified
//   Line 140: Local 'pi' - never modified
//   Line 194: Local 'printValue' - only passed to printf (by value)
//   Line 198: Local 'message' - only passed to fmt::print (by value)
//   Line 219: Local 'obj' - only const methods called
//   Line 220: Local 'size' - never modified
//   Line 221: Local 'empty' - never modified
//   Line 222: Local 'val' - never modified

#include <vector>
#include <string>
#include <map>
#include <unordered_map>
#include <memory>

// =============================================================================
// PARAMETER CHECKS - Should warn
// =============================================================================

// Bad: non-const reference parameters that are only read (should warn)
void processData(std::vector<int>& data) {
    for (int x : data) {
        printf("%d\n", x);
    }
}

void processMap(std::map<int, std::string>& mapping) {
    for (auto& [k, v] : mapping) {
        printf("%d: %s\n", k, v.c_str());
    }
}

// =============================================================================
// PARAMETER CHECKS - Should NOT warn
// =============================================================================

// Good: const reference parameters (should NOT warn)
void readData(const std::vector<int>& data) {
    for (int x : data) {
        printf("%d\n", x);
    }
}

void readMap(const std::map<int, std::string>& mapping) {
    for (auto& [k, v] : mapping) {
        printf("%d: %s\n", k, v.c_str());
    }
}

// Good: output parameters by name convention (should NOT warn)
void getData(std::vector<int>& result) {
    result.push_back(1);
}

void getValues(std::vector<int>& output) {
    output.clear();
    output.push_back(42);
}

// Good: modified via push_back (should NOT warn)
void appendData(std::vector<int>& data) {
    data.push_back(100);
}

// Good: modified via subscript assignment (should NOT warn)
void modifyMap(std::map<int, std::string>& mapping) {
    mapping[1] = "one";
}

// Good: map subscript access without assignment - still non-const (should NOT warn)
std::string& getFromMap(std::map<int, std::string>& mapping, int key) {
    return mapping[key];  // operator[] is non-const for maps
}

// Good: passed to another function (should NOT warn - conservative)
void passToFunction(std::vector<int>& data) {
    processVector(data);
}

// Good: passed as first argument (should NOT warn)
void firstArgument(std::vector<int>& vec) {
    doSomething(vec, 42);
}

// =============================================================================
// FALSE POSITIVE: Return type matching parameter pattern
// =============================================================================

// Good: Return type is vector ref, getVec is function name not param (should NOT warn)
std::vector<int>& getVec(int id) {
    static std::vector<int> v;
    return v;
}

// Good: Same pattern with map (should NOT warn)
std::map<int, std::string>& getMap(int id) {
    static std::map<int, std::string> m;
    return m;
}

// =============================================================================
// FALSE POSITIVE: Word boundary - map vs unordered_map
// =============================================================================

// Bad: unordered_map param only uses find() which is const (should warn)
void findInUnorderedMap(std::unordered_map<int, int>& map) {
    auto it = map.find(42);  // find() is const
    if (it != map.end()) {
        printf("%d\n", it->second);
    }
}

// Good: unordered_map modified via operator[] (should NOT warn)
void modifyUnorderedMap(std::unordered_map<int, int>& map) {
    map[1] = 100;  // Non-const access
}

// =============================================================================
// LOCAL VARIABLE CHECKS - Should warn
// =============================================================================

void testLocalVariablesShouldWarn() {
    // Bad: never modified, should be const (should warn)
    int value = 42;
    printf("%d\n", value);

    // Bad: never modified, should be const (should warn)
    double pi = 3.14159;
    printf("%f\n", pi);
}

// =============================================================================
// LOCAL VARIABLE CHECKS - Should NOT warn
// =============================================================================

void testLocalVariablesShouldNotWarn() {
    // Good: modified after declaration (should NOT warn)
    int counter = 0;
    counter++;
    printf("%d\n", counter);

    // Good: modified via compound assignment (should NOT warn)
    int sum = 10;
    sum += 5;
    printf("%d\n", sum);

    // Good: loop variable (should NOT warn - excluded)
    for (int i = 0; i < 10; i++) {
        printf("%d\n", i);
    }

    // Good: iterator variable (should NOT warn - excluded)
    for (int it = 0; it < 5; it++) {
        printf("%d\n", it);
    }
}

// =============================================================================
// FALSE POSITIVE: Variables passed to functions
// =============================================================================

void testFunctionArguments() {
    // Good: passed to a function that might modify it (should NOT warn)
    auto data = createData();
    processData(data);

    // Good: passed as first argument (should NOT warn)
    auto vec = createVector();
    doSomething(vec, 42);

    // Good: passed in nested function call (should NOT warn)
    auto result = createResult();
    processNested(getData(), result, getCount());
}

// =============================================================================
// FALSE POSITIVE: Const-safe functions (printf, fmt::print)
// =============================================================================

void testConstSafeFunctions() {
    // Bad: only passed to printf which is by-value (should warn)
    int printValue = 123;
    printf("%d\n", printValue);

    // Bad: only used in fmt::print (should warn)
    std::string message = "hello";
    fmt::print("{}\n", message);
}

// =============================================================================
// FALSE POSITIVE: Local reference variables
// =============================================================================

void testLocalReferences() {
    // Good: This is a local reference, not a function parameter (should NOT warn as param)
    // The checker should not confuse this with a function parameter declaration
    std::vector<int>& localRef = getGlobalVector();
    localRef.push_back(1);
}

// =============================================================================
// Method calls with known const prefixes
// =============================================================================

void testConstMethods() {
    // Bad: only const methods called, so obj could be const (should warn)
    auto obj = createObject();
    int size = obj.size();
    bool empty = obj.empty();
    auto val = obj.getValue();
    printf("%d %d %d\n", size, empty, val);
    // Note: size, empty, val should also warn as they're never modified

    // Good: non-const method called (should NOT warn)
    auto mutableObj = createObject();
    mutableObj.setValue(42);
    mutableObj.clear();
}

// =============================================================================
// FALSE POSITIVE: RAII objects and locks
// =============================================================================

void testRAIIObjects() {
    // Good: lock_guard is RAII, shouldn't suggest const (should NOT warn)
    std::lock_guard<std::mutex> lock(mutex);
    doProtectedWork();

    // Good: unique_lock is RAII (should NOT warn)
    std::unique_lock<std::mutex> ulock(mutex);
    doProtectedWork();
}

// =============================================================================
// FALSE POSITIVE: Smart pointers
// =============================================================================

void testSmartPointers() {
    // Good: unique_ptr commonly moved/returned (should NOT warn)
    auto ptr = std::make_unique<Widget>();
    useWidget(std::move(ptr));

    // Good: shared_ptr (should NOT warn)
    auto sptr = std::make_shared<Widget>();
    useSharedWidget(sptr);
}

// =============================================================================
// FALSE POSITIVE: std::move usage
// =============================================================================

void testMoveSemantics() {
    // Good: variable is moved, cannot be const (should NOT warn)
    auto movable = createMovable();
    consumeMovable(std::move(movable));
}

// =============================================================================
// FALSE POSITIVE: Address-of operator
// =============================================================================

void testAddressOf() {
    // Good: address taken, could be modified (should NOT warn)
    int value = 42;
    modifyByPointer(&value);
    printf("%d\n", value);
}
