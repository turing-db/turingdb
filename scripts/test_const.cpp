// Test file for const checking
// Run: python3 code_review.py test_const.cpp

#include <vector>
#include <string>
#include <map>

// Bad: non-const reference parameters (should warn)
void processData(std::vector<int>& data) {
    // Just reading data, should be const
    for (int x : data) {
        printf("%d\n", x);
    }
}

void processMap(std::map<int, std::string>& mapping) {
    // Just reading, should be const
    for (auto& [k, v] : mapping) {
        printf("%d: %s\n", k, v.c_str());
    }
}

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

// Good: output parameters (should NOT warn due to naming)
void getData(std::vector<int>& result) {
    result.push_back(1);
    result.push_back(2);
}

void getValues(std::vector<int>& output) {
    output.clear();
    output.push_back(42);
}

// Test local variable const detection
void testLocalVariables() {
    // Bad: never modified, should be const (should warn)
    int value = 42;
    printf("%d\n", value);

    // Good: modified after declaration (should NOT warn)
    int counter = 0;
    counter++;
    printf("%d\n", counter);

    // Good: modified via compound assignment (should NOT warn)
    int sum = 10;
    sum += 5;
    printf("%d\n", sum);

    // Bad: never modified, should be const (should warn)
    double pi = 3.14159;
    printf("%f\n", pi);

    // Good: loop variable (should NOT warn - excluded)
    for (int i = 0; i < 10; i++) {
        printf("%d\n", i);
    }
}
