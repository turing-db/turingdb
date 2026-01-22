// Test file with intentional style violations for testing code_review.py
// Run: python3 code_review.py test_violations.h test_violations.cpp
#pragma once

#include "SomeClass.h"
#include <vector>
#include <string>
#include <spdlog/spdlog.h>

using namespace std;
using namespace db;


// Two consecutive blank lines above

class TestClass {
public:
    TestClass();
    ~TestClass();

    void process();

private:
    // Bad: no initialization
    Node* _node1;

    // Bad: using = style
    Node* _node2 = nullptr;

    // Bad: empty braces
    Node* _node3 {};

    // Good: correct style
    Node* _node4 {nullptr};

    // Good: smart pointers don't need nullptr
    std::unique_ptr<Node> _smartNode;
};

struct BadStruct {
    Data* data;
    int* values;
    const char* name;
};

struct GoodStruct {
    Data* data {nullptr};
    int* values {nullptr};
    const char* name {nullptr};
};

// Bad: returning STL containers by value
std::vector<int> getValues();
std::map<int, std::string> getMapping();
std::string getName();

// Good: returning by reference
const std::vector<int>& getValuesRef();
const std::string& getNameRef();

// Non-trivial class (has STL member)
class NonTrivialClass {
private:
    std::vector<int> _data;
};

// Bad: returning non-trivial class by value
NonTrivialClass getNonTrivial();

// Good: returning by pointer
NonTrivialClass* getNonTrivialPtr();
