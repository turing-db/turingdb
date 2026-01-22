// Test file that passes all style checks
// Run: python3 code_review.py test_clean.h test_clean.cpp
#pragma once

#include <string>
#include <vector>

#include <spdlog/spdlog.h>

#include "SomeClass.h"

class CleanClass {
public:
    CleanClass();
    ~CleanClass();

    void process();

private:
    Node* _node1 {nullptr};
    Node* _node2 {nullptr};
    const Data* _data {nullptr};

    std::unique_ptr<Node> _smartNode;
    std::vector<int> _values;
};

struct CleanStruct {
    Data* data {nullptr};
    int* values {nullptr};
    const char* name {nullptr};
};

// Good: returning by reference (not by value)
const std::vector<int>& getValuesRef();
const std::string& getNameRef();

// Good: using output parameters
void getValues(std::vector<int>& output);
void getName(std::string& output);
