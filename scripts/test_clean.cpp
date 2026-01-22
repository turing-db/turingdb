// Test file that passes all style checks
// Run: python3 code_review.py test_clean.h test_clean.cpp
#include "test_clean.h"

#include <string>
#include <vector>

#include <spdlog/spdlog.h>

#include "OtherClass.h"

using namespace db;

CleanClass::CleanClass()
    : _node1(nullptr),
    _node2(nullptr)
{
}

CleanClass::~CleanClass() {
}

void CleanClass::process() {
    if (true) {
        // Do something
    }

    for (int i = 0; i < 10; i++) {
        // Loop
    }

    while (false) {
        // Never
    }
}

void cleanFunction() {
    int a = 1;
    int b = 2;
    int c = 3;

    process(a, b, c);

    int d = 4;
    int e = 5;

    finalize(d, e);

    int f = 6;
    int g = 7;
}
