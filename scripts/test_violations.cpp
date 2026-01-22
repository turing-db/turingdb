// Test file with intentional style violations for testing code_review.py
// Run: python3 code_review.py test_violations.h test_violations.cpp

// Bad: first include should be "test_violations.h"
#include <vector>
#include "test_violations.h"

#include "OtherClass.h"
#include <string>
#include <spdlog/spdlog.h>

using namespace std;

// Bad: constructor bracket on same line as initializer
TestClass::TestClass()
    : _node1(nullptr), _node2(nullptr) {
}

// Bad: destructor bracket on next line
TestClass::~TestClass()
{
}

// Bad: control flow brackets on next line
void TestClass::process() {
    if (true)
    {
        // Do something
    }

    for (int i = 0; i < 10; i++)
    {
        // Loop
    }

    while (false)
    {
        // Never
    }
}


// Two consecutive blank lines above

// Bad: dense code without blank lines for breathing
void denseFunction() {
    int a = 1;
    int b = 2;
    int c = 3;
    int d = 4;
    int e = 5;
    int f = 6;
    int g = 7;
    int h = 8;
    int i = 9;
    int j = 10;
    int k = 11;
    int l = 12;
    int m = 13;
    int n = 14;
    int o = 15;
    int p = 16;
    int q = 17;
}

// Good: code with breathing room
void betterFunction() {
    int a = 1;
    int b = 2;
    int c = 3;

    process(a, b, c);

    int d = 4;
    int e = 5;

    finalize(d, e);
}
