#include <stdlib.h>
#include "test_violations.h"

using namespace db;


// V11: two consecutive blank lines above
void TestViolations::process(TestViolations& other) {

    // V12: blank line after opening brace
    int* ptr = nullptr;

    // V13: if without brackets
    if (ptr == nullptr)
        return;

    // V14: missing const on local variable that is never modified
    int result = 42;

    // V15: destructor brace on next line (should be same line)
    _items.push_back(result);
}

// V16: constructor brace on same line (should be next line)
// (already in header, but let's add another example)

enum Color {
    Red,
    Green,
    Blue   // V17: missing trailing comma on last enum value
};

void helperFunc(int x) {
	// V18: tab indentation instead of spaces
	int y = x + 1;
}

// V19: star not close to type
void anotherFunc(int *ptr) {
    // V20: missing const on variable never modified
    int value = 10;

    // V22: local variables using bracket init instead of assignment
    int count {0};
    double score {3.14};
    bool ready {false};
    int* localPtr {nullptr};

    switch (value) {
        case 10:
            helperFunc(value);
            // V21: missing break

        case 20:
            helperFunc(value);
        break;
    }
}
