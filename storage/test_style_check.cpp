// Test file with intentional style violations
// This file is used to verify that the Code Style Review workflow triggers

#include <iostream>

using namespace std;  // VIOLATION: Never use 'using namespace std'

class TestClass {
  public:  // VIOLATION: public should be aligned with class
    int x;

    void doSomething()
    {
        // VIOLATION: Opening brace should be on same line
        cout << "Hello" << endl;
    }
};

int main() {
    TestClass t;
    t.doSomething();
    return 0;
}
