// Test file with no style violations
// Used to verify the code review workflow skips commenting when no issues found

#include <stdio.h>
#include <vector>

class TestClass {
public:
    TestClass(int value);
    ~TestClass();

    int getValue() const { return _value; }
    void setValue(int value);

private:
    int _value {0};
    std::vector<int> _data;

    void processData();
};

TestClass::TestClass(int value)
    : _value(value)
{
}

TestClass::~TestClass() {
}

void TestClass::setValue(int value) {
    _value = value;
}

void TestClass::processData() {
    for (const auto& item : _data) {
        if (item > 0) {
            _value += item;
        }
    }
}

int main() {
    TestClass obj(42);
    obj.setValue(100);
    return 0;
}
