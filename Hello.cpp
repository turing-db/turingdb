#include <string>

class MyClass {
public:
    MyClass(const std::string& hello)
        : _hello(hello)
    {
    }

private:
    std::string _hello;
};
