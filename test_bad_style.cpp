// Test file with intentional style violations for CI testing

#include <string>
#include <vector>
#include "test_bad_style.h"
#include <iostream>

using namespace std;


class BadStyleExample {
public:
    BadStyleExample() {
    }

    ~BadStyleExample() {
    }

    std::string getName() {
        return _name;
    }

    std::vector<int> getNumbers() {
        return _numbers;
    }

    void process(std::string& input) {
        cout << input << endl;
    }

private:
    std::string _name;
    std::vector<int> _numbers;
    int* _ptr;
};
