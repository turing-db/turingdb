#pragma once

#include <vector>
#include <string>
#include <memory>
#include <map>

using namespace std;  // V1: using namespace in header

namespace db {

class TestViolations {
public:
    int publicMember;  // V2: public member variable

    // V3: constructor implemented in header with non-trivial members
    TestViolations(int x) : publicMember(x) {
    }

    // V4: destructor in header with non-trivial members
    ~TestViolations() {
    }

    // V5: returning STL container
    std::vector<int> getItems() const { return _items; }

    // V6: returning string
    std::string getName() const { return _name; }

    // V7: passing class by reference instead of pointer
    void process(TestViolations& other);

    // V8: shared_ptr usage
    void setData(std::shared_ptr<int> data);

private:
    int* _rawPtr;  // V9: pointer not initialized to nullptr
    bool _flag;    // V10: primitive not bracket-initialized
    std::vector<int> _items;
    std::string _name;
    std::map<std::string, int> _lookup;
};

}
