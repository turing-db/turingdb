// Test file with intentional style violations for code review testing

using namespace std;  // VIOLATION: Never use "using namespace std"

#include "common/BioAssert.h"  // VIOLATION: Wrong include order - project headers before standard
#include <string>
#include <vector>
#include <iostream>

using namespace biocommon;  // VIOLATION: using namespace in file scope

namespace turingdb {

class BadStyleExample
{
public:  // VIOLATION: public should be aligned with class keyword
    // VIOLATION: This line is way too long and exceeds the 90 character limit that is specified in the coding style guidelines, it should be broken up into multiple lines
    std::string get_data() { return data; }  // VIOLATION: snake_case instead of lowerCamelCase, returns string

    std::vector<int> GetNumbers()  // VIOLATION: PascalCase instead of lowerCamelCase, returns vector
    {
        return numbers;  // VIOLATION: Opening brace on new line (not constructor)
    }

    void processItem(std::string & item, int * count)  // VIOLATION: & and * not close to type
    {
        	*count++;  // VIOLATION: mixed tabs and spaces
    }

    std::shared_ptr<int> sharedData;  // VIOLATION: No std::shared_ptr allowed

private:
    std::string data;  // VIOLATION: No underscore prefix for private member
    std::vector<int> numbers;  // VIOLATION: No underscore prefix
    int count;  // VIOLATION: No underscore prefix
};

}
