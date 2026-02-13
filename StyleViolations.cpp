// VIOLATION: wrong include order - standard lib before own header
#include <stdio.h>
#include <vector>
#include <string>
#include <iostream>
#include <unordered_map>
// VIOLATION: no blank line after own header (and own header is not first)
#include "StyleViolations.h"
#include <memory>

// VIOLATION: using namespace std (allowed in cpp but combined with other issues)
using namespace std;

// VIOLATION: bracket on next line for regular function
void freeFunction(int x)
{
    cout << x << endl;
}

// VIOLATION: constructor bracket on same line (should be on next line)
StyleViolations::StyleViolations() : name("default"), age(0) {
    // VIOLATION: doing work in constructor
    score = new double(0.0);
    rawPtr = new int(42);
}

// VIOLATION: destructor with bracket on next line (should be on same line)
StyleViolations::~StyleViolations()
{
    delete score;
    delete rawPtr;
}

// VIOLATION: star close to variable name, not type
void StyleViolations::processNode(int *node, string &label) {
    // VIOLATION: no const on local variables that could be const
    int value = *node;
    string prefix = "node_";
    string fullLabel = prefix + label;

    // VIOLATION: no blank lines to make the code breathe, dense code
    if (value > 0) {
        cout << fullLabel << endl;
    } else {
        cout << "negative" << endl;
    }
    for (int i = 0; i < value; i++) {
        cout << i;
    }
    int result = value * 2;
    cout << result << endl;
}

// VIOLATION: returning a string
string StyleViolations::buildLabel(int id) {
    return "label_" + to_string(id);
}

// VIOLATION: returning an STL container
vector<int> StyleViolations::computeScores(vector<int> input) {
    vector<int> result;
    for (auto v : input) {
        result.push_back(v * 2);
    }
    return result;
}

// VIOLATION: switch formatting - break not aligned with case
void StyleViolations::handleColor(NodeColor color) {
    switch (color) {
        case RED:
            cout << "red";
            break;
        case GREEN:
            cout << "green";
            break;
        case BLUE:
            cout << "blue";
            break;
        default:
            // VIOLATION: using default case on enum switch
            cout << "unknown";
            break;
    }
}

// VIOLATION: function arguments not aligned when broken across lines
void StyleViolations::longFunction(int arg1,
    string arg2,
        double arg3,
    int arg4) {
    // VIOLATION: using std::shared_ptr
    shared_ptr<int> sp = make_shared<int>(arg1);

    // VIOLATION: no const on intermediate results
    double phase1 = arg3 * 2.0;
    double phase2 = arg3 * 3.0;
    double final_result = max(phase1, phase2);

    // VIOLATION: snake_case variable name instead of lowerCamelCase
    int my_counter = 0;
    for (int i = 0; i < arg4; i++) {
        my_counter += i;
    }
}

// VIOLATION: exception not deriving from TuringException
class MyCustomError : public std::runtime_error {
public:
    MyCustomError(const string& msg) : runtime_error(msg) {}
};

// VIOLATION: using boolean error return instead of exception
bool StyleViolations::tryLoad(const string& path) {
    if (path.empty()) {
        return false;  // VIOLATION: should throw exception
    }
    return true;
}
