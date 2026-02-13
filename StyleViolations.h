#pragma once

// VIOLATION: using namespace in a header file
using namespace std;

// VIOLATION: #include <cstdlib> style instead of <stdlib.h>
#include <cstdlib>
#include <cstring>

// VIOLATION: includes not in the correct order (utility before project headers)
#include "common/BioAssert.h"
#include <vector>
#include <string>
#include <unordered_map>
#include <memory>

// VIOLATION: using namespace std in header
using namespace std;

// VIOLATION: regular enum instead of enum class, no trailing comma
enum NodeColor { RED, GREEN, BLUE };

// VIOLATION: public member variables, no underscore prefix on members,
// tabs mixed with spaces, star close to variable name instead of type
class StyleViolations
{
public:
	// VIOLATION: big implementation in header file
	void processData(vector<string> input) {
		unordered_map<string, int> counts;
		for (auto& s : input) {
			counts[s]++;
		}
		for (auto& [key, val] : counts) {
			if (val > 1)
			{
				cout << key << ": " << val << endl;
			}
		}
	}

	// VIOLATION: returning an STL container
	vector<string> getNames() {
		vector<string> result;
		result.push_back("Alice");
		result.push_back("Bob");
		return result;
	}

	// VIOLATION: returning a string
	string getName() { return name; }

	// VIOLATION: public member variables
	string name;
	int age;
	double *score;

	// VIOLATION: using shared_ptr
	shared_ptr<int> sharedData;

	// VIOLATION: pointer not initialized to nullptr
	int *rawPtr;

private:
	// VIOLATION: no underscore prefix, pointer not close to type
	int * count;
	string label;

	// VIOLATION: using = instead of {} for member initialization
	int maxSize = 100;
};

// VIOLATION: opening brace on next line for non-constructor
class DataProcessor
{
public:
    // VIOLATION: work done in constructor, implementation in header
    DataProcessor(int size)
        : _data(new int[size]), _size(size)
    {
        // VIOLATION: doing work in constructor
        for (int i = 0; i < size; i++) {
            _data[i] = i * 2;
        }
        _computeSum();
    }

    // VIOLATION: passing smart pointer as argument
    void setShared(shared_ptr<int> ptr) {
        _sharedRef = ptr;
    }

    // VIOLATION: using assert instead of bioassert
    void validate() {
        assert(_size > 0);
    }

    // VIOLATION: returning STL container
    unordered_map<string, int> buildIndex() {
        unordered_map<string, int> index;
        index["a"] = 1;
        return index;
    }

    // VIOLATION: move semantics usage
    void takeOwnership(vector<int>&& data) {
        _moved = std::move(data);
    }

    // VIOLATION: mixed passing styles (pointer + reference in wrong context)
    void mixedArgs(int &simpleArg, vector<int> *containerArg) {
        // VIOLATION: reference for simple type, pointer for container
    }

private:
    int* _data {nullptr};
    int _size {0};
    int _sum {0};
    shared_ptr<int> _sharedRef;
    vector<int> _moved;

    // VIOLATION: big implementation in header
    void _computeSum() {
        _sum = 0;
        for (int i = 0; i < _size; i++) {
            _sum += _data[i];
        }
    }
};

// VIOLATION: this line is way too long, it goes beyond the 90 character limit and keeps going and going and going to really make the point clear that this is a violation of the line length rule
