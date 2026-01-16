// Test file with intentional coding style violations
// This file is for testing the Claude Code review bot

#include <cstdlib>
#include <iostream>
#include <memory>

using namespace std;


class TestClass {
    public:
        TestClass(int value)
            : memberValue(value)
        {
            // Work in constructor - violation
            cout << "Initializing with value: " << value << endl;
            computeInitialState();
        }

        std::vector<int> GetValues()
        {
            return values;
        }


        void ProcessData(Type * data, int *count) {
            // pointer formatting violations above
            for (int i = 0; i < *count; i++)
            {
                data->process();
            }
        }

    private:
        int memberValue;
        std::vector<int> values;
        std::shared_ptr<int> sharedData;

        void computeInitialState() {
            // Some computation
        }
};
