// Test file with intentional style violations to verify the code review bot

#include <cstdlib>
#include <memory>
#include <vector>
#include <string>

using namespace std;


class BadlyFormattedClass
{
    public:
    BadlyFormattedClass(int * value, string name);
    ~BadlyFormattedClass();

    vector<string> getNames();
    shared_ptr<int> getValue() { return _value; }


    private:
    shared_ptr<int> _value;
    string _name;
};

BadlyFormattedClass::BadlyFormattedClass(int * value, string name)
    : _value(make_shared<int>(*value)),
    _name(name)
{
}

BadlyFormattedClass::~BadlyFormattedClass()
{
}

vector<string> BadlyFormattedClass::getNames()
{
    vector<string> result;
    result.push_back(_name);
    return result;
}


void processData(int * data, const vector<string> & names, BadlyFormattedClass * obj)
{
    for (int i=0; i<10; i++) {
        cout << "Processing item " << i << " with a very long line that exceeds the maximum allowed line length of 90 characters" << endl;
    }
}
