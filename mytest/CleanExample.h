#pragma once

#include <stdlib.h>
#include <vector>
#include <string>

class CleanExample {
public:
    CleanExample(int maxSize);
    ~CleanExample();

    int maxSize() const { return _maxSize; }
    bool isEmpty() const { return _entries.empty(); }

    void process(const std::vector<int>& input,
                 std::vector<int>& result);

private:
    int _maxSize {0};
    double* _data {nullptr};
    std::vector<std::string> _entries;

    void _sortEntries();
};
