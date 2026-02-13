#include "CleanExample.h"

#include <stdlib.h>
#include <algorithm>

CleanExample::CleanExample(int maxSize)
    : _maxSize(maxSize)
{
}

CleanExample::~CleanExample() {
    delete _data;
}

void CleanExample::process(const std::vector<int>& input,
                           std::vector<int>& result) {
    result.clear();

    for (const auto& val : input) {
        const int scaled = val * _maxSize;

        if (scaled > 0) {
            result.push_back(scaled);
        }
    }

    const size_t count = result.size();
    if (count > 1) {
        std::sort(result.begin(), result.end());
    }
}

void CleanExample::_sortEntries() {
    std::sort(_entries.begin(), _entries.end());
}
