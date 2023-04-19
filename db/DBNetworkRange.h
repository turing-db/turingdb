#pragma once

#include "Range.h"

#include "DB.h"

namespace db {

class DBAccessor;

class DBNetworkRange {
public:
    friend DBAccessor;
    using BaseRange = STLValueMapRange<StringRef, Network*>;
    using Iterator = BaseRange::Iterator;

    DBNetworkRange() = default;

    bool empty() const { return _range.empty(); }
    size_t size() const { return _range.size(); }

    Iterator begin() const { return _range.begin(); }
    Iterator end() const { return _range.end(); }

private:
    BaseRange _range;

    DBNetworkRange(DB* db);
};

}
