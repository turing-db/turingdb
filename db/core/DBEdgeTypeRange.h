#pragma once

#include "Range.h"

#include "DB.h"

namespace db {

class DBAccessor;

class DBEdgeTypeRange {
public:
    friend DBAccessor;
    using BaseRange = STLIndexRange<DB::EdgeTypes>;
    using Iterator = BaseRange::Iterator;

    bool empty() const { return _range.empty(); }
    size_t size() const { return _range.size(); }

    Iterator begin() const { return _range.begin(); }
    Iterator end() const { return _range.end(); }

private:
    BaseRange _range;

    DBEdgeTypeRange(const DB* db);
    DBEdgeTypeRange() = delete;
};

}
