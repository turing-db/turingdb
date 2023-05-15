#pragma once

#include "Range.h"

#include "DB.h"

namespace db {

class DBAccessor;

class DBComponentTypeRange {
public:
    friend DBAccessor;
    using BaseRange = STLIndexRange<DB::ComponentTypes>;
    using Iterator = BaseRange::Iterator;

    bool empty() const { return _range.empty(); }
    size_t size() const { return _range.size(); }

    Iterator begin() const { return _range.begin(); }
    Iterator end() const { return _range.end(); }

private:
    BaseRange _range;

    DBComponentTypeRange(const DB* db);
    DBComponentTypeRange() = delete;
};

}
