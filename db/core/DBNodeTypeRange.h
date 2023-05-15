#pragma once

#include "Range.h"

#include "DB.h"

namespace db {

class DBAccessor;

class DBNodeTypeRange {
public:
    friend DBAccessor;
    using BaseRange = STLIndexRange<DB::NodeTypes>;
    using Iterator = BaseRange::Iterator;

    bool empty() const { return _range.empty(); }
    size_t size() const { return _range.size(); }

    Iterator begin() const { return _range.begin(); }
    Iterator end() const { return _range.end(); }

private:
    BaseRange _range;

    DBNodeTypeRange(const DB* db);
    DBNodeTypeRange() = delete;
};

}
