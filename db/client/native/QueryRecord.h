#pragma once

#include <stddef.h>

#include "QueryValue.h"

class Record;

namespace turing::db::client {

class QueryRecord {
public:
    QueryRecord(const Record& record)
        : _record(record)
    {
    }

    size_t size() const;

    QueryValue operator[](size_t i) const;

private:
    const Record& _record;
};

}
