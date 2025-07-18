#pragma once

namespace db {

class QueryCompound {
public:
    QueryCompound() = default;
    virtual ~QueryCompound() = default;

    QueryCompound(const QueryCompound&) = delete;
    QueryCompound(QueryCompound&&) = delete;
    QueryCompound& operator=(const QueryCompound&) = delete;
    QueryCompound& operator=(QueryCompound&&) = delete;
};

}
