#pragma once

namespace db {

class Query {
public:
    Query() = default;
    virtual ~Query() = default;

    Query(const Query&) = delete;
    Query(Query&&) = delete;
    Query& operator=(const Query&) = delete;
    Query& operator=(Query&&) = delete;
};

}
