#pragma once

namespace db {

class Column;

class Index {
public:
    Index() = default;
    virtual ~Index() = default;

    Index(const Index&) = delete;
    Index(Index&&) = delete;
    Index& operator=(const Index&) = delete;
    Index& operator=(Index&&) = delete;

    virtual Column* query(const Column* input) = 0;
};

}
