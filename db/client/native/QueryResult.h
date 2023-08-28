#pragma once

#include <stdint.h>
#include <iterator>

#include "QueryRecord.h"

class SessionResponse;
class PullResponse;

namespace turing::db::client {

class TuringClientImpl;
class QueryIterator;

class QueryResult {
public:
    friend QueryIterator;

    QueryResult() = default;
    QueryResult(TuringClientImpl* client, uint64_t qid);
    ~QueryResult();

    operator bool () const { return isValid(); }
    bool isValid() const { return _client; }

    QueryIterator begin();
    QueryIterator end();

private:
    TuringClientImpl* _client {nullptr};
    uint64_t _qid {0};
    SessionResponse* _response {nullptr};

    const PullResponse* fetch();
};

class QueryIterator {
public:
    using iterator_category = std::input_iterator_tag;
    using value_type = QueryRecord;
    using pointer = value_type*;
    using reference = value_type&;

    QueryIterator() = default;

    QueryIterator(QueryResult* result,
                  const PullResponse* pullResponse)
        : _result(result),
        _pullResponse(pullResponse)
    {
    }

    bool operator==(const QueryIterator& other) const {
        return _pullResponse == other._pullResponse
            && _currentRow == other._currentRow;
    }

    bool operator!=(const QueryIterator& other) const {
        return !(*this == other);
    }

    QueryRecord operator*() const;

    QueryIterator& operator++();

private:
    QueryResult* _result {nullptr};
    const PullResponse* _pullResponse {nullptr};
    size_t _currentRow {0};
};

}
