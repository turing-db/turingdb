#include "QueryResult.h"

#include "TuringClientImpl.h"

using namespace turing::db::client;

QueryIterator& QueryIterator::operator++() {
    if (_currentRow >= (size_t)_pullResponse->records().size()) {
        _pullResponse = _result->fetch();
        _currentRow = 0;
    } else {
        _currentRow++;
    }

    return *this;
}

QueryRecord QueryIterator::operator*() const {
    return QueryRecord(_pullResponse->records()[_currentRow]);
}

QueryResult::QueryResult(TuringClientImpl* client, uint64_t qid)
    : _client(client),
    _qid(qid)
{

    _response = new SessionResponse();
}

QueryResult::~QueryResult() {
    if (_response) {
        delete _response;
    }
}

const PullResponse* QueryResult::fetch() {
    if (!_client->fetch(_response, _qid)) {
        return nullptr;
    }

    if (!_response->has_pull_res()) {
        return nullptr;
    }

    return &_response->pull_res();
}

QueryIterator QueryResult::begin() {
    const PullResponse* pullResponse = fetch();
    if (!pullResponse) {
        return QueryIterator();
    }

    return QueryIterator(this, pullResponse);
}

QueryIterator QueryResult::end() {
    return QueryIterator();
}
