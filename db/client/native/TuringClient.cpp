#include "TuringClient.h"

#include "TuringClientImpl.h"

using namespace turing::db::client;

TuringClient::TuringClient()
{
    _impl = new TuringClientImpl(_config);
}

TuringClient::~TuringClient() {
    if (_impl) {
        delete _impl;
    }
}

bool TuringClient::connect() {
    return _impl->connect();
}

QueryResult TuringClient::exec(const std::string& query) {
    return _impl->exec(query);
}
