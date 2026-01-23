#pragma once

#include "JobSystem.h"
#include "Neo4JHttpRequest.h"

namespace db {
struct DBStats;

class QueryManager {
public:
    using Response = std::optional<std::string>;

    struct Query {
        std::unique_ptr<Neo4JHttpRequest> _request = nullptr;
        SharedFuture<Response> _future;
        std::string _response;
        std::mutex _mutex;
        bool _ready = false;
        bool _success = false;

        Query()
        {
        }

        explicit Query(std::unique_ptr<Neo4JHttpRequest> q)
            : _request(std::move(q))
        {
        }

        Query(Query&& other) noexcept
        {
            std::unique_lock guard(other._mutex);

            _request = std::move(other._request);
            _future = std::move(other._future);
            _response = std::move(other._response);
            _ready = other._ready;
            _success = other._success;
        }

        bool waitAnswer() {
            std::unique_lock guard(_mutex);

            if (!_ready) {
                auto res = _future.get();

                if (!res.has_value()) {
                    _ready = true;
                    _success = false;
                    return false;
                }
                _response = std::move(res.value());
                _success = true;
                _ready = true;
            }

            return _success;
        }
    };

    bool isServerRunning() const;

    Query dbStatsQuery() const;
    Query nodeLabelsQuery() const;
    Query nodeLabelSetsQuery() const;
    Query edgeTypesQuery() const;
    Query nodePropertiesQuery() const;
    Query edgePropertiesQuery() const;
    void nodesQueries(size_t nodeCount,
                      size_t countPerQuery,
                      std::vector<Query>& result) const;
    void edgesQueries(size_t edgeCount,
                      size_t countPerQuery,
                      std::vector<Query>& result) const;

    void setUrl(std::string url) { _url = std::move(url); }
    void setUrlSuffix(std::string urlSuffix) { _urlSuffix = std::move(urlSuffix); }
    void setUsername(std::string username) { _username = std::move(username); }
    void setPassword(std::string password) { _password = std::move(password); }
    void setPort(uint64_t port) { _port = port; }

private:
    std::string _url = "localhost";
    std::string _urlSuffix = "/db/data/transaction/commit";
    std::string _username = "neo4j";
    std::string _password = "turing";
    uint64_t _port = 7474;
};

}
