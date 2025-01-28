#pragma once

#include <string>

namespace db {

class Neo4JHttpRequest {
public:
    Neo4JHttpRequest() = default;
    ~Neo4JHttpRequest() = default;

    Neo4JHttpRequest(const Neo4JHttpRequest&) = delete;
    Neo4JHttpRequest(Neo4JHttpRequest&&) noexcept = default;
    Neo4JHttpRequest& operator=(const Neo4JHttpRequest&) = delete;
    Neo4JHttpRequest& operator=(Neo4JHttpRequest&&) noexcept = default;

    bool exec();

    void setStatement(std::string statement) { _statement = std::move(statement); }
    void setUrl(std::string url) { _url = std::move(url); }
    void setUrlSuffix(std::string urlSuffix) { _urlSuffix = std::move(urlSuffix); }
    void setPort(uint64_t port) { _port = port; }
    void setUsername(std::string username) { _username = std::move(username); }
    void setPassword(std::string password) { _password = std::move(password); }
    void setMethod(std::string method) { _method = std::move(method); }

    [[nodiscard]] const std::string& getResponse() const { return _response; }
    [[nodiscard]] std::string consumeResponse() { return std::move(_response); }

    static bool execStatic(std::string* response,
                           const std::string& url,
                           const std::string& urlSuffix,
                           const std::string& username,
                           const std::string& password,
                           uint64_t port,
                           std::string_view method,
                           const std::string& statement);

private:
    std::string _response;
    std::string _url = "localhost";
    std::string _urlSuffix = "/db/data/transaction/commmit";
    std::string _username = "neo4j";
    std::string _password = "turing";
    std::string _method = "POST";
    std::string _statement;
    uint64_t _port = 7474;
};

}
