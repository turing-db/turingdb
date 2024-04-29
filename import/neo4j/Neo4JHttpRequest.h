#pragma once

#include <condition_variable>
#include <filesystem>
#include <mutex>

class Neo4JHttpRequest {
public:
    struct RequestProps {
        std::string statement;
        std::string host = "localhost";
        std::string user = "neo4j";
        std::string password = "turing";
        uint16_t port = 7474;
        bool silent = false;
    };

    explicit Neo4JHttpRequest(const std::string& statement);
    Neo4JHttpRequest(const Neo4JHttpRequest&) = delete;
    Neo4JHttpRequest(Neo4JHttpRequest&&);
    ~Neo4JHttpRequest();

    void setStatement(const std::string& s) { _statement = s; }
    void setUrl(const std::string& url) { _url = url; };
    void setUrlSuffix(const std::string& urlSuffix) { _urlSuffix = urlSuffix; };
    void setUsername(const std::string& username) { _username = username; };
    void setPassword(const std::string& password) { _password = password; };
    void setPort(uint64_t port) { _port = port; };

    void exec();
    bool writeToFile(const std::filesystem::path& path) const;
    void clear();
    void reportError() const;
    void setReady();
    void waitReady();

    const std::string& getData() const { return _data; }

    bool success() const { return _result; }

private:
    std::string _url;
    std::string _urlSuffix;
    std::string _username;
    std::string _password;
    uint64_t _port = 7474;
    std::string _statement;
    std::string _jsonRequest;
    std::string _data;
    std::mutex _readyMutex;
    std::condition_variable _readyCond;
    bool _isReady {false};
    bool _result {false};
    bool _silent {false};
};
