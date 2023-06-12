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

    Neo4JHttpRequest(RequestProps&& props);
    Neo4JHttpRequest(std::string&& statement);
    Neo4JHttpRequest(const Neo4JHttpRequest&) = delete;
    Neo4JHttpRequest(Neo4JHttpRequest&&);
    ~Neo4JHttpRequest();

    void setStatement(const std::string& s) { _statement = s; }

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
    std::string _username;
    std::string _password;
    std::string _statement;
    std::string _jsonRequest;
    std::string _data;
    std::mutex _readyMutex;
    std::condition_variable _readyCond;
    bool _isReady {false};
    bool _result {false};
    bool _silent {false};
};
