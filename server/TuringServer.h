#pragma once

#include <memory>
#include <thread>

namespace net {
    class HTTPServer;
}

namespace db {

class DBServerConfig;
class TuringDB;

class TuringServer {
public:
    TuringServer(const DBServerConfig& config, TuringDB& db);
    ~TuringServer();

    void start();

    void wait();
    void stop();
    
private:
    const DBServerConfig& _config;
    TuringDB& _db;
    std::unique_ptr<net::HTTPServer> _server;
    std::thread _serverThread;
};

}
