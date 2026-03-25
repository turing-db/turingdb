#pragma once

#include <string>
#include <memory>
#include <thread>

namespace net {
class TCPServer;
}

namespace db {

class VisualizerProxy {
public:
    VisualizerProxy(const std::string& address, uint32_t proxyPort, uint32_t dbPort);
    ~VisualizerProxy();

    void start();
    void stop();
    void wait();

private:
    std::string _address;
    uint32_t _proxyPort {0};
    uint32_t _dbPort {0};
    std::unique_ptr<net::TCPServer> _server;
    std::thread _serverThread;
};

}
