#pragma once

namespace db {

class DBServerConfig;
class TuringDB;

class TuringServer {
public:
    TuringServer(const DBServerConfig& config, TuringDB& db);
    ~TuringServer();

    bool start();
    
private:
    const DBServerConfig& _config;
    TuringDB& _db;
};

}
