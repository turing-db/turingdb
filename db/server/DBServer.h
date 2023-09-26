#pragma once

namespace db {

class DBServerConfig;
class DBManager;
class Interpreter;
class InterpreterContext;

class DBServer {
public:
    explicit DBServer(const DBServerConfig& serverConfig);
    ~DBServer();

    void start();

private:
    const DBServerConfig& _config;
    DBManager* _dbMan {nullptr};
    InterpreterContext* _interpCtxt {nullptr};
    Interpreter* _interp {nullptr};
};

}
