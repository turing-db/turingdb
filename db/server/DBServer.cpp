#include "DBServer.h"

#include "DBServerConfig.h"
#include "Server.h"
#include "DBServerHandler.h"
#include "Interpreter.h"
#include "InterpreterContext.h"
#include "DBManager.h"

using namespace db;
using namespace net;

DBServer::DBServer(const DBServerConfig& serverConfig)
    : _config(serverConfig)
{
    _dbMan = new DBManager(_config.getDatabasesPath());
    _interpCtxt = new InterpreterContext(_dbMan);
    _interp = new Interpreter(_interpCtxt);
}

DBServer::~DBServer() {
    if (_interp) {
        delete _interp;
    }

    if (_interpCtxt) {
        delete _interpCtxt;
    }

    if (_dbMan) {
        delete _dbMan;
    }
}

void DBServer::start() {
    DBServerHandler serverHandler(_interp);
    Server server(&serverHandler, _config.getServerConfig());
    server.start();
}
