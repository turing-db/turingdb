#include "DBServer.h"

#include "DBServerConfig.h"
#include "Server.h"

using namespace turing::network;

class DBServerContext {
public:
    void process(Buffer* outBuffer, const std::string& uri) {
        auto writer = outBuffer->getWriter();

        strcpy(writer.getBuffer(), headerOk.c_str());
        writer.setWrittenBytes(headerOk.size()); 
        
        strcpy(writer.getBuffer(), emptyLine.c_str());
        writer.setWrittenBytes(emptyLine.size());

        strcpy(writer.getBuffer(), body.c_str());
        writer.setWrittenBytes(body.size());
    }

private:
    std::string headerOk =
        "HTTP/1.1 200 OK\r\n";
    std::string emptyLine = "\r\n";
    std::string body = "{\"data\":[],\"errors\":[]}";
};

DBServer::DBServer(const DBServerConfig& serverConfig)
    : _config(serverConfig)
{
}

DBServer::~DBServer() {
}

bool DBServer::run() {
    DBServerContext serverContext;
    Server server(&serverContext, _config.getServerConfig());
    server.start();
    return true;
}
