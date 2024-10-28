#include "AbstractThreadContext.h"
#include "HTTPParser.h"
#include "Server.h"

#include <spdlog/spdlog.h>
#include <argparse.hpp>

#include "TCPConnection.h"
#include "ToolInit.h"

class EmptyContext : public net::AbstractThreadContext {};

int main(int argc, const char** argv) {
    ToolInit toolInit("simple_server");
    toolInit.disableOutputDir();

    bool startServer = false;
    auto& argparser = toolInit.getArgParser();
    argparser.add_argument("-s")
             .help("Start server")
             .store_into(startServer);

    toolInit.init(argc, argv);

    if (!startServer) {
        spdlog::info("Execute with -s to start the server");
        return 0;
    }

    using Parser = net::HTTPParser<net::URIParser>;

    net::Server::Functions functions {
        ._processor =
            [&](net::AbstractThreadContext*, net::TCPConnection& con) {
                const auto& httpInfo = con.getParser()->getHttpInfo();

                if (httpInfo._path == "/say-hello") {
                    auto& writer = con.getWriter();
                    writer.setFirstLine(net::HTTP::Status::OK);
                    writer.addConnection(net::getConnectionHeader(con.isCloseRequired()));
                    writer.addChunkedTransferEncoding();
                    writer.flushHeader();

                    writer.write("Hello!");
                    writer.flush();
                    writer.flush(); // Flush empty to end response
                    return;
                }

                if (httpInfo._path == "/say-goodbye") {
                    auto& writer = con.getWriter();
                    writer.setFirstLine(net::HTTP::Status::OK);
                    writer.addConnection(net::getConnectionHeader(con.isCloseRequired()));
                    writer.addChunkedTransferEncoding();
                    writer.flushHeader();

                    writer.write("Goodbye!");
                    writer.flush();
                    writer.flush(); // Flush empty to end response
                    return;
                }

                auto& writer = con.getWriter();
                writer.setFirstLine(net::HTTP::Status::NOT_FOUND);
                writer.addConnection(net::getConnectionHeader(con.isCloseRequired()));
                writer.addChunkedTransferEncoding();
                writer.flushHeader();

                writer.flush();
            },
        ._createThreadContext =[] {
                return std::unique_ptr<net::AbstractThreadContext>(new EmptyContext);
            },
        ._createHttpParser =
            [](Buffer* buf) {
                return std::unique_ptr<net::AbstractHTTPParser>(new Parser(buf));
            },
    };

    net::Server server(std::move(functions));

    server.setAddress("127.0.0.1");
    server.setPort(6665);
    server.setWorkerCount(16);
    server.setMaxConnections(1024);

    if (auto res = server.initialize(); res != net::FlowStatus::OK) {
        spdlog::error("Could not initialize server: {}", (uint32_t)res);
        server.terminate();
        return 1;
    }

    spdlog::info("Server listening on address: {}:{}", server.getAddress(), server.getPort());

    if (auto res = server.start(); res != net::FlowStatus::OK) {
        spdlog::error("Could not start server: {}", (uint32_t)res);
        server.terminate();
        return 1;
    };

    server.terminate();

    return 0;
}
