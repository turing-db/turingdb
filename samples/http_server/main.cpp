#include "Server.h"

#include <spdlog/spdlog.h>
#include <argparse.hpp>

#include "LogUtils.h"
#include "TCPConnection.h"
#include "ToolInit.h"

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

    net::Server server;

    server.setProcessor([&](net::TCPConnection& con) {
        const auto& httpInfo = con.getParser().getHttpInfo();

        if (httpInfo._path == "/say-hello") {
            auto& writer = con.getWriter();
            writer.setFirstLine(net::HTTP::Status::OK);
            writer.addKeepAlive(!con.isCloseRequired());
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
            writer.addKeepAlive(!con.isCloseRequired());
            writer.addChunkedTransferEncoding();
            writer.flushHeader();

            writer.write("Goodbye!");
            writer.flush();
            writer.flush(); // Flush empty to end response
            return;
        }

        auto& writer = con.getWriter();
        writer.setFirstLine(net::HTTP::Status::NOT_FOUND);
        writer.addKeepAlive(!con.isCloseRequired());
        writer.addChunkedTransferEncoding();
        writer.flushHeader();

        writer.flush();
    });

    server.setAddress("127.0.0.1");
    server.setPort(6665);
    server.setWorkerCount(16);
    server.setMaxConnections(1024);

    if (auto res = server.initialize(); res != net::FlowStatus::OK) {
        logt::error("Could not initialize server: {}", (uint32_t)res);
        server.terminate();
        return 1;
    }

    logt::info("Server listening on address: {}:{}", server.getAddress(), server.getPort());

    if (auto res = server.start(); res != net::FlowStatus::OK) {
        logt::error("Could not start server: {}", (uint32_t)res);
        server.terminate();
        return 1;
    };

    server.terminate();

    return 0;
}
