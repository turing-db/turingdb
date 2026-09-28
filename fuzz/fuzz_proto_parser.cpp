// AFL++ / stdin fuzzing harness for the TuringDB binary protocol. The input is a Cypher query
// string. A real TuringServer runs in-process in binary-protocol mode over SimpleGraph data, and
// net::proto::TuringClient sends the query and decodes the response — the harness does no framing
// or decoding of its own. An internal error the server reports in the decoded status aborts, and
// any exception the client raises on an unreadable response crashes the process for AFL to report.

#include <stdlib.h>
#include <string.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <chrono>
#include <memory>
#include <string>
#include <string_view>
#include <thread>

#include "TuringTestEnv.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"

#include "DBServerConfig.h"
#include "TuringServer.h"
#include "TuringClient.h"
#include "TuringProtoHeaders.h"
#include "QueryStatus.h"
#include "dataframe/Dataframe.h"

#include "BioAssert.h"

using namespace turing::test;

namespace {

std::unique_ptr<TuringTestEnv> g_env;
std::unique_ptr<db::TuringServer> g_server;
std::unique_ptr<net::proto::TuringClient> g_client;
std::string g_rootDirectory;

uint16_t reserveFreePort() {
    const int socket = ::socket(AF_INET, SOCK_STREAM, 0);
    bioassert(socket >= 0, "Could not create a socket to reserve a port");

    sockaddr_in address {};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;

    const int bound = ::bind(socket, reinterpret_cast<const sockaddr*>(&address), sizeof(address));
    bioassert(bound == 0, "Could not bind a socket to reserve a port");

    sockaddr_in boundAddress {};
    socklen_t boundAddressLen = sizeof(boundAddress);
    ::getsockname(socket, reinterpret_cast<sockaddr*>(&boundAddress), &boundAddressLen);
    ::close(socket);

    return ntohs(boundAddress.sin_port);
}

bool waitUntilListening(uint16_t port) {
    for (int attempt = 0; attempt < 200; attempt++) {
        const int socket = ::socket(AF_INET, SOCK_STREAM, 0);
        if (socket >= 0) {
            sockaddr_in address {};
            address.sin_family = AF_INET;
            address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
            address.sin_port = htons(port);

            const int connected = ::connect(socket, reinterpret_cast<const sockaddr*>(&address), sizeof(address));
            ::close(socket);

            if (connected == 0) {
                return true;
            }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    return false;
}

void initOnce() {
    if (g_env) {
        return;
    }

    g_rootDirectory = "/tmp/fuzz_proto_parser_" + std::to_string(getpid());

    g_env = TuringTestEnv::create(fs::Path(g_rootDirectory));
    db::SystemAccessor system = g_env->getSystemManager().accessUnique();
    db::SimpleGraph::createSimpleGraph(system.getGraph("default"));

    const uint16_t port = reserveFreePort();

    setenv("USE_TURING_PROTO", "1", 1);

    db::DBServerConfig serverConfig;
    serverConfig.setAddress("127.0.0.1");
    serverConfig.setPort(port);
    serverConfig.setWorkerCount(1);
    serverConfig.setMaxConnections(16);

    g_server = std::make_unique<db::TuringServer>(serverConfig, g_env->getDB());
    g_server->start();

    bioassert(waitUntilListening(port), "The in-process proto server never started listening");

    g_client = std::make_unique<net::proto::TuringClient>("127.0.0.1", std::to_string(port), &g_env->getMem());
    g_client->connect();
}

void tearDown() {
    if (g_client) {
        g_client->disconnect();
        g_client.reset();
    }

    if (g_server) {
        g_server->stop();
        g_server->wait();
        g_server.reset();
    }

    g_env.reset();
    fs::Path(g_rootDirectory).rm();
}

int fuzzOne(const char* data, size_t size) {
    if (size > net::proto::MAX_WIRE_SIZE) {
        size = net::proto::MAX_WIRE_SIZE;
    }

    const std::string query(data, size);

    const db::QueryStatus status = g_client->sendQuery(query, [](const db::Dataframe*) {});

    const std::string_view message = status.getError();
    const bool internalError = message.starts_with("Unexpected exception")
                            || message == "Unknown exception occurred";
    bioassert(!internalError, "The server answered with an internal error: {}", message);

    return 0;
}

}

#if defined(__AFL_COMPILER) && defined(__AFL_HAVE_MANUAL_CONTROL)
__AFL_FUZZ_INIT();

int main(int argc, char** argv) {
    __AFL_INIT();
    initOnce();

    unsigned char* buf = __AFL_FUZZ_TESTCASE_BUF;

    while (__AFL_LOOP(10000)) {
        const int len = __AFL_FUZZ_TESTCASE_LEN;
        fuzzOne(reinterpret_cast<const char*>(buf), len);
    }

    tearDown();

    return EXIT_SUCCESS;
}

#else
int main(int argc, char** argv) {
    initOnce();

    std::string input;
    char buf[4096];
    while (const size_t n = fread(buf, 1, sizeof(buf), stdin)) {
        input.append(buf, n);
    }

    const int status = fuzzOne(input.data(), input.size());

    tearDown();

    return status;
}
#endif
