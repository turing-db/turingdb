// AFL++ / stdin fuzzing harness for the TuringDB HTTP server. The input is one recv() on a
// connection, handled as TCPConnectionManager handles it: HTTPParser<DBURIParser>, then
// DBServerProcessor runs the query on SimpleGraph data and writes the JSON response to a socket.
// The server answers internal errors with a JSON error, so the harness aborts on those and on invalid JSON.

#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <unistd.h>
#include <algorithm>
#include <charconv>
#include <memory>
#include <string>
#include <string_view>
#include <thread>

#include <nlohmann/json.hpp>

#include "TuringTestEnv.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"

#include "DBServerProcessor.h"
#include "DBThreadContext.h"
#include "DBURIParser.h"
#include "HTTPParser.h"
#include "HTTPWriter.h"
#include "TCPConnection.h"

#include "BioAssert.h"

using namespace turing::test;

namespace {

std::unique_ptr<TuringTestEnv> g_env;
std::unique_ptr<db::DBThreadContext> g_threadContext;
std::unique_ptr<net::TCPConnection> g_connection;
std::string g_rootDirectory;

void initOnce() {
    if (g_env) {
        return;
    }

    g_rootDirectory = "/tmp/fuzz_http_parser_" + std::to_string(getpid());

    g_env = TuringTestEnv::create(fs::Path(g_rootDirectory));
    db::SystemAccessor system = g_env->getSystemManager().accessUnique();
    db::SimpleGraph::createSimpleGraph(system.getGraph("default"));

    g_threadContext = std::make_unique<db::DBThreadContext>();

    g_connection = std::make_unique<net::TCPConnection>();
    g_connection->setWriter(std::make_unique<net::HTTPWriter>());
    g_connection->setParser(std::make_unique<net::HTTPParser<db::DBURIParser>>(&g_connection->getInputBuffer()));
}

void tearDown() {
    g_connection.reset();
    g_threadContext.reset();
    g_env.reset();
    fs::Path(g_rootDirectory).rm();
}

void processReceivedBytes(const char* data, size_t size) {
    net::TCPConnection& connection = *g_connection;

    auto inputWriter = connection.getInputBuffer().getWriter();
    const size_t bytesRead = std::min(size, inputWriter.getBufferSize());
    inputWriter.writeString(data, bytesRead);

    net::AbstractTCPParser& parser = connection.getParser();
    const auto analyzeRes = parser.analyze();
    net::AbstractTCPWriter& writer = connection.getWriter();

    if (!analyzeRes) {
        parser.handleAnalyzeError(analyzeRes.error(), writer);
    } else if (analyzeRes.value()) {
        db::DBServerProcessor processor(g_env->getDB(), connection);
        processor.process(g_threadContext.get());

        if (writer.getBytesWritten() != 0) {
            writer.flush();
        }

        if (writer.wroteNonEmptyChunk()) {
            writer.flush();
        }
    }

    writer.reset();
    parser.reset();
    inputWriter.reset();
}

void readResponse(int socket, std::string& response) {
    char buffer[64 * 1024];

    while (true) {
        const ssize_t received = recv(socket, buffer, sizeof(buffer), 0);
        if (received <= 0) {
            return;
        }

        response.append(buffer, received);
    }
}

void decodeChunkedBody(std::string_view encoded, std::string& body) {
    while (true) {
        const size_t sizeEnd = encoded.find("\r\n");
        bioassert(sizeEnd != std::string_view::npos, "The response has an unterminated chunk size");

        const char* sizeBegin = encoded.data();
        size_t chunkSize = 0;
        const auto [sizeParsedUntil, sizeError] = std::from_chars(sizeBegin, sizeBegin + sizeEnd, chunkSize, 16);
        bioassert(sizeError == std::errc() && sizeParsedUntil == sizeBegin + sizeEnd,
                  "The response has a malformed chunk size");

        encoded.remove_prefix(sizeEnd + 2);

        const bool chunkComplete = encoded.size() >= chunkSize + 2 && encoded.substr(chunkSize, 2) == "\r\n";
        bioassert(chunkComplete, "The response has a truncated chunk");

        if (chunkSize == 0) {
            bioassert(encoded.size() == 2, "The response has bytes after its last chunk");
            return;
        }

        body.append(encoded.substr(0, chunkSize));
        encoded.remove_prefix(chunkSize + 2);
    }
}

void checkResponse(std::string_view response) {
    if (!response.starts_with("HTTP/1.1 200 OK\r\n")) {
        return;
    }

    const size_t headerEnd = response.find("\r\n\r\n");
    bioassert(headerEnd != std::string_view::npos, "The response header is not terminated");

    std::string body;
    decodeChunkedBody(response.substr(headerEnd + 4), body);

    const nlohmann::json json = nlohmann::json::parse(body, nullptr, false);
    bioassert(!json.is_discarded(), "The response body is not valid JSON:\n{}", body);

    const auto errorDetails = json.find("error_details");
    if (errorDetails == json.end() || !errorDetails->is_string()) {
        return;
    }

    const std::string_view message = errorDetails->get_ref<const std::string&>();
    const bool internalError = message.starts_with("Unexpected exception") || message == "Unknown exception occurred";
    bioassert(!internalError, "The server answered with an internal error: {}", message);
}

int fuzzOne(const char* data, size_t size) {
    int sockets[2] {};
    const int created = socketpair(AF_UNIX, SOCK_STREAM, 0, sockets);
    bioassert(created == 0, "Could not create the socket pair");

    const int serverSocket = sockets[0];
    const int clientSocket = sockets[1];

    std::string response;
    std::thread client([clientSocket, &response] { readResponse(clientSocket, response); });

    g_connection->setSocket(serverSocket);
    processReceivedBytes(data, size);

    close(serverSocket);
    client.join();
    close(clientSocket);

    checkResponse(response);

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
