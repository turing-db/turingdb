#include "VisualizerProxy.h"

#include <stdlib.h>
#include <curl/curl.h>
#include <spdlog/spdlog.h>

#include "TCPServer.h"
#include "HTTPParser.h"
#include "UriParser.h"
#include "AbstractThreadContext.h"
#include "TCPConnection.h"
#include "HTTP.h"

using namespace db;

namespace {

struct CurlResponse {
    std::string _body;
    std::string _contentType;
    long _statusCode {200};
};

size_t writeCallback(char* ptr, size_t size, size_t nmemb, void* userdata) {
    const size_t total = size * nmemb;
    auto* response = static_cast<CurlResponse*>(userdata);
    response->_body.append(ptr, total);
    return total;
}

size_t headerCallback(char* buffer, size_t size, size_t nitems, void* userdata) {
    const size_t total = size * nitems;
    auto* response = static_cast<CurlResponse*>(userdata);

    std::string_view line(buffer, total);

    // Extract Content-Type from upstream response headers for forwarding
    static constexpr std::string_view contentTypeKey = "content-type:";
    if (line.size() > contentTypeKey.size()) {
        const bool match = std::equal(contentTypeKey.begin(), contentTypeKey.end(),
                                line.begin(),
                                [](char a, char b) { return a == tolower(b); });
        if (match) {
            auto value = line.substr(contentTypeKey.size());
            // Trim leading whitespace
            while (!value.empty() && (value.front() == ' ' || value.front() == '\t')) {
                value.remove_prefix(1);
            }
            // Trim trailing \r\n
            while (!value.empty() && (value.back() == '\r' || value.back() == '\n')) {
                value.remove_suffix(1);
            }
            response->_contentType = std::string(value);
        }
    }

    return total;
}

}

VisualizerProxy::VisualizerProxy(const std::string& address,
                                 uint32_t proxyPort,
                                 uint32_t dbPort)
    : _address(address),
    _proxyPort(proxyPort),
    _dbPort(dbPort)
{
    curl_global_init(CURL_GLOBAL_DEFAULT);
}

VisualizerProxy::~VisualizerProxy() {
    if (_server) {
        _server->terminate();
    }
    curl_global_cleanup();
}

void VisualizerProxy::start() {
    const std::string dbBaseUrl = "http://" + _address + ":" + std::to_string(_dbPort);

    const char* visEnv = getenv("TURINGDB_VIS_URL");
    const std::string visBaseUrl = visEnv ? visEnv : "https://vis.turingdb.org";

    net::TCPServer::Functions functions;
    functions._processor =
        [dbBaseUrl, visBaseUrl](net::AbstractThreadContext*, net::TCPConnection& connection) {
            auto& parser = connection.getParser<net::HTTPParser<net::URIParser>>();
            const auto& httpInfo = parser.getHttpInfo();

            const std::string_view path = httpInfo._path;
            const std::string_view uri = httpInfo._uri;
            const std::string_view payload = httpInfo._payload;
            const bool isPost = (httpInfo._method == net::HTTP::Method::POST);

            // Build target URL (use uri to preserve query parameters)
            std::string targetUrl;
            static constexpr std::string_view apiPrefix = "/api/";
            static constexpr std::string_view apiPrefixExact = "/api";

            if (path.starts_with(apiPrefix)) {
                // /api/foo?bar=baz → <dbBaseUrl>/foo?bar=baz
                targetUrl = dbBaseUrl + "/" + std::string(uri.substr(apiPrefix.size()));
            } else if (path == apiPrefixExact) {
                targetUrl = dbBaseUrl + "/" + std::string(uri.substr(apiPrefixExact.size()));
            } else {
                targetUrl = visBaseUrl + std::string(uri);
            }

            // Perform curl request
            CURL* curl = curl_easy_init();
            if (!curl) {
                auto& writer = connection.getWriter<net::HTTPWriter>();
                writer.setFirstLine(net::HTTP::Status::INTERNAL_SERVER_ERROR);
                writer.addConnection(net::ConnectionHeader::CLOSE);
                writer.addChunkedTransferEncoding();
                writer.flushHeader();
                writer.flush();
                return;
            }

            CurlResponse response;

            curl_easy_setopt(curl, CURLOPT_URL, targetUrl.c_str());
            curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeCallback);
            curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
            curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, headerCallback);
            curl_easy_setopt(curl, CURLOPT_HEADERDATA, &response);
            curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
            curl_easy_setopt(curl, CURLOPT_TIMEOUT, 30L);

            if (isPost) {
                curl_easy_setopt(curl, CURLOPT_POST, 1L);
                curl_easy_setopt(curl, CURLOPT_POSTFIELDS, payload.data());
                curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, (long)payload.size());
            }

            const CURLcode res = curl_easy_perform(curl);
            auto& writer = connection.getWriter<net::HTTPWriter>();

            if (res != CURLE_OK) {
                spdlog::warn("Proxy curl error: {}", curl_easy_strerror(res));
                curl_easy_cleanup(curl);

                writer.setFirstLine(net::HTTP::Status::BAD_GATEWAY);
                writer.addConnection(net::ConnectionHeader::CLOSE);
                writer.addChunkedTransferEncoding();
                writer.flushHeader();
                writer.flush();
                return;
            }

            curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response._statusCode);
            curl_easy_cleanup(curl);

            // Write response
            const auto status = net::HTTP::codeToStatus(response._statusCode);
            writer.setFirstLine(status);
            writer.addConnection(net::ConnectionHeader::CLOSE);

            if (!response._contentType.empty()) {
                const std::string header = "Content-type: " + response._contentType;
                writer.addRawHeader(header);
            }

            writer.addChunkedTransferEncoding();
            writer.flushHeader();
            writer.write(response._body);
            writer.flush();
        };

    functions._createThreadContext =
        [] {
            return std::make_unique<net::AbstractThreadContext>();
        };

    functions._createParser =
        [](net::NetBuffer* inputBuffer, net::BaseConnectionState* /*state*/) {
            return std::unique_ptr<net::AbstractTCPParser>(
                new net::HTTPParser<net::URIParser>(inputBuffer));
        };
    functions._createWriter =
        [](net::BaseConnectionState* /*state*/) {
            return std::make_unique<net::HTTPWriter>();
        };

    _server = std::make_unique<net::TCPServer>(std::move(functions));
    _server->setName("Visualizer");
    _server->setAddress(_address.c_str());
    _server->setPort(_proxyPort);
    _server->setWorkerCount(1);
    _server->setMaxConnections(64);

    const auto initRes = _server->initialize();
    if (initRes != net::FlowStatus::OK) {
        spdlog::error("Failed to initialize visualizer proxy on port {}", _proxyPort);
        _server->terminate();
        return;
    }

    const auto serverFunc = [this]() {
        const auto startRes = _server->start();
        if (startRes != net::FlowStatus::OK) {
            _server->terminate();
            return;
        }
    };

    _serverThread = std::thread(serverFunc);

    spdlog::info("Visualizer available at http://{}:{}", _address, _proxyPort);
}

void VisualizerProxy::wait() {
    if (_serverThread.joinable()) {
        _serverThread.join();
    }
    _server->terminate();
}

void VisualizerProxy::stop() {
    _server->terminate();
    if (_serverThread.joinable()) {
        _serverThread.join();
    }
}
