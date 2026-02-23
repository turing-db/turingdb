#include "StartCmd.h"

#include <spdlog/spdlog.h>
#include <argparse.hpp>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <chrono>
#include <thread>

#include "TuringConfig.h"
#include "TuringDB.h"
#include "LocalMemory.h"
#include "TuringShell.h"
#include "TuringServer.h"
#include "DBServerConfig.h"
#include "Demonology.h"
#include "LogSetup.h"

#include "TuringException.h"

using namespace db;

StartCmd::StartCmd()
    : _argParser("start")
{
}

StartCmd::~StartCmd() = default;

static bool waitForPort(const std::string& address, unsigned port, size_t timeoutMs) {
    const auto deadline = std::chrono::steady_clock::now()
                          + std::chrono::seconds(timeoutMs);

    constexpr size_t intervalMs = 100;

    while (std::chrono::steady_clock::now() < deadline) {
        const int sockFd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (sockFd < 0) {
            break;
        }

        sockaddr_in addr {};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        ::inet_pton(AF_INET, address.c_str(), &addr.sin_addr);

        const int res = ::connect(sockFd, (sockaddr*)&addr, sizeof(addr));
        ::close(sockFd);

        if (res == 0) {
            return true;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs));
    }

    return false;
}

int StartCmd::execute() {
    LogSetup::setupLogConsole();

    // Config
    TuringConfig config;
    config.setSyncedOnDisk(!_inMemory);

    if (!_turingDir.empty()) {
        fs::Path absTuringDir(_turingDir);

        if (!absTuringDir.toAbsolute()) {
            spdlog::error("Failed to get absolute path of turing directory {}", _turingDir);
            return EXIT_FAILURE;
        }

        config.setTuringDirectory(absTuringDir);
    }

    const fs::Path& turingDir = config.getTuringDir();
    const fs::Path& graphsDir = config.getGraphsDir();
    const fs::Path& logsDir = config.getLogsDir();

    if (_demonize) {
        const DemonResult demonResult = Demonology::demonize();

        if (demonResult == DemonResult::Intermediate) {
            return EXIT_SUCCESS;
        }

        if (demonResult == DemonResult::Parent) {
            if (!waitForPort(_address, _port, _startTimeout)) {
                spdlog::error("TuringDB did not start within {} s", _startTimeout);
                return EXIT_FAILURE;
            }
            spdlog::info("TuringDB is ready on port {}", _port);
            return EXIT_SUCCESS;
        }
    }

    try {
        std::unique_ptr<TuringServer> server;
        std::unique_ptr<TuringShell> shell;

        // Run TuringDB
        LocalMemory mem;
        TuringDB turingDB(&config);

        config.setOnStopRequest([&] {
            if (shell) {
                shell->stop();
            } else if (server) {
                server->stop();
            }
        });

        turingDB.init();
        LogSetup::setupLogFileBacked((logsDir / "turingdb.log").get(), false);

        // Delete existing `default` graph if requested
        if (_resetDefault) {
            spdlog::info("Resetting default graph.");
            spdlog::info("Searching for default in {}.", graphsDir.get());

            const fs::Path defaultGraphPath = config.getGraphsDir() / "default";

            if (defaultGraphPath.exists()) {
                defaultGraphPath.rm();
                spdlog::info("Default graph deleted.");
            } else {
                spdlog::warn("Default graph not found.");
            }
        }

        // Load graphs
        for (const auto& graphName : _graphsToLoad) {
            const QueryStatus res = turingDB.query("load graph " + graphName, "", &mem);
            if (!res.isOk()) {
                return EXIT_FAILURE;
            }
        }

        // Server config
        DBServerConfig serverConfig;
        serverConfig.setPort(_port);
        serverConfig.setAddress(_address);

        spdlog::info("TuringDB started");
        spdlog::info("  - Home directory: {}", turingDir.get());

        server = std::make_unique<TuringServer>(serverConfig, turingDB);
        server->start();

        if (_demonize) {
            server->wait();
            server.reset();
        } else {
            shell = std::make_unique<TuringShell>(turingDB, &mem);

            if (!_graphsToLoad.empty()) {
                shell->setGraphName(_graphsToLoad.front());
            }

            shell->startLoop();
            shell.reset();

            server->stop();
            server->wait();
            server.reset();
        }

    } catch (TuringException& e) {
        spdlog::error("{}", e.what());
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

std::unique_ptr<StartCmd> StartCmd::create() {
    std::unique_ptr<StartCmd> cmd {new StartCmd()};

    cmd->initialize();

    return cmd;
}

void StartCmd::initialize() {
    _argParser.add_description("Starts the TuringDB server");

    _argParser.add_argument("-turing-dir")
        .metavar("path")
        .help("Root Turing directory")
        .store_into(_turingDir);
    _argParser.add_argument("-p")
        .metavar("port")
        .help("Server listen port")
        .store_into(_port);
    _argParser.add_argument("-i")
        .metavar("addr")
        .help("Server listen address (localhost by default)")
        .store_into(_address);
    _argParser.add_argument("-demon")
        .help("Launch TuringDB as a daemon in the background")
        .store_into(_demonize);
    _argParser.add_argument("-reset-default")
        .help("Reset the content of the default graph")
        .store_into(_resetDefault);
    _argParser.add_argument("-load")
        .metavar("graph_name")
        .help("Load a graph at startup")
        .store_into(_graphsToLoad);
    _argParser.add_argument("-in-memory")
        .help("Run turingdb in-memory only without writing graphs on disk")
        .store_into(_inMemory);
    _argParser.add_argument("-start-timeout")
        .metavar("seconds")
        .help("Milliseconds to wait for the HTTP server to become ready when daemonizing (default: 30)")
        .store_into(_startTimeout);
}
