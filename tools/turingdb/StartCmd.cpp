#include "StartCmd.h"

#include <spdlog/spdlog.h>
#include <argparse.hpp>

#include <sys/socket.h>
#include <sys/un.h>
#include <limits.h>
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
#include "SystemEventHandler.h"
#include "BannerDisplay.h"
#include "BuildInfo.h"
#include "DateTimeFmt.h"

#include "TuringException.h"

using namespace db;

#define STRINGIFY(x) #x
#define TOSTRING(x) STRINGIFY(x)

namespace {

void TuringDBCommitInfo() {
    std::cout << "TuringDB - " << TOSTRING(HEAD_COMMIT_HASH)
              << " - " << formatUnixTime(BUILD_TIMESTAMP) << "\n\n";
}

enum class PingResult {
    Pong,
    Init,
    NoResponse,
};

PingResult pingServer(const fs::Path& socketPath) {
    // Socket file not created yet, or does not exist
    if (!socketPath.exists()) {
        return PingResult::NoResponse;
    }

    // Could not create socket
    const int sockFd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (sockFd < 0) {
        return PingResult::NoResponse;
    }

    // chdir to socket directory to stay within the sun_path length limit
    char savedCwd[PATH_MAX];
    if (::getcwd(savedCwd, sizeof(savedCwd)) == nullptr) {
        ::close(sockFd);
        return PingResult::NoResponse;
    }

    if (::chdir(socketPath.parent().c_str()) != 0) {
        ::close(sockFd);
        return PingResult::NoResponse;
    }

    sockaddr_un addr {0};
    addr.sun_family = AF_UNIX;
    const std::string sockName {socketPath.filename()};
    strncpy(addr.sun_path, sockName.c_str(), sizeof(addr.sun_path) - 1);

    const int res = ::connect(sockFd, (sockaddr*)&addr, sizeof(addr));

    [[maybe_unused]] const int cwdRes = ::chdir(savedCwd);

    // Could not connect to the socket
    if (res < 0) {
        ::close(sockFd);
        return PingResult::NoResponse;
    }

    constexpr std::string_view ping = "PING";
    if (::write(sockFd, ping.data(), ping.size()) != (ssize_t)ping.size()) {
        ::close(sockFd);
        return PingResult::NoResponse;
    }

    char buf[5] {};
    const ssize_t nread = ::read(sockFd, buf, sizeof(buf) - 1);
    ::close(sockFd);

    if (nread <= 0) {
        return PingResult::NoResponse;
    }

    const std::string_view response {buf, (size_t)nread};

    if (response == "PONG") {
        return PingResult::Pong;
    }

    if (response == "INIT") {
        return PingResult::Init;
    }

    return PingResult::NoResponse;
}

// Waits for the server to signal readiness via PING/INIT/PONG.
// initTimeoutMs: max time to wait for at least an INIT response.
// Once INIT is received, waits indefinitely for PONG.
bool waitForReady(const fs::Path& socketPath, size_t initTimeoutMs) {
    const auto initDeadline = std::chrono::steady_clock::now()
                            + std::chrono::milliseconds(initTimeoutMs);

    constexpr size_t intervalMs = 50;

    bool initReceived = false;

    while (true) {
        const PingResult result = pingServer(socketPath);

        if (result == PingResult::Pong) {
            return true;
        }
        if (result == PingResult::Init) {
            initReceived = true;
        }

        if (!initReceived && std::chrono::steady_clock::now() >= initDeadline) {
            return false;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs));
    }
}

}

StartCmd::StartCmd()
    : _argParser("start")
{
}

StartCmd::~StartCmd() = default;

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

    BannerDisplay::printBanner();
    TuringDBCommitInfo();

    if (_demonize) {
        LockFile lockFile;
        lockFile.setPath(config.getLockFilePath());

        const auto pid = lockFile.getOwningProcess();

        if (pid) {
            spdlog::error("TuringDB is already running (PID: {})", *pid);
            return EXIT_FAILURE;
        } else if (pid.error().getType() != LockFileErrorType::NOT_LOCKED) {
            spdlog::error("Could not read PID from lock file: {}", pid.error().fmtMessage());
            return EXIT_FAILURE;
        }

        const DemonResult demonResult = Demonology::demonize();

        // Process was forked, we are now the parent
        // We need to wait for the server to be ready
        // before exiting. This ensures that the HTTP server
        // is ready to accept connections.

        if (demonResult == DemonResult::Intermediate) {
            return EXIT_SUCCESS;
        }

        if (demonResult == DemonResult::Parent) {
            if (!waitForReady(config.getSocketPath(), _startTimeout)) {
                spdlog::error("TuringDB failed to start");
                spdlog::error("Check the logs at {}/turingdb.log for more information",
                              config.getLogsDir().get());
                return EXIT_FAILURE;
            }

            // Server replied "PONG" -> ready

            spdlog::info("TuringDB is ready");
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
            SystemEventHandler::setReady();
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
        .help("Milliseconds to wait for the first INIT response when daemonizing (default: 500)")
        .store_into(_startTimeout);
}
