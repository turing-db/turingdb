#include "StartCmd.h"

#include <spdlog/spdlog.h>
#include <argparse.hpp>

#include <stdlib.h>
#include <stop_token>

#include "LineNoiseHandle.h"
#include "TuringConfig.h"
#include "TuringDB.h"
#include "LocalMemory.h"
#include "CompilerContext.h"
#include "TuringShell.h"
#include "TuringServer.h"
#include "VisualizerProxy.h"
#include "DBServerConfig.h"
#include "Authenticator.h"
#include "Demonology.h"
#include "DemonReadiness.h"
#include "LogSetup.h"
#include "LockFile.h"
#include "LockFile.h"
#include "BannerDisplay.h"
#include "BuildInfo.h"
#include "DateTimeFmt.h"

#include "TuringException.h"

using namespace db;

#define STRINGIFY(x) #x
#define TOSTRING(x) STRINGIFY(x)

namespace {

void printTuringDBCommitInfo() {
    std::cout << "TuringDB - " << TOSTRING(HEAD_COMMIT_HASH)
              << " - " << formatUnixTime(BUILD_TIMESTAMP) << "\n\n";
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
    config.setMmapVectorIndexes(!_noVectorMmap);

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
    printTuringDBCommitInfo();

    DemonReadiness readiness;

    try {
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

            const DemonResult demonResult = Demonology::demonize(&readiness);

            // Process was forked, we are now the parent
            // We need to wait for the server to be ready
            // before exiting. This ensures that the HTTP server
            // is ready to accept connections.

            if (demonResult == DemonResult::Intermediate) {
                return EXIT_SUCCESS;
            }

            if (demonResult == DemonResult::Parent) {
                std::string error;
                const DemonStartResult startResult = readiness.waitForDemon(error);

                if (startResult == DemonStartResult::Ready) {
                    spdlog::info("TuringDB is ready");
                    return EXIT_SUCCESS;
                } else if (startResult == DemonStartResult::Failed) {
                    spdlog::error("{}", error);
                    spdlog::error("TuringDB failed to start");
                } else {
                    spdlog::error("TuringDB failed to start");
                    spdlog::error("Check the logs at {}/turingdb.log for more information", logsDir.get());
                }

                return EXIT_FAILURE;
            }
        }

        // Authentication is opt-in via -auth-on. When enabled, the API key is
        // read from the TURINGDB_AUTH_TOKEN environment variable; clients
        // present it directly in the Authorization header. The Authenticator
        // must outlive the server, so it is declared before it: locals are
        // destroyed in reverse declaration order, so on any path the server
        // is torn down before the Authenticator it dereferences.
        std::unique_ptr<Authenticator> authenticator;
        std::stop_source stopSource;

        // Run TuringDB
        LocalMemory mem;
        CompilerContext compilerContext;
        TuringDB turingDB(&config);

        if (_authOn) {
            const char* envKey = getenv("TURINGDB_AUTH_TOKEN");
            if (envKey == nullptr || *envKey == '\0') {
                throw TuringException("-auth-on requires the TURINGDB_AUTH_TOKEN environment variable to be set");
            }

            authenticator = std::make_unique<Authenticator>(envKey);
            turingDB.setAuthenticator(authenticator.get());
            spdlog::info("Authentication enabled (API key from TURINGDB_AUTH_TOKEN)");
        }

        config.setOnStopRequest([&stopSource] {
            stopSource.request_stop();
        });

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

        turingDB.init();
        auto consoleSink = LogSetup::setupLogFileBacked((logsDir / "turingdb.log").get(), false);

        // Load graphs
        const auto& queryConfig = turingDB.getDefaultQueryConfig();
        for (const auto& graphName : _graphsToLoad) {
            const QueryState state("", &mem, &compilerContext, &queryConfig, nullptr);
            const QueryStatus res = turingDB.query("load graph " + graphName, state);
            if (!res.isOk()) {
                throw TuringException(fmt::format("{}: {}", QueryStatusDescription::value(res.getStatus()), res.getError()));
            }
        }

        // Server config
        DBServerConfig serverConfig;
        serverConfig.setPort(_port);
        serverConfig.setAddress(_address);

        spdlog::info("TuringDB started");
        spdlog::info("  - Home directory: {}", turingDir.get());

        std::unique_ptr<VisualizerProxy> proxy;
        if (_launchUI) {
            proxy = std::make_unique<VisualizerProxy>(_address, _uiPort, _port);
            proxy->start();
        }

        TuringServer server(serverConfig, turingDB);
        server.start();

        if (stopSource.stop_requested()) {
            throw TuringException("TuringDB was stopped before it was ready");
        }

        if (_demonize) {
            const std::stop_callback stopServer(stopSource.get_token(), [&server] { server.stop(); });
            readiness.notifyReady();
            server.wait();
        } else {
            LineNoiseHandle lineNoiseHandle;
            lineNoiseHandle.initProtectedLogger(consoleSink);
            TuringShell shell(turingDB, &mem, &lineNoiseHandle);

            if (!_graphsToLoad.empty()) {
                shell.setGraphName(_graphsToLoad.front());
            }

            const std::stop_callback stopShell(stopSource.get_token(), [&shell] { shell.stop(); });
            shell.startLoop();
        }

    } catch (TuringException& e) {
        spdlog::error("{}", e.what());
        readiness.notifyFailure(e.what());
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
    _argParser.add_argument("-ui")
        .help("Launch visualizer proxy")
        .store_into(_launchUI);
    _argParser.add_argument("-auth-on")
        .help("Enable authentication; the API key is read from the TURINGDB_AUTH_TOKEN "
              "environment variable and clients present it as a bearer token")
        .store_into(_authOn);
    _argParser.add_argument("-ui-port")
        .metavar("port")
        .help("Visualizer proxy port (default: 8080)")
        .store_into(_uiPort);
    _argParser.add_argument("-no-vector-mmap")
        .help("Read HNSW vector indexes into memory at startup instead of mapping the "
              "index file; a server that adds vectors reads them in anyway")
        .store_into(_noVectorMmap);
}
