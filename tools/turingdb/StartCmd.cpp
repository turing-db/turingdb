#include "StartCmd.h"

#include <spdlog/spdlog.h>
#include <argparse.hpp>

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

int StartCmd::execute() {
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
        Demonology::demonize();
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
        spdlog::info("TuringDB path: {}", turingDir.get());

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

        server = std::make_unique<TuringServer>(serverConfig, turingDB);
        server->start();

        if (_demonize) {
            server->wait();
            server.reset();

            turingDB.stop();
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

            turingDB.stop();
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
}
