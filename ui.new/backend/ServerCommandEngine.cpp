#include "ServerCommandEngine.h"

#include "BioserverCommand.h"
#include "FlaskCommand.h"
#include "ReactCommand.h"
#include "ServerCommand.h"

#include "BioLog.h"
#include "MsgUIServer.h"
#include "MsgCommon.h"

using namespace Log;

namespace ui {

using namespace std::chrono_literals;

ServerCommandEngine::ServerCommandEngine(const FileUtils::Path& rootDir)
    : _rootDir(rootDir)
{
}

ServerCommandEngine::~ServerCommandEngine() {
    for (auto& server : _servers) {
        if (server) {
            server.reset();
        }
    }
}

void ServerCommandEngine::run() {
    std::filesystem::current_path(_rootDir);

    _group = std::make_unique<boost::process::group>();

    try {
        auto& flaskCommand = _servers[(uint8_t)ServerType::FLASK];
        flaskCommand = std::make_unique<FlaskCommand>();
        flaskCommand->run(_group);
    } catch (const boost::process::process_error& e) {
        BioLog::log(msg::ERROR_IMPOSSIBLE_TO_RUN_COMMAND() << "flask command");
        return;
    }

    try {
        auto& bioserverCommand = _servers[(uint8_t)ServerType::BIOSERVER];
        bioserverCommand = std::make_unique<BioserverCommand>();
        bioserverCommand->run(_group);
    } catch (const boost::process::process_error& e) {
        BioLog::log(msg::ERROR_IMPOSSIBLE_TO_RUN_COMMAND() << "bioserver command");
        return;
    }
}

void ServerCommandEngine::runDev() {
#ifdef TURING_DEV
    BioLog::log(msg::INFO_STARTING_DEV_UI_SERVER());

    _group = std::make_unique<boost::process::group>();

    std::filesystem::current_path(_rootDir);
    auto& flaskCommand = _servers[(uint8_t)ServerType::FLASK];
    flaskCommand = std::make_unique<FlaskCommand>();
    flaskCommand->runDev(_group);

    auto& reactCommand = _servers[(uint8_t)ServerType::REACT];
    reactCommand = std::make_unique<ReactCommand>();
    reactCommand->runDev(_group);
#endif
}

void ServerCommandEngine::terminate() {
    BioLog::log(msg::INFO_TERMINATING_UI_SERVER());
    for (auto& server : _servers) {
        if (server && !server->isDone()) {
            server->terminate();
        }
    }
}

ServerType ServerCommandEngine::waitServerDone() {
    while (true) {
        for (uint8_t i = 0; i < (uint8_t)ServerType::_SIZE; i++) {
            const ServerType type = static_cast<ServerType>(i);
            const auto& server = _servers[i];

            if (server && server->isDone()) {
                return type;
            }
        }

        std::this_thread::sleep_for(100ms);
    }

    return ServerType::_SIZE; // Error
}

int ServerCommandEngine::getReturnCode(ServerType serverType) const {
    if (!_servers[(uint8_t)serverType]) {
        return 0;
    }

    return _servers[(uint8_t)serverType]->getReturnCode();
}

void ServerCommandEngine::getOutput(ServerType serverType, std::string& output) const {
    const auto& server = _servers[(uint8_t)serverType];
    if (server) {
        FileUtils::readContent(server->getLogFilePath(), output);
    }
}

}
