#include "ServerThreadEngine.h"

#include "BioLog.h"
#include "ServerThread.h"
#include "BioserverThread.h"
#include "FlaskThread.h"
#include "MsgUIServer.h"
#include "ReactThread.h"

namespace ui {

ServerThreadEngine::ServerThreadEngine(const FileUtils::Path& rootDir)
    : _rootDir(rootDir)
{
}

void ServerThreadEngine::run() {
    std::filesystem::current_path(_rootDir);
    auto& flaskThread = _threads[(uint8_t)ServerType::FLASK];
    flaskThread = std::make_unique<FlaskThread>();
    flaskThread->run();

    auto& bioserverThread = _threads[(uint8_t)ServerType::BIOSERVER];
    bioserverThread = std::make_unique<BioserverThread>();
    bioserverThread->run();
}

void ServerThreadEngine::runDev() {
#ifdef TURING_DEV
    Log::BioLog::log(msg::INFO_STARTING_DEV_UI_SERVER());

    std::filesystem::current_path(_rootDir);
    auto& flaskThread = _threads[(uint8_t)ServerType::FLASK];
    flaskThread = std::make_unique<FlaskThread>();
    flaskThread->runDev();

    auto& reactThread = _threads[(uint8_t)ServerType::REACT];
    reactThread = std::make_unique<ReactThread>();
    reactThread->runDev();
#endif
}

void ServerThreadEngine::wait() {
    for (auto& thread : _threads) {
        if (thread) {
            thread->join();
        }
    }
}

int ServerThreadEngine::getReturnCode(ServerType serverType) const {
    if (!_threads[(uint8_t)serverType]) {
        return 0;
    }

    return _threads[(uint8_t)serverType]->getReturnCode();
}

void ServerThreadEngine::getOutput(ServerType serverType, std::string& output) const {
    FileUtils::readContent(_threads[(uint8_t)serverType]->getLogFilePath(), output);
}

}
