#include "ServerThreadEngine.h"

#include "BioLog.h"
#include "BioserverThread.h"
#include "FlaskThread.h"
#include "MsgUIServer.h"
#include "ReactThread.h"
#include "TailwindThread.h"

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

    auto& tailwindTread = _threads[(uint8_t)ServerType::TAILWIND];
    tailwindTread = std::make_unique<TailwindThread>();
    tailwindTread->runDev();
#endif
}

void ServerThreadEngine::wait() {
    for (auto& thread : _threads) {
        if (thread) {
            thread->join();
        }
    }
}

int ServerThreadEngine::getReturnCode() const {
    return _returnCode;
}

}
