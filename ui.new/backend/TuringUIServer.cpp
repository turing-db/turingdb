#include "TuringUIServer.h"

#include "ServerThreadEngine.h"
#include "ServerThread.h"
#include "SiteArchive.h"

#include "BioLog.h"
#include "MsgCommon.h"
#include "MsgUIServer.h"

using namespace Log;

namespace ui {


TuringUIServer::TuringUIServer(const FileUtils::Path& outDir)
    : _outDir(outDir),
      _engine(new ServerThreadEngine(outDir))
{
    const auto sitePath = _outDir / SiteArchive::getSiteDirectoryName();
}

TuringUIServer::~TuringUIServer() {
}

bool TuringUIServer::start() {
    // Decompress site archive
    if (!SiteArchive::decompress(_outDir)) {
        return false;
    }

    BioLog::log(msg::INFO_RUNNING_UI_SERVER() << 5000);
    _engine->run();
    return true;
}

bool TuringUIServer::startDev() {
#ifdef TURING_DEV
    BioLog::log(msg::INFO_RUNNING_UI_SERVER() << 3000);
    _engine->runDev();
    return true;
#else
    BioLog::log(msg::ERROR_CANNOT_START_DEV_UI_SERVER());
    return false;
#endif
}

void TuringUIServer::wait() {
    _engine->wait();
}

int TuringUIServer::getReturnCode(ServerType serverType) const {
    return _engine->getReturnCode(serverType);
}

void TuringUIServer::getOutput(ServerType serverType, std::string& output) const {
    _engine->getOutput(serverType, output);
}

}
