#include "TuringUIServer.h"

#include <filesystem>
#include <stdlib.h>

#include "SiteArchive.h"
#include "Command.h"
#include "FileUtils.h"

#include "BioLog.h"
#include "MsgCommon.h"
#include "MsgUIServer.h"

using namespace ui;
using namespace Log;

TuringUIServer::TuringUIServer(const Path& outDir)
    : _outDir(outDir)
    , _cmd("python3")
{
    const auto sitePath = _outDir/SiteArchive::getSiteDirectoryName();
    _cmd.addArg("-m");
    _cmd.addArg("turingui");
    _cmd.addArg(sitePath.string());
    _cmd.setWorkingDir(sitePath);
    _cmd.setScriptPath(sitePath/"serve.sh");
    _cmd.setLogFile(sitePath/"serve.log");
}

TuringUIServer::~TuringUIServer() {
}

int TuringUIServer::start() {
    // Decompress site archive
    if (!SiteArchive::decompress(_outDir)) {
        return -1;
    }

    BioLog::log(msg::INFO_RUN_FLASK_SERVER() << 5000);
    bool success = _cmd.run();
    int code = _cmd.getReturnCode();

    if (!success) {
        BioLog::log(msg::ERROR_UI_SERVER_ERROR_OCCURED() << code);
        return code;
    }

    BioLog::log(msg::INFO_STOPPING_FLASK_SERVER());

    if (_cleanEnabled) {
        cleanSite();
    }

    return code;
}

int TuringUIServer::getReturnCode() const {
    return _cmd.getReturnCode();
}

void TuringUIServer::getLogs(std::string& data) const {
    _cmd.getLogs(data);
}

void TuringUIServer::cleanSite() {
    BioLog::log(msg::INFO_CLEANING_SITE());

    const auto sitePath = _outDir/SiteArchive::getSiteDirectoryName();
    if (!FileUtils::removeDirectory(sitePath)) {
        BioLog::log(msg::ERROR_FAILED_TO_REMOVE_DIRECTORY()
                    << sitePath.string());
        return;
    }

    const auto archivePath = _outDir/SiteArchive::getSiteArchiveName();
    if (!FileUtils::removeFile(archivePath)) {
        BioLog::log(msg::ERROR_FAILED_TO_REMOVE_FILE()
                    << archivePath.string());
    }
}
