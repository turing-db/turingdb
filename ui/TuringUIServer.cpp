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
{
}

TuringUIServer::~TuringUIServer() {
}

void TuringUIServer::start() {
    // Decompress site archive
    if (!SiteArchive::decompress(_outDir)) {
        return;
    }

    // Run node server
    const auto sitePath = _outDir/SiteArchive::getSiteDirectoryName();
    Command serverCmd("npx");
    serverCmd.addArg("serve");
    serverCmd.addArg("-s");
    serverCmd.addArg("build");
    serverCmd.setWorkingDir(sitePath);
    serverCmd.setEnvVar("WDS_SOCKET_PORT", "443");
    serverCmd.setScriptPath(sitePath/"serve.sh");
    serverCmd.setLogFile(sitePath/"serve.log");

    BioLog::log(msg::INFO_RUN_NODE_SERVER() << 3000);
    serverCmd.run();
    BioLog::log(msg::INFO_STOPPING_NODE_SERVER());

    if (_cleanEnabled) {
        cleanSite();
    }
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
