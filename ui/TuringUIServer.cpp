#include "TuringUIServer.h"

#include <filesystem>

#include "SiteArchive.h"
#include "System.h"
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
    std::string nodeStartCmd = "cd ";
    nodeStartCmd += sitePath.string();
    nodeStartCmd += " && npx serve -s build";

    BioLog::log(msg::INFO_RUN_NODE_SERVER() << 3000);
    System::runCommand(nodeStartCmd);
    BioLog::log(msg::INFO_STOPPING_NODE_SERVER());

    cleanSite();
}

void TuringUIServer::cleanSite() {
    BioLog::log(msg::INFO_CLEANING_SITE());

    const auto sitePath = _outDir/SiteArchive::getSiteDirectoryName();
    if (!files::removeDirectory(sitePath)) {
        BioLog::log(msg::ERROR_FAILED_TO_REMOVE_DIRECTORY()
                    << sitePath.string());
        return;
    }

    const auto archivePath = _outDir/SiteArchive::getSiteArchiveName();
    if (!files::removeFile(archivePath)) {
        BioLog::log(msg::ERROR_FAILED_TO_REMOVE_FILE()
                    << archivePath.string());
    }
}
