#include "SiteArchive.h"

#include <string>
#include <fstream>

#include "SiteArchiveData.h"

#include "Command.h"
#include "FileUtils.h"
#include "BioLog.h"

#include "MsgCommon.h"
#include "MsgUIServer.h"

using namespace Log;

static constexpr const char* SiteArchiveName = "site.tar";
static constexpr const char* SiteName = "site";

using namespace ui;

SiteArchive::SiteArchive()
{
}

SiteArchive::~SiteArchive() {
}

std::string SiteArchive::getSiteArchiveName() {
    return SiteArchiveName;
}

std::string SiteArchive::getSiteDirectoryName() {
    return SiteName;
}

bool SiteArchive::decompress(const std::filesystem::path& outDir) {
    if (!FileUtils::exists(outDir) || !FileUtils::isDirectory(outDir)) {
        BioLog::log(msg::ERROR_NOT_A_DIRECTORY() << outDir.string());
        return false;
    }

    // Write archive file in output directory
    const auto archiveFileName = outDir/SiteArchiveName;
    if (!FileUtils::writeBinary(archiveFileName, (const char*)globfs_site_data, globfs_site_size)) {
        BioLog::log(msg::ERROR_FAILED_TO_WRITE_FILE() << archiveFileName.string());
        return false;
    }

    // Decompress archive file
    Command untarCmd("tar");
    untarCmd.setScriptPath(outDir/"decompress.sh");
    untarCmd.setLogFile(outDir/"reports"/"decompress.log");
    untarCmd.setWorkingDir(outDir);
    untarCmd.addArg("-xf");
    untarCmd.addArg(archiveFileName);

    BioLog::log(msg::INFO_DECOMPRESSING_SITE());
    if (!untarCmd.run() || untarCmd.getReturnCode() != 0) {
        BioLog::log(msg::ERROR_FAILED_TO_DECOMPRESS_SITE()
                    << outDir.string()); 
        return false;
    }

    return true;
}
