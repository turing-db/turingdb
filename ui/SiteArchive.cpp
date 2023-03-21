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
    if (!files::exists(outDir) || !files::isDirectory(outDir)) {
        BioLog::log(msg::ERROR_NOT_A_DIRECTORY() << outDir.string());
        return false;
    }

    // Write archive file in output directory
    const auto archiveFileName = outDir/SiteArchiveName;
    std::ofstream archiveFile(archiveFileName, std::ios_base::binary);
    archiveFile.write((const char*)globfs_site_data, globfs_site_size);

    // Decompress archive file
    Command untarCmd("tar");
    untarCmd.setWorkingDir(outDir);
    untarCmd.addArg("-xf");
    untarCmd.addArg(SiteArchiveName);

    BioLog::log(msg::INFO_DECOMPRESSING_SITE());
    if (!untarCmd.run()) {
        BioLog::log(msg::ERROR_FAILED_TO_DECOMPRESS_SITE()
                    << outDir.string()); 
        return false;
    }

    return true;
}
