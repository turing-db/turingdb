#include "SiteArchive.h"

#include <string>
#include <fstream>

#include "SiteArchiveData.h"

#include "System.h"
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
    std::string untarCmd = "cd ";
    untarCmd += outDir.string(); 
    untarCmd += " && tar -xf ";
    untarCmd += SiteArchiveName;

    BioLog::log(msg::INFO_DECOMPRESSING_SITE());
    const int decompressResult = System::runCommand(untarCmd);
    if (decompressResult != 0) {
        BioLog::log(msg::ERROR_FAILED_TO_DECOMPRESS_SITE() << outDir.string()); 
        return false;
    }

    return true;
}
