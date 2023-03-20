#ifndef _UI_SITE_ARCHIVE_
#define _UI_SITE_ARCHIVE_

#include <filesystem>

namespace ui {

class SiteArchive {
public:
    static bool decompress(const std::filesystem::path& outDir);

    static std::string getSiteArchiveName();
    static std::string getSiteDirectoryName();

private:
    SiteArchive();
    ~SiteArchive();
};

}

#endif
