#include "DBServerConfig.h"

#include <stdlib.h>

#include "FileUtils.h"
#include "BioAssert.h"

DBServerConfig::DBServerConfig()
{
    const char* home = std::getenv("HOME");
    bioassert(home);
    _databasesPath = (FileUtils::Path {home} / "databases").string();
}

DBServerConfig::~DBServerConfig() {
}
