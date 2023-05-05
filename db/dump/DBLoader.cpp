#include "DBLoader.h"

#include "DBDumper.h"

#include "FileUtils.h"

#include "BioLog.h"
#include "MsgCommon.h"

using namespace db;
using namespace Log;

DBLoader::DBLoader(const Path& outDir)
    : _outDir(outDir),
    _dbDirName(DBDumper::getDefaultDBDirectoryName())
{
}

DBLoader::~DBLoader() {
}

bool DBLoader::load() {
    const auto dbPath = FileUtils::abspath(_outDir/_dbDirName);
    if (!FileUtils::isDirectory(dbPath)) {
        BioLog::log(msg::ERROR_DIRECTORY_NOT_EXISTS() << dbPath);
        return false;
    }

    return true;
}
