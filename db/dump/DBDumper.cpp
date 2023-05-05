#include "DBDumper.h"

#include "FileUtils.h"

#include "BioLog.h"
#include "MsgCommon.h"
#include "MsgDB.h"

using namespace db;
using namespace Log;

DBDumper::DBDumper(const Path& outDir)
    : _outDir(outDir),
    _dbDirName(getDefaultDBDirectoryName())
{
}

DBDumper::~DBDumper() {
}

std::string DBDumper::getDefaultDBDirectoryName() {
    return "turing.out";
}

void DBDumper::setDBDirectoryName(const std::string& dirName) {
    _dbDirName = dirName;
}

bool DBDumper::dump() {
    const auto dbDumpPath = FileUtils::abspath(_outDir/_dbDirName);

    BioLog::log(msg::INFO_DB_DUMPING_DATABASE() << dbDumpPath);

    // Remove DB dump directory if it already exists
    if (FileUtils::exists(dbDumpPath)) {
        if (!FileUtils::removeDirectory(dbDumpPath)) {
            BioLog::log(msg::ERROR_FAILED_TO_REMOVE_DIRECTORY() << dbDumpPath);
            return false;
        }
    }

    if (!FileUtils::createDirectory(dbDumpPath)) {
        BioLog::log(msg::ERROR_FAILED_TO_CREATE_DIRECTORY() << dbDumpPath);
        return false;
    }

    return true;
}
