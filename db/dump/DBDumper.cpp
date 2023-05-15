#include "DBDumper.h"
#include "BioLog.h"
#include "DB.h"
#include "FileUtils.h"
#include "MsgCommon.h"
#include "MsgDB.h"
#include "StringIndexDumper.h"

using namespace db;
using namespace Log;

DBDumper::DBDumper(DB* db, const Path& outDir)
    : _outDir(outDir),
      _dbDirName(getDefaultDBDirectoryName()),
      _db(db)
{
}

DBDumper::~DBDumper() = default;

std::string DBDumper::getDefaultDBDirectoryName() {
    return "turing.db";
}

void DBDumper::setDBDirectoryName(const std::string& dirName) {
    _dbDirName = dirName;
}

bool DBDumper::dump() {
    Path dbPath = FileUtils::abspath(_outDir / _dbDirName);
    Path stringIndexPath = dbPath / "smap";

    BioLog::log(msg::INFO_DB_DUMPING_DATABASE() << dbPath);

    // Remove DB dump directory if it already exists
    if (FileUtils::exists(dbPath)) {
        if (!FileUtils::removeDirectory(dbPath)) {
            BioLog::log(msg::ERROR_FAILED_TO_REMOVE_DIRECTORY() << dbPath);
            return false;
        }
    }

    if (!FileUtils::createDirectory(dbPath)) {
        BioLog::log(msg::ERROR_FAILED_TO_CREATE_DIRECTORY() << dbPath);
        return false;
    }

    StringIndexDumper strDumper{stringIndexPath};
    if (!strDumper.dump(_db->_strIndex)) {
        BioLog::log(msg::ERROR_DB_DUMPING_DATABASE() << dbPath);
        return false;
    }

    return true;
}
