#include "DBDumper.h"
#include "BioLog.h"
#include "DB.h"
#include "FileUtils.h"
#include "MsgCommon.h"
#include "MsgDB.h"
#include "StringIndexDumper.h"

using namespace db;
using namespace Log;

DBDumper::DBDumper(DB** db, const Path& outDir)
    : _outDir(outDir),
      _db(db)
{
    setDBDirectoryName(getDefaultDBDirectoryName());
}

DBDumper::~DBDumper() = default;

std::string DBDumper::getDefaultDBDirectoryName() {
    return "turing.db";
}

void DBDumper::setDBDirectoryName(const std::string& dirName) {
    _dbDirName = dirName;
    _dbPath = FileUtils::abspath(_outDir / _dbDirName);
    _stringIndexPath = _dbPath / "smap";
}

bool DBDumper::dump() {
    DB* db = *_db;

    BioLog::log(msg::INFO_DB_DUMPING_DATABASE() << _dbPath);

    // Remove DB dump directory if it already exists
    if (FileUtils::exists(_dbPath)) {
        if (!FileUtils::removeDirectory(_dbPath)) {
            BioLog::log(msg::ERROR_FAILED_TO_REMOVE_DIRECTORY() << _dbPath);
            return false;
        }
    }

    if (!FileUtils::createDirectory(_dbPath)) {
        BioLog::log(msg::ERROR_FAILED_TO_CREATE_DIRECTORY() << _dbPath);
        return false;
    }

    StringIndexDumper strDumper{_stringIndexPath};
    if (!strDumper.dump(db->_strIndex)) {
        BioLog::log(msg::ERROR_DB_DUMPING_DATABASE() << _dbPath);
        return false;
    }

    return true;
}
