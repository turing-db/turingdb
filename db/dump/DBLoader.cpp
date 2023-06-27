#include "DBLoader.h"
#include "BioLog.h"
#include "DB.h"
#include "EntityLoader.h"
#include "FileUtils.h"
#include "MsgCommon.h"
#include "MsgDB.h"
#include "StringIndexLoader.h"
#include "TypeLoader.h"
#include "TimerStat.h"

using namespace db;
using namespace Log;

DBLoader::DBLoader(DB* db, const Path& outDir)
    : _outDir(outDir),
      _dbDirName(getDefaultDBDirectoryName()),
      _db(db)
{
}

DBLoader::~DBLoader() = default;

std::string DBLoader::getDefaultDBDirectoryName() {
    return "turing.db";
}

void DBLoader::setDBDirectoryName(const std::string& dirName) {
    _dbDirName = dirName;
}

bool DBLoader::load() {
    Path dbPath = FileUtils::abspath(_outDir / _dbDirName);
    TimerStat timer {"Loading Turing db: " + dbPath.string() };

    Path stringIndexPath = dbPath / "smap";
    Path typeIndexPath = dbPath / "types";
    Path entityIndexPath = dbPath / "data";

    BioLog::log(msg::INFO_DB_LOADING_DATABASE() << dbPath);

    BioLog::log(msg::INFO_DB_LOADING_STRING_INDEX());
    StringIndexLoader strLoader {stringIndexPath};
    if (!strLoader.load(_db->_strIndex)) {
        BioLog::log(msg::ERROR_DB_LOADING_DATABASE() << dbPath);
        return false;
    }

    BioLog::log(msg::INFO_DB_LOADING_TYPE_INDEX());
    TypeLoader typeLoader {_db, typeIndexPath};
    if (!typeLoader.load(strLoader)) {
        BioLog::log(msg::ERROR_DB_LOADING_DATABASE() << dbPath);
        return false;
    }

    BioLog::log(msg::INFO_DB_LOADING_ENTITY_INDEX());
    EntityLoader entityLoader {_db, entityIndexPath};
    if (!entityLoader.load(strLoader)) {
        BioLog::log(msg::ERROR_DB_LOADING_DATABASE() << dbPath);
        return false;
    }

    return true;
}
