#include "DBLoader.h"
#include "BioLog.h"
#include "DB.h"
#include "EntityLoader.h"
#include "FileUtils.h"
#include "MsgDB.h"
#include "StringIndexLoader.h"
#include "TypeLoader.h"
#include "TimerStat.h"

using namespace db;
using namespace Log;

DBLoader::DBLoader(DB* db, const Path& dbDir)
    : _dbDir(dbDir),
      _db(db)
{
}

DBLoader::~DBLoader() = default;

bool DBLoader::load() {
    if (!_db) {
        return false;
    }

    TimerStat timer {"Loading Turing db: " + _dbDir.string() };

    Path stringIndexPath = _dbDir / "smap";
    Path typeIndexPath = _dbDir / "types";
    Path entityIndexPath = _dbDir / "data";

    BioLog::log(msg::INFO_DB_LOADING_DATABASE() << _dbDir);

    BioLog::log(msg::INFO_DB_LOADING_STRING_INDEX());
    StringIndexLoader strLoader {stringIndexPath};
    if (!strLoader.load(_db->_strIndex)) {
        BioLog::log(msg::ERROR_DB_LOADING_DATABASE() << _dbDir);
        return false;
    }

    BioLog::log(msg::INFO_DB_LOADING_TYPE_INDEX());
    TypeLoader typeLoader {_db, typeIndexPath};
    if (!typeLoader.load(strLoader)) {
        BioLog::log(msg::ERROR_DB_LOADING_DATABASE() << _dbDir);
        return false;
    }

    BioLog::log(msg::INFO_DB_LOADING_ENTITY_INDEX());
    EntityLoader entityLoader {_db, entityIndexPath};
    if (!entityLoader.load(strLoader)) {
        BioLog::log(msg::ERROR_DB_LOADING_DATABASE() << _dbDir);
        return false;
    }

    BioLog::log(msg::INFO_DB_DONE_LOADING_DATABASE() << _dbDir);

    return true;
}

