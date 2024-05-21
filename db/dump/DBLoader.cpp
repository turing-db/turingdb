#include "DBLoader.h"

#include <spdlog/spdlog.h>

#include "DB.h"
#include "EntityLoader.h"
#include "FileUtils.h"
#include "StringIndexLoader.h"
#include "TypeLoader.h"
#include "TimerStat.h"

using namespace db;

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

    spdlog::info("Loading database {}", _dbDir.string());

    spdlog::info("Loading database string index");
    StringIndexLoader strLoader {stringIndexPath};
    if (!strLoader.load(_db->_strIndex)) {
        spdlog::error("Error loading database string file {}", _dbDir.string());
        return false;
    }

    spdlog::info("Loading type index");
    TypeLoader typeLoader {_db, typeIndexPath};
    if (!typeLoader.load(strLoader)) {
        spdlog::error("Error loading database type file {}", _dbDir.string());
        return false;
    }

    spdlog::info("Loading entity index");
    EntityLoader entityLoader {_db, entityIndexPath};
    if (!entityLoader.load(strLoader)) {
        spdlog::error("Error loading database type file {}", _dbDir.string());
        return false;
    }

    spdlog::info("Done loading database {}", _dbDir.string());

    return true;
}

