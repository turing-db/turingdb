#include "DBLoader.h"
#include "BioLog.h"
#include "DB.h"
#include "EntityLoader.h"
#include "TypeLoader.h"
#include "FileUtils.h"
#include "MsgCommon.h"
#include "MsgDB.h"
#include "StringIndexLoader.h"

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
    Path stringIndexPath = dbPath / "smap";
    Path typeIndexPath = dbPath / "types";
    Path entityIndexPath = dbPath / "entities";
    std::vector<StringRef> stringRefs;

    BioLog::log(msg::INFO_DB_LOADING_DATABASE() << dbPath);

    StringIndexLoader strLoader {stringIndexPath};
    if (!strLoader.load(_db->_strIndex, stringRefs)) {
        BioLog::log(msg::ERROR_DB_LOADING_DATABASE() << dbPath);
        return false;
    }

    TypeLoader typeLoader {_db, typeIndexPath};
    if (!typeLoader.load(stringRefs)) {
        BioLog::log(msg::ERROR_DB_LOADING_DATABASE() << dbPath);
        return false;
    }

    EntityLoader entityLoader {_db, entityIndexPath};
    if (!entityLoader.load(stringRefs)) {
        BioLog::log(msg::ERROR_DB_LOADING_DATABASE() << dbPath);
        return false;
    }

    return true;
}
