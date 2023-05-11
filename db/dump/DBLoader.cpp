#include "DBLoader.h"
#include "BioLog.h"
#include "DB.h"
#include "FileUtils.h"
#include "MsgCommon.h"
#include "MsgDB.h"
#include "StringIndexLoader.h"

using namespace db;
using namespace Log;

DBLoader::DBLoader(DB** db, const Path& outDir)
    : _outDir(outDir),
      _db(db)
{
    setDBDirectoryName(getDefaultDBDirectoryName());
}

DBLoader::~DBLoader() = default;

std::string DBLoader::getDefaultDBDirectoryName() {
    return "turing.db";
}

void DBLoader::setDBDirectoryName(const std::string& dirName) {
    _dbDirName = dirName;
    _dbPath = FileUtils::abspath(_outDir / _dbDirName);
    _stringIndexPath = _dbPath / "smap";
}

bool DBLoader::load() {
    DB* db = DB::create();
    *_db = db;

    BioLog::log(msg::INFO_DB_LOADING_DATABASE() << _dbPath);

    StringIndexLoader strLoader{_stringIndexPath};
    if (!strLoader.load(db->_strIndex)) {
        BioLog::log(msg::ERROR_DB_LOADING_DATABASE() << _dbPath);
        return false;
    }

    return true;
}
