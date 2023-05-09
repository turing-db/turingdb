#include "DBSerializer.h"
#include "BioLog.h"
#include "DB.h"
#include "FileUtils.h"
#include "MsgCommon.h"
#include "MsgDB.h"
#include "StringIndexSerializer.h"
#include "capnp_src/StringIndex.capnp.h"

#include <capnp/message.h>
#include <capnp/serialize-packed.h>
#include <cassert>
#include <iostream>
#include <unistd.h>

using namespace db;
using namespace Log;

DBSerializer::DBSerializer(DB** db, const Path& outDir)
    : _outDir(outDir),
      _dbDirName(getDefaultDBDirectoryName()),
      _db(db) //
{
}

DBSerializer::~DBSerializer() = default;

std::string DBSerializer::getDefaultDBDirectoryName() {
    return "turing.out";
}

void DBSerializer::setDBDirectoryName(const std::string& dirName) {
    _dbDirName = dirName;
}

Serializer::Result DBSerializer::load() {
    DB* db = DB::create();
    *_db = db;

    const auto dbPath = FileUtils::abspath(_outDir / _dbDirName);
    const auto strIndexPath = dbPath / "strings.biodb";

    BioLog::log(msg::INFO_DB_LOADING_DATABASE() << dbPath);

    StringIndexSerializer strSerializer{strIndexPath};
    if (auto res = strSerializer.load(db->_strIndex);
        res != Serializer::Result::SUCCESS)
        return res;

    return Serializer::Result::SUCCESS;
}

Serializer::Result DBSerializer::dump() {
    DB* db = *_db;

    const auto dbPath = FileUtils::abspath(_outDir / _dbDirName);
    const auto strIndexPath = dbPath / "strings.biodb";

    BioLog::log(msg::INFO_DB_DUMPING_DATABASE() << dbPath);

    // Remove DB dump directory if it already exists
    if (FileUtils::exists(dbPath)) {
        if (!FileUtils::removeDirectory(dbPath)) {
            BioLog::log(msg::ERROR_FAILED_TO_REMOVE_DIRECTORY() << dbPath);
            return Serializer::Result::CANT_ACCESS_DUMP_DIR;
        }
    }

    if (!FileUtils::createDirectory(dbPath)) {
        BioLog::log(msg::ERROR_FAILED_TO_CREATE_DIRECTORY() << dbPath);
        return Serializer::Result::CANT_ACCESS_DUMP_DIR;
    }

    StringIndexSerializer strSerializer{strIndexPath};
    if (auto res = strSerializer.dump(db->_strIndex);
        res != Serializer::Result::SUCCESS)
        return res;

    return Serializer::Result::SUCCESS;
}
