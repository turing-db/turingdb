#pragma once

#include "StringRef.h"
#include "FileUtils.h"

#include <vector>

namespace db {

class DB;
class StringIndex;

class TypeLoader{
public:
    TypeLoader(db::DB* db, const FileUtils::Path& dbPath);

    bool load(const std::vector<StringRef>& strIndex);

private:
    FileUtils::Path _indexPath;
    db::DB* _db{nullptr};
};

}
