#pragma once

#include "FileUtils.h"

namespace db {

class DB;

class TypeDumper {
public:
    TypeDumper(db::DB* db, const FileUtils::Path& indexPath);

    bool dump();

private:
    FileUtils::Path _indexPath;
    db::DB* _db {nullptr};
};

}
