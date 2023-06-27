#pragma once

#include "FileUtils.h"

namespace db {

class DB;

class TypeDumper {
public:
    TypeDumper(const db::DB* db, const FileUtils::Path& indexPath);

    bool dump();

private:
    FileUtils::Path _indexPath;
    const db::DB* _db {nullptr};
};

}
