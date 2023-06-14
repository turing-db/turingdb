#pragma once

#include "FileUtils.h"

namespace db {

class DB;

class EntityDumper {
public:
    EntityDumper(db::DB* db, const FileUtils::Path& indexPath);

    bool dump();

private:
    FileUtils::Path _indexPath;
    db::DB* _db {nullptr};
};

}
