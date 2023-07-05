#pragma once

#include "FileUtils.h"

namespace db {

class DB;

class SchemaReport {
public:
    using Path = FileUtils::Path;

    SchemaReport(const Path& reportsDir, const DB* db);
    ~SchemaReport();

    void writeReport();

private:
    Path _reportsDir;
    const DB* _db {nullptr};
};

}
