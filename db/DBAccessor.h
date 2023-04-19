#pragma once

#include "DBNetworkRange.h"

namespace db {

class DB;

class DBAccessor {
public:
    DBAccessor(DB* db);
    ~DBAccessor();

    DBNetworkRange networks() const { return DBNetworkRange(_db); }

private:
    DB* _db {nullptr};
};

}
