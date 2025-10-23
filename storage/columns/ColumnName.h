#pragma once

namespace db {

class ColumnNameSharedString;

class ColumnName {
public:
private:
    size_t _id {0};
    ColumnNameSharedString* _str {nullptr};
};

}
