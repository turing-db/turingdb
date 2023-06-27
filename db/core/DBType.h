#pragma once

#include "DBObject.h"

#include "Comparator.h"
#include "StringRef.h"

namespace db {

class DBType : public DBObject {
public:
    friend DBComparator;

    StringRef getName() const { return _name; }

protected:
    DBType(DBIndex index, StringRef name);
    ~DBType();

private:
    StringRef _name;
};

}
