#pragma once

#include "DBIndex.h"
#include "Comparator.h"

namespace db {

class DBObject {
public:
    friend DBComparator;

    struct Sorter{
        bool operator()(const DBObject* obj1, const DBObject* obj2) const {
            return obj1->getIndex() < obj2->getIndex();
        }
    };

    DBIndex getIndex() const { return _index; }

protected:
    DBObject(DBIndex index);
    ~DBObject();

private:
    DBIndex _index;
};

}
