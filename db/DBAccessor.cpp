#include "DBAccessor.h"

using namespace db;

DBAccessor::DBAccessor(DB* db)
    : _db(db)
{
}

DBAccessor::~DBAccessor() {
}
