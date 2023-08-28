#include "QueryRecord.h"

#include "DBService.pb.h"

using namespace turing::db::client;

size_t QueryRecord::size() const { return _record.cells().size(); }

QueryValue QueryRecord::operator[](size_t i) const {
    return QueryValue(_record.cells()[i]);
}
