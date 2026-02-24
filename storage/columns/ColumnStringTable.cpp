#include "ColumnStringTable.h"

#include <ostream>

#include "BioAssert.h"

using namespace db;

ColumnStringTable::ColumnStringTable()
    : Column(_staticKind)
{
}

ColumnStringTable::~ColumnStringTable() = default;

size_t ColumnStringTable::getRowCount() const {
    if (_columns.empty()) return 0;
    return _columns[0]->size();
}

void ColumnStringTable::clear() {
    for (StringColumn* col : _columns) {
        col->clear();
    }
}

void ColumnStringTable::assign(const Column* other) {
    const auto* src = static_cast<const ColumnStringTable*>(other);
    bioassert(_columns.size() == src->_columns.size(),
              "ColumnStringTable::assign: field count mismatch");

    for (size_t i = 0; i < _columns.size(); i++) {
        _columns[i]->assign(src->_columns[i]);
    }
}

void ColumnStringTable::assignFromLine(const Column* other,
                                       size_t startLine,
                                       size_t rowCount) {
    const auto* src = static_cast<const ColumnStringTable*>(other);
    bioassert(_columns.size() == src->_columns.size(),
              "ColumnStringTable::assignFromLine: field count mismatch");

    for (size_t i = 0; i < _columns.size(); i++) {
        _columns[i]->assignFromLine(src->_columns[i], startLine, rowCount);
    }
}

void ColumnStringTable::dump(std::ostream& out) const {
    out << "ColumnStringTable(fields=" << _columns.size()
        << ", rows=" << getRowCount() << ")";
}
