#include "Block.h"

using namespace db;

Block::Block()
{
}

Block::Block(Block&& other) noexcept
    : _columns(std::move(other._columns))
{
}

Block& Block::operator=(Block&& other) noexcept {
    _columns = std::move(other._columns);
    return *this;
}

Block::~Block() {
}

void Block::clear() {
    _columns.clear();
}

void Block::addColumn(Column* col) {
    _columns.push_back(col);
}

void Block::append(const Block &other) {
    std::copy(other._columns.begin(), other._columns.end(),
              std::back_inserter(_columns));
}

size_t Block::getRowCount() const {
    if (_columns.empty()) {
        return 0;
    }

    return _columns.front()->size();
}

void Block::assignFrom(const Block &other) {
    const Columns& otherColumns = other.columns();
    const size_t assignSize = std::min(_columns.size(), otherColumns.size());
    for (size_t i = 0; i < assignSize; i++) {
        _columns[i]->assign(otherColumns[i]);
    }
}

void Block::assignFromLine(const Block &other, size_t startLine, size_t rowCount) {
    const Columns& otherColumns = other.columns();
    const size_t assignSize = std::min(_columns.size(), otherColumns.size());
    for (size_t i = 0; i < assignSize; i++) {
        _columns[i]->assignFromLine(otherColumns[i], startLine, rowCount);
    }
}
