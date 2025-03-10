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

size_t Block::getBlockRowCount() const {
    size_t rowCount = 0;
    for (const Column *column : _columns) {
        rowCount = std::max(rowCount, column->size());
    }
    return rowCount;
}
