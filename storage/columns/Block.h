#pragma once

#include <vector>

#include "columns/Column.h"

namespace db {

class Column;

class Block {
public:
    using Columns = std::vector<Column*>;

    Block();
    
    Block(Block&&) noexcept;
    Block& operator=(Block&&) noexcept;

    ~Block();

    Block(const Block&) = delete;
    Block& operator=(const Block&) = delete;

    const Columns& columns() const { return _columns; }
    bool empty() const { return _columns.empty(); }
    size_t size() const { return _columns.size(); }
    const Column* operator[](size_t i) const noexcept { return _columns[i]; }
    Column* operator[](size_t i) noexcept { return _columns[i]; }

    void clear();

    void addColumn(Column *col);
    void append(const Block &other);

    void assignFrom(const Block &other);
    void assignFromLine(const Block &other, size_t startLine, size_t rowCount);

    size_t getBlockRowCount() const;

  private:
    Columns _columns;
};

}
