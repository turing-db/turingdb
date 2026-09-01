#pragma once

#include <vector>
#include <ostream>

#include "ColumnTag.h"
#include "NamedColumn.h"

#include "DynamicLookupTable.h"

namespace db {

class Column;

// A basic Dataframe class with columns indexed by ColumnTag.
class Dataframe {
public:
    using NamedColumns = std::vector<NamedColumn*>;
    friend NamedColumn;

    Dataframe();

    Dataframe(const Dataframe&) = delete;
    Dataframe& operator=(const Dataframe&) = delete;

    ~Dataframe();

    size_t size() const { return _cols.size(); }
    void clear();

    /**
     * @brief Determines the logical length of a dataframe.
     * @detail Uses the first non-ColumnConst column in @ref this to determine
     * the logical row count of this dataframe.
     * @warn Does not check for validity of pointers to columns.
     */
    size_t getLogicalRowCount() const;

    /**
     * @brief Determines whether all columns in a dataframe of equal logical
     * length (row count).
     * @detail All @ref ColumnVector<T> must be of same size = SIZE. Any @ref
     * ColumnConst<T> are treated as having a logical size of SIZE. Checks
     * validity of all column pointers.
     * @throws If any of @ref _cols or @ref _cols[i]::_column is null.
     */
    bool isRectangular() const;

    const NamedColumns& cols() const { return _cols; }

    void addColumn(NamedColumn* column);

    NamedColumn* getColumn(ColumnTag tag) const {
        return _tagToColumnMap.lookup(tag.getValue());
    }

    Column* getColumn(size_t idx) const {
        return _cols.at(idx)->getColumn();
    }

    bool hasColumn(ColumnTag tag) const {
        return getColumn(tag) != nullptr;
    }

    template <typename T>
    requires std::is_base_of_v<Column, T>
    T* getColumn(ColumnTag tag) const {
        auto* col = getColumn(tag);
        if (!col) {
            return nullptr;
        }

        return dynamic_cast<T*>(col->getColumn());
    }

    void copyFromLine(const Dataframe* other, size_t startRow, size_t rowCount);
    void copyFrom(const Dataframe* other);

    void append(const Dataframe* other);

    // Returns true if the dataframes have the same number of columns
    // and each pair of columns has the same kind
    bool hasSameShape(const Dataframe* other) const;

    void dump(std::ostream& out) const;

private:
    NamedColumns _cols;
    DynamicLookupTable<NamedColumn*> _tagToColumnMap;
};

}
