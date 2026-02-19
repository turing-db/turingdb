#pragma once

#include <vector>

#include "Column.h"
#include "ColumnVector.h"

namespace db {

class ColumnStringTable : public Column {
public:
    using StringColumn = ColumnVector<std::string>;

    ColumnStringTable();
    ~ColumnStringTable() override;

    size_t getFieldCount() const { return _columns.size(); }
    size_t getRowCount() const;
    size_t size() const override { return getRowCount(); }

    StringColumn* getFieldColumn(size_t index) const { return _columns[index]; }

    void addFieldColumn(StringColumn* col) { _columns.push_back(col); }

    void clear();
    void assign(const Column* other) override;
    void assignFromLine(const Column* other, size_t startLine, size_t rowCount) override;

    void dump(std::ostream& out) const override;

    ContainerKind::Code getContainerKind() const override {
        return ContainerKind::code<ColumnStringTable>();
    }

    InternalKind::Code getInternalKind() const override { return 0; }

    static consteval auto staticKind() { return _staticKind; }

    static constexpr auto _staticKind = ColumnKind::code<ColumnStringTable>();

private:
    std::vector<StringColumn*> _columns;
};

}
