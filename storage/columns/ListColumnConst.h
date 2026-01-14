#pragma once

#include "ListColumn.h"
#include "ColumnKind.h"
#include "ValueVariant.h"

#include "DebugDump.h"
#include "BioAssert.h"

namespace db {

class ListColumnConst : public ListColumn {
public:
    static constexpr ColumnKind::ColumnKindCode BaseKind = ColumnKind::getContainerTypeKind<ListColumnConst>();

    using ValueType = Column*;

    ListColumnConst()
        : ListColumn(_staticKind) {
    }

    ~ListColumnConst() override = default;

    ListColumnConst(const ListColumnConst&) = default;
    ListColumnConst(ListColumnConst&&) noexcept = default;

    ListColumnConst& operator=(const ListColumnConst&) = default;
    ListColumnConst& operator=(ListColumnConst&&) noexcept = default;

    size_t size() const override { return 1; }

    void assign(const Column* other) override {
        const ListColumnConst* otherCol = dynamic_cast<const ListColumnConst*>(other);
        bioassert(otherCol, "ListColumnConst::assign: other is not a ListColumnConst of the same type");
        _values = otherCol->_values;
    }

    void assignFromLine(const Column* other, size_t startLine, size_t rowCount) override {
        const ListColumnConst* otherCol = dynamic_cast<const ListColumnConst*>(other);
        bioassert(otherCol, "ListColumnConst::assignFromLine: other is not a ListColumnConst of the same type");

        if (rowCount == 0) {
            return;
        }

        _values = otherCol->_values;
    }

    void dump(std::ostream& out) const override {
        DebugDump::dump(out, _values);
    }

    std::vector<ValueVariant>& getRaw() { return _values; }
    const std::vector<ValueVariant>& getRaw() const { return _values; }

    static consteval auto staticKind() { return _staticKind; }

private:
    std::vector<ValueVariant> _values;

    static constexpr auto _staticKind = ColumnKind::getColumnKind<ListColumnConst>();
};

}
