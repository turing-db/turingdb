#pragma once

#include <span>
#include <vector>

#include "Column.h"

#include "BioAssert.h"
#include "NameOf.h"

namespace db {

class ColumnEmbeddingConst : public Column {
public:
    ColumnEmbeddingConst()
        : Column(_staticKind)
    {
    }

    ~ColumnEmbeddingConst() override = default;

    ColumnEmbeddingConst(const ColumnEmbeddingConst&) = default;
    ColumnEmbeddingConst(ColumnEmbeddingConst&&) noexcept = default;
    ColumnEmbeddingConst& operator=(const ColumnEmbeddingConst&) = default;
    ColumnEmbeddingConst& operator=(ColumnEmbeddingConst&&) noexcept = default;

    std::span<const float> operator[](size_t /*unused*/) const {
        return {_data.data(), _data.size()};
    }

    std::span<const float> at(size_t /*unused*/) const {
        return {_data.data(), _data.size()};
    }

    size_t size() const override { return 1; }

    void set(std::vector<float>&& data) { _data = std::move(data); }

    void assign(const Column* other) override {
        const auto* otherCol = dynamic_cast<const ColumnEmbeddingConst*>(other);
        bioassert(otherCol, "ColumnEmbeddingConst::assign: other is not a ColumnEmbeddingConst");
        _data = otherCol->_data;
    }

    void assignFromLine(const Column* other, size_t startLine, size_t rowCount) override {
        const auto* otherCol = dynamic_cast<const ColumnEmbeddingConst*>(other);
        bioassert(otherCol, "ColumnEmbeddingConst::assignFromLine: other is not a ColumnEmbeddingConst");

        if (rowCount == 0) {
            return;
        }

        _data = otherCol->_data;
    }

    void dump(std::ostream& out) const override {
        (void)out;
    }

    static consteval auto staticKind() { return _staticKind; }

    ContainerKind::Code getContainerKind() const override {
        return ContainerKind::code<ColumnEmbeddingConst>();
    }

    InternalKind::Code getInternalKind() const override { return 0; }
    std::string_view getTypeName() const override { return NameOf<ColumnEmbeddingConst>::get(); }

private:
    std::vector<float> _data;

    static constexpr auto _staticKind = ColumnKind::code<ColumnEmbeddingConst>();
};

}
