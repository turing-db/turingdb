#pragma once

#include <span>
#include <vector>

#include "Column.h"

#include "BioAssert.h"
#include "NameOf.h"

namespace db {

class ColumnEmbeddingMany : public Column {
public:
    ColumnEmbeddingMany()
        : Column(_staticKind),
        _dim(0)
    {
    }

    explicit ColumnEmbeddingMany(uint32_t dim)
        : Column(_staticKind),
        _dim(dim)
    {
    }

    ~ColumnEmbeddingMany() override = default;

    ColumnEmbeddingMany(const ColumnEmbeddingMany&) = default;
    ColumnEmbeddingMany(ColumnEmbeddingMany&&) noexcept = default;
    ColumnEmbeddingMany& operator=(const ColumnEmbeddingMany&) = default;
    ColumnEmbeddingMany& operator=(ColumnEmbeddingMany&&) noexcept = default;

    std::span<const float> operator[](size_t i) const {
        return {_data.data() + i * _dim, _dim};
    }

    std::span<const float> at(size_t i) const {
        return {_data.data() + i * _dim, _dim};
    }

    void push_back(std::span<const float> embedding) {
        _data.insert(_data.end(), embedding.begin(), embedding.end());
    }

    void reserve(size_t count) { _data.reserve(count * _dim); }

    size_t size() const override { return _dim > 0 ? _data.size() / _dim : 0; }

    void assign(const Column* other) override {
        const auto* otherCol = dynamic_cast<const ColumnEmbeddingMany*>(other);
        bioassert(otherCol, "ColumnEmbeddingMany::assign: other is not a ColumnEmbeddingMany");
        _data = otherCol->_data;
        _dim = otherCol->_dim;
    }

    void assignFromLine(const Column* other, size_t startLine, size_t rowCount) override {
        const auto* otherCol = dynamic_cast<const ColumnEmbeddingMany*>(other);
        bioassert(otherCol, "ColumnEmbeddingMany::assignFromLine: other is not a ColumnEmbeddingMany");

        _data.clear();
        _dim = otherCol->_dim;
        const auto otherStart = otherCol->_data.cbegin() + startLine * _dim;
        _data.assign(otherStart, otherStart + rowCount * _dim);
    }

    void clear() { _data.clear(); }
    void setDimension(uint32_t dim) { _dim = dim; }

    void dump(std::ostream& out) const override {
        (void)out;
    }

    uint32_t dimension() const { return _dim; }

    static consteval auto staticKind() { return _staticKind; }

    ContainerKind::Code getContainerKind() const override {
        return ContainerKind::code<ColumnEmbeddingMany>();
    }

    InternalKind::Code getInternalKind() const override { return 0; }
    std::string_view getTypeName() const override { return NameOf<ColumnEmbeddingMany>::get(); }

private:
    std::vector<float> _data;
    uint32_t _dim;

    static constexpr auto _staticKind = ColumnKind::code<ColumnEmbeddingMany>();
};

}
