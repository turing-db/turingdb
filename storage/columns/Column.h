#pragma once

#include "ColumnKind.h"

namespace db {

class Column {
public:
    Column() = delete;
    Column(const Column&) = default;
    Column(Column&&) noexcept = default;
    Column& operator=(const Column&) = default;
    Column& operator=(Column&&) noexcept = default;
    virtual ~Column();

    virtual size_t size() const = 0;

    virtual void resize(size_t newSize) = 0;

    virtual void assign(const Column* other) = 0;
    virtual void assignFromLine(const Column* other, size_t startLine, size_t rowCount) = 0;

    virtual void dump(std::ostream& out) const = 0;

    ColumnKind::ColumnKindCode getKind() const { return _kind; }

    template <typename T>
    T* cast() { return static_cast<T*>(this); }

protected:
    explicit Column(ColumnKind::ColumnKindCode kind)
        : _kind(kind)
    {
    }

    ColumnKind::ColumnKindCode _kind = 0;
};

}
