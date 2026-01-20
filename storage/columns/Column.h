#pragma once

#include "ColumnKind.h"

namespace db {

template <class>
struct ColumnSupportFor : std::false_type {};

template <template <typename> class Container, typename T>
inline constexpr bool ColumnSupportsInternal =
    ColumnSupportFor<Container<T>>::value;

class Column {
public:
    Column() = delete;
    Column(const Column&) = default;
    Column(Column&&) noexcept = default;
    Column& operator=(const Column&) = default;
    Column& operator=(Column&&) noexcept = default;
    virtual ~Column();

    virtual size_t size() const = 0;

    virtual void assign(const Column* other) = 0;
    virtual void assignFromLine(const Column* other, size_t startLine, size_t rowCount) = 0;

    virtual void dump(std::ostream& out) const = 0;

    ColumnKind::Code getKind() const { return _kind; }

    virtual ContainerKind::Code getContainerKind() const = 0;
    virtual InternalKind::Code getInternalKind() const = 0;

    template <typename T>
    T* cast() { return static_cast<T*>(this); }

protected:
    explicit Column(ColumnKind::Code kind)
        : _kind(kind)
    {
    }

    ColumnKind::Code _kind = 0;
};

}
