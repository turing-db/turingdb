#pragma once

#include "VariableType.h"
#include "attribution/DeclID.h"
#include "attribution/DeclContainer.h"

namespace db {

class ConstVariableDecl;

class VariableDecl {
public:
    ~VariableDecl() = default;

    VariableDecl(DeclContainer& container, DeclID id = {-1})
        : _container(container),
          _id(id)
    {
    }

    VariableDecl(const VariableDecl&) = default;
    VariableDecl(VariableDecl&&) = default;
    VariableDecl& operator=(const VariableDecl&) = delete;
    VariableDecl& operator=(VariableDecl&&) = delete;

    operator ConstVariableDecl() const;

    DeclID id() const {
        return _id;
    }

    bool valid() const {
        return _id.valid();
    }

    DeclContainer& container() {
        return _container;
    }

    const DeclContainer& container() const {
        return _container;
    }

    VariableType type() const {
        return _container.getType(_id);
    }

    const VariableData& data() const {
        return _container.getData(_id);
    }

    void setData(VariableData&& data) {
        _container.setData(_id, std::move(data));
    }

    std::string_view name() const {
        return _container.getName(_id);
    }

private:
    DeclContainer& _container;
    DeclID _id;
};

class ConstVariableDecl {
public:
    ~ConstVariableDecl() = default;

    ConstVariableDecl(const DeclContainer& container, DeclID id = {-1})
        : _container(container),
          _id(id)
    {
    }

    ConstVariableDecl(const ConstVariableDecl&) = default;
    ConstVariableDecl(ConstVariableDecl&&) = default;
    ConstVariableDecl& operator=(const ConstVariableDecl&) = delete;
    ConstVariableDecl& operator=(ConstVariableDecl&&) = delete;

    DeclID id() const {
        return _id;
    }

    bool valid() const {
        return _id.valid();
    }

    const DeclContainer& container() const {
        return _container;
    }

    VariableType type() const {
        return _container.getType(_id);
    }

    const VariableData& data() const {
        return _container.getData(_id);
    }

    std::string_view name() const {
        return _container.getName(_id);
    }

private:
    const DeclContainer& _container;
    DeclID _id;
};

inline VariableDecl::operator ConstVariableDecl() const {
    return ConstVariableDecl {_container, _id};
}

}
