#pragma once

#include "VariableType.h"

namespace db {

class VarDecl {
public:
    VarDecl() = default;
    ~VarDecl() = default;

    VarDecl(std::string_view name, VariableType type)
        : _name(name), _type(type)
    {
    }

    explicit VarDecl(VariableType type)
        : _type(type)
    {
    }

    VarDecl(const VarDecl&) = default;
    VarDecl& operator=(const VarDecl&) = default;
    VarDecl(VarDecl&&) = default;
    VarDecl& operator=(VarDecl&&) = default;

    const std::string_view& name() const {
        return _name;
    }

    VariableType type() const {
        return _type;
    }

private:
    std::string_view _name;
    VariableType _type {};
};

}
