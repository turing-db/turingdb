#pragma once

#include "decl/DeclID.h"
#include "decl/EvaluatedType.h"

namespace db::v2 {

class VarDecl {
public:
    ~VarDecl() = default;

    VarDecl(EvaluatedType type = EvaluatedType::Invalid, DeclID id = -1)
        : _id(id),
          _type(type)
    {
    }

    VarDecl(const VarDecl&) = delete;
    VarDecl(VarDecl&&) = delete;
    VarDecl& operator=(const VarDecl&) = delete;
    VarDecl& operator=(VarDecl&&) = delete;

    DeclID id() const {
        return _id;
    }

    bool valid() const {
        return _id.valid();
    }

    EvaluatedType type() const {
        return _type;
    }

    const std::string_view& name() const {
        return _name;
    }

    void setName(std::string_view name) {
        _name = name;
    }

private:
    DeclID _id;
    EvaluatedType _type {EvaluatedType::Invalid};
    std::string_view _name;
};

}
