#pragma once

#include <string_view>

#include "dataframe/ColumnTag.h"
#include "decl/EvaluatedType.h"

namespace db::v2 {

class CypherAST;
class DeclContext;

class VarDecl {
public:
    friend CypherAST;

    static VarDecl* create(CypherAST* ast,
                           DeclContext* declContext,
                           std::string_view name,
                           EvaluatedType type);

    void setTag(ColumnTag tag) { _tag = tag; }

    EvaluatedType getType() const { return _type; }
    const std::string_view& getName() const { return _name; }
    const ColumnTag& getTag() const { return _tag; }

private:
    EvaluatedType _type {EvaluatedType::Invalid};
    std::string_view _name;
    ColumnTag _tag {UINT64_MAX};

    VarDecl(EvaluatedType type, std::string_view name)
        : _type(type),
        _name(name)
    {
    }

    ~VarDecl() = default;
};

}
