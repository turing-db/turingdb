#pragma once

#include "PlanGraphNode.h"

#include <stdint.h>
#include <string_view>
#include <vector>

namespace db {

class VarDecl;

class VectorSearchNode : public PlanGraphNode {
public:
    VectorSearchNode(std::string_view indexName,
                     uint64_t k,
                     const std::vector<float>& queryVector)
        : PlanGraphNode(PlanGraphOpcode::VECTOR_SEARCH),
        _indexName(indexName),
        _k(k),
        _queryVector(queryVector)
    {
    }

    std::string_view getIndexName() const { return _indexName; }
    uint64_t getK() const { return _k; }
    const std::vector<float>& getQueryVector() const { return _queryVector; }

    void setIDsVarDecl(const VarDecl* decl) { _idsVarDecl = decl; }
    const VarDecl* getIDsVarDecl() const { return _idsVarDecl; }

private:
    std::string_view _indexName;
    uint64_t _k {10};
    std::vector<float> _queryVector;
    const VarDecl* _idsVarDecl {nullptr};
};

}
