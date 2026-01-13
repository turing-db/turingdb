#pragma once

#include "ExprProgram.h"

namespace db {

class Column;

class PredicateProgram final : public ExprProgram {
public:
    static PredicateProgram* create(PipelineV2* pipeline);

    void addTopLevelPredicate(Column* resultCol) {
        _topLevelPredicates.push_back(resultCol);
    }

    const std::vector<Column*>& getTopLevelPredicates() const { return _topLevelPredicates; }

private:
    // Contains pointers to the result columns of different instructions. All need be true.
    std::vector<Column*> _topLevelPredicates;

    PredicateProgram() = default;
    ~PredicateProgram() final = default;
};
}
