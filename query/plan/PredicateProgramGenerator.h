#pragma once

#include "ExprProgramGenerator.h"

#include "metadata/LabelSet.h"

namespace db {

class PredicateProgramGenerator final : public ExprProgramGenerator {
public:
    PredicateProgramGenerator(PipelineGenerator* gen,
                              ExprProgram* exprProg,
                              const PendingOutputView& pendingOut)
        : ExprProgramGenerator(gen, exprProg, pendingOut)
    {
    }

    ~PredicateProgramGenerator() final = default;
    
    void generatePredicate(const Predicate* pred);
    void addLabelConstraint(Column* lblsetCol, const LabelSet& lblConstraint);
    void addEdgeTypeConstraint(Column* edgeTypeCol, const EdgeTypeID& typeConstr);
};

}
