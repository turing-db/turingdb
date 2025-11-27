#pragma once

#include <vector>
#include <ostream>

namespace db::v2 {

class PipelineBranch;
class PlanGraph;
class PlanGraphNode;

class PipelineBranches {
public:
    friend PipelineBranch;
    using Branches = std::vector<PipelineBranch*>;

    PipelineBranches();
    ~PipelineBranches();

    const Branches& branches() const { return _branches; }
    const Branches& rootBranches() const { return _roots; }

    void generate(const PlanGraph* planGraph);

    void topologicalSort(std::vector<PipelineBranch*>& sort);

    void dumpMermaid(std::ostream& out);

private:
    Branches _branches;
    Branches _roots;

    void addBranch(PipelineBranch* branch);
};

}
