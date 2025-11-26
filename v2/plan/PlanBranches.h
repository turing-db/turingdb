#pragma once

#include <vector>
#include <ostream>

namespace db::v2 {

class PlanBranch;
class PlanGraph;
class PlanGraphNode;

class PlanBranches {
public:
    friend PlanBranch;
    using Branches = std::vector<PlanBranch*>;

    PlanBranches();
    ~PlanBranches();

    const Branches& branches() const { return _branches; }
    const Branches& rootBranches() const { return _roots; }

    void generate(const PlanGraph* planGraph);

    void topologicalSort(std::vector<PlanBranch*>& sort);

    void dump(std::ostream& out);

private:
    Branches _branches;
    Branches _roots;

    void addBranch(PlanBranch* branch);
};

}
