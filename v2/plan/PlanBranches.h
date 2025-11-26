#pragma once

#include <vector>
#include <ostream>

namespace db::v2 {

class PlanBranch;
class PlanGraph;

class PlanBranches {
public:
    friend PlanBranch;

    PlanBranches();
    ~PlanBranches();

    void generate(const PlanGraph* planGraph);

    void dump(std::ostream& out);

private:
    std::vector<PlanBranch*> _branches;

    void addBranch(PlanBranch* branch);
};

}
