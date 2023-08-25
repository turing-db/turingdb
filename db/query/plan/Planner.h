#pragma once

namespace db::query {

class PullPlan;
class QueryCommand;
class ListCommand;

class Planner {
public:
    Planner();
    ~Planner();

    PullPlan* makePlan(const QueryCommand* cmd);

private:
    PullPlan* planListCommand(const ListCommand* cmd);
    PullPlan* planListDatabases(const ListCommand* cmd);
};

}
