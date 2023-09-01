#pragma once

namespace db::query {

class InterpreterContext;
class PullPlan;
class QueryCommand;
class ListCommand;

class Planner {
public:
    Planner(InterpreterContext* interpCtxt);
    ~Planner();

    PullPlan* makePlan(const QueryCommand* cmd);

private:
    InterpreterContext* _interpCtxt {nullptr};

    PullPlan* planListCommand(const ListCommand* cmd);
    PullPlan* planListDatabases(const ListCommand* cmd);
};

}
