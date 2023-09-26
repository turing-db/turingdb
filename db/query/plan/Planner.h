#pragma once

namespace db {

class InterpreterContext;
class PullPlan;
class QueryCommand;
class ListCommand;
class OpenCommand;

class Planner {
public:
    Planner(InterpreterContext* interpCtxt);
    ~Planner();

    PullPlan* makePlan(const QueryCommand* cmd);

private:
    InterpreterContext* _interpCtxt {nullptr};

    PullPlan* planListCommand(const ListCommand* cmd);
    PullPlan* planListDatabases(const ListCommand* cmd);
    PullPlan* planOpenCommand(const OpenCommand* cmd);
};

}
