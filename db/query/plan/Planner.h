#pragma once

namespace db {

class QueryCommand;
class InterpreterContext;
class QueryPlan;
class QueryPlanStep;
class ListCommand;
class OpenCommand;
class SelectCommand;
class PathPattern;
class NodePattern;
class PathElement;

class Planner {
public:
    Planner(const QueryCommand* query,
            InterpreterContext* interpCtxt);
    ~Planner();

    QueryPlan* getQueryPlan() const { return _plan; }

    bool buildQueryPlan();

private:
    const QueryCommand* _query {nullptr};
    InterpreterContext* _interpCtxt {nullptr};
    QueryPlan* _plan {nullptr};

    bool planListCommand(const ListCommand* cmd);
    bool planOpenCommand(const OpenCommand* cmd);
    bool planSelectCommand(const SelectCommand* cmd);
    QueryPlanStep* planPathPattern(const PathPattern* pattern);
    QueryPlanStep* planNodePattern(const NodePattern* pattern);
    QueryPlanStep* planPathElement(const PathElement* elem,
                                   QueryPlanStep* prevStep);
};

}
