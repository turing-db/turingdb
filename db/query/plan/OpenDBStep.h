#pragma once

#include <string>

#include "QueryPlanStep.h"

namespace db {

class QueryPlan;

class OpenDBStep : public QueryPlanStep {
public:
    std::string getName() const override;

    static OpenDBStep* create(QueryPlan* plan, const std::string& path);

private:
    std::string _dbPath;

    OpenDBStep(const std::string& path);
    ~OpenDBStep();
};

}
