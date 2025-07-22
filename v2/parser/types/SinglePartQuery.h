#pragma once

#include <memory>

#include "QueryCommand.h"

namespace db {

class StatementContainer;

class SinglePartQuery : public QueryCommand {
public:
    SinglePartQuery() = default;
    ~SinglePartQuery() override = default;

    SinglePartQuery(StatementContainer* statements)
        : _statements(statements)
    {
    }

    SinglePartQuery(const SinglePartQuery&) = delete;
    SinglePartQuery(SinglePartQuery&&) = delete;
    SinglePartQuery& operator=(const SinglePartQuery&) = delete;
    SinglePartQuery& operator=(SinglePartQuery&&) = delete;

    static std::unique_ptr<SinglePartQuery> create(StatementContainer* statements) {
        return std::make_unique<SinglePartQuery>(statements);
    }

    const StatementContainer& getStatements() const {
        return *_statements;
    }

private:
    StatementContainer* _statements {nullptr};
};


}
