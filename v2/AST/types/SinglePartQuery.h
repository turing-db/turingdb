#pragma once

#include <memory>

#include "QueryCommand.h"

namespace db {

class StatementContainer;

class SinglePartQuery : public QueryCommand {
public:
    ~SinglePartQuery() override = default;

    SinglePartQuery(DeclContainer& declContainer,
                    StatementContainer* statements)
        : QueryCommand(declContainer),
        _statements(statements)
    {
    }

    SinglePartQuery(const SinglePartQuery&) = delete;
    SinglePartQuery(SinglePartQuery&&) = delete;
    SinglePartQuery& operator=(const SinglePartQuery&) = delete;
    SinglePartQuery& operator=(SinglePartQuery&&) = delete;

    static std::unique_ptr<SinglePartQuery> create(DeclContainer& declContainer,
                                                   StatementContainer* statements) {
        return std::make_unique<SinglePartQuery>(declContainer, statements);
    }

    const StatementContainer& getStatements() const {
        return *_statements;
    }

private:
    StatementContainer* _statements {nullptr};
};


}
