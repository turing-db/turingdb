#pragma once

#include <memory>

namespace db {

class DeclContext;
class DeclContainer;

class QueryCommand {
public:
    explicit QueryCommand(DeclContainer& declContainer);
    virtual ~QueryCommand() = 0;

    QueryCommand(const QueryCommand&) = delete;
    QueryCommand(QueryCommand&&) = delete;
    QueryCommand& operator=(const QueryCommand&) = delete;
    QueryCommand& operator=(QueryCommand&&) = delete;

    DeclContext& getRootContext() {
        return *_rootCtxt;
    }

    const DeclContext& getRootContextPtr() const {
        return *_rootCtxt;
    }

private:
    std::unique_ptr<DeclContext> _rootCtxt;
};

}
