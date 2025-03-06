#pragma once

#include <string>
#include <vector>
#include <memory>

namespace db {

class ASTContext;
class DeclContext;
class SelectProjection;
class FromTarget;

class QueryCommand {
public:
    friend ASTContext;

    enum class Kind {
        SELECT_COMMAND = 0,
        CREATE_GRAPH_COMMAND,
        LIST_GRAPH_COMMAND,
        LOAD_GRAPH_COMMAND,
        EXPLAIN_COMMAND
    };

    virtual Kind getKind() const = 0;

protected:
    QueryCommand() = default;
    virtual ~QueryCommand() = default;
    void registerCmd(ASTContext* ctxt);
};

class SelectCommand : public QueryCommand {
public:
    using FromTargets = std::vector<FromTarget*>;

    static SelectCommand* create(ASTContext* ctxt);

    DeclContext* getDeclContext() const { return _declContext.get(); }

    Kind getKind() const override { return Kind::SELECT_COMMAND; }

    SelectProjection* getProjection() const { return _proj; }
    const FromTargets& fromTargets() const { return _fromTargets; }

    void setProjection(SelectProjection* proj) { _proj = proj; }
    void addFromTarget(FromTarget* fromTarget);

private:
    std::unique_ptr<DeclContext> _declContext;
    SelectProjection* _proj {nullptr};
    FromTargets _fromTargets;

    SelectCommand();
    ~SelectCommand() override;
};

class CreateGraphCommand : public QueryCommand {
public:

    static CreateGraphCommand* create(ASTContext* ctxt, const std::string& name);

    Kind getKind() const override { return Kind::CREATE_GRAPH_COMMAND; }

    const std::string& getName() const { return _name; }

private:
    std::string _name;

    CreateGraphCommand(const std::string& name);
    ~CreateGraphCommand() override;
};

class ListGraphCommand : public QueryCommand {
public:
    static ListGraphCommand* create(ASTContext* ctxt);

    Kind getKind() const override { return Kind::LIST_GRAPH_COMMAND; }

private:
    ListGraphCommand() = default;
    ~ListGraphCommand() override = default;
};

class LoadGraphCommand : public QueryCommand {
public:
    static LoadGraphCommand* create(ASTContext* ctxt, const std::string& name);

    Kind getKind() const override { return Kind::LOAD_GRAPH_COMMAND; }

    const std::string& getName() const { return _name; }

private:
    std::string _name;

    LoadGraphCommand(const std::string& name);
    ~LoadGraphCommand() override = default;
};

class ExplainCommand : public QueryCommand {
public:
    static ExplainCommand* create(ASTContext* ctxt, QueryCommand* query);

    Kind getKind() const override { return Kind::EXPLAIN_COMMAND; }

    QueryCommand* getQuery() const { return _query; }

private:
    QueryCommand* _query {nullptr};

    ExplainCommand(QueryCommand* query);
    ~ExplainCommand() override = default;
};

}
