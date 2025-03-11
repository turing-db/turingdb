#pragma once

#include <string>
#include <vector>
#include <memory>

namespace db {

class ASTContext;
class DeclContext;
class ReturnProjection;
class MatchTarget;

class QueryCommand {
public:
    friend ASTContext;

    enum class Kind {
        MATCH_COMMAND = 0,
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

class MatchCommand : public QueryCommand {
public:
    using MatchTargets = std::vector<MatchTarget*>;

    static MatchCommand* create(ASTContext* ctxt);

    DeclContext* getDeclContext() const { return _declContext.get(); }

    Kind getKind() const override { return Kind::MATCH_COMMAND; }

    ReturnProjection* getProjection() const { return _proj; }
    const MatchTargets& matchTargets() const { return _matchTargets; }

    void setProjection(ReturnProjection* proj) { _proj = proj; }
    void addMatchTarget(MatchTarget* matchTarget);

private:
    std::unique_ptr<DeclContext> _declContext;
    ReturnProjection* _proj {nullptr};
    MatchTargets _matchTargets;

    MatchCommand();
    ~MatchCommand() override;
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
