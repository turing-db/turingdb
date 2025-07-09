#pragma once

#include <string>
#include <vector>
#include <memory>

namespace db {

class ASTContext;
class DeclContext;
class ReturnProjection;
class MatchTargets;
class CreateTarget;
class CreateTargets;

class QueryCommand {
public:
    friend ASTContext;

    enum class Kind {
        MATCH_COMMAND = 0,
        CREATE_COMMAND,
        COMMIT_COMMAND,
        CREATE_GRAPH_COMMAND,
        LIST_GRAPH_COMMAND,
        LOAD_GRAPH_COMMAND,
        EXPLAIN_COMMAND,
        HISTORY_COMMAND,
        CHANGE_COMMAND
    };

    virtual Kind getKind() const = 0;

protected:
    QueryCommand() = default;
    virtual ~QueryCommand() = default;
    void registerCmd(ASTContext* ctxt);
};

class MatchCommand : public QueryCommand {
public:
    static MatchCommand* create(ASTContext* ctxt);

    DeclContext* getDeclContext() const { return _declContext.get(); }

    Kind getKind() const override { return Kind::MATCH_COMMAND; }

    ReturnProjection* getProjection() const { return _proj; }
    MatchTargets* getMatchTargets() const { return _matchTargets; }

    void setProjection(ReturnProjection* proj) { _proj = proj; }
    void setMatchTargets(MatchTargets* targets) { _matchTargets = targets; }

private:
    std::unique_ptr<DeclContext> _declContext;
    ReturnProjection* _proj {nullptr};
    MatchTargets* _matchTargets {nullptr};

    MatchCommand();
    ~MatchCommand() override;
};

class CreateCommand : public QueryCommand {
public:
    static CreateCommand* create(ASTContext* ctxt, CreateTargets* targets);

    DeclContext* getDeclContext() const { return _declContext.get(); }
    const CreateTargets& createTargets() const { return *_createTargets; }

    Kind getKind() const override { return Kind::CREATE_COMMAND; }

private:
    std::unique_ptr<DeclContext> _declContext;
    CreateTargets* _createTargets {nullptr};

    explicit CreateCommand(CreateTargets* targets);
    ~CreateCommand() override;
};

class CommitCommand : public QueryCommand {
public:
    static CommitCommand* create(ASTContext* ctxt);

    Kind getKind() const override { return Kind::COMMIT_COMMAND; }

private:
    CommitCommand();
    ~CommitCommand() override;
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

class HistoryCommand : public QueryCommand {
public:
    static HistoryCommand* create(ASTContext* ctxt);

    Kind getKind() const override { return Kind::HISTORY_COMMAND; }

private:
    HistoryCommand();
    ~HistoryCommand() override = default;
};

}
