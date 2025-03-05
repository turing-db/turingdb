#include "QueryCommand.h"

#include "ASTContext.h"
#include "DeclContext.h"

using namespace db;

void QueryCommand::registerCmd(ASTContext* ctxt) {
    ctxt->addCmd(this);
}

// SelectCommand

SelectCommand::SelectCommand()
    : _declContext(std::make_unique<DeclContext>())
{
}

SelectCommand::~SelectCommand() {
}

SelectCommand* SelectCommand::create(ASTContext* ctxt) {
    SelectCommand* selectCmd = new SelectCommand();
    selectCmd->registerCmd(ctxt);
    return selectCmd;
}

void SelectCommand::addFromTarget(FromTarget* fromTarget) {
    _fromTargets.push_back(fromTarget);
}

// CreateGraphCommand

CreateGraphCommand::CreateGraphCommand(const std::string& name)
    : _name(name)
{
}

CreateGraphCommand::~CreateGraphCommand() {
}

CreateGraphCommand* CreateGraphCommand::create(ASTContext* ctxt, const std::string& name) {
    CreateGraphCommand* cmd = new CreateGraphCommand(name);
    cmd->registerCmd(ctxt);
    return cmd;
}

// ListGraphCommand

ListGraphCommand* ListGraphCommand::create(ASTContext* ctxt) {
    ListGraphCommand* cmd = new ListGraphCommand();
    cmd->registerCmd(ctxt);
    return cmd;
}

// LoadGraphCommand

LoadGraphCommand::LoadGraphCommand(const std::string& name)
    : _name(name)
{
}

LoadGraphCommand* LoadGraphCommand::create(ASTContext* ctxt, const std::string& name) {
    LoadGraphCommand* cmd = new LoadGraphCommand(name);
    cmd->registerCmd(ctxt);
    return cmd;
}

// ExplainCommand

ExplainCommand::ExplainCommand(const QueryCommand* query)
    : _query(query)
{
}

ExplainCommand* ExplainCommand::create(ASTContext* ctxt, const QueryCommand* query) {
    ExplainCommand* cmd = new ExplainCommand(query);
    cmd->registerCmd(ctxt);
    return cmd;
}
