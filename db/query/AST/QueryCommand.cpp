#include "QueryCommand.h"

#include "ASTContext.h"

using namespace db;

void QueryCommand::registerCmd(ASTContext* ctxt) {
    ctxt->addCmd(this);
}

/* ListCommand */

ListCommand::ListCommand()
{
}

ListCommand::~ListCommand() {
}

ListCommand* ListCommand::create(ASTContext* ctxt) {
    ListCommand* listCmd = new ListCommand();
    listCmd->registerCmd(ctxt);
    return listCmd;
}

/* OpenCommand */

OpenCommand::OpenCommand(const std::string& path)
    : _path(path)
{
}

OpenCommand::~OpenCommand() {
}

OpenCommand* OpenCommand::create(ASTContext* ctxt, const std::string& path) {
    OpenCommand* openCmd = new OpenCommand(path);
    openCmd->registerCmd(ctxt);
    return openCmd;
}

/* SelectCommand */

SelectCommand::SelectCommand()
{
}

SelectCommand::~SelectCommand() {
}

SelectCommand* SelectCommand::create(ASTContext* ctxt) {
    SelectCommand* selectCmd = new SelectCommand();
    selectCmd->registerCmd(ctxt);
    return selectCmd;
}

void SelectCommand::addSelectField(SelectField* field) {
    _selectFields.push_back(field);
}