#include "QueryCommand.h"

using namespace db;

QueryCommand::QueryCommand(DeclContext* declContext)
    : _declContext(declContext)
{
}

QueryCommand::~QueryCommand() {
}
