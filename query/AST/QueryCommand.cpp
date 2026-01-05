#include "QueryCommand.h"

using namespace db::v2;

QueryCommand::QueryCommand(DeclContext* declContext)
    : _declContext(declContext)
{
}

QueryCommand::~QueryCommand() {
}
