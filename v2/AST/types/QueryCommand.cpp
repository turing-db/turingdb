#include "QueryCommand.h"

#include "decl/DeclContext.h"

using namespace db::v2;

QueryCommand::QueryCommand(DeclContainer& declContainer)
    : _rootCtxt(std::make_unique<DeclContext>(declContainer))
{
}

QueryCommand::~QueryCommand() = default;

