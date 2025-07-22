#include "QueryCommand.h"

#include "attribution/DeclContext.h"

using namespace db;

QueryCommand::QueryCommand()
    : _rootCtxt(std::make_unique<DeclContext>()) {
}

QueryCommand::~QueryCommand() = default;

