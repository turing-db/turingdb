#include "Parameter.h"

#include "CypherAST.h"

using namespace db;

Parameter::Parameter(std::string_view name)
    : _name(name)
{
}

Parameter::~Parameter() {
}

Parameter* Parameter::create(CypherAST* ast, std::string_view name) {
    Parameter* parameter = new Parameter(name);
    ast->addParameter(parameter);
    return parameter;
}
