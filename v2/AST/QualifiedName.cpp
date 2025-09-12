#include "QualifiedName.h"

#include "CypherAST.h"

using namespace db::v2;

QualifiedName::QualifiedName()
{
}

QualifiedName::~QualifiedName() {
}

QualifiedName* QualifiedName::create(CypherAST* ast) {
    QualifiedName* qualifiedName = new QualifiedName();
    ast->addQualifiedName(qualifiedName);
    return qualifiedName;
}

void QualifiedName::addName(Symbol* symbol) {
    _names.push_back(symbol);
}
