#include "PatternElement.h"

#include "CypherAST.h"

using namespace db;

PatternElement::PatternElement()
{
}

PatternElement::~PatternElement() {
}

PatternElement* PatternElement::create(CypherAST* ast) {
    PatternElement* element = new PatternElement();
    ast->addPatternElement(element);
    return element;
}
