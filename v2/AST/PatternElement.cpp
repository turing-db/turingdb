#include "PatternElement.h"

#include "CypherAST.h"

using namespace db::v2;

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
