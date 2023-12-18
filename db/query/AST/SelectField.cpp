#include "SelectField.h"

#include "ASTContext.h"

using namespace db;

SelectField::SelectField()
{
}

SelectField::~SelectField()
{
}

SelectField* SelectField::create(ASTContext* ctxt) {
    SelectField* field = new SelectField();
    ctxt->addSelectField(field);
    return field;
}
