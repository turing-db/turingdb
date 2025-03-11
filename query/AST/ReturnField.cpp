#include "ReturnField.h"

#include "ASTContext.h"

using namespace db;

ReturnField::ReturnField()
{
}

ReturnField::~ReturnField(){
}

ReturnField* ReturnField::create(ASTContext* ctxt) {
    ReturnField* field = new ReturnField();
    ctxt->addReturnField(field);
    return field;
}
