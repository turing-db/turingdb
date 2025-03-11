#include "ReturnProjection.h"

#include "ASTContext.h"

using namespace db;

ReturnProjection::ReturnProjection()
{
}

ReturnProjection::~ReturnProjection() {
}

void ReturnProjection::addField(ReturnField* field) {
    _fields.push_back(field);
}

ReturnProjection* ReturnProjection::create(ASTContext* ctxt) {
    ReturnProjection* proj = new ReturnProjection();
    ctxt->addReturnProjection(proj);
    return proj;
}
