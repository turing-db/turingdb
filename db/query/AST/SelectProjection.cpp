#include "SelectProjection.h"

#include "ASTContext.h"

using namespace db;

SelectProjection::SelectProjection()
{
}

SelectProjection::~SelectProjection() {
}

void SelectProjection::addField(SelectField* field) {
    _fields.push_back(field);
}

SelectProjection* SelectProjection::create(ASTContext* ctxt) {
    SelectProjection* proj = new SelectProjection();
    ctxt->addSelectProjection(proj);
    return proj;
}