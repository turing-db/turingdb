#pragma once

#include <vector>

namespace db {

class SelectField;
class ASTContext;

class SelectProjection {
public:
    friend ASTContext;
    using SelectFields = std::vector<SelectField*>;

    const SelectFields& selectFields() const { return _fields; }

    void addField(SelectField* field);

    static SelectProjection* create(ASTContext* ctxt);

private:
    SelectFields _fields;

    SelectProjection();
    ~SelectProjection();
};

}