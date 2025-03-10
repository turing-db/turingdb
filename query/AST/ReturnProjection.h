#pragma once

#include <vector>

namespace db {

class ReturnField;
class ASTContext;

class ReturnProjection {
public:
    friend ASTContext;
    using ReturnFields = std::vector<ReturnField*>;

    const ReturnFields& returnFields() const { return _fields; }

    void addField(ReturnField* field);

    static ReturnProjection* create(ASTContext* ctxt);

private:
    ReturnFields _fields;

    ReturnProjection();
    ~ReturnProjection();
};

}
