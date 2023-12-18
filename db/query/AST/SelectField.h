#pragma once

#include <string>

namespace db {

class ASTContext;

class SelectField {
public:
    friend ASTContext;

    static SelectField* create(ASTContext* ctxt);

    void setAll(bool isAll) { _isAll = isAll; }
    bool isAll() const { return _isAll; }

    void setName(const std::string& name) { _name = name; }
    const std::string& getName() const { return _name; }

private:
    bool _isAll {false};
    std::string _name;

    SelectField();
    ~SelectField();
};

}
