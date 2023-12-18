#pragma once

#include <string>
#include <vector>

#include "DeclContext.h"

namespace db {

class ASTContext;
class SelectField;
class FromTarget;

class QueryCommand {
public:
    friend ASTContext;

    enum Kind {
        QCOM_LIST_COMMAND,
        QCOM_OPEN_COMMAND,
        QCOM_SELECT_COMMAND
    };

    virtual Kind getKind() const = 0;

protected:
    QueryCommand() = default;
    virtual ~QueryCommand() = default;
    void registerCmd(ASTContext* ctxt);
};

class ListCommand : public QueryCommand {
public:
    enum SubType {
        LCOM_UNKNOWN,
        LCOM_DATABASES
    };

    Kind getKind() const override { return QCOM_LIST_COMMAND; }

    SubType getSubType() const { return _subType; }
    void setSubType(SubType subType) { _subType = subType; }

    static ListCommand* create(ASTContext* ctxt);

private:
    SubType _subType {LCOM_UNKNOWN};

    ListCommand();
    ~ListCommand();
};

class OpenCommand : public QueryCommand {
public:
    static OpenCommand* create(ASTContext* ctxt, const std::string& path);

    Kind getKind() const override { return QCOM_OPEN_COMMAND; }

    const std::string& getPath() const { return _path; }

private:
    std::string _path;

    OpenCommand(const std::string& path);
    ~OpenCommand();
};

class SelectCommand : public QueryCommand {
public:
    using SelectFields = std::vector<SelectField*>;
    using FromTargets = std::vector<FromTarget*>;

    static SelectCommand* create(ASTContext* ctxt);

    Kind getKind() const override { return QCOM_SELECT_COMMAND; }

    const SelectFields& selectFields() const { return _selectFields; }
    const FromTargets& fromTargets() const { return _fromTargets; }

    void addSelectField(SelectField* field);
    void addFromTarget(FromTarget* fromTarget);

private:
    SelectFields _selectFields;
    FromTargets _fromTargets;

    SelectCommand();
    ~SelectCommand();
};

}
