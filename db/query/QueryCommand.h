#pragma once

#include <string>

namespace db {

class ASTContext;

class QueryCommand {
public:
    friend ASTContext;

    enum Kind {
        QCOM_LIST_COMMAND,
        QCOM_OPEN_COMMAND
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

}
