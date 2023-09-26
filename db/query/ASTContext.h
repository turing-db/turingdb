#pragma once

#include <vector>

namespace db {

class QueryCommand;

class ASTContext {
public:
    friend QueryCommand;

    ASTContext();
    ~ASTContext();

    QueryCommand* getRoot() const { return _root; } 

    void setRoot(QueryCommand* cmd) { _root = cmd; }

    bool hasError() const { return _isError; }
    void setError(bool hasError) { _isError = hasError; }

private:
    bool _isError {false};
    QueryCommand* _root {nullptr};
    std::vector<QueryCommand*> _cmds;

    void addCmd(QueryCommand* cmd);
};

} 
