#pragma once

#include <memory>

namespace db {

class CypherAST;

class CypherParser {
public:
    CypherParser();
    ~CypherParser();

    CypherParser(const CypherParser&) = delete;
    CypherParser(CypherParser&&) = default;
    CypherParser& operator=(const CypherParser&) = delete;
    CypherParser& operator=(CypherParser&&) = default;

    void parse(std::string_view query);

    void allowNotImplemented(bool allow) { _allowNotImplemented = allow; }

    const CypherAST& getAST() const {
        return *_ast;
    }

private:
    std::unique_ptr<CypherAST> _ast;
    bool _allowNotImplemented = false;
};

}
