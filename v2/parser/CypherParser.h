#pragma once

#include <string_view>

namespace db::v2 {

class CypherAST;

class CypherParser {
public:
    explicit CypherParser(CypherAST& ast);
    ~CypherParser() = default;

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
    CypherAST* _ast {nullptr};
    bool _allowNotImplemented = false;
};

}
