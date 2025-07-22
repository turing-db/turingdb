#pragma once

namespace db {

class Statement {
public:
    Statement() = default;
    virtual ~Statement() = 0;

    Statement(const Statement&) = default;
    Statement& operator=(const Statement&) = default;
    Statement(Statement&&) = default;
    Statement& operator=(Statement&&) = default;
};

inline Statement::~Statement() = default;

}
