#pragma once

namespace db::v2 {

class SubStatement {
public:
    SubStatement() = default;
    virtual ~SubStatement() = 0;

    SubStatement(const SubStatement&) = default;
    SubStatement& operator=(const SubStatement&) = default;
    SubStatement(SubStatement&&) = default;
    SubStatement& operator=(SubStatement&&) = default;
};

inline SubStatement::~SubStatement() = default;

}
