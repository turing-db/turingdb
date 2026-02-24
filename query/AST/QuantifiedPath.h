#pragma once

#include <limits>
#include <stdint.h>

namespace db {

class CypherAST;

class QuantifiedPath {
public:
    static constexpr int64_t UNBOUNDED = std::numeric_limits<int64_t>::max();

    friend CypherAST;

    static QuantifiedPath* create(CypherAST* ast);

    void setLhs(int64_t lhs) { _lhs = lhs; }
    void setRhs(int64_t rhs) { _rhs = rhs; }

    int64_t getLhs() const { return _lhs; }
    int64_t getRhs() const { return _rhs; }

    bool isRhsUnbounded() const { return _rhs == UNBOUNDED; }

private:
    int64_t _lhs {0};
    int64_t _rhs {UNBOUNDED};

    QuantifiedPath();
    ~QuantifiedPath();
};

}
