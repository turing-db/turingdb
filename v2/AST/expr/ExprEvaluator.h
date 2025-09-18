#pragma once

#include <cstdint>
#include <vector>

namespace db::v2 {

class ExprEvaluator {
public:
    struct ExpressionInfo {
        uint16_t _exprId;
    };

    ExprEvaluator() = default;
    ~ExprEvaluator() = default;

    ExprEvaluator(const ExprEvaluator&) = delete;
    ExprEvaluator(ExprEvaluator&&) = delete;
    ExprEvaluator& operator=(const ExprEvaluator&) = delete;
    ExprEvaluator& operator=(ExprEvaluator&&) = delete;

private:
    std::vector<ExpressionInfo> _exprInfos;
};

}
