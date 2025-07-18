#pragma once

#include <memory>

#include "Expression.h"
#include "types/PatternPart.h"

namespace db {

class PathExpression : public Expression {
public:
    PathExpression() = delete;
    ~PathExpression() override = default;

    PathExpression(const PathExpression&) = delete;
    PathExpression(PathExpression&&) = delete;
    PathExpression& operator=(const PathExpression&) = delete;
    PathExpression& operator=(PathExpression&&) = delete;

    explicit PathExpression(PatternPart* pattern)
        : Expression(ExpressionType::Path),
          _pattern(pattern)
    {
    }


    static std::unique_ptr<PathExpression> create(PatternPart* pattern) {
        return std::make_unique<PathExpression>(pattern);
    }

    const PatternPart* pattern() const {
        return _pattern;
    }


    void addNode(PatternNode* node) {
        _pattern->addNode(node);
    }

    void addEdge(PatternEdge* edge) {
        _pattern->addEdge(edge);
    }

private:
    PatternPart* _pattern {nullptr};
};

}
