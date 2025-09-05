#pragma once

#include <memory>

#include "Expression.h"
#include "types/PatternElement.h"

namespace db::v2 {

class PathExpression : public Expression {
public:
    PathExpression() = delete;
    ~PathExpression() override = default;

    PathExpression(const PathExpression&) = delete;
    PathExpression(PathExpression&&) = delete;
    PathExpression& operator=(const PathExpression&) = delete;
    PathExpression& operator=(PathExpression&&) = delete;

    explicit PathExpression(PatternElement* pattern)
        : Expression(ExpressionType::Path),
        _pattern(pattern)
    {
    }

    static std::unique_ptr<PathExpression> create(PatternElement* pattern) {
        return std::make_unique<PathExpression>(pattern);
    }

    const PatternElement& pattern() const {
        return *_pattern;
    }

    void addNode(NodePattern* node) {
        _pattern->addNode(node);
    }

    void addEdge(EdgePattern* edge) {
        _pattern->addEdge(edge);
    }

private:
    PatternElement* _pattern {nullptr};
};

}
