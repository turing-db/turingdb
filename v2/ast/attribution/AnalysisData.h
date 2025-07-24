#pragma once

#include <variant>
#include <memory>

#include "ASTException.h"
#include "attribution/ASTNodeID.h"
#include "attribution/EvaluatedType.h"

namespace db {

class VariableDecl;

struct PropertyExpressionData;
struct NodeLabelExpressionData;
struct LiteralExpressionData;
struct SymbolData;
struct NodePatternData;
struct EdgePatternData;

class AnalysisData {
public:
    template <typename T>
    using UniquePtr = std::unique_ptr<T, void (*)(T*)>;

    using Variant = std::variant<std::monostate,
                                 UniquePtr<PropertyExpressionData>,
                                 UniquePtr<NodeLabelExpressionData>,
                                 UniquePtr<LiteralExpressionData>,
                                 UniquePtr<SymbolData>,
                                 UniquePtr<NodePatternData>,
                                 UniquePtr<EdgePatternData>>;
    AnalysisData() = default;
    ~AnalysisData() = default;

    AnalysisData(ASTNodeID id, EvaluatedType type)
        : _id(id),
          _type(type)
    {
    }

    AnalysisData(const AnalysisData&) = delete;
    AnalysisData(AnalysisData&&) = default;
    AnalysisData& operator=(const AnalysisData&) = delete;
    AnalysisData& operator=(AnalysisData&&) = default;

    template <typename T>
    bool is() const {
        return std::holds_alternative<std::unique_ptr<T, void (*)(T*)>>(_data);
    }

    template <typename T>
    T& as() {
        return *std::get<UniquePtr<T>>(_data);
    }

    template <typename T>
    const T& as() const {
        return *std::get<UniquePtr<T>>(_data);
    }

    template <typename T, typename... Args>
    T& emplace(Args&&... args) {
        return *_data.emplace<UniquePtr<T>>(T::create(std::forward<Args>(args)...));
    }

    EvaluatedType type() const {
        return _type;
    }

    ASTNodeID id() const {
        return _id;
    }

private:
    ASTNodeID _id;
    Variant _data;
    EvaluatedType _type {};
};


}
