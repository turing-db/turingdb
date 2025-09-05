#pragma once

#include <memory>
#include <optional>
#include <vector>
#include <unordered_map>

#include "SourceLocation.h"
#include "decl/AnalysisData.h"
#include "decl/DeclContainer.h"
#include "statements/StatementContainer.h"
#include "statements/SubStatement.h"

namespace db::v2 {

class NodePattern;
class EdgePattern;
class PatternElement;
class EntityPattern;
class Pattern;
class Expression;
class QueryCommand;
class Projection;
class MapLiteral;
class SinglePartQuery;
class Statement;
class Symbol;
class ExprData;

class CypherAST {
public:
    template <typename T>
    using UniquePtrVector = std::vector<std::unique_ptr<T>>;

    CypherAST(std::string_view query);
    ~CypherAST();

    CypherAST(const CypherAST&) = delete;
    CypherAST(CypherAST&&) = delete;
    CypherAST& operator=(const CypherAST&) = delete;
    CypherAST& operator=(CypherAST&&) = delete;

    const std::string_view& query() const {
        return _query;
    }

    NodePattern* nodeFromExpression(Expression* e);
    Pattern* newPattern();
    PatternElement* newPatternElem();
    EdgePattern* newOutEdge(const std::optional<Symbol>& symbol,
                            std::optional<std::vector<std::string_view>>&& types,
                            MapLiteral* properties);
    EdgePattern* newInEdge(const std::optional<Symbol>& symbol,
                           std::optional<std::vector<std::string_view>>&& types,
                           MapLiteral* properties);
    Projection* newProjection();
    MapLiteral* newMapLiteral();
    SinglePartQuery* newSinglePartQuery();
    NodePattern* newNode(const std::optional<Symbol>& symbol,
                         std::optional<std::vector<std::string_view>>&& labels,
                         MapLiteral* properties);

    template <typename T, typename... Args>
        requires std::is_base_of_v<Statement, T>
    T* newStatement(Args&&... args) {
        auto st = T::create(std::forward<Args>(args)...);
        auto* ptr = st.get();
        _statements.emplace_back(std::move(st));
        _currentStatements->add(ptr);

        return ptr;
    }

    template <typename T, typename... Args>
        requires std::is_base_of_v<SubStatement, T>
    T* newSubStatement(Args&&... args) {
        auto st = T::create(std::forward<Args>(args)...);
        auto* ptr = st.get();
        _subStatements.emplace_back(std::move(st));

        return ptr;
    }

    template <typename T, typename... Args>
        requires std::is_base_of_v<Expression, T>
    T* newExpression(Args&&... args) {
        auto expr = T::create(std::forward<Args>(args)...);
        auto* ptr = expr.get();
        _expressions.emplace_back(std::move(expr));

        return ptr;
    }

    template <typename T, typename... Args>
    T& newAnalysisData(EvaluatedType type, Args&&... args) {
        auto& data = _data.emplace_back();
        return data.emplace<T>(std::forward<Args>(args)...);
    }

    const UniquePtrVector<QueryCommand>& queries() const {
        return _queries;
    }

    const VarDecl& getVarDecl(DeclID id) const {
        return _declContainer.getDecl(id);
    }

    template <typename T>
    void setLocation(const T* ptr, const SourceLocation& loc) {
        if (!_debugLocation) {
            return;
        }

        _locationMap[(std::uintptr_t)ptr] = loc;
    }

    const SourceLocation* getLocation(uintptr_t ptr) const {
        auto it = _locationMap.find(ptr);
        if (it != _locationMap.end()) {
            return &it->second;
        }
        return nullptr;
    }

    void setDebugLocation(bool debug) {
        _debugLocation = debug;
    }

private:
    std::string_view _query;

    UniquePtrVector<Expression> _expressions;
    UniquePtrVector<Pattern> _patterns;
    UniquePtrVector<PatternElement> _patternElems;
    UniquePtrVector<EntityPattern> _patternEntity;
    UniquePtrVector<Statement> _statements;
    UniquePtrVector<SubStatement> _subStatements;
    UniquePtrVector<Projection> _projections;
    UniquePtrVector<QueryCommand> _queries;
    UniquePtrVector<MapLiteral> _mapLiterals;
    UniquePtrVector<StatementContainer> _statementContainers;
    std::vector<AnalysisData> _data;

    std::unordered_map<std::uintptr_t, SourceLocation> _locationMap;

    StatementContainer* _currentStatements {nullptr};
    DeclContainer _declContainer;

    bool _debugLocation {true};

    StatementContainer* newStatementContainer();
};

}
