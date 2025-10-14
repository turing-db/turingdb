#include "WriteStmtAnalyzer.h"

#include <spdlog/fmt/bundled/format.h>

#include "AnalyzeException.h"
#include "CypherAST.h"
#include "DiagnosticsManager.h"

#include "EdgePattern.h"
#include "ExprAnalyzer.h"
#include "NodePattern.h"
#include "Pattern.h"
#include "PatternElement.h"
#include "Symbol.h"
#include "decl/DeclContext.h"
#include "decl/EvaluatedType.h"
#include "decl/PatternData.h"
#include "decl/VarDecl.h"
#include "expr/Expr.h"
#include "expr/Literal.h"
#include "stmt/Stmt.h"
#include "stmt/CreateStmt.h"

using namespace db::v2;

WriteStmtAnalyzer::WriteStmtAnalyzer(CypherAST* ast, GraphView graphView)
    : _ast(ast),
    _graphView(graphView),
    _graphMetadata(_graphView.metadata())
{
}

WriteStmtAnalyzer::~WriteStmtAnalyzer() {
}

void WriteStmtAnalyzer::analyze(const Stmt* stmt) {
    switch (stmt->getKind()) {
        case Stmt::Kind::CREATE:
            analyze(static_cast<const CreateStmt*>(stmt));
            break;

        default:
            throwError(fmt::format("Unsupported write statement type: {}", (uint64_t)stmt->getKind()), stmt);
            break;
    }
}

void WriteStmtAnalyzer::analyze(const CreateStmt* createStmt) {
    if (createStmt->getPattern()) {
        analyze(createStmt->getPattern());
    }
}

void WriteStmtAnalyzer::analyze(const Pattern* pattern) {
    for (const PatternElement* element : pattern->elements()) {
        analyze(element);
    }
}

void WriteStmtAnalyzer::analyze(const PatternElement* element) {
    const auto& entities = element->getEntities();

    for (EntityPattern* entity : entities) {
        if (NodePattern* node = dynamic_cast<NodePattern*>(entity)) {
            analyze(node);
        } else if (EdgePattern* edge = dynamic_cast<EdgePattern*>(entity)) {
            analyze(edge);
        } else {
            throwError(fmt::format("Unsupported pattern entity type"), entity);
        }
    }
}

void WriteStmtAnalyzer::analyze(NodePattern* nodePattern) {
    VarDecl* decl = nullptr;

    if (Symbol* symbol = nodePattern->getSymbol()) {
        decl = _ctxt->getDecl(symbol->getName());
        if (decl) {
            if (_toBeCreated.contains(decl)) {
                // Already defined in the write statement
                throwError("Variable already defined", nodePattern);
            }

            // Node is an input to the write query
            if (decl->getType() != EvaluatedType::NodePattern) {
                throwError(fmt::format("Type mismatch. Expected NodePattern but is {} instead ",
                                       EvaluatedTypeName::value(decl->getType())),
                           nodePattern);
            }

            if (nodePattern->getData() != nullptr || !nodePattern->labels().empty()) {
                throwError("Input nodes to write statements cannot have constraints", nodePattern);
            }

            nodePattern->setDecl(decl);
            return;
        }
        decl = _ctxt->getOrCreateNamedVariable(_ast, EvaluatedType::NodePattern, symbol->getName());
    } else {
        decl = _ctxt->createUnnamedVariable(_ast, EvaluatedType::NodePattern);
    }

    _toBeCreated.insert(decl);

    NodePatternData* data = NodePatternData::create(_ast);

    nodePattern->setDecl(decl);
    nodePattern->setData(data);

    const auto& labels = nodePattern->labels();
    if (labels.empty()) {
        throwError("Node pattern must have at least one label", nodePattern);
    }

    for (const Symbol* label : labels) {
        data->addLabelConstraint(label->getName());
    }

    const MapLiteral* properties = nodePattern->getProperties();
    if (properties) {
        const PropertyTypeMap& propTypeMap = _graphMetadata.propTypes();

        for (const auto& [propName, expr] : *properties) {
            _exprAnalyzer->analyze(expr);

            const std::optional<PropertyType> propType = propTypeMap.get(propName->getName());
            if (propType) {
                // Property type already exists
                if (!ExprAnalyzer::propTypeCompatible(propType->_valueType, expr->getType())) {
                    throwError(fmt::format("Cannot evaluate node property: types '{}' and '{}' are incompatible",
                                           ValueTypeName::value(propType->_valueType),
                                           EvaluatedTypeName::value(expr->getType())),
                               nodePattern);
                }
                data->addExprConstraint(propName->getName(), propType->_valueType, expr);
            } else {
                // Property type needs to be created
                const ValueType valueType = evaluatedToValueType(expr->getType());
                if (valueType == ValueType::Invalid) {
                    throwError(fmt::format("Cannot evaluate node property: unsupported type '{}'", EvaluatedTypeName::value(expr->getType())),
                               expr);
                }
                data->addExprConstraint(propName->getName(), valueType, expr);
            }
        }
    }
}

void WriteStmtAnalyzer::analyze(EdgePattern* edgePattern) {
    VarDecl* decl = nullptr;

    if (Symbol* symbol = edgePattern->getSymbol()) {
        decl = _ctxt->getDecl(symbol->getName());
        if (decl) {
            throwError("Edges cannot be inputs to write queries", edgePattern);
        }

        decl = _ctxt->getOrCreateNamedVariable(_ast, EvaluatedType::EdgePattern, symbol->getName());
    } else {
        decl = _ctxt->createUnnamedVariable(_ast, EvaluatedType::EdgePattern);
    }

    EdgePatternData* data = EdgePatternData::create(_ast);

    edgePattern->setDecl(decl);
    edgePattern->setData(data);

    const auto& types = edgePattern->types();
    if (types.empty()) {
        throwError("Edge pattern must have at least one edge type", edgePattern);
    }

    if (types.size() > 1) {
        throwError("An edge cannot have more than one edge type", edgePattern);
    }

    data->addEdgeTypeConstraint(types.front()->getName());

    const MapLiteral* properties = edgePattern->getProperties();
    if (properties) {
        const PropertyTypeMap& propTypeMap = _graphMetadata.propTypes();

        for (const auto& [propName, expr] : *properties) {
            _exprAnalyzer->analyze(expr);

            const std::optional<PropertyType> propType = propTypeMap.get(propName->getName());
            if (propType) {
                // Property type already exists
                if (!ExprAnalyzer::propTypeCompatible(propType->_valueType, expr->getType())) {
                    throwError(fmt::format("Cannot evaluate node property: types '{}' and '{}' are incompatible",
                                           ValueTypeName::value(propType->_valueType),
                                           EvaluatedTypeName::value(expr->getType())),
                               edgePattern);
                }
                data->addExprConstraint(propName->getName(), propType->_valueType, expr);
            } else {
                // Property type needs to be created
                const ValueType valueType = evaluatedToValueType(expr->getType());
                if (valueType == ValueType::Invalid) {
                    throwError(fmt::format("Cannot evaluate edge property: unsupported type '{}'", EvaluatedTypeName::value(expr->getType())),
                               expr);
                }
                data->addExprConstraint(propName->getName(), valueType, expr);
            }
        }
    }
}

void WriteStmtAnalyzer::throwError(std::string_view msg, const void* obj) const {
    throw AnalyzeException(_ast->getDiagnosticsManager()->createErrorString(msg, obj));
}

db::ValueType WriteStmtAnalyzer::evaluatedToValueType(EvaluatedType type) {
    switch (type) {
        case EvaluatedType::Bool:
            return ValueType::Bool;
        case EvaluatedType::Char:
        case EvaluatedType::String:
            return ValueType::String;
        case EvaluatedType::Double:
            return ValueType::Double;
        case EvaluatedType::Integer:
            return ValueType::Int64;
        case EvaluatedType::Null:
        case EvaluatedType::NodePattern:
        case EvaluatedType::EdgePattern:
        case EvaluatedType::List:
        case EvaluatedType::Map:
        case EvaluatedType::Invalid:
        case EvaluatedType::_SIZE:
            return ValueType::Invalid;
    }

    return ValueType::Invalid;
}

