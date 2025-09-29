#include "QueryAnalyzer.h"

#include <cstdint>
#include <optional>
#include <range/v3/view.hpp>
#include <range/v3/action/sort.hpp>
#include <range/v3/algorithm/adjacent_find.hpp>

#include "views/GraphView.h"
#include "reader/GraphReader.h"
#include "AnalyzeException.h"
#include "Profiler.h"
#include "metadata/PropertyType.h"
#include "reader/GraphReader.h"
#include "DeclContext.h"
#include "Expr.h"
#include "MatchTarget.h"
#include "MatchTargets.h"
#include "PathPattern.h"
#include "QueryCommand.h"
#include "ReturnField.h"
#include "ASTContext.h"
#include "ReturnProjection.h"
#include "CreateTarget.h"
#include "VarDecl.h"
#include "InjectedIDs.h"
#include "BioAssert.h"

using namespace db;
namespace rv = ranges::views;

namespace {

void returnAllVariables(MatchCommand* cmd) {
    const auto& targets = cmd->getMatchTargets()->targets();
    for (const MatchTarget* target : targets) {
        const PathPattern* pattern = target->getPattern();
        for (EntityPattern* entityPattern : pattern->elements()) {
            if (VarExpr* var = entityPattern->getVar()) {
                if (VarDecl* decl = var->getDecl()) {
                    decl->setUsed(true);
                }
            }
        }
    }
}

template <typename T>
const T* enforceType(const Expr* exp) {
    const T* castedExp = dynamic_cast<const T*>(exp);
    return castedExp;
}

}

QueryAnalyzer::QueryAnalyzer(const GraphView& view, ASTContext* ctxt)
    : _view(view),
    _ctxt(ctxt)
{
}

QueryAnalyzer::~QueryAnalyzer() {
}

void QueryAnalyzer::analyze(QueryCommand* cmd) {
    Profile profile {"QueryAnalyzer::analyze"};

    switch (cmd->getKind()) {
        case QueryCommand::Kind::MATCH_COMMAND:
            analyzeMatch(static_cast<MatchCommand*>(cmd));
        break;

        case QueryCommand::Kind::CREATE_COMMAND:
            analyzeCreate(static_cast<CreateCommand*>(cmd));
        break;

        case QueryCommand::Kind::CREATE_GRAPH_COMMAND:
            analyzeCreateGraph(static_cast<CreateGraphCommand*>(cmd));
        break;

        case QueryCommand::Kind::LOAD_GRAPH_COMMAND:
            analyzeLoadGraph(static_cast<LoadGraphCommand*>(cmd));
        break;

        case QueryCommand::Kind::EXPLAIN_COMMAND:
            analyzeExplain(static_cast<ExplainCommand*>(cmd));
        break;

        case QueryCommand::Kind::S3TRANSFER_COMMAND:
            analyzeS3Transfer(static_cast<S3TransferCommand*>(cmd));
        break;

        case QueryCommand::Kind::LIST_GRAPH_COMMAND:
        case QueryCommand::Kind::HISTORY_COMMAND:
        case QueryCommand::Kind::CHANGE_COMMAND:
        case QueryCommand::Kind::COMMIT_COMMAND:
        case QueryCommand::Kind::CALL_COMMAND:
        case QueryCommand::Kind::S3CONNECT_COMMAND:
            return;
        break;

        default:
            return;
        break;
    }
}

void QueryAnalyzer::analyzeExplain(ExplainCommand* cmd) {
    analyze(cmd->getQuery());
}

void QueryAnalyzer::analyzeCreateGraph(CreateGraphCommand* cmd) {
    const std::string& name = cmd->getName();
    if (name.empty()) {
        throw AnalyzeException("No graph name provided.");
    }

    // Check that the graph name is only [A-Z0-9_]+
    for (char c : name) {
        if (!(isalnum(c) || c == '_')) [[unlikely]] {
            throw AnalyzeException(
                fmt::format("Graph name must only contain alphanumeric characters "
                            "or '_': character '{}' not allowed.", c));
        }
    }
}

// Checks if a match target contains multiple variables
void QueryAnalyzer::ensureMatchVarsUnique(const MatchTarget* target) {
    using PathElements = std::vector<EntityPattern*>;

    const PathPattern* ptn = target->getPattern();

    const PathElements& elements = ptn->elements();

    std::vector<std::string> varNames;
    for (auto&& e : elements) {
        if (e && e->getVar()) {
            varNames.push_back(e->getVar()->getName());
        }
    }
    std::ranges::sort(varNames);

    // Adjacent find returns an iterator to the first occurences of two consecutive
    // identical elements. Since the vector is sorted, identical variable names are
    // contiguous, and thus if adjacent_find returns no such two consecutive
    // identical elements, all variables must be unique.
    const auto duplicateVarIt = ranges::adjacent_find(varNames);
    const bool hasDuplicate = duplicateVarIt != std::end(varNames);

    if (hasDuplicate) [[unlikely]] {
        const std::string duplicateVarName = *duplicateVarIt;
        throw AnalyzeException(fmt::format(
            "Variable {} occurs multiple times in MATCH query",
            duplicateVarName));
    }
}

void QueryAnalyzer::analyzeMatch(MatchCommand* cmd) {
    bool isCreate {false}; // Flag to distinguish create and match commands
    ReturnProjection* proj = cmd->getProjection();
    if (!proj) {
        throw AnalyzeException("Could not get return projection.");
    }

    // match targets
    DeclContext* declContext = cmd->getDeclContext();
    const auto& matchTargets = cmd->getMatchTargets()->targets();
    for (const MatchTarget* target : matchTargets) {
        const PathPattern* pattern = target->getPattern();
        if (pattern == nullptr) {
            throw AnalyzeException("Match target path pattern not found.");
        }

        ensureMatchVarsUnique(target);
        const auto& elements = pattern->elements();

        EntityPattern* entityPattern = elements[0];

        // The InjectID code can be moved into analyzeEntityPattern
        // when we support injecting IDs at any node
        if (elements[0]->getInjectedIDs()) {
            const auto& injectedNodes = entityPattern->getInjectedIDs()->getIDs();
            for (const auto& node : injectedNodes) {
                if (!_view.read().graphHasNode(node)) {
                    throw AnalyzeException("Could Not Find Injected Node In Graph:\""
                                           + std::to_string(node.getValue()) + "\"");
                }
            }
        }

        analyzeEntityPattern(declContext, entityPattern, isCreate);

        if (elements.size() >= 2) {
            bioassert(elements.size() >= 3);
            for (auto pair : elements | rv::drop(1) | rv::chunk(2)) {
                EntityPattern* edge = pair[0];
                EntityPattern* target = pair[1];

                analyzeEntityPattern(declContext, edge, isCreate);

                if (target->getInjectedIDs()) {
                    throw AnalyzeException("Injecting Nodes In Non-Primary Entity Not Supported,"
                                           "For Entity:\""
                                           + target->getVar()->getName() + "\"");
                }
                analyzeEntityPattern(declContext, target, isCreate);
            }
        }
    }

    // Return fields
    const auto& returnFields = proj->returnFields();
    bool returnAll = false;
    for (ReturnField* field : returnFields) {
        if (field->isAll()) {
            returnAll = true;
            continue;
        } else {
            const auto& name = field->getName();
            // Check that the variable exists in the declContext
            VarDecl* decl = declContext->getDecl(name);
            if (!decl) {
                throw AnalyzeException("Could not get declation context.");
            }

            decl->setUsed(true);
            field->setDecl(decl);
            const auto& memberName = field->getMemberName();

            const auto& propTypeMap = _view.metadata().propTypes();
            // If returning a member, get (and check) its type
            if (!memberName.empty()) {
                const auto propTypeRes = propTypeMap.get(memberName);
                if (!propTypeRes) {
                    throw AnalyzeException(
                        "Property type not found for property member \""
                       + field->getMemberName() + "\"");
                }

                const auto propType = propTypeRes.value();
                field->setMemberType(propType);
            }
        }
    }

    // At this point: a declaration has been created for each variable
    // in each pattern and return fields are connected to the var decl
    if (returnAll) {
        returnAllVariables(cmd);
    }
}

void QueryAnalyzer::analyzeCreate(CreateCommand* cmd) {
    bool isCreate {true}; // Flag to distinguish create and match commands

    DeclContext* declContext = cmd->getDeclContext();
    const auto& targets = cmd->createTargets();
    for (const CreateTarget* target : targets) {
        const PathPattern* pattern = target->getPattern();
        const auto& elements = pattern->elements();

        EntityPattern* entityPattern = elements[0];
        if (elements.empty()) {
            throw AnalyzeException("Entity pattern has no elements.");
        }

        // When creating a node, you should not be able to inject IDs
        if (elements.size() == 1) {
            if (elements[0]->getInjectedIDs()) {
                throw AnalyzeException("Cannot create a node with a specific ID");
            }
        }

        analyzeEntityPattern(declContext, entityPattern, isCreate);

        if (elements.size() >= 2) {
            bioassert(elements.size() >= 3);
            for (auto pair : elements | rv::drop(1) | rv::chunk(2)) {
                EntityPattern* edge = pair[0];
                EntityPattern* target = pair[1];

                analyzeEntityPattern(declContext, edge, isCreate);

                analyzeEntityPattern(declContext, target, isCreate);
            }
        }
    }
}

void QueryAnalyzer::typeCheckBinExprConstr(const PropertyType lhs,
                                           const ExprConst* rhs) {
    // NOTE: Directly accessing struct member
    const ValueType lhsType = lhs._valueType;
    const ValueType rhsType = rhs->getType();

    if (lhsType == rhsType) {
        return;
    } else if (lhsType == ValueType::UInt64 && rhsType == ValueType::Int64) {
        const auto rhsI64 = static_cast<const Int64ExprConst*>(rhs);
        const int64_t rhsValue = rhsI64->getVal();
        // Ensure the Int64 is not out of UInt64 range
        if (rhsValue >= 0) {
            return;
        }
    }
    throw AnalyzeException("Type check failed");
}

void QueryAnalyzer::analyzeBinExprConstraint(const BinExpr* binExpr,
                                             bool isCreate) {
    const Expr* constExpr = binExpr->getRightExpr();

    switch (binExpr->getOpType()) {
        case BinExpr::OP_EQUAL:
            // No special handling requrired
        break;

        case BinExpr::OP_STR_APPROX:
            // Ensure the constant value is a string
            if (!enforceType<StringExprConst>(constExpr)) [[unlikely]] {
                throw AnalyzeException("Operator '~=' must be "
                                       "used with values of type 'String'.");
            }
        break;

        default:
            throw AnalyzeException("Unsupported operator");
        break;
    }
    
    // Assumes that variable is left operand, constant is right operand
    const VarExpr* lhsExpr =
        static_cast<VarExpr*>(binExpr->getLeftExpr());
    const ExprConst* rhsExpr =
        static_cast<ExprConst*>(binExpr->getRightExpr());

    // Query graph for name and type of variable
    const std::string& lhsName = lhsExpr->getName();
    const GraphReader reader = _view.read();
    const auto lhsPropTypeOpt = reader.getMetadata().propTypes().get(lhsName);

    // Property type does not exist
    if (lhsPropTypeOpt == std::nullopt) {
        // If this is a match query: error
        if (!isCreate) [[unlikely]] {
            throw AnalyzeException("Variable '" + lhsName +
                                   "' does not exist.");
        } else { // If a create query: no need to type check
            return;
        }
    }

    // If property type exists: get types and type check
    const PropertyType lhsPropType = lhsPropTypeOpt.value();

    try {
        typeCheckBinExprConstr(lhsPropType, rhsExpr);
    }
    catch (AnalyzeException& _) {
        const ValueType lhsType = lhsPropType._valueType;
        const ValueType rhsType = rhsExpr->getType();
        const std::string varTypeName = std::string(ValueTypeName::value(lhsType));
        const std::string exprTypeName = std::string(ValueTypeName::value(rhsType));
        const std::string verb = isCreate ? "assigned" : "compared to";
        throw AnalyzeException("Variable '" + lhsName + "' of type " + varTypeName +
                               " cannot be " + verb + " value of type " + exprTypeName);
    }
}

void QueryAnalyzer::analyzeEntityPattern(DeclContext* declContext, EntityPattern* entity,
                                         bool isCreate) {
    VarExpr* var = entity->getVar();
    // Handle the case where the entity is unnamed edge (i.e. (n)--(m) or
    // (n)-[:LABEL]-(m)) or node (i.e. (:LABEL))
    if (!var) {
        var = VarExpr::create(_ctxt, createVarName());
        entity->setVar(var);
    }

    uint64_t idToSet = entity->getEntityID();

    // If attempting to inject IDs in a CREATE, ensure they are all valid
    // NOTE: Attempts to create a node whilst injecting IDs is prevented
    // in @ref analyzeCreate
    if (isCreate && entity->getInjectedIDs()) {
        auto& injectedIDs = entity->getInjectedIDs()->getIDs();
        // Since we already check for creating a single node with injected nodes, if we
        // are here, we must be creating an edge where at least one of its nodes is an
        // injected ID. We only can support this if a single ID is provided, otherwise it
        // requires creating multiple edges from/to each of the injected nodes.
        if (injectedIDs.size() != 1) {
            throw AnalyzeException(
                "Edges may only be created between nodes with at most one specified ID");
        }
        // We have a single injected node: set this VarDecl to have the injected ID
        idToSet = injectedIDs.at(0).getValue();
        if (!_view.read().graphHasNode(idToSet)) {
            throw AnalyzeException("No such node with ID: \""
                                   + std::to_string(idToSet) + "\"");
        }
    }

    // Create the variable declaration in the scope of the command
    VarDecl* decl = VarDecl::create(_ctxt,
                                    declContext,
                                    var->getName(),
                                    entity->getKind(),
                                    idToSet);

    if (!decl) {
        // decl already exists from prev targets
        decl = declContext->getDecl(var->getName());
        decl->setUsed(true);
        if (decl->getEntityID() != entity->getEntityID()) {
            throw AnalyzeException(
                fmt::format("Variable {} has conflicting declarations.", var->getName()));
        }
    }

    auto* exprConstraint = entity->getExprConstraint();
    // If there are no constraints, no need to type check
    if (exprConstraint == nullptr) {
        var->setDecl(decl);
        return;
    }

    // Otherwise, verify all constraints are valid
    const auto binExprs = exprConstraint->getExpressions();
    for (const BinExpr* binExpr : binExprs) {
        analyzeBinExprConstraint(binExpr, isCreate);
    }

    var->setDecl(decl);
}

std::string QueryAnalyzer::createVarName() {
    return std::to_string(_nextNewVarID++);
}

void QueryAnalyzer::analyzeLoadGraph(LoadGraphCommand* cmd) {
    const std::string& graphName = cmd->getGraphName();
    if (graphName.empty()) {
        throw AnalyzeException("No graph name provided.");
    }

    // Check that the graph name is only [A-Z0-9_]+
    for (char c : graphName) {
        if (!(isalnum(c) || c == '_')) [[unlikely]] {
            throw AnalyzeException(
                fmt::format("Graph name must only contain alphanumeric characters "
                            "or '_': character '{}' not allowed.",
                            c));
        }
    }
}

void parseS3Url(std::string_view s3URL, std::string_view& bucket, std::string_view& prefix, std::string_view& fileName) {
    if (s3URL.substr(0, 5) != "s3://") {
        throw AnalyzeException(fmt::format("Invalid S3 URL: {}", s3URL));
    }
    s3URL.remove_prefix(5);
    const auto bucketEnd = s3URL.find('/');

    if (bucketEnd == std::string_view::npos) {
        throw AnalyzeException(fmt::format("S3 Bucket Not Found: {}", s3URL));
    }

    bucket = s3URL.substr(0, bucketEnd);
    s3URL.remove_prefix(bucketEnd + 1);

    if (s3URL.empty()) {
        throw AnalyzeException(fmt::format("S3 Prefix/Folder not found: {}", s3URL));
    }

    if (s3URL.back() != '/') {
        // S3 'file' resource
        fileName = s3URL;
        return;
    }

    // S3 Directory Resource
    prefix = s3URL;
}

void QueryAnalyzer::analyzeS3Transfer(S3TransferCommand* cmd) {
    parseS3Url(cmd->getS3URL(), cmd->getBucket(), cmd->getPrefix(), cmd->getFile());
    // For now we let user write/read to/from anywhere on the system
}
