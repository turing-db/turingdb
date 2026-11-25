#include "CypherASTDumper.h"

#include "CypherAST.h"
#include "SinglePartQuery.h"
#include "UnionQuery.h"
#include "ConditionalQuery.h"
#include "stmt/WithStmt.h"
#include "ChangeQuery.h"
#include "CommitQuery.h"
#include "MergeDataPartsQuery.h"
#include "stmt/StmtContainer.h"
#include "stmt/MatchStmt.h"
#include "stmt/ReturnStmt.h"
#include "stmt/CallStmt.h"
#include "stmt/CallSubqueryStmt.h"
#include "stmt/NextSegmentStmt.h"
#include "stmt/DeleteStmt.h"
#include "stmt/SetStmt.h"
#include "stmt/Limit.h"
#include "stmt/Skip.h"
#include "stmt/CreateStmt.h"
#include "stmt/MergeStmt.h"
#include "stmt/ShortestPathStmt.h"
#include "Projection.h"
#include "Pattern.h"
#include "WhereClause.h"
#include "Symbol.h"
#include "decl/VarDecl.h"
#include "LoadGraphQuery.h"
#include "ListGraphQuery.h"
#include "ListAvailableGraphsQuery.h"
#include "CreateGraphQuery.h"
#include "LoadGMLQuery.h"
#include "LoadParquetQuery.h"
#include "LoadJsonlQuery.h"
#include "S3ConnectQuery.h"
#include "S3TransferQuery.h"
#include "YieldClause.h"
#include "YieldItems.h"
#include "stmt/SetItem.h"
#include "stmt/UnwindStmt.h"

#include "expr/All.h"

#include "Overloaded.h"

using namespace db;

CypherASTDumper::CypherASTDumper(const CypherAST* ast)
    : _ast(ast)
{
}

void CypherASTDumper::dump(std::ostream& out) {
    out << "---\n";
    out << "config:\n";
    out << "  layout: hierarchical\n";

    out << "---\n";
    out << "erDiagram\n";

    for (const auto& query : _ast->queries()) {
        switch (query->getKind()) {
            case QueryCommand::Kind::SINGLE_PART_QUERY:
            case QueryCommand::Kind::UNION_QUERY:
            case QueryCommand::Kind::CONDITIONAL_QUERY:
                dump(out, static_cast<const RegularQuery*>(query));
            break;

            case QueryCommand::Kind::LOAD_GRAPH_QUERY:
                dump(out, static_cast<const LoadGraphQuery*>(query));
            break;

            case QueryCommand::Kind::LOAD_JSONL_QUERY:
                dump(out, static_cast<const LoadJsonlQuery*>(query));
            break;

            case QueryCommand::Kind::CHANGE_QUERY:
                dump(out, static_cast<const ChangeQuery*>(query));
            break;

            case QueryCommand::Kind::COMMIT_QUERY:
                dump(out, static_cast<const CommitQuery*>(query));
            break;

            case QueryCommand::Kind::LIST_GRAPH_QUERY:
                dump(out, static_cast<const ListGraphQuery*>(query));
            break;

            case QueryCommand::Kind::LIST_AVAILABLE_GRAPHS_QUERY:
                dump(out, static_cast<const ListAvailableGraphsQuery*>(query));
            break;

            case QueryCommand::Kind::CREATE_GRAPH_QUERY:
                dump(out, static_cast<const CreateGraphQuery*>(query));
            break;

            case QueryCommand::Kind::LOAD_GML_QUERY:
                dump(out, static_cast<const LoadGMLQuery*>(query));
            break;

            case QueryCommand::Kind::LOAD_PARQUET_QUERY:
                dump(out, static_cast<const LoadParquetQuery*>(query));
            break;

            case QueryCommand::Kind::S3_CONNECT_QUERY:
                dump(out, static_cast<const S3ConnectQuery*>(query));
            break;

            case QueryCommand::Kind::S3_TRANSFER_QUERY:
                dump(out, static_cast<const S3TransferQuery*>(query));
            break;

            case QueryCommand::Kind::SHOW_PROCEDURES_QUERY:
                out << "    script ||--o{ SHOW_PROCEDURES : \"\"\n";
            break;

            case QueryCommand::Kind::CREATE_VECTOR_INDEX_QUERY:
                out << "    script ||--o{ CREATE_VECTOR_INDEX : \"\"\n";
            break;

            case QueryCommand::Kind::LOAD_VECTOR_QUERY:
                out << "    script ||--o{ LOAD_VECTOR : \"\"\n";
            break;

            case QueryCommand::Kind::LOAD_EMBEDDING_QUERY:
                out << "    script ||--o{ LOAD_EMBEDDING : \"\"\n";
            break;

            case QueryCommand::Kind::DELETE_VECTOR_INDEX_QUERY:
                out << "    script ||--o{ DELETE_VECTOR_INDEX : \"\"\n";
            break;

            case QueryCommand::Kind::SHOW_VECTOR_INDEXES_QUERY:
                out << "    script ||--o{ SHOW_VECTOR_INDEXES : \"\"\n";

            case QueryCommand::Kind::LOAD_COMMIT_QUERY:
                out << "    script ||--o{ LOAD_COMMIT : \"\"\n";
            break;

            case QueryCommand::Kind::INSTALL_EXTENSION_QUERY:
                out << "    script ||--o{ INSTALL_EXTENSION : \"\"\n";
            break;

            case QueryCommand::Kind::SHOW_EXTENSIONS_QUERY:
                out << "    script ||--o{ SHOW_EXTENSIONS : \"\"\n";
            break;

            case QueryCommand::Kind::CREATE_NODE_PROPERTY_INDEX_QUERY:
                out << "    script ||--o{ CREATE_NODE_PROPERTY_INDEX : \"\"\n";
            break;

            case QueryCommand::Kind::CREATE_EDGE_PROPERTY_INDEX_QUERY:
                out << "    script ||--o{ CREATE_EDGE_PROPERTY_INDEX : \"\"\n";
            break;

            case QueryCommand::Kind::DROP_INDEX_QUERY:
                out << "    script ||--o{ DROP_INDEX : \"\"\n";
            break;

            case QueryCommand::Kind::MERGE_DATAPARTS_QUERY:
                dump(out, static_cast<const MergeDataPartsQuery*>(query));
            break;

        }
    }
}

void CypherASTDumper::dump(std::ostream& out, const RegularQuery* query) {
    out << "    script ||--o{ _" << std::hex << query << " : \"\"\n";

    dumpRegularQuery(out, query);
}

void CypherASTDumper::dumpRegularQuery(std::ostream& out, const RegularQuery* query) {
    switch (query->getKind()) {
        case QueryCommand::Kind::UNION_QUERY:
            dumpUnion(out, static_cast<const UnionQuery*>(query));
        break;

        case QueryCommand::Kind::CONDITIONAL_QUERY:
            dumpConditional(out, static_cast<const ConditionalQuery*>(query));
        break;

        default:
            dumpQueryBody(out, static_cast<const SinglePartQuery*>(query));
        break;
    }
}

void CypherASTDumper::dumpUnion(std::ostream& out, const UnionQuery* query) {
    out << "    _" << std::hex << query << " {\n";
    out << "        ASTType UnionQuery\n";
    out << "    }\n";

    // The first branch is joined to nothing, so the operator labelling an edge is the
    // one written ahead of the branch it leads to
    bool isFirstBranch = true;
    for (const UnionQuery::Branch& branch : query->branches()) {
        const char* const label = isFirstBranch ? "" : (branch._all ? "UNION ALL" : "UNION");
        isFirstBranch = false;

        out << "    _" << std::hex << query << " ||--o{ _" << std::hex << branch._query
            << " : \"" << label << "\"\n";

        dumpRegularQuery(out, branch._query);
    }
}

void CypherASTDumper::dumpConditional(std::ostream& out, const ConditionalQuery* query) {
    out << "    _" << std::hex << query << " {\n";
    out << "        ASTType ConditionalQuery\n";
    out << "    }\n";

    for (const ConditionalQuery::Branch& branch : query->branches()) {
        const char* const label = branch._condition ? "WHEN" : "ELSE";

        out << "    _" << std::hex << query << " ||--o{ _" << std::hex << branch._query
            << " : \"" << label << "\"\n";

        dumpRegularQuery(out, branch._query);
    }
}

void CypherASTDumper::dumpQueryBody(std::ostream& out, const SinglePartQuery* query) {
    out << "    _" << std::hex << query << " {\n";
    out << "        ASTType SinglePartQuery\n";
    out << "    }\n";

    auto dumpContainer = [&](const StmtContainer* container) {
        for (const Stmt* stmt : container->stmts()) {
            switch (stmt->getKind()) {
                case Stmt::Kind::MATCH: {
                    const MatchStmt* matchStmt = static_cast<const MatchStmt*>(stmt);
                    out << "    _" << std::hex << query << " ||--o{ _" << std::hex << matchStmt << " : \"\"\n";
                    dump(out, matchStmt);
                }
                break;

                case Stmt::Kind::CREATE: {
                    const CreateStmt* createStmt = static_cast<const CreateStmt*>(stmt);
                    out << "    _" << std::hex << query << " ||--o{ _" << std::hex << createStmt << " : \"\"\n";
                    dump(out, createStmt);
                }
                break;

                case Stmt::Kind::MERGE: {
                    const MergeStmt* mergeStmt = static_cast<const MergeStmt*>(stmt);
                    out << "    _" << std::hex << query << " ||--o{ _" << std::hex << mergeStmt << " : \"\"\n";
                    dump(out, mergeStmt);
                }
                break;

                case Stmt::Kind::CALL: {
                    const CallStmt* callStmt = static_cast<const CallStmt*>(stmt);
                    out << "    _" << std::hex << query << " ||--o{ _" << std::hex << callStmt << " : \"\"\n";
                    dump(out, callStmt);
                }
                break;

                case Stmt::Kind::DELETE: {
                    const DeleteStmt* deleteStmt = static_cast<const DeleteStmt*>(stmt);
                    out << "    _" << std::hex << query << " ||--o{ _" << std::hex << deleteStmt << " : \"\"\n";
                    dump(out, deleteStmt);
                }
                break;

                case Stmt::Kind::RETURN: {
                    const ReturnStmt* returnStmt = static_cast<const ReturnStmt*>(stmt);
                    out << "    _" << std::hex << query << " ||--o{ _" << std::hex << returnStmt << " : \"\"\n";
                    dump(out, returnStmt);
                }
                break;

                case Stmt::Kind::SET: {
                    const SetStmt* setStmt = static_cast<const SetStmt*>(stmt);
                    out << "    _" << std::hex << query << " ||--o{ _" << std::hex << setStmt << " : \"\"\n";
                    dump(out, setStmt);
                }
                break;

                case Stmt::Kind::REMOVE: {
                    out << "    _" << std::hex << stmt << " {\n";
                    out << "        ASTType RemoveStmt\n";
                    out << "    }\n";
                    out << "    _" << std::hex << query << " ||--o{ _" << std::hex << stmt << " : \"\"\n";
                }
                break;

                case Stmt::Kind::SHORTESTPATH: {
                    const ShortestPathStmt* shortestPathStmt = static_cast<const ShortestPathStmt*>(stmt);
                    out << "    _" << std::hex << query << " ||--o{ _" << std::hex << shortestPathStmt << " : \"\"\n";
                    dump(out, shortestPathStmt);
                }
                break;

                case Stmt::Kind::LOAD_CSV: {
                    out << "    _" << std::hex << stmt << " {\n";
                    out << "        ASTType LoadCSVStmt\n";
                    out << "    }\n";
                    out << "    _" << std::hex << query << " ||--o{ _" << std::hex << stmt << " : \"\"\n";
                }
                break;

                case Stmt::Kind::VECTOR_SEARCH: {
                    out << "    _" << std::hex << query << " ||--o{ _" << std::hex << stmt << " : \"\"\n";
                    out << "    _" << std::hex << stmt << " {\n";
                    out << "        ASTType VECTOR_SEARCH\n";
                    out << "    }\n";
                }
                break;

                case Stmt::Kind::WITH: {
                    const WithStmt* withStmt = static_cast<const WithStmt*>(stmt);
                    out << "    _" << std::hex << query << " ||--o{ _" << std::hex << withStmt << " : \"\"\n";
                    dump(out, withStmt);
                }
                break;

                case Stmt::Kind::CALL_SUBQUERY: {
                    const CallSubqueryStmt* subquery = static_cast<const CallSubqueryStmt*>(stmt);
                    out << "    _" << std::hex << query << " ||--o{ _" << std::hex << subquery << " : \"\"\n";
                    out << "    _" << std::hex << subquery << " {\n";
                    out << "        ASTType CALL_SUBQUERY\n";
                    out << "    }\n";
                    dump(out, subquery->getBody());
                }
                break;

                case Stmt::Kind::NEXT_SEGMENT: {
                    const NextSegmentStmt* segment = static_cast<const NextSegmentStmt*>(stmt);
                    out << "    _" << std::hex << query << " ||--o{ _" << std::hex << segment << " : \"\"\n";
                    out << "    _" << std::hex << segment << " {\n";
                    out << "        ASTType NEXT_SEGMENT\n";
                    out << "    }\n";
                    dump(out, segment->getBody()->getBody());
                }
                break;

                case Stmt::Kind::UNWIND: {
                    const UnwindStmt* unwStmt = static_cast<const UnwindStmt*>(stmt);
                    const Expr* arg = unwStmt->arg();
                    const Symbol* sym = unwStmt->symbol();

                    out << "    _" << std::hex << query << " ||--o{ _" << std::hex << unwStmt << " : \"\"\n";
                    out << "    _" << std::hex << unwStmt << " {\n";
                    out << "        ASTType UNWIND\n";
                    out << "        Symbol " << sym->getName() << "\n";
                    out << "    }\n";
                    out << "    _" << std::hex << unwStmt << " ||--o{ _" << std::hex << arg << " : \"\"\n";
                    dump(out, arg);
                } break;
            }
        }
    };

    if (const StmtContainer* stmts = query->getStmts()) {
        dumpContainer(stmts);
    }

    if (const ReturnStmt* retSt = query->getReturnStmt()) {
        out << "    _" << std::hex << query << " ||--o{ _" << std::hex << retSt << " : \"\"\n";
        dump(out, retSt);
    }
}

void CypherASTDumper::dump(std::ostream& out, const LoadGraphQuery* query) {
    out << "    script ||--o{ _" << std::hex << query << " : \"\"\n";
    out << "    _" << std::hex << query << " {\n";
    out << "        ASTType LoadGraphQuery\n";
    out << "    }\n";
}

void CypherASTDumper::dump(std::ostream& out, const LoadJsonlQuery* query) {
    out << "    script ||--o{ _" << std::hex << query << " : \"\"\n";
    out << "    _" << std::hex << query << " {\n";
    out << "        ASTType LoadJsonlQuery\n";
    out << "    }\n";
}

void CypherASTDumper::dump(std::ostream& out, const ChangeQuery* query) {
    out << "    script ||--o{ _" << std::hex << query << " : \"\"\n";
    out << "    _" << std::hex << query << " {\n";
    out << "        ASTType ChangeQuery\n";
    out << "    }\n";

    ChangeOp op = query->getOp();
    switch (op) {
        case ChangeOp::NEW:
            out << "    _" << std::hex << query << " ||--o{ NEW : \"\"\n";
            break;
        case ChangeOp::DELETE:
            out << "    _" << std::hex << query << " ||--o{ DELETE : \"\"\n";
            break;
        case ChangeOp::LIST:
            out << "    _" << std::hex << query << " ||--o{ LIST : \"\"\n";
            break;
        case ChangeOp::SUBMIT:
            out << "    _" << std::hex << query << " ||--o{ SUBMIT : \"\"\n";
            break;
    }
}

void CypherASTDumper::dump(std::ostream& out, const CommitQuery* query) {
    out << "    script ||--o{ _" << std::hex << query << " : \"\"\n";
    out << "    _" << std::hex << query << " {\n";
    out << "        ASTType CommitQuery\n";
    out << "    }\n";
}

void CypherASTDumper::dump(std::ostream& out, const MergeDataPartsQuery* query) {
    out << "    script ||--o{ _" << std::hex << query << " : \"\"\n";
    out << "    _" << std::hex << query << " {\n";
    out << "        ASTType MergeDataPartsQuery\n";
    out << "    }\n";
}

void CypherASTDumper::dump(std::ostream& out, const LoadGMLQuery* query) {
    out << "    script ||--o{ _" << std::hex << query << " : \"\"\n";
    out << "    _" << std::hex << query << " {\n";
    out << "        ASTType LoadGMLQuery\n";
    out << "    }\n";
}

void CypherASTDumper::dump(std::ostream& out, const LoadParquetQuery* query) {
    out << "    script ||--o{ _" << std::hex << query << " : \"\"\n";
    out << "    _" << std::hex << query << " {\n";
    out << "        ASTType LoadParquetQuery\n";
    out << "    }\n";
}

void CypherASTDumper::dump(std::ostream& out, const ListGraphQuery* query) {
    out << "    script ||--o{ _" << std::hex << query << " : \"\"\n";
    out << "    _" << std::hex << query << " {\n";
    out << "        ASTType ListGraphQuery\n";
    out << "    }\n";
}

void CypherASTDumper::dump(std::ostream& out, const ListAvailableGraphsQuery* query) {
    out << "    script ||--o{ _" << std::hex << query << " : \"\"\n";
    out << "    _" << std::hex << query << " {\n";
    out << "        ASTType ListAvailableGraphsQuery\n";
    out << "    }\n";
}

void CypherASTDumper::dump(std::ostream& out, const CreateGraphQuery* query) {
    out << "    script ||--o{ _" << std::hex << query << " : \"\"\n";
    out << "    _" << std::hex << query << " {\n";
    out << "        ASTType CreateGraphQuery\n";
    out << "    }\n";
}

void CypherASTDumper::dump(std::ostream& out, const S3ConnectQuery* query) {
    out << "    script ||--o{ _" << std::hex << query << " : \"\"\n";
    out << "    _" << std::hex << query << " {\n";
    out << "        ASTType S3ConnectQuery\n";
    out << "        AccessId " << query->getAccessId() << "\n";
    out << "        Region " << query->getRegion() << "\n";
    out << "    }\n";
}

void CypherASTDumper::dump(std::ostream& out, const S3TransferQuery* query) {
    out << "    script ||--o{ _" << std::hex << query << " : \"\"\n";
    out << "    _" << std::hex << query << " {\n";
    out << "        ASTType S3TransferQuery\n";

    if (query->getDirection() == S3TransferQuery::Direction::PULL) {
        out << "        Direction PULL\n";
    } else {
        out << "        Direction PUSH\n";
    }

    out << "        S3Bucket " << query->getS3Bucket() << "\n";

    if (!query->getS3Prefix().empty()) {
        out << "        S3Prefix " << query->getS3Prefix() << "\n";
    }

    if (!query->getS3File().empty()) {
        out << "        S3File " << query->getS3File() << "\n";
    }

    out << "        LocalPath " << query->getLocalPath() << "\n";
    out << "    }\n";
}

void CypherASTDumper::dump(std::ostream& out, const MatchStmt* match) {
    out << "    _" << std::hex << match << " {\n";

    if (match->isOptional()) {
        out << "        ASTType OPTIONAL_MATCH\n";
    } else {
        out << "        ASTType MATCH\n";
    }

    out << "    }\n";

    const Pattern* pattern = match->getPattern();
    out << "    _" << std::hex << match << " ||--o{ _" << std::hex << pattern << " : \"\"\n";

    dump(out, pattern);

    if (match->hasLimit()) {
        const Limit* lim = match->getLimit();
        out << "    _" << std::hex << match << " ||--o{ _" << std::hex << lim << " : \"\"\n";
        dump(out, lim);
    }

    if (match->hasSkip()) {
        const Skip* skip = match->getSkip();
        out << "    _" << std::hex << match << " ||--o{ _" << std::hex << skip << " : \"\"\n";
        dump(out, skip);
    }
}

void CypherASTDumper::dump(std::ostream& out, const MergeStmt* merge) {
    out << "    _" << std::hex << merge << " {\n";
    out << "        ASTType MERGE\n";
    out << "    }\n";

    const Pattern* pattern = merge->getPattern();
    out << "    _" << std::hex << merge << " ||--o{ _" << std::hex << pattern << " : \"\"\n";

    dump(out, pattern);

    if (const SetStmt* onCreate = merge->getOnCreate()) {
        out << "    _" << std::hex << merge << " ||--o{ _" << std::hex << onCreate << " : \"ON CREATE\"\n";
        dump(out, onCreate);
    }

    if (const SetStmt* onMatch = merge->getOnMatch()) {
        out << "    _" << std::hex << merge << " ||--o{ _" << std::hex << onMatch << " : \"ON MATCH\"\n";
        dump(out, onMatch);
    }
}

void CypherASTDumper::dump(std::ostream& out, const CreateStmt* create) {
    out << "    _" << std::hex << create << " {\n";
    out << "        ASTType CREATE\n";
    out << "    }\n";

    const Pattern* pattern = create->getPattern();
    out << "    _" << std::hex << create << " ||--o{ _" << std::hex << pattern << " : \"\"\n";

    dump(out, pattern);
}

void CypherASTDumper::dump(std::ostream& out, const CallStmt* call) {
    out << "    _" << std::hex << call << " {\n";
    if (call->isOptional()) {
        out << "        ASTType CALL\n";
    } else {
        out << "        ASTType CALL OPTIONAL\n";
    }
    out << "    }\n";

    const FunctionInvocationExpr* funcExpr = call->getFunc();
    out << "    _" << std::hex << call << " ||--o{ _" << std::hex << funcExpr << " : \"\"\n";
    dump(out, funcExpr);

    const YieldClause* yield = call->getYield();
    if (yield) {
        out << "    _" << std::hex << call << " ||--o{ _" << std::hex << yield << " : \"\"\n";
        dump(out, yield);
    }
}

void CypherASTDumper::dump(std::ostream& out, const YieldClause* yield) {
    out << "    _" << std::hex << yield << " {\n";
    out << "        ASTType YIELD\n";
    out << "    }\n";

    const YieldItems* yieldItems = yield->getItems();
    if (!yieldItems) {
        return;
    }

    for (const SymbolExpr* expr : yieldItems->getItems()) {
        out << "    _" << std::hex << yield << " ||--o{ _" << std::hex << expr << " : \"\"\n";
        dump(out, expr);
    }

    const WhereClause* where = yieldItems->getWhereClause();
    if (where) {
        out << "    _" << std::hex << yield << " ||--o{ _" << std::hex << where << " : \"\"\n";
        dump(out, where);
    }
}

void CypherASTDumper::dump(std::ostream& out, const DeleteStmt* deleteStmt) {
    out << "    _" << std::hex << deleteStmt << " {\n";
    if (deleteStmt->isDetaching()) {
        out << "        ASTType DELETE DETACH\n";
    } else {
        out << "        ASTType DELETE\n";
    }
    out << "    }\n";

    const ExprChain* exprChain = deleteStmt->getExpressions();
    out << "    _" << std::hex << deleteStmt << " ||--o{ _" << std::hex << exprChain << " : \"\"\n";
    dump(out, exprChain);
}

void CypherASTDumper::dump(std::ostream& out, const ExprChain* chain) {
    out << "    _" << std::hex << chain << " {\n";
    out << "        ASTType EXPR_CHAIN\n";
    out << "    }\n";

    for (const Expr* expr : chain->getExprs()) {
        out << "    _" << std::hex << chain << " ||--o{ _" << std::hex << expr << " : \"\"\n";
        dump(out, expr);
    }
}

void CypherASTDumper::dump(std::ostream& out, const SetStmt* setStmt) {
    out << "    _" << std::hex << setStmt << " {\n";
    out << "        ASTType SET\n";
    out << "    }\n";

    for (const SetItem* item : setStmt->getItems()) {
        out << "    _" << std::hex << setStmt << " ||--o{ _" << std::hex << item << " : \"\"\n";
        dump(out, item);
    }
}

void CypherASTDumper::dump(std::ostream& out, const SetItem* setItem) {
    out << "    _" << std::hex << setItem << " {\n";
    out << "        ASTType SET_ITEM\n";
    out << "    }\n";

    const auto visitor = Overloaded {
        [&](const SetItem::PropertyExprAssign& assign) {
            out << "    _" << std::hex << setItem << " ||--o{ _" << std::hex << assign._propTypeExpr << " : \"\"\n";
            dump(out, assign._propTypeExpr);
            out << "    _" << std::hex << setItem << " ||--o{ _" << std::hex << assign._propValueExpr << " : \"\"\n";
            dump(out, assign._propValueExpr);
        },
        [&](const SetItem::EntityPropertiesAssign& assign) {
            out << "    _" << std::hex << setItem << " ||--o{ _" << std::hex << assign._entity << " : \"" << (assign._replaces ? "=" : "+=") << "\"\n";
            dump(out, assign._entity);
            out << "    _" << std::hex << setItem << " ||--o{ _" << std::hex << assign._value << " : \"\"\n";
            dump(out, assign._value);
        },
        [&](const SetItem::SymbolEntityTypes& symbolEntityTypes) {
            out << "    _" << std::hex << setItem << " ||--o{ _" << std::hex << symbolEntityTypes._value << " : \"\"\n";
            dump(out, symbolEntityTypes._value);
        }
    };

    std::visit(visitor, setItem->item());
}

void CypherASTDumper::dump(std::ostream& out, const Symbol* symbol) {
    out << "    _" << std::hex << symbol << " {\n";
    out << "        ASTType SYMBOL\n";
    out << "        Name " << symbol->getName() << "\n";
    out << "    }\n";
}

void CypherASTDumper::dump(std::ostream& out, const Limit* limit) {
    out << "    _" << std::hex << limit << " {\n";
    out << "        ASTType LIMIT\n";
    out << "    }\n";

    const Expr* expr = limit->getExpr();

    out << "    _" << std::hex << limit << " ||--o{ _" << std::hex << expr << " : \"\"\n";

    dump(out, expr);
}

void CypherASTDumper::dump(std::ostream& out, const Skip* skip) {
    out << "    _" << std::hex << skip << " {\n";
    out << "        ASTType SKIP\n";
    out << "    }\n";

    const Expr* expr = skip->getExpr();

    out << "    _" << std::hex << skip << " ||--o{ _" << std::hex << expr << " : \"\"\n";

    dump(out, expr);
}

void CypherASTDumper::dump(std::ostream& out, const ReturnStmt* ret) {
    out << "    _" << std::hex << ret << " {\n";
    out << "        ASTType RETURN\n";
    out << "    }\n";

    const Projection* projection = ret->getProjection();

    out << "    _" << std::hex << ret << " ||--o{ _" << std::hex << projection << " : \"\"\n";

    dump(out, projection);
}

void CypherASTDumper::dump(std::ostream& out, const WithStmt* with) {
    out << "    _" << std::hex << with << " {\n";
    out << "        ASTType WITH\n";
    out << "    }\n";

    const Projection* projection = with->getProjection();

    out << "    _" << std::hex << with << " ||--o{ _" << std::hex << projection << " : \"\"\n";

    dump(out, projection);

    if (const WhereClause* where = with->getWhere()) {
        out << "    _" << std::hex << with << " ||--o{ _" << std::hex << where << " : \"\"\n";
        dump(out, where);
    }
}

void CypherASTDumper::dump(std::ostream& out, const Projection* projection) {
    out << "    _" << std::hex << projection << " {\n";
    out << "        ASTType Projection\n";

    if (projection->isDistinct()) {
        out << "        Distinct true\n";
    }

    if (projection->hasLimit()) {
        const Limit* limit = projection->getLimit();
        out << "        Limit _" << std::hex << limit << "\n";
    }

    if (projection->hasSkip()) {
        const Skip* skip = projection->getSkip();
        out << "        Skip _" << std::hex << skip << "\n";
    }

    if (projection->isReturningAll()) {
        out << "        ProjectAll true\n";
    }

    const auto& items = projection->items();
    for (const auto& item : items) {
        if (const auto* exprPtr = std::get_if<Expr*>(&item)) {
            const Expr* expr = *exprPtr;
            out << "        Item _" << std::hex << expr << "\n";
        } else if (const auto* declPtr = std::get_if<VarDecl*>(&item)) {
            const VarDecl* decl = *declPtr;
            out << "        Item VAR_" << decl << "\n";
        } else {
            throw TuringException("Unknown projection item type");
        }
    }

    out << "    }\n";

    if (projection->hasLimit()) {
        const Limit* limit = projection->getLimit();
        out << "    _" << std::hex << projection << " ||--o{ _" << std::hex << limit << " : \"\"\n";
        dump(out, limit);
    }

    if (projection->hasSkip()) {
        const Skip* skip = projection->getSkip();
        out << "    _" << std::hex << projection << " ||--o{ _" << std::hex << skip << " : \"\"\n";
        dump(out, skip);
    }

    if (projection->isReturningAll()) {
        return;
    }
}

void CypherASTDumper::dump(std::ostream& out, const ShortestPathStmt* stmt) {
    out << "    SHORTEST PATH:" << " {\n";
    out << "    SOURCE:" << stmt->getSource()->getName() << ",\n";
    out << "    TARGET:" << stmt->getTarget()->getName() << ",\n";
    out << "    WEIGHT:" << stmt->getEdgeProperty()->getName() << "\n";

    out << "    }\n";
}
