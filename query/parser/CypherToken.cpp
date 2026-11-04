#include "CypherToken.h"

#include <array>

using namespace db;

namespace {

struct TokenName {
    CypherTokenKind _kind {CypherTokenKind::PROG_END};
    std::string_view _name;
};

constexpr auto TOKEN_NAMES = std::to_array<TokenName>({
    {CypherTokenKind::PROG_END, "PROG_END"},
    {CypherTokenKind::SEMI_COLON, "SEMI_COLON"},
    {CypherTokenKind::ADD_ASSIGN, "ADD_ASSIGN"},
    {CypherTokenKind::NOT_EQUAL, "NOT_EQUAL"},
    {CypherTokenKind::RANGE, "RANGE"},
    {CypherTokenKind::LE, "LE"},
    {CypherTokenKind::GE, "GE"},
    {CypherTokenKind::OPAREN, "OPAREN"},
    {CypherTokenKind::CPAREN, "CPAREN"},
    {CypherTokenKind::OBRACE, "OBRACE"},
    {CypherTokenKind::CBRACE, "CBRACE"},
    {CypherTokenKind::OBRACK, "OBRACK"},
    {CypherTokenKind::CBRACK, "CBRACK"},
    {CypherTokenKind::ASSIGN, "ASSIGN"},
    {CypherTokenKind::DOLLAR, "DOLLAR"},
    {CypherTokenKind::COMMA, "COMMA"},
    {CypherTokenKind::COLON, "COLON"},
    {CypherTokenKind::CARET, "CARET"},
    {CypherTokenKind::PIPE, "PIPE"},
    {CypherTokenKind::AMPERSAND, "AMPERSAND"},
    {CypherTokenKind::EXCLAMATION, "EXCLAMATION"},
    {CypherTokenKind::PLUS, "PLUS"},
    {CypherTokenKind::CONCAT, "CONCAT"},
    {CypherTokenKind::MULT, "MULT"},
    {CypherTokenKind::ESC, "ESC"},
    {CypherTokenKind::SUB, "SUB"},
    {CypherTokenKind::DOT, "DOT"},
    {CypherTokenKind::DIV, "DIV"},
    {CypherTokenKind::MOD, "MOD"},
    {CypherTokenKind::GT, "GT"},
    {CypherTokenKind::LT, "LT"},
    {CypherTokenKind::PROCEDURES, "PROCEDURES"},
    {CypherTokenKind::SHORTESTPATH, "SHORTESTPATH"},
    {CypherTokenKind::DESCENDING, "DESCENDING"},
    {CypherTokenKind::CONSTRAINT, "CONSTRAINT"},
    {CypherTokenKind::MANDATORY, "MANDATORY"},
    {CypherTokenKind::ASCENDING, "ASCENDING"},
    {CypherTokenKind::OPTIONAL, "OPTIONAL"},
    {CypherTokenKind::CONTAINS, "CONTAINS"},
    {CypherTokenKind::DISTINCT, "DISTINCT"},
    {CypherTokenKind::EXPLAIN, "EXPLAIN"},
    {CypherTokenKind::EXTRACT, "EXTRACT"},
    {CypherTokenKind::REQUIRE, "REQUIRE"},
    {CypherTokenKind::COLLECT, "COLLECT"},
    {CypherTokenKind::CONNECT, "CONNECT"},
    {CypherTokenKind::SUBMIT, "SUBMIT"},
    {CypherTokenKind::CHANGE, "CHANGE"},
    {CypherTokenKind::COMMIT, "COMMIT"},
    {CypherTokenKind::STARTS, "STARTS"},
    {CypherTokenKind::UNIQUE, "UNIQUE"},
    {CypherTokenKind::FILTER, "FILTER"},
    {CypherTokenKind::SINGLE, "SINGLE"},
    {CypherTokenKind::SCALAR, "SCALAR"},
    {CypherTokenKind::UNWIND, "UNWIND"},
    {CypherTokenKind::FOREACH, "FOREACH"},
    {CypherTokenKind::REMOVE, "REMOVE"},
    {CypherTokenKind::RETURN, "RETURN"},
    {CypherTokenKind::CREATE, "CREATE"},
    {CypherTokenKind::DELETE, "DELETE"},
    {CypherTokenKind::DETACH, "DETACH"},
    {CypherTokenKind::EXISTS, "EXISTS"},
    {CypherTokenKind::LIMIT, "LIMIT"},
    {CypherTokenKind::YIELD, "YIELD"},
    {CypherTokenKind::MATCH, "MATCH"},
    {CypherTokenKind::MERGE, "MERGE"},
    {CypherTokenKind::MERGE_DATAPARTS, "MERGE_DATAPARTS"},
    {CypherTokenKind::ORDER, "ORDER"},
    {CypherTokenKind::WHERE, "WHERE"},
    {CypherTokenKind::UNION, "UNION"},
    {CypherTokenKind::FALSE, "FALSE"},
    {CypherTokenKind::COUNT, "COUNT"},
    {CypherTokenKind::REDUCE, "REDUCE"},
    {CypherTokenKind::ALLREDUCE, "ALLREDUCE"},
    {CypherTokenKind::NORMALIZE, "NORMALIZE"},
    {CypherTokenKind::NORMALIZED, "NORMALIZED"},
    {CypherTokenKind::NFC, "NFC"},
    {CypherTokenKind::NFD, "NFD"},
    {CypherTokenKind::NFKC, "NFKC"},
    {CypherTokenKind::NFKD, "NFKD"},
    {CypherTokenKind::GRAPH, "GRAPH"},
    {CypherTokenKind::GRAPHS, "GRAPHS"},
    {CypherTokenKind::AVAILABLE, "AVAILABLE"},
    {CypherTokenKind::HEADERS, "HEADERS"},
    {CypherTokenKind::DATETIMES, "DATETIMES"},
    {CypherTokenKind::DURATIONS, "DURATIONS"},
    {CypherTokenKind::EMBEDDINGS, "EMBEDDINGS"},
    {CypherTokenKind::JSONL, "JSONL"},
    {CypherTokenKind::LIST, "LIST"},
    {CypherTokenKind::DESC, "DESC"},
    {CypherTokenKind::CALL, "CALL"},
    {CypherTokenKind::INSTALL, "INSTALL"},
    {CypherTokenKind::EXTENSIONS, "EXTENSIONS"},
    {CypherTokenKind::NULL_, "NULL_"},
    {CypherTokenKind::TRUE, "TRUE"},
    {CypherTokenKind::WHEN, "WHEN"},
    {CypherTokenKind::NONE, "NONE"},
    {CypherTokenKind::THEN, "THEN"},
    {CypherTokenKind::ELSE, "ELSE"},
    {CypherTokenKind::CASE, "CASE"},
    {CypherTokenKind::ERROR_, "ERROR_"},
    {CypherTokenKind::ENDS, "ENDS"},
    {CypherTokenKind::DROP, "DROP"},
    {CypherTokenKind::SHOW, "SHOW"},
    {CypherTokenKind::SKIP, "SKIP"},
    {CypherTokenKind::WITH, "WITH"},
    {CypherTokenKind::LOAD, "LOAD"},
    {CypherTokenKind::PUSH, "PUSH"},
    {CypherTokenKind::PULL, "PULL"},
    {CypherTokenKind::NEW, "NEW"},
    {CypherTokenKind::NEXT, "NEXT"},
    {CypherTokenKind::FAIL, "FAIL"},
    {CypherTokenKind::CSV, "CSV"},
    {CypherTokenKind::GML, "GML"},
    {CypherTokenKind::PARQUET, "PARQUET"},
    {CypherTokenKind::ANY, "ANY"},
    {CypherTokenKind::SET, "SET"},
    {CypherTokenKind::ALL, "ALL"},
    {CypherTokenKind::ASC, "ASC"},
    {CypherTokenKind::NOT, "NOT"},
    {CypherTokenKind::END, "END"},
    {CypherTokenKind::XOR, "XOR"},
    {CypherTokenKind::FOR, "FOR"},
    {CypherTokenKind::FROM, "FROM"},
    {CypherTokenKind::ADD, "ADD"},
    {CypherTokenKind::AND, "AND"},
    {CypherTokenKind::OR, "OR"},
    {CypherTokenKind::IN, "IN"},
    {CypherTokenKind::IS, "IS"},
    {CypherTokenKind::S3, "S3"},
    {CypherTokenKind::BY, "BY"},
    {CypherTokenKind::DO, "DO"},
    {CypherTokenKind::OF, "OF"},
    {CypherTokenKind::ON, "ON"},
    {CypherTokenKind::IF, "IF"},
    {CypherTokenKind::AS, "AS"},
    {CypherTokenKind::VECTOR, "VECTOR"},
    {CypherTokenKind::EMBEDDING, "EMBEDDING"},
    {CypherTokenKind::INDEX, "INDEX"},
    {CypherTokenKind::DIMENSION, "DIMENSION"},
    {CypherTokenKind::METRIC, "METRIC"},
    {CypherTokenKind::EUCLID, "EUCLID"},
    {CypherTokenKind::COSINE, "COSINE"},
    {CypherTokenKind::HNSW, "HNSW"},
    {CypherTokenKind::FLAT, "FLAT"},
    {CypherTokenKind::TYPE, "TYPE"},
    {CypherTokenKind::INDEXES, "INDEXES"},
    {CypherTokenKind::SEARCH, "SEARCH"},
    {CypherTokenKind::ESC_LITERAL, "ESC_LITERAL"},
    {CypherTokenKind::ID, "ID"},
    {CypherTokenKind::STRING_LITERAL, "STRING_LITERAL"},
    {CypherTokenKind::IS_VALUE_TEST, "IS_VALUE_TEST"},
    {CypherTokenKind::DIGIT, "DIGIT"},
    {CypherTokenKind::DOUBLE, "DOUBLE"},
    {CypherTokenKind::INT64_MIN_MAGNITUDE, "integer literal 9223372036854775808, which exceeds int64 range unless negated"},
    {CypherTokenKind::UNKNOWN, "UNKNOWN"},
    {CypherTokenKind::LEXER_ERROR, "LEXER_ERROR"},
});

constexpr bool namesEveryKindInOrder() {
    for (size_t index = 0; index < TOKEN_NAMES.size(); index++) {
        if (static_cast<size_t>(TOKEN_NAMES[index]._kind) != index) {
            return false;
        }
    }

    return true;
}

static_assert(TOKEN_NAMES.size() == CypherToken::KIND_COUNT);

static_assert(namesEveryKindInOrder());

}

std::string_view CypherToken::toString(CypherTokenKind kind) {
    return TOKEN_NAMES[static_cast<size_t>(kind)]._name;
}

bool CypherToken::isName(CypherTokenKind kind) {
    return CypherTokenKind::PROCEDURES <= kind && kind <= CypherTokenKind::ID;
}

bool CypherToken::isSymbol(CypherTokenKind kind) {
    const bool spellsALiteral = kind == CypherTokenKind::NULL_
                                || kind == CypherTokenKind::TRUE
                                || kind == CypherTokenKind::FALSE;

    return isName(kind) && !spellsALiteral;
}

bool CypherToken::opensBracket(CypherTokenKind kind) {
    return kind == CypherTokenKind::OPAREN
           || kind == CypherTokenKind::OBRACK
           || kind == CypherTokenKind::OBRACE;
}

bool CypherToken::closesBracket(CypherTokenKind kind) {
    return kind == CypherTokenKind::CPAREN
           || kind == CypherTokenKind::CBRACK
           || kind == CypherTokenKind::CBRACE;
}

CypherTokenKind CypherToken::closerOf(CypherTokenKind opener) {
    if (opener == CypherTokenKind::OPAREN) {
        return CypherTokenKind::CPAREN;
    } else if (opener == CypherTokenKind::OBRACK) {
        return CypherTokenKind::CBRACK;
    }

    return CypherTokenKind::CBRACE;
}
