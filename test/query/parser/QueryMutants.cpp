#include "QueryMutants.h"

#include <stddef.h>
#include <array>
#include <set>
#include <string_view>

#include "CypherLexer.h"
#include "CypherToken.h"

using namespace db;

namespace {

struct TokenSpan {
    size_t _begin {0};
    size_t _end {0};
};

constexpr std::array<std::string_view, 21> REPLACEMENTS {
    "(", ")", "[", "]", "{", "}", "-", "<", ">", "|", ":", ",", " AS ", " WHERE ", " IS ", " NULL ", " CASE ", " NOT ",
    " DISTINCT ", " COUNT ", "=",
};

}

void turing::test::buildQueryMutants(const std::string& query, std::vector<std::string>& mutants) {
    std::vector<TokenSpan> spans;

    CypherLexer lexer(query);
    CypherToken token;
    lexer.lex(token);

    while (token._kind != CypherTokenKind::PROG_END && token._kind != CypherTokenKind::LEXER_ERROR) {
        spans.push_back({token._location._beginOffset, token._location._endOffset});
        lexer.lex(token);
    }

    std::set<std::string> distinct;
    for (size_t index = 0; index < spans.size(); index++) {
        const TokenSpan& span = spans[index];
        const std::string before = query.substr(0, span._begin);
        const std::string text = query.substr(span._begin, span._end - span._begin);
        const std::string after = query.substr(span._end);

        distinct.insert(before + after);
        distinct.insert(before + text + " " + text + after);

        if (index + 1 < spans.size()) {
            const TokenSpan& next = spans[index + 1];
            const std::string between = query.substr(span._end, next._begin - span._end);
            const std::string nextText = query.substr(next._begin, next._end - next._begin);
            distinct.insert(before + nextText + between + text + query.substr(next._end));
        }

        for (const std::string_view replacement : REPLACEMENTS) {
            distinct.insert(before + std::string(replacement) + after);
        }
    }

    distinct.erase(query);
    mutants.assign(distinct.begin(), distinct.end());
}
