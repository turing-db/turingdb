#pragma once

#include <string>
#include <string_view>

namespace turing::test {

// The report of a query that fails to parse, without its title line. For a query that parses,
// the kind of each query, then each expression and each node or relationship pattern the parse
// created, in creation order, with the text it spans
void renderParseOutcome(std::string_view query, std::string& outcome);

// A query parses or is turned away with a syntax error report: anything else, a failed assertion
// included, is described in @param problem
bool isWellFormedParse(std::string_view query, std::string& problem);

}
