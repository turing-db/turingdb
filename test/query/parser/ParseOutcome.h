#pragma once

#include <string>
#include <string_view>

namespace turing::test {

// The report of a query that fails to parse, without its title line; empty when it parses
void renderParseError(std::string_view query, std::string& report);

// A query parses or is turned away with a syntax error report: anything else, a failed assertion
// included, is described in @param problem
bool isWellFormedParse(std::string_view query, std::string& problem);

}
