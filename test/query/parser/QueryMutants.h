#pragma once

#include <string>
#include <vector>

namespace turing::test {

// Every distinct query one edit away from @param query: each of its tokens deleted, doubled,
// swapped with the next one, or replaced by a token that opens, closes or decides a construct
void buildQueryMutants(const std::string& query, std::vector<std::string>& mutants);

}
