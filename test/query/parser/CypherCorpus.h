#pragma once

#include <string>
#include <vector>

namespace turing::test {

// The queries of the fuzzing corpus, fuzz/corpus/cypher, in the order of their file names
void readCypherCorpus(std::vector<std::string>& queries);

}
