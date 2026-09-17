#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace turing::test {

struct QueryTestSpec {
    std::string _name;
    std::string _graphName {"simpledb"};
    std::string _query;
    std::string _expectResult;
    std::string _expectMlir;
    std::vector<std::string> _tags;
    bool _enabled {true};
    bool _writeRequired {false};
    std::string _disabledReason;
};

struct V3QueryTestResult {
    std::string _name;
    std::string _resultOutput;
    std::string _mlirOutput;
    bool _resultMatched {false};
    bool _mlirMatched {false};
    uint64_t _timeUs {0};
};

}
