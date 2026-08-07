#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace turing::test {

struct QueryTestSpec {
    std::string _name;
    std::string _graphName {"simpledb"};
    std::string _query;
    std::string _expectPlan;
    std::string _expectResult;
    std::string _expectResultJson;
    std::string _expectMlir;
    std::vector<std::string> _tags;
    bool _enabled {true};
    bool _remoteEnabled {true};
    bool _writeRequired {false};
    std::string _disabledReason;
    std::string _remoteDisabledReason;
};

struct QueryTestResult {
    std::string _name;
    std::string _planOutput;
    std::string _resultOutput;
    std::string _resultJsonOutput;
    std::string _resultJsonError;
    bool _planMatched {false};
    bool _resultMatched {false};
    bool _resultJsonMatched {false};
    bool _resultJsonValid {false};
    uint64_t _timeUs {0};
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
