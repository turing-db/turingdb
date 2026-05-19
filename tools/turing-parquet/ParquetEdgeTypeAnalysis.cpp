#include "ParquetEdgeTypeAnalysis.h"

using namespace db;

ParquetEdgeTypeAnalysis::ParquetEdgeTypeAnalysis() {
}

ParquetEdgeTypeAnalysis::~ParquetEdgeTypeAnalysis() {
}

void ParquetEdgeTypeAnalysis::recordType(std::string_view value) {
    const auto it = _typeCounts.find(std::string(value));
    if (it != _typeCounts.end()) {
        ++it->second;
    } else {
        _typeCounts.emplace(std::string(value), 1);
    }
    ++_totalCount;
}
