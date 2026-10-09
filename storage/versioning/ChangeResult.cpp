#include "ChangeResult.h"

using namespace db;

std::string ChangeError::fmtMessage() const {
    if (_commitError) {
        return fmt::format("ChangeError: {} ({})",
                           ChangeErrorTypeDescription::value(_type),
                           CommitErrorTypeDescription::value(_commitError->getType()));
    }

    switch (_type) {
        case ChangeErrorType::GRAPH_NOT_FOUND:
            return fmt::format("Graph '{}' does not exist", _graphName);
        break;
        case ChangeErrorType::GRAPH_NOT_LOADED:
            return fmt::format("Graph '{0}' is on disk but not loaded - use LOAD GRAPH {0} to load it", _graphName);
        break;
        default:
            return std::string {ChangeErrorTypeDescription::value(_type)};
        break;
    }
}
