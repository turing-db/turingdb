#include "ChangeResult.h"

using namespace db;

ChangeError::ChangeError(ChangeErrorType type, ChangeID changeID)
    : _type(type)
{
    changeID.appendString(_name);
}

ChangeError::ChangeError(ChangeErrorType type, CommitHash commitHash)
    : _type(type)
{
    commitHash.appendString(_name);
}

std::string ChangeError::fmtMessage() const {
    if (_commitError) {
        return fmt::format("ChangeError: {} ({})",
                           ChangeErrorTypeDescription::value(_type),
                           CommitErrorTypeDescription::value(_commitError->getType()));
    }

    switch (_type) {
        case ChangeErrorType::GRAPH_NOT_FOUND:
            return fmt::format("Graph '{}' does not exist", _name);
        break;
        case ChangeErrorType::GRAPH_NOT_LOADED:
            return fmt::format("Graph '{0}' is on disk but not loaded - use LOAD GRAPH {0} to load it", _name);
        break;
        case ChangeErrorType::CHANGE_NOT_FOUND:
            return fmt::format("Change '{}' does not exist", _name);
        break;
        case ChangeErrorType::COMMIT_NOT_FOUND:
            return fmt::format("Commit '{}' does not exist", _name);
        break;
        case ChangeErrorType::COMMIT_NOT_LOADED:
            return fmt::format("Commit '{0}' is not loaded to memory - use LOAD COMMIT '{0}' to load it", _name);
        break;
        default:
            return std::string {ChangeErrorTypeDescription::value(_type)};
        break;
    }
}
