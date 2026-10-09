#pragma once

#include <optional>
#include <string>
#include <string_view>

#include "BasicResult.h"
#include "EnumToString.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"
#include "versioning/CommitResult.h"

namespace db {

enum class ChangeErrorType : uint8_t {
    GRAPH_NOT_FOUND,
    GRAPH_NOT_LOADED,
    CHANGE_NOT_FOUND,
    COMMIT_NOT_FOUND,
    COMMIT_NOT_LOADED,
    COULD_NOT_CREATE_CHANGE,
    COULD_NOT_ACCEPT_CHANGE,
    SERIALIZATION_ERROR,

    _SIZE,
};

using ChangeErrorTypeDescription = EnumToString<ChangeErrorType>::Create<
    EnumStringPair<ChangeErrorType::GRAPH_NOT_FOUND, "Graph does not exist">,
    EnumStringPair<ChangeErrorType::GRAPH_NOT_LOADED, "Graph is on disk but not loaded - use LOAD GRAPH <name> to load it">,
    EnumStringPair<ChangeErrorType::CHANGE_NOT_FOUND, "Change does not exist">,
    EnumStringPair<ChangeErrorType::COMMIT_NOT_FOUND, "Commit does not exist">,
    EnumStringPair<ChangeErrorType::COMMIT_NOT_LOADED, "Commit not loaded to memory - use LOAD COMMIT '<hash>' to load a commit">,
    EnumStringPair<ChangeErrorType::COULD_NOT_CREATE_CHANGE, "Could not create change">,
    EnumStringPair<ChangeErrorType::COULD_NOT_ACCEPT_CHANGE, "Could not accept change">,
    EnumStringPair<ChangeErrorType::SERIALIZATION_ERROR, "Could not sync the graph state with the disk">>;

class ChangeError {
public:
    explicit ChangeError(ChangeErrorType type)
        : _type(type)
    {
    }

    ChangeError(ChangeErrorType type, CommitError commitError)
        : _commitError(commitError),
          _type(type)
        
    {
    }

    ChangeError(ChangeErrorType type, std::string_view graphName)
        : _name(graphName),
          _type(type)
    {
    }

    ChangeError(ChangeErrorType type, ChangeID changeID);
    ChangeError(ChangeErrorType type, CommitHash commitHash);

    [[nodiscard]] ChangeErrorType getType() const { return _type; }
    [[nodiscard]] std::string fmtMessage() const;

    template <typename... T>
    static BadResult<ChangeError> result(ChangeErrorType type) {
        return BadResult<ChangeError>(ChangeError(type));
    }

    template <typename... T>
    static BadResult<ChangeError> result(ChangeErrorType type, CommitError commitError) {
        return BadResult<ChangeError>(ChangeError(type, commitError));
    }

    static BadResult<ChangeError> result(ChangeErrorType type, ChangeID changeID) {
        return BadResult<ChangeError>(ChangeError(type, changeID));
    }

    static BadResult<ChangeError> result(ChangeErrorType type, CommitHash commitHash) {
        return BadResult<ChangeError>(ChangeError(type, commitHash));
    }

    static BadResult<ChangeError> graphResult(ChangeErrorType type, std::string_view graphName) {
        return BadResult<ChangeError>(ChangeError(type, graphName));
    }

private:
    std::optional<CommitError> _commitError;
    std::string _name;
    ChangeErrorType _type {ChangeErrorType::GRAPH_NOT_FOUND};
};

template <typename T>
using ChangeResult = BasicResult<T, class ChangeError>;

}
