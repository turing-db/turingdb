#pragma once

#include <optional>
#include <string>

#include "BasicResult.h"
#include "EnumToString.h"
#include "FileResult.h"

namespace db {

class Path;

enum class DumpErrorType : uint8_t {
    UNKNOWN = 0,

    NOT_TURING_FILE,
    OUTDATED,
    NO_COMMITS,

    GRAPH_DIR_ALREADY_EXISTS,
    COMMIT_ALREADY_EXISTS,
    DATAPART_ALREADY_EXISTS,
    GRAPH_DOES_NOT_EXIST,
    COMMIT_DOES_NOT_EXIST,
    DATAPART_DOES_NOT_EXIST,
    NOT_DIRECTORY,
    CANNOT_MKDIR_GRAPH,
    CANNOT_MKDIR_COMMIT,
    CANNOT_MKDIR_DATAPART,

    CANNOT_WRITE_GRAPH_TYPE,

    CANNOT_OPEN_GRAPH_INFO,
    CANNOT_OPEN_LABELS,
    CANNOT_OPEN_LABELSETS,
    CANNOT_OPEN_EDGE_TYPES,
    CANNOT_OPEN_PROPERTY_TYPES,

    CANNOT_LIST_COMMITS,
    CANNOT_LIST_COMMIT_FILES,

    CANNOT_LIST_DATAPARTS,
    CANNOT_LIST_DATAPART_FILES,

    CANNOT_OPEN_DATAPART_INFO,
    CANNOT_OPEN_DATAPART_NODES,
    CANNOT_OPEN_DATAPART_EDGES,
    CANNOT_OPEN_DATAPART_EDGE_INDEXER,
    CANNOT_OPEN_DATAPART_NODE_PROPS,
    CANNOT_OPEN_DATAPART_EDGE_PROPS,
    CANNOT_OPEN_DATAPART_NODE_PROP_INDEXER,
    CANNOT_OPEN_DATAPART_EDGE_PROP_INDEXER,

    INCORRECT_PROPERTY_TYPE_ID,

    COULD_NOT_WRITE_GRAPH_INFO,
    COULD_NOT_WRITE_DATAPART_INFO,
    COULD_NOT_WRITE_PROP_TYPES,
    COULD_NOT_WRITE_LABELS,
    COULD_NOT_WRITE_LABELSETS,
    COULD_NOT_WRITE_EDGE_TYPES,
    COULD_NOT_WRITE_NODES,
    COULD_NOT_WRITE_EDGES,
    COULD_NOT_WRITE_EDGE_INDEXER,
    COULD_NOT_WRITE_PROPS,
    COULD_NOT_WRITE_PROP_INDEXER,

    COULD_NOT_READ_GRAPH_INFO,
    COULD_NOT_READ_DATAPART_INFO,
    COULD_NOT_READ_PROP_TYPES,
    COULD_NOT_READ_LABELS,
    COULD_NOT_READ_LABELSETS,
    COULD_NOT_READ_EDGE_TYPES,
    COULD_NOT_READ_NODES,
    COULD_NOT_READ_EDGES,
    COULD_NOT_READ_EDGE_INDEXER,
    COULD_NOT_READ_PROPS,
    COULD_NOT_READ_PROP_INDEXER,

    _SIZE,
};

using DumpErrorTypeDescription = EnumToString<DumpErrorType>::Create<
    EnumStringPair<DumpErrorType::UNKNOWN, "Unknown">,
    EnumStringPair<DumpErrorType::NOT_TURING_FILE, "Not a turing file">,
    EnumStringPair<DumpErrorType::OUTDATED, "File outdated">,
    EnumStringPair<DumpErrorType::NO_COMMITS, "Graph does not have commits">,
    EnumStringPair<DumpErrorType::GRAPH_DIR_ALREADY_EXISTS, "Graph directory already exists">,
    EnumStringPair<DumpErrorType::COMMIT_ALREADY_EXISTS, "Commit already exists">,
    EnumStringPair<DumpErrorType::DATAPART_ALREADY_EXISTS, "Datapart already exists">,
    EnumStringPair<DumpErrorType::GRAPH_DOES_NOT_EXIST, "Graph does not exist">,
    EnumStringPair<DumpErrorType::COMMIT_DOES_NOT_EXIST, "Commit does not exist">,
    EnumStringPair<DumpErrorType::DATAPART_DOES_NOT_EXIST, "Datapart does not exist">,
    EnumStringPair<DumpErrorType::NOT_DIRECTORY, "Not a directory">,
    EnumStringPair<DumpErrorType::CANNOT_MKDIR_GRAPH, "Cannot create graph directory">,
    EnumStringPair<DumpErrorType::CANNOT_MKDIR_COMMIT, "Cannot create commit directory">,
    EnumStringPair<DumpErrorType::CANNOT_MKDIR_DATAPART, "Cannot create datapart directory">,
    EnumStringPair<DumpErrorType::CANNOT_WRITE_GRAPH_TYPE, "Cannot write graph type">,
    EnumStringPair<DumpErrorType::CANNOT_OPEN_GRAPH_INFO, "Cannot open graph info">,
    EnumStringPair<DumpErrorType::CANNOT_OPEN_LABELS, "Cannot open graph labels">,
    EnumStringPair<DumpErrorType::CANNOT_OPEN_LABELSETS, "Cannot open graph labelsets">,
    EnumStringPair<DumpErrorType::CANNOT_OPEN_EDGE_TYPES, "Cannot open graph edge types">,
    EnumStringPair<DumpErrorType::CANNOT_OPEN_PROPERTY_TYPES, "Cannot open graph property types">,
    EnumStringPair<DumpErrorType::CANNOT_LIST_COMMITS, "Cannot list commits">,
    EnumStringPair<DumpErrorType::CANNOT_LIST_COMMIT_FILES, "Cannot list commit files">,
    EnumStringPair<DumpErrorType::CANNOT_LIST_DATAPARTS, "Cannot list dataparts">,
    EnumStringPair<DumpErrorType::CANNOT_LIST_DATAPART_FILES, "Cannot list datapart files">,
    EnumStringPair<DumpErrorType::CANNOT_OPEN_DATAPART_INFO, "Cannot open datapart info">,
    EnumStringPair<DumpErrorType::CANNOT_OPEN_DATAPART_NODES, "Cannot open datapart nodes">,
    EnumStringPair<DumpErrorType::CANNOT_OPEN_DATAPART_EDGES, "Cannot open datapart edges">,
    EnumStringPair<DumpErrorType::CANNOT_OPEN_DATAPART_EDGE_INDEXER, "Cannot open datapart edge indexer">,
    EnumStringPair<DumpErrorType::CANNOT_OPEN_DATAPART_NODE_PROPS, "Cannot open datapart node properties">,
    EnumStringPair<DumpErrorType::CANNOT_OPEN_DATAPART_EDGE_PROPS, "Cannot open datapart edge properties">,
    EnumStringPair<DumpErrorType::CANNOT_OPEN_DATAPART_NODE_PROP_INDEXER, "Cannot open datapart node property indexer">,
    EnumStringPair<DumpErrorType::CANNOT_OPEN_DATAPART_EDGE_PROP_INDEXER, "Cannot open datapart edge property indexer">,
    EnumStringPair<DumpErrorType::INCORRECT_PROPERTY_TYPE_ID, "Incorrect property type id">,
    EnumStringPair<DumpErrorType::COULD_NOT_WRITE_GRAPH_INFO, "Could not write graph info">,
    EnumStringPair<DumpErrorType::COULD_NOT_WRITE_DATAPART_INFO, "Could not write datapart info">,
    EnumStringPair<DumpErrorType::COULD_NOT_WRITE_PROP_TYPES, "Could not write property types">,
    EnumStringPair<DumpErrorType::COULD_NOT_WRITE_LABELS, "Could not write labels">,
    EnumStringPair<DumpErrorType::COULD_NOT_WRITE_LABELSETS, "Could not write labelsets">,
    EnumStringPair<DumpErrorType::COULD_NOT_WRITE_EDGE_TYPES, "Could not write edge types">,
    EnumStringPair<DumpErrorType::COULD_NOT_WRITE_NODES, "Could not write nodes">,
    EnumStringPair<DumpErrorType::COULD_NOT_WRITE_EDGES, "Could not write edges">,
    EnumStringPair<DumpErrorType::COULD_NOT_WRITE_EDGE_INDEXER, "Could not write edge indexer">,
    EnumStringPair<DumpErrorType::COULD_NOT_WRITE_PROPS, "Could not write entity properties">,
    EnumStringPair<DumpErrorType::COULD_NOT_WRITE_PROP_INDEXER, "Could not write entity property indexer">,
    EnumStringPair<DumpErrorType::COULD_NOT_READ_GRAPH_INFO, "Could not read graph info">,
    EnumStringPair<DumpErrorType::COULD_NOT_READ_DATAPART_INFO, "Could not read datapart info">,
    EnumStringPair<DumpErrorType::COULD_NOT_READ_PROP_TYPES, "Could not read property types">,
    EnumStringPair<DumpErrorType::COULD_NOT_READ_LABELS, "Could not read labels">,
    EnumStringPair<DumpErrorType::COULD_NOT_READ_LABELSETS, "Could not read labelsets">,
    EnumStringPair<DumpErrorType::COULD_NOT_READ_EDGE_TYPES, "Could not read edge types">,
    EnumStringPair<DumpErrorType::COULD_NOT_READ_NODES, "Could not read nodes">,
    EnumStringPair<DumpErrorType::COULD_NOT_READ_EDGES, "Could not read edges">,
    EnumStringPair<DumpErrorType::COULD_NOT_READ_EDGE_INDEXER, "Could not read edge indexer">,
    EnumStringPair<DumpErrorType::COULD_NOT_READ_PROPS, "Could not read entity properties">,
    EnumStringPair<DumpErrorType::COULD_NOT_READ_PROP_INDEXER, "Could not read entity property indexer">>;

class DumpError {
public:
    explicit DumpError(DumpErrorType type)
        : _type(type)
    {
    }

    DumpError(DumpErrorType type,
              std::optional<fs::Error> fileError)
        : _type(type),
        _fileError(fileError)
    {
    }

    [[nodiscard]] DumpErrorType getType() const { return _type; }
    [[nodiscard]] std::optional<fs::Error> getFileError() const { return _fileError; }
    [[nodiscard]] std::string fmtMessage() const;

    template <typename... T>
    static BadResult<DumpError> result(DumpErrorType type) {
        return BadResult<DumpError>(DumpError(type, std::nullopt));
    }

    template <typename... T>
    static BadResult<DumpError> result(DumpErrorType type,
                                       fs::Error fileError) {
        return BadResult<DumpError>(DumpError(type, fileError));
    }

private:
    DumpErrorType _type {DumpErrorType::UNKNOWN};
    std::optional<fs::Error> _fileError;
};

template <typename T>
using DumpResult = BasicResult<T, class DumpError>;

}
