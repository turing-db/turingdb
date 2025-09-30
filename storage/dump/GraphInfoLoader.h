#pragma once

#include "FilePageReader.h"
#include "GraphDumpHelper.h"
#include "Graph.h"

namespace db {

class GraphInfoLoader {
public:
    explicit GraphInfoLoader(fs::FilePageReader& reader)
        : _reader(reader)
    {
    }

    [[nodiscard]] DumpResult<bool> isSameGraph(const Graph& graph) const {
        _reader.nextPage();

        if (_reader.errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_GRAPH_INFO,
                                     _reader.error().value());
        }

        // Start reading metadata page
        auto it = _reader.begin();

        if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_GRAPH_INFO);
        }

        if (auto res = GraphDumpHelper::checkFileHeader(it); !res) {
            return res.get_unexpected();
        }

        const GraphID graphID {it.get<GraphID::ValueType>()};

        if (graphID != graph.getID()) {
            return false;
        }

        const uint64_t nameSize = it.get<uint64_t>();
        const std::string_view name = it.get<char>(nameSize);

        if (name != graph._graphName) {
            return false;
        }

        return true;
    }

    [[nodiscard]] DumpResult<void> load(Graph& graph) {
        _reader.nextPage();

        if (_reader.errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_GRAPH_INFO,
                                     _reader.error().value());
        }

        // Start reading metadata page
        auto it = _reader.begin();

        if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_GRAPH_INFO);
        }

        if (auto res = GraphDumpHelper::checkFileHeader(it); !res) {
            return res.get_unexpected();
        }

        const GraphID graphID {it.get<GraphID::ValueType>()};
        const uint64_t nameSize = it.get<uint64_t>();
        const std::string_view name = it.get<char>(nameSize);

        graph._graphID = graphID;
        graph._graphName = name;

        return {};
    }

private:
    fs::FilePageReader& _reader;
};

}
