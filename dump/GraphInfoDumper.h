#pragma once

#include "GraphDumpHelper.h"
#include "Graph.h"

namespace db {

class GraphInfoDumper {
public:
    explicit GraphInfoDumper(fs::FilePageWriter& writer)
        : _writer(writer)
    {
    }

    [[nodiscard]] DumpResult<void> dump(const Graph& graph) {
        GraphDumpHelper::writeFileHeader(_writer);

        const auto& name = graph.getName();

        _writer.writeToCurrentPage(graph.getID().get());
        _writer.writeToCurrentPage((uint64_t)name.size());
        _writer.writeToCurrentPage(name);

        _writer.finish();

        if (_writer.errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_WRITE_GRAPH_INFO,
                                     _writer.error().value());
        }

        return {};
    }

private:
    fs::FilePageWriter& _writer;
};

};
