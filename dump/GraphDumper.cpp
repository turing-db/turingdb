#include "GraphDumper.h"

#include <range/v3/view/enumerate.hpp>

#include "Graph.h"
#include "GraphMetadata.h"
#include "reader/GraphReader.h"
#include "GraphInfoDumper.h"
#include "LabelMapDumper.h"
#include "DataPartDumper.h"
#include "LabelSetMapDumper.h"
#include "EdgeTypeMapDumper.h"
#include "PropertyTypeMapDumper.h"
#include "GraphFileType.h"
#include "FileUtils.h"

using namespace db;
namespace rg = ranges;
namespace rv = rg::views;

DumpResult<void> GraphDumper::dump(const Graph& graph, const fs::Path& path) {
    if (path.exists()) {
        return DumpError::result(DumpErrorType::GRAPH_DIR_ALREADY_EXISTS);
    }

    // Create directory
    if (auto res = path.mkdir(); !res) {
        return DumpError::result(DumpErrorType::CANNOT_MKDIR_GRAPH, res.error());
    }

    // Dump graph type
    {
        const fs::Path graphTypePath = path / "type";
        const auto typeTag = GraphFileTypeDescription::value(GraphFileType::BINARY);
        if (!FileUtils::writeFile(graphTypePath.get(), typeTag.data())) {
            return DumpError::result(DumpErrorType::CANNOT_WRITE_GRAPH_TYPE);
        }
    }

    // Dumping graph info
    {
        const fs::Path infoPath = path / "info";

        auto writer = fs::FilePageWriter::open(infoPath);
        if (!writer) {
            return DumpError::result(DumpErrorType::CANNOT_OPEN_GRAPH_INFO, writer.error());
        }

        GraphInfoDumper dumper {writer.value()};

        if (auto res = dumper.dump(graph); !res) {
            return res;
        }
    }

    const auto& metadata = graph.getMetadata();

    // Dumping labels
    {
        const fs::Path labelsPath = path / "labels";

        auto writer = fs::FilePageWriter::open(labelsPath);
        if (!writer) {
            return DumpError::result(DumpErrorType::CANNOT_OPEN_LABELS, writer.error());
        }

        LabelMapDumper dumper {writer.value()};

        if (auto res = dumper.dump(metadata->labels()); !res) {
            return res;
        }
    }

    // Dumping edge types
    {
        const fs::Path edgetypesPath = path / "edge-types";

        auto writer = fs::FilePageWriter::open(edgetypesPath);
        if (!writer) {
            return DumpError::result(DumpErrorType::CANNOT_OPEN_EDGE_TYPES, writer.error());
        }

        EdgeTypeMapDumper dumper {writer.value()};

        if (auto res = dumper.dump(metadata->edgeTypes()); !res) {
            return res;
        }
    }

    // Dumping property types
    {
        const fs::Path proptypesPath = path / "property-types";

        auto writer = fs::FilePageWriter::open(proptypesPath);
        if (!writer) {
            return DumpError::result(DumpErrorType::CANNOT_OPEN_PROPERTY_TYPES, writer.error());
        }

        PropertyTypeMapDumper dumper {writer.value()};

        if (auto res = dumper.dump(metadata->propTypes()); !res) {
            return res;
        }
    }

    // Dumping labelsets
    {
        const fs::Path labelsetsPath = path / "labelsets";

        auto writer = fs::FilePageWriter::open(labelsetsPath);
        if (!writer) {
            return DumpError::result(DumpErrorType::CANNOT_OPEN_LABELSETS, writer.error());
        }

        LabelSetMapDumper dumper {writer.value()};

        if (auto res = dumper.dump(metadata->labelsets()); !res) {
            return res;
        }
    }

    GraphReader reader = graph.read();
    for (const auto& [i, part] : reader.dataparts() | rv::enumerate) {
        const fs::Path partPath = path / "datapart-" + std::to_string(i);

        if (auto res = DataPartDumper::dump(*part, partPath); !res) {
            return res;
        }
    }

    return {};
}

