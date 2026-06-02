#include "DataPartParquetDumper.h"

#include <stddef.h>
#include <stdint.h>

#include <string_view>
#include <vector>

#include "ParquetWriteSchema.h"
#include "ParquetWriter.h"

#include "DataPartParquetLayout.h"
#include "NodeContainerParquetDumper.h"
#include "EdgeContainerParquetDumper.h"
#include "EdgeIndexerParquetDumper.h"
#include "PropertyIndexerParquetDumper.h"
#include "PropertyContainerParquetDumper.h"
#include "StringPropertyIndexerParquetDumper.h"

#include "datapart/DataPart.h"
#include "properties/PropertyManager.h"
#include "Path.h"

#include "FatalException.h"

using namespace db;

namespace {

constexpr std::string_view DATA_PART_ID_COLUMN = "data_part_id";
constexpr std::string_view FIRST_NODE_ID_COLUMN = "first_node_id";
constexpr std::string_view FIRST_EDGE_ID_COLUMN = "first_edge_id";

void writeInfo(const DataPart& part, const fs::Path& path) {
    ParquetWriteSchema schema;
    schema.addColumn(DATA_PART_ID_COLUMN, ParquetColumnType::UInt64);
    schema.addColumn(FIRST_NODE_ID_COLUMN, ParquetColumnType::UInt64);
    schema.addColumn(FIRST_EDGE_ID_COLUMN, ParquetColumnType::UInt64);

    ParquetWriter writer(path, schema);

    const std::vector<int64_t> dataPartId {static_cast<int64_t>(part.getID().get())};
    const std::vector<int64_t> firstNodeId {static_cast<int64_t>(part.getFirstNodeID().getValue())};
    const std::vector<int64_t> firstEdgeId {static_cast<int64_t>(part.getFirstEdgeID().getValue())};

    writer.beginRowGroup(1);
    writer.writeInt64Column(0, dataPartId);
    writer.writeInt64Column(1, firstNodeId);
    writer.writeInt64Column(2, firstEdgeId);
    writer.finish();
}

}

void DataPartParquetDumper::dump(const DataPart& part, const fs::Path& partDir) {
    namespace layout = dataPartParquetLayout;

    if (!partDir.exists()) {
        if (const auto res = partDir.mkdir(); !res) {
            throw FatalException("DataPartParquetDumper: cannot create part directory");
        }
    }

    writeInfo(part, layout::info(partDir));

    NodeContainerParquetDumper::dump(part.nodes(),
                                     layout::nodeRanges(partDir),
                                     layout::nodeRecords(partDir));

    EdgeContainerParquetDumper::dump(part.edges(),
                                     layout::edgesOut(partDir),
                                     layout::edgesIn(partDir));

    EdgeIndexerParquetDumper::dump(part.edgeIndexer(),
                                   layout::edgeIndexerNodeData(partDir),
                                   layout::edgeIndexerPatch(partDir),
                                   layout::edgeIndexerOutSpans(partDir),
                                   layout::edgeIndexerInSpans(partDir));

    const PropertyManager& nodeProperties = part.nodeProperties();
    PropertyIndexerParquetDumper::dump(nodeProperties.indexers(), layout::nodePropIndexer(partDir));
    for (const auto& [propertyTypeID, container] : nodeProperties) {
        PropertyContainerParquetDumper::dump(*container,
                                             layout::nodeProps(partDir, propertyTypeID.getValue()));
    }

    const PropertyManager& edgeProperties = part.edgeProperties();
    PropertyIndexerParquetDumper::dump(edgeProperties.indexers(), layout::edgePropIndexer(partDir));
    for (const auto& [propertyTypeID, container] : edgeProperties) {
        PropertyContainerParquetDumper::dump(*container,
                                             layout::edgeProps(partDir, propertyTypeID.getValue()));
    }

    StringPropertyIndexerParquetDumper::dump(part.getNodeStrPropIndexer(),
                                             layout::nodeStringIndexes(partDir),
                                             layout::nodeStringChildren(partDir),
                                             layout::nodeStringOwners(partDir));

    StringPropertyIndexerParquetDumper::dump(part.getEdgeStrPropIndexer(),
                                             layout::edgeStringIndexes(partDir),
                                             layout::edgeStringChildren(partDir),
                                             layout::edgeStringOwners(partDir));
}
