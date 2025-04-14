#include "GraphReport.h"

#include "reader/GraphReader.h"
#include "DataPart.h"
#include "metadata/GraphMetadata.h"
#include "properties/PropertyManager.h"

namespace db {

void GraphReport::getReport(const GraphReader& reader, std::stringstream& report) {
    const GraphMetadata& metadata = reader.getMetadata();
    const auto& labelMap = metadata.labels();
    const auto& labelsetMap = metadata.labelsets();
    const auto& edgeTypeMap = metadata.edgeTypes();
    const auto& propTypeMap = metadata.propTypes();

    report << "Graph has " << std::endl;
    report << "  - " << reader.getDatapartCount() << " dataparts" << std::endl;
    report << "  - " << labelMap.getCount() << " labels" << std::endl;
    report << "  - " << labelsetMap.getCount() << " labelsets" << std::endl;
    report << "  - " << edgeTypeMap.getCount() << " edge types" << std::endl;
    report << "  - " << propTypeMap.getCount() << " property types" << std::endl;
    report << "# Name (type) id=X Count=X [LabelSetIndexed] (LabelSetBinarySearched)" << std::endl;

    for (const auto& part : reader.dataparts()) {
        const PropertyManager& nodeProperties = part->nodeProperties();
        for (const auto& [pt, ptName] : propTypeMap) {
            const auto* indexerPtr = nodeProperties.tryGetIndexer(pt._id);

            if (indexerPtr) {
                report << "# " << ptName << " id=" << pt._id.getValue() << " Count="
                       << nodeProperties.count(pt._id);
                const auto& indexer = *indexerPtr;

                report << "  [" << indexer.size() << "]";
                report << std::endl;
            }
        }
    }
}

}
