#include "GraphReport.h"

#include "reader/GraphReader.h"
#include "DataPart.h"
#include "GraphMetadata.h"
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
        for (const auto& [ptID, ptName] : propTypeMap._idMap) {
            const auto* indexerPtr = nodeProperties.tryGetIndexer(ptID);

            if (indexerPtr) {
                report << "# " << ptName << " id=" << ptID.getValue() << " Count="
                       << nodeProperties.count(ptID);
                const auto& indexer = *indexerPtr;
                size_t i = 0;
                size_t binarySearched = 0;
                for (const auto& [labelset, infos] : indexer) {
                    if (infos._binarySearched) {
                        binarySearched++;
                    } else {
                        i++;
                    }
                }
                report << "  [" << i << "]";
                if (binarySearched != 0) {
                    report << " (BS=" << binarySearched << ")";
                }
                report << std::endl;
            }
        }
    }
}

}
