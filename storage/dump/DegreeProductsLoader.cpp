#include "DegreeProductsLoader.h"

#include <optional>
#include <vector>

#include "datapart/DegreeProducts.h"
#include "metadata/GraphMetadata.h"
#include "DegreeProductsDumpConstants.h"
#include "GraphDumpHelper.h"
#include "LoadUtils.h"

using namespace db;

DegreeProductsLoader::DegreeProductsLoader(fs::FilePageReader& reader)
    : _reader(reader)
{
}

DegreeProductsLoader::~DegreeProductsLoader() {
}

DumpResult<void> DegreeProductsLoader::load(const GraphMetadata& metadata, DegreeProducts& products) {
    _reader.nextPage();

    if (_reader.errorOccured()) {
        return DumpError::result(DumpErrorType::COULD_NOT_READ_DEGREE_PRODUCTS,
                                 _reader.error().value());
    }

    auto it = _reader.begin();
    LoadUtils::ensureIteratorReadPage(it);

    if (auto res = GraphDumpHelper::checkFileHeader(it); !res) {
        return res.get_unexpected();
    }

    LoadUtils::ensureLoadSpace(DegreeProductsDumpConstants::PRODUCT_COUNT_STRIDE, _reader, it);
    const uint64_t productCount = it.get<uint64_t>();

    const LabelSetMap& labelSets = metadata.labelsets();

    std::vector<DegreeProduct> entries;
    entries.reserve(productCount);

    for (uint64_t i = 0; i < productCount; i++) {
        LoadUtils::ensureLoadSpace(DegreeProductsDumpConstants::PRODUCT_STRIDE, _reader, it);

        const LabelSetID labelSetID = it.get<LabelSetID::Type>();
        const EdgeTypeID firstType = it.get<EdgeTypeID::Type>();
        const uint8_t firstIsOut = it.get<uint8_t>();
        const EdgeTypeID secondType = it.get<EdgeTypeID::Type>();
        const uint8_t secondIsOut = it.get<uint8_t>();
        const uint64_t sum = it.get<uint64_t>();

        const std::optional<LabelSetHandle> labelSet = labelSets.getValue(labelSetID);
        if (!labelSet) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_DEGREE_PRODUCTS);
        }

        entries.push_back(DegreeProduct {._labelSet = *labelSet,
                                         ._first = TypedDegree {._edgeTypeID = firstType, ._isOut = firstIsOut != 0},
                                         ._second = TypedDegree {._edgeTypeID = secondType, ._isOut = secondIsOut != 0},
                                         ._sum = sum});
    }

    products.assign(entries);

    return {};
}
