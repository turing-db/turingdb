#include "DegreeProductsDumper.h"

#include "datapart/DegreeProducts.h"
#include "DegreeProductsDumpConstants.h"
#include "GraphDumpHelper.h"
#include "dump/DumpUtils.h"

using namespace db;

DegreeProductsDumper::DegreeProductsDumper(fs::FilePageWriter& writer)
    : _writer(writer)
{
}

DegreeProductsDumper::~DegreeProductsDumper() {
}

DumpResult<void> DegreeProductsDumper::dump(const DegreeProducts& products) {
    GraphDumpHelper::writeFileHeader(_writer);

    const std::span<const DegreeProduct> entries = products.getProducts();
    const uint64_t productCount = entries.size();

    DumpUtils::ensureDumpSpace(DegreeProductsDumpConstants::PRODUCT_COUNT_STRIDE, _writer);
    _writer.writeToCurrentPage(productCount);

    for (const DegreeProduct& product : entries) {
        const uint8_t firstIsOut = product._first._isOut ? 1 : 0;
        const uint8_t secondIsOut = product._second._isOut ? 1 : 0;
        const uint64_t sum = product._sum;

        DumpUtils::ensureDumpSpace(DegreeProductsDumpConstants::PRODUCT_STRIDE, _writer);
        _writer.writeToCurrentPage(product._labelSet.getID().getValue());
        _writer.writeToCurrentPage(product._first._edgeTypeID.getValue());
        _writer.writeToCurrentPage(firstIsOut);
        _writer.writeToCurrentPage(product._second._edgeTypeID.getValue());
        _writer.writeToCurrentPage(secondIsOut);
        _writer.writeToCurrentPage(sum);
    }

    return {};
}
