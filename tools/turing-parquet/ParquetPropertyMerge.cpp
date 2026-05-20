#include "ParquetPropertyMerge.h"

using namespace db;

namespace {

// Merge `source` into `target`: accumulate counts, propagate nullable/mixed, reconcile value types, and recurse into sub-properties and the array element type.
void mergePropertyType(const ParquetPropertyType& source, ParquetPropertyType& target) {
    target.addCount(source.getCount());
    if (source.isNullable()) {
        target.markNullable();
    }

    if (source.hasNonNullValue()) {
        if (!target.hasNonNullValue()) {
            target.setValueType(source.getValueType());
        } else if (target.getValueType() != source.getValueType()) {
            target.markMixed();
        }
    }
    if (source.isMixed()) {
        target.markMixed();
    }

    for (const auto& entry : source.getSubProperties()) {
        mergePropertyType(*entry.second, target.getOrCreateSubProperty(entry.first));
    }

    const ParquetPropertyType* const sourceElement = source.getElementType();
    if (sourceElement != nullptr) {
        mergePropertyType(*sourceElement, target.getOrCreateElementType());
    }
}

}

ParquetPropertyMerge::ParquetPropertyMerge(ParquetPropertyAnalysis& merged)
    : _merged(merged)
{
}

ParquetPropertyMerge::~ParquetPropertyMerge() {
}

void ParquetPropertyMerge::merge(const ParquetPropertyAnalysis& source) {
    for (size_t typeIndex = 0; typeIndex < ParquetPropertyAnalysis::TYPE_COUNT; ++typeIndex) {
        const auto type = static_cast<ParquetJsonValueType>(typeIndex);
        _merged.addTypeCount(type, source.getTypeCount(type));
    }

    for (const std::string& preview : source.getArrayPreviews()) {
        _merged.recordArrayPreview(preview);
    }
    
    for (const std::string& preview : source.getObjectPreviews()) {
        _merged.recordObjectPreview(preview);
    }

    for (const auto& entry : source.getPropertyTypes()) {
        mergePropertyType(*entry.second, _merged.getOrCreatePropertyType(entry.first));
    }
}
