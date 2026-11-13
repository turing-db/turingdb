#include "NLRuntimeProperty.h"

#include "writers/MetadataBuilder.h"

#include "NLWriteProperties.h"

using namespace db;

NLRuntimeProperty::NLRuntimeProperty(std::string_view name)
    : _name(name)
{
}

NLRuntimeProperty::~NLRuntimeProperty() {
}

bool NLRuntimeProperty::resolve(MetadataBuilder* metadata,
                                const Column* cells,
                                size_t rowCount,
                                const ColumnVector<CustomBool>* written) {
    if (findType(metadata)) {
        return true;
    }

    const ValueType widest = cellPropertyType(cells, rowCount, written, _name);
    if (widest == ValueType::Invalid) {
        return false;
    }

    _type = metadata->getOrCreatePropertyType(_name, widest);

    return true;
}

bool NLRuntimeProperty::stage(MetadataBuilder* metadata,
                              const Column* cells,
                              size_t rowCount,
                              const ColumnVector<CustomBool>* written,
                              CommitWriteBuffer::UntypedProperties& buffer) {
    if (!resolve(metadata, cells, rowCount, written)) {
        buffer.clear();
        return false;
    }

    extractCellProperties(cells, rowCount, written, _type._id, _type._valueType, buffer);

    return true;
}

bool NLRuntimeProperty::stageNulls(MetadataBuilder* metadata, size_t rowCount, CommitWriteBuffer::UntypedProperties& buffer) {
    if (!findType(metadata)) {
        buffer.clear();
        return false;
    }

    fillNullProperties(rowCount, _type._id, _type._valueType, buffer);

    return true;
}

bool NLRuntimeProperty::findType(const MetadataBuilder* metadata) {
    if (!_type.isValid()) {
        _type = metadata->findPropertyType(_name).value_or(PropertyType {});
    }

    return _type.isValid();
}
