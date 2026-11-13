#pragma once

#include <stddef.h>

#include <string>
#include <string_view>

#include "metadata/PropertyType.h"
#include "versioning/CommitWriteBuffer.h"

namespace db {

class Column;

template <typename T>
class ColumnVector;

class MetadataBuilder;

// A property a write reaches by name and types as the program runs. It has the type the
// change knows it by or, where it has none, the widest type the values of the first chunk
// of tagged cells holding one are staged as. Every such write to the name in a program
// shares it, so a removal sees the type a write of cells gave the property before it.
class NLRuntimeProperty {
public:
    explicit NLRuntimeProperty(std::string_view name);
    ~NLRuntimeProperty();

    std::string_view getName() const { return _name; }
    const PropertyType& getType() const { return _type; }

    // False where the change knows the property by no type yet
    bool findType(const MetadataBuilder* metadata);

    // False where the property has no type and the rows @param written holds - every row
    // for a null mask - are none or hold only nulls, which leaves it none
    bool resolve(MetadataBuilder* metadata, const Column* cells, size_t rowCount, const ColumnVector<CustomBool>* written);

    bool stage(MetadataBuilder* metadata,
               const Column* cells,
               size_t rowCount,
               const ColumnVector<CustomBool>* written,
               CommitWriteBuffer::UntypedProperties& buffer);

    bool stageNulls(MetadataBuilder* metadata, size_t rowCount, CommitWriteBuffer::UntypedProperties& buffer);

private:
    std::string _name;
    PropertyType _type;
};

}
