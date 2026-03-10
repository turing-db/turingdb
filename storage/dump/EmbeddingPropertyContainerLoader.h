#pragma once

#include <memory>

#include "Profiler.h"
#include "properties/PropertyContainer.h"
#include "FilePageReader.h"
#include "DumpConfig.h"
#include "GraphDumpHelper.h"
#include "PropertyContainerDumpConstants.h"

namespace db {

class EmbeddingPropertyContainerLoader {
public:
    using Constants = EmbeddingPropertyContainerDumpConstants;

    explicit EmbeddingPropertyContainerLoader(fs::FilePageReader& reader)
        : _reader(reader)
    {
    }

    [[nodiscard]] DumpResult<std::unique_ptr<PropertyContainer>> load();

private:
    fs::FilePageReader& _reader;
};

}
