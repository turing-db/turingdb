#pragma once

#include "Profiler.h"
#include "properties/PropertyContainer.h"
#include "GraphDumpHelper.h"
#include "FilePageWriter.h"
#include "PropertyContainerDumpConstants.h"

namespace db {

class EmbeddingPropertyContainerDumper {
public:
    using Constants = EmbeddingPropertyContainerDumpConstants;

    explicit EmbeddingPropertyContainerDumper(fs::FilePageWriter& writer)
        : _writer(writer)
    {
    }

    [[nodiscard]] DumpResult<void> dump(const TypedPropertyContainer<types::Embedding>& props);

private:
    fs::FilePageWriter& _writer;
};

}
