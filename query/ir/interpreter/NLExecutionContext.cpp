#include "NLExecutionContext.h"

#include "SystemAccessor.h"
#include "SystemManager.h"
#include "TuringConfig.h"
#include "VectorDatabase.h"

#include "NLSystemContext.h"
#include "NLWrittenValues.h"

using namespace db;

NLExecutionContext::NLExecutionContext(const GraphView* view,
                                       NLOutputSink* sink,
                                       size_t chunkSize,
                                       CommitWriteBuffer* writeBuffer,
                                       const NLSystemContext* system)
    : _view(view),
    _sink(sink),
    _chunkSize(chunkSize),
    _writeBuffer(writeBuffer),
    _system(system),
    _writtenValues(std::make_unique<NLWrittenValues>())
{
}

NLExecutionContext::~NLExecutionContext() {
}

vec::VectorDatabase* NLExecutionContext::getVectorDatabase() const {
    SystemAccessor* const accessor = _system ? _system->getAccessor() : nullptr;

    return accessor ? accessor->getVectorDatabase() : nullptr;
}

const fs::Path* NLExecutionContext::getDataDir() const {
    const SystemManager* const manager = _system ? _system->getSystemManager() : nullptr;
    if (!manager) {
        return nullptr;
    }

    return &manager->getConfig()->getDataDir();
}
