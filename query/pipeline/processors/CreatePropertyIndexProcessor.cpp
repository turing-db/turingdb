#include "CreatePropertyIndexProcessor.h"

#include <string_view>

#include "ExecutionContext.h"
#include "PipelineException.h"

#include "PipelineExecutor.h"
#include "PipelinePort.h"

#include "metadata/PropertyType.h"
#include "metadata/PropertyTypeDispatcher.h"
#include "metadata/SupportedType.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"

#include "FatalException.h"

using namespace db;

CreatePropertyIndexProcessor::CreatePropertyIndexProcessor(std::string_view indexName,
                                                           std::string_view propName,
                                                           bool isNodeIndex)
    : _indexName(indexName),
    _propertyName(propName),
    _isNodeIndex(isNodeIndex)
{
}

std::string CreatePropertyIndexProcessor::describe() const {
    return fmt::format("CreatePropertyIndexProcessor @={}", fmt::ptr(this));
}

CreatePropertyIndexProcessor* CreatePropertyIndexProcessor::create(PipelineV2* pipeline,
                                                                   std::string_view indexName,
                                                                   std::string_view propName,
                                                                   bool isNodeIndex) {
    CreatePropertyIndexProcessor* proc = new CreatePropertyIndexProcessor(indexName, propName, isNodeIndex);

    {
        PipelineOutputPort* out = PipelineOutputPort::create(pipeline, proc);

        proc->_output.setPort(out);
        proc->addOutput(out);
    }

    proc->postCreate(pipeline);

    return proc;
}

void CreatePropertyIndexProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;

    Transaction* rawTx = ctxt->getTransaction();
    if (!rawTx) {
        throw FatalException(
            "Attempted to create property index without transaction.");
    }

    if (!rawTx->writingPendingCommit()) {
        throw PipelineException(
            "Create index: Cannot perform writes outside of a write transaction");
    }

    auto& tx = rawTx->get<PendingCommitWriteTx>();
    _commitBuilder = tx.commitBuilder();

    bioassert(_commitBuilder,
              "Could not get commit builder to create property index.");

    markAsPrepared();
}

void CreatePropertyIndexProcessor::reset() {
    markAsReset();
}

void CreatePropertyIndexProcessor::execute() {
    const GraphView view = _ctxt->getGraphView();

    const GraphMetadata& metadata = view.metadata();
    const PropertyTypeMap& propMap = metadata.propTypes();

    const std::optional<PropertyType> maybeProp = propMap.get(_propertyName);

    bioassert(maybeProp.has_value(), "Property {} does not exist", _propertyName);

    const PropertyType prop = *maybeProp;

    const ValueType valueType = prop._valueType;
    const PropertyTypeID propID = prop._id;

    WeakArc<Index> newIndex {};

    const auto createIndex = [&]<SupportedType T>() {
        newIndex = _isNodeIndex
                     ? _commitBuilder->newNodePropertyIndex<T>(_indexName, propID)
                     : _commitBuilder->newEdgePropertyIndex<T>(_indexName, propID);
    };

    PropertyTypeDispatcher {valueType}.execute(createIndex);

    bioassert(newIndex, "Failed to create new index.");

    newIndex->init(view);

    CommitWriteBuffer& wb = _commitBuilder->writeBuffer();

    wb.addPendingIndex(newIndex);

    finish();
}
