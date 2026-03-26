#include "CreateNodePropertyIndexProcessor.h"

#include <string_view>

#include "ExecutionContext.h"
#include "PipelineException.h"

#include "PipelineExecutor.h"
#include "PipelinePort.h"

#include "metadata/PropertyType.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"

#include "FatalException.h"

using namespace db;

CreateNodePropertyIndexProcessor::CreateNodePropertyIndexProcessor(std::string_view indexName,
                                                                   std::string_view propName)
    : _indexName(indexName),
    _propertyName(propName)
{
}

std::string CreateNodePropertyIndexProcessor::describe() const {
    return fmt::format("CreatePropertyIndexProcessor @={}", fmt::ptr(this));
}

CreateNodePropertyIndexProcessor* CreateNodePropertyIndexProcessor::create(PipelineV2* pipeline,
                                                                           std::string_view indexName,
                                                                           std::string_view propName) {
    CreateNodePropertyIndexProcessor* proc = new CreateNodePropertyIndexProcessor(indexName, propName);

    {
        PipelineOutputPort* out = PipelineOutputPort::create(pipeline, proc);

        proc->_output.setPort(out);
        proc->addOutput(out);
    }

    proc->postCreate(pipeline);

    return proc;
}

void CreateNodePropertyIndexProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;

    Transaction* rawTx = ctxt->getTransaction();
    if (!rawTx) {
        throw FatalException(
            "Attempted to create node property index without transaction.");
    }

    if (!rawTx->writingPendingCommit()) {
        throw PipelineException(
            "Create index: Cannot perform writes outside of a write transaction");
    }

    auto& tx = rawTx->get<PendingCommitWriteTx>();
    _commitBuilder = tx.commitBuilder();

    bioassert(_commitBuilder,
              "Could not get commit builder to create node property index.");

    markAsPrepared();
}

void CreateNodePropertyIndexProcessor::reset() {
    markAsReset();
}

void CreateNodePropertyIndexProcessor::execute() {
    const GraphView view = _ctxt->getGraphView();

    const GraphMetadata& metadata = view.metadata();
    const PropertyTypeMap& propMap = metadata.propTypes();

    const std::optional<PropertyType> maybeProp = propMap.get(_propertyName);

    bioassert(maybeProp.has_value(), "Property {} does not exist", _propertyName);

    const PropertyType prop = *maybeProp;

    const ValueType valueType = prop._valueType;
    const PropertyTypeID propID = prop._id;

    switch (valueType) {

        case ValueType::Int64:
            _commitBuilder->newNodePropertyIndex<types::Int64>(propID);
        break;
        case ValueType::UInt64:
            _commitBuilder->newNodePropertyIndex<types::UInt64>(propID);
        break;
        case ValueType::Double:
            _commitBuilder->newNodePropertyIndex<types::Double>(propID);
        break;
        case ValueType::String:
            _commitBuilder->newNodePropertyIndex<types::String>(propID);
        break;
        case ValueType::Bool:
            _commitBuilder->newNodePropertyIndex<types::Bool>(propID);
        break;
        case ValueType::Embedding:
            _commitBuilder->newNodePropertyIndex<types::Embedding>(propID);
        break;

        case ValueType::_SIZE:
        case ValueType::Invalid:
            throw FatalException("Attempted to create index on invalid property type.");
        break;
    }

}
