#include "CreatePropertyIndexProcessor.h"

#include <string_view>

#include "ExecutionContext.h"
#include "PipelineException.h"

#include "PipelineExecutor.h"
#include "PipelinePort.h"

using namespace db;

CreatePropertyIndexProcessor::CreatePropertyIndexProcessor(std::string_view propertyName)
    : _propertyName(propertyName)
{
}

CreatePropertyIndexProcessor* CreatePropertyIndexProcessor::create(PipelineV2* pipeline,
                                                                   std::string_view propertyName) {
    CreatePropertyIndexProcessor* proc = new CreatePropertyIndexProcessor(propertyName);

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
    markAsPrepared();
}

void CreatePropertyIndexProcessor::reset() {
    markAsReset();
}

void CreatePropertyIndexProcessor::execute() {
    throw PipelineException("Property indexes are not yet supported.");
}
