#include "LoadCommitProcessor.h"

#include <spdlog/spdlog.h>
#include <spdlog/fmt/fmt.h>

#include "ExecutionContext.h"
#include "SystemManager.h"
#include "PipelineException.h"
#include "versioning/CommitHash.h"

using namespace db;

LoadCommitProcessor::LoadCommitProcessor(std::string_view hashStr)
    : _hashStr(hashStr) {
}

LoadCommitProcessor::~LoadCommitProcessor() {
}

LoadCommitProcessor* LoadCommitProcessor::create(PipelineV2* pipeline,
                                                 std::string_view hashStr) {
    LoadCommitProcessor* proc = new LoadCommitProcessor(hashStr);

    PipelineOutputPort* output = PipelineOutputPort::create(pipeline, proc);
    proc->_output.setPort(output);
    proc->addOutput(output);
    proc->postCreate(pipeline);

    return proc;
}

std::string LoadCommitProcessor::describe() const {
    return fmt::format("LoadCommitProcessor @={}", fmt::ptr(this));
}

void LoadCommitProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;
    markAsPrepared();
}

void LoadCommitProcessor::reset() {
}

void LoadCommitProcessor::execute() {
    const auto hashRes = CommitHash::fromString(_hashStr);
    if (!hashRes) {
        throw PipelineException(
            fmt::format("Invalid commit hash: '{}'", _hashStr));
    }

    SystemAccessor* system = _ctxt->getSystemAccessor();
    auto res = system->loadCommit(_ctxt->getGraphName(), hashRes.value());

    if (!res) {
        throw PipelineException(
            fmt::format("Failed to load commit '{}': {}", _hashStr, res.error().fmtMessage()));
    }

    spdlog::info("Commit {} has been loaded", _hashStr);
    _output.getPort()->writeData();
    finish();
}
