#include "SystemAccessor.h"

#include "SystemManager.h"

#include "versioning/Transaction.h"

using namespace db;

SystemAccessor::SystemAccessor(SystemManager* sysMan, SharedAccess)
    : Accessor(sysMan->_sysLock, SharedAccess{}),
    _sysMan(sysMan)
{
}

SystemAccessor::SystemAccessor(SystemManager* sysMan, UniqueAccess)
    : Accessor(sysMan->_sysLock, UniqueAccess{}),
    _sysMan(sysMan)
{
}

SystemAccessor::~SystemAccessor() {
}

Graph* SystemAccessor::getDefaultGraph() const {
    return _sysMan->getDefaultGraph();
}

Graph* SystemAccessor::getGraph(std::string_view name) const {
    return _sysMan->getGraph(name);
}

size_t SystemAccessor::getGraphCount() const {
    return _sysMan->getGraphCount();
}

Graph* SystemAccessor::createGraph(std::string_view graphName) {
    return _sysMan->createGraph(graphName);
}

void SystemAccessor::listGraphs(std::vector<std::string_view>& names) const {
    _sysMan->listGraphs(names);
}

void SystemAccessor::listAvailableGraphs(std::vector<std::string>& names) const {
    _sysMan->listAvailableGraphs(names);
}

void SystemAccessor::setDefaultGraph(std::string_view name) {
    _sysMan->setDefaultGraph(name);
}

Graph* SystemAccessor::loadGraph(std::string_view name) {
    return _sysMan->loadGraph(name);
}

bool SystemAccessor::isGraphLoading(std::string_view name) const {
    return _sysMan->isGraphLoading(name);
}

DumpResult<void> SystemAccessor::loadCommit(std::string_view name, CommitHash hash) {
    return _sysMan->loadCommit(name, hash);
}

Graph* SystemAccessor::importGraph(const fs::Path& path, std::string_view name) {
    return _sysMan->importGraph(path, name);
}

GraphFileType SystemAccessor::getGraphFileType(const fs::Path& path) const {
    return _sysMan->getGraphFileType(path);
}

DumpResult<void> SystemAccessor::dumpGraph(std::string_view name) {
    return _sysMan->dumpGraph(name);
}

ChangeResult<Change*> SystemAccessor::newChange(std::string_view name, CommitHash baseHash) {
    return _sysMan->newChange(name, baseHash);
}

ChangeResult<Change*> SystemAccessor::getChange(const Graph* graph, ChangeID changeID) {
    return _sysMan->getChange(graph, changeID);
}

ChangeResult<void> SystemAccessor::submitChange(ChangeAccessor& accessor, JobSystem& jobSystem) {
    return _sysMan->submitChange(accessor, jobSystem);
}

ChangeResult<void> SystemAccessor::deleteChange(ChangeAccessor& accessor, ChangeID changeID) {
    return _sysMan->deleteChange(accessor, changeID);
}

void SystemAccessor::listChanges(std::vector<const Change*>& changes, const Graph* graph) const {
    _sysMan->listChanges(changes, graph);
}

DataPartMergeResult<void> SystemAccessor::mergeDataParts(Graph* graph, JobSystem& jobSystem) {
    return _sysMan->mergeDataParts(graph, jobSystem);
}

ChangeResult<Transaction> SystemAccessor::openTransaction(std::string_view graphName,
                                                          CommitHash commitHash,
                                                          ChangeID changeID) {
    return _sysMan->openTransaction(graphName, commitHash, changeID);
}

void SystemAccessor::createS3Client(const std::string& accessId,
                                    const std::string& secretKey,
                                    const std::string& region) {
    _sysMan->createS3Client(accessId, secretKey, region);
}

S3::TuringS3Client<S3::MinioS3ClientWrapper>* SystemAccessor::getS3Client() {
    return _sysMan->getS3Client();
}
