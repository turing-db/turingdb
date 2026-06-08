#include "SystemAccessor.h"

#include "SystemManager.h"

#include "BioAssert.h"

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

void SystemAccessor::init() {
    bioassert(this->isUnique(), "SystemAccessor::init requires unique access");

    _sysMan->init();
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

Graph* SystemAccessor::loadGraph(std::string_view name) {
    return _sysMan->loadGraph(name);
}

bool SystemAccessor::isGraphLoading(std::string_view name) const {
    return _sysMan->isGraphLoading(name);
}

Graph* SystemAccessor::importGraph(const fs::Path& path, std::string_view name) {
    return _sysMan->importGraph(path, name);
}

GraphFileType SystemAccessor::getGraphFileType(const fs::Path& path) const {
    return _sysMan->getGraphFileType(path);
}
